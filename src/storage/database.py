"""PostgreSQL storage layer.

Design notes
------------
* The schema lives in ``schema.sql`` (idempotent DDL) and is the single source of truth.
* Positions are bulk-loaded with ``COPY`` into a temporary staging table, then merged with
  ``INSERT ... ON CONFLICT DO NOTHING`` on the natural key (mmsi, base_datetime).
  Re-running the same input is therefore a no-op: loads are idempotent.
* Vessel metrics are recomputed from the *full* history of every vessel touched by a load
  and upserted, so they stay correct when data arrives day by day.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# NOAA / MarineCadastre column name -> database column name
AIS_COLUMN_MAP = {
    "MMSI": "mmsi",
    "BaseDateTime": "base_datetime",
    "LAT": "latitude",
    "LON": "longitude",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "VesselName": "vessel_name",
    "IMO": "imo",
    "CallSign": "call_sign",
    "VesselType": "vessel_type",
    "Status": "status",
    "Length": "length",
    "Width": "width",
    "Draft": "draft",
    "Cargo": "cargo",
    "TransceiverClass": "transceiver_class",
}
AIS_DB_COLUMNS = list(AIS_COLUMN_MAP.values())
INTEGER_COLUMNS = ["mmsi", "vessel_type", "status", "cargo"]

METRIC_COLUMNS = [
    "mmsi",
    "vessel_name",
    "vessel_type",
    "total_distance_nm",
    "total_time_hours",
    "moving_time_hours",
    "at_dock_time_hours",
    "point_count",
    "avg_speed_knots",
    "max_speed_knots",
    "first_seen",
    "last_seen",
]


def to_db_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Rename NOAA columns to DB columns, add missing ones, and fix dtypes for COPY."""
    out = df.rename(columns=AIS_COLUMN_MAP)
    for col in AIS_DB_COLUMNS:
        if col not in out.columns:
            out[col] = None
    out = out[AIS_DB_COLUMNS].copy()
    # Integer codes arrive as floats when they contain NaN ("70.0"); Postgres SMALLINT rejects that.
    for col in INTEGER_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce").round().astype("Int64")
    out["base_datetime"] = pd.to_datetime(out["base_datetime"], utc=True)
    for col in ("vessel_name", "imo", "call_sign", "transceiver_class"):
        out[col] = out[col].astype("string").str.slice(0, 100)
    return out


class DatabaseManager:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, pool_pre_ping=True)

    # ------------------------------------------------------------------ schema
    def create_tables(self) -> None:
        """Create tables, reference data and views if they don't exist."""
        with self.engine.begin() as conn:
            conn.exec_driver_sql(SCHEMA_PATH.read_text(encoding="utf-8"))
        logger.info("Schema is up to date")

    # ------------------------------------------------------------------ positions
    def save_ais_data(self, df: pd.DataFrame) -> int:
        """Bulk-load cleaned positions. Returns the number of *new* rows inserted."""
        if df.empty:
            return 0
        frame = to_db_frame(df)
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False, header=False, na_rep="", date_format="%Y-%m-%d %H:%M:%S%z")
        buffer.seek(0)

        cols = ", ".join(AIS_DB_COLUMNS)
        with self.engine.begin() as conn:
            cur = conn.connection.cursor()
            cur.execute(
                "CREATE TEMP TABLE ais_staging (LIKE ais_data INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            cur.copy_expert(f"COPY ais_staging ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')", buffer)
            cur.execute(
                f"INSERT INTO ais_data ({cols}) SELECT {cols} FROM ais_staging "
                "ON CONFLICT (mmsi, base_datetime) DO NOTHING"
            )
            inserted = cur.rowcount
        logger.info("Positions: %s received, %s new, %s already present",
                    len(frame), inserted, len(frame) - inserted)
        return inserted

    def load_positions(self, mmsis: Iterable[int]) -> pd.DataFrame:
        """Full position history for the given vessels, with NOAA-style column names."""
        query = text(
            """
            SELECT mmsi, base_datetime, latitude, longitude, sog, status, vessel_name, vessel_type
            FROM ais_data
            WHERE mmsi = ANY(:mmsis)
            ORDER BY mmsi, base_datetime
            """
        )
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"mmsis": [int(m) for m in mmsis]})
        reverse = {v: k for k, v in AIS_COLUMN_MAP.items()}
        return df.rename(columns=reverse)

    # ------------------------------------------------------------------ metrics
    def upsert_vessel_metrics(self, metrics: pd.DataFrame) -> int:
        if metrics.empty:
            return 0
        frame = metrics[METRIC_COLUMNS].astype(object).where(metrics[METRIC_COLUMNS].notna(), None)
        rows = [tuple(r) for r in frame.itertuples(index=False, name=None)]
        cols = ", ".join(METRIC_COLUMNS)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in METRIC_COLUMNS if c != "mmsi")
        sql = (
            f"INSERT INTO vessel_metrics ({cols}) VALUES %s "
            f"ON CONFLICT (mmsi) DO UPDATE SET {updates}, last_updated = now()"
        )
        with self.engine.begin() as conn:
            execute_values(conn.connection.cursor(), sql, rows, page_size=1000)
        return len(rows)

    def refresh_vessel_metrics(self, mmsis: Iterable[int], processor, chunk_size: int = 500) -> int:
        """Recompute metrics from each vessel's full history (keeps memory bounded)."""
        mmsis = sorted({int(m) for m in mmsis})
        total = 0
        for i in range(0, len(mmsis), chunk_size):
            positions = self.load_positions(mmsis[i : i + chunk_size])
            total += self.upsert_vessel_metrics(processor.calculate_vessel_metrics(positions))
        logger.info("Vessel metrics refreshed for %s vessels", total)
        return total

    def refresh_views(self) -> None:
        with self.engine.begin() as conn:
            conn.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_daily_traffic_stats")
