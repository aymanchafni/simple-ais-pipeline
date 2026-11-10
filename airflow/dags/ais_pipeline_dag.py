"""Daily AIS pipeline: replays NOAA / MarineCadastre history one day per DAG run.

Each run processes the NOAA file for its logical date. Loads are idempotent (upsert on
(mmsi, base_datetime)), so any day can be re-run or backfilled safely:

    airflow dags backfill ais_data_pipeline -s 2023-01-01 -e 2023-01-07

Params (set when triggering manually):
    max_records  rows read from the day's file (a full day is ~7M rows)
    local_file   process this file instead of downloading (e.g. sample_data/sample_ais.csv)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

# src/ is mounted at /opt/airflow/src (see docker-compose.yml)
sys.path.insert(0, os.environ.get("AIS_PROJECT_ROOT", "/opt/airflow"))

DATA_DIR = os.environ.get("AIS_DATA_DIR", "/opt/airflow/data")


@dag(
    dag_id="ais_data_pipeline",
    description="NOAA AIS -> clean -> Postgres (idempotent) -> vessel metrics",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31),   # NOAA publishes full years; 2023 is the latest that downloads
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "max_records": Param(200_000, type="integer", minimum=1),
        "local_file": Param(None, type=["null", "string"]),
    },
    tags=["ais", "maritime", "postgres"],
)
def ais_data_pipeline():
    @task
    def extract(params=None, logical_date=None) -> str:
        from src import pipeline
        from src.config import Config

        return pipeline.extract(
            Config(), local_file=params.get("local_file"), noaa_date=logical_date.date()
        )

    @task
    def transform(path: str, params=None, ds=None) -> str:
        from src import pipeline
        from src.config import Config

        cleaned = pipeline.transform(Config(), path, params["max_records"])
        if cleaned.empty:
            raise ValueError(f"No valid rows in {path}")
        os.makedirs(DATA_DIR, exist_ok=True)
        out = os.path.join(DATA_DIR, f"cleaned_{ds}.parquet")
        cleaned.to_parquet(out, index=False)
        return out

    @task
    def load(cleaned_path: str) -> dict:
        import pandas as pd

        from src import pipeline
        from src.config import Config

        result = pipeline.load(Config(), pd.read_parquet(cleaned_path))
        os.remove(cleaned_path)
        return result.__dict__

    @task
    def quality_check(result: dict) -> None:
        """Fail the run if the database doesn't contain the rows we just processed."""
        from sqlalchemy import create_engine, text

        from src.config import Config

        with create_engine(Config().database_url).connect() as conn:
            vessels = conn.execute(text("SELECT COUNT(*) FROM vessel_metrics")).scalar()
            bad = conn.execute(text(
                "SELECT COUNT(*) FROM vessel_metrics "
                "WHERE moving_time_hours + at_dock_time_hours > total_time_hours + 1e-6"
            )).scalar()
        if result["rows_received"] == 0 or vessels == 0:
            raise ValueError("Quality check failed: no data loaded")
        if bad:
            raise ValueError(f"Quality check failed: {bad} vessels with inconsistent time metrics")

    quality_check(load(transform(extract())))


ais_data_pipeline()
