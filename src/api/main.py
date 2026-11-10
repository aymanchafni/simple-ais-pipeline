"""REST API over the AIS tables.

* All user input goes through bound parameters (no SQL string formatting).
* Static routes (/vessels/search) are declared before parameterised ones (/vessels/{mmsi}),
  otherwise FastAPI would try to parse "search" as an MMSI and return 422.
* Handlers are plain ``def``: FastAPI runs them in a thread pool, so blocking DB calls
  don't stall the event loop.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.config import Config
from src.storage.database import DatabaseManager

logger = logging.getLogger(__name__)

app = FastAPI(
    title="AIS Maritime Traffic API",
    version="2.0.0",
    description="Vessel positions and per-vessel metrics computed by the AIS data pipeline.",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

config = Config()
db_manager = DatabaseManager(config.database_url)


def get_connection() -> Iterator[Connection]:
    with db_manager.engine.connect() as conn:
        yield conn


def rows(result) -> list[dict]:
    return [dict(r._mapping) for r in result]


VESSEL_COLUMNS = """
    mmsi, vessel_name, vessel_type, total_distance_nm, total_time_hours,
    moving_time_hours, at_dock_time_hours, point_count, avg_speed_knots,
    max_speed_knots, first_seen, last_seen, last_updated
"""


@app.get("/")
def root():
    return {
        "message": "AIS Data Pipeline API",
        "version": app.version,
        "docs": "/docs",
        "endpoints": [
            "/health", "/statistics", "/vessels", "/vessels/search", "/vessels/{mmsi}",
            "/metrics/time-analysis", "/metrics/quality", "/metrics/summary",
        ],
    }


@app.get("/health")
def health_check():
    try:
        with db_manager.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:  # noqa: BLE001 - health endpoint must never raise
        logger.error("Health check failed: %s", e)
        return {"status": "unhealthy", "database": "disconnected", "timestamp": datetime.now().isoformat()}


@app.get("/vessels")
def get_vessels(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    conn: Connection = Depends(get_connection),
):
    query = text(
        f"SELECT {VESSEL_COLUMNS} FROM vessel_metrics "
        "ORDER BY total_distance_nm DESC LIMIT :limit OFFSET :offset"
    )
    vessels = rows(conn.execute(query, {"limit": limit, "offset": offset}))
    return {"vessels": vessels, "count": len(vessels), "limit": limit, "offset": offset}


@app.get("/vessels/search")
def search_vessels(
    name: Optional[str] = Query(None, max_length=100, description="Partial, case-insensitive name"),
    vessel_type: Optional[int] = Query(None, description="ITU vessel type code, e.g. 70 = cargo"),
    min_distance: Optional[float] = Query(None, ge=0),
    max_distance: Optional[float] = Query(None, ge=0),
    limit: int = Query(50, ge=1, le=500),
    conn: Connection = Depends(get_connection),
):
    conditions, params = [], {"limit": limit}
    if name:
        conditions.append("vessel_name ILIKE :name")
        params["name"] = f"%{name}%"
    if vessel_type is not None:
        conditions.append("vessel_type = :vessel_type")
        params["vessel_type"] = vessel_type
    if min_distance is not None:
        conditions.append("total_distance_nm >= :min_distance")
        params["min_distance"] = min_distance
    if max_distance is not None:
        conditions.append("total_distance_nm <= :max_distance")
        params["max_distance"] = max_distance
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = text(
        f"SELECT {VESSEL_COLUMNS} FROM vessel_metrics {where} "
        "ORDER BY total_distance_nm DESC LIMIT :limit"
    )
    vessels = rows(conn.execute(query, params))
    filters = {k: v for k, v in params.items() if k != "limit"}
    return {"vessels": vessels, "count": len(vessels), "filters_applied": filters}


@app.get("/vessels/{mmsi}")
def get_vessel_details(mmsi: int, conn: Connection = Depends(get_connection)):
    metrics = conn.execute(
        text(f"SELECT {VESSEL_COLUMNS} FROM vessel_metrics WHERE mmsi = :mmsi"), {"mmsi": mmsi}
    ).fetchone()
    if not metrics:
        raise HTTPException(status_code=404, detail="Vessel not found")

    positions = rows(conn.execute(
        text(
            "SELECT base_datetime, latitude, longitude, sog, cog, heading, status "
            "FROM ais_data WHERE mmsi = :mmsi ORDER BY base_datetime DESC LIMIT 100"
        ),
        {"mmsi": mmsi},
    ))
    return {"metrics": dict(metrics._mapping), "recent_positions": positions,
            "positions_count": len(positions)}


@app.get("/statistics")
def get_statistics(conn: Connection = Depends(get_connection)):
    # Aggregates are computed on vessel_metrics alone. (Joining ais_data here would repeat each
    # vessel once per position and silently weight the averages by ping count.)
    global_stats = conn.execute(text(
        """
        SELECT
            (SELECT COUNT(*) FROM vessel_metrics) AS total_vessels,
            (SELECT COUNT(*) FROM ais_data)       AS total_positions,
            AVG(total_distance_nm)                AS avg_distance,
            AVG(moving_time_hours)                AS avg_moving_time,
            AVG(at_dock_time_hours)               AS avg_dock_time,
            AVG(avg_speed_knots)                  AS avg_speed_fleet,
            MAX(max_speed_knots)                  AS max_speed_recorded
        FROM vessel_metrics
        """
    )).fetchone()
    top = rows(conn.execute(text(
        """
        SELECT mmsi, vessel_name, total_distance_nm, avg_speed_knots, point_count,
               moving_time_hours, at_dock_time_hours
        FROM vessel_metrics WHERE total_distance_nm > 0
        ORDER BY total_distance_nm DESC LIMIT 5
        """
    )))
    return {
        "global_statistics": dict(global_stats._mapping),
        "top_vessels_by_distance": top,
        "generated_at": datetime.now().isoformat(),
    }


@app.get("/metrics/time-analysis")
def get_time_analysis(conn: Connection = Depends(get_connection)):
    stats = dict(conn.execute(text(
        """
        SELECT
            COALESCE(SUM(moving_time_hours), 0)  AS total_moving_time,
            COALESCE(SUM(at_dock_time_hours), 0) AS total_dock_time,
            COALESCE(SUM(total_time_hours), 0)   AS total_time,
            COALESCE(AVG(moving_time_hours), 0)  AS avg_moving_time_per_vessel,
            COALESCE(AVG(at_dock_time_hours), 0) AS avg_dock_time_per_vessel,
            COUNT(*)                             AS vessels_count
        FROM vessel_metrics WHERE total_time_hours > 0
        """
    )).fetchone()._mapping)
    total = stats["total_time"] or 0
    stats["moving_time_percentage"] = stats["total_moving_time"] / total * 100 if total else 0
    stats["dock_time_percentage"] = stats["total_dock_time"] / total * 100 if total else 0
    return {"time_analysis": stats, "generated_at": datetime.now().isoformat()}


@app.get("/metrics/quality")
def get_data_quality(conn: Connection = Depends(get_connection)):
    stats = dict(conn.execute(text(
        """
        SELECT
            COUNT(*)                                                      AS total_records,
            COUNT(*) FILTER (WHERE sog IS NOT NULL)                       AS valid_speeds,
            COUNT(*) FILTER (WHERE heading IS NOT NULL)                   AS valid_headings,
            COUNT(*) FILTER (WHERE vessel_name IS NOT NULL AND vessel_name <> '') AS valid_names,
            COUNT(*) FILTER (WHERE vessel_type IS NOT NULL)               AS valid_types,
            COUNT(DISTINCT mmsi)                                          AS unique_vessels
        FROM ais_data
        """
    )).fetchone()._mapping)
    total = stats["total_records"]
    for field in ("valid_speeds", "valid_headings", "valid_names", "valid_types"):
        stats[f"{field}_percentage"] = stats[field] / total * 100 if total else 0
    # Positions and timestamps are enforced NOT NULL by the schema, so completeness is
    # measured on the optional attributes.
    stats["overall_quality_score"] = (
        sum(stats[f"{f}_percentage"] for f in ("valid_speeds", "valid_names", "valid_types")) / 3
    )
    return {"data_quality": stats, "generated_at": datetime.now().isoformat()}


@app.get("/metrics/summary")
def get_metrics_summary(conn: Connection = Depends(get_connection)):
    return {
        "summary": {
            "statistics": get_statistics(conn),
            "time_analysis": get_time_analysis(conn),
            "data_quality": get_data_quality(conn),
        },
        "generated_at": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
