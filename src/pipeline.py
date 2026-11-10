"""Extract -> transform -> load steps, shared by the CLI (scripts/run_pipeline.py)
and the Airflow DAG (airflow/dags/ais_pipeline_dag.py)."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Optional

import pandas as pd

from src.config import Config, noaa_daily_url
from src.ingestion.data_loader import AISDataLoader
from src.storage.database import DatabaseManager
from src.transformation.data_processor import AISDataProcessor

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    rows_received: int
    rows_inserted: int
    vessels_refreshed: int


def extract(
    config: Config,
    local_file: Optional[str] = None,
    url: Optional[str] = None,
    noaa_date: Optional[date] = None,
) -> str:
    """Return a local path to the raw file, downloading it if needed (cached)."""
    if local_file:
        if not os.path.isfile(local_file):
            raise FileNotFoundError(local_file)
        return local_file
    if noaa_date:
        url = noaa_daily_url(noaa_date)
    if not url:
        raise ValueError("Provide one of local_file, url or noaa_date")

    path = os.path.join(config.DATA_DIR, os.path.basename(url))
    if not AISDataLoader(config).download_ais_data(url, path):
        raise RuntimeError(f"Download failed: {url}")
    return path


def transform(config: Config, path: str, max_records: Optional[int] = None) -> pd.DataFrame:
    df = AISDataLoader(config).load_csv_data(path, nrows=max_records)
    if df is None:
        raise RuntimeError(f"Could not read {path}")
    return AISDataProcessor().clean_data(df)


def load(config: Config, cleaned: pd.DataFrame) -> LoadResult:
    db = DatabaseManager(config.database_url)
    db.create_tables()
    inserted = db.save_ais_data(cleaned)
    refreshed = db.refresh_vessel_metrics(cleaned["MMSI"].unique(), AISDataProcessor())
    db.refresh_views()
    return LoadResult(len(cleaned), inserted, refreshed)
