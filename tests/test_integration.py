"""End-to-end load against a real PostgreSQL. Run with:

    DATABASE_URL=postgresql://postgres:password@localhost:5432/tanger_med pytest -m integration
"""

import os

import pytest
from sqlalchemy import create_engine, text

from src import pipeline
from src.config import Config

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set"),
]


def counts(url):
    with create_engine(url).connect() as conn:
        return (
            conn.execute(text("SELECT COUNT(*) FROM ais_data")).scalar(),
            conn.execute(text("SELECT COUNT(*) FROM vessel_metrics")).scalar(),
        )


def test_load_is_idempotent():
    config = Config()
    cleaned = pipeline.transform(config, "sample_data/sample_ais.csv")

    pipeline.load(config, cleaned)
    first = counts(config.database_url)

    second_run = pipeline.load(config, cleaned)   # same input again
    assert second_run.rows_inserted == 0
    assert counts(config.database_url) == first
    assert first[0] >= len(cleaned)
