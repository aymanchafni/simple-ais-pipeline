#!/usr/bin/env python3
"""Run the AIS pipeline once: extract -> clean -> load (idempotent) -> refresh metrics.

Examples
--------
    # Bundled sample (14 positions near Tanger Med)
    python scripts/run_pipeline.py --local-file sample_data/sample_ais.csv

    # One day of real NOAA / MarineCadastre data (US coastal waters, ~320 MB download)
    python scripts/run_pipeline.py --noaa-date 2023-01-01 --max-records 200000

    # Any CSV/ZIP URL in the NOAA format
    python scripts/run_pipeline.py --url https://.../AIS_2023_01_02.zip

    # Just print the statistics report for what is already in the database
    python scripts/run_pipeline.py --stats-only
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import pipeline  # noqa: E402
from src.analytics.statistics import StatisticsGenerator  # noqa: E402
from src.config import Config  # noqa: E402

logger = logging.getLogger("run_pipeline")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = p.add_mutually_exclusive_group()
    source.add_argument("--local-file", help="Local AIS file (CSV or ZIP, NOAA format)")
    source.add_argument("--url", help="URL of an AIS file (CSV or ZIP)")
    source.add_argument("--noaa-date", type=date.fromisoformat, help="Day to load from NOAA, YYYY-MM-DD")
    p.add_argument("--max-records", type=int, help="Read at most N rows from the source file")
    p.add_argument("--stats-only", action="store_true", help="Only print the statistics report")
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args(argv)
    if not args.stats_only and not (args.local_file or args.url or args.noaa_date):
        p.error("one of --local-file, --url, --noaa-date or --stats-only is required")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    config = Config()

    if not args.stats_only:
        t0 = time.perf_counter()
        path = pipeline.extract(config, args.local_file, args.url, args.noaa_date)
        cleaned = pipeline.transform(config, path, args.max_records)
        result = pipeline.load(config, cleaned)
        logger.info(
            "Done in %.1fs - %s clean rows, %s new rows inserted, %s vessels refreshed",
            time.perf_counter() - t0, result.rows_received, result.rows_inserted,
            result.vessels_refreshed,
        )

    report = StatisticsGenerator(config).generate_comprehensive_report()
    print(json.dumps(report["time_analysis"], indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
