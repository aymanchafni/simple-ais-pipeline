# AIS Maritime Data Pipeline

![CI](https://github.com/aymanchafni/simple-ais-pipeline/actions/workflows/ci.yml/badge.svg)

A batch data pipeline that ingests ship-tracking (AIS) data, cleans it, loads it
**idempotently** into PostgreSQL, computes per-vessel metrics, and serves them through a
REST API and a Streamlit dashboard. It's orchestrated with Airflow and runs with one
`docker compose` command.

Originally built as a port-traffic monitoring prototype for **Tanger Med**. The public
dataset used for the demo is NOAA / MarineCadastre's archive of US coastal AIS data,
which uses the same AIS message format.

![Dashboard](docs/dashboard.png)

## Architecture

```mermaid
flowchart LR
    A[NOAA daily AIS file<br/>~7M positions/day] -->|extract: cached, atomic download| B[Clean & validate<br/>pandas, vectorised]
    B -->|COPY → staging → INSERT … ON CONFLICT| C[(PostgreSQL<br/>ais_data)]
    C -->|recompute touched vessels| D[(vessel_metrics)]
    C --> E[(mv_daily_traffic_stats)]
    D --> F[FastAPI]
    F --> G[Streamlit dashboard]
    H[Airflow DAG<br/>one run per day] -.orchestrates.-> A
```

| Layer          | Tech                                   | Where                              |
|----------------|----------------------------------------|------------------------------------|
| Orchestration  | Airflow 2.9 (TaskFlow API)             | `airflow/dags/ais_pipeline_dag.py` |
| Ingestion      | requests, pandas                       | `src/ingestion/`                   |
| Transformation | pandas / NumPy (vectorised)            | `src/transformation/`              |
| Storage        | PostgreSQL 15, `COPY` + upserts        | `src/storage/` (`schema.sql`)      |
| Serving        | FastAPI, Streamlit                     | `src/api/`, `dashboard/`           |
| Quality        | pytest, ruff, GitHub Actions           | `tests/`, `.github/workflows/`     |

## Quick start

```bash
docker compose up -d                 # Postgres + pipeline on the bundled sample + API
open http://localhost:8000/docs      # interactive API docs

docker compose --profile full up -d  # + dashboard (http://localhost:8501) + Airflow (http://localhost:8080, admin/admin)
```

Load a real day of data (about a 320 MB download, cached in a Docker volume):

```bash
PIPELINE_ARGS="--noaa-date 2023-01-01 --max-records 200000" docker compose run --rm data-init
```

> The bundled `sample_data/sample_ais.csv` is synthetic (15 positions near Tanger Med).
> Run `docker compose down -v` before loading real data if you don't want the two mixed.

## Design decisions

**Idempotent loads.** Positions have a natural key `(mmsi, base_datetime)`. Each batch is
`COPY`-ed into a temporary staging table and merged with `INSERT … ON CONFLICT DO NOTHING`.
Re-running a day, retrying a failed Airflow task or backfilling inserts nothing twice.
On 200,000 real positions, a re-run inserts 0 rows and finishes in about 13 s.

**Metrics stay correct as data arrives day by day.** After each load, metrics are
recomputed from the *full history* of every vessel touched by the batch (in chunks of 500
vessels to bound memory) and upserted.

**Vectorised transforms.** Distances (haversine) and time attribution are computed with
grouped `shift()` operations instead of Python loops over rows. 200,000 real positions
(12,600 vessels) are cleaned, loaded and aggregated in about 13 s on a laptop.

**Explicit data-quality rules** (`src/transformation/data_processor.py`, all unit-tested):

| Rule | Why |
|------|-----|
| MMSI must be 9 digits; lat/lon in range; `(0, 0)` dropped | invalid or default GPS fixes |
| SOG must be within 0–50 kn | 102.3 = "not available" in AIS; higher values are noise |
| Heading 511 / out-of-range COG → `NULL` | AIS "not available" sentinels |
| Segments with implied speed > 60 kn excluded from distance | GPS jumps |
| Gaps > 2 h: no time or distance attributed | vessel out of receiver coverage |
| "At dock" = stationary **and** navigational status *Moored* (5) | a ship at anchor isn't docked |

**Safe API.** All queries use bound parameters, static routes are declared before
parameterised ones, and handlers are synchronous so FastAPI runs blocking DB calls in its
thread pool.

## Airflow

The DAG replays NOAA history: each run processes the file for its logical date,
`extract → transform → load → quality_check`.

```bash
# backfill a week (safe to re-run thanks to idempotent loads)
docker compose exec airflow-scheduler airflow dags backfill ais_data_pipeline -s 2023-01-01 -e 2023-01-07

# run once on the sample file
docker compose exec airflow-scheduler airflow dags test ais_data_pipeline 2023-01-01 \
  -c '{"local_file": "sample_data/sample_ais.csv"}'
```

Params: `max_records` (rows read per day, default 200,000) and `local_file`.

## API

| Endpoint | Description |
|----------|-------------|
| `GET /vessels?limit=&offset=` | Vessels ranked by distance travelled |
| `GET /vessels/search?name=&vessel_type=&min_distance=&max_distance=` | Filtered search |
| `GET /vessels/{mmsi}` | Metrics + last 100 positions of one vessel |
| `GET /statistics` | Fleet-level aggregates and top 5 vessels |
| `GET /metrics/time-analysis` | Moving vs. at-dock time |
| `GET /metrics/quality` | Completeness of optional AIS fields |

## Running without Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env                       # point it at a PostgreSQL instance
python scripts/run_pipeline.py --local-file sample_data/sample_ais.csv
uvicorn src.api.main:app --reload
```

## Tests

```bash
pytest                                                    # unit tests (no DB needed)
DATABASE_URL=postgresql://postgres:password@localhost:5432/tanger_med pytest -m integration
```

CI runs lint, unit and integration tests against a Postgres service. It also runs the
pipeline twice to check idempotency and checks that the Airflow DAG parses.

## Possible next steps

- Streaming ingestion from a live AIS feed (e.g. aisstream.io) through Kafka/Redpanda
- Port geofences (PostGIS) to measure berth occupancy and waiting time at anchor
- Partition `ais_data` by day once it grows beyond a few hundred million rows
