# GHCND Big Data API Server

This repository implements a FastAPI + Spark backend over the **NOAA Global Historical Climatology Network – Daily (GHCND)** dataset. The system provides REST endpoints that serve precomputed climate aggregates for use by a frontend or other clients.

The source dataset used is:

- **NOAA GHCND Daily**
- Kaggle: `https://www.kaggle.com/datasets/noaa/noaa-global-historical-climatology-network-daily/data`

---

## Repository Overview

- `app/`
  - `main.py` – FastAPI application entrypoint; includes and mounts all routers.
  - `spark.py` – SparkSession configuration and HDFS default filesystem settings.
  - `routers/`
    - `overview.py` – Country-level climate overview and extreme summaries.
    - `map.py` – Per-station climate statistics and coordinates for map views.
    - `trends.py` – Temporal trends in temperature, precipitation, and snow.
    - `seasons.py` – Monthly and seasonal aggregates.
    - `statistics.py` – Statistical indicators and value distributions.
    - `coverage.py` – Data coverage and missingness.
    - `extreme_events.py` – Extreme event counts and most recent occurrences.
    - `comparisons.py` – Comparative metrics between two countries.
- `scripts/`
  - `agg_tables/`
    - `agg_table_monthly_aggregates.py` – Builds `/ghcnd/agg_tables/monthly_aggregates/`.
    - `agg_table_yearly_aggregates.py` – Builds `/ghcnd/agg_tables/yearly_aggregates/`.
    - `agg_table_coverage.py` – Builds `/ghcnd/agg_tables/coverage/`.
    - `agg_table_distribution.py` – Builds `/ghcnd/agg_tables/distribution/`.
    - `agg_table_extreme_events.py` – Detects daily extreme events and writes `/extreme_events/`.
    - `agg_table_yearly_extreme_events.py` – Yearly extreme event counts.
    - `agg_table_extreme_events_yearly_summary.py` – Yearly extreme event summaries.
    - `agg_table_recent_extreme_events.py` – Most recent extreme events by type.
    - `agg_table_template.py` – Template for additional aggregated tables.
  - `run_spark.sh` – Wrapper around `spark-submit` that standardises Spark memory, temp directories, Hadoop configuration, and logging. Recommended for running all `agg_tables` jobs.
  - `exploratory_data_analysis.sh` – Convenience script for exploratory Spark analysis over the GHCND data.
  - `spark_jobs.py` – Python module that can orchestrate or batch Spark jobs.
- Top-level helper scripts:
  - `deploy.sh` – Remote deployment helper for updating a server and restarting the `ghcnd-api` service.
  - `local_deploy.sh` – Local deployment helper for updating the current machine and restarting the `ghcnd-api` service.

The API assumes all aggregated tables under `/ghcnd/agg_tables/` already exist in HDFS as Parquet datasets.

---

## Documentation

Complete documentation is available:

- **Online**: [GitHub Pages Documentation](https://xhcfs.github.io/UNI-BIG-DATA-COURSE-PROJECT-SERVER/)
- **Local files** in the [`docs/`](docs/) directory:
  - [Setup and Environment](docs/setup.md) – Prerequisites, installation steps, HDFS configuration, Python environment setup, and building aggregated tables.
  - [Running the API](docs/running.md) – Manual execution instructions and systemd service configuration for production deployment.
  - [API Documentation](docs/api.md) – Complete reference for all REST endpoints including request parameters, response formats, and examples.
  - [Operations and Maintenance](docs/operations.md) – Operational notes, troubleshooting, and maintenance procedures for the API server.

---

## Example Dashboard (Sister Repository)

An example application that consumes this API is the climatology dashboard repository:

- [UNI-BIG-DATA-CIE427-COURSE-PROJECT](https://github.com/XHCFS/UNI-BIG-DATA-CIE427-COURSE-PROJECT) – A Streamlit-based climatology report dashboard optimized using Hadoop that uses these API endpoints as its data source.

---

## Quick Start

1. Install prerequisites (Java 17, Hadoop, Spark, Python 3.10+)
2. Download and upload the NOAA GHCND dataset to HDFS
3. Build aggregated tables using `scripts/run_spark.sh`
4. Run the API with `uvicorn app.main:app --host 0.0.0.0 --port 8000`

See the [Setup and Environment](docs/setup.md) guide for detailed instructions.

---

## Acknowledgements

- NOAA for providing the GHCND dataset.
- Kaggle for hosting the dataset.
- Apache Hadoop and Apache Spark for the data processing stack.
- FastAPI and Uvicorn for the API framework and ASGI server.
