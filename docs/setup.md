## Setup and Environment

This document describes how to prepare the environment, ingest the NOAA GHCND dataset, and build the aggregated tables used by the API.

### Dataset

The backend is built on top of the **NOAA Global Historical Climatology Network – Daily (GHCND)** dataset:

- Kaggle: `https://www.kaggle.com/datasets/noaa/noaa-global-historical-climatology-network-daily/data`

Download the dataset from Kaggle and unzip all files locally. You will need:

- Daily observation files (GHCND daily data)
- Station metadata file (for example `ghcnd-stations.csv`)

### HDFS Layout

Upload the data into HDFS with at least the following layout:

- Raw data:
  - `/ghcnd/*.csv` – daily records
  - `/ghcnd/stations/ghcnd-stations.csv` – station metadata
- Aggregated Parquet tables (created by Spark jobs):
  - `/ghcnd/agg_tables/monthly_aggregates/`
  - `/ghcnd/agg_tables/yearly_aggregates/`
  - `/ghcnd/agg_tables/coverage/`
  - `/ghcnd/agg_tables/distribution/`
  - `/ghcnd/agg_tables/extreme_events/`
  - `/ghcnd/agg_tables/extreme_events_yearly_aggregates/`
  - `/ghcnd/agg_tables/extreme_events_yearly_summary/`
  - `/ghcnd/agg_tables/recent_extreme_events/`

Example commands (adapt paths as needed):

```bash
hdfs dfs -mkdir -p /ghcnd
hdfs dfs -mkdir -p /ghcnd/stations

hdfs dfs -put /data/ghcnd/daily/*.csv /ghcnd/
hdfs dfs -put /data/ghcnd/ghcnd-stations.csv /ghcnd/stations/
```

### Prerequisites

- Linux server
- **Java**: OpenJDK 17
- **Hadoop**: HDFS configured with a default filesystem, for example:

```xml
<!-- core-site.xml -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

- **Spark**: Apache Spark with PySpark available
- **Python**: Version compatible with FastAPI and PySpark (for example 3.10+)
- Python packages (installed into a virtual environment or pyenv environment):
  - `fastapi`
  - `uvicorn`
  - `pyspark`
  - `pandas`

### Python Environment

Using `pyenv` as an example:

```bash
cd /root/UNI-BIG-DATA-COURSE-PROJECT-SERVER

pyenv install 3.13.0
pyenv virtualenv 3.13.0 ghcnd-api
pyenv local ghcnd-api

pip install fastapi uvicorn pyspark pandas
```

Adjust versions and package list as required.

### Building Aggregated Tables

From the project root:

```bash
cd /root/UNI-BIG-DATA-COURSE-PROJECT-SERVER

# Preferred: use the helper script to run Spark jobs with consistent settings
scripts/run_spark.sh scripts/agg_tables/agg_table_monthly_aggregates.py
scripts/run_spark.sh scripts/agg_tables/agg_table_yearly_aggregates.py
scripts/run_spark.sh scripts/agg_tables/agg_table_coverage.py
scripts/run_spark.sh scripts/agg_tables/agg_table_distribution.py
scripts/run_spark.sh scripts/agg_tables/agg_table_extreme_events.py
scripts/run_spark.sh scripts/agg_tables/agg_table_yearly_extreme_events.py
scripts/run_spark.sh scripts/agg_tables/agg_table_extreme_events_yearly_summary.py
scripts/run_spark.sh scripts/agg_tables/agg_table_recent_extreme_events.py
```

The `scripts/run_spark.sh` wrapper configures Spark with appropriate memory limits, temporary directories, and Hadoop configuration, and writes a log file for each job (for example `agg_table_yearly_aggregates_report.txt`).

Verify the resulting tables:

```bash
hdfs dfs -ls /ghcnd/agg_tables/
```

You should see all expected tables under `/ghcnd/agg_tables/`.

### Spark Session Configuration

The shared `SparkSession` is configured in `app/spark.py`:

```python
from pyspark.sql import SparkSession

HDFS_NAMENODE = "hdfs://localhost:9000"

spark = (
    SparkSession.builder
    .appName("GHCND_API")
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    .getOrCreate()
)
```

If your HDFS namenode is different, update `HDFS_NAMENODE` accordingly.


