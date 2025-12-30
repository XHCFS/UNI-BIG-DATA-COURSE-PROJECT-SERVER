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

## Data and HDFS Layout

### Source Data

1. Download the dataset from Kaggle:
   - `https://www.kaggle.com/datasets/noaa/noaa-global-historical-climatology-network-daily/data`
2. Unzip all files locally.
3. Identify:
   - Daily observation files (GHCND daily data).
   - Station metadata file (e.g. `ghcnd-stations.csv`).

### HDFS Structure

Upload data into HDFS with at least the following layout:

- Raw data:
  - `/ghcnd/*.csv` – daily records.
  - `/ghcnd/stations/ghcnd-stations.csv` – station metadata.
- Aggregated Parquet tables (created by Spark jobs in `scripts/agg_tables/`):
  - `/ghcnd/agg_tables/monthly_aggregates/`
  - `/ghcnd/agg_tables/yearly_aggregates/`
  - `/ghcnd/agg_tables/coverage/`
  - `/ghcnd/agg_tables/distribution/`
  - `/ghcnd/agg_tables/extreme_events/`
  - `/ghcnd/agg_tables/extreme_events_yearly_aggregates/`
  - `/ghcnd/agg_tables/extreme_events_yearly_summary/`
  - `/ghcnd/agg_tables/recent_extreme_events/`

---

## Prerequisites

- Linux server.
- **Java**: OpenJDK 17.
- **Hadoop**: HDFS configured with a default filesystem, e.g.:

  ```xml
  <!-- core-site.xml -->
  <configuration>
    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://localhost:9000</value>
    </property>
  </configuration>
  ```

- **Spark**: Apache Spark with PySpark available.
- **Python**: Version compatible with FastAPI and PySpark (e.g. 3.10+).
- **Python packages** (installed into a virtual environment or pyenv environment):
  - `fastapi`
  - `uvicorn`
  - `pyspark`
  - `pandas`
  - plus any other dependencies you use.

---

## Environment Setup

### 1. Install Java 17

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
java -version
```

### 2. Install and Start Hadoop (HDFS)

Install Hadoop, configure `fs.defaultFS` as above, then:

```bash
hdfs namenode -format     # first time only
start-dfs.sh              # or equivalent start script for your installation
hdfs dfs -ls /
```

### 3. Install Spark and PySpark

Install Spark binaries and PySpark Python bindings. Example (PySpark via pip):

```bash
pip install pyspark
```

Confirm Spark can access HDFS:

```bash
pyspark
>>> spark.read.text("hdfs:///").show()
```

### 4. Upload NOAA Data into HDFS

Assuming the unzipped GHCND data is under `/data/ghcnd`:

```bash
hdfs dfs -mkdir -p /ghcnd
hdfs dfs -mkdir -p /ghcnd/stations

# Adjust file patterns/names to your local layout
hdfs dfs -put /data/ghcnd/daily/*.csv /ghcnd/
hdfs dfs -put /data/ghcnd/ghcnd-stations.csv /ghcnd/stations/
```

Verify:

```bash
hdfs dfs -ls /ghcnd
hdfs dfs -ls /ghcnd/stations
```

### 5. Python Environment

Use `pyenv` or `venv`. Example with `pyenv`:

```bash
cd /root/UNI-BIG-DATA-COURSE-PROJECT-SERVER

pyenv install 3.13.0
pyenv virtualenv 3.13.0 ghcnd-api
pyenv local ghcnd-api

pip install fastapi uvicorn pyspark pandas
```

Adjust versions as needed.

### 6. Build Aggregated Tables

From the project root:

```bash
cd /root/UNI-BIG-DATA-COURSE-PROJECT-SERVER

spark-submit scripts/agg_tables/agg_table_monthly_aggregates.py
spark-submit scripts/agg_tables/agg_table_yearly_aggregates.py
spark-submit scripts/agg_tables/agg_table_coverage.py
spark-submit scripts/agg_tables/agg_table_distribution.py
spark-submit scripts/agg_tables/agg_table_extreme_events.py
spark-submit scripts/agg_tables/agg_table_yearly_extreme_events.py
spark-submit scripts/agg_tables/agg_table_extreme_events_yearly_summary.py
spark-submit scripts/agg_tables/agg_table_recent_extreme_events.py
```

Confirm:

```bash
hdfs dfs -ls /ghcnd/agg_tables/
```

You should see `monthly_aggregates`, `yearly_aggregates`, `coverage`, `distribution`, `extreme_events`, `extreme_events_yearly_aggregates`, `extreme_events_yearly_summary`, `recent_extreme_events`.

### 7. Spark Session Configuration

`app/spark.py` configures the shared SparkSession:

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

If your HDFS namenode is different, update `HDFS_NAMENODE`.

---

## Running the API (Manual)

From the project root, with the Python environment active:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open:

- Swagger UI: `http://<server>:8000/docs`
- ReDoc: `http://<server>:8000/redoc`

---

## Running the API via systemd

A `systemd` unit can be used to run the API as a service.

### Unit File

Create `/etc/systemd/system/ghcnd-api.service`:

```ini
[Unit]
Description=GHCND FastAPI + Spark API
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/UNI-BIG-DATA-COURSE-PROJECT-SERVER

# Load pyenv environment and start Uvicorn
ExecStart=/bin/bash -c 'source /opt/pyenv/bin/activate && uvicorn app.main:app --host 0.0.0.0 --port 8000'

Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Adjust `User`, `WorkingDirectory`, and the `source` path to match your environment.

### Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable ghcnd-api
sudo systemctl start ghcnd-api
sudo systemctl status ghcnd-api
```

Tail logs:

```bash
journalctl -u ghcnd-api -f
```

---

## API Documentation

All endpoints are RESTful and return JSON. Query parameters are required unless otherwise noted. All temperature values are in Celsius, precipitation in millimeters, and snow depth in centimeters.

### Base URL

```
http://<server>:8000
```

### Test Endpoint

**GET** `/test`

Basic connectivity check. No parameters required.

**Response:**

```json
"HI :>"
```

---

### Overview

**GET** `/overview/`

Returns a comprehensive climate overview for a country over a specified time period, including average temperatures, precipitation, snow depth, extreme event records, and data coverage metrics.

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix (e.g., `US`, `FR`, `CA`)
- `start_year` (integer, required): Start year of the analysis period
- `end_year` (integer, required): End year of the analysis period (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/overview/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "temperature": {
    "avg_min_temp": 12.5,
    "avg_max_temp": 25.3
  },
  "total_precipitation": 860.2,
  "avg_snow_depth": 15.7,
  "extreme_events": {
    "coldest_day": {
      "temperature": -15.0,
      "date": "2001-12-20"
    },
    "hottest_day": {
      "temperature": 42.5,
      "date": "2016-08-02"
    },
    "heaviest_day": {
      "amount": 250.0,
      "date": "2010-04-08"
    },
    "largest_snowfall": {
      "depth": 89.0,
      "date": "1856-01-25"
    }
  },
  "extreme_events_count": 51,
  "coverage": 90.5
}
```

**Field Descriptions:**

- `temperature.avg_min_temp`: Average minimum temperature across all stations and years (Celsius)
- `temperature.avg_max_temp`: Average maximum temperature across all stations and years (Celsius)
- `total_precipitation`: Total precipitation accumulated over the period (millimeters)
- `avg_snow_depth`: Average snow depth across all stations and years (centimeters)
- `extreme_events.coldest_day`: Record coldest day with date (may be `null` if no data)
- `extreme_events.hottest_day`: Record hottest day with date (may be `null` if no data)
- `extreme_events.heaviest_day`: Record heaviest single-day precipitation with date (may be `null`)
- `extreme_events.largest_snowfall`: Record largest single-day snowfall depth with date (may be `null`)
- `extreme_events_count`: Total count of extreme events (heatwaves, coldwaves, heavy precipitation, heavy snowfall)
- `coverage`: Average data coverage percentage (100 - missing_percentage)

---

### Map

**GET** `/map/`

Returns per-station climate statistics and geographic coordinates for mapping visualization. Each station includes aggregated climate metrics and location data.

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/map/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "stations": [
    {
      "station_id": "USC00123456",
      "name": "STATION NAME",
      "latitude": 40.7128,
      "longitude": -74.0060,
      "elevation": 10.5,
      "avg_tmin": 8.5,
      "avg_tmax": 20.3,
      "avg_temp": 14.4,
      "total_precip": 1200.5,
      "avg_snow_depth": 5.2
    }
  ]
}
```

**Field Descriptions:**

- `stations`: Array of station objects
- `station_id`: Unique station identifier
- `name`: Station name
- `latitude`: Station latitude (decimal degrees)
- `longitude`: Station longitude (decimal degrees)
- `elevation`: Station elevation above sea level (meters)
- `avg_tmin`: Average minimum temperature for this station over the period (Celsius)
- `avg_tmax`: Average maximum temperature for this station over the period (Celsius)
- `avg_temp`: Calculated average temperature (avg_tmin + avg_tmax) / 2 (Celsius)
- `total_precip`: Total precipitation for this station over the period (millimeters)
- `avg_snow_depth`: Average snow depth for this station over the period (centimeters)

---

### Trends

**GET** `/trends/`

Returns time series data for temperature, precipitation, and snow depth. The aggregation granularity (monthly or yearly) is automatically determined based on the period length. Periods longer than approximately 16 years use yearly aggregation; shorter periods use monthly aggregation.

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/trends/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "temperature_points": [
    {
      "timestamp": "2000-01",
      "avg_tmin": 5.2,
      "avg_tmax": 15.8
    },
    {
      "timestamp": "2000-02",
      "avg_tmin": 6.1,
      "avg_tmax": 16.5
    }
  ],
  "precip_points": [
    {
      "timestamp": "2000-01",
      "total_precip": 85.3
    },
    {
      "timestamp": "2000-02",
      "total_precip": 92.1
    }
  ],
  "snow_points": [
    {
      "timestamp": "2000-01",
      "avg_snow_depth": 12.5
    },
    {
      "timestamp": "2000-02",
      "avg_snow_depth": 8.3
    }
  ]
}
```

**Field Descriptions:**

- `temperature_points`: Array of temperature data points
  - `timestamp`: Time period identifier (format: `YYYY-MM` for monthly, `YYYY` for yearly)
  - `avg_tmin`: Average minimum temperature for the period (Celsius)
  - `avg_tmax`: Average maximum temperature for the period (Celsius)
- `precip_points`: Array of precipitation data points
  - `timestamp`: Time period identifier
  - `total_precip`: Total precipitation for the period (millimeters)
- `snow_points`: Array of snow depth data points
  - `timestamp`: Time period identifier
  - `avg_snow_depth`: Average snow depth for the period (centimeters)

---

### Seasons

**GET** `/seasons/`

Returns seasonal and monthly climate statistics. Seasons are defined as: Winter (Dec, Jan, Feb), Spring (Mar, Apr, May), Summer (Jun, Jul, Aug), Autumn (Sep, Oct, Nov).

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/seasons/?country_prefix=FR&start_year=2000&end_year=2019"
```

**Response Structure:**

```json
{
  "seasonal_summary": {
    "winter": {
      "avg_temp": 5.2,
      "temp_range": {
        "min": -3.0,
        "max": 15.0
      },
      "total_precip": 300.5,
      "avg_snow_depth": 36.2
    },
    "spring": {
      "avg_temp": 15.8,
      "temp_range": {
        "min": 10.0,
        "max": 22.0
      },
      "total_precip": 250.3,
      "avg_snow_depth": 0.0
    },
    "summer": {
      "avg_temp": 25.5,
      "temp_range": {
        "min": 20.0,
        "max": 32.0
      },
      "total_precip": 150.2,
      "avg_snow_depth": 0.0
    },
    "autumn": {
      "avg_temp": 12.3,
      "temp_range": {
        "min": 8.0,
        "max": 18.0
      },
      "total_precip": 280.7,
      "avg_snow_depth": 2.5
    }
  },
  "monthly_data": [
    {
      "month": 1,
      "avg_tmin": -2.5,
      "avg_tmax": 8.5,
      "avg_temp": 3.0,
      "total_precip": 95.2,
      "avg_snow_depth": 45.0,
      "season": "Winter"
    },
    {
      "month": 2,
      "avg_tmin": -1.0,
      "avg_tmax": 10.0,
      "avg_temp": 4.5,
      "total_precip": 88.3,
      "avg_snow_depth": 30.5,
      "season": "Winter"
    }
  ]
}
```

**Field Descriptions:**

- `seasonal_summary`: Object with keys `winter`, `spring`, `summer`, `autumn`
  - `avg_temp`: Average temperature for the season (Celsius)
  - `temp_range.min`: Minimum temperature observed during the season (Celsius)
  - `temp_range.max`: Maximum temperature observed during the season (Celsius)
  - `total_precip`: Total precipitation for the season (millimeters)
  - `avg_snow_depth`: Average snow depth for the season (centimeters)
- `monthly_data`: Array of monthly statistics
  - `month`: Month number (1-12)
  - `avg_tmin`, `avg_tmax`, `avg_temp`: Temperature metrics (Celsius)
  - `total_precip`: Total precipitation for the month (millimeters)
  - `avg_snow_depth`: Average snow depth for the month (centimeters)
  - `season`: Season label ("Winter", "Spring", "Summer", "Autumn")

---

### Coverage

**GET** `/coverage/`

Returns data coverage and missingness metrics, including summary statistics, per-station coverage, and per-year missing data breakdowns by variable type.

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/coverage/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "coverage_summary": {
    "total_missing_days": 250,
    "missing_percentage": 10.5,
    "stations_count": 150
  },
  "coverage_per_station": [
    {
      "station_id": "USC00123456",
      "coverage_percentage": 88.5,
      "missing_days": 360,
      "start_year": 1865,
      "end_year": 2001
    }
  ],
  "coverage_per_year": [
    {
      "year": 2000,
      "missing_temp": 60,
      "missing_precip": 20,
      "missing_snow": 150
    },
    {
      "year": 2001,
      "missing_temp": 55,
      "missing_precip": 25,
      "missing_snow": 145
    }
  ]
}
```

**Field Descriptions:**

- `coverage_summary`: Aggregate coverage statistics
  - `total_missing_days`: Total number of missing observation days across all variables
  - `missing_percentage`: Average missing data percentage across all stations
  - `stations_count`: Number of unique stations in the dataset
- `coverage_per_station`: Array of per-station coverage metrics
  - `station_id`: Station identifier
  - `coverage_percentage`: Data coverage percentage for this station (100 - missing_percentage)
  - `missing_days`: Total missing days for this station across all variables
  - `start_year`: First year of data for this station
  - `end_year`: Last year of data for this station
- `coverage_per_year`: Array of per-year missing data counts
  - `year`: Year
  - `missing_temp`: Missing days for temperature (min + max combined)
  - `missing_precip`: Missing days for precipitation
  - `missing_snow`: Missing days for snow depth

---

### Statistics

**GET** `/statistics/`

Returns statistical indicators (mean, standard deviation, skewness, ranges) and distribution histograms for temperature, precipitation, and snow depth.

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/statistics/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "stat_indicators": {
    "mean": {
      "mean_temp_min": 8.5,
      "mean_temp_max": 20.3,
      "mean_precip": 95.2,
      "mean_snow": 12.5
    },
    "std": {
      "std_temp_min": 5.2,
      "std_temp_max": 6.8,
      "std_precip": 45.3,
      "std_snow": 8.7
    },
    "skewness": {
      "skew_temp_min": 0.3,
      "skew_temp_max": -0.2,
      "skew_precip": 1.5,
      "skew_snow": 2.1
    },
    "range": {
      "range_temp_min": {
        "start": -15.0,
        "end": 25.0
      },
      "range_temp_max": {
        "start": 5.0,
        "end": 45.0
      },
      "range_precip": {
        "start": 0.0,
        "end": 500.0
      },
      "range_snow": {
        "start": 0.0,
        "end": 150.0
      }
    }
  },
  "data_points": {
    "max_temp": [
      {
        "start": 0,
        "end": 10,
        "count": 50
      },
      {
        "start": 10,
        "end": 20,
        "count": 120
      }
    ],
    "min_temp": [
      {
        "start": -10,
        "end": 0,
        "count": 45
      },
      {
        "start": 0,
        "end": 10,
        "count": 130
      }
    ],
    "precipitation": [
      {
        "start": 0,
        "end": 50,
        "count": 200
      },
      {
        "start": 50,
        "end": 100,
        "count": 150
      }
    ],
    "snow_depth": [
      {
        "start": 0,
        "end": 10,
        "count": 300
      },
      {
        "start": 10,
        "end": 20,
        "count": 80
      }
    ]
  }
}
```

**Field Descriptions:**

- `stat_indicators`: Statistical measures
  - `mean`: Mean values for min temperature, max temperature, precipitation, snow depth
  - `std`: Standard deviations
  - `skewness`: Skewness coefficients (positive = right-skewed, negative = left-skewed)
  - `range`: Minimum and maximum observed values (`start` = min, `end` = max)
- `data_points`: Distribution histograms (binned counts)
  - `max_temp`, `min_temp`, `precipitation`, `snow_depth`: Arrays of bins
    - `start`: Lower bound of bin
    - `end`: Upper bound of bin
    - `count`: Number of observations in this bin

---

### Extreme Events

**GET** `/extreme_events/`

Returns extreme event statistics including total counts, yearly breakdowns, and the most recent occurrence for each event type (heatwaves, coldwaves, heavy precipitation, heavy snowfall).

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/extreme_events/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "heatwave": {
    "total_count": 60,
    "yearly_counts": [
      {
        "year": 2000,
        "count": 12
      },
      {
        "year": 2001,
        "count": 15
      }
    ],
    "most_recent": {
      "date": "2016-08-02",
      "value": 38.5
    }
  },
  "coldwave": {
    "total_count": 50,
    "yearly_counts": [
      {
        "year": 2000,
        "count": 8
      },
      {
        "year": 2001,
        "count": 10
      }
    ],
    "most_recent": {
      "date": "2001-12-20",
      "value": -10.0
    }
  },
  "heavy_precipitation": {
    "total_count": 20,
    "yearly_counts": [
      {
        "year": 2000,
        "count": 5
      },
      {
        "year": 2001,
        "count": 4
      }
    ],
    "most_recent": {
      "date": "2010-04-08",
      "value": 200.0
    }
  },
  "heavy_snowfall": {
    "total_count": 10,
    "yearly_counts": [
      {
        "year": 2000,
        "count": 2
      },
      {
        "year": 2001,
        "count": 3
      }
    ],
    "most_recent": {
      "date": "2011-01-25",
      "value": 20.0
    }
  }
}
```

**Field Descriptions:**

- `heatwave`, `coldwave`, `heavy_precipitation`, `heavy_snowfall`: Event type objects
  - `total_count`: Total number of events of this type in the period
  - `yearly_counts`: Array of per-year counts
    - `year`: Year
    - `count`: Number of events in this year
  - `most_recent`: Most recent occurrence (may be `null` if no events found)
    - `date`: Date of occurrence (ISO format string)
    - `value`: Event magnitude (temperature in Celsius for heat/cold waves, precipitation/snow in mm/cm)

---

### Comparisons

**GET** `/comparisons/`

Compares climate metrics between two countries over the same time period, including summary statistics, differences, and parallel time series.

**Query Parameters:**

- `country_A_prefix` (string, required): Two-letter country code for first country
- `country_B_prefix` (string, required): Two-letter country code for second country
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/comparisons/?country_A_prefix=US&country_B_prefix=FR&start_year=2000&end_year=2004"
```

**Response Structure:**

```json
{
  "country_A_summary": {
    "avg_temp": 15.2,
    "temp_range": {
      "start": 5.0,
      "end": 30.0
    },
    "total_precip": 500.5,
    "extreme_events": 40
  },
  "country_B_summary": {
    "avg_temp": 12.8,
    "temp_range": {
      "start": 0.0,
      "end": 28.0
    },
    "total_precip": 600.3,
    "extreme_events": 35
  },
  "difference": {
    "avg_temp": 2.4,
    "total_precip": -99.8,
    "extreme_events": 5
  },
  "data_point": {
    "max_temp_A": [
      {
        "timestamp": "2000-01",
        "max_temp": 25.0
      }
    ],
    "max_temp_B": [
      {
        "timestamp": "2000-01",
        "max_temp": 22.0
      }
    ],
    "min_temp_A": [
      {
        "timestamp": "2000-01",
        "min_temp": 5.0
      }
    ],
    "min_temp_B": [
      {
        "timestamp": "2000-01",
        "min_temp": 2.0
      }
    ],
    "total_precip_A": [
      {
        "timestamp": "2000-01",
        "total_precip": 85.0
      }
    ],
    "total_precip_B": [
      {
        "timestamp": "2000-01",
        "total_precip": 95.0
      }
    ]
  }
}
```

**Field Descriptions:**

- `country_A_summary`, `country_B_summary`: Climate summaries for each country
  - `avg_temp`: Average temperature (Celsius)
  - `temp_range.start`: Minimum temperature observed (Celsius)
  - `temp_range.end`: Maximum temperature observed (Celsius)
  - `total_precip`: Total precipitation (millimeters)
  - `extreme_events`: Total count of extreme events
- `difference`: Differences between country A and country B (A - B)
  - `avg_temp`: Temperature difference (Celsius)
  - `total_precip`: Precipitation difference (millimeters)
  - `extreme_events`: Extreme events count difference
- `data_point`: Parallel time series for both countries
  - `max_temp_A`, `max_temp_B`: Maximum temperature series
  - `min_temp_A`, `min_temp_B`: Minimum temperature series
  - `total_precip_A`, `total_precip_B`: Precipitation series
  - Each series contains objects with `timestamp` (format depends on period length) and the corresponding metric value

---

## Operational Notes

- If an aggregated HDFS table is created with the wrong schema (e.g., wrong script or path), delete and regenerate it:

  ```bash
  hdfs dfs -rm -r /ghcnd/agg_tables/yearly_aggregates
  spark-submit scripts/agg_tables/agg_table_yearly_aggregates.py
  ```

- If the service encounters Spark temporary directory issues (e.g. `/tmp` cleaned), restart the service:

  ```bash
  sudo systemctl restart ghcnd-api
  ```

- Some responses can contain `null` values when no data is found for the requested filters. Client code must handle these cases explicitly.

---

## Acknowledgements

- NOAA for providing the GHCND dataset.
- Kaggle for hosting the dataset.
- Apache Hadoop and Apache Spark for the data processing stack.
- FastAPI and Uvicorn for the API framework and ASGI server.
