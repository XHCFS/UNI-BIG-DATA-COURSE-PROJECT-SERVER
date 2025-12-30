## API Documentation

All endpoints are RESTful and return JSON. Query parameters are required unless otherwise noted. All temperature values are in Celsius, precipitation in millimeters, and snow depth in centimeters.

Base URL:

```text
http://<server>:8000
```

---

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

Returns a climate overview for a country over a specified time period, including average temperatures, precipitation, snow depth, extreme event records, and coverage metrics.

**Query Parameters:**

- `country_prefix` (string, required): Two-letter country code prefix (for example `US`, `FR`, `CA`)
- `start_year` (integer, required): Start year
- `end_year` (integer, required): End year (inclusive)

**Example Request:**

```bash
curl "http://localhost:8000/overview/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure (example):**

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

**Field Summary:**

- `temperature.avg_min_temp`, `temperature.avg_max_temp`: Average min/max temperatures (Celsius)
- `total_precipitation`: Total precipitation over the period (millimeters)
- `avg_snow_depth`: Average snow depth (centimeters)
- `extreme_events.*`: Record events and dates (may be `null` if no data)
- `extreme_events_count`: Total number of extreme events in the period
- `coverage`: Average data coverage percentage

---

### Map

**GET** `/map/`

Per-station climate statistics and geographic coordinates for mapping.

**Query Parameters:**

- `country_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/map/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure (example):**

```json
{
  "stations": [
    {
      "station_id": "USC00123456",
      "name": "STATION NAME",
      "latitude": 40.7128,
      "longitude": -74.006,
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

---

### Trends

**GET** `/trends/`

Time series for temperature, precipitation, and snow depth, aggregated monthly or yearly depending on the period length.

**Query Parameters:**

- `country_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/trends/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure (example):**

```json
{
  "temperature_points": [
    {
      "timestamp": "2000-01",
      "avg_tmin": 5.2,
      "avg_tmax": 15.8
    }
  ],
  "precip_points": [
    {
      "timestamp": "2000-01",
      "total_precip": 85.3
    }
  ],
  "snow_points": [
    {
      "timestamp": "2000-01",
      "avg_snow_depth": 12.5
    }
  ]
}
```

`timestamp` is `YYYY-MM` for monthly aggregation and `YYYY` for yearly aggregation.

---

### Seasons

**GET** `/seasons/`

Seasonal and monthly climate statistics. Seasons:

- Winter: months 12, 1, 2
- Spring: 3, 4, 5
- Summer: 6, 7, 8
- Autumn: 9, 10, 11

**Query Parameters:**

- `country_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/seasons/?country_prefix=FR&start_year=2000&end_year=2019"
```

**Response Structure (example):**

```json
{
  "seasonal_summary": {
    "winter": {
      "avg_temp": 5.2,
      "temp_range": { "min": -3.0, "max": 15.0 },
      "total_precip": 300.5,
      "avg_snow_depth": 36.2
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
    }
  ]
}
```

---

### Coverage

**GET** `/coverage/`

Data coverage and missingness metrics.

**Query Parameters:**

- `country_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/coverage/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure (example):**

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
    }
  ]
}
```

---

### Statistics

**GET** `/statistics/`

Statistical indicators and distribution histograms for temperature, precipitation, and snow depth.

**Query Parameters:**

- `country_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/statistics/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure (example):**

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
      "range_temp_min": { "start": -15.0, "end": 25.0 },
      "range_temp_max": { "start": 5.0, "end": 45.0 },
      "range_precip": { "start": 0.0, "end": 500.0 },
      "range_snow": { "start": 0.0, "end": 150.0 }
    }
  },
  "data_points": {
    "max_temp": [
      { "start": 0, "end": 10, "count": 50 }
    ],
    "min_temp": [
      { "start": -10, "end": 0, "count": 45 }
    ],
    "precipitation": [
      { "start": 0, "end": 50, "count": 200 }
    ],
    "snow_depth": [
      { "start": 0, "end": 10, "count": 300 }
    ]
  }
}
```

---

### Extreme Events

**GET** `/extreme_events/`

Extreme event statistics (totals, yearly counts, and most recent events) for:

- Heatwaves
- Coldwaves
- Heavy precipitation
- Heavy snowfall

**Query Parameters:**

- `country_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/extreme_events/?country_prefix=US&start_year=2000&end_year=2004"
```

**Response Structure (example):**

```json
{
  "heatwave": {
    "total_count": 60,
    "yearly_counts": [
      { "year": 2000, "count": 12 }
    ],
    "most_recent": {
      "date": "2016-08-02",
      "value": 38.5
    }
  },
  "coldwave": {
    "total_count": 50,
    "yearly_counts": [
      { "year": 2000, "count": 8 }
    ],
    "most_recent": {
      "date": "2001-12-20",
      "value": -10.0
    }
  },
  "heavy_precipitation": {
    "total_count": 20,
    "yearly_counts": [
      { "year": 2000, "count": 5 }
    ],
    "most_recent": {
      "date": "2010-04-08",
      "value": 200.0
    }
  },
  "heavy_snowfall": {
    "total_count": 10,
    "yearly_counts": [
      { "year": 2000, "count": 2 }
    ],
    "most_recent": {
      "date": "2011-01-25",
      "value": 20.0
    }
  }
}
```

---

### Comparisons

**GET** `/comparisons/`

Compares climate metrics between two countries.

**Query Parameters:**

- `country_A_prefix` (string, required)
- `country_B_prefix` (string, required)
- `start_year` (integer, required)
- `end_year` (integer, required)

**Example Request:**

```bash
curl "http://localhost:8000/comparisons/?country_A_prefix=US&country_B_prefix=FR&start_year=2000&end_year=2004"
```

**Response Structure (example):**

```json
{
  "country_A_summary": {
    "avg_temp": 15.2,
    "temp_range": { "start": 5.0, "end": 30.0 },
    "total_precip": 500.5,
    "extreme_events": 40
  },
  "country_B_summary": {
    "avg_temp": 12.8,
    "temp_range": { "start": 0.0, "end": 28.0 },
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
      { "timestamp": "2000-01", "max_temp": 25.0 }
    ],
    "max_temp_B": [
      { "timestamp": "2000-01", "max_temp": 22.0 }
    ],
    "min_temp_A": [
      { "timestamp": "2000-01", "min_temp": 5.0 }
    ],
    "min_temp_B": [
      { "timestamp": "2000-01", "min_temp": 2.0 }
    ],
    "total_precip_A": [
      { "timestamp": "2000-01", "total_precip": 85.0 }
    ],
    "total_precip_B": [
      { "timestamp": "2000-01", "total_precip": 95.0 }
    ]
  }
}
```


