from fastapi import APIRouter, Query
from app.spark import spark
from datetime import date

router = APIRouter(prefix="/extreme_events", tags=["extreme_events"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"
RECENT_EXTREME_EVENT_PATH = "hdfs:///ghcnd/agg_tables/recent_extreme_events/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_extreme_events_data(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # TODO:
    # Load aggregated tables needed and filter by station_id and year
    # Use Spark to process and calculate the values need for dashboard page

    # ====================================================================
    # Using hardcoded values for now
    # ====================================================================

    # NOTE: calculate the average values across all stations in the selected country

    # Values from YEARLY_EXTREME_COUNTS 
    # ====================================================================

    heatwave_count = 60
    coldwave_count = 50
    heavy_precip_count = 20
    heavy_snow_count = 10

    heatwave_yearly_count = []
    example = {
        "year": 1865, 
        "count": 10 
    }
    heatwave_yearly_count.append(example)

    coldwave_yearly_count = []
    example = {
        "year": 1865, 
        "count": 6
    }
    coldwave_yearly_count.append(example)

    heavy_precip_yearly_count = []
    example = {
        "year": 1865, 
        "count": 4
    }
    heavy_precip_yearly_count.append(example)

    heavy_snow_yearly_count = []
    example = {
        "year": 1865, 
        "count": 2
    }
    heavy_snow_yearly_count.append(example)

    # Values from RECENT_EXTREME_EVENT
    # ====================================================================

    recent_heatwave_value = 38
    recent_heatwave_date = date(2016, 8, 2).isoformat()

    recent_coldwave_value = 10
    recent_coldwave_date = date(2001, 12, 20).isoformat()

    recent_heavy_precip_value = 200
    recent_heavy_precip_date = date(2010, 4, 8).isoformat()

    recent_heavy_snow_value = 20
    recent_heavy_snow_date = date(2011, 1, 25).isoformat()

    # ====================================================================

    return {
        "heatwave": {
            "total_count": heatwave_count,
            "yearly_counts": heatwave_yearly_count,
            "most_recent": {
                "date": recent_heatwave_date,
                "value": recent_heatwave_value,
            }
        },
        "coldwave": {
            "total_count": coldwave_count,
            "yearly_counts": coldwave_yearly_count,
            "most_recent": {
                "date": recent_coldwave_date,
                "value": recent_coldwave_value,
            }
        },
        "heavy_precipitation": {
            "total_count": heavy_precip_count,
            "yearly_counts": heavy_precip_yearly_count,
            "most_recent": {
                "date": recent_heavy_precip_date,
                "value": recent_heavy_precip_value,
            }
        },
        "heavy_snowfall": {
            "total_count": heavy_snow_count,
            "yearly_counts": heavy_snow_yearly_count,
            "most_recent": {
                "date": recent_heavy_snow_date,
                "value": recent_heavy_snow_value,
            }
        }
    }
