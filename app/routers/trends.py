from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/trends", tags=["trends"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/trends")
def get_trends_data(
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

    # Period length determine aggregation level
    period_length = end_year - start_year + 1
    if period_length * 12 > 200:
        time_unit = "year"
    else:
        time_unit = "month"

    # NOTE: calculate the average values across all stations in the selected country

    # Temperature
    # ====================================================================
    temperature_points = []

    temperature_point_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "avg_tmin": 12,
        "avg_tmax": 40,
    }
    temperature_points.append(temperature_point_example)

    # Precipitation
    # ====================================================================
    precip_points = []

    precip_point_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "total_precip": 860,
    }
    precip_points.append(precip_point_example)

    # Snow Depth
    # ====================================================================
    snow_points = []

    snow_point_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "avg_snow_depth": 30
    }
    snow_points.append(snow_point_example)


    # ====================================================================

    return {
        "data_points": {
            "temperature": temperature_points,
            "precipitation": precip_points,
            "snow_depth": snow_points,
        }
    }
