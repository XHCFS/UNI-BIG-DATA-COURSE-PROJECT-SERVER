from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/seasons", tags=["seasons"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/seasons")
def get_seasons_data(
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
    # NOTE: calculate the average values for each month in all years in the selected period

    # Values from MONTHLY_AGG 
    # ====================================================================

    # Seasons Summary
    winter = {
        "avg_temp": 5,
        "temp_range": { "min": -5, "max": 15 },
        "avg_precip": 80,
        "avg_snow_depth": 36
    }

    spring = {
        "avg_temp": 22,
        "temp_range": { "min": 18, "max": 24 },
        "avg_precip": 40,
        "avg_snow_depth": 0
    }

    autumn = {
        "avg_temp": 15,
        "temp_range": { "min": 11, "max": 20 },
        "avg_precip": 95,
        "avg_snow_depth": 3
    }

    summer = {
        "avg_temp": 27,
        "temp_range": { "min": 25, "max": 31 },
        "avg_precip": 2,
        "avg_snow_depth": 0
    }
    
    # Monthly data (for bar charts)
    monthly_data = []

    data_example = {
        "month": 1,
        "avg_tmin": -3,
        "avg_tmax": 5,
        "avg_temp": 1,
        "avg_precip": 300,
        "season": "Winter"
    }
    monthly_data.append(data_example)

    # ====================================================================

    return {
        "seasonal_summary": {
            "winter": winter,
            "spring": spring,
            "summer": summer,
            "autumn": autumn
        },
        "monthly_data": monthly_data
    }

