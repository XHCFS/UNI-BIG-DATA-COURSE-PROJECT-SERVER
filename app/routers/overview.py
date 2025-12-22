from fastapi import APIRouter, Query
from app.spark import spark
from datetime import date

router = APIRouter(prefix="/overview", tags=["overview"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_overview(
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


    # Values from YEARLY_AGG 
    # ====================================================================
    # Temperature
    avg_min_temp = 12
    avg_max_temp = 40

    # Preciptation
    total_precip = 860

    # Snow Depth
    avg_snow_depth = 127

    # Extreme Events Count
    extreme_counts = 51

    # Values from COVERAGE_TABLE 
    # ====================================================================
    # Data Coverage
    data_coverage = 90

    # Values from EXTREME_DAY_YEARLY_SUMMARY
    # ====================================================================
    # Extreme Events
    coldest_day_value = 10
    coldest_day_date = date(2001, 12, 20).isoformat()
    hottest_day_value = 45
    hottest_day_date = date(2016, 8, 2).isoformat()
    heaviest_precip = 250
    heaviest_precip_date = date(2010, 4, 8).isoformat()
    largest_snow = 89
    largest_snow_date = date(1856, 1, 25).isoformat()

    # ====================================================================

    return {
        "temperature": {
            "avg_min_temp": avg_min_temp,
            "avg_max_temp": avg_max_temp,
        },
        "total_precipitation": total_precip,
        "avg_snow_depth": avg_snow_depth,

        "extreme_events": {
            "coldest_day": {
                "temperature": coldest_day_value,
                "date": coldest_day_date,
            },
            "hottest_day": {
                "temperature": hottest_day_value,
                "date": hottest_day_date,
            },
            "heaviest_day": {
                "amount": heaviest_precip,
                "date": heaviest_precip_date,
            },
            "largest_snowfall": {
                "depth": largest_snow,
                "date": largest_snow_date,
            },
        },
        "coverage": data_coverage
    }
