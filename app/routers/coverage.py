from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/coverage", tags=["coverage"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"
RECENT_EXTREME_EVENT_PATH = "hdfs:///ghcnd/agg_tables/recent_extreme_events/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_coverage_data(
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


    # Values from COVERAGE_TABLE
    # ====================================================================

    total_missing_days = 250
    missing_percentage = 10
    stations_count = 100

    coverage_per_station = []
    example = {
        "station_id": "ACW00011647",
        "coverage_percentage" : 88,
        "missing_days": 360,
        "start_year": 1865,
        "end_year": 2001,
    }
    coverage_per_station.append(example)

    coverage_per_year = []
    example = {
        "year" : 2000,
        "missing_temp": 60,     # sume min and max temperature
        "missing_precip": 20,
        "missing_snow": 150,
    }
    coverage_per_year.append(example)

    # ====================================================================

    return {
        "coverage_summary": {
            "total_missing_days": total_missing_days,
            "missing_percentage": missing_percentage,
            "stations_count": stations_count,
        },
        "coverage_per_station": coverage_per_station,
        "coverage_per_year": coverage_per_year,
    }
