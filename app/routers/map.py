from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/map", tags=["map"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_map_data(
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
    stations_data = []

    station_example = {
        "station_id": "ACW00011647",

    # Values from ghcnd_stations file
    # ====================================================================
        "name": "ST JOHNS",
        "latitude": 17.1333,
        "longitude": -61.7833,
        "elevation": 19.2,

    # Values from YEARLY_AGG
    # ====================================================================
        "avg_temp": 26,
        "avg_tmin": 12,
        "avg_tmax": 40,
        "total_precip": 860,
        "avg_snow_depth": 127,
    }

    stations_data.append(station_example)

    # ====================================================================

    return {"stations": stations_data}
