from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/statistics", tags=["statistics"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_statistics(
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



    #============================================================
    # Values from MONTHLY_AGG
    #============================================================
    # Getting Statistical Indicators 
    mean_temp = 20
    mean_percip = 100
    mean_snow = 15
    median_temp = 20
    median_percip = 100
    median_snow = 15
    std_temp = 20
    std_percip = 100
    std_snow = 15
    skew_temp = 20
    skew_percip = 100
    skew_snow = 15
    range_temp = {"start": 5, "end": 10}
    range_percip = {"start": 5, "end": 10}
    range_snow = {"start": 5, "end": 10}


    # Values from Distribution Table
    #=================================================
    #Maximam Temp Graph
    max_temp = []

    max_temp_point_example = {
        "start": 10,
        "end": 20,
        "count": 50
    }

    max_temp.append(max_temp_point_example)

    #Minimum Temp Graph
    min_temp = []

    min_temp_point_example = {
        "start": 10,
        "end": 20,
        "count": 50
    }

    min_temp.append(min_temp_point_example)

    #Perciptation Graph
    percip = []

    percip_point_example = {
        "start": 10,
        "end": 20,
        "count": 50
    }

    percip.append(percip_point_example)

    #Snow Depth Graph
    snow_depth = []

    snow_depth_point_example = {
        "start": 10,
        "end": 20,
        "count": 50
    }

    snow_depth.append(snow_depth_point_example)

    
    # ====================================================================

    return {
        "stat_indicators": {
            "mean":{
                "mean_temp": mean_temp,
                "mean_percip": mean_percip,
                "mean_snow": mean_snow
            },

            "median":{
                "median_temp": median_temp,
                "median_percip": median_percip,
                "median_snow": median_snow
            },

            "std":{
                "std_temp": std_temp,
                "std_percip": std_percip,
                "std_snow": std_snow
            },

            "skewness":{
                "skew_temp": skew_temp,
                "skew_percip": skew_percip,
                "skew_snow": skew_snow
            },

            "range":{
              "range_temp": range_temp,
              "range_percip": range_percip,
              "range_snow": range_snow
            },

            "data_points": {
                "max_temp": max_temp,
                "min_temp": min_temp,
                "precipitation": percip,
                "snow_depth": snow_depth
            }
        }
    }
