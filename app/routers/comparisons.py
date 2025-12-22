from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/comparisons", tags=["comparisons"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_comparisons_data(
    country_A_prefix: str = Query(...),
    country_B_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # TODO:
    # Load aggregated tables needed and filter by station_id and year
    # Use Spark to process and calculate the values need for dashboard page

    # ====================================================================
    # Using hardcoded values for now
    # ====================================================================
   
    #Country A Summary
    # Values from YEARLY_AGG 
    # ====================================================================
    # Temperature
    avg_temp_A = 15
    temp_range_A = {"start": 20, "end": 30}

    #precipitation
    total_precip_A = 500

    #Values from YEARLY_EXTREME_EVENTS 
    #=====================================================================
    #Extreme Event
    extreme_event_count_A = 40


    #Country B Summary
    # Values from YEARLY_AGG 
    # ====================================================================
    # Temperature
    avg_temp_B = 15
    temp_range_B = {"start": 20, "end": 30}

    #precipitation
    total_precip_B = 500

    #Values from YEARLY_EXTREME_EVENTS 
    #=====================================================================
    #Extreme Event
    extreme_event_count_B = 40


    #Difference Summary
    #Temperature
    avg_temp_diff = avg_temp_A - avg_temp_B

    #precipitation
    total_precip_diff = total_precip_A - total_precip_B

    #Extreme Events
    extreme_events_diff = extreme_event_count_A - extreme_event_count_B


    # Period length determine aggregation level
    period_length = end_year - start_year + 1
    if period_length * 12 > 200:
        time_unit = "year"
    else:
        time_unit = "month"

    # NOTE: calculate the average values across all stations in the selected country

    # *** Country A ***
    # Maximum Temperature
    # ====================================================================
    max_temp_points_A = []

    max_temp_point_A_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "avg_tmax": 40
    }
    max_temp_points_A.append(max_temp_point_A_example)

    # Minimum Temperature
    # ====================================================================
    min_temp_points_A = []

    min_temp_point_A_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "avg_tmin": 12
    }
    min_temp_points_A.append(min_temp_point_A_example)

    # Precipitation
    # ====================================================================
    precip_points_A = []

    precip_point_A_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "total_precip": 860,
    }
    precip_points_A.append(precip_point_A_example)


    # *** Country B ***
    # Maximum Temperature
    # ====================================================================
    max_temp_points_B = []

    max_temp_point_B_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "avg_tmax": 40
    }
    max_temp_points_B.append(max_temp_point_B_example)

    # Minimum Temperature
    # ====================================================================
    min_temp_points_B = []

    min_temp_point_B_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "avg_tmin": 12
    }
    min_temp_points_B.append(min_temp_point_B_example)

    # Precipitation
    # ====================================================================
    precip_points_B = []

    precip_point_B_example = {
        "timestamp": "2000-01",

    # Values from MONTHLY_AGG or YEARLY_AGG
        "total_precip": 860,
    }
    precip_points_B.append(precip_point_B_example)
    
    # ====================================================================

    return {
        "country_A_summary": {
            "avg_temp": avg_temp_A,
            "temp_range": temp_range_A,
            "total_precip": total_precip_A,
            "extreme_events": extreme_event_count_A
        },
        "country_B_summary": {
            "avg_temp": avg_temp_B,
            "temp_range": temp_range_B,
            "total_precip": total_precip_B,
            "extreme_events": extreme_event_count_B
        },
        "difference": {
            "avg_temp": avg_temp_diff,
            "total_precip": total_precip_diff,
            "extreme_events": extreme_events_diff
        },
        "data_point": {
            "max_temp_A": max_temp_points_A,
            "max_temp_B": max_temp_points_B,
            "min_temp_A": min_temp_points_A,
            "min_temp_B": min_temp_points_B,
            "total_precip_A": total_precip_A,
            "total_precip_B": total_precip_B
        }
    }
