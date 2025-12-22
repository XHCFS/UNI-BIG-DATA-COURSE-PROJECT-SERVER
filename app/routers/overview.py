from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, sum, row_number, substring, when
from pyspark.sql.window import Window

router = APIRouter(prefix="/overview", tags=["overview"])

YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"
YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
EXTREME_DAY_YEARLY_SUMMARY_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_summary/"
COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"


@router.get("/")
def get_overview(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    yearly_df = spark.read.parquet(YEARLY_AGG_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    yearly_extreme_df = spark.read.parquet(YEARLY_EXTREME_COUNTS_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    extreme_day_df = spark.read.parquet(EXTREME_DAY_YEARLY_SUMMARY_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    coverage_df = spark.read.parquet(COVERAGE_TABLE_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    # Values from YEARLY_AGG 
    # ====================================================================
    yearly_stats = yearly_df.agg(
        avg("avg_tmin").alias("avg_tmin"),
        avg("avg_tmax").alias("avg_tmax"),
        sum("total_precip").alias("total_precip"),
        avg("avg_snow_depth").alias("avg_snow_depth")
    ).first()


    # Temperature
    avg_min_temp = yearly_stats["avg_tmin"]
    avg_max_temp = yearly_stats["avg_tmax"]

    # Preciptation
    total_precip = yearly_stats["total_precip"]

    # Snow Depth
    avg_snow_depth = yearly_stats["avg_snow_depth"]

    # Extreme Events Count
    extreme_counts = (
        yearly_extreme_df.withColumn(
            "total_events",
            col("heatwave_count")
            + col("coldwave_count")
            + col("heavy_precip_count")
            + col("snowfall_count")
        )
        .agg(sum("total_events")).first()[0]
    )

    # Values from COVERAGE_TABLE 
    # ====================================================================
    # Data Coverage
    data_coverage = coverage_df.agg(avg("missing_percentage")).first()[0]


    # Values from EXTREME_DAY_YEARLY_SUMMARY
    # ====================================================================
    # Extreme Events

    extreme_day_df = extreme_day_df.withColumn(
        "sort_value",
        when(col("event_type") == "coldwave", -col("value")).otherwise(col("value"))
    )
    window = Window.partitionBy("event_type").orderBy(col("sort_value").desc())

    extreme_events_max = extreme_day_df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
    extreme_day_info = extreme_events_max.select("event_type", "value", "date")

    hottest_day = extreme_day_info.filter(col("event_type") == "heatwave").first()
    if hottest_day:
        hottest_day_value = hottest_day["value"]
        hottest_day_date = hottest_day["date"]
    else:
        hottest_day_value = None
        hottest_day_date = None

    coldest_day = extreme_day_info.filter(col("event_type") == "coldwave").first()
    if coldest_day:
        coldest_day_value = coldest_day["value"]
        coldest_day_date = coldest_day["date"]
    else:
        coldest_day_value = None
        coldest_day_date = None

    heaviest_precip = extreme_day_info.filter(col("event_type") == "heavy_precip").first()
    if heaviest_precip:
        heaviest_precip_value = heaviest_precip["value"]
        heaviest_precip_date = heaviest_precip["date"]
    else:
        heaviest_precip_value = None
        heaviest_precip_date = None

    largest_snow = extreme_day_info.filter(col("event_type") == "heavy_snow").first()
    if largest_snow:
        largest_snow_value = largest_snow["value"]
        largest_snow_date = largest_snow["date"]
    else:
        largest_snow_value = None
        largest_snow_date = None


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
                "amount": heaviest_precip_value,
                "date": heaviest_precip_date,
            },
            "largest_snowfall": {
                "depth": largest_snow_value,
                "date": largest_snow_date,
            },
        },
        "extreme_events_count": extreme_counts,
        "coverage": data_coverage,
    }
