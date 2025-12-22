from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, stddev, skewness, min, max, count, expr

router = APIRouter(prefix="/statistics", tags=["statistics"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
DISTRIBUTION_TABLE_PATH = "hdfs:///ghcnd/agg_tables/distribution/"


@router.get("/")
def get_statistics(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    monthly_df = spark.read.parquet(MONTHLY_AGG_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    dist_df = spark.read.parquet(DISTRIBUTION_TABLE_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    # Values from MONTHLY_AGG
    # ====================================================================
    # Getting Statistical Indicators 
    stats_row = monthly_df.agg(
        avg("avg_tmin").alias("mean_tmin"),
        avg("avg_tmax").alias("mean_tmax"),
        avg("total_precip").alias("mean_precip"),
        avg("avg_snow_depth").alias("mean_snow"),
        stddev("avg_tmin").alias("std_tmin"),
        stddev("avg_tmax").alias("std_tmax"),
        stddev("total_precip").alias("std_precip"),
        stddev("avg_snow_depth").alias("std_snow"),
        skewness("avg_tmin").alias("skew_tmin"),
        skewness("avg_tmax").alias("skew_tmax"),
        skewness("total_precip").alias("skew_precip"),
        skewness("avg_snow_depth").alias("skew_snow"),
        min("avg_tmin").alias("min_tmin"),
        max("avg_tmin").alias("max_tmin"),
        min("avg_tmax").alias("min_tmax"),
        max("avg_tmax").alias("max_tmax"),
        min("total_precip").alias("min_precip"),
        max("total_precip").alias("max_precip"),
        min("avg_snow_depth").alias("min_snow"),
        max("avg_snow_depth").alias("max_snow")
    ).first()


    # Values from DISTRIBUTION_TABLE
    # ====================================================================

    def get_distribution(df, column):
        return [{"start": row["bin_min"], "end": row["bin_max"], "count": row["count"]} 
            for row in df.filter(col("variable") == column).collect()]

    max_temp_dist = get_distribution(dist_df, "max_temp")
    min_temp_dist = get_distribution(dist_df, "min_temp")
    precip_dist = get_distribution(dist_df, "precipitation")
    snow_depth_dist = get_distribution(dist_df, "snow_depth")

    # ====================================================================

    if stats_row:
        stat_indicators = {
            "mean": {
                "mean_temp_min": stats_row["mean_tmin"],
                "mean_temp_max": stats_row["mean_tmax"],
                "mean_precip": stats_row["mean_precip"],
                "mean_snow": stats_row["mean_snow"]
            },
            "std": {
                "std_temp_min": stats_row["std_tmin"],
                "std_temp_max": stats_row["std_tmax"],
                "std_precip": stats_row["std_precip"],
                "std_snow": stats_row["std_snow"]
            },
            "skewness": {
                "skew_temp_min": stats_row["skew_tmin"],
                "skew_temp_max": stats_row["skew_tmax"],
                "skew_precip": stats_row["skew_precip"],
                "skew_snow": stats_row["skew_snow"]
            },
            "range": {
                "range_temp_min": {"start": stats_row["min_tmin"], "end": stats_row["max_tmin"]},
                "range_temp_max": {"start": stats_row["min_tmax"], "end": stats_row["max_tmax"]},
                "range_precip": {"start": stats_row["min_precip"], "end": stats_row["max_precip"]},
                "range_snow": {"start": stats_row["min_snow"], "end": stats_row["max_snow"]}
            }, 
        }
    else:
        stat_indicators = None

    return {
        "stat_indicators": stat_indicators,
        "data_points": {
            "max_temp": max_temp_dist,
            "min_temp": min_temp_dist,
            "precipitation": precip_dist,
            "snow_depth": snow_depth_dist
        }
    }
