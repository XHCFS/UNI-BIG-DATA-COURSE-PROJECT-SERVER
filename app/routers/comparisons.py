from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, sum , min, max, concat_ws, lpad

router = APIRouter(prefix="/comparisons", tags=["comparisons"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"
YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"


@router.get("/")
def get_comparisons_data(
    country_A_prefix: str = Query(...),
    country_B_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    yearly_df = spark.read.parquet(YEARLY_AGG_PATH).filter(
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    extreme_df = spark.read.parquet(YEARLY_EXTREME_COUNTS_PATH).filter(
        (col("year") >= start_year) & (col("year") <= end_year)
    )


    def get_country_summary(df, extreme_df, country_prefix):
        df_country = df.filter(col("station_id").startswith(country_prefix))
        extreme_country = extreme_df.filter(col("station_id").startswith(country_prefix))
   
        # Values from YEARLY_AGG 
        # ====================================================================
        agg = df_country.agg(
            avg("avg_temp").alias("avg_temp"),
            min("min_temp").alias("min_temp"),
            max("max_temp").alias("max_temp"),
            sum("total_precip").alias("total_precip")
        ).collect()[0]

        #Values from YEARLY_EXTREME_EVENTS 
        #=====================================================================
        total_extremes = extreme_country.agg(
            (sum("heatwave_count") + 
             sum("coldwave_count") + 
             sum("heavy_precip_count") + 
             sum("snowfall_count")).alias("total_extremes")
        ).collect()[0]["total_extremes"]

        temp_range = {"start": agg["min_temp"], "end": agg["max_temp"]}

        return {
            "avg_temp": agg["avg_temp"],
            "temp_range": temp_range,
            "total_precip": agg["total_precip"],
            "extreme_events": total_extremes
        }

    summary_A = get_country_summary(yearly_df, extreme_df, country_A_prefix)
    summary_B = get_country_summary(yearly_df, extreme_df, country_B_prefix)

    # Difference
    # ====================================================================

    diff_temp = summary_A["avg_temp"] - summary_B["avg_temp"]
    diff_precip = summary_A["total_precip"] - summary_B["total_precip"]
    diff_extreme = summary_A["extreme_events"] - summary_B["extreme_events"]


    # Data Points
    # ====================================================================
    
    def get_country_data(country_prefix):
        period_length = end_year - start_year + 1
        if period_length * 12 > 200:
            # Use YEARLY_AGG
            df = yearly_df.filter(col("station_id").startswith(country_prefix))
            df = df.withColumn("timestamp", col("year").cast("string"))
        else:
            # Use MONTHLY_AGG
            df = spark.read.parquet(MONTHLY_AGG_PATH).filter(
                (col("station_id").startswith(country_prefix)) &
                (col("year") >= start_year) & (col("year") <= end_year)
            )
            df = df.withColumn("timestamp", concat_ws("-", col("year"), lpad(col("month"), 2, "0")))

        agg_df = df.groupBy("timestamp").agg(
            max("max_temp").alias("max_temp"),
            min("min_temp").alias("min_temp"),
            sum("total_precip").alias("total_precip")
        ).orderBy("timestamp")

        rows = agg_df.collect()

        max_temp_points = [{"timestamp": row.timestamp, "max_temp": row.max_temp} for row in rows]
        min_temp_points = [{"timestamp": row.timestamp, "min_temp": row.min_temp} for row in rows]
        total_precip_points = [{"timestamp": row.timestamp, "total_precip": row.total_precip} for row in rows]

        return max_temp_points, min_temp_points, total_precip_points

    max_temp_points_A, min_temp_points_A, total_precip_points_A = get_country_data(country_A_prefix)
    max_temp_points_B, min_temp_points_B, total_precip_points_B = get_country_data(country_B_prefix)

    # ====================================================================

    return {
        "country_A_summary": summary_A,
        "country_B_summary": summary_B,
        "difference": {
            "avg_temp": diff_temp,
            "total_precip": diff_precip,
            "extreme_events": diff_extreme
        },
        "data_point": {
            "max_temp_A": max_temp_points_A,
            "max_temp_B": max_temp_points_B,
            "min_temp_A": min_temp_points_A,
            "min_temp_B": min_temp_points_B,
            "total_precip_A": total_precip_points_A,
            "total_precip_B": total_precip_points_B
        }
    }
