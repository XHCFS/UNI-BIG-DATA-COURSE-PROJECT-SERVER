from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, sum, concat_ws, lpad

router = APIRouter(prefix="/trends", tags=["trends"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"
YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"


@router.get("/trends")
def get_trends_data(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Period length determine aggregation level
    period_length = end_year - start_year + 1
    if period_length * 12 > 200:
        time_unit = "year"
    else:
        time_unit = "month"

    # Monthly
    # ====================================================================
    if time_unit == "month":
        df = spark.read.parquet(MONTHLY_AGG_PATH).filter(
            (col("station_id").startswith(country_prefix)) &
            (col("year") >= start_year) & (col("year") <= end_year)
        )

        # Create a timestamp like "YYYY-MM"
        df = df.withColumn("timestamp", concat_ws("-", col("year"), lpad(col("month"), 2, "0")))

    # Yearly
    # ====================================================================
    else:
        df = spark.read.parquet(YEARLY_AGG_PATH).filter(
            (col("station_id").startswith(country_prefix)) &
            (col("year") >= start_year) & (col("year") <= end_year)
        )

        df = df.withColumn("timestamp", col("year").cast("string"))


    agg_df = df.groupBy("timestamp").agg(
        avg("avg_tmin").alias("avg_tmin"),
        avg("avg_tmax").alias("avg_tmax"),
        sum("total_precip").alias("total_precip"),
        avg("avg_snow_depth").alias("avg_snow_depth")
    ).orderBy("timestamp")

    rows = agg_df.collect()

    # Temperature
    # ====================================================================
    temperature_points = [{"timestamp": row.timestamp, "avg_tmin": row.avg_tmin, "avg_tmax": row.avg_tmax} for row in rows]
    
    # Precipitation
    # ====================================================================
    precip_points = [{"timestamp": row.timestamp, "total_precip": row.total_precip} for row in rows]
    
    # Snow Depth
    # ====================================================================
    snow_points = [{"timestamp": row.timestamp, "avg_snow_depth": row.avg_snow_depth} for row in rows]
    

    # ====================================================================

    return {
        "data_points": {
            "temperature": temperature_points,
            "precipitation": precip_points,
            "snow_depth": snow_points,
        }
    }
