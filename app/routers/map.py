from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, sum

router = APIRouter(prefix="/map", tags=["map"])

YEARLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/yearly_aggregates/"
STATIONS_TABLE_PATH = "hdfs:///ghcnd/stations/"


@router.get("/")
def get_map_data(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    yearly_df = spark.read.parquet(YEARLY_AGG_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    stations_df = spark.read.parquet(STATIONS_TABLE_PATH).select(
        col("id").alias("station_id"),
        "name",
        "latitude",
        "longitude",
        "elevation",
    ).filter(col("station_id").startswith(country_prefix))

    # Values from YEARLY_AGG 
    # ====================================================================
    station_stat_df = yearly_df.groupBy("station_id").agg(
        avg("avg_tmin").alias("avg_tmin"),
        avg("avg_tmax").alias("avg_tmax"),
        sum("total_precip").alias("total_precip"),
        avg("avg_snow_depth").alias("avg_snow_depth")
    )

    joined_df = station_stat_df.join(stations_df, on="station_id", how="left")
    joined_df = joined_df.withColumn("avg_temp", (col("avg_tmin") + col("avg_tmax")) / 2)

    stations_data = [row.asDict() for row in joined_df.collect()]

    # ====================================================================

    return {"stations": stations_data}
