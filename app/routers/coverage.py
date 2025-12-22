from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, sum , min, max

router = APIRouter(prefix="/coverage", tags=["coverage"])

COVERAGE_TABLE_PATH = "hdfs:///ghcnd/agg_tables/coverage/"


@router.get("/")
def get_coverage_data(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    coverage_df = spark.read.parquet(COVERAGE_TABLE_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    # Values from COVERAGE_TABLE
    # ====================================================================

    # Summary
    total_missing_result = coverage_df.select(
        (col("missing_tmin") + col("missing_tmax") + col("missing_precip") + col("missing_snow")).alias("total_missing")
    ).groupBy().sum("total_missing").first()
    total_missing_days = total_missing_result[0] if total_missing_result else None

    missing_pct_result = coverage_df.agg(avg("missing_percentage")).first()
    missing_percentage = missing_pct_result[0] if missing_pct_result else None

    stations_count = coverage_df.select("station_id").distinct().count()

    # coverage_per_station
    coverage_per_station = coverage_df.groupBy("station_id").agg(
        avg("missing_percentage").alias("missing_percentage"),
        sum(col("missing_tmin") + col("missing_tmax") + col("missing_precip") + col("missing_snow")).alias("missing_days"),
        min("year").alias("start_year"),
        max("year").alias("end_year")
    ).collect()

    coverage_per_station_list = [
        {
            "station_id": row["station_id"],
            "coverage_percentage": 100 - row["missing_percentage"],
            "missing_days": row["missing_days"],
            "start_year": row["start_year"],
            "end_year": row["end_year"]
        }
        for row in coverage_per_station
    ]

    #coverage_per_year
    coverage_per_year = coverage_df.groupBy("year").agg(
        (sum("missing_tmin") + sum("missing_tmax")).alias("missing_temp"),
        sum("missing_precip").alias("missing_precip"),
        sum("missing_snow").alias("missing_snow")
    ).orderBy("year").collect()

    coverage_per_year_list = [
        {
            "year": row["year"],
            "missing_temp": row["missing_temp"],
            "missing_precip": row["missing_precip"],
            "missing_snow": row["missing_snow"]
        }
        for row in coverage_per_year
    ]

    # ====================================================================

    return {
        "coverage_summary": {
            "total_missing_days": total_missing_days,
            "missing_percentage": missing_percentage,
            "stations_count": stations_count,
        },
        "coverage_per_station": coverage_per_station_list,
        "coverage_per_year": coverage_per_year_list
    }
