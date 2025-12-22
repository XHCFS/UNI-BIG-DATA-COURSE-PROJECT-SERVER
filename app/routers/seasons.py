from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, avg, sum

router = APIRouter(prefix="/seasons", tags=["seasons"])

MONTHLY_AGG_PATH = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"


@router.get("/seasons")
def get_seasons_data(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    df = spark.read.parquet(MONTHLY_AGG_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    # Values from MONTHLY_AGG 
    # ====================================================================

    monthly_agg_df = df.groupBy("month").agg(
        avg("avg_tmin").alias("avg_tmin"),
        avg("avg_tmax").alias("avg_tmax"),
        sum("total_precip").alias("total_precip"),
        avg("avg_snow_depth").alias("avg_snow_depth")
    ).orderBy("month")

    monthly_agg_df = monthly_agg_df.withColumn("avg_temp", (col("avg_tmin") + col("avg_tmax")) / 2)

    season_months = {
        "Winter": [12, 1, 2],
        "Spring": [3, 4, 5],
        "Summer": [6, 7, 8],
        "Autumn": [9, 10, 11]
    }

    rows = monthly_agg_df.collect()
    monthly_data = []
    for row in rows:
        if row.month in season_months["Winter"]:
            season = "Winter"
        elif row.month in season_months["Spring"]:
            season = "Spring"
        elif row.month in season_months["Summer"]:
            season = "Summer"
        else:
            season = "Autumn"

        monthly_data.append({
            "month": row.month,
            "avg_tmin": row.avg_tmin,
            "avg_tmax": row.avg_tmax,
            "avg_temp": row.avg_temp,
            "total_precip": row.total_precip,
            "avg_snow_depth": row.avg_snow_depth,
            "season": season
        })


    seasonal_summary = {}
    for season, months in season_months.items():
        season_rows = [m for m in monthly_data if m["month"] in months]
        if season_rows:
            avg_temp = sum(m["avg_temp"] for m in season_rows) / len(season_rows)
            avg_tmin = min(m["avg_tmin"] for m in season_rows)
            avg_tmax = max(m["avg_tmax"] for m in season_rows)
            total_precip = sum(m["total_precip"] for m in season_rows) / len(season_rows)
            avg_snow_depth = sum(m["avg_snow_depth"] for m in season_rows) / len(season_rows)

            seasonal_summary[season.lower()] = {
                "avg_temp": avg_temp,
                "temp_range": {"min": avg_tmin, "max": avg_tmax},
                "total_precip": total_precip,
                "avg_snow_depth": avg_snow_depth
            }

    # ====================================================================

    return {
        "seasonal_summary": seasonal_summary,
        "monthly_data": monthly_data
    }
