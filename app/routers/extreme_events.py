from fastapi import APIRouter, Query
from app.spark import spark
from pyspark.sql.functions import col, sum

router = APIRouter(prefix="/extreme_events", tags=["extreme_events"])

YEARLY_EXTREME_COUNTS_PATH = "hdfs:///ghcnd/agg_tables/extreme_events_yearly_aggregates/"
RECENT_EXTREME_EVENT_PATH = "hdfs:///ghcnd/agg_tables/recent_extreme_events/"


@router.get("/")
def get_extreme_events_data(
    country_prefix: str = Query(...),
    start_year: int = Query(...),
    end_year: int = Query(...)
):
    # Load aggregated tables and filter by station_id and year
    yearly_df  = spark.read.parquet(YEARLY_EXTREME_COUNTS_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    recent_df = spark.read.parquet(RECENT_EXTREME_EVENT_PATH).filter(
        (col("station_id").startswith(country_prefix)) &
        (col("year") >= start_year) & (col("year") <= end_year)
    )

    # Values from YEARLY_EXTREME_COUNTS 
    # ====================================================================

    extreme_types = ["heatwave_count", "coldwave_count", "heavy_precip_count", "snowfall_count"]

    yearly_counts_df = yearly_df.groupBy("year").sum(*extreme_types).orderBy("year")
    yearly_counts = {
        e: [{"year": row.year, "count": row[f"sum({e})"]} for row in yearly_counts_df.collect()]
        for e in extreme_types
    }


    totals_result = yearly_df.agg(
        sum("heatwave_count").alias("heatwave_total"),
        sum("coldwave_count").alias("coldwave_total"),
        sum("heavy_precip_count").alias("heavy_precip_total"),
        sum("snowfall_count").alias("snowfall_total")
    ).first()

    total_counts = {
        "heatwave_count": totals_result["heatwave_total"] if totals_result else None,
        "coldwave_count": totals_result["coldwave_total"] if totals_result else None,
        "heavy_precip_count": totals_result["heavy_precip_total"] if totals_result else None,
        "snowfall_count": totals_result["snowfall_total"] if totals_result else None
    }


    # Values from RECENT_EXTREME_EVENT 
    # ====================================================================

    def format_date(d):
        return str(d) if d else None

    recent_events = {}
    for e in ["heatwave", "coldwave", "heavy_precip", "snowfall"]:
        df = recent_df.filter(col("event_type") == e)
        most_recent_row = df.orderBy(col("date").desc()).limit(1).collect()
        if most_recent_row:
            row = most_recent_row[0]
            recent_events[e] = {"date": format_date(row.date), "value": row.value}
        else:
            recent_events[e] = {"date": None, "value": None}

    # ====================================================================

    return {
        "heatwave": {
            "total_count": total_counts["heatwave_count"],
            "yearly_counts": yearly_counts["heatwave_count"],
            "most_recent": recent_events["heatwave"]
        },
        "coldwave": {
            "total_count": total_counts["coldwave_count"],
            "yearly_counts": yearly_counts["coldwave_count"],
            "most_recent": recent_events["coldwave"]
        },
        "heavy_precipitation": {
            "total_count": total_counts["heavy_precip_count"],
            "yearly_counts": yearly_counts["heavy_precip_count"],
            "most_recent": recent_events["heavy_precip"]
        },
        "heavy_snowfall": {
            "total_count": total_counts["snowfall_count"],
            "yearly_counts": yearly_counts["snowfall_count"],
            "most_recent": recent_events["snowfall"]
        }
    }
