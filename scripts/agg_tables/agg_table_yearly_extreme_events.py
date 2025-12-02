from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    year, substring, when, isnull, round as _round, first, to_date
)
import sys

# ============================================================================
# SPARK INITIALIZATION
# ============================================================================

# Initialize with MINIMAL memory settings
spark = SparkSession.builder \
    .appName("Aggregated Table") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "30") \
    .config("spark.sql.files.maxPartitionBytes", "512MB") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "5MB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# PATHS INITIALIZATION
# ============================================================================

table_name = "extreme_events_yearly_aggregates"

if not table_name:
    raise ValueError("ERROR: Missing table name..")

extreme_events_table_path = "hdfs:///ghcnd/agg_tables/extreme_events/"

if not extreme_events_table_path:
    raise ValueError("ERROR: where is monthly table..")


hdfs_path = "hdfs:///ghcnd/*.csv"
output_path = f"hdfs:///ghcnd/agg_tables/{table_name}/"

# ============================================================================
# LOADING EXTREME EVENTS
# ============================================================================

print("\n" + "="*80)
print(f"CREATING AGGREGATED TABLE FOR {table_name}!")
print("="*80 + "\n")

# Read data
print("Loading data from extreme events table...")
events  = spark.read.parquet(extreme_events_table_path)
       

# ============================================================================
# AGGREGATION LOGIC 
# ============================================================================
print("\n" + "="*80)
print("RUNNING AGGREGATION PROCESSING...")
print("="*80)

events = events.withColumn("year", year(col("date")))

# ====================================================================
# COUNT EVENTS PER TYPE
# ====================================================================

final_table = (
    events.groupBy("station_id", "year")
    .agg(
        count(when(col("event_type") == "heatwave", True)).alias("heatwave_count"),
        count(when(col("event_type") == "coldwave", True)).alias("coldwave_count"),
        count(when(col("event_type") == "heavy_precip", True)).alias("heavy_precip_count"),
        count(when(col("event_type") == "heavy_snow", True)).alias("snowfall_count")
    )
)


# =====================================================================
# SAVE TABLE
# =====================================================================

print("\nWRITING OUTPUT TO PARQUET")
final_table.write.mode("overwrite").parquet(output_path)


print("\n" + "="*80)
print(f"{table_name} TABLE CREATED!!")
print("="*80 + "\n")

spark.stop()