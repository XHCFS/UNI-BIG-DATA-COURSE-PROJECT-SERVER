from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    year, substring, when, isnull, round as _round, first, to_date
)
import sys
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


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

table_name = "recent_extreme_events"

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

window = (
    Window
    .partitionBy("station_id", "event_type")
    .orderBy(col("date").desc())
)


# final_table = (
#     events
#     .withColumn("rn", row_number().over(window))
#     .filter(col("rn") == 1)
#     .select(
#         "station_id",
#         "year",
#         "event_type",
#         "value",
#         "date"
#     )
# )

final_table = (
    events
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    # divide snow depth value by 10 for consistency with other tables
    .withColumn(
        "value",
        when(col("event_type") == "snow_depth", col("value") / 10)
        .otherwise(col("value"))
    )
    .select(
        "station_id",
        "year",
        "event_type",
        "value",
        "date"
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