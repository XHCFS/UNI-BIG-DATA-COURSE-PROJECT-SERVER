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

table_name = "coverage"

if not table_name:
    raise ValueError("ERROR: Missing table name..")

monthly_table_path = "hdfs:///ghcnd/agg_tables/monthly_aggregates/"

if not monthly_table_path:
    raise ValueError("ERROR: where is monthly table..")


hdfs_path = "hdfs:///ghcnd/*.csv"
output_path = f"hdfs:///ghcnd/agg_tables/{table_name}/"

# ============================================================================
# LOADING MONTHLY AGGREGATES
# ============================================================================

print("\n" + "="*80)
print(f"CREATING AGGREGATED TABLE FOR {table_name}!")
print("="*80 + "\n")

# Read data
print("Loading data from monthly table...")
monthly = spark.read.parquet(monthly_table_path)

# ============================================================================
# AGGREGATION LOGIC 
# ============================================================================

print("\n" + "="*80)
print("RUNNING AGGREGATION PROCESSING...")
print("="*80)

coverage = monthly.groupBy("station_id", "year").agg(
    _sum("missing_tmin").alias("missing_tmin"),
    _sum("missing_tmax").alias("missing_tmax"),
    _sum("missing_precip").alias("missing_precip"),
    _sum("missing_snow").alias("missing_snow")
)

# Compute missing percentage (out of 12 months per element)
coverage = coverage.withColumn(
    "days_in_year",
    when((col("year") % 4 == 0) & ((col("year") % 100 != 0) | (col("year") % 400 == 0)), 366)
    .otherwise(365)
)

final_table = coverage.withColumn(
    "missing_percentage",
    _round(
        (col("missing_tmin") + col("missing_tmax") + col("missing_precip") + col("missing_snow")) / (col("days_in_year") * 4) * 100,
        2
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
