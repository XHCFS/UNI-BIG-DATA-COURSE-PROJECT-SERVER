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

table_name = "yearly_aggregates"

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

final_table = monthly.groupBy("station_id","year").agg(
    # Temperatures
    avg("avg_tmin").alias("avg_tmin"),
    avg("avg_tmax").alias("avg_tmax"),
    ((avg("avg_tmin")+avg("avg_tmax"))/2).alias("avg_temp"),
    _min("min_temp").alias("min_temp"),
    _max("max_temp").alias("max_temp"),

    # Precipitation
    _sum("total_precip").alias("total_precip"),
    _max("max_precip").alias("max_precip"),

    # Snow Depth
    avg("avg_snow_depth").alias("avg_snow_depth"),
    _max("max_snow_depth").alias("max_snow_depth"),

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
