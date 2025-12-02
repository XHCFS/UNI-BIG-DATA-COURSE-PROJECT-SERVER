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

table_name = ""     # TODO: WRITE TABLE NAME HERE

if not table_name:
    raise ValueError("ERROR: Missing table name..")

hdfs_path = "hdfs:///ghcnd/*.csv"
output_path = f"hdfs:///ghcnd/agg_tables/{table_name}/"

# ============================================================================
# LOADING RAW DATA
# ============================================================================

print("\n" + "="*80)
print("CREATING AGGREGATED TABLE FOR {table_name}!")
print("="*80 + "\n")

# Read data
print("Loading data from HDFS...")
df = spark.read.csv(
    hdfs_path,
    header=False,
    inferSchema=False
).toDF("station_id", "date", "element", "value", "mflag", "qflag", "sflag", "obs_time")

df = df.withColumn("value", col("value").cast("int")) \
       .withColumn("date", col("date").cast("int")) \
       .withColumn("year", year(to_date(col("date").cast("string"), "yyyyMMdd"))) \
       .withColumn("country_code", substring(col("station_id"), 1, 2))

# ============================================================================
# AGGREGATION LOGIC (TODO)
# ============================================================================
print("\n" + "="*80)
print("RUNNING AGGREGATION PROCESSING...")
print("="*80)

final_table = df    # TODO: replace this with the actual aggregated table


print("\nWRITING OUTPUT TO PARQUET")
final_table.write.mode("overwrite").parquet(output_path)


print("\n" + "="*80)
print(f"{table_name} TABLE CREATED!!")
print("="*80 + "\n")

spark.stop()
