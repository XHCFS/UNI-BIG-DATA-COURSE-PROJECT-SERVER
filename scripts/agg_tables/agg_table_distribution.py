from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    year, substring, when, isnull, round as _round, first, to_date
)
import sys
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import floor
from pyspark.sql.functions import lit

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

table_name = "distribution"

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

# ====================================================================
# DEFINE BIN SIZE PER VARIABLE
# ====================================================================
bin_sizes = {
    "avg_tmin": 1,
    "avg_tmax": 1,
    "avg_temp": 1,
    "total_precip": 5,
    "avg_snow_depth": 5
}

# ====================================================================
# CREATE DISTRIBUTION TABLE
# ====================================================================

distributions = []

for element, bin_size in bin_sizes.items():
    dist = (
        monthly
        .filter(col(element).isNotNull())
        .withColumn("value", col(element))
        .withColumn("bin_min", floor(col("value") / bin_size) * bin_size)
        .withColumn("bin_max", floor(col("value") / bin_size) * bin_size + bin_size)
        .groupBy("station_id", "bin_min", "bin_max")
        .count()
        .withColumn("variable", lit(element))
        .select("station_id", "variable", "bin_min", "bin_max", "count")
    )
    distributions.append(dist)


# Union all variable distributions
final_table = reduce(DataFrame.unionByName, distributions)

# =====================================================================
# SAVE TABLE
# =====================================================================

print("\nWRITING OUTPUT TO PARQUET")
final_table.write.mode("overwrite").parquet(output_path)


print("\n" + "="*80)
print(f"{table_name} TABLE CREATED!!")
print("="*80 + "\n")

spark.stop()
