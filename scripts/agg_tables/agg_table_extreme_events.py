from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    year, substring, when, isnull, round as _round, first, to_date
)
import sys
from pyspark.sql.functions import expr

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

table_name = "extreme_events"

if not table_name:
    raise ValueError("ERROR: Missing table name..")


hdfs_path = "hdfs:///ghcnd/*.csv"
output_path = f"hdfs:///ghcnd/agg_tables/{table_name}/"

# ====================================================================
# THRESHOLDS
# ====================================================================
tmax_threshold = 300
tmin_threshold = 50
precp_threshold = 700
snwd_threshold = 10

# ====================================================================
# LOAD RAW GHCN DAILY
# ====================================================================

df = spark.read.csv(
    hdfs_path,
    header=False
).toDF("station_id", "date", "element", "value", "mflag", "qflag", "sflag", "obs_time")

df = df.withColumn("value", col("value").cast("int")) \
       .withColumn("date", to_date(col("date").cast("string"), "yyyyMMdd"))

# ============================================================================
# AGGREGATION LOGIC 
# ============================================================================

print("\n" + "="*80)
print("RUNNING AGGREGATION PROCESSING...")
print("="*80)


# ====================================================================
# EXTREME CONDITIONS
# ====================================================================

extreme = (
    df
    # SNOW DEPTH
    .withColumn(
        "heavy_snow",
        when((col("element") == "SNWD") & (col("value") >= snwd_threshold), col("value"))
    )
    # PRECIP
    .withColumn(
        "heavy_precip",
        when((col("element") == "PRCP") & (col("value") >= precp_threshold), col("value")/10)
    )
    # HEATWAVE threshold
    .withColumn(
        "heatwave_temp",
        when((col("element") == "TMAX") & (col("value") > tmax_threshold), col("value")/10)
    )
    # COLDWAVE threshold
    .withColumn(
        "coldwave_temp",
        when((col("element") == "TMIN") & (col("value") < tmin_threshold), col("value")/10)
    )
)

# ====================================================================
# RESHAPE INTO STANDARD FORMAT
# ====================================================================

final_table = extreme.select(
    "station_id", "date",
    expr("stack(4, \
        'heavy_snow', heavy_snow, \
        'heavy_precip', heavy_precip, \
        'heatwave', heatwave_temp, \
        'coldwave', coldwave_temp \
    ) as (event_type, value)")
).filter(col("value").isNotNull())

# =====================================================================
# SAVE TABLE
# =====================================================================

print("\nWRITING OUTPUT TO PARQUET")
final_table.write.mode("overwrite").parquet(output_path)


print("\n" + "="*80)
print(f"{table_name} TABLE CREATED!!")
print("="*80 + "\n")

spark.stop()
