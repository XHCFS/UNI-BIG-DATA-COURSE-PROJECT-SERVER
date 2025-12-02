from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    year, month, substring, when, isnull, round as _round, first, to_date, expr
)
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
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

table_name = "monthly_aggregates"

if not table_name:
    raise ValueError("ERROR: Missing table name..")

hdfs_path = "hdfs:///ghcnd/*.csv"
output_path = f"hdfs:///ghcnd/agg_tables/{table_name}/"

# ============================================================================
# LOADING RAW DATA
# ============================================================================

print("\n" + "="*80)
print(f"CREATING AGGREGATED TABLE FOR {table_name}!")
print("="*80 + "\n")

# Read data
print("Loading data from HDFS...")
df = spark.read.csv(
    hdfs_path,
    header=False,
    inferSchema=False
).toDF("station_id", "date", "element", "value", "mflag", "qflag", "sflag", "obs_time")

df = df.withColumn("value", col("value").cast("int")) \
       .withColumn("date", to_date(col("date").cast("string"), "yyyyMMdd")) \
       .withColumn("year", year(col("date"))) \
       .withColumn("month", month(col("date"))) \
       .withColumn("country_code", substring(col("station_id"), 1, 2))



df = df.repartition("station_id")

# ============================================================================
# AGGREGATION LOGIC 
# ============================================================================
print("\n" + "="*80)
print("RUNNING AGGREGATION PROCESSING...")
print("="*80)

# =====================================================================
# AGGREGATIONS PER ELEMENT
# =====================================================================

monthly = df.groupBy("station_id", "year", "month").agg(
    # TMIN
    avg(when(col("element")=="TMIN", col("value"))).alias("avg_tmin"),
    _min(when(col("element")=="TMIN", col("value"))).alias("min_temp"),
    count(when(col("element")=="TMIN", col("value"))).alias("count_tmin"),

    # TMAX
    avg(when(col("element")=="TMAX", col("value"))).alias("avg_tmax"),
    _max(when(col("element")=="TMAX", col("value"))).alias("max_temp"),
    count(when(col("element")=="TMAX", col("value"))).alias("count_tmax"),

    #PRCP
    _sum(when(col("element")=="PRCP", col("value"))).alias("total_precip"),
    _max(when(col("element")=="PRCP", col("value"))).alias("max_precip"),
    count(when(col("element")=="PRCP", col("value"))).alias("count_prcp"),

    # SNWD
    avg(when(col("element")=="SNWD", col("value"))).alias("avg_snow_depth"),
    _max(when(col("element")=="SNWD", col("value"))).alias("max_snow_depth"),
    count(when(col("element")=="SNWD", col("value"))).alias("count_snwd")
)

# =====================================================================
# CALCULATE MISSING DAYS
# =====================================================================
monthly = monthly.withColumn(
    "days_in_month",
    expr("day(last_day(to_date(concat(year,'-',month,'-01'))))")
)

monthly = monthly.withColumn("missing_tmin", col("days_in_month") - col("count_tmin")) \
                 .withColumn("missing_tmax", col("days_in_month") - col("count_tmax")) \
                 .withColumn("missing_precip", col("days_in_month") - col("count_prcp")) \
                 .withColumn("missing_snow", col("days_in_month") - col("count_snwd"))



# =====================================================================
# FINAL TABLE
# =====================================================================
final_table = monthly.select(
    "station_id", "year", "month",
    # Temperatures
    (col("avg_tmin")/10.0).alias("avg_tmin"),
    (col("avg_tmax")/10.0).alias("avg_tmax"),
    ((col("avg_tmin")+col("avg_tmax"))/20.0).alias("avg_temp"),
    (col("min_temp")/10.0).alias("min_temp"),
    (col("max_temp")/10.0).alias("max_temp"),

    # Precipitation
    (col("total_precip")/10.0).alias("total_precip"),
    (col("max_precip")/10.0).alias("max_precip"),

    # Snow Depth
    (col("avg_snow_depth")/10.0).alias("avg_snow_depth"),
    (col("max_snow_depth")/10.0).alias("max_snow_depth"),

    # Missing Values
    "missing_tmin", "missing_tmax", "missing_precip", "missing_snow"
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
