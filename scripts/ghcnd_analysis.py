from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
    year, substring, when, isnull, round as _round, first, to_date
)
import sys

# Initialize with MINIMAL memory settings
spark = SparkSession.builder \
    .appName("GHCN-D Chunked Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "30") \
    .config("spark.sql.files.maxPartitionBytes", "512MB") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "5MB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

hdfs_path = "hdfs:///ghcnd/*.csv"

print("\n" + "="*80)
print("GHCN-D CLIMATE DATA QUALITY ANALYSIS REPORT")
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
# BASIC STATISTICS (No caching, direct aggregation)
# ============================================================================
print("\n" + "="*80)
print("BASIC STATISTICS")
print("="*80)

basic_stats = df.agg(
    count("*").alias("total_records"),
    countDistinct("station_id").alias("total_stations"),
    countDistinct("country_code").alias("total_countries"),
    _min("year").alias("first_year"),
    _max("year").alias("last_year"),
    _sum(when(isnull(col("value")), 1).otherwise(0)).alias("missing_values")
).collect()[0]

print(f"\nTotal Records:      {basic_stats['total_records']:,}")
print(f"Total Stations:     {basic_stats['total_stations']:,}")
print(f"Total Countries:    {basic_stats['total_countries']:,}")
print(f"Year Range:         {basic_stats['first_year']} - {basic_stats['last_year']}")
missing_pct = (basic_stats['missing_values'] / basic_stats['total_records'] * 100)
print(f"Missing Values:     {basic_stats['missing_values']:,} ({missing_pct:.2f}%)")

# ============================================================================
# TOP STATIONS BY RECORD COUNT
# ============================================================================
print("\n" + "="*80)
print("TOP 20 STATIONS BY RECORD COUNT")
print("="*80 + "\n")

station_stats = df.groupBy("station_id", "country_code").agg(
    count("*").alias("records"),
    countDistinct("year").alias("years"),
    _min("year").alias("first_year"),
    _max("year").alias("last_year"),
    _sum(when(isnull(col("value")), 1).otherwise(0)).alias("missing")
).withColumn(
    "missing_pct",
    _round((col("missing") / col("records") * 100), 1)
).orderBy(col("records").desc())

station_stats.select(
    col("station_id").alias("Station ID"),
    col("country_code").alias("Country"),
    col("records").alias("Records"),
    col("years").alias("Years"),
    col("first_year").alias("From"),
    col("last_year").alias("To"),
    col("missing_pct").alias("Missing %")
).show(20, truncate=False)

# ============================================================================
# STATIONS PER COUNTRY
# ============================================================================
print("\n" + "="*80)
print("TOP 20 COUNTRIES BY NUMBER OF STATIONS")
print("="*80 + "\n")

country_stats = df.groupBy("country_code").agg(
    countDistinct("station_id").alias("stations"),
    count("*").alias("records")
).orderBy(col("stations").desc())

country_stats.select(
    col("country_code").alias("Country"),
    col("stations").alias("Stations"),
    col("records").alias("Records")
).show(20, truncate=False)

# ============================================================================
# ELEMENT DISTRIBUTION
# ============================================================================
print("\n" + "="*80)
print("OBSERVATION ELEMENT DISTRIBUTION (Top 15)")
print("="*80 + "\n")

element_stats = df.groupBy("element").agg(
    count("*").alias("records"),
    countDistinct("station_id").alias("stations")
).orderBy(col("records").desc())

element_stats.select(
    col("element").alias("Element"),
    col("records").alias("Records"),
    col("stations").alias("Stations")
).show(15, truncate=False)

# ============================================================================
# TEMPERATURE STATISTICS (TMAX/TMIN only)
# ============================================================================
print("\n" + "="*80)
print("TEMPERATURE STATISTICS")
print("="*80 + "\n")

temp_df = df.filter(col("element").isin(["TMAX", "TMIN"]))

temp_validity = temp_df.agg(
    count("*").alias("temp_records"),
    _sum(when(col("value") == 9999, 1).otherwise(0)).alias("sentinel_9999"),
    _sum(when((col("value") < -900) | (col("value") > 600), 1).otherwise(0)).alias("out_of_range"),
    _sum(when(isnull(col("value")), 1).otherwise(0)).alias("null_values")
).collect()[0]

print(f"Total Temperature Records:  {temp_validity['temp_records']:,}")
print(f"Sentinel Values (9999):     {temp_validity['sentinel_9999']:,}")
print(f"Out of Range (-90°C - 60°C):{temp_validity['out_of_range']:,}")
print(f"Null Values:                {temp_validity['null_values']:,}")

# Top 10 stations by temperature data
print("\nTop 10 Stations by Temperature Records:\n")
temp_clean = temp_df.filter(
    (col("value").isNotNull()) &
    (col("value") != 9999) &
    (col("value") >= -900) &
    (col("value") <= 600)
).withColumn("temp_c", col("value") / 10.0)

temp_station_stats = temp_clean.groupBy("station_id", "country_code", "element").agg(
    count("*").alias("records"),
    _round(avg("temp_c"), 1).alias("avg_temp")
)

temp_pivot = temp_station_stats.groupBy("station_id", "country_code").pivot("element").agg(
    first("records").alias("count"),
    first("avg_temp").alias("avg")
)

temp_pivot.select(
    col("station_id").alias("Station ID"),
    col("country_code").alias("Country"),
    col("TMAX_count").alias("TMAX Records"),
    col("TMAX_avg").alias("Avg TMAX °C"),
    col("TMIN_count").alias("TMIN Records"),
    col("TMIN_avg").alias("Avg TMIN °C")
).orderBy((col("TMAX_count") + col("TMIN_count")).desc()).show(10, truncate=False)

# ============================================================================
# PRECIPITATION STATISTICS
# ============================================================================
print("\n" + "="*80)
print("PRECIPITATION STATISTICS")
print("="*80 + "\n")

precip_df = df.filter(col("element") == "PRCP")

precip_stats = precip_df.agg(
    count("*").alias("prcp_records"),
    _sum(when(isnull(col("value")), 1).otherwise(0)).alias("null_values"),
    _sum(when(col("value") > 0, 1).otherwise(0)).alias("rainy_days")
).collect()[0]

print(f"Total Precipitation Records: {precip_stats['prcp_records']:,}")
print(f"Null Values:                 {precip_stats['null_values']:,}")
print(f"Days with Precipitation:     {precip_stats['rainy_days']:,}")

# Top 10 wettest stations
print("\nTop 10 Stations by Precipitation Records:\n")
precip_clean = precip_df.filter(
    (col("value").isNotNull()) &
    (col("value") != 9999) &
    (col("value") >= 0)
)

precip_station_stats = precip_clean.groupBy("station_id", "country_code").agg(
    count("*").alias("records"),
    _round(avg(col("value")) / 10, 1).alias("avg_mm"),
    _round(_max(col("value")) / 10, 1).alias("max_mm")
)

precip_station_stats.select(
    col("station_id").alias("Station ID"),
    col("country_code").alias("Country"),
    col("records").alias("Records"),
    col("avg_mm").alias("Avg mm"),
    col("max_mm").alias("Max mm")
).orderBy(col("records").desc()).show(10, truncate=False)

# ============================================================================
# TEMPORAL COVERAGE
# ============================================================================
print("\n" + "="*80)
print("TEMPORAL COVERAGE - FIRST 10 YEARS")
print("="*80 + "\n")

temporal_stats = df.groupBy("year").agg(
    count("*").alias("records"),
    countDistinct("station_id").alias("stations")
).orderBy("year")

temporal_stats.select(
    col("year").alias("Year"),
    col("records").alias("Records"),
    col("stations").alias("Stations")
).show(10, truncate=False)

print("\n" + "="*80)
print("TEMPORAL COVERAGE - LAST 10 YEARS")
print("="*80 + "\n")

temporal_stats.orderBy(col("year").desc()).select(
    col("year").alias("Year"),
    col("records").alias("Records"),
    col("stations").alias("Stations")
).show(10, truncate=False)

# ============================================================================
# DATA QUALITY BY RECENT YEARS
# ============================================================================
print("\n" + "="*80)
print("DATA QUALITY TRENDS (2015-2024)")
print("="*80 + "\n")

recent_quality = df.filter(col("year") >= 2015).groupBy("year").agg(
    count("*").alias("records"),
    countDistinct("station_id").alias("stations"),
    _sum(when(isnull(col("value")), 1).otherwise(0)).alias("missing")
).withColumn(
    "missing_pct",
    _round((col("missing") / col("records") * 100), 2)
).orderBy(col("year").desc())

recent_quality.select(
    col("year").alias("Year"),
    col("records").alias("Records"),
    col("stations").alias("Stations"),
    col("missing_pct").alias("Missing %")
).show(20, truncate=False)

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

spark.stop()
