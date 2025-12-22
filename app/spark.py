from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GHCND_API")
    .getOrCreate()
)
