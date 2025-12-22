from pyspark.sql import SparkSession

HDFS_NAMENODE = "hdfs://localhost:9000"

spark = (
    SparkSession.builder
    .appName("GHCND_API")
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
    .getOrCreate()
)
