from fastapi import FastAPI
from pyspark.sql import SparkSession

app = FastAPI(title="GHCND API")
spark = SparkSession.builder.appName("GHCND_API").getOrCreate()

#df_raw = spark.read.csv("hdfs://localhost:9000/ghcnd/*.csv.gz", header=True, inferSchema=True)
#df_raw.createOrReplaceTempView("ghcnd_raw")

# test
@app.get("/test")
def test_connection():
    return "HI :>"
