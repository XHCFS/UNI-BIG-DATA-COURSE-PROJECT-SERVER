#!/bin/bash

export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop

# Clean up any previous temp files
rm -rf /tmp/spark-* /tmp/blockmgr-*

echo "Starting Spark analysis..."
echo "Using 10GB driver memory to leave room for OS"

# Use only 10GB instead of 16GB to leave room for OS
spark-submit \
  --master local[4] \
  --driver-memory 10g \
  --executor-memory 2g \
  --conf spark.local.dir=/tmp \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.sql.shuffle.partitions=50 \
  ghcnd_analysis.py 2>&1 | tee analysis_report.txt

echo "Analysis complete. Report saved to analysis_report.txt"
