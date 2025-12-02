#!/bin/bash

# Check if filename argument is provided
if [ $# -eq 0 ]; then
    echo "Error: No Python file specified"
    echo "Usage: $0 <python_file.py>"
    exit 1
fi

# Get the input filename
INPUT_FILE="$1"

# Check if file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: File '$INPUT_FILE' not found"
    exit 1
fi

# Extract filename without extension for log file
BASENAME=$(basename "$INPUT_FILE" .py)
LOG_FILE="${BASENAME}_report.txt"

export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop

# Create a work directory on the main disk (not in tmpfs)
WORK_DIR="/var/spark-temp"
mkdir -p "$WORK_DIR"

# Clean up any previous temp files
rm -rf /tmp/spark-* /tmp/blockmgr-* "$WORK_DIR"/*

echo "Starting Spark analysis..."
echo "Input file: $INPUT_FILE"
echo "Log file: $LOG_FILE"
echo "Using 10GB driver memory to leave room for OS"

# Use only 10GB instead of 16GB to leave room for OS
spark-submit \
  --master local[4] \
  --driver-memory 10g \
  --executor-memory 2g \
  --conf spark.local.dir="$WORK_DIR" \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.sql.shuffle.partitions=50 \
  "$INPUT_FILE" 2>&1 | tee "$LOG_FILE"

echo "Analysis complete. Report saved to $LOG_FILE"
