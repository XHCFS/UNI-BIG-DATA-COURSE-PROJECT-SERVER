## Operations and Maintenance

### Data Correction and Regeneration

If an aggregated HDFS table is created with an incorrect schema or data (for example, due to pointing a Spark job at the wrong input directory), remove it and regenerate:

```bash
hdfs dfs -rm -r /ghcnd/agg_tables/yearly_aggregates
scripts/run_spark.sh scripts/agg_tables/agg_table_yearly_aggregates.py
```

Apply the same pattern for other tables under `/ghcnd/agg_tables/` as needed.

The `scripts/run_spark.sh` helper should be used for all table-building jobs; it configures Spark memory limits, Hadoop configuration, and a stable temporary directory, and writes a `*_report.txt` log file for each run.

### Spark Temporary Directories

The API uses a long-lived `SparkSession` running under a `systemd` service. If `/tmp` is cleaned aggressively, Spark may experience missing block manager directories (errors mentioning `/tmp/blockmgr-...`).

In such cases, restart the service to create new temporary directories:

```bash
sudo systemctl restart ghcnd-api
```

### Null Values in Responses

When no data is found for a requested combination of `country_prefix`, `start_year`, and `end_year`, some endpoints return `null` values rather than failing:

- Aggregate metrics (means, sums, ranges) may be `null`
- Extreme event records may have `null` `date` or `value`

Client code should be written to handle `null` values explicitly.

### Monitoring

Use standard `systemd` and journal commands:

```bash
sudo systemctl status ghcnd-api
journalctl -u ghcnd-api -f
```

Monitor:

- Application logs (FastAPI and Uvicorn)
- Spark logs and warnings
- HDFS health and disk usage

### Analysis and Spark Helper Scripts

The repository includes several additional shell and Python helpers:

- `scripts/run_spark.sh`: Wrapper for `spark-submit` that standardises memory, temporary directories, and logging. Use this for all aggregation jobs under `scripts/agg_tables/`.
- `scripts/exploratory_data_analysis.sh`: Convenience script for running exploratory Spark jobs defined in `scripts/ghcnd_analysis.py` or related analysis code.
- `scripts/spark_jobs.py`: Python module that can be extended to orchestrate or schedule multiple Spark jobs.
- `deploy.sh` / `local_deploy.sh`: Deployment helpers that fetch the latest code for a given branch and restart the `ghcnd-api` systemd service (remote and local variants respectively).

## Acknowledgements

- NOAA for providing the GHCND dataset.
- Kaggle for hosting the dataset.
- Apache Hadoop and Apache Spark for the data processing stack.
- FastAPI and Uvicorn for the API framework and ASGI server.


