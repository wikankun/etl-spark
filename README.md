# ETL Spark

ETL Learning Project Using Apache Spark

## Prerequisite

- Google Cloud Platform
- Python 3 for local development

## How to run

- Create a project within Google Cloud Platform
- Define your project variables in `spark_run.sh` and `spark_job.py`
- Simply run
  ```
  bash spark_run.sh
  ```
  for windows, or
  ```
  . spark_run.sh
  ```
  for linux

## What does my code do?

The main goal is to make a spark job to process the data into Bigquery table.

- The `spark_run.sh` contains bash script to get the data to Google Cloud Storage, create Dataproc cluster, and submit `spark_job.py` to the Dataproc cluster.
- The `spark_job.py` contains python script to extract files, transform the data, and load it into Bigquery and several different file types.

## Outputs

The outputs are:
- Bigquery tables
- JSON files
- CSV file
- Parquet file
