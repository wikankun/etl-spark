#!/bin/bash

# declare your variables
TEMPLATE=flights_etl
REGION=us-central1
ZONE=us-central1-a
PROJECT_ID=blank-space-315611
CLUSTER_NAME=pyspark-week3
BUCKET_NAME=spark-week3

# rename file and copy them from local to GCS
for filename in ./data/*.json
do
  EXTRACTED_FILENAME=$(echo $filename | grep -Eo '[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}')
  DATE_ADDED=$(date -d "$EXTRACTED_FILENAME +723 days" '+%Y-%m-%d')
  gsutil cp ${filename} gs://${BUCKET_NAME}/data/${DATE_ADDED}.json
done

# set project ID
gcloud config set project ${PROJECT_ID}

# copy spark job from local to GCS
gsutil cp spark_job.py gs://${BUCKET_NAME}/spark_job.py

# create dataproc template
gcloud dataproc workflow-templates create ${TEMPLATE} \
  --region=${REGION}

# create dataproc cluster by workflow-template
gcloud dataproc workflow-templates set-managed-cluster ${TEMPLATE} \
  --region=${REGION} \
  --bucket=${BUCKET_NAME} \
  --zone=${ZONE} \
  --cluster-name=${CLUSTER_NAME} \
  --master-machine-type=n1-standard-2 \
  --num-workers 2 \
  --master-boot-disk-size 50

# add pyspark job to dataproc cluster
gcloud dataproc workflow-templates add-job pyspark gs://${BUCKET_NAME}/spark_job.py \
  --step-id="flights_etl_job" \
  --workflow-template=${TEMPLATE} \
  --region=${REGION} \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

gcloud dataproc workflow-templates instantiate ${TEMPLATE} \
  --region=${REGION}
