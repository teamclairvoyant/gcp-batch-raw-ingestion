import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.utils.dates import days_ago

bucket_path = "gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow"
project_id = "playground-375318"
gce_zone = "us-central1"


default_args = {
    "start_date": days_ago(1),
    "owner": "Ravi",
    "dataflow_default_options": {
        "project": project_id,
        "tempLocation": bucket_path + "/tmp/",
    },
}

with models.DAG(
    "gcp-batch-raw-ingestion-dataflow",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    mongodb_to_gcs = DataflowTemplatedJobStartOperator(
        task_id="mongodb_to_gcs",
        retries=0,
        template="gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/mongodb_to_gcs",
        location=gce_zone,
        cancel_timeout=60,
    )

    gcs_to_bq = DataflowTemplatedJobStartOperator(
        task_id="gcs_to_bq",
        retries=0,
        template="gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/gcs_to_bq",
        location=gce_zone,
        cancel_timeout=60,
    )
mongodb_to_gcs >> gcs_to_bq