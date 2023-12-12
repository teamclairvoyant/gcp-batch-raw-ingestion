from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator


default_arguments = {"owner": "Ravi", "start_date": days_ago(1)}



SPARK_JOB = {
    "reference": {"project_id": "playground-375318"},
    "placement": {"cluster_name": "spark-scala-etl"},
    "spark_job": {
        "jar_file_uris": ["gs://dataproc_ravi_poc/spark_jar/spark-scala-etl-1.0-SNAPSHOT-jar-with-dependencies.jar"],
        "main_class": "org.example.hello",
    },
}

args = {
    'owner': 'Airflow',
}

with DAG(
        "spark-scala-etl",
        schedule_interval=None,
        catchup=False,
        default_args=default_arguments,
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='playground-375318',
        cluster_name='spark-scala-etl',
        num_workers=2,
        worker_machine_type='n1-standard-2',
        storage_bucket="dataproc_ravi_poc",
        region='us-central1',
        zone='',
    )

    spark_submit = DataprocSubmitJobOperator(
        task_id="spark_submit", job=SPARK_JOB, region='us-central1', project_id='playground-375318'
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="playground-375318",
        cluster_name="spark-scala-etl",
        region='us-central1',
        trigger_rule="all_done",
    )

create_cluster >> spark_submit >> delete_cluster