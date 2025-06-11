import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

GCP_PROJECT_LOCATION = os.environ['GCP_PROJECT_LOCATION']
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
GCP_PROJECT_GCS_BUCKET = os.environ['GCP_PROJECT_GCS_BUCKET']
GCP_DATAPROC_CLUSTER = os.environ['GCP_DATAPROC_CLUSTER']

SPARK_JOB_FAILURE_SENSORS_FILE_PATH = f'gs://{GCP_PROJECT_GCS_BUCKET}/scripts/equipment_failure_sensors.py'
SPARK_JOB_EQUIPMENT_RELATION_FILE_PATH = f'gs://{GCP_PROJECT_GCS_BUCKET}/scripts/equipment_sensors_relation.py'
SPARK_JOB_FACT_SENSORS_LOGS_FILE_PATH = f'gs://{GCP_PROJECT_GCS_BUCKET}/scripts/fact_sensors_logs.py'


def get_spark_submit_config(spark_file_path):
    return {
        "reference": {
            "project_id": GCP_PROJECT_ID
        },
        "placement": {
            "cluster_name": GCP_DATAPROC_CLUSTER
        },
        "pyspark_job": {
            "main_python_file_uri": spark_file_path,
            "args": [
                "--gcp_project_id", GCP_PROJECT_ID
            ],
        },
    }


with DAG(
    'sensors_failures_pipeline',
    start_date = datetime(2025, 6, 1),
    schedule_interval = '@daily',
    default_args = {
        'owner': 'jordanamaralvicente@gmail.com',
        'retries': 2,
        'retry_delay': timedelta(minutes = 2)
    },
) as dag:
    submit_silver_layer_equipments_relation_spark = DataprocSubmitJobOperator(
        task_id = "submit_silver_layer_equipments_spark",
        region = GCP_PROJECT_LOCATION,
        project_id = GCP_PROJECT_ID,
        job = get_spark_submit_config(SPARK_JOB_EQUIPMENT_RELATION_FILE_PATH),
    )

    submit_silver_layer_sensors_failures_spark = DataprocSubmitJobOperator(
        task_id = "submit_silver_layer_sensors_failures_spark",
        region = GCP_PROJECT_LOCATION,
        project_id = GCP_PROJECT_ID,
        job = get_spark_submit_config(SPARK_JOB_FAILURE_SENSORS_FILE_PATH),
    )

    submit_gold_layer_sensors_failures_spark = DataprocSubmitJobOperator(
        task_id = "submit_gold_layer_sensors_failures_spark",
        region = GCP_PROJECT_LOCATION,
        project_id = GCP_PROJECT_ID,
        job = get_spark_submit_config(SPARK_JOB_FACT_SENSORS_LOGS_FILE_PATH),
    )

    [submit_silver_layer_equipments_relation_spark, submit_silver_layer_sensors_failures_spark] >> submit_gold_layer_sensors_failures_spark
