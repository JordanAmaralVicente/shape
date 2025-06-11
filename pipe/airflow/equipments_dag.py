import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

GCP_PROJECT_LOCATION = os.environ['GCP_PROJECT_LOCATION']
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
GCP_PROJECT_GCS_BUCKET = os.environ['GCP_PROJECT_GCS_BUCKET']
GCP_DATAPROC_CLUSTER = os.environ['GCP_DATAPROC_CLUSTER']

SPARK_JOB_SILVER_FILE_PATH = f'gs://{GCP_PROJECT_GCS_BUCKET}/scripts/equipments.py'
SPARK_JOB_GOLD_FILE_PATH = f'gs://{GCP_PROJECT_GCS_BUCKET}/scripts/dm_assets.py'


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
    'equipments_pipeline',
    start_date = datetime(2025, 6, 1),
    schedule_interval = '@daily',
    default_args = {
        'owner': 'jordanamaralvicente@gmail.com',
        'retries': 2,
        'retry_delay': timedelta(minutes = 2)
    },
) as dag:
    submit_silver_layer_equipments_spark = DataprocSubmitJobOperator(
        task_id = "submit_silver_layer_equipments_spark",
        region = GCP_PROJECT_LOCATION,
        project_id = GCP_PROJECT_ID,
        job = get_spark_submit_config(SPARK_JOB_SILVER_FILE_PATH),
    )

    submit_gold_layer_equipments_spark = DataprocSubmitJobOperator(
        task_id = "submit_gold_layer_equipments_spark",
        region = GCP_PROJECT_LOCATION,
        project_id = GCP_PROJECT_ID,
        job = get_spark_submit_config(SPARK_JOB_GOLD_FILE_PATH),
    )

    submit_silver_layer_equipments_spark >> submit_gold_layer_equipments_spark
