from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# Default arguments
default_args = {
    "owner": "carlos.lopez",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

CLUSTER_NAME = "dataproc-cluster"
REGION = "us-central1"
PROJECT_ID = "wizeline-bootcamp-330020"
PYSPARK_URI = "gs://wizeline-bootcamp-330020/logic_user_behavior.py"

# Cluster definition
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
        },

}

with DAG(
    dag_id="user_behavior_dataproc",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        gcp_conn_id="dataproc_conn",
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        gcp_conn_id="dataproc_conn",
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        gcp_conn_id="dataproc_conn",
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    create_cluster >> pyspark_task >> delete_cluster
