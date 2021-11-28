"""
DAG using PostgresToGoogleCloudStorageOperator.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.models import Variable

PROJECT_ID = Variable.get("PROJECT_ID")
GCS_BUCKET = Variable.get("STAGING_BUCKET")
FILENAME = "test_file"
SQL_QUERY = "select * from user_purchase;"

with models.DAG(
    dag_id="",
    schedule_interval="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["postgres", "gcs"],
) as dag:
    upload_data = PostgresToGCSOperator(
        task_id="get_data",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        export_format="csv",
        gzip=False,
    )

    upload_data
