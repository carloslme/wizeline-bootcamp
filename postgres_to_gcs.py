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
FILENAME = "user_purchase.csv"
SQL_QUERY = "SELECT * FROM postgres.user_purchase;"

with models.DAG(
    dag_id="postgres_to_gcs",
    schedule_interval="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["postgres", "gcs"],
) as dag:
    upload_data = PostgresToGCSOperator(
        postgres_conn_id="cloud_postgres_sql",
        task_id="postgres_to_gcs",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        export_format="csv",
        gzip=False,
    )

    upload_data
