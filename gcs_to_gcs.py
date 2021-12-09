"""
DAG using PostgresToGoogleCloudStorageOperator.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.models import Variable


PROJECT_ID = Variable.get("PROJECT_ID")
GCS_BUCKET_SOURCE = Variable.get("GCS_SOURCE")
GCS_BUCKET_DESTINATION = Variable.get("RAW_BUCKET")
OBJECT = Variable.get("FILENAME")

with models.DAG(
    dag_id="gcs_to_gcs",
    schedule_interval="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs"],
) as dag:
    copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=GCS_BUCKET_SOURCE,
        source_object=OBJECT,
        destination_bucket=GCS_BUCKET_DESTINATION,  # If not supplied the source_bucket value will be used
        destination_object="movie_review.csv",  # If not supplied the source_object value will be used
    )

    copy_single_file
