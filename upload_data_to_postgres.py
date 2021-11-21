import psycopg2
from datetime import datetime
from airflow import models
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.utils.dates import days_ago


def load_data_to_postgres():

    # Download the file to a temporary directory and return a file handle
    gcs_hook = GCSHook()
    tmp_file = gcs_hook.provide_file(
        object_url="gs://wizeline-bootcamp-330020/data_source/user_purchase - user_purchase.csv"
    )

    # Open Postgres Connection
    get_postgres_conn = PostgresHook(postgres_conn_id="postgres_default").get_conn()
    curr = get_postgres_conn.cursor()

    # Load csv into postgres
    with tmp_file as tmp:
        tmp.flush()
        with open(tmp.name) as fh:
            curr.copy_expert(
                "COPY bootcampdb.user_purchase FROM STDIN WITH CSV HEADER", fh
            )
            get_postgres_conn.commit()


with models.DAG(
    "upload-data-to-postgress",
    schedule_interval="@once",  # Override to match your needs
    start_date=days_ago(0),
    tags=["load"],
) as dag:

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        provide_context=True,
        python_callable=load_data_to_postgres,
    )

    load_data_to_postgres
