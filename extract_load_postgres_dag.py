import uuid
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.postgres_operator import PostgresOperator


dag_params = {
    "dag_id": "create_table",
    "start_date": datetime(2019, 10, 7),
    "schedule_interval": "@once",
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        postgres_conn_id="cloud_postgres_sql",
        task_id="create_table",
        sql="""CREATE SCHEMA IF NOT EXISTS postgres;
            CREATE TABLE IF NOT EXISTS postgres.user_purchase(
                invoice_number VARCHAR(10),
                stock_code VARCHAR(20),
                detail VARCHAR(1000),
                quantity INTEGER,
                invoice_date TIMESTAMP,
                unit_price NUMERIC (8, 3),
                customer_id INTEGER,
                country VARCHAR(20)
            );""",
    )

    create_table
