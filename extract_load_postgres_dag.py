import uuid
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.postgres_operator import PostgresOperator


dag_params = {
    "dag_id": "extract_load_dag",
    "start_date": datetime(2019, 10, 7),
    "schedule_interval": None,
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        sql="""CREATE TABLE user_purchase(
                invoice_number VARCHAR(10) NOT NULL,
                stock_code VARCHAR(20) NOT NULL,
                detail VARCHAR(1000) NOT NULL,
                quantity INTEGER NOT NULL,
                invoice_date TIMESTAMP NOT NULL,
                unit_price NUMERIC (8, 3) NOT NULL,
                customer_id INTEGER NOT NULL,
                country VARCHAR(20) NOT NULL
            );""",
    )

    create_table
