from datetime import datetime, timedelta

from airflow import DAG

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 4, 0, 0, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="invoices_dag_save",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    None