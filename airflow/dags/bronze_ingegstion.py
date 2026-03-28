from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.bronze_loader import load_crm_data


with DAG(
    dag_id="bronze_full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    load_crm = PythonOperator(
        task_id="load_crm",
        python_callable=load_crm_data,
    )
