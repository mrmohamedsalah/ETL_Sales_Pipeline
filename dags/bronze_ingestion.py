from airflow import DAG
from airflow.operators.python import PythonOperator  # ← changed this line
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text


# 🔹 Postgres connection
def get_engine():
    return create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )


# ✅ Task 0: Create Bronze Schema
def create_bronze_schema():
    engine = get_engine()
    with engine.begin() as conn:        # ← change connect() to begin()
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    print("✅ Bronze schema ready")


# ✅ Task 1: Load CRM Files
def load_crm_data():
    engine = get_engine()

    crm_files = {
        "cust_info.csv": "crm_cust_info",
        "prd_info.csv": "crm_prd_info",
        "sales_details.csv": "crm_sales_details",
    }

    for file_name, table_name in crm_files.items():
        df = pd.read_csv(f"/opt/airflow/datasets/source_crm/{file_name}")
        df.to_sql(
            name=table_name,
            con=engine,
            schema="bronze",
            if_exists="replace",
            index=False,
        )
        print(f"✅ Loaded {file_name} → bronze.{table_name} ({len(df)} rows)")


# ✅ Task 2: Load ERP Files
def load_erp_data():
    engine = get_engine()

    erp_files = {
        "CUST_AZ12.csv": "erp_cust_az12",
        "LOC_A101.csv": "erp_loc_a101",
        "PX_CAT_G1V2.csv": "erp_px_cat_g1v2",
    }

    for file_name, table_name in erp_files.items():
        df = pd.read_csv(f"/opt/airflow/datasets/source_erp/{file_name}")
        df.to_sql(
            name=table_name,
            con=engine,
            schema="bronze",
            if_exists="replace",
            index=False,
        )
        print(f"✅ Loaded {file_name} → bronze.{table_name} ({len(df)} rows)")


# ✅ DAG Definition
with DAG(
    dag_id="bronze_full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze"],
) as dag:

    create_schema = PythonOperator(
        task_id="create_bronze_schema",
        python_callable=create_bronze_schema,
    )

    load_crm = PythonOperator(
        task_id="load_crm",
        python_callable=load_crm_data,
    )

    load_erp = PythonOperator(
        task_id="load_erp",
        python_callable=load_erp_data,
    )

    create_schema >> load_crm >> load_erp
