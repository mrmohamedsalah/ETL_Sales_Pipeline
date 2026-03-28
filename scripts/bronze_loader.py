import pandas as pd
from sqlalchemy import create_engine


def get_engine():
    return create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )


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
