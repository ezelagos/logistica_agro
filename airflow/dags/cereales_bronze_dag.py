from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.bronze.cereales_bronze import process_cereales_raw

with DAG(
    dag_id="bronze_cereales_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "cereales"]
) as dag:

    bronze_task = PythonOperator(
        task_id="process_cereales_bronze",
        python_callable=process_cereales_raw,
        op_kwargs={
            "input_path": "data/raw/logistica_transporte.csv",
            "output_path": "data/bronze/cereales.parquet"
        }
    )
