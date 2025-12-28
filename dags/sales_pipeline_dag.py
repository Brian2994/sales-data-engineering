from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="sales_data_pipeline",
    start_data=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    ingest = BashOperator(
        task_id="ingest_data",
        bash_command="python ingestion/load_raw_to_processed.py"
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command="python processing/transform_to_trusted.py"
    )

    build_dw = BashOperator(
        task_id="build_dw",
        bash_command="python warehouse/build_dw.py"
    )

    load_dw = BashOperator(
        task_id="load_dw_postgres",
        bash_command="python warehouse/load_dw_to_postgres.py"
    )

    ingest >> transform >> build_dw >> load_dw