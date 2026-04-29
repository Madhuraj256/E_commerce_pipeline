from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "madhu",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    run_pipeline = BashOperator(
        task_id="run_full_pipeline",
        bash_command="docker exec bd-pyspark-jupyter-lab python /opt/project/main.py",
    )