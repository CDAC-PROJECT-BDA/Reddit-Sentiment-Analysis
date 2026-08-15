from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "reddit_project",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="reddit_data_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 5, 15),
    schedule="0 * * * *",
    catchup=False
) as dag:

    fetch_reddit_data = BashOperator(
        task_id="fetch_reddit_data",
        bash_command="python3 /home/sunbeam/Desktop/Reddit/reddit_fetch.py"
    )
