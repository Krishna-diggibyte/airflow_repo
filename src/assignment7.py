from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'krishna',
    'retries': 2,
    'retry-delay': timedelta(minutes=2)
}

with DAG(
        dag_id='my_assignment7_dag',
        default_args=default_args,
        start_date=datetime(2024, 5, 6, 0),
        # schedule_interval="0 * * * *",
        schedule_interval='@daily',
        catchup=False
) as dag:
    task = BashOperator(
        task_id="task1",
        bash_command='echo I am testing corn scheduling'
    )
