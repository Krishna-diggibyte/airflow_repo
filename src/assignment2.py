from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner':'krishna',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id='my_assignment2_dag',
    default_args=default_args,
    start_date=datetime(2024,5,7),
    schedule_interval='0 0 * * *'
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo Hi i am krishna and i am using bash Operator"
    )
