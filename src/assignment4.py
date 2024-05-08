from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args={
    'owner':'krishna',
    'retries':1,
    'retry-delay':timedelta(minutes=2)
}
def start():
    print('start')

def done():
    print('done')

with DAG(
    default_args=default_args,
    dag_id='my_assignment4_dag',
    start_date=datetime(2024,5,8),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1=TriggerDagRunOperator(
        task_id='first_trigger',
        trigger_dag_id='my_assignment2_dag',
        wait_for_completion=True
    )

    task2=PythonOperator(
        task_id="start_task",
        python_callable=start
    )
    task3=PythonOperator(
        task_id='done_task',
        python_callable=done
    )

task2>>task1>>task3

