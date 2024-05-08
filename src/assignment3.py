from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner':'krishna',
    'retries':1,
    'retry-delay':timedelta(minutes=2)
}

def greet(name,company):
    print(f"Hi All, My self {name}, and i am working at {company} ")

with DAG(
    default_args=default_args,
    dag_id='my_assignment3_dag',
    start_date=datetime(2024,5,7),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='task_first',
        python_callable=greet,
        op_kwargs={'name':'Krishna','company':'Diggibyte'}
    )
