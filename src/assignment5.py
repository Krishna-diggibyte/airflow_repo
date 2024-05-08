from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
defaults_args={
    'owner':'krishna',
    'retries':1,
    'retry-delay':timedelta(minutes=1) }
def get_name(ti):
    ti.xcom_push(key='name',value='Krishna')
    ti.xcom_push(key='company',value='Diggibyte')
def get_age(ti):
    ti.xcom_push(key="age",value=23)
def intro(ti):
    name=ti.xcom_pull(task_ids="get_data" ,key='name')
    comp=ti.xcom_pull(task_ids="get_data" ,key='company')
    age=ti.xcom_pull(task_ids="get_age",key='age')
    print(f"Hi my name is {name}, i work at {comp} and i am {age} year old.")
with DAG (
    default_args=defaults_args,
    dag_id='my_assignment5_dag',
    start_date=datetime(2024,5,7),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id="get_data",
        python_callable=get_name
    )
    task2 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
    )
    task3=PythonOperator(
        task_id="give_intro",
        python_callable=intro
    )
task1.set_downstream(task3)
task2.set_downstream(task3)

