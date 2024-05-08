from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_sensor import TimeSensor

# from airflow.contrib.sensors.file_sensor import FileSensor

# Define default arguments for the DAG
default_args = {
    'owner': 'krishna',
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    dag_id='my_assignment6_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# Define tasks
start_task = EmptyOperator(task_id='start_task', dag=dag)
end_task = EmptyOperator(task_id='end_task', dag=dag)

# Define the TimeSensor to wait until a specific time before triggering downstream tasks
wait_for_time = TimeSensor(
    task_id='wait_for_time',
    target_time=datetime(2024, 5, 1, 0,0,15),  # Wait until May 1, 2024, 12:00 PM
    dag=dag,
)

start_task >> wait_for_time >> end_task
