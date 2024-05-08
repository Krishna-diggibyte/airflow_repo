from datetime import datetime, timedelta,timezone
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_sensor import TimeSensor

default_args = {
    'owner': 'krishna',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='my_assignment6_dag',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')
    t1 = TimeSensor(
        task_id="timeout_task",
        timeout=1,
        target_time=(datetime.now(timezone.utc) + timedelta(minutes=1)).time(),
    )
start_task >> t1 >> end_task