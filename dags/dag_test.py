from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'MLTeam',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


# Define the DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Process tweet topics data',
    schedule_interval='@daily',
)

task_1 = BashOperator(
    task_id=f'task_id',
    bash_command="pwd",
    dag=dag,
)

task_1