from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def hello_world():
    print("Hello from Airflow running in Codespaces!")
    return "Task completed successfully"

def print_date():
    print(f"Current date and time: {datetime.now()}")

default_args = {
    'owner': 'codespace-user',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'codespace_example',
    default_args=default_args,
    description='A simple example DAG for Codespaces',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

# Python task
hello_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag,
)

# Another Python task
date_task = PythonOperator(
    task_id='print_date_task',
    python_callable=print_date,
    dag=dag,
)

# Bash task
bash_task = BashOperator(
    task_id='bash_example',
    bash_command='echo "This is a bash command running in Codespaces"',
    dag=dag,
)

# Set task dependencies
hello_task >> date_task >> bash_task
