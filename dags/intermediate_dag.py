from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ---------- Python function for processing ----------
def process_data(**kwargs):
    """Simulate a data processing step"""
    raw_data = [1, 2, 3, 4, 5]
    processed = [x**2 for x in raw_data]  # square each number
    print(f"Processed data: {processed}")
    return processed  # will be saved in XCom for later tasks


def summarize_data(**kwargs):
    """Pull data from XCom and summarize it"""
    ti = kwargs['ti']  # task instance
    processed = ti.xcom_pull(task_ids='process_data')
    summary = {
        "count": len(processed),
        "max": max(processed),
        "min": min(processed)
    }
    print(f"Summary: {summary}")
    return summary


# ---------- Default Arguments ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ---------- DAG Definition ----------
with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="A simple data pipeline DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "python"],
) as dag:

    # Step 1: Print the date
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    # Step 2: Process the data in Python
    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        provide_context=True,
    )

    # Step 3: Summarize the processed data
    summarize = PythonOperator(
        task_id="summarize_data",
        python_callable=summarize_data,
        provide_context=True,
    )

    # DAG order
    print_date >> process >> summarize