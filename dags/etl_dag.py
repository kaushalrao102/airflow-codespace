import os
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ---------- Extract ----------
def extract_data(**kwargs):
    url = "https://jsonplaceholder.typicode.com/posts"  # fake API
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print(f"Extracted {len(data)} records")
    return data  # stored in XCom


# ---------- Transform ----------
def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract')

    df = pd.DataFrame(raw_data)
    # Simple transformation: keep only userId, id, and title
    transformed = df[['userId', 'id', 'title']].copy()
    transformed['title_length'] = transformed['title'].apply(len)

    print(f"Transformed dataset shape: {transformed.shape}")
    return transformed.to_dict(orient="records")  # return as JSON-compatible


# ---------- Load ----------
def load_data(**kwargs):
    ti = kwargs['ti']
    transformed = ti.xcom_pull(task_ids='transform')

    df = pd.DataFrame(transformed)
    output_path = os.path.expanduser("~/airflow/output/etl_output.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)
    print(f"Saved CSV to {output_path}")


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
    "etl_pipeline_dag",
    default_args=default_args,
    description="ETL pipeline example with extract, transform, load",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "example"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
        provide_context=True,
    )

    extract >> transform >> load