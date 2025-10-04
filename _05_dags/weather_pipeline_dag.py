from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

from soda.scan import Scan
from _01_extraction.fetch_weather import fetch_weather_multiple, cities
from _02_transform.transform_weather import transform_weather
from _03_load.load_to_postgres import load_weather_to_postgres

# --- Paths for Soda ---
SODA_YML_PATH = r"./_04_dq_checks/weather_checks.yml"
REPORT_DIR = r"./_06_Soda_reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# --- Default DAG args ---
default_args = {
    'owner': 'jeyashree',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Weather ETL Pipeline',
    schedule_interval='@hourly',  # every hour
    start_date=datetime(2025, 9, 20),
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    # --- Extract Task ---
    def extract_callable(**kwargs):
        data = fetch_weather_multiple(cities)
        return data  # push to XCom

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_callable,
        provide_context=True,
    )

    # --- Transform Task ---
    def transform_callable(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(task_ids='extract_weather')
        transformed = [transform_weather(d) for d in extracted_data]
        return transformed

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_callable,
        provide_context=True,
    )

    # --- Load Task ---
    def load_callable(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_weather')
        for record in transformed_data:
            load_weather_to_postgres(record)

        # --- Run Soda check after load ---
        scan = Scan()
        scan.set_scan_config_file(SODA_YML_PATH)
        scan.set_scan_name("weather_pipeline_soda")
        scan.set_output_path(REPORT_DIR)
        scan.execute()

    load_task = PythonOperator(
        task_id='load_weather_to_postgres',
        python_callable=load_callable,
        provide_context=True,
    )

    # --- Task Dependencies ---
    extract_task >> transform_task >> load_task
