from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Import ETL functions
from 01_extraction.fetch_weather import fetch_weather_multiple, cities
from 02_transform.transform_weather import transform_weather
from 03_load.load_to_postgres import load_weather_to_postgres

# Soda Core DQ checks
from soda.scan import Scan

default_args = {
    'owner': 'jeyashree',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Directory to save Soda reports
REPORT_DIR = r"C:\Users\BJeyshree\Dataops_framework\06_Soda_reports"
os.makedirs(REPORT_DIR, exist_ok=True)

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Weather ETL Pipeline with Soda DQ Reports',
    schedule_interval='@hourly',
    start_date=datetime(2025, 9, 20),
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    # -------------------------
    # Soda check function with reporting
    # -------------------------
    def run_soda_checks(checks_file, report_name):
        """
        Run Soda Core checks and save JSON + HTML reports.
        """
        scan = Scan()
        scan.add_scan_yaml_file(checks_file)

        # Execute scan
        result = scan.execute()
        print(f"Soda scan result: {result.status}")
        for check in result.check_results:
            print(f"{check.check_name}: {check.status}")

        # Save reports
        json_path = os.path.join(REPORT_DIR, f"{report_name}.json")
        html_path = os.path.join(REPORT_DIR, f"{report_name}.html")
        scan.execute(output_format="json", output_path=json_path)
        scan.execute(output_format="html", output_path=html_path)

        if result.status != "PASS":
            raise ValueError(f"Soda Core checks failed for {report_name}!")

    # -------------------------
    # ETL Functions
    # -------------------------
    def extract_callable(**kwargs):
        data = fetch_weather_multiple(cities)
        return data

    def transform_callable(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(task_ids='extract_weather')
        transformed = [transform_weather(d) for d in extracted_data]
        return transformed

    def load_callable(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids='transform_weather')
        for record in transformed_data:
            load_weather_to_postgres(record)

    # -------------------------
    # Airflow Tasks
    # -------------------------
    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_callable,
        provide_context=True,
    )

    soda_check_raw = PythonOperator(
        task_id='soda_check_raw',
        python_callable=lambda: run_soda_checks(
            r"C:\Users\BJeyshree\Dataops_framework\04_dq_checks\weather_checks.yml",
            report_name=f"soda_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_callable,
        provide_context=True,
    )

    soda_check_processed = PythonOperator(
        task_id='soda_check_processed',
        python_callable=lambda: run_soda_checks(
            r"C:\Users\BJeyshree\Dataops_framework\04_dq_checks\weather_checks.yml",
            report_name=f"soda_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
    )

    load_task = PythonOperator(
        task_id='load_weather_to_postgres',
        python_callable=load_callable,
        provide_context=True,
    )

    # -------------------------
    # Task Dependencies
    # -------------------------
    extract_task >> soda_check_raw >> transform_task >> soda_check_processed >> load_task
