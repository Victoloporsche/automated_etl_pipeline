from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging
from scripts.extract import save_to_gcs
from scripts.load_clean_data_to_bigquery import load_shopify_facebook_clean_data

# This defines the Airflow DAG for the ETL pipeline
with DAG(
        'intermate_etl_dag',
        default_args={
            'owner': 'airflow',
            'start_date': datetime(2025, 3, 16),
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval='@daily',  # batch aggregate to auto refresh daily
        catchup=False,
) as dag:
    # Step 1: extract the shopify and facebook data from csv, clean them and save to GSC
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=save_to_gcs,
    )

    # Step 2: Load the cleaned Raw data from GCS to BigQuery
    load_data_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_shopify_facebook_clean_data,
    )

    # Step 3: Run dbt transformations (staging and fact models)
    def run_dbt():
        try:
            dbt_project_dir = "./dbt_project"  # Update this path

            result = subprocess.run(
                ["dbt", "run"],
                check=True,
                capture_output=True,
                text=True,
                cwd=dbt_project_dir
            )
            logging.info(result.stdout)
            logging.info(result.stderr)
        except subprocess.CalledProcessError as e:
            logging.error(f"DBT Run failed: {e.stderr}")
            raise e


    run_dbt_task = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt,
    )

    # Task Dependencies
    extract_data_task >> load_data_task >> run_dbt_task
