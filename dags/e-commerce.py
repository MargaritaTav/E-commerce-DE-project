from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Import your custom functions for the DAG tasks
from main_functions import (
    upload_csv_to_postgresql,
    postgres_to_gcs,
    clean_and_process_data,
    gcs_to_bigquery
)

# Default DAG task parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG setup
dag = DAG(
    'ETL_Process_for_Shopping_Data',
    default_args=default_args,
    description='ETL process for shopping data',
    schedule_interval=None,  # Set to None for manual triggering
    catchup=False,
)

# Task to upload a CSV file to PostgreSQL
upload_csv_to_postgresql_task = PythonOperator(
    task_id='upload_csv_to_postgresql',
    python_callable=upload_csv_to_postgresql,
    op_kwargs={
        'csv_file_path': os.getenv('CSV_FILE_PATH', '/opt/airflow/data/default.csv'),
        'schema': os.getenv('DB_SCHEMA', 'default_schema'),
        'table_name': os.getenv('DB_TABLE', 'default_table')
    },
    dag=dag,
)

# Task to extract data from PostgreSQL to Google Cloud Storage (GCS)
postgres_to_gcs_task = PythonOperator(
    task_id='postgres_to_gcs',
    python_callable=postgres_to_gcs,
    op_kwargs={
        'schema': os.getenv('DB_SCHEMA', 'default_schema'),
        'table_name': os.getenv('DB_TABLE', 'default_table')
    },
    dag=dag,
)

clean_and_process_data_task = PythonOperator(
    task_id='clean_and_process_data',
    python_callable=clean_and_process_data,
    op_kwargs={
        'gcs_path_of_raw': os.getenv('GCS_PATH_OF_RAW', 'gs://default-bucket/raw_data.csv'),
        'destination_gcs_path': os.getenv('GCS_CLEANED_PATH', 'gs://default-bucket/cleaned_data.parquet'),
        'clean_operations': {'drop_duplicates': True, 'fillna': {'Review_Rating': 0}},
    },
    dag=dag,
)

gcs_to_bigquery_task = PythonOperator(
    task_id='gcs_to_bigquery',
    python_callable=gcs_to_bigquery,
    op_kwargs={
        'gcs_url': os.getenv('GCS_CLEANED_URL', 'gs://default-bucket/cleaned_data.parquet'),
        'project_id': os.getenv('GCP_PROJECT_ID', 'default-project-id'),
        'destination': os.getenv('BIGQUERY_DEST_TABLE', 'default_dataset.default_table')
    },
    dag=dag,
)

# Task dependencies for DAG
upload_csv_to_postgresql_task >> postgres_to_gcs_task >> clean_and_process_data_task >> gcs_to_bigquery_task
