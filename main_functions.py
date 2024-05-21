import os
import pandas as pd
import psycopg2
from google.cloud import storage
import gcsfs
import re
import json
from google.cloud import bigquery

# Database and cloud storage configuration using environment variables
DB_PARAMS = {
    'host': os.getenv('DB_HOST', 'default-host'),
    'dbname': os.getenv('DB_NAME', 'airflow'),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
    'port': os.getenv('DB_PORT', '5432')
}
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'default-bucket')

# Function to create table in PostgreSQL
def get_sql_create_table_statement(schema, table_name):
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        Customer_ID INTEGER,
        Age INTEGER,
        Gender VARCHAR(10),
        Item_Purchased VARCHAR(255),
        Category VARCHAR(50),
        Purchase_Amount_USD INTEGER,
        Location VARCHAR(100),
        Size VARCHAR(10),
        Color VARCHAR(50),
        Season VARCHAR(50),
        Review_Rating FLOAT,
        Subscription_Status VARCHAR(10),
        Payment_Method VARCHAR(20),
        Shipping_Type VARCHAR(20),
        Discount_Applied VARCHAR(10),
        Promo_Code_Used VARCHAR(10),
        Previous_Purchases INTEGER,
        Preferred_Payment_Method VARCHAR(20),
        Frequency_of_Purchases VARCHAR(20)
    );
    """

# Function to sanitize column names
def sanitize_column_name(name):
    return re.sub(r"[^\w\s]", "", name).replace(" ", "_")

# Upload CSV data to PostgreSQL
def upload_csv_to_postgresql(csv_file_path, schema, table_name):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        df = pd.read_csv(csv_file_path)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cursor.execute(get_sql_create_table_statement(schema, table_name))
        for _, row in df.iterrows():
            insert_sql = f"INSERT INTO {schema}.{table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))});"
            cursor.execute(insert_sql, tuple(row))
        conn.commit()
        print("Data successfully uploaded to PostgreSQL.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error uploading CSV to PostgreSQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# Function to extract data from PostgreSQL to Google Cloud Storage
def postgres_to_gcs(schema, table_name):
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        sql_query = f"SELECT * FROM {schema}.{table_name};"
        cursor.execute(sql_query)
        rows = cursor.fetchall()

        # Convert to DataFrame and export to CSV
        column_names = [desc[0] for desc in cursor.description]
        csv_content = pd.DataFrame(rows, columns=column_names).to_csv(index=False)

        # Upload CSV to GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(f"{schema}_{table_name}.csv")
        blob.upload_from_string(csv_content, content_type='text/csv')

        print(f"Data from {schema}.{table_name} successfully uploaded to GCS.")

    except Exception as e:
        print(f"Failed to extract and upload to GCS: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

def clean_and_process_data(gcs_path_of_raw, clean_operations, destination_gcs_path):
    try:
        # Initialize Google Cloud Storage File System
        fs = gcsfs.GCSFileSystem()

        # Read data from the specified GCS path
        df = pd.read_csv(f'gs://{gcs_path_of_raw}')

        # Apply cleaning operations
        if 'drop_duplicates' in clean_operations:
            df.drop_duplicates(inplace=True)

        if 'fillna' in clean_operations:
            df.fillna(clean_operations['fillna'], inplace=True)

        # Save the cleaned data to a Parquet file in GCS
        df.to_parquet(f'gs://{destination_gcs_path}', index=False)
        print(f"Data cleaned and saved to {destination_gcs_path}")

    except Exception as e:
        # Handle exceptions and log the error message
        print(f"Failed to clean and process data: {e}")
        raise  # Re-raise the exception to signal task failure in Airflow
# Function to load data from Google Cloud Storage to BigQuery

# Function to load data from Google Cloud Storage to BigQuery
def gcs_to_bigquery(gcs_url, project_id, destination):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{destination}"

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    uri = f"gs://{gcs_url}"

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    print(f'Data loaded into BigQuery table {table_id}')


