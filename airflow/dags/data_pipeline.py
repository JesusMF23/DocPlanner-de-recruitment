import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import polars as pl
from io import BytesIO
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'climate_data_ingestion',
    default_args=default_args,
    description='Ingest climate data from the GHCND API and store in LocalStack S3',
    schedule_interval="@daily",
    catchup=False
)

def fetch_climate_data(**kwargs):
    logging.info("Starting the fetch_climate_data task")
    
    try:
        execution_date = kwargs.get('execution_date', datetime.now())
        start_date = kwargs.get('start_date', (execution_date - timedelta(days=1)).strftime('%Y-%m-%d'))
        end_date = kwargs.get('end_date', (execution_date - timedelta(days=1)).strftime('%Y-%m-%d'))

        logging.info(f"Using start_date: {start_date} and end_date: {end_date}")

        api_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data'
        api_token = os.environ['API_TOKEN']
        headers = {'token': api_token}
        params = {
            'datasetid': 'GHCND',
            'startdate': start_date,
            'enddate': end_date,
            'limit': 1000
        }

        logging.info(f"Making API request to {api_url} with params: {params}")
        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code != 200:
            error_message = f"Failed API request with status code: {response.status_code} and response: {response.text}"
            logging.error(error_message)
            raise AirflowException(error_message)

        logging.info("API request successful, processing the data")
        data = response.json()

        if len(data['results']) == 0:
            error_message = "API returned an empty dataset"
            logging.error(error_message)
            raise AirflowException(error_message)

        s3 = S3Hook(aws_conn_id='docplanner_aws')
        s3_bucket = 'raw-climate-data'
        raw_s3_key = f"raw_data_{start_date}_to_{end_date}.json"
        
        logging.info(f"Uploading JSON to S3 bucket: {s3_bucket} with key: {raw_s3_key}")
        s3.load_string(json.dumps(data), key=raw_s3_key, bucket_name=s3_bucket)
        logging.info("JSON successfully uploaded to S3")

        kwargs['ti'].xcom_push(key='raw_s3_key', value=raw_s3_key)
        logging.info(f"S3 key {raw_s3_key} pushed to XCom for the next task")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise AirflowException(f"Task failed due to error: {str(e)}")

def process_climate_data(**kwargs):
    raw_s3_key = kwargs['ti'].xcom_pull(key='raw_s3_key', task_ids='fetch_climate_data')

    logging.info(f"Starting the process_climate_data task with S3 key: {raw_s3_key}")
    
    try:
        s3 = S3Hook(aws_conn_id='docplanner_aws')
        raw_s3_bucket = 'raw-climate-data'
        staging_s3_bucket = "staging-climate-data"

        logging.info(f"Downloading JSON from S3 bucket: {raw_s3_bucket} with key: {raw_s3_key}")
        json_content = s3.read_key(key=raw_s3_key, bucket_name=raw_s3_bucket)
        data = json.loads(json_content)

        df = pl.DataFrame(data['results'])

        df = df.drop_nulls(subset=['station', 'date', 'value', 'datatype'])

        df = df.with_columns([
            pl.col('date').str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S"),
            pl.col('value').cast(pl.Float64, strict=False)
        ])

        df_pivoted = df.pivot(
            values="value",
            index=["station", "date"],
            columns="datatype"
        )

        # little cleaning not to be so restrictive, this must be updated with needs for the use case
        df_pivoted = df_pivoted.filter(
            pl.col("TAVG").is_not_null() |
            pl.col("TMIN").is_not_null() |
            pl.col("TMAX").is_not_null() |
            pl.col("PRCP").is_not_null()
        )

        staging_parquet_buffer = BytesIO()
        df_pivoted.write_parquet(staging_parquet_buffer)
        staging_parquet_buffer.seek(0)

        staging_s3_key = raw_s3_key.replace('raw_data', 'staging').replace('.json', '.parquet')
        logging.info(f"Uploading staging Parquet to S3 bucket: {staging_s3_bucket} with key: {staging_s3_key}")
        s3.load_bytes(staging_parquet_buffer.getvalue(), key=staging_s3_key, bucket_name=staging_s3_bucket)

        kwargs['ti'].xcom_push(key='staging_s3_key', value=staging_s3_key)
        logging.info(f"Staging S3 key {staging_s3_key} pushed to XCom for the next task")

    except Exception as e:
        logging.error(f"An error occurred in process_climate_data: {str(e)}")
        raise
def load_data_to_postgres(**kwargs):
    staging_s3_key = kwargs['ti'].xcom_pull(key='staging_s3_key', task_ids='process_climate_data')

    logging.info(f"Starting the load_data_to_postgres task with S3 key: {staging_s3_key}")
    
    try:
        s3 = S3Hook(aws_conn_id='docplanner_aws')
        staging_s3_bucket = "staging-climate-data"

        logging.info(f"Downloading processed Parquet from S3 bucket: {staging_s3_bucket} with key: {staging_s3_key}")
        parquet_content = s3.get_key(key=staging_s3_key, bucket_name=staging_s3_bucket).get()["Body"].read()

        df = pl.read_parquet(BytesIO(parquet_content))

        pg_hook = PostgresHook(postgres_conn_id='docplanner_dwh')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # based on the cleaning and table defined, this must also need to be refined depending on the use case
        logging.info("Inserting data into the weather_data table in Postgres")
        insert_query = """
            INSERT INTO weather_data (station_id, date, tavg, tmin, tmax, prcp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for row in df.iter_rows(named=True):
            cursor.execute(insert_query, (
                row['station'], 
                row['date'], 
                row.get('TAVG', None), 
                row.get('TMIN', None), 
                row.get('TMAX', None), 
                row.get('PRCP', None)
            ))

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data successfully loaded into Postgres")

    except Exception as e:
        logging.error(f"An error occurred in load_data_to_postgres: {str(e)}")
        raise

fetch_data_task = PythonOperator(
    task_id='fetch_climate_data',
    python_callable=fetch_climate_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_climate_data',
    python_callable=process_climate_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

fetch_data_task >> process_data_task >> load_data_task