from builtins import range
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
import json

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='test_s3',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example']
)


def load_to_s3():
    s3 = S3Hook('docplanner_aws')
    s3_bucket = "test"
    if not s3.check_for_bucket(s3_bucket):
        s3.create_bucket("test")
    # One JSON Object Per Line
    data = [("a", 1), ("b", 2), ("c", 3)]
    data = [json.dumps(d, ensure_ascii=False ) for d in data]
    data = '\n'.join(data)
    s3.load_string(data, "test.json", bucket_name=s3_bucket, replace=True)


run_this_last = PythonOperator(
    task_id='test_s3',
    dag=dag,
    python_callable=load_to_s3
)


if __name__ == "__main__":
    dag.cli()