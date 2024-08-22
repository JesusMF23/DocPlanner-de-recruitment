from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='test_postgres',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example']
)


def check_db():
    psql = PostgresHook('docplanner_dwh')

    data = psql.get_records("SELECT 1")
    print(f"READ TEST {data}")

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.test_table
    (
       id             integer        GENERATED ALWAYS AS IDENTITY,
       name       varchar(50)
    );
    """, autocommit=True
    )
    psql.run("INSERT INTO public.test_table (name) VALUES ('a')", autocommit=True);


task = PythonOperator(
    task_id='test_postgres',
    dag=dag,
    python_callable=check_db
)


if __name__ == "__main__":
    dag.cli()