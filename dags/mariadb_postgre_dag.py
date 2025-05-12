from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mariadb_to_postgres_etl',
    default_args=default_args,
    description='ETL job to sync MariaDB to PostgreSQL',
    schedule_interval=timedelta(days=7),  # Run weekly on Sunday only. Enforce this in the DAG.
)

def run_etl_script():
    # Run your ETL script
    subprocess.run(['python', 'etl_job/mariadb_postgre_pipe.py'], check=True)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl_script,
    dag=dag,
)