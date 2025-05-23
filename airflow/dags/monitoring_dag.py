from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add monitoring utilities
sys.path.append('/opt/airflow/dags/etl_job')
from monitoring_utils import check_etl_health, generate_etl_summary_report, create_monitoring_tables

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': days_ago(1),
}

# Health monitoring DAG
health_dag = DAG(
    dag_id='dental_etl_health_monitoring',
    default_args=default_args,
    description='Monitor ETL pipeline health and generate reports',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'dental-clinic', 'etl'],
)

with health_dag:
    # Initialize monitoring tables
    init_monitoring = PythonOperator(
        task_id='initialize_monitoring_tables',
        python_callable=create_monitoring_tables,
    )
    
    # Run health checks
    health_check = PythonOperator(
        task_id='check_etl_health',
        python_callable=check_etl_health,
    )
    
    init_monitoring >> health_check

# Daily summary report DAG
summary_dag = DAG(
    dag_id='dental_etl_daily_summary',
    default_args=default_args,
    description='Generate daily ETL summary reports',
    schedule_interval='0 8 * * *',  # Every day at 8 AM
    catchup=False,
    max_active_runs=1,
    tags=['reporting', 'dental-clinic', 'etl'],
)

with summary_dag:
    generate_summary = PythonOperator(
        task_id='generate_daily_summary',
        python_callable=generate_etl_summary_report,
        provide_context=True,
    )