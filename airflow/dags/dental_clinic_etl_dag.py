from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import subprocess
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=3),
    'start_date': days_ago(1),
}

# Dental Clinic ETL DAG
dental_clinic_etl_dag = DAG(
    dag_id='dental_clinic_etl',
    default_args=default_args,
    description='Daily ETL from OpenDental MySQL to PostgreSQL Analytics',
    schedule_interval='0 1 * * *',  # 1 AM UTC daily
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'dental-clinic', 'daily', 'mysql-to-postgresql'],
)

def run_etl_pipeline(**context):
    """
    Run the ETL pipeline using the new CLI.
    """
    try:
        execution_date = context['execution_date']
        logger.info(f"Starting ETL run for execution date: {execution_date}")
        
        # Get the project root directory
        project_root = "/opt/airflow/dags/dbt_dental_clinic"  # Adjust path as needed
        
        # Run ETL using the new CLI
        cmd = [
            'python', 'run_etl.py', 'run',
            '--config', 'etl_pipeline/etl_pipeline/config/pipeline.yaml',
            '--parallel', '4'
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10800  # 3 hour timeout
        )
        
        logger.info("ETL completed successfully")
        logger.info(f"ETL Output: {result.stdout}")
        
        if result.stderr:
            logger.warning(f"ETL Warnings: {result.stderr}")
            
        return "ETL_SUCCESS"
            
    except subprocess.TimeoutExpired:
        error_msg = "ETL process timed out after 3 hours"
        logger.error(error_msg)
        raise Exception(error_msg)
    except subprocess.CalledProcessError as e:
        error_msg = f"ETL process failed with return code {e.returncode}"
        logger.error(error_msg)
        logger.error(f"Error output: {e.stderr}")
        raise Exception(f"{error_msg}: {e.stderr}")
    except Exception as e:
        error_msg = f"Unexpected error during ETL: {str(e)}"
        logger.error(error_msg)
        raise

def check_pipeline_status(**context):
    """
    Check the status of the ETL pipeline.
    """
    try:
        execution_date = context['execution_date']
        logger.info(f"Checking pipeline status for: {execution_date}")
        
        project_root = "/opt/airflow/dags/dbt_dental_clinic"
        
        cmd = [
            'python', 'run_etl.py', 'status',
            '--config', 'etl_pipeline/etl_pipeline/config/pipeline.yaml',
            '--format', 'summary'
        ]
        
        result = subprocess.run(
            cmd,
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        logger.info("Pipeline status check completed")
        logger.info(f"Status Output: {result.stdout}")
        
        return "STATUS_CHECK_SUCCESS"
        
    except Exception as e:
        error_msg = f"Status check failed: {str(e)}"
        logger.error(error_msg)
        raise

def test_connections(**context):
    """
    Test all database connections before running ETL.
    """
    try:
        logger.info("Testing database connections")
        
        project_root = "/opt/airflow/dags/dbt_dental_clinic"
        
        cmd = ['python', 'run_etl.py', 'test-connections']
        
        result = subprocess.run(
            cmd,
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        logger.info("Connection test completed")
        logger.info(f"Connection test output: {result.stdout}")
        
        return "CONNECTION_TEST_SUCCESS"
        
    except Exception as e:
        error_msg = f"Connection test failed: {str(e)}"
        logger.error(error_msg)
        raise

# Define the DAG structure
with dental_clinic_etl_dag:
    
    # Start task
    start_etl = DummyOperator(
        task_id='start_etl',
    )
    
    # Test connections first
    test_connections_task = PythonOperator(
        task_id='test_connections',
        python_callable=test_connections,
        provide_context=True,
    )
    
    # Main ETL task
    run_etl = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl_pipeline,
        provide_context=True,
    )
    
    # Check status
    check_status = PythonOperator(
        task_id='check_pipeline_status',
        python_callable=check_pipeline_status,
        provide_context=True,
    )
    
    # End task
    end_etl = DummyOperator(
        task_id='end_etl',
    )
    
    # Set up task dependencies
    start_etl >> test_connections_task >> run_etl >> check_status >> end_etl


# Manual trigger DAG for testing
manual_etl_dag = DAG(
    dag_id='dental_clinic_manual_etl',
    default_args=default_args,
    description='Manual trigger for dental clinic ETL',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'dental-clinic', 'manual'],
)

with manual_etl_dag:
    manual_start = DummyOperator(task_id='manual_start')
    manual_test_connections = PythonOperator(
        task_id='manual_test_connections',
        python_callable=test_connections,
        provide_context=True,
    )
    manual_etl = PythonOperator(
        task_id='manual_etl_run',
        python_callable=run_etl_pipeline,
        provide_context=True,
    )
    manual_status = PythonOperator(
        task_id='manual_status_check',
        python_callable=check_pipeline_status,
        provide_context=True,
    )
    manual_end = DummyOperator(task_id='manual_end')
    
    manual_start >> manual_test_connections >> manual_etl >> manual_status >> manual_end 