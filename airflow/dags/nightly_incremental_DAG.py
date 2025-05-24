from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import subprocess
import logging

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
    'execution_timeout': timedelta(hours=3),  # Should be plenty for 3.7GB
    'start_date': days_ago(1),
}

# Single nightly DAG for all tables
nightly_etl_dag = DAG(
    dag_id='dental_clinic_nightly_etl',
    default_args=default_args,
    description='Nightly incremental ETL from OpenDental MySQL to PostgreSQL Analytics',
    schedule_interval='0 1 * * *',  # 1 AM UTC = 8 PM Central (during standard time)
    # Note: Use '0 2 * * *' during daylight saving time (8 PM CDT)
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'dental-clinic', 'nightly', 'mysql-to-postgresql'],
)

def run_full_etl(**context):
    """
    Run incremental ETL for all 432 tables.
    With auto-discovery, this will process all tables in the OpenDental database.
    """
    try:
        execution_date = context['execution_date']
        logger.info(f"Starting nightly ETL run for execution date: {execution_date}")
        
        # Run ETL script without specifying tables (processes all discovered tables)
        result = subprocess.run(
            ['python', '/opt/airflow/dags/etl_job/mysql_postgre_incremental.py'],
            check=True,
            capture_output=True,
            text=True,
            timeout=10800  # 3 hour timeout
        )
        
        logger.info("Nightly ETL completed successfully")
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

def validate_etl_completion(**context):
    """
    Validate that the ETL completed successfully by checking sync status.
    """
    try:
        import sys
        sys.path.append('/opt/airflow/dags/etl_job')
        from connection_factory import get_target_connection
        from sqlalchemy import text
        
        with get_target_connection().connect() as conn:
            # Check how many tables were synced today
            today = context['execution_date'].date()
            
            sync_check_query = text("""
                SELECT 
                    COUNT(*) as tables_synced,
                    SUM(rows_processed) as total_rows,
                    COUNT(CASE WHEN sync_status = 'success' THEN 1 END) as successful_syncs,
                    COUNT(CASE WHEN sync_status != 'success' THEN 1 END) as failed_syncs
                FROM etl_sync_status 
                WHERE DATE(last_modified) = :today
            """)
            
            result = conn.execute(sync_check_query.bindparams(today=today)).fetchone()
            
            if result:
                tables_synced = result[0]
                total_rows = result[1] or 0
                successful_syncs = result[2]
                failed_syncs = result[3]
                
                logger.info(f"ETL Validation Results:")
                logger.info(f"  Tables processed: {tables_synced}")
                logger.info(f"  Successful syncs: {successful_syncs}")
                logger.info(f"  Failed syncs: {failed_syncs}")
                logger.info(f"  Total rows processed: {total_rows:,}")
                
                if failed_syncs > 0:
                    # Get details of failed syncs
                    failed_query = text("""
                        SELECT table_name, sync_status, error_message
                        FROM etl_sync_status 
                        WHERE DATE(last_modified) = :today
                        AND sync_status != 'success'
                    """)
                    failed_results = conn.execute(failed_query.bindparams(today=today)).fetchall()
                    
                    failed_tables = [f"{row[0]} ({row[1]})" for row in failed_results]
                    logger.warning(f"Failed table syncs: {', '.join(failed_tables)}")
                    
                    # Don't fail the task for a few failed tables, but log warnings
                    if failed_syncs > 10:  # Fail if more than 10 tables failed
                        raise Exception(f"Too many failed syncs: {failed_syncs} tables failed")
                
                return {
                    'tables_synced': tables_synced,
                    'successful_syncs': successful_syncs,
                    'failed_syncs': failed_syncs,
                    'total_rows': total_rows
                }
            else:
                raise Exception("No sync records found for today")
                
    except Exception as e:
        logger.error(f"ETL validation failed: {str(e)}")
        raise

# Define the DAG structure
with nightly_etl_dag:
    
    # Start task
    start_etl = DummyOperator(
        task_id='start_nightly_etl',
    )
    
    # Main ETL task
    run_etl = PythonOperator(
        task_id='run_incremental_etl',
        python_callable=run_full_etl,
        provide_context=True,
    )
    
    # Validation task
    validate_etl = PythonOperator(
        task_id='validate_etl_completion',
        python_callable=validate_etl_completion,
        provide_context=True,
    )
    
    # End task
    end_etl = DummyOperator(
        task_id='end_nightly_etl',
    )
    
    # Set up task dependencies
    start_etl >> run_etl >> validate_etl >> end_etl


# Optional: Create a manual trigger DAG for ad-hoc runs
manual_etl_dag = DAG(
    dag_id='dental_clinic_manual_etl',
    default_args=default_args,
    description='Manual trigger for dental clinic ETL (for testing or catch-up runs)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'dental-clinic', 'manual', 'mysql-to-postgresql'],
)

with manual_etl_dag:
    manual_start = DummyOperator(task_id='manual_start')
    manual_etl = PythonOperator(
        task_id='manual_etl_run',
        python_callable=run_full_etl,
        provide_context=True,
    )
    manual_validate = PythonOperator(
        task_id='manual_validate',
        python_callable=validate_etl_completion,
        provide_context=True,
    )
    manual_end = DummyOperator(task_id='manual_end')
    
    manual_start >> manual_etl >> manual_validate >> manual_end