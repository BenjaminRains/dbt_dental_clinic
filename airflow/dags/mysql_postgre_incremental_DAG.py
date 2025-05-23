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

# Define table categories based on refresh frequency
HIGH_FREQUENCY_TABLES = [
    'appointment',
    'procedurelog',
    'payment',
    'claim',
    'commlog',
    'entrylog'
]

MEDIUM_FREQUENCY_TABLES = [
    'patient',
    'insplan',
    'provider',
    'feesched'
]

LOW_FREQUENCY_TABLES = [
    'definition',
    'preference',
    'securitylog'
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create separate DAGs for different frequencies
def create_dag(dag_id, schedule_interval, description):
    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,
    )

# High frequency DAG (every 6 hours)
high_freq_dag = create_dag(
    'mysql_to_postgres_high_freq',
    '0 */6 * * *',  # Every 6 hours
    'ETL job for high-frequency tables'
)

# Medium frequency DAG (weekly)
medium_freq_dag = create_dag(
    'mysql_to_postgres_medium_freq',
    '0 2 * * 0',  # Every Sunday at 2 AM
    'ETL job for medium-frequency tables'
)

# Low frequency DAG (monthly)
low_freq_dag = create_dag(
    'mysql_to_postgres_low_freq',
    '0 3 1 * *',  # First day of every month at 3 AM
    'ETL job for low-frequency tables'
)

def run_etl_for_tables(tables):
    """Run ETL for specific tables with proper error handling and logging."""
    try:
        # Convert tables list to space-separated string
        tables_str = ' '.join(tables)
        
        # Run ETL script with specific tables
        result = subprocess.run(
            ['python', 'etl_job/mysql_postgre_incremental.py', '--tables'] + tables,
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"ETL completed successfully for tables: {tables_str}")
        logger.info(f"Output: {result.stdout}")
        
        if result.stderr:
            logger.warning(f"Warnings/Errors: {result.stderr}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"ETL failed for tables {tables_str}: {str(e)}")
        logger.error(f"Error output: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

# Create tasks for each frequency
def create_etl_tasks(dag, tables, task_prefix):
    start = DummyOperator(task_id=f'{task_prefix}_start', dag=dag)
    end = DummyOperator(task_id=f'{task_prefix}_end', dag=dag)
    
    # Create a task for each table
    tasks = []
    for table in tables:
        task = PythonOperator(
            task_id=f'{task_prefix}_{table}',
            python_callable=run_etl_for_tables,
            op_kwargs={'tables': [table]},
            dag=dag,
        )
        tasks.append(task)
    
    # Set up dependencies
    start >> tasks >> end
    return start, end

# Create tasks for each DAG
high_freq_start, high_freq_end = create_etl_tasks(
    high_freq_dag, 
    HIGH_FREQUENCY_TABLES, 
    'high_freq'
)

medium_freq_start, medium_freq_end = create_etl_tasks(
    medium_freq_dag, 
    MEDIUM_FREQUENCY_TABLES, 
    'medium_freq'
)

low_freq_start, low_freq_end = create_etl_tasks(
    low_freq_dag, 
    LOW_FREQUENCY_TABLES, 
    'low_freq'
)