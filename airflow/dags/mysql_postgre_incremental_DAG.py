from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
import sys
import os

# Add the ETL job directory to Python path
sys.path.append('/opt/airflow/dags/etl_job')

# Import your ETL functions directly
from mysql_postgre_incremental import sync_table_incremental, TABLE_CONFIGS
from connection_factory import test_connections

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define table categories with dental clinic priorities
HIGH_FREQUENCY_TABLES = [
    'appointment',      # Critical: scheduling changes frequently
    'procedurelog',     # Critical: procedures completed throughout day
    'payment',          # Critical: payments processed continuously
    'paysplit',         # Critical: payments split
    'claim',           # Important: insurance claims status changes
    'commlog',         # Important: communication logs
    'entrylog',        # Important: audit trail
    'recall',          # Important: patient recall status
]

MEDIUM_FREQUENCY_TABLES = [
    'patient',         # Core: patient demographics change occasionally
    'insplan',         # Important: insurance plans change monthly/quarterly
    'provider',        # Stable: provider info changes infrequently
    'feesched',        # Stable: fee schedules change periodically
    'treatment',       # Important: treatment plans updated regularly
    'family',          # Core: family relationships
]

LOW_FREQUENCY_TABLES = [
    'definition',      # Static: procedure definitions
    'preference',      # Static: system preferences
    'securitylog',     # Archive: security events
    'userod',          # Static: user definitions
    'groupname',       # Static: user groups
]

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'start_date': days_ago(1),
}

def test_database_connections():
    """Test both source and target connections before ETL."""
    try:
        if test_connections():
            logger.info("All database connections verified successfully")
            return True
        else:
            raise Exception("Database connection test failed")
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise

def sync_table_with_monitoring(table_name: str, **context):
    """Sync table with enhanced monitoring and error handling."""
    try:
        # Log execution context
        execution_date = context['execution_date']
        task_instance = context['task_instance']
        
        logger.info(f"Starting ETL for table: {table_name}")
        logger.info(f"Execution date: {execution_date}")
        
        # Check if table exists in our configurations
        if table_name not in TABLE_CONFIGS:
            logger.warning(f"Table {table_name} not found in configurations. Available tables: {list(TABLE_CONFIGS.keys())}")
            return False
        
        # Perform the sync
        success = sync_table_incremental(table_name)
        
        if success:
            logger.info(f"Successfully synced table: {table_name}")
            
            # Push metrics to XCom for monitoring
            task_instance.xcom_push(key=f'{table_name}_status', value='success')
            task_instance.xcom_push(key=f'{table_name}_sync_time', value=str(datetime.now()))
            
            return True
        else:
            logger.error(f"Failed to sync table: {table_name}")
            raise Exception(f"ETL failed for table {table_name}")
            
    except Exception as e:
        logger.error(f"Error syncing table {table_name}: {str(e)}")
        # Push failure status to XCom
        context['task_instance'].xcom_push(key=f'{table_name}_status', value='failed')
        raise

def validate_etl_completion(tables_list: list, **context):
    """Validate that all tables in the group were processed successfully."""
    task_instance = context['task_instance']
    failed_tables = []
    
    for table in tables_list:
        status = task_instance.xcom_pull(key=f'{table}_status', task_ids=f'sync_{table}')
        if status != 'success':
            failed_tables.append(table)
    
    if failed_tables:
        raise Exception(f"ETL validation failed for tables: {failed_tables}")
    
    logger.info(f"All tables processed successfully: {tables_list}")

# Create separate DAGs for different frequencies
def create_etl_dag(dag_id: str, schedule_interval: str, description: str, tables: list):
    """Create ETL DAG with proper task dependencies and monitoring."""
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,
        tags=['etl', 'dental-clinic', 'incremental'],
    )
    
    with dag:
        # Connection test task
        test_conn_task = PythonOperator(
            task_id='test_connections',
            python_callable=test_database_connections,
            retries=1,
        )
        
        # Start marker
        start_task = DummyOperator(task_id='start_etl')
        
        # Create ETL tasks for each table
        etl_tasks = []
        for table in tables:
            if table in TABLE_CONFIGS:  # Only create tasks for configured tables
                task = PythonOperator(
                    task_id=f'sync_{table}',
                    python_callable=sync_table_with_monitoring,
                    op_kwargs={'table_name': table},
                    provide_context=True,
                )
                etl_tasks.append(task)
            else:
                logger.warning(f"Skipping task creation for unconfigured table: {table}")
        
        # Validation task
        validate_task = PythonOperator(
            task_id='validate_completion',
            python_callable=validate_etl_completion,
            op_kwargs={'tables_list': [t for t in tables if t in TABLE_CONFIGS]},
            provide_context=True,
        )
        
        # End marker
        end_task = DummyOperator(task_id='end_etl')
        
        # Define dependencies
        test_conn_task >> start_task >> etl_tasks >> validate_task >> end_task
    
    return dag

# Create the three DAGs
high_freq_dag = create_etl_dag(
    dag_id='dental_etl_high_frequency',
    schedule_interval='0 */4 * * *',  # Every 4 hours (more realistic for dental clinics)
    description='High-frequency ETL for operational dental clinic data',
    tables=HIGH_FREQUENCY_TABLES
)

medium_freq_dag = create_etl_dag(
    dag_id='dental_etl_medium_frequency', 
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    description='Medium-frequency ETL for patient and administrative data',
    tables=MEDIUM_FREQUENCY_TABLES
)

low_freq_dag = create_etl_dag(
    dag_id='dental_etl_low_frequency',
    schedule_interval='0 3 * * 0',  # Weekly on Sunday at 3 AM
    description='Low-frequency ETL for configuration and reference data',
    tables=LOW_FREQUENCY_TABLES
)

# Optional: Create a sensor to ensure high-freq DAG completes before medium-freq
# This prevents resource conflicts during business hours
medium_freq_sensor = ExternalTaskSensor(
    task_id='wait_for_high_freq_completion',
    external_dag_id='dental_etl_high_frequency',
    external_task_id='end_etl',
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    timeout=300,  # 5 minutes timeout
    dag=medium_freq_dag,
)

# Add the sensor to medium frequency DAG dependency chain
with medium_freq_dag:
    test_conn = medium_freq_dag.get_task('test_connections')
    start_etl = medium_freq_dag.get_task('start_etl')
    
    # Modify dependency: sensor should run after connection test but before start
    test_conn >> medium_freq_sensor >> start_etl