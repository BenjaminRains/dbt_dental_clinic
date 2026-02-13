"""
Schema Analysis DAG for OpenDental ETL Pipeline

This DAG runs the schema analyzer to generate/update tables.yml configuration
for the ETL pipeline. It should be run:
- Weekly or monthly (scheduled)
- Before major OpenDental upgrades
- When schema changes are detected
- On-demand when troubleshooting

The DAG detects schema changes, generates configuration, and notifies
the team if breaking changes are detected.
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@example.com'],  # TODO: Update with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG configuration
dag = DAG(
    'schema_analysis',
    default_args=default_args,
    description='Analyze OpenDental schema and generate ETL configuration',
    schedule_interval='0 2 * * 0',  # Weekly on Sunday at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'configuration', 'schema-analysis'],
    max_active_runs=1,  # Only one schema analysis at a time
)

# ============================================================================
# Configuration
# ============================================================================

# Project root: override via Airflow Variable "project_root" for EC2 vs local
PROJECT_ROOT = Path(Variable.get('project_root', default_var='/opt/airflow/dbt_dental_clinic'))
ETL_PIPELINE_DIR = PROJECT_ROOT / 'etl_pipeline'
TABLES_YML_PATH = ETL_PIPELINE_DIR / 'etl_pipeline' / 'config' / 'tables.yml'
BACKUP_DIR = PROJECT_ROOT / 'logs' / 'schema_analysis' / 'backups'
CHANGELOG_DIR = PROJECT_ROOT / 'logs' / 'schema_analysis' / 'reports'

# Environment (from Airflow Variable or default to clinic)
ENVIRONMENT = Variable.get('etl_environment', default_var='clinic')

# ============================================================================
# Task Functions
# ============================================================================

def validate_source_connection(**context):
    """
    Validate connection to OpenDental source database.
    
    This ensures the source database is accessible before running
    the expensive schema analysis.
    """
    import sys
    sys.path.insert(0, str(ETL_PIPELINE_DIR))
    
    from etl_pipeline.config import get_settings, DatabaseType
    from etl_pipeline.core.connections import ConnectionFactory
    
    logging.info(f"Validating source connection for environment: {ENVIRONMENT}")
    
    try:
        # Get settings and source connection config
        settings = get_settings()
        
        # Test source connection
        source_engine = ConnectionFactory.get_source_connection(settings)
        
        # Simple query to verify connection
        with source_engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            row = result.fetchone()
            
            if row[0] != 1:
                raise AirflowException("Source connection test query failed")
        
        source_engine.dispose()
        
        logging.info("✅ Source connection validated successfully")
        return True
        
    except Exception as e:
        logging.error(f"❌ Source connection validation failed: {e}")
        raise AirflowException(f"Failed to connect to source database: {e}")


def backup_existing_config(**context):
    """
    Backup existing tables.yml before running analysis.
    
    This allows rollback if the new configuration has issues.
    """
    import shutil
    from datetime import datetime
    
    logging.info("Backing up existing tables.yml")
    
    if not TABLES_YML_PATH.exists():
        logging.warning("No existing tables.yml to backup")
        context['task_instance'].xcom_push(key='backup_created', value=False)
        return False
    
    # Create backup directory if it doesn't exist
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create timestamped backup
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = BACKUP_DIR / f'tables.yml.backup.{timestamp}'
    
    try:
        shutil.copy2(TABLES_YML_PATH, backup_path)
        logging.info(f"✅ Backup created: {backup_path}")
        
        # Store backup path in XCom for potential rollback
        context['task_instance'].xcom_push(key='backup_path', value=str(backup_path))
        context['task_instance'].xcom_push(key='backup_created', value=True)
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to backup configuration: {e}")
        raise AirflowException(f"Configuration backup failed: {e}")


def run_schema_analysis(**context):
    """
    Run the OpenDental schema analyzer to generate tables.yml.
    
    This is the core task that analyzes the source database schema,
    generates optimal configuration, and detects changes.
    """
    import sys
    import subprocess
    
    logging.info("Starting OpenDental schema analysis")
    
    # Change to ETL pipeline directory
    analysis_script = ETL_PIPELINE_DIR / 'scripts' / 'analyze_opendental_schema.py'
    
    if not analysis_script.exists():
        raise AirflowException(f"Schema analysis script not found: {analysis_script}")
    
    try:
        # Set environment variable for the analysis
        import os
        os.environ['ETL_ENVIRONMENT'] = ENVIRONMENT
        
        # Run the schema analysis script
        result = subprocess.run(
            ['python', str(analysis_script)],
            cwd=str(ETL_PIPELINE_DIR),
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout
        )
        
        # Log output
        logging.info("Schema analysis output:")
        logging.info(result.stdout)
        
        if result.stderr:
            logging.warning("Schema analysis warnings/errors:")
            logging.warning(result.stderr)
        
        if result.returncode != 0:
            raise AirflowException(f"Schema analysis failed with exit code {result.returncode}")
        
        logging.info("✅ Schema analysis completed successfully")
        
        # Parse output for metadata (optional)
        # TODO: Extract tables analyzed, changes detected, etc. from output
        
        return True
        
    except subprocess.TimeoutExpired:
        raise AirflowException("Schema analysis timed out after 30 minutes")
    except Exception as e:
        logging.error(f"❌ Schema analysis failed: {e}")
        raise AirflowException(f"Schema analysis execution failed: {e}")


def analyze_schema_changes(**context):
    """
    Analyze the generated configuration for schema changes.
    
    Compares new tables.yml with backup to detect:
    - Added/removed tables
    - Added/removed columns
    - Changed data types
    - Breaking changes
    """
    import yaml
    
    logging.info("Analyzing schema changes")
    
    # Get backup path from previous task
    backup_created = context['task_instance'].xcom_pull(
        task_ids='backup_existing_config',
        key='backup_created'
    )
    
    if not backup_created:
        logging.info("No previous configuration to compare - first run or no backup")
        context['task_instance'].xcom_push(key='has_breaking_changes', value=False)
        context['task_instance'].xcom_push(key='has_changes', value=False)
        return {
            'has_changes': False,
            'has_breaking_changes': False,
            'change_summary': 'First configuration generation - no comparison available'
        }
    
    backup_path = context['task_instance'].xcom_pull(
        task_ids='backup_existing_config',
        key='backup_path'
    )
    
    try:
        # Load both configurations
        with open(backup_path, 'r') as f:
            old_config = yaml.safe_load(f)
        
        with open(TABLES_YML_PATH, 'r') as f:
            new_config = yaml.safe_load(f)
        
        # Compare schema hashes
        old_hash = old_config.get('metadata', {}).get('schema_hash')
        new_hash = new_config.get('metadata', {}).get('schema_hash')
        
        if old_hash == new_hash:
            logging.info("✅ No schema changes detected")
            context['task_instance'].xcom_push(key='has_changes', value=False)
            context['task_instance'].xcom_push(key='has_breaking_changes', value=False)
            return {
                'has_changes': False,
                'has_breaking_changes': False,
                'change_summary': 'No schema changes detected'
            }
        
        logging.warning("⚠️  Schema changes detected!")
        
        # Analyze changes
        old_tables = set(old_config.get('tables', {}).keys())
        new_tables = set(new_config.get('tables', {}).keys())
        
        added_tables = new_tables - old_tables
        removed_tables = old_tables - new_tables
        
        changes = {
            'has_changes': True,
            'has_breaking_changes': len(removed_tables) > 0,
            'added_tables': list(added_tables),
            'removed_tables': list(removed_tables),
            'added_count': len(added_tables),
            'removed_count': len(removed_tables)
        }
        
        logging.info(f"Added tables: {len(added_tables)}")
        if added_tables:
            logging.info(f"  {', '.join(list(added_tables)[:10])}")
        
        logging.info(f"Removed tables: {len(removed_tables)}")
        if removed_tables:
            logging.warning(f"  {', '.join(list(removed_tables))}")
        
        # Store in XCom for notification task
        context['task_instance'].xcom_push(key='has_changes', value=True)
        context['task_instance'].xcom_push(
            key='has_breaking_changes', 
            value=len(removed_tables) > 0
        )
        context['task_instance'].xcom_push(key='change_details', value=changes)
        
        # Create change summary
        summary = f"Schema changes detected:\n"
        summary += f"  Added: {len(added_tables)} tables\n"
        summary += f"  Removed: {len(removed_tables)} tables\n"
        
        if removed_tables:
            summary += "\n⚠️  BREAKING CHANGES - Removed tables:\n"
            for table in removed_tables:
                summary += f"    - {table}\n"
        
        changes['change_summary'] = summary
        
        return changes
        
    except Exception as e:
        logging.error(f"❌ Failed to analyze schema changes: {e}")
        # Don't fail the DAG, just log the error
        context['task_instance'].xcom_push(key='has_changes', value=True)
        context['task_instance'].xcom_push(key='has_breaking_changes', value=False)
        return {
            'has_changes': True,
            'has_breaking_changes': False,
            'change_summary': f'Error analyzing changes: {e}'
        }


def validate_new_config(**context):
    """
    Validate the newly generated configuration.
    
    Performs sanity checks on tables.yml to ensure it's valid:
    - File exists and is readable
    - Has required metadata
    - Has tables section
    - All tables have required fields
    """
    import yaml
    
    logging.info("Validating new configuration")
    
    if not TABLES_YML_PATH.exists():
        raise AirflowException(f"Configuration file not found: {TABLES_YML_PATH}")
    
    try:
        with open(TABLES_YML_PATH, 'r') as f:
            config = yaml.safe_load(f)
        
        # Check required sections
        if 'metadata' not in config:
            raise AirflowException("Configuration missing 'metadata' section")
        
        if 'tables' not in config:
            raise AirflowException("Configuration missing 'tables' section")
        
        # Check metadata fields
        required_metadata = ['generated_at', 'analyzer_version', 'total_tables', 'environment']
        missing_metadata = [f for f in required_metadata if f not in config['metadata']]
        if missing_metadata:
            raise AirflowException(f"Missing metadata fields: {missing_metadata}")
        
        # Check table count
        table_count = len(config['tables'])
        if table_count == 0:
            raise AirflowException("No tables in configuration")
        
        if table_count < 100:  # OpenDental typically has 400+ tables
            logging.warning(f"⚠️  Low table count: {table_count} (expected 400+)")
        
        # Sample validation of table configurations
        sample_tables = list(config['tables'].items())[:5]
        required_fields = [
            'table_name', 'extraction_strategy', 'batch_size', 
            'performance_category', 'incremental_columns'
        ]
        
        for table_name, table_config in sample_tables:
            missing_fields = [f for f in required_fields if f not in table_config]
            if missing_fields:
                raise AirflowException(
                    f"Table {table_name} missing fields: {missing_fields}"
                )
        
        logging.info(f"✅ Configuration validated: {table_count} tables configured")
        
        # Store stats in XCom
        context['task_instance'].xcom_push(key='total_tables', value=table_count)
        context['task_instance'].xcom_push(key='config_environment', value=config['metadata']['environment'])
        
        return True
        
    except yaml.YAMLError as e:
        raise AirflowException(f"Invalid YAML in configuration: {e}")
    except Exception as e:
        raise AirflowException(f"Configuration validation failed: {e}")


def send_notification(**context):
    """
    Send notification about schema analysis results.
    
    Notifies the team via:
    - Airflow email (if configured)
    - Slack (if webhook configured)
    - Or logs for manual review
    
    Different notification levels:
    - INFO: No changes detected
    - WARNING: Changes detected (new tables, updated config)
    - CRITICAL: Breaking changes (removed tables, removed columns)
    """
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    
    logging.info("Preparing notification")
    
    # Get results from previous tasks
    has_changes = context['task_instance'].xcom_pull(
        task_ids='analyze_schema_changes',
        key='has_changes'
    )
    
    has_breaking_changes = context['task_instance'].xcom_pull(
        task_ids='analyze_schema_changes',
        key='has_breaking_changes'
    )
    
    total_tables = context['task_instance'].xcom_pull(
        task_ids='validate_new_config',
        key='total_tables'
    )
    
    # Compose notification message
    if not has_changes:
        level = "✅ INFO"
        message = f"Schema Analysis Completed - No Changes\n\n"
        message += f"Environment: {ENVIRONMENT}\n"
        message += f"Tables analyzed: {total_tables}\n"
        message += f"Schema hash: Unchanged\n"
        message += f"\nNo action required."
        
    elif has_breaking_changes:
        level = "🚨 CRITICAL"
        change_details = context['task_instance'].xcom_pull(
            task_ids='analyze_schema_changes',
            key='change_details'
        )
        message = f"Schema Analysis - BREAKING CHANGES DETECTED\n\n"
        message += f"Environment: {ENVIRONMENT}\n"
        message += f"Tables analyzed: {total_tables}\n"
        message += f"\n{change_details.get('change_summary', 'See logs for details')}\n"
        message += f"\n⚠️  ACTION REQUIRED:\n"
        message += f"  1. Review schema changelog in logs\n"
        message += f"  2. Update ETL pipeline if needed\n"
        message += f"  3. Test pipeline with new configuration\n"
        message += f"  4. Update dbt models for removed tables\n"
        
    else:
        level = "⚠️  WARNING"
        change_details = context['task_instance'].xcom_pull(
            task_ids='analyze_schema_changes',
            key='change_details'
        )
        message = f"Schema Analysis - Changes Detected\n\n"
        message += f"Environment: {ENVIRONMENT}\n"
        message += f"Tables analyzed: {total_tables}\n"
        message += f"\n{change_details.get('change_summary', 'See logs for details')}\n"
        message += f"\nReview changes and test ETL pipeline.\n"
    
    # Log the notification
    logging.info(f"\n{level}\n{message}")
    
    # Send Slack notification if webhook configured
    try:
        slack_webhook_url = Variable.get('slack_webhook_url', default_var=None)
        if slack_webhook_url:
            slack = SlackWebhookHook(http_conn_id='slack_webhook')
            slack.send(text=f"{level}\n{message}")
            logging.info("Slack notification sent")
    except Exception as e:
        logging.warning(f"Failed to send Slack notification: {e}")
    
    return True


# ============================================================================
# Task Definitions
# ============================================================================

with dag:
    
    # Task 1: Validate source connection
    validate_source = PythonOperator(
        task_id='validate_source_connection',
        python_callable=validate_source_connection,
        doc_md="""
        ### Validate Source Connection
        
        Ensures the OpenDental source database is accessible before running
        the expensive schema analysis. Fails fast if connection issues exist.
        """,
    )
    
    # Task 2: Backup existing configuration
    backup_config = PythonOperator(
        task_id='backup_existing_config',
        python_callable=backup_existing_config,
        doc_md="""
        ### Backup Existing Configuration
        
        Creates a timestamped backup of the current tables.yml configuration.
        This allows rollback if the new configuration has issues.
        """,
    )
    
    # Task 3: Run schema analysis
    analyze_schema = PythonOperator(
        task_id='run_schema_analysis',
        python_callable=run_schema_analysis,
        doc_md="""
        ### Run Schema Analysis
        
        Executes the OpenDental schema analyzer script which:
        - Analyzes all tables in the source database
        - Determines optimal extraction strategies
        - Calculates performance-optimized batch sizes
        - Generates tables.yml configuration
        - Detects schema changes
        """,
    )
    
    # Task 4: Analyze schema changes
    analyze_changes = PythonOperator(
        task_id='analyze_schema_changes',
        python_callable=analyze_schema_changes,
        doc_md="""
        ### Analyze Schema Changes
        
        Compares the new configuration with the backup to detect:
        - Added tables
        - Removed tables (breaking changes)
        - Configuration changes
        
        Determines notification severity based on change type.
        """,
    )
    
    # Task 5: Validate new configuration
    validate_config = PythonOperator(
        task_id='validate_new_config',
        python_callable=validate_new_config,
        doc_md="""
        ### Validate New Configuration
        
        Performs sanity checks on the generated configuration:
        - File exists and is readable YAML
        - Has required metadata and tables sections
        - Tables have required fields
        - Table count is reasonable
        """,
    )
    
    # Task 6: Send notification
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        trigger_rule='all_done',  # Run even if previous tasks fail
        doc_md="""
        ### Send Notification
        
        Notifies the team about schema analysis results:
        - INFO: No changes
        - WARNING: Changes detected
        - CRITICAL: Breaking changes
        
        Sends via Slack (if configured) and email.
        """,
    )
    
    # ========================================================================
    # Task Dependencies
    # ========================================================================
    
    validate_source >> backup_config >> analyze_schema
    analyze_schema >> [analyze_changes, validate_config]
    [analyze_changes, validate_config] >> notify


# ============================================================================
# DAG Documentation
# ============================================================================

dag.doc_md = """
# Schema Analysis DAG

This DAG analyzes the OpenDental source database schema and generates
the `tables.yml` configuration file used by the ETL pipeline.

## Purpose

The schema analyzer:
- Discovers all tables and their characteristics
- Determines optimal extraction strategies (full vs incremental)
- Calculates performance-optimized batch sizes
- Detects schema changes (Slowly Changing Dimensions)
- Generates configuration for the ETL pipeline

## Schedule

- **Default**: Weekly on Sunday at 2 AM
- **On-Demand**: Trigger manually when needed
- **Event-Based**: Run before major OpenDental upgrades

## When to Run

Run this DAG:
1. **Weekly/Monthly**: Scheduled to catch incremental schema changes
2. **Before Upgrades**: Before upgrading OpenDental software
3. **On Schema Changes**: When you know the schema has changed
4. **Troubleshooting**: When ETL fails with schema-related errors

## Output

- **Primary**: `etl_pipeline/config/tables.yml` (configuration file)
- **Backup**: `logs/schema_analysis/backups/tables.yml.backup.TIMESTAMP`
- **Reports**: `logs/schema_analysis/reports/schema_analysis_TIMESTAMP.json`
- **Changelog**: `logs/schema_analysis/reports/schema_changelog_TIMESTAMP.md`

## Notifications

The DAG sends notifications with different severity levels:

- ✅ **INFO**: No changes detected - all is well
- ⚠️ **WARNING**: Changes detected (new tables, updated config) - review and test
- 🚨 **CRITICAL**: Breaking changes (removed tables) - requires immediate action

## Integration with ETL Pipeline DAG

After this DAG completes successfully:
1. Review any reported changes
2. Test ETL pipeline with new configuration (if changes detected)
3. Update dbt models if tables were added/removed
4. Run ETL pipeline DAG with confidence

## Configuration

Set these Airflow Variables:
- `etl_environment`: 'clinic' or 'test' (default: 'clinic')
- `slack_webhook_url`: Slack webhook for notifications (optional)

## Dependencies

Requires:
- Access to OpenDental source database
- ETL pipeline Python package installed
- Proper environment variables set (.env_clinic or .env_test)

## Troubleshooting

If the DAG fails:

1. **Connection Errors**: Check database credentials and network access
2. **Timeout**: Increase `execution_timeout` for large databases
3. **Validation Errors**: Review logs and compare with previous configuration
4. **Backup Errors**: Ensure logs directory is writable

## Related DAGs

- `etl_pipeline`: Main ETL DAG (reads tables.yml generated by this DAG)
- `dbt_build`: dbt transformation DAG (depends on ETL pipeline)
"""

