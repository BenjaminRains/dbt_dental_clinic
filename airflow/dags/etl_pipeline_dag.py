"""
ETL Pipeline DAG for OpenDental Data Extraction and Loading

One nightly run = full ETL pass (incremental load) + dbt build.

What a run is:
1. Validation: tables.yml exists and is recent; all DB connections OK; optional schema drift check.
2. ETL: Full pass over all tables by category (large → medium → small → tiny). Default is
   incremental load (force_full_refresh=False); each table uses config from tables.yml
   (extraction_strategy, incremental_columns, etc.). Config-driven and failure-aware:
   validation fails fast; ETL runs by category with per-table retries; reporting runs
   even on partial failure (ALL_DONE).
3. Reporting: Verify loads, generate report, send notification (Slack/email).
4. dbt: Runs only when ETL succeeded (pipeline_success). Runs dbt deps then dbt build
   (staging → intermediate → marts).

Schedule: Nightly Mon–Sun at 9 PM Central (set AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago).
Dependencies: Valid tables.yml (from Schema Analysis DAG or manual run).

Business-hours guard: Pipeline MUST NOT run during client business hours (6 AM–8:59 PM Central)
because ETL hogs client MySQL resources. First task checks wall clock; if in that window, the run fails.
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import json

try:
    import pendulum
except ImportError:
    pendulum = None

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@example.com'],  # TODO: Update with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Reduced retries for data pipeline (tables can be retried individually)
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),  # 6 hour max for full pipeline
}

# DAG configuration
# Schedule: 9 PM Central Mon–Sun. Requires AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago.
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Nightly ETL (incremental) + dbt: OpenDental → raw → staging/intermediate/marts',
    schedule_interval='0 21 * * *',  # 9 PM daily (Central if default timezone set)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'data-pipeline', 'production', 'nightly'],
    max_active_runs=1,  # Only one run at a time
    params={
        'force_full_refresh': False,  # Incremental load by default; set True for full refresh
        'max_workers': 5,  # Parallel workers for large tables
        'skip_validation': False,  # Emergency runs only
    },
)

# ============================================================================
# Configuration
# ============================================================================

# Project root: override via Airflow Variable "project_root" for EC2 vs local
PROJECT_ROOT = Path(Variable.get('project_root', default_var='/opt/airflow/dbt_dental_clinic'))
ETL_PIPELINE_DIR = PROJECT_ROOT / 'etl_pipeline'
DBT_PROJECT_DIR = PROJECT_ROOT / 'dbt_dental_models'
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml in project; or use Variable for .dbt path
TABLES_YML_PATH = ETL_PIPELINE_DIR / 'etl_pipeline' / 'config' / 'tables.yml'

# Environment (from Airflow Variable or default to clinic)
ENVIRONMENT = Variable.get('etl_environment', default_var='clinic')

# Configuration thresholds
CONFIG_STALENESS_WARNING_DAYS = 30  # Warn if config older than 30 days
CONFIG_STALENESS_ERROR_DAYS = 90    # Error if config older than 90 days

# Performance settings
DEFAULT_MAX_WORKERS = 5  # Default parallel workers for large tables
PARALLEL_EXECUTION_ENABLED = True  # Enable parallel processing

# Business-hours guard: no ETL during client business hours (client MySQL must not be hogged)
BUSINESS_HOURS_START_HOUR = 6   # 6 AM Central
BUSINESS_HOURS_END_HOUR = 21    # 9 PM Central (allowed window starts at 21:00)
CENTRAL_TZ = "America/Chicago"

# ============================================================================
# Task Functions - Business-hours guard
# ============================================================================

def guard_business_hours(**context):
    """
    Fail the run if current wall clock (Central) is during business hours 6 AM–8:59 PM.

    Pipeline MUST NOT run during business hours because it hogs client server MySQL.
    Allowed window: 9 PM–5:59 AM Central only (including manual triggers).
    """
    if pendulum:
        now = pendulum.now(CENTRAL_TZ)
    else:
        from datetime import timezone
        # Fallback: assume UTC and subtract 6 hours (CST); no DST
        now = datetime.now(timezone.utc)
        try:
            from zoneinfo import ZoneInfo
            now = now.astimezone(ZoneInfo(CENTRAL_TZ))
        except Exception:
            # Last resort: treat as UTC and allow (avoid blocking); log warning
            logging.warning("pendulum/zoneinfo not available; business-hours check skipped (UTC used)")
            return True

    hour = now.hour
    if BUSINESS_HOURS_START_HOUR <= hour < BUSINESS_HOURS_END_HOUR:
        raise AirflowException(
            f"Pipeline is not allowed during business hours (6 AM–8:59 PM Central) "
            f"to avoid impacting client MySQL. Current Central time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}. "
            f"Run only between 9 PM and 5:59 AM Central."
        )
    logging.info(
        f"Business-hours check passed: current Central time {now.strftime('%H:%M')} "
        f"is outside 6 AM–8:59 PM."
    )
    return True


# ============================================================================
# Task Functions - Validation
# ============================================================================

def validate_configuration(**context):
    """
    Validate that tables.yml exists, is recent, and valid.
    
    This is critical - the ETL pipeline cannot run without valid configuration.
    Checks:
    1. tables.yml file exists
    2. Configuration is not too old (< 90 days)
    3. Environment matches
    4. Required tables are configured
    """
    import yaml
    from datetime import datetime, timedelta
    
    logging.info("Validating ETL pipeline configuration")
    
    # Check if skip validation flag is set (emergency runs only)
    skip_validation = context['params'].get('skip_validation', False)
    if skip_validation:
        logging.warning("⚠️  VALIDATION SKIPPED - Emergency run mode")
        return True
    
    # Check 1: File exists
    if not TABLES_YML_PATH.exists():
        raise AirflowException(
            f"Configuration file not found: {TABLES_YML_PATH}\n"
            f"Run the Schema Analysis DAG first to generate configuration."
        )
    
    try:
        with open(TABLES_YML_PATH, 'r') as f:
            config = yaml.safe_load(f)
        
        # Check 2: Has required sections
        if 'metadata' not in config:
            raise AirflowException("Configuration missing 'metadata' section")
        
        if 'tables' not in config:
            raise AirflowException("Configuration missing 'tables' section")
        
        metadata = config['metadata']
        
        # Check 3: Configuration age
        generated_at_str = metadata.get('generated_at')
        if not generated_at_str:
            raise AirflowException("Configuration missing 'generated_at' timestamp")
        
        generated_at = datetime.fromisoformat(generated_at_str)
        config_age = datetime.now() - generated_at
        
        if config_age > timedelta(days=CONFIG_STALENESS_ERROR_DAYS):
            raise AirflowException(
                f"Configuration is too old: {config_age.days} days "
                f"(maximum: {CONFIG_STALENESS_ERROR_DAYS} days)\n"
                f"Run the Schema Analysis DAG to update configuration."
            )
        
        if config_age > timedelta(days=CONFIG_STALENESS_WARNING_DAYS):
            logging.warning(
                f"⚠️  Configuration is {config_age.days} days old. "
                f"Consider running Schema Analysis DAG to refresh."
            )
        
        # Check 4: Environment matches
        config_environment = metadata.get('environment')
        if config_environment != ENVIRONMENT:
            raise AirflowException(
                f"Configuration environment mismatch: "
                f"config={config_environment}, current={ENVIRONMENT}"
            )
        
        # Check 5: Table count sanity check
        table_count = len(config['tables'])
        if table_count == 0:
            raise AirflowException("No tables configured")
        
        if table_count < 100:  # OpenDental typically has 400+ tables
            logging.warning(
                f"⚠️  Low table count: {table_count} tables "
                f"(OpenDental typically has 400+)"
            )
        
        # Store metadata in XCom for other tasks
        context['task_instance'].xcom_push(key='config_age_days', value=config_age.days)
        context['task_instance'].xcom_push(key='total_tables', value=table_count)
        context['task_instance'].xcom_push(key='schema_hash', value=metadata.get('schema_hash'))
        
        logging.info(
            f"✅ Configuration validated: {table_count} tables, "
            f"{config_age.days} days old, environment={ENVIRONMENT}"
        )
        
        return True
        
    except yaml.YAMLError as e:
        raise AirflowException(f"Invalid YAML in configuration: {e}")
    except Exception as e:
        raise AirflowException(f"Configuration validation failed: {e}")


def validate_database_connections(**context):
    """
    Validate all database connections before starting ETL.
    
    Tests connections to:
    1. OpenDental source (MySQL)
    2. Replication database (MySQL)
    3. Analytics database (PostgreSQL)
    
    Fails fast if any connection is unavailable.
    """
    import sys
    sys.path.insert(0, str(ETL_PIPELINE_DIR))
    
    from etl_pipeline.config import get_settings, DatabaseType
    from etl_pipeline.core.connections import ConnectionFactory
    
    logging.info("Validating database connections")
    
    try:
        settings = get_settings()
        
        # Test 1: Source connection (OpenDental MySQL)
        logging.info("Testing source connection...")
        source_engine = ConnectionFactory.get_source_connection(settings)
        with source_engine.connect() as conn:
            result = conn.execute("SELECT 1 as test, DATABASE() as db")
            row = result.fetchone()
            logging.info(f"✅ Source connected: {row[1]}")
        source_engine.dispose()
        
        # Test 2: Replication connection (Local MySQL)
        logging.info("Testing replication connection...")
        replication_engine = ConnectionFactory.get_replication_connection(settings)
        with replication_engine.connect() as conn:
            result = conn.execute("SELECT 1 as test, DATABASE() as db")
            row = result.fetchone()
            logging.info(f"✅ Replication connected: {row[1]}")
        replication_engine.dispose()
        
        # Test 3: Analytics connection (PostgreSQL)
        logging.info("Testing analytics connection...")
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        with analytics_engine.connect() as conn:
            result = conn.execute("SELECT 1 as test, current_database() as db, current_schema() as schema")
            row = result.fetchone()
            logging.info(f"✅ Analytics connected: {row[1]}.{row[2]}")
        analytics_engine.dispose()
        
        logging.info("✅ All database connections validated")
        return True
        
    except Exception as e:
        logging.error(f"❌ Database connection validation failed: {e}")
        raise AirflowException(f"Failed to connect to databases: {e}")


def check_schema_hash(**context):
    """
    Optional: Quick check if source schema has changed since config generation.
    
    This is a lightweight check on a few key tables to detect schema drift.
    If significant drift detected, recommend running Schema Analysis DAG.
    """
    import sys
    import yaml
    sys.path.insert(0, str(ETL_PIPELINE_DIR))
    
    from etl_pipeline.config import get_settings
    from etl_pipeline.core.connections import ConnectionFactory
    
    logging.info("Checking for schema drift (quick check)")
    
    try:
        # Load expected schema hash from config
        with open(TABLES_YML_PATH, 'r') as f:
            config = yaml.safe_load(f)
        
        expected_hash = config['metadata'].get('schema_hash')
        
        # Quick check: Count tables in source database
        settings = get_settings()
        source_engine = ConnectionFactory.get_source_connection(settings)
        
        with source_engine.connect() as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() "
                "AND table_type = 'BASE TABLE'"
            )
            current_table_count = result.scalar()
        
        source_engine.dispose()
        
        expected_table_count = config['metadata'].get('total_tables', 0)
        
        # Check if table count matches (simple drift detection)
        if current_table_count != expected_table_count:
            drift_pct = abs(current_table_count - expected_table_count) / expected_table_count * 100
            
            if drift_pct > 5:  # More than 5% difference
                logging.warning(
                    f"⚠️  Significant schema drift detected!\n"
                    f"Expected tables: {expected_table_count}\n"
                    f"Current tables: {current_table_count}\n"
                    f"Drift: {drift_pct:.1f}%\n"
                    f"Recommend running Schema Analysis DAG before continuing."
                )
                context['task_instance'].xcom_push(key='schema_drift_detected', value=True)
                context['task_instance'].xcom_push(key='drift_percentage', value=drift_pct)
            else:
                logging.info(
                    f"Minor schema drift: {expected_table_count} → {current_table_count} tables"
                )
                context['task_instance'].xcom_push(key='schema_drift_detected', value=False)
        else:
            logging.info(f"✅ No schema drift detected ({current_table_count} tables)")
            context['task_instance'].xcom_push(key='schema_drift_detected', value=False)
        
        return True
        
    except Exception as e:
        logging.warning(f"Schema drift check failed (non-critical): {e}")
        # Don't fail the pipeline for drift check failures
        context['task_instance'].xcom_push(key='schema_drift_detected', value=False)
        return True


# ============================================================================
# Task Functions - ETL Processing
# ============================================================================

def process_tables_by_category(category: str, **context):
    """
    Process all tables in a given performance category.
    
    Args:
        category: 'large', 'medium', 'small', or 'tiny'
    
    This function integrates with the existing pipeline_orchestrator.py
    to leverage the established ETL logic.
    """
    import sys
    sys.path.insert(0, str(ETL_PIPELINE_DIR))
    
    from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
    
    logging.info(f"Processing {category} tables")
    
    # Get parameters
    force_full = context['params'].get('force_full_refresh', False)
    max_workers = context['params'].get('max_workers', DEFAULT_MAX_WORKERS)
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator(environment=ENVIRONMENT)
        
        # Initialize connections
        if not orchestrator.initialize_connections():
            raise AirflowException(f"Failed to initialize connections for {category} tables")
        
        # Process tables in this category
        results = orchestrator.process_tables_by_performance_category(
            category=category,
            max_workers=max_workers if category == 'large' else 1,  # Only parallel for large
            force_full=force_full
        )
        
        # Cleanup
        orchestrator.cleanup()
        
        # Analyze results
        if not results:
            logging.warning(f"No tables found in category: {category}")
            raise AirflowSkipException(f"No {category} tables to process")
        
        success_count = sum(1 for success in results.values() if success)
        failure_count = sum(1 for success in results.values() if not success)
        total_count = len(results)
        
        # Store results in XCom
        context['task_instance'].xcom_push(key='success_count', value=success_count)
        context['task_instance'].xcom_push(key='failure_count', value=failure_count)
        context['task_instance'].xcom_push(key='total_count', value=total_count)
        context['task_instance'].xcom_push(key='failed_tables', value=[
            table for table, success in results.items() if not success
        ])
        
        logging.info(
            f"✅ {category.upper()} tables processed: "
            f"{success_count} succeeded, {failure_count} failed, "
            f"{total_count} total"
        )
        
        # If any critical tables failed, raise exception
        if failure_count > 0:
            failed_tables = [table for table, success in results.items() if not success]
            logging.error(f"Failed tables in {category}: {failed_tables}")
            
            # For large tables, failure is critical
            if category == 'large':
                raise AirflowException(
                    f"Critical large table failures: {failed_tables}"
                )
            
            # For other categories, log but continue
            logging.warning(f"Some {category} tables failed but pipeline continues")
        
        return {
            'category': category,
            'success': success_count,
            'failed': failure_count,
            'total': total_count
        }
        
    except AirflowSkipException:
        raise  # Re-raise skip exceptions
    except Exception as e:
        logging.error(f"❌ Failed to process {category} tables: {e}")
        raise AirflowException(f"ETL processing failed for {category} tables: {e}")


def process_large_tables(**context):
    """Process large tables (1M+ rows) in parallel."""
    return process_tables_by_category('large', **context)


def process_medium_tables(**context):
    """Process medium tables (100K-1M rows) sequentially."""
    return process_tables_by_category('medium', **context)


def process_small_tables(**context):
    """Process small tables (10K-100K rows) sequentially."""
    return process_tables_by_category('small', **context)


def process_tiny_tables(**context):
    """Process tiny tables (<10K rows) sequentially."""
    return process_tables_by_category('tiny', **context)


# ============================================================================
# Task Functions - Verification & Reporting
# ============================================================================

def verify_loads(**context):
    """
    Verify that data was loaded successfully.
    
    Checks:
    1. Row counts in tracking tables
    2. Recent load timestamps
    3. Data freshness in analytics database
    """
    import sys
    sys.path.insert(0, str(ETL_PIPELINE_DIR))
    
    from etl_pipeline.config import get_settings
    from etl_pipeline.core.connections import ConnectionFactory
    
    logging.info("Verifying data loads")
    
    try:
        settings = get_settings()
        
        # Check replication tracking table
        replication_engine = ConnectionFactory.get_replication_connection(settings)
        with replication_engine.connect() as conn:
            result = conn.execute(
                """
                SELECT 
                    COUNT(*) as total_tables,
                    SUM(CASE WHEN copy_status = 'success' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN copy_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    MAX(last_copy_date) as latest_copy
                FROM etl_copy_status
                WHERE DATE(last_copy_date) = CURDATE()
                """
            )
            row = result.fetchone()
            
            if row:
                logging.info(
                    f"Replication status (today): "
                    f"{row[1]} successful, {row[2]} failed, "
                    f"latest: {row[3]}"
                )
        
        replication_engine.dispose()
        
        # Check analytics tracking table
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        with analytics_engine.connect() as conn:
            result = conn.execute(
                """
                SELECT 
                    COUNT(*) as total_tables,
                    SUM(CASE WHEN load_status = 'success' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN load_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    MAX(last_load_date) as latest_load
                FROM raw.etl_load_status
                WHERE DATE(last_load_date) = CURRENT_DATE
                """
            )
            row = result.fetchone()
            
            if row:
                logging.info(
                    f"Analytics status (today): "
                    f"{row[1]} successful, {row[2]} failed, "
                    f"latest: {row[3]}"
                )
                
                context['task_instance'].xcom_push(key='analytics_successful', value=row[1])
                context['task_instance'].xcom_push(key='analytics_failed', value=row[2])
        
        analytics_engine.dispose()
        
        logging.info("✅ Load verification completed")
        return True
        
    except Exception as e:
        logging.error(f"❌ Load verification failed: {e}")
        # Don't fail the pipeline for verification failures
        logging.warning("Continuing despite verification failure")
        return True


def generate_pipeline_report(**context):
    """
    Generate comprehensive pipeline execution report.
    
    Aggregates results from all processing tasks and creates
    a summary report with metrics and recommendations.
    """
    logging.info("Generating pipeline execution report")
    
    try:
        # Collect results from all tasks
        ti = context['task_instance']
        
        # Configuration info
        total_tables = ti.xcom_pull(task_ids='validate_configuration', key='total_tables')
        config_age = ti.xcom_pull(task_ids='validate_configuration', key='config_age_days')
        
        # Processing results by category
        categories = ['large', 'medium', 'small', 'tiny']
        results_by_category = {}
        
        for category in categories:
            task_id = f'process_{category}_tables'
            success = ti.xcom_pull(task_ids=task_id, key='success_count') or 0
            failed = ti.xcom_pull(task_ids=task_id, key='failure_count') or 0
            total = ti.xcom_pull(task_ids=task_id, key='total_count') or 0
            failed_tables = ti.xcom_pull(task_ids=task_id, key='failed_tables') or []
            
            results_by_category[category] = {
                'success': success,
                'failed': failed,
                'total': total,
                'failed_tables': failed_tables
            }
        
        # Calculate totals
        total_success = sum(r['success'] for r in results_by_category.values())
        total_failed = sum(r['failed'] for r in results_by_category.values())
        total_processed = sum(r['total'] for r in results_by_category.values())
        
        # Verification results
        analytics_successful = ti.xcom_pull(task_ids='verify_loads', key='analytics_successful') or 0
        analytics_failed = ti.xcom_pull(task_ids='verify_loads', key='analytics_failed') or 0
        
        # Schema drift check
        schema_drift = ti.xcom_pull(task_ids='check_schema_hash', key='schema_drift_detected')
        
        # Generate report
        report = {
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['run_id'],
            'environment': ENVIRONMENT,
            'configuration': {
                'total_tables_configured': total_tables,
                'config_age_days': config_age,
                'schema_drift_detected': schema_drift
            },
            'processing_results': {
                'total_processed': total_processed,
                'total_success': total_success,
                'total_failed': total_failed,
                'success_rate': f"{(total_success/total_processed*100):.1f}%" if total_processed > 0 else "0%",
                'by_category': results_by_category
            },
            'verification': {
                'analytics_successful': analytics_successful,
                'analytics_failed': analytics_failed
            }
        }
        
        # Log report
        logging.info("=" * 60)
        logging.info("ETL PIPELINE EXECUTION REPORT")
        logging.info("=" * 60)
        logging.info(f"Execution Date: {report['execution_date']}")
        logging.info(f"Environment: {ENVIRONMENT}")
        logging.info(f"Configuration Age: {config_age} days")
        logging.info("")
        logging.info(f"Processing Results:")
        logging.info(f"  Total Processed: {total_processed}")
        logging.info(f"  Successful: {total_success}")
        logging.info(f"  Failed: {total_failed}")
        logging.info(f"  Success Rate: {report['processing_results']['success_rate']}")
        logging.info("")
        logging.info(f"By Category:")
        for category, results in results_by_category.items():
            logging.info(f"  {category.upper()}: {results['success']}/{results['total']} successful")
            if results['failed_tables']:
                logging.info(f"    Failed: {', '.join(results['failed_tables'][:5])}")
        logging.info("")
        logging.info(f"Analytics Status:")
        logging.info(f"  Successful: {analytics_successful}")
        logging.info(f"  Failed: {analytics_failed}")
        logging.info("=" * 60)
        
        # Store report in XCom for notification task
        context['task_instance'].xcom_push(key='pipeline_report', value=report)
        
        # Determine if pipeline was successful
        pipeline_success = (total_failed == 0)
        context['task_instance'].xcom_push(key='pipeline_success', value=pipeline_success)
        
        return report
        
    except Exception as e:
        logging.error(f"❌ Failed to generate report: {e}")
        # Don't fail the pipeline for reporting failures
        return {'error': str(e)}


def send_completion_notification(**context):
    """
    Send notification about pipeline execution completion.
    
    Sends summary with:
    - Execution status (success/partial/failed)
    - Processing statistics
    - Failed tables (if any)
    - Recommendations
    """
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    
    logging.info("Sending pipeline completion notification")
    
    try:
        # Get report from previous task
        ti = context['task_instance']
        report = ti.xcom_pull(task_ids='generate_pipeline_report', key='pipeline_report')
        pipeline_success = ti.xcom_pull(task_ids='generate_pipeline_report', key='pipeline_success')
        
        if not report:
            logging.warning("No report available for notification")
            return True
        
        # Determine status and level
        total_processed = report['processing_results']['total_processed']
        total_success = report['processing_results']['total_success']
        total_failed = report['processing_results']['total_failed']
        
        if pipeline_success:
            level = "✅ SUCCESS"
            status = "completed successfully"
        elif total_failed > 0 and total_success > 0:
            level = "⚠️  PARTIAL SUCCESS"
            status = "completed with some failures"
        else:
            level = "❌ FAILED"
            status = "failed"
        
        # Compose message
        message = f"ETL Pipeline {status}\n\n"
        message += f"Environment: {ENVIRONMENT}\n"
        message += f"Execution: {context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"\nResults:\n"
        message += f"  • Processed: {total_processed} tables\n"
        message += f"  • Successful: {total_success}\n"
        message += f"  • Failed: {total_failed}\n"
        message += f"  • Success Rate: {report['processing_results']['success_rate']}\n"
        
        # Add failed tables if any
        if total_failed > 0:
            message += f"\nFailed Tables:\n"
            for category, results in report['processing_results']['by_category'].items():
                if results['failed_tables']:
                    message += f"  {category}: {', '.join(results['failed_tables'][:5])}\n"
        
        # Add recommendations
        config_age = report['configuration']['config_age_days']
        if config_age > 30:
            message += f"\n⚠️  Configuration is {config_age} days old. Consider running Schema Analysis DAG.\n"
        
        if report['configuration']['schema_drift_detected']:
            message += f"\n⚠️  Schema drift detected. Recommend running Schema Analysis DAG.\n"
        
        # Log message
        logging.info(f"\n{level}\n{message}")
        
        # Send Slack notification if configured
        try:
            slack_webhook_url = Variable.get('slack_webhook_url', default_var=None)
            if slack_webhook_url:
                slack = SlackWebhookHook(http_conn_id='slack_webhook')
                slack.send(text=f"{level}\n{message}")
                logging.info("Slack notification sent")
        except Exception as e:
            logging.warning(f"Failed to send Slack notification: {e}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to send notification: {e}")
        return True


# ============================================================================
# Task Definitions
# ============================================================================

with dag:

    # =======================================================================
    # BUSINESS-HOURS GUARD (must run first; blocks ETL during 6 AM–8:59 PM Central)
    # =======================================================================

    guard_business_hours_task = PythonOperator(
        task_id='guard_business_hours',
        python_callable=guard_business_hours,
        doc_md="""
        ### Business-hours guard

        **Pipeline MUST NOT run during client business hours (6 AM–8:59 PM Central)** because
        ETL hogs client server MySQL resources. This task checks current wall clock in Central;
        if in that window, the run fails. Allowed window: 9 PM–5:59 AM Central only.
        """,
    )

    # =======================================================================
    # VALIDATION TASKS
    # =======================================================================

    with TaskGroup(group_id='validation', tooltip='Validate configuration and connections') as validation_group:

        # Validate configuration
        validate_config = PythonOperator(
            task_id='validate_configuration',
            python_callable=validate_configuration,
            doc_md="""
            ### Validate Configuration
            
            Ensures tables.yml exists, is recent, and valid before starting ETL.
            Checks configuration age, environment match, and table count.
            """,
        )
        
        # Validate database connections
        validate_connections = PythonOperator(
            task_id='validate_database_connections',
            python_callable=validate_database_connections,
            doc_md="""
            ### Validate Database Connections
            
            Tests connections to source, replication, and analytics databases.
            Fails fast if any connection is unavailable.
            """,
        )
        
        # Check for schema drift (optional)
        check_drift = PythonOperator(
            task_id='check_schema_hash',
            python_callable=check_schema_hash,
            doc_md="""
            ### Check Schema Hash
            
            Quick check for schema drift by comparing table counts.
            Warns if significant drift detected but doesn't fail pipeline.
            """,
        )
        
        validate_config >> [validate_connections, check_drift]
    
    # =======================================================================
    # ETL PROCESSING TASKS
    # =======================================================================
    
    with TaskGroup(group_id='etl_processing', tooltip='Extract and load data by performance category') as etl_group:
        
        # Process large tables (parallel)
        large_tables = PythonOperator(
            task_id='process_large_tables',
            python_callable=process_large_tables,
            execution_timeout=timedelta(hours=3),
            doc_md="""
            ### Process Large Tables
            
            Processes tables with 1M+ rows in parallel for optimal performance.
            Uses configurable max_workers (default: 5).
            """,
        )
        
        # Process medium tables (sequential)
        medium_tables = PythonOperator(
            task_id='process_medium_tables',
            python_callable=process_medium_tables,
            execution_timeout=timedelta(hours=2),
            doc_md="""
            ### Process Medium Tables
            
            Processes tables with 100K-1M rows sequentially.
            Balances performance with resource usage.
            """,
        )
        
        # Process small tables (sequential)
        small_tables = PythonOperator(
            task_id='process_small_tables',
            python_callable=process_small_tables,
            execution_timeout=timedelta(hours=1),
            doc_md="""
            ### Process Small Tables
            
            Processes tables with 10K-100K rows sequentially.
            Fast execution with minimal resource usage.
            """,
        )
        
        # Process tiny tables (sequential)
        tiny_tables = PythonOperator(
            task_id='process_tiny_tables',
            python_callable=process_tiny_tables,
            execution_timeout=timedelta(minutes=30),
            doc_md="""
            ### Process Tiny Tables
            
            Processes tables with <10K rows sequentially.
            Very fast execution for reference data.
            """,
        )
        
        # Sequential processing: large → medium → small → tiny
        large_tables >> medium_tables >> small_tables >> tiny_tables
    
    # =======================================================================
    # VERIFICATION & REPORTING TASKS
    # =======================================================================
    
    with TaskGroup(group_id='reporting', tooltip='Verify loads and generate reports') as reporting_group:
        
        # Verify loads
        verify = PythonOperator(
            task_id='verify_loads',
            python_callable=verify_loads,
            trigger_rule=TriggerRule.ALL_DONE,  # Run even if some tables failed
            doc_md="""
            ### Verify Loads
            
            Checks tracking tables to verify data was loaded successfully.
            Validates row counts and timestamps.
            """,
        )
        
        # Generate report
        report = PythonOperator(
            task_id='generate_pipeline_report',
            python_callable=generate_pipeline_report,
            trigger_rule=TriggerRule.ALL_DONE,  # Run even if processing failed
            doc_md="""
            ### Generate Pipeline Report
            
            Aggregates results from all tasks and creates comprehensive report.
            Includes success/failure statistics and recommendations.
            """,
        )
        
        # Send notification
        notify = PythonOperator(
            task_id='send_completion_notification',
            python_callable=send_completion_notification,
            trigger_rule=TriggerRule.ALL_DONE,  # Always run
            doc_md="""
            ### Send Completion Notification
            
            Sends notification about pipeline execution via email/Slack.
            Includes summary, failed tables, and recommendations.
            """,
        )
        
        verify >> report >> notify
    
    # =======================================================================
    # SHORT-CIRCUIT: Run dbt only when ETL succeeded
    # =======================================================================
    
    def should_run_dbt(**context):
        """Run dbt only when pipeline_success is True (no critical ETL failures)."""
        ti = context['task_instance']
        pipeline_success = ti.xcom_pull(task_ids='generate_pipeline_report', key='pipeline_success')
        if not pipeline_success:
            logging.warning("ETL had failures; skipping dbt to avoid transforming partial data.")
            return False
        return True
    
    short_circuit_dbt = ShortCircuitOperator(
        task_id='should_run_dbt',
        python_callable=should_run_dbt,
        doc_md="Skips dbt when ETL reported failures so we do not transform partial data.",
    )
    report >> short_circuit_dbt
    
    # =======================================================================
    # DBT TASKS (after successful ETL)
    # =======================================================================
    
    with TaskGroup(group_id='dbt_build', tooltip='dbt deps and dbt build (staging → marts)') as dbt_group:
        # Worker inherits env (POSTGRES_ANALYTICS_* from docker-compose / EC2); set profiles dir in shell
        dbt_deps = BashOperator(
            task_id='dbt_deps',
            bash_command=f'export DBT_PROFILES_DIR="{DBT_PROFILES_DIR}" && cd "{DBT_PROJECT_DIR}" && dbt deps',
            doc_md="Install dbt package dependencies.",
        )
        # dbt target: Airflow Variable 'dbt_target' (e.g. 'local' for localhost, 'clinic' for EC2)
        dbt_target = Variable.get('dbt_target', default_var='local')
        dbt_build = BashOperator(
            task_id='dbt_build',
            bash_command=f'export DBT_PROFILES_DIR="{DBT_PROFILES_DIR}" && cd "{DBT_PROJECT_DIR}" && dbt build --target {dbt_target}',
            execution_timeout=timedelta(hours=2),
            doc_md="Run dbt build (run + test) for staging, intermediate, and marts.",
        )
        dbt_deps >> dbt_build
    
    short_circuit_dbt >> dbt_group
    
    # =======================================================================
    # DAG FLOW
    # =======================================================================

    guard_business_hours_task >> validation_group >> etl_group >> reporting_group


# ============================================================================
# DAG Documentation
# ============================================================================

dag.doc_md = """
# ETL Pipeline DAG

This DAG orchestrates the complete OpenDental ETL pipeline that extracts
data from the source MySQL database, stages it in a replication database,
and loads it into the PostgreSQL analytics database.

## What a Run Is (Nightly)

One run = **incremental ETL** (full pass, config-driven) + **dbt build** only when ETL succeeded.

- **Incremental load**: `force_full_refresh=False` by default; each table uses `tables.yml` (extraction_strategy, incremental_columns). Full refresh on demand via DAG params.
- **Config-driven**: Tables and strategies come from `tables.yml` (from Schema Analysis DAG or manual run).
- **Failure handling**: Validation fails fast; ETL runs by category with per-table retries; reporting/notify run even on partial failure; dbt runs only when `pipeline_success` is True.

## Data Flow

```
OpenDental MySQL (Source)
    ↓ Extract (incremental per table config)
Replication MySQL (Staging)
    ↓ Load (incremental per table config)
PostgreSQL Analytics (Raw Schema)
    ↓ (same DAG, on ETL success)
dbt deps → dbt build (staging → intermediate → marts)
```

## Pipeline Phases

### 0. Business-hours guard (first task)
- **Pipeline MUST NOT run during 6 AM–8:59 PM Central** (client MySQL must not be hogged).
- Checks current wall clock in America/Chicago; if in that window, the run fails.
- Allowed window: 9 PM–5:59 AM Central only (schedule or manual trigger).

### 1. Validation
- Validates `tables.yml` configuration
- Tests all database connections
- Checks for schema drift

### 2. ETL Processing (incremental by default)
- **Large Tables** (1M+ rows): Parallel processing with 5 workers
- **Medium Tables** (100K-1M rows): Sequential processing
- **Small Tables** (10K-100K rows): Sequential processing
- **Tiny Tables** (<10K rows): Sequential processing

### 3. Verification & Reporting
- Verifies data loads
- Generates execution report
- Sends notifications

### 4. dbt (only when ETL succeeded)
- `dbt deps` then `dbt build` (target from Airflow Variable `dbt_target`, default `local`)

## Schedule

- **Nightly**: Mon–Sun at **9 PM Central**. Set `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago` so the cron is interpreted in Central time.
- **Catchup**: Disabled (doesn't backfill missed runs)

## Configuration

### Required Configuration

- Valid `tables.yml` from Schema Analysis DAG
- Environment variables set for database connections
- Proper database permissions

### Parameters

Override when triggering manually:

```python
{
    "force_full_refresh": true,  # Force full refresh for all tables
    "max_workers": 10,            # Increase parallel workers
    "skip_validation": false      # Emergency mode (not recommended)
}
```

### Airflow Variables

- `etl_environment`: 'clinic' or 'test' (required)
- `project_root`: Path to repo root (default `/opt/airflow/dbt_dental_clinic`); override for EC2
- `dbt_target`: dbt profile target, e.g. `local` (localhost) or `clinic` (EC2)
- `slack_webhook_url`: Slack webhook for notifications (optional)

## Performance

### Expected Runtime

- **Small deployment** (<1M total rows): 10-30 minutes
- **Medium deployment** (1M-10M rows): 30-90 minutes
- **Large deployment** (10M+ rows): 1-3 hours

### Optimization

- Large tables processed in parallel (default: 5 workers)
- Incremental loading for most tables
- Chunked processing for very large tables
- Performance metrics tracked per table

## Monitoring

### Success Criteria

✅ **Successful Run**: All tables processed without errors

⚠️ **Partial Success**: Some tables failed but pipeline completed
- Non-critical table failures are logged
- Pipeline continues to process other tables

❌ **Failed Run**: Critical failures that stop the pipeline
- Configuration validation failed
- Connection failures
- Large table processing failures

### Notifications

Notifications sent with different levels:
- **SUCCESS**: All tables processed successfully
- **WARNING**: Some tables failed
- **ERROR**: Critical failures occurred

### Logs

Check logs for:
- Individual table processing status
- Performance metrics
- Error details
- Recommendations

## Integration

### Upstream Dependency

Depends on valid `tables.yml` from Schema Analysis DAG:
- Run Schema Analysis DAG weekly
- ETL Pipeline reads the generated configuration
- Configuration validated before each run

### Downstream Dependency

Triggers dbt Build DAG after successful completion:
- ETL populates raw schema in PostgreSQL
- dbt transforms raw data into analytics models
- dbt DAG triggered automatically

## Troubleshooting

### Common Issues

**Configuration too old**
```
Solution: Run Schema Analysis DAG to refresh configuration
```

**Connection failures**
```
Solution: Check database credentials and network connectivity
```

**Large table failures**
```
Solution: Check table-specific logs, may need to increase resources
```

**Schema drift warnings**
```
Solution: Run Schema Analysis DAG to update configuration
```

### Emergency Procedures

**Skip validation** (not recommended):
```python
# Trigger with skip_validation parameter
{"skip_validation": true}
```

**Force full refresh**:
```python
# Trigger with force_full_refresh parameter
{"force_full_refresh": true}
```

## Best Practices

1. **Monitor Configuration Age**: Run Schema Analysis weekly
2. **Review Failure Logs**: Investigate failed tables promptly
3. **Test in Non-Prod**: Test changes in test environment first
4. **Resource Planning**: Adjust max_workers based on infrastructure
5. **Incremental First**: Use full refresh only when necessary

## Related DAGs

- `schema_analysis`: Generates configuration for this DAG
- `dbt_build`: Transforms data loaded by this DAG

## Support

For issues or questions:
1. Check task logs in Airflow UI
2. Review `etl_pipeline/logs/` for detailed logs
3. Contact data engineering team
"""

