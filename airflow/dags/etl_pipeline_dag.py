"""
ETL Pipeline DAG for OpenDental Data Extraction and Loading

One nightly run = schema refresh + incremental ETL + dbt build + publish to clinic RDS.

What a run is:
0. Schema refresh: backup tables.yml and run analyze_opendental_schema.py (fresh config before ETL).
1. Validation: tables.yml exists and is recent; all DB connections OK; optional schema drift check.
2. ETL: Full pass over all tables by category (large → medium → small → tiny). Default is
   incremental load (force_full_refresh=False); each table uses config from tables.yml.
3. Reporting: Verify loads, generate execution report.
4. dbt: When ETL succeeded — mdc dbt deps + build --target {dbt_target} (default local).
5. Publish: When publish_environment is set (e.g. clinic) — mdc publish analytics to RDS.
6. Notify: Slack/email summary (after report and publish when configured).

Schedule: Nightly Mon–Sun at 9 PM Central (set AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago).
Dependencies: Valid tables.yml (from Schema Analysis DAG or manual run).

Airflow Variables: etl_environment, dbt_target, publish_environment, project_root (see dag doc_md).

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

from airflow.sdk import DAG, TaskGroup, Variable
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    ShortCircuitOperator,
)
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.task.trigger_rule import TriggerRule

from lib.mdc_runner import run_mdc, run_mdc_etl_invoke

# Default arguments for all tasks
default_args = {
    'owner': 'benjamin-rains',
    'depends_on_past': False,
    'email': ['rains.bp@gmail.com'],
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
    schedule='0 21 * * *',  # 9 PM daily (Central if default timezone set)
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

# Project root: override via Airflow Variable "project_root" (native Option A: repo root on disk)
_REPO_ROOT_DEFAULT = Path(__file__).resolve().parent.parent.parent
PROJECT_ROOT = Path(Variable.get('project_root', default=str(_REPO_ROOT_DEFAULT)))
ETL_PIPELINE_DIR = PROJECT_ROOT / 'etl_pipeline'
DBT_PROJECT_DIR = PROJECT_ROOT / 'dbt_dental_models'
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml in project; or use Variable for .dbt path
TABLES_YML_PATH = ETL_PIPELINE_DIR / 'etl_pipeline' / 'config' / 'tables.yml'

# Orchestration variables (set in Airflow UI — used every run, not manual follow-up steps)
ENVIRONMENT = Variable.get('etl_environment', default='clinic')
DBT_TARGET = Variable.get('dbt_target', default='local')
# When set (e.g. clinic), runs mdc publish analytics after dbt. Empty/unset skips publish.
PUBLISH_ENVIRONMENT = Variable.get('publish_environment', default='')


def _run_mdc_cmd(mdc_args: list, *, timeout_seconds: int | None = None) -> None:
    """Run mdc CLI from project root (same env isolation as manual mdc commands)."""
    run_mdc(mdc_args, cwd=PROJECT_ROOT, timeout_seconds=timeout_seconds)


def run_dbt_deps(**context):
    """Install dbt packages via mdc (loads dbt env for DBT_TARGET)."""
    _run_mdc_cmd(['dbt', 'invoke', '--env', DBT_TARGET, '--', 'deps'])


def run_dbt_build(**context):
    """Run dbt build via mdc against DBT_TARGET (local = laptop Postgres)."""
    _run_mdc_cmd(
        ['dbt', 'invoke', '--env', DBT_TARGET, '--', 'build', '--target', DBT_TARGET],
        timeout_seconds=7200,
    )


def publish_analytics(**context):
    """
    Publish local marts to clinic RDS via mdc publish analytics.

    Requires SSM tunnel on localhost (mdc tunnel clinic-db) unless publish_skip_tunnel_check.
    """
    stage = (PUBLISH_ENVIRONMENT or '').strip()
    if not stage:
        raise AirflowSkipException("publish_environment not set")

    if stage != 'clinic':
        raise AirflowException(
            f"publish_environment={stage!r} not supported (clinic only today)"
        )

    tunnel_port = Variable.get('publish_tunnel_port', default='5433')
    cmd = ['publish', 'analytics', '--env', stage, '--tunnel-port', str(tunnel_port)]
    ensure_tunnel = Variable.get('publish_ensure_tunnel', default='true').lower()
    if ensure_tunnel in ('1', 'true', 'yes'):
        cmd.append('--ensure-tunnel')
    elif Variable.get('publish_skip_tunnel_check', default='').lower() in ('1', 'true', 'yes'):
        cmd.append('--skip-tunnel-check')

    _run_mdc_cmd(cmd, timeout_seconds=3600)
    context['task_instance'].xcom_push(key='publish_success', value=True)
    return True


def refresh_schema_before_etl(**context):
    """
    Regenerate tables.yml from live OpenDental schema before ETL.

    Prevents nightly failures when the source schema changed since the last
    schema_analysis run (weekly DAG or manual). Requires same VPN access as ETL.
    """
    from lib.schema_refresh import refresh_schema_configuration

    refresh_schema_configuration(
        PROJECT_ROOT,
        ENVIRONMENT,
        task_instance=context['task_instance'],
    )
    return True


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
        
        # Check 4: Environment matches (warn when Phase A keeps clinic-sized tables.yml
        # after refusing a tiny test-OD schema refresh)
        config_environment = metadata.get('environment')
        if config_environment != ENVIRONMENT:
            table_count_preview = len(config.get('tables') or {})
            if table_count_preview >= 100:
                logging.warning(
                    "Configuration environment mismatch (continuing): "
                    "config=%s, current=%s, tables=%s — likely protected "
                    "clinic-sized tables.yml under a test Airflow Variable",
                    config_environment,
                    ENVIRONMENT,
                    table_count_preview,
                )
            else:
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

    Runs via mdc etl test-connections (ETL Pipenv), not in-process in .venv-airflow.
    """
    logging.info("Validating database connections")
    run_mdc(
        ['etl', 'test-connections', '--env', ENVIRONMENT],
        cwd=PROJECT_ROOT,
    )
    logging.info("✅ All database connections validated")
    return True


def check_schema_hash(**context):
    """
    Optional: Quick check if source schema has changed since config generation.

    Runs via mdc etl invoke check-schema-drift (ETL Pipenv).
    """
    logging.info("Checking for schema drift (quick check)")

    result = run_mdc_etl_invoke(
        ENVIRONMENT,
        ['check-schema-drift', '--config', str(TABLES_YML_PATH)],
        project_root=PROJECT_ROOT,
    )

    schema_drift = result.get('schema_drift_detected', False)
    context['task_instance'].xcom_push(key='schema_drift_detected', value=schema_drift)
    if 'drift_percentage' in result:
        context['task_instance'].xcom_push(
            key='drift_percentage', value=result['drift_percentage']
        )

    if schema_drift:
        logging.warning("⚠️  Significant schema drift detected")
    else:
        logging.info("✅ No significant schema drift detected")

    return True


# ============================================================================
# Task Functions - ETL Processing
# ============================================================================

_CATEGORY_TIMEOUT_SECONDS = {
    'large': 3 * 3600,
    'medium': 2 * 3600,
    'small': 3600,
    'tiny': 30 * 60,
}


def process_tables_by_category(category: str, **context):
    """
    Process all tables in a given performance category via mdc etl invoke run-category.
    """
    logging.info(f"Processing {category} tables")

    force_full = context['params'].get('force_full_refresh', False)
    max_workers = context['params'].get('max_workers', DEFAULT_MAX_WORKERS)

    invoke_args = [
        'run-category',
        '--category', category,
        '--parallel', str(max_workers),
        '--config', str(TABLES_YML_PATH),
    ]
    if force_full:
        invoke_args.append('--force')

    try:
        result = run_mdc_etl_invoke(
            ENVIRONMENT,
            invoke_args,
            project_root=PROJECT_ROOT,
            timeout_seconds=_CATEGORY_TIMEOUT_SECONDS.get(category),
        )
    except AirflowException as e:
        logging.error(f"❌ Failed to process {category} tables: {e}")
        raise AirflowException(f"ETL processing failed for {category} tables: {e}") from e

    if result.get('skipped') or result.get('total_count', 0) == 0:
        logging.warning(f"No tables found in category: {category}")
        raise AirflowSkipException(f"No {category} tables to process")

    success_count = result.get('success_count', 0)
    failure_count = result.get('failure_count', 0)
    total_count = result.get('total_count', 0)
    failed_tables = result.get('failed_tables', [])

    context['task_instance'].xcom_push(key='success_count', value=success_count)
    context['task_instance'].xcom_push(key='failure_count', value=failure_count)
    context['task_instance'].xcom_push(key='total_count', value=total_count)
    context['task_instance'].xcom_push(key='failed_tables', value=failed_tables)

    logging.info(
        f"✅ {category.upper()} tables processed: "
        f"{success_count} succeeded, {failure_count} failed, {total_count} total"
    )

    if failure_count > 0:
        logging.error(f"Failed tables in {category}: {failed_tables}")
        if category == 'large':
            raise AirflowException(f"Critical large table failures: {failed_tables}")
        logging.warning(f"Some {category} tables failed but pipeline continues")

    return {
        'category': category,
        'success': success_count,
        'failed': failure_count,
        'total': total_count,
    }


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

def run_layer0_replica_checks(**context):
    """
    Layer 0: MySQL source vs raw aggregate drift checks (Phase 3 Tier A).

    Runs via mdc etl invoke check-replica-drift --tier A --warn-only during rollout.
    """
    logging.info("Running Layer 0 replica drift checks (Tier A)")

    result = run_mdc_etl_invoke(
        ENVIRONMENT,
        ['check-replica-drift', '--tier', 'A', '--warn-only'],
        project_root=PROJECT_ROOT,
        timeout_seconds=600,
    )

    ti = context['task_instance']
    ti.xcom_push(key='replica_drift_detected', value=result.get('replica_drift_detected', False))
    ti.xcom_push(key='layer0_checks_run', value=result.get('checks_run', 0))
    ti.xcom_push(key='layer0_checks_failed', value=result.get('checks_failed', 0))

    if result.get('replica_drift_detected'):
        logging.warning(
            "⚠️  Layer 0 replica drift detected (%s/%s checks failed)",
            result.get('checks_failed', 0),
            result.get('checks_run', 0),
        )
    else:
        logging.info(
            "✅ Layer 0 replica checks passed (%s check(s))",
            result.get('checks_run', 0),
        )

    return True


def verify_loads(**context):
    """
    Verify that data was loaded successfully via mdc etl invoke verify-loads.
    """
    logging.info("Verifying data loads")

    result = run_mdc_etl_invoke(
        ENVIRONMENT,
        ['verify-loads'],
        project_root=PROJECT_ROOT,
    )

    context['task_instance'].xcom_push(
        key='analytics_successful',
        value=result.get('analytics_successful', 0),
    )
    context['task_instance'].xcom_push(
        key='analytics_failed',
        value=result.get('analytics_failed', 0),
    )

    logging.info("✅ Load verification completed")
    return True


def _context_run_timestamp(context: dict):
    """
    Best-effort run timestamp for reports/notifications.

    Airflow 3 may omit ``logical_date`` from the task context on manual runs
    (and asset-triggered runs). Prefer context keys, then dag_run attributes.
    """
    for key in ('logical_date', 'data_interval_start', 'dag_run_logical_date'):
        value = context.get(key)
        if value is not None:
            return value
    dag_run = context.get('dag_run')
    if dag_run is not None:
        for attr in ('logical_date', 'data_interval_start', 'run_after', 'start_date'):
            value = getattr(dag_run, attr, None)
            if value is not None:
                return value
    return None


def _format_context_timestamp(context: dict) -> str:
    ts = _context_run_timestamp(context)
    if ts is None:
        return context.get('run_id') or 'unknown'
    if hasattr(ts, 'isoformat'):
        return ts.isoformat()
    return str(ts)


def generate_pipeline_report(**context):
    """
    Generate comprehensive pipeline execution report.
    
    Aggregates results from all processing tasks and creates
    a summary report with metrics and recommendations.
    """
    logging.info("Generating pipeline execution report")

    # Collect results outside the report-formatting try so a KeyError on
    # Airflow 3 context keys cannot leave pipeline_success unset (which
    # falsely skips dbt via should_run_dbt).
    ti = context['task_instance']
    total_tables = ti.xcom_pull(task_ids='validation.validate_configuration', key='total_tables')
    config_age = ti.xcom_pull(task_ids='validation.validate_configuration', key='config_age_days')

    categories = ['large', 'medium', 'small', 'tiny']
    results_by_category = {}
    for category in categories:
        task_id = f'etl_processing.process_{category}_tables'
        success = ti.xcom_pull(task_ids=task_id, key='success_count') or 0
        failed = ti.xcom_pull(task_ids=task_id, key='failure_count') or 0
        total = ti.xcom_pull(task_ids=task_id, key='total_count') or 0
        failed_tables = ti.xcom_pull(task_ids=task_id, key='failed_tables') or []
        results_by_category[category] = {
            'success': success,
            'failed': failed,
            'total': total,
            'failed_tables': failed_tables,
        }

    total_success = sum(r['success'] for r in results_by_category.values())
    total_failed = sum(r['failed'] for r in results_by_category.values())
    total_processed = sum(r['total'] for r in results_by_category.values())
    pipeline_success = total_processed > 0 and total_failed == 0
    ti.xcom_push(key='pipeline_success', value=pipeline_success)

    analytics_successful = ti.xcom_pull(task_ids='reporting.verify_loads', key='analytics_successful') or 0
    analytics_failed = ti.xcom_pull(task_ids='reporting.verify_loads', key='analytics_failed') or 0
    schema_drift = ti.xcom_pull(task_ids='validation.check_schema_hash', key='schema_drift_detected')

    try:
        report = {
            'execution_date': _format_context_timestamp(context),
            'dag_run_id': context['run_id'],
            'environment': ENVIRONMENT,
            'configuration': {
                'total_tables_configured': total_tables,
                'config_age_days': config_age,
                'schema_drift_detected': schema_drift,
            },
            'processing_results': {
                'total_processed': total_processed,
                'total_success': total_success,
                'total_failed': total_failed,
                'success_rate': f"{(total_success/total_processed*100):.1f}%" if total_processed > 0 else "0%",
                'by_category': results_by_category,
            },
            'verification': {
                'analytics_successful': analytics_successful,
                'analytics_failed': analytics_failed,
            },
        }

        logging.info("=" * 60)
        logging.info("ETL PIPELINE EXECUTION REPORT")
        logging.info("=" * 60)
        logging.info(f"Execution Date: {report['execution_date']}")
        logging.info(f"Environment: {ENVIRONMENT}")
        logging.info(f"Configuration Age: {config_age} days")
        logging.info("")
        logging.info("Processing Results:")
        logging.info(f"  Total Processed: {total_processed}")
        logging.info(f"  Successful: {total_success}")
        logging.info(f"  Failed: {total_failed}")
        logging.info(f"  Success Rate: {report['processing_results']['success_rate']}")
        logging.info("")
        logging.info("By Category:")
        for category, results in results_by_category.items():
            logging.info(f"  {category.upper()}: {results['success']}/{results['total']} successful")
            if results['failed_tables']:
                logging.info(f"    Failed: {', '.join(results['failed_tables'][:5])}")
        logging.info("")
        logging.info("Analytics Status:")
        logging.info(f"  Successful: {analytics_successful}")
        logging.info(f"  Failed: {analytics_failed}")
        logging.info("=" * 60)

        ti.xcom_push(key='pipeline_report', value=report)
        return report

    except Exception as e:
        logging.error(f"❌ Failed to generate report: {e}")
        # Don't fail the pipeline for reporting failures; pipeline_success already pushed.
        return {'error': str(e), 'pipeline_success': pipeline_success}


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
        report = ti.xcom_pull(task_ids='reporting.generate_pipeline_report', key='pipeline_report')
        pipeline_success = ti.xcom_pull(task_ids='reporting.generate_pipeline_report', key='pipeline_success')
        
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
        message += f"dbt target: {DBT_TARGET}\n"
        if (PUBLISH_ENVIRONMENT or '').strip():
            message += f"Publish: {PUBLISH_ENVIRONMENT.strip()}\n"
        message += f"Execution: {_format_context_timestamp(context)}\n"
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

        publish_ok = ti.xcom_pull(task_ids='publish_analytics', key='publish_success')
        if publish_ok:
            message += "\n✅ Analytics published to clinic RDS.\n"
        
        # Log message
        logging.info(f"\n{level}\n{message}")
        
        # Send Slack notification if configured
        try:
            slack_webhook_url = Variable.get('slack_webhook_url', default=None)
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

    refresh_schema_task = PythonOperator(
        task_id='refresh_schema_configuration',
        python_callable=refresh_schema_before_etl,
        execution_timeout=timedelta(minutes=35),
        doc_md="""
        ### Refresh Schema Configuration

        Backs up `tables.yml` and runs `analyze_opendental_schema.py` against OpenDental
        so ETL uses current column/table definitions. Runs every nightly before validation.
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
    # LAYER 0 REPLICA CHECKS (post-ETL, pre-reporting)
    # =======================================================================
    
    with TaskGroup(group_id='layer0_replica_checks', tooltip='MySQL vs raw aggregate drift (Tier A)') as layer0_group:
        layer0_checks = PythonOperator(
            task_id='check_replica_drift_tier_a',
            python_callable=run_layer0_replica_checks,
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=timedelta(minutes=15),
            doc_md="""
            ### Layer 0 Replica Drift Checks
            
            Compares MySQL source vs raw.* aggregate totals over a 30-day window
            for procedurelog, payment, claimproc, and adjustment.
            
            Initial rollout uses `--warn-only`; flip to hard-fail per check after baseline green.
            """,
        )
    
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
        
        verify >> report
    
    # =======================================================================
    # SHORT-CIRCUIT: Run dbt only when ETL succeeded
    # =======================================================================
    
    def should_run_dbt(**context):
        """Run dbt only when pipeline_success is True (no critical ETL failures)."""
        ti = context['task_instance']
        pipeline_success = ti.xcom_pull(task_ids='reporting.generate_pipeline_report', key='pipeline_success')
        if not pipeline_success:
            logging.warning("ETL had failures; skipping dbt to avoid transforming partial data.")
            return False
        return True

    def should_run_publish(**context):
        """Run publish when configured and ETL succeeded (dbt upstream must succeed)."""
        if not (PUBLISH_ENVIRONMENT or '').strip():
            logging.info("publish_environment not set; skipping publish.")
            return False
        ti = context['task_instance']
        pipeline_success = ti.xcom_pull(task_ids='reporting.generate_pipeline_report', key='pipeline_success')
        if not pipeline_success:
            logging.warning("ETL had failures; skipping publish.")
            return False
        return True
    
    short_circuit_dbt = ShortCircuitOperator(
        task_id='should_run_dbt',
        python_callable=should_run_dbt,
        doc_md="Skips dbt when ETL reported failures so we do not transform partial data.",
    )
    report >> short_circuit_dbt
    
    # =======================================================================
    # DBT TASKS (after successful ETL) — via mdc (same env as manual runs)
    # =======================================================================
    
    with TaskGroup(group_id='dbt_build', tooltip='dbt deps and dbt build (staging → marts)') as dbt_group:
        dbt_deps = PythonOperator(
            task_id='dbt_deps',
            python_callable=run_dbt_deps,
            doc_md=f"Install dbt package dependencies (mdc dbt invoke --env {DBT_TARGET}).",
        )
        dbt_build = PythonOperator(
            task_id='dbt_build',
            python_callable=run_dbt_build,
            execution_timeout=timedelta(hours=2),
            doc_md=f"Run dbt build --target {DBT_TARGET} via mdc.",
        )
        dbt_deps >> dbt_build
    
    short_circuit_dbt >> dbt_group

    # =======================================================================
    # PUBLISH (local marts → clinic RDS)
    # =======================================================================

    short_circuit_publish = ShortCircuitOperator(
        task_id='should_run_publish',
        python_callable=should_run_publish,
        doc_md="Skips publish when publish_environment is unset or ETL failed.",
    )
    publish_analytics_task = PythonOperator(
        task_id='publish_analytics',
        python_callable=publish_analytics,
        execution_timeout=timedelta(hours=1),
        doc_md=f"""
        ### Publish analytics to RDS

        Runs `mdc publish analytics --env {PUBLISH_ENVIRONMENT or 'clinic'}` after dbt.
        Starts SSM tunnel automatically when `publish_ensure_tunnel` is true (default).
        Manual tunnel still works: `mdc tunnel clinic-db` before the run.
        """,
    )
    dbt_build >> short_circuit_publish >> publish_analytics_task

    # =======================================================================
    # NOTIFICATION (always after report; includes publish outcome when run)
    # =======================================================================

    notify = PythonOperator(
        task_id='send_completion_notification',
        python_callable=send_completion_notification,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="""
        ### Send Completion Notification

        Sends summary via logs/Slack after ETL report and optional publish step.
        """,
    )
    report >> notify
    publish_analytics_task >> notify
    
    # =======================================================================
    # DAG FLOW
    # =======================================================================

    guard_business_hours_task >> refresh_schema_task >> validation_group >> etl_group >> layer0_group >> reporting_group


# ============================================================================
# DAG Documentation
# ============================================================================

dag.doc_md = """
# ETL Pipeline DAG

This DAG orchestrates the complete OpenDental ETL pipeline that extracts
data from the source MySQL database, stages it in a replication database,
and loads it into the PostgreSQL analytics database.

## What a Run Is (Nightly)

One run = **incremental ETL** (full pass, config-driven) + **dbt build** + **publish to RDS** (when configured).

- **Incremental load**: `force_full_refresh=False` by default; each table uses `tables.yml`.
- **dbt**: `mdc dbt invoke --env {dbt_target}` then `build --target {dbt_target}` when ETL succeeded.
- **Publish**: `mdc publish analytics --env {publish_environment}` after dbt when `publish_environment` is set.
- **Failure handling**: Validation fails fast; dbt/publish skipped when ETL failed; notify runs last.

## Data Flow

```
OpenDental MySQL (Source, VPN)
    ↓ analyze_opendental_schema.py (refresh tables.yml)
    ↓ Extract / load (etl_environment=clinic)
Local PostgreSQL (raw → staging → int → marts via dbt_target=local)
    ↓ mdc publish analytics (publish_environment=clinic, SSM tunnel)
Clinic RDS (marts on api-clinic)
```

## Pipeline Phases

### 0. Business-hours guard (first task)
- **Pipeline MUST NOT run during 6 AM–8:59 PM Central** (client MySQL must not be hogged).
- Allowed window: 9 PM–5:59 AM Central only.

### 1. Schema refresh (before validation)
- Backs up and regenerates `tables.yml` from live OpenDental (`analyze_opendental_schema.py`)
- Avoids ETL failures when source schema changed since last config generation

### 2. Validation
- Validates `tables.yml` configuration
- Tests all database connections
- Checks for schema drift

### 3. ETL Processing (incremental by default)
- **Large Tables** (1M+ rows): Parallel processing with 5 workers
- **Medium Tables** (100K-1M rows): Sequential processing
- **Small Tables** (10K-100K rows): Sequential processing
- **Tiny Tables** (<10K rows): Sequential processing

### 4. Verification & Reporting
- Verifies data loads
- Generates execution report
- Sends notifications

### 5. dbt (only when ETL succeeded)
- `mdc dbt invoke --env {dbt_target}` → deps + build (default target `local`)

### 6. Publish (only when ETL succeeded and publish_environment set)
- `mdc publish analytics --env {publish_environment}` (default `clinic`)
- Auto-starts SSM tunnel when `publish_ensure_tunnel=true` (default); or run `mdc tunnel clinic-db` manually

### 7. Notification
- Summary after report and publish (Slack optional)

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

| Variable | Clinic nightly (Option A) | Phase A test |
|----------|---------------------------|--------------|
| `project_root` | Repo root on disk | same |
| `etl_environment` | `clinic` | `test` |
| `dbt_target` | `local` | `local` |
| `publish_environment` | `clinic` | *(unset — skips publish)* |
| `publish_tunnel_port` | `5433` (optional) | optional |
| `publish_ensure_tunnel` | `true` (default) | `true` — auto-start SSM tunnel for publish |
| `slack_webhook_url` | recommended | recommended |

Leave `publish_environment` unset to skip RDS publish (smoke tests). Do **not** set `dbt_target=clinic` until dbt can build safely against RDS directly.

## Performance

### Expected Runtime (clinic Option A — observed)

| Phase | Duration |
|-------|----------|
| Schema refresh (`refresh_schema_configuration`) | ~7 min (446 tables; logs Mar–Jun 2026) |
| Incremental ETL | ~25 min (typical when run every couple of days) |
| Full `dbt build` | ~52 min (optional scope; see open decision on dbt_target) |
| Publish to RDS | varies |

**~32 min** to complete schema + ETL before dbt starts. Full pipeline with dbt + publish often **~1.5–2 hours** from 9 PM Central.

Legacy category estimates (ETL-only, without schema refresh):
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
2. Review `etl_pipeline/logs/etl_pipeline/` for detailed ETL run logs
3. Contact data engineering team
"""

