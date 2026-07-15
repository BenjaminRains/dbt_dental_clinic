"""
ETL Pipeline CLI Commands
File: etl_pipeline/cli/commands.py

Core CLI commands for the ETL pipeline.

DATA FLOW FOR REFACTORING REFERENCE:
====================================
User runs: etl run --tables patient
    ↓
cli/main.py (Click group)
    ↓
cli/commands.py (run function) ← This file
    ↓
PipelineOrchestrator (framework)
    ↓
main.py (pipeline execution)

CLI FLOW:
User runs: python -m etl_pipeline
    ↓
__main__.py (redirects to CLI)
    ↓
cli/__main__.py (handles CLI execution)
    ↓
cli/main.py (Click-based)
    ↓
cli/commands.py (actual commands) ← This file
    ↓
PipelineOrchestrator (framework)
    ↓
main.py (pipeline execution)
"""

import logging
import click
import sys
import time
import shutil
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import json
import yaml
from pathlib import Path
from tabulate import tabulate
import os
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Add scripts directory to path for method tracking
scripts_path = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_path))

from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
from etl_pipeline.config.logging import get_logger, get_current_log_file_path
from etl_pipeline.config.paths import ensure_dir, schema_analysis_backups_dir
from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.config.config_reader import ConfigReader
from etl_pipeline.orchestration import PipelineOrchestrator

# =============================
# Environment Resolution Guards
# =============================
def _resolve_environment(cli_env: Optional[str]) -> str:
    """Resolve ETL environment with safe defaults.
    - If CLI flag provided, use it
    - Else use ETL_ENVIRONMENT
    - If still unset, default to 'test' (fail-fast rule: never default to production)
    """
    env = cli_env or os.environ.get("ETL_ENVIRONMENT")
    if not env:
        env = "test"
        os.environ["ETL_ENVIRONMENT"] = "test"
    return env


def _assert_env_consistency(cli_env: Optional[str], resolved_env: str) -> None:
    """Fail-fast if CLI flag conflicts with environment variable."""
    if cli_env and cli_env != resolved_env:
        raise click.UsageError(
            f"Environment mismatch: --environment={cli_env} but ETL_ENVIRONMENT={resolved_env}"
        )


def _validate_test_db_names(settings) -> None:
    """When running in test, ensure DB names include 'test' to prevent prod accidents."""
    analytics_cfg = settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
    replication_cfg = settings.get_database_config(DatabaseType.REPLICATION)
    source_cfg = settings.get_database_config(DatabaseType.SOURCE)

    names = [
        analytics_cfg.get("database", ""),
        replication_cfg.get("database", ""),
        source_cfg.get("database", ""),
    ]
    unsafe = [n for n in names if n and "test" not in n.lower()]
    if unsafe:
        raise click.UsageError(
            "SAFETY CHECK FAILED: In test mode, database names must contain 'test'. "
            f"Unsafe: {unsafe}"
        )

# Method usage tracking imports
from method_tracker import save_tracking_report, print_tracking_report

# Import custom exceptions for structured error handling
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

logger = get_logger(__name__)

# Global variable for test settings injection (used by tests)
_test_settings = None

def inject_test_settings(settings):
    """Inject test settings for testing purposes."""
    global _test_settings
    _test_settings = settings

def clear_test_settings():
    """Clear injected test settings."""
    global _test_settings
    _test_settings = None

@click.command()
@click.option('--config', '-c', type=click.Path(exists=True), help='Path to configuration file')
@click.option('--tables', '-t', multiple=True, help='Specific tables to process')
@click.option('--full', is_flag=True, help='Run full pipeline')
@click.option('--force', is_flag=True, help='Force run even if no new data')
@click.option('--parallel', '-p', type=int, default=4, help='Number of parallel workers')
@click.option('--dry-run', is_flag=True, help='Show what would be done without making changes')
@click.option('--environment', type=click.Choice(['local', 'test', 'clinic']), help='Override ETL environment (fail-fast on mismatch)')
def run(config: Optional[str], tables: List[str], full: bool, force: bool, parallel: int, dry_run: bool, environment: Optional[str]):
    """Run the ETL pipeline."""
    try:
        # Resolve and validate environment before any settings/DB access
        resolved_env = _resolve_environment(environment)
        _assert_env_consistency(environment, resolved_env)
        if resolved_env not in ('local', 'test', 'clinic'):
            # Special error message for deprecated "production" environment
            if resolved_env == 'production':
                click.echo(f"❌ Invalid ETL_ENVIRONMENT='{resolved_env}'. 'production' has been removed. Use 'local' or 'clinic'.")
            else:
                click.echo(f"❌ Invalid ETL_ENVIRONMENT='{resolved_env}'. Use 'local', 'test', or 'clinic'.")
            raise click.Abort()

        # In test mode, enforce safety on DB names to avoid prod accidents
        if resolved_env == 'test':
            try:
                settings_for_validation = get_settings()
                _validate_test_db_names(settings_for_validation)
            except Exception as e:
                click.echo(f"❌ Test environment safety check failed: {e}")
                raise click.Abort()

        # Validate parallel workers
        if parallel < 1 or parallel > 20:
            click.echo("❌ Error: Parallel workers must be between 1 and 20")
            raise click.Abort()
        
        # Initialize pipeline orchestrator with test settings if available
        if _test_settings is not None:
            # Use injected test settings
            # Note: Test config reader should be injected via fixtures
            orchestrator = PipelineOrchestrator(config_path=config, environment='test', settings=_test_settings)
        else:
            # Use default settings
            orchestrator = PipelineOrchestrator(config_path=config, environment=resolved_env)
        
        if dry_run:
            # DRY RUN MODE: Show what would be done without making changes
            _execute_dry_run(orchestrator, config, tables, full, force, parallel)
            return
        
        # Initialize connections for actual execution
        if not orchestrator.initialize_connections():
            click.echo("❌ Failed to initialize connections")
            raise click.Abort()
            
        try:
            # Handle --full flag: force full refresh for all tables
            if full:
                force = True
                click.echo("🔄 Full pipeline mode: Forcing full refresh for all tables")
                # Snapshot tracking state before full refresh to preserve history
                try:
                    from etl_pipeline.core.tracking_snapshot import snapshot_tracking_before_full_refresh
                    if snapshot_tracking_before_full_refresh(orchestrator.settings):
                        click.echo("📸 State history snapshot taken (etl_copy_status + etl_load_status)")
                    else:
                        click.echo("⚠️ State history snapshot failed (pipeline will continue)")
                except Exception as e:
                    logger.warning(f"State history snapshot error: {e}")
                    click.echo("⚠️ State history snapshot skipped (pipeline will continue)")
            
            if tables:
                # Process specific tables
                click.echo(f"📋 Processing {len(tables)} specific tables: {', '.join(tables)}")
                for table in tables:
                    success = orchestrator.run_pipeline_for_table(table, force_full=force)
                    if not success:
                        click.echo(f"❌ Failed to process table: {table}")
                        raise click.Abort()
            else:
                # Process all tables by priority
                click.echo(f"🚀 Processing all tables by priority with {parallel} workers")
                results = orchestrator.process_tables_by_priority(
                    max_workers=parallel,
                    force_full=force
                )
                
                # Check results
                for level, table_results in results.items():
                    if table_results['failed']:
                        click.echo(f"❌ Failed to process tables in {level}: {', '.join(table_results['failed'])}")
                        raise click.Abort()
                        
            click.echo("✅ Pipeline completed successfully")
            
        finally:
            orchestrator.cleanup()
            # Print log file path before method usage report
            log_path = get_current_log_file_path()
            if log_path:
                click.echo(f"📝 Log file: {log_path}")
            # Save method usage tracking report
            try:
                save_tracking_report()
                print_tracking_report()
            except Exception as e:
                logger.warning(f"Failed to save method tracking report: {e}")
            
    except ConfigurationError as e:
        logger.error(f"Configuration error in pipeline: {e}")
        click.echo(f"❌ Configuration Error: {e}")
        raise click.Abort()
    except EnvironmentError as e:
        logger.error(f"Environment error in pipeline: {e}")
        click.echo(f"❌ Environment Error: {e}")
        raise click.Abort()
    except DataExtractionError as e:
        logger.error(f"Data extraction error in pipeline: {e}")
        click.echo(f"❌ Data Extraction Error: {e}")
        raise click.Abort()
    except DataLoadingError as e:
        logger.error(f"Data loading error in pipeline: {e}")
        click.echo(f"❌ Data Loading Error: {e}")
        raise click.Abort()
    except DatabaseConnectionError as e:
        logger.error(f"Database connection error in pipeline: {e}")
        click.echo(f"❌ Database Connection Error: {e}")
        raise click.Abort()
    except DatabaseTransactionError as e:
        logger.error(f"Database transaction error in pipeline: {e}")
        click.echo(f"❌ Database Transaction Error: {e}")
        raise click.Abort()
    except Exception as e:
        logger.error(f"Unexpected error in pipeline: {str(e)}")
        click.echo(f"❌ Unexpected Error: {str(e)}")
        raise click.Abort()


def _execute_dry_run(orchestrator: PipelineOrchestrator, config: Optional[str], 
                    tables: List[str], full: bool, force: bool, parallel: int):
    """Execute dry run mode to show what would be done without making changes."""
    click.echo("🔍 DRY RUN MODE - No changes will be made")
    click.echo("=" * 60)
    
    # Show configuration
    click.echo("📋 Configuration:")
    click.echo(f"  • Config file: {config or 'Default settings'}")
    click.echo(f"  • Parallel workers: {parallel}")
    click.echo(f"  • Force full run: {force}")
    click.echo(f"  • Full pipeline mode: {full}")
    if full:
        click.echo("  • Note: Full mode will force refresh for all tables")
    
    # Test connections
    click.echo("\n🔌 Testing connections...")
    try:
        if orchestrator.initialize_connections():
            click.echo("✅ All connections successful")
        else:
            click.echo("❌ Connection test failed")
            click.echo("⚠️  Pipeline would fail during execution")
            return
    except ConfigurationError as e:
        click.echo(f"❌ Configuration Error: {e}")
        click.echo("⚠️  Pipeline would fail during execution")
        return
    except EnvironmentError as e:
        click.echo(f"❌ Environment Error: {e}")
        click.echo("⚠️  Pipeline would fail during execution")
        return
    except DatabaseConnectionError as e:
        click.echo(f"❌ Database Connection Error: {e}")
        click.echo("⚠️  Pipeline would fail during execution")
        return
    except Exception as e:
        click.echo(f"❌ Connection test failed: {str(e)}")
        click.echo("⚠️  Pipeline would fail during execution")
        return
    
    # Show loader selection hint based on environment flag
    try:
        use_refactor = os.environ.get('ETL_USE_REFACTORED_LOADER', '').lower() in ['1', 'true', 'yes']
        click.echo(f"\n🧩 Loader implementation: {'Refactored (strategy-based)' if use_refactor else 'Current (legacy)'}")
    except Exception:
        pass

    # Show processing plan
    if tables:
        click.echo(f"\n📊 Would process {len(tables)} specific tables:")
        for i, table in enumerate(tables, 1):
            click.echo(f"  {i}. {table}")
    else:
        click.echo("\n📊 Would process all tables by priority:")
        
        # Get table information from settings
        settings = orchestrator.settings
        total_tables = 0
        # Show table distribution by size instead of importance
        large_tables = []
        medium_tables = []
        small_tables = []
        
        for table_name in settings.list_tables():
            config = settings.get_table_config(table_name)
            estimated_rows = config.get('estimated_rows', 0)
            
            if estimated_rows > 1000000:  # >1M rows
                large_tables.append(table_name)
            elif estimated_rows > 10000:  # 10K-1M rows
                medium_tables.append(table_name)
            else:  # <10K rows
                small_tables.append(table_name)
        
        if large_tables:
            total_tables += len(large_tables)
            click.echo(f"  • LARGE (>1M rows): {len(large_tables)} tables")
            for table in large_tables[:3]:
                click.echo(f"    - {table}")
            if len(large_tables) > 3:
                click.echo(f"    ... and {len(large_tables) - 3} more")
        
        if medium_tables:
            total_tables += len(medium_tables)
            click.echo(f"  • MEDIUM (10K-1M rows): {len(medium_tables)} tables")
            for table in medium_tables[:3]:
                click.echo(f"    - {table}")
            if len(medium_tables) > 3:
                click.echo(f"    ... and {len(medium_tables) - 3} more")
        
        if small_tables:
            total_tables += len(small_tables)
            click.echo(f"  • SMALL (<10K rows): {len(small_tables)} tables")
            for table in small_tables[:3]:
                click.echo(f"    - {table}")
            if len(small_tables) > 3:
                click.echo(f"    ... and {len(small_tables) - 3} more")
        
        click.echo(f"\n📈 Total tables to process: {total_tables}")
    
    # Show processing strategy
    click.echo("\n⚡ Processing Strategy:")
    if tables:
        click.echo("  • Sequential processing of specified tables")
    else:
        click.echo("  • Important tables: Parallel processing for speed")
        click.echo("  • Other tables: Sequential processing to manage resources")
        click.echo(f"  • Max parallel workers: {parallel}")
    
    # Show estimated impact
    click.echo("\n📊 Estimated Impact:")
    if force or full:
        click.echo("  • Full refresh mode: All data will be re-extracted")
        click.echo("  • Longer processing time expected")
    else:
        click.echo("  • Incremental mode: Only new/changed data will be processed")
        click.echo("  • Faster processing time expected")
    
    click.echo("\n✅ Dry run completed - no changes made")
    click.echo("💡 To execute the pipeline, run without --dry-run flag")

@click.command()
@click.option('--config', '-c', required=True, help='Path to pipeline configuration file')
@click.option('--format', type=click.Choice(['table', 'json', 'summary']), 
              default='table', help='Output format (default: table)')
@click.option('--table', help='Show status for specific table only')
@click.option('--watch', is_flag=True, help='Watch status in real-time')
@click.option('--output', '-o', help='Output file for status report')
def status(config: str, format: str, table: Optional[str], watch: bool, output: Optional[str]) -> None:
    """Show current status of the ETL pipeline."""
    try:
        # Load configuration
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Initialize components with settings for persistence
        settings = get_settings()
        monitor = UnifiedMetricsCollector(settings=settings)
        
        # Get pipeline status
        status_data = monitor.get_pipeline_status(table=table)
        
        # Display status
        _display_status(status_data, format)
        
        # Handle output file if specified
        if output:
            with open(output, 'w') as f:
                if format == 'json':
                    json.dump(status_data, f, indent=2)
                else:
                    f.write(str(status_data))
            click.echo(f"✅ Status report written to {output}")
        
        # Handle watch mode
        if watch:
            click.echo("👀 Watching pipeline status (press Ctrl+C to stop)...")
            try:
                while True:
                    status_data = monitor.get_pipeline_status(table=table)
                    click.clear()
                    _display_status(status_data, format)
                    time.sleep(5)  # Update every 5 seconds
            except KeyboardInterrupt:
                click.echo("\nStopped watching.")
                
    except ConfigurationError as e:
        logger.error(f"Configuration error in status command: {e}")
        click.echo(f"❌ Configuration Error: {e}")
        raise click.ClickException(str(e))
    except EnvironmentError as e:
        logger.error(f"Environment error in status command: {e}")
        click.echo(f"❌ Environment Error: {e}")
        raise click.ClickException(str(e))
    except DatabaseConnectionError as e:
        logger.error(f"Database connection error in status command: {e}")
        click.echo(f"❌ Database Connection Error: {e}")
        raise click.ClickException(str(e))
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {str(e)}")
        click.echo(f"❌ Unexpected Error: {str(e)}")
        raise click.ClickException(str(e))

def _display_status(status_data: Dict[str, Any], format: str) -> None:
    """Display pipeline status in the specified format."""
    if format == 'json':
        click.echo(json.dumps(status_data, indent=2))
    elif format == 'summary':
        click.echo("\n📊 Pipeline Status Summary")
        click.echo("=" * 50)
        click.echo(f"Last Update: {status_data.get('last_update', 'N/A')}")
        click.echo(f"Overall Status: {status_data.get('status', 'N/A')}")
        click.echo(f"Total Tables: {len(status_data.get('tables', []))}")
        click.echo("=" * 50)
    else:  # table format
        tables = status_data.get('tables', [])
        if not tables:
            click.echo("No tables found in pipeline status.")
            return
            
        # Prepare table data
        table_data = []
        for table in tables:
            table_data.append([
                table.get('name', 'N/A'),
                table.get('status', 'N/A'),
                table.get('last_sync', 'N/A'),
                table.get('records_processed', 'N/A'),
                table.get('error', 'N/A')
            ])
        
        # Display table
        headers = ['Table', 'Status', 'Last Sync', 'Records', 'Error']
        click.echo("\n📊 Pipeline Status")
        click.echo("=" * 100)
        click.echo(tabulate(table_data, headers=headers, tablefmt='grid'))
        click.echo("=" * 100)

@click.command()
def test_connections() -> None:
    """Test all database connections."""
    try:
        # Get settings for connection configuration
        settings = get_settings()
        
        # Test OpenDental source connection
        logger.debug("Testing OpenDental source connection...")
        source_engine = ConnectionFactory.get_source_connection(settings)
        # Actually attempt to connect
        logger.debug("Attempting to connect to OpenDental source...")
        with source_engine.connect() as conn:
            pass
        logger.debug("OpenDental source connection tested.")
        click.echo("✅ OpenDental source connection: OK")
        
        # Test MySQL replication connection
        logger.debug("Testing MySQL replication connection...")
        repl_engine = ConnectionFactory.get_replication_connection(settings)
        # Actually attempt to connect
        logger.debug("Attempting to connect to MySQL replication...")
        with repl_engine.connect() as conn:
            pass
        logger.debug("MySQL replication connection tested.")
        click.echo("✅ MySQL replication connection: OK")
        
        # Test PostgreSQL analytics connection
        logger.debug("Testing PostgreSQL analytics connection...")
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        # Actually attempt to connect
        logger.debug("Attempting to connect to PostgreSQL analytics...")
        with analytics_engine.connect() as conn:
            pass
        logger.debug("PostgreSQL analytics connection tested.")
        click.echo("✅ PostgreSQL analytics connection: OK")
        
        # Clean up connections
        for engine in [source_engine, repl_engine, analytics_engine]:
            if engine:
                engine.dispose()
                
    except ConfigurationError as e:
        logger.error(f"Configuration error in connection test: {e}")
        click.echo(f"❌ Configuration Error: {e}")
        raise click.ClickException(str(e))
    except EnvironmentError as e:
        logger.error(f"Environment error in connection test: {e}")
        click.echo(f"❌ Environment Error: {e}")
        raise click.ClickException(str(e))
    except DatabaseConnectionError as e:
        logger.error(f"Database connection error in connection test: {e}")
        click.echo(f"❌ Database Connection Error: {e}")
        raise click.ClickException(str(e))
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        click.echo(f"❌ Unexpected Error: {str(e)}")
        raise click.ClickException(str(e))

@click.command()
@click.option('--backup', is_flag=True, default=True, help='Create backup of current configuration (default: true)')
@click.option('--force', is_flag=True, default=False, help='Force update even if errors detected (default: false)')
@click.option('--output-dir', type=click.Path(), default='etl_pipeline/config', help='Output directory for configuration (default: etl_pipeline/config)')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), default='INFO', help='Set logging level (default: INFO)')
def update_schema(backup: bool, force: bool, output_dir: str, log_level: str):
    """Update ETL schema configuration by running analyze_opendental_schema.py."""
    try:
        # Set logging level
        logging.getLogger().setLevel(getattr(logging, log_level))
        
        click.echo("ETL Schema Update")
        click.echo("=" * 17)
        click.echo()
        
        # Get settings for environment info
        settings = get_settings()
        environment = getattr(settings, 'environment', 'unknown')
        source_db = os.getenv('OPENDENTAL_SOURCE_DB', 'unknown')
        
        click.echo(f"Environment: {environment}")
        click.echo(f"Database: {source_db}")
        click.echo(f"Configuration: {output_dir}/tables.yml")
        click.echo()
        
        # Pre-flight checks
        logger.info("Performing pre-flight checks...")
        
        # Test database connectivity
        try:
            source_engine = ConnectionFactory.get_source_connection(settings)
            with source_engine.connect() as conn:
                pass
            source_engine.dispose()
            logger.info("Database connectivity verified")
        except Exception as e:
            error_msg = f"Database connection failed: {str(e)}"
            logger.error(error_msg)
            click.echo(f"[ERROR] {error_msg}")
            click.echo("[ERROR] Cannot proceed with schema analysis")
            click.echo()
            click.echo("Troubleshooting:")
            click.echo("1. Check database connectivity")
            click.echo("2. Verify environment variables")
            click.echo("3. Ensure database permissions")
            click.echo()
            click.echo("Configuration unchanged.")
            raise click.ClickException("Database connection failed")
        
        # Create backup if requested
        backup_path = None
        if backup:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = ensure_dir(schema_analysis_backups_dir()) / f"tables.yml.backup.{timestamp}"
            
            current_config = Path(output_dir) / "tables.yml"
            if current_config.exists():
                shutil.copy2(current_config, backup_path)
                logger.info(f"Created backup: {backup_path}")
                click.echo(f"[INFO] Creating backup: {backup_path}")
            else:
                logger.warning(f"No existing configuration found at {current_config}")
                click.echo(f"[WARNING] No existing configuration found at {current_config}")
        
        # Run schema analysis
        click.echo("[INFO] Running schema analysis...")
        logger.info("Executing analyze_opendental_schema.py script")
        
        # Get the script path
        script_path = Path(__file__).parent.parent.parent / "scripts" / "analyze_opendental_schema.py"
        if not script_path.exists():
            error_msg = f"Schema analysis script not found: {script_path}"
            logger.error(error_msg)
            click.echo(f"[ERROR] {error_msg}")
            raise click.ClickException("Schema analysis script not found")
        
        # Execute the script directly (no subprocess)
        click.echo()
        click.echo("=" * 60)
        click.echo("EXECUTING SCHEMA ANALYSIS SCRIPT")
        click.echo("=" * 60)
        click.echo()
        click.echo(f"Script: {script_path}")
        click.echo(f"Working Directory: {project_root}")
        click.echo()
        click.echo("The schema analysis script will now run directly.")
        click.echo("Follow the prompts and wait for completion.")
        click.echo()
        
        # Check if schema analysis is needed
        current_config = Path(output_dir) / "tables.yml"
        if current_config.exists() and not force:
            click.echo("Checking if schema analysis is needed...")
            try:
                with open(current_config, 'r') as f:
                    existing_config = yaml.safe_load(f)
                existing_hash = existing_config.get('metadata', {}).get('schema_hash', '')
                if existing_hash:
                    click.echo(f"Existing schema hash found: {existing_hash[:8]}...")
                    click.echo("Schema analysis may not be needed if no changes detected.")
                    click.echo("Use --force to run analysis anyway.")
                    click.echo()
            except Exception as e:
                logger.warning(f"Could not check existing configuration: {e}")
        
        # Run the script as a subprocess
        click.echo("Executing schema analysis script...")
        click.echo("This may take 4-5 minutes to complete...")
        click.echo()
        
        try:
            import subprocess
            # Run the script as a subprocess
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=project_root,
                capture_output=False,  # Show output in real-time
                text=True,
                check=True
            )
            logger.info("Schema analysis completed successfully")
        except subprocess.CalledProcessError as e:
            error_msg = f"Schema analysis failed with exit code {e.returncode}"
            logger.error(error_msg)
            click.echo(f"[ERROR] {error_msg}")
            if not force:
                raise click.ClickException("Schema analysis failed")
            else:
                click.echo("[WARNING] Proceeding with --force despite errors")
        except Exception as e:
            error_msg = f"Schema analysis failed: {str(e)}"
            logger.error(error_msg)
            click.echo(f"[ERROR] {error_msg}")
            if not force:
                raise click.ClickException("Schema analysis failed")
            else:
                click.echo("[WARNING] Proceeding with --force despite errors")
        
        # Validate new configuration
        new_config_path = Path(output_dir) / "tables.yml"
        if not new_config_path.exists():
            error_msg = f"New configuration not found: {new_config_path}"
            logger.error(error_msg)
            click.echo(f"[ERROR] {error_msg}")
            raise click.ClickException("New configuration not generated")
        
        # Validate YAML syntax
        try:
            with open(new_config_path, 'r') as f:
                yaml.safe_load(f)
            logger.info("New configuration YAML syntax validated")
        except yaml.YAMLError as e:
            error_msg = f"Invalid YAML syntax in new configuration: {str(e)}"
            logger.error(error_msg)
            click.echo(f"[ERROR] {error_msg}")
            raise click.ClickException("New configuration has invalid YAML syntax")
        
        # Detect changes (simplified version)
        changes_detected = _detect_schema_changes(backup_path, new_config_path)
        
        # Display results
        click.echo()
        if changes_detected:
            click.echo("Schema Changes Detected:")
            for change in changes_detected:
                click.echo(f"- {change}")
        else:
            click.echo("No significant schema changes detected")
        
        click.echo()
        click.echo("Configuration updated successfully!")
        if backup_path:
            click.echo(f"Backup saved: {backup_path}")
        
        click.echo()
        click.echo("Next steps:")
        click.echo("1. Test ETL with updated configuration: etl-run --tables appointment,statement,document")
        click.echo("2. Monitor for any remaining issues")
        click.echo("3. Run full ETL pipeline when ready")
        
        logger.info("Schema update completed successfully")
        
    except click.ClickException:
        raise
    except Exception as e:
        logger.error(f"Schema update failed: {str(e)}")
        click.echo(f"❌ Unexpected Error: {str(e)}")
        raise click.ClickException(str(e))

def _detect_schema_changes(backup_path: Optional[Path], new_config_path: Path) -> List[str]:
    """Detect schema changes between backup and new configuration."""
    changes = []
    
    if not backup_path or not backup_path.exists():
        changes.append("New configuration generated (no previous configuration found)")
        return changes
    
    try:
        # Load both configurations
        with open(backup_path, 'r') as f:
            old_config = yaml.safe_load(f)
        with open(new_config_path, 'r') as f:
            new_config = yaml.safe_load(f)
        
        old_tables = old_config.get('tables', {})
        new_tables = new_config.get('tables', {})
        
        # Count changes
        old_table_count = len(old_tables)
        new_table_count = len(new_tables)
        
        if new_table_count != old_table_count:
            changes.append(f"Table count changed: {old_table_count} -> {new_table_count}")
        
        # Check for strategy changes
        strategy_changes = 0
        for table_name, new_table_config in new_tables.items():
            if table_name in old_tables:
                old_strategy = old_tables[table_name].get('extraction_strategy', 'unknown')
                new_strategy = new_table_config.get('extraction_strategy', 'unknown')
                if old_strategy != new_strategy:
                    strategy_changes += 1
        
        if strategy_changes > 0:
            changes.append(f"Tables with strategy changes: {strategy_changes}")
        
        # Check for column changes (simplified)
        column_changes = 0
        for table_name, new_table_config in new_tables.items():
            if table_name in old_tables:
                old_columns = old_tables[table_name].get('incremental_columns', [])
                new_columns = new_table_config.get('incremental_columns', [])
                if old_columns != new_columns:
                    column_changes += 1
        
        if column_changes > 0:
            changes.append(f"Tables with column changes: {column_changes}")
        
        if not changes:
            changes.append("Configuration updated with no significant changes detected")
        
    except Exception as e:
        logger.warning(f"Could not detect schema changes: {str(e)}")
        changes.append("Configuration updated (change detection failed)")
    
    return changes


# ---------------------------------------------------------------------------
# Airflow orchestration helpers (invoked via `mdc etl invoke --env <stage> --`)
# Emit ###AIRFLOW_RESULT###<json> on the last line for XCom parsing in the DAG.
# ---------------------------------------------------------------------------

AIRFLOW_RESULT_PREFIX = "###AIRFLOW_RESULT###"


def _emit_airflow_result(payload: Dict[str, Any]) -> None:
    click.echo(f"{AIRFLOW_RESULT_PREFIX}{json.dumps(payload)}")


def _summarize_tracking_status(
    configured_tables: set[str],
    rows: List[tuple],
    recent_cutoff: datetime,
) -> Dict[str, Any]:
    """Summarize tracking rows against tables.yml for verify-loads."""
    cutoff = recent_cutoff.replace(tzinfo=None) if recent_cutoff.tzinfo else recent_cutoff

    def _naive_ts(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value

    by_table: Dict[str, tuple] = {}
    for table_name, status, activity_at in rows:
        by_table[table_name] = (status or "pending", _naive_ts(activity_at))

    successful: List[str] = []
    failed: List[str] = []
    missing: List[str] = []
    other: List[str] = []
    recent_success: List[str] = []

    for table in configured_tables:
        if table not in by_table:
            missing.append(table)
            continue
        status, activity_at = by_table[table]
        if status == "success":
            successful.append(table)
            if activity_at is not None and activity_at >= cutoff:
                recent_success.append(table)
        elif status == "failed":
            failed.append(table)
        else:
            other.append(table)

    latest_activity = None
    for status, activity_at in by_table.values():
        if activity_at is not None and (latest_activity is None or activity_at > latest_activity):
            latest_activity = activity_at

    return {
        "successful": len(successful),
        "failed": len(failed),
        "missing": len(missing),
        "other": len(other),
        "recent_success": len(recent_success),
        "latest_activity": latest_activity,
        "failed_tables": failed[:20],
        "missing_tables": missing[:20],
        "other_tables": other[:20],
    }


def _echo_tracking_summary(label: str, expected: int, summary: Dict[str, Any], hours: int) -> None:
    click.echo(f"{label} (configured {expected} tables):")
    click.echo(
        f"  success={summary['successful']}, failed={summary['failed']}, "
        f"missing={summary['missing']}, other={summary['other']}"
    )
    click.echo(
        f"  touched in last {hours}h: {summary['recent_success']}, "
        f"latest activity={summary['latest_activity']}"
    )
    if summary["failed_tables"]:
        click.echo(f"  failed tables: {', '.join(summary['failed_tables'])}")
    if summary["missing_tables"]:
        click.echo(f"  missing from tracking: {', '.join(summary['missing_tables'])}")


@click.command("run-category")
@click.option(
    "--category",
    required=True,
    type=click.Choice(["large", "medium", "small", "tiny"]),
)
@click.option("--parallel", "-p", default=5, type=int, help="Parallel workers (large only)")
@click.option("--force", is_flag=True, help="Force full refresh")
@click.option("--config", "-c", type=click.Path(exists=True), default=None)
@click.option("--environment", type=click.Choice(["local", "test", "clinic"]), default=None)
def run_category(
    category: str,
    parallel: int,
    force: bool,
    config: Optional[str],
    environment: Optional[str],
) -> None:
    """Process tables in a performance category (Airflow etl_processing tasks)."""
    resolved_env = _resolve_environment(environment)
    _assert_env_consistency(environment, resolved_env)

    if parallel < 1 or parallel > 20:
        click.echo("❌ Error: Parallel workers must be between 1 and 20")
        raise click.Abort()

    max_workers = parallel if category == "large" else 1
    orchestrator = PipelineOrchestrator(config_path=config, environment=resolved_env)

    try:
        if not orchestrator.initialize_connections():
            click.echo("❌ Failed to initialize connections")
            raise click.Abort()

        results = orchestrator.process_tables_by_performance_category(
            category=category,
            max_workers=max_workers,
            force_full=force,
        )

        if not results:
            click.echo(f"No tables in category: {category}")
            _emit_airflow_result(
                {
                    "category": category,
                    "success_count": 0,
                    "failure_count": 0,
                    "total_count": 0,
                    "failed_tables": [],
                    "skipped": True,
                }
            )
            return

        success_count = sum(1 for ok in results.values() if ok)
        failure_count = sum(1 for ok in results.values() if not ok)
        failed_tables = [name for name, ok in results.items() if not ok]

        click.echo(
            f"✅ {category.upper()}: {success_count} succeeded, "
            f"{failure_count} failed, {len(results)} total"
        )
        _emit_airflow_result(
            {
                "category": category,
                "success_count": success_count,
                "failure_count": failure_count,
                "total_count": len(results),
                "failed_tables": failed_tables,
                "skipped": False,
            }
        )

        if failure_count > 0:
            if category == "large":
                raise click.Abort()
            click.echo(f"⚠️ Some {category} tables failed; pipeline continues")
    finally:
        orchestrator.cleanup()


@click.command("check-schema-drift")
@click.option("--config", "-c", type=click.Path(exists=True), default=None)
@click.option("--environment", type=click.Choice(["local", "test", "clinic"]), default=None)
def check_schema_drift(config: Optional[str], environment: Optional[str]) -> None:
    """Quick schema drift check by comparing source table count to tables.yml."""
    resolved_env = _resolve_environment(environment)
    _assert_env_consistency(environment, resolved_env)

    config_path = Path(config) if config else Path(
        os.environ.get(
            "ETL_CONFIG_PATH",
            Path(__file__).resolve().parent.parent / "config" / "tables.yml",
        )
    )

    with open(config_path, "r", encoding="utf-8") as f:
        tables_config = yaml.safe_load(f)

    expected_table_count = tables_config["metadata"].get("total_tables", 0)
    schema_drift_detected = False
    drift_percentage = 0.0

    try:
        settings = get_settings()
        source_engine = ConnectionFactory.get_source_connection(settings)
        with source_engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() "
                    "AND table_type = 'BASE TABLE'"
                )
            )
            current_table_count = result.scalar()
        source_engine.dispose()

        if current_table_count != expected_table_count:
            drift_percentage = (
                abs(current_table_count - expected_table_count)
                / max(expected_table_count, 1)
                * 100
            )
            schema_drift_detected = drift_percentage > 5
            click.echo(
                f"Table count: expected={expected_table_count}, "
                f"current={current_table_count}, drift={drift_percentage:.1f}%"
            )
        else:
            click.echo(f"✅ No schema drift ({current_table_count} tables)")
    except Exception as e:
        click.echo(f"⚠️ Schema drift check failed (non-critical): {e}")

    _emit_airflow_result(
        {
            "schema_drift_detected": schema_drift_detected,
            "drift_percentage": drift_percentage,
        }
    )


@click.command("check-procedurelog-drift")
@click.option(
    "--lookback-days",
    default=30,
    show_default=True,
    type=int,
    help="Rolling window for complete-production comparison (DateComplete)",
)
@click.option(
    "--warn-only",
    is_flag=True,
    help="Report drift but do not fail (exit 0)",
)
@click.option("--environment", type=click.Choice(["local", "test", "clinic"]), default=None)
def check_procedurelog_drift_cmd(
    lookback_days: int,
    warn_only: bool,
    environment: Optional[str],
) -> None:
    """Compare MySQL source vs raw.procedurelog complete-production totals (ETL-FND-001)."""
    from etl_pipeline.monitoring.procedurelog_drift import check_procedurelog_drift

    if lookback_days < 1:
        click.echo("❌ --lookback-days must be at least 1")
        raise click.Abort()

    resolved_env = _resolve_environment(environment)
    _assert_env_consistency(environment, resolved_env)

    procedurelog_drift_detected = False
    source_rows = 0
    raw_rows = 0
    source_fees = 0.0
    raw_fees = 0.0

    try:
        settings = get_settings()
        source_engine = ConnectionFactory.get_source_connection(settings)
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

        result = check_procedurelog_drift(
            source_engine,
            analytics_engine,
            source_database=source_db,
            lookback_days=lookback_days,
        )
        source_engine.dispose()
        analytics_engine.dispose()

        procedurelog_drift_detected = result.drift_detected
        source_rows = result.source.complete_rows
        raw_rows = result.raw.complete_rows
        source_fees = float(result.source.complete_fees)
        raw_fees = float(result.raw.complete_fees)

        click.echo(result.message)
        if result.drift_detected:
            click.echo("❌ procedurelog replica drift detected")
            if not warn_only:
                raise click.Abort()
        else:
            click.echo("✅ procedurelog replica in sync")
    except click.Abort:
        raise
    except Exception as e:
        click.echo(f"⚠️ procedurelog drift check failed: {e}")
        if not warn_only:
            raise click.Abort()

    _emit_airflow_result(
        {
            "procedurelog_drift_detected": procedurelog_drift_detected,
            "lookback_days": lookback_days,
            "source_complete_rows": source_rows,
            "raw_complete_rows": raw_rows,
            "source_complete_fees": source_fees,
            "raw_complete_fees": raw_fees,
        }
    )


@click.command("check-replica-drift")
@click.option(
    "--tier",
    type=click.Choice(["A", "B", "C"], case_sensitive=False),
    default=None,
    help="Run all enabled checks in this tier",
)
@click.option(
    "--check",
    "check_ids",
    multiple=True,
    help="Run specific check id(s), e.g. L0-PAY-001 (repeatable)",
)
@click.option(
    "--lookback-days",
    default=None,
    type=int,
    help="Override lookback window for all selected checks",
)
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to replica_drift_checks.yml (default: bundled config)",
)
@click.option(
    "--warn-only",
    is_flag=True,
    help="Report drift but do not fail (exit 0)",
)
@click.option("--environment", type=click.Choice(["local", "test", "clinic"]), default=None)
def check_replica_drift_cmd(
    tier: Optional[str],
    check_ids: tuple[str, ...],
    lookback_days: Optional[int],
    config: Optional[str],
    warn_only: bool,
    environment: Optional[str],
) -> None:
    """Run Layer 0 MySQL vs raw aggregate drift checks (Phase 3)."""
    from pathlib import Path as PathLib

    from etl_pipeline.monitoring.replica_aggregate_drift import (
        load_check_definitions,
        run_replica_drift_checks,
        summary_to_airflow_payload,
    )

    if tier is None and not check_ids:
        click.echo("❌ Specify --tier or at least one --check")
        raise click.Abort()
    if lookback_days is not None and lookback_days < 1:
        click.echo("❌ --lookback-days must be at least 1")
        raise click.Abort()

    resolved_env = _resolve_environment(environment)
    _assert_env_consistency(environment, resolved_env)

    config_path = PathLib(config) if config else None
    selected_ids = list(check_ids) if check_ids else None
    try:
        preview = load_check_definitions(
            config_path,
            tier=tier,
            check_ids=selected_ids,
        )
    except Exception as exc:
        click.echo(f"❌ Failed to load replica drift checks: {exc}")
        raise click.Abort()

    if not preview:
        click.echo("❌ No matching Layer 0 checks found")
        raise click.Abort()

    summary_payload: Dict[str, Any] = {
        "replica_drift_detected": False,
        "checks_run": 0,
        "checks_passed": 0,
        "checks_failed": 0,
        "checks": [],
    }

    try:
        settings = get_settings()
        source_engine = ConnectionFactory.get_source_connection(settings)
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        source_db = settings.get_database_config(DatabaseType.SOURCE).get("database", "opendental")

        summary = run_replica_drift_checks(
            source_engine,
            analytics_engine,
            source_database=source_db,
            config_path=config_path,
            tier=tier,
            check_ids=selected_ids,
            lookback_days=lookback_days,
        )
        source_engine.dispose()
        analytics_engine.dispose()
        summary_payload = summary_to_airflow_payload(summary)

        click.echo(f"Layer 0 replica drift — {summary.checks_run} check(s)")
        for result in summary.results:
            click.echo(result.message)
            if result.drift_detected:
                click.echo(f"❌ {result.check_id} ({result.table}) drift detected")
            else:
                click.echo(f"✅ {result.check_id} ({result.table}) in sync")

        if summary.any_drift_detected:
            click.echo(
                f"❌ Layer 0 summary: {summary.checks_failed}/{summary.checks_run} check(s) failed"
            )
            if not warn_only:
                raise click.Abort()
        else:
            click.echo(f"✅ Layer 0 summary: all {summary.checks_run} check(s) passed")
    except click.Abort:
        raise
    except Exception as e:
        click.echo(f"⚠️ Layer 0 replica drift check failed: {e}")
        if not warn_only:
            raise click.Abort()

    _emit_airflow_result(summary_payload)


@click.command("verify-loads")
@click.option("--config", "-c", type=click.Path(exists=True), default=None)
@click.option(
    "--hours",
    default=36,
    show_default=True,
    type=int,
    help="Label tables copied/loaded within this many hours as recently touched",
)
@click.option("--environment", type=click.Choice(["local", "test", "clinic"]), default=None)
def verify_loads_cmd(
    config: Optional[str],
    hours: int,
    environment: Optional[str],
) -> None:
    """Verify replication and analytics tracking against tables.yml."""
    if hours < 1:
        click.echo("❌ --hours must be at least 1")
        raise click.Abort()

    resolved_env = _resolve_environment(environment)
    _assert_env_consistency(environment, resolved_env)

    config_reader = ConfigReader(config_path=config)
    configured_tables = set(config_reader.config.get("tables", {}).keys())
    expected_tables = len(configured_tables)
    recent_cutoff = datetime.now() - timedelta(hours=hours)

    settings = get_settings()
    replication_summary: Dict[str, Any] = {
        "successful": 0,
        "failed": 0,
        "missing": expected_tables,
        "other": 0,
        "recent_success": 0,
        "latest_activity": None,
        "failed_tables": [],
        "missing_tables": list(configured_tables)[:20],
        "other_tables": [],
    }
    analytics_summary = dict(replication_summary)

    replication_engine = ConnectionFactory.get_replication_connection(settings)
    try:
        with replication_engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, copy_status, last_copied
                    FROM etl_copy_status
                    """
                )
            ).fetchall()
        replication_summary = _summarize_tracking_status(
            configured_tables,
            [(row[0], row[1], row[2]) for row in rows],
            recent_cutoff,
        )
        _echo_tracking_summary("Replication", expected_tables, replication_summary, hours)
    except Exception as e:
        click.echo(f"⚠️ Replication load verification failed (non-fatal): {e}")
    finally:
        replication_engine.dispose()

    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
    try:
        with analytics_engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT table_name, load_status, _loaded_at
                    FROM raw.etl_load_status
                    """
                )
            ).fetchall()
        analytics_summary = _summarize_tracking_status(
            configured_tables,
            [(row[0], row[1], row[2]) for row in rows],
            recent_cutoff,
        )
        _echo_tracking_summary("Analytics", expected_tables, analytics_summary, hours)
    except Exception as e:
        click.echo(f"❌ Analytics load verification failed: {e}")
        raise
    finally:
        analytics_engine.dispose()

    analytics_ok = (
        analytics_summary["failed"] == 0
        and analytics_summary["missing"] == 0
        and analytics_summary["other"] == 0
        and analytics_summary["successful"] == expected_tables
    )

    if analytics_ok:
        click.echo(
            f"✅ Load verification passed: {analytics_summary['successful']}/{expected_tables} "
            f"configured tables show load_status=success"
        )
    else:
        click.echo(
            f"❌ Load verification failed: {analytics_summary['successful']}/{expected_tables} "
            f"configured tables show load_status=success"
        )
        raise click.Abort()

    _emit_airflow_result(
        {
            "expected_tables": expected_tables,
            "hours_window": hours,
            "replication_successful": replication_summary["successful"],
            "replication_failed": replication_summary["failed"],
            "replication_missing": replication_summary["missing"],
            "replication_recent": replication_summary["recent_success"],
            "analytics_successful": analytics_summary["successful"],
            "analytics_failed": analytics_summary["failed"],
            "analytics_missing": analytics_summary["missing"],
            "analytics_recent": analytics_summary["recent_success"],
        }
    )