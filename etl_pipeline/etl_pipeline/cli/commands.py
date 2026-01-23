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

TODO: Remove entry.py (argparse-based) as it's not used in current flow
"""

import logging
import click
import sys
import time
import shutil
from typing import Optional, Dict, Any, List
from datetime import datetime
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
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
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
@click.option('--environment', type=click.Choice(['test', 'clinic']), help='Override ETL environment (fail-fast on mismatch)')
def run(config: Optional[str], tables: List[str], full: bool, force: bool, parallel: int, dry_run: bool, environment: Optional[str]):
    """Run the ETL pipeline."""
    try:
        # Resolve and validate environment before any settings/DB access
        resolved_env = _resolve_environment(environment)
        _assert_env_consistency(environment, resolved_env)
        if resolved_env not in ('test', 'clinic'):
            # Special error message for deprecated "production" environment
            if resolved_env == 'production':
                click.echo(f"❌ Invalid ETL_ENVIRONMENT='{resolved_env}'. 'production' has been removed. Use 'clinic' for clinic deployment.")
            else:
                click.echo(f"❌ Invalid ETL_ENVIRONMENT='{resolved_env}'. Use 'test' or 'clinic'.")
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
            backup_dir = Path("logs/schema_analysis")
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"tables.yml.backup.{timestamp}"
            
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