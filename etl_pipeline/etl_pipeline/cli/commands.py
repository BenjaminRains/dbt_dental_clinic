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

from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config import get_settings
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

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
def run(config: Optional[str], tables: List[str], full: bool, force: bool, parallel: int, dry_run: bool):
    """Run the ETL pipeline."""
    try:
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
            orchestrator = PipelineOrchestrator(config_path=config)
        
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
    
    # Show processing plan
    if tables:
        click.echo(f"\n📊 Would process {len(tables)} specific tables:")
        for i, table in enumerate(tables, 1):
            click.echo(f"  {i}. {table}")
    else:
        click.echo("\n📊 Would process all tables by priority:")
        
        # Get table information from settings
        settings = orchestrator.settings
        importance_levels = ['important', 'audit', 'standard']
        
        total_tables = 0
        for importance in importance_levels:
            try:
                tables_in_level = settings.get_tables_by_importance(importance)
                if tables_in_level:
                    total_tables += len(tables_in_level)
                    click.echo(f"  • {importance.upper()}: {len(tables_in_level)} tables")
                    
                    # Show first few tables in each level
                    if len(tables_in_level) <= 5:
                        for table in tables_in_level:
                            click.echo(f"    - {table}")
                    else:
                        for table in tables_in_level[:3]:
                            click.echo(f"    - {table}")
                        click.echo(f"    ... and {len(tables_in_level) - 3} more")
            except ConfigurationError as e:
                click.echo(f"  • {importance.upper()}: Configuration Error - {e}")
            except EnvironmentError as e:
                click.echo(f"  • {importance.upper()}: Environment Error - {e}")
            except Exception as e:
                click.echo(f"  • {importance.upper()}: Error getting tables - {str(e)}")
        
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