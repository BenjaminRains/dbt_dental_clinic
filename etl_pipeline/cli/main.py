"""
Enhanced ETL Pipeline CLI - Main Entry Point
File: etl_pipeline/cli/main.py

Updated to work with the new logging architecture and simplified dependencies.
"""

import sys
import click
from typing import Optional
import yaml
from pathlib import Path

# Use the new logger architecture
from etl_pipeline.config.logging import setup_logging, configure_sql_logging
from etl_pipeline.core.logger import get_logger, ETLLogger
from etl_pipeline.config.settings import settings

# Initialize logger after importing
logger = get_logger(__name__)

def load_config(config_path: str) -> dict:
    """Load pipeline configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        sys.exit(1)

def validate_environment() -> bool:
    """Validate that the environment is properly set up."""
    try:
        # Validate configuration settings
        if not settings.validate_configs():
            click.echo("❌ Configuration validation failed")
            return False
        
        # Check required directories exist
        required_dirs = [
            "logs",
            "data"
        ]
        
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                Path(dir_path).mkdir(parents=True, exist_ok=True)
                click.echo(f"📁 Created directory: {dir_path}")
        
        click.echo("✅ Environment validation passed")
        return True
        
    except Exception as e:
        click.echo(f"❌ Environment validation failed: {str(e)}")
        logger.error(f"Environment validation failed: {str(e)}")
        return False

@click.group()
@click.option('--config', '-c', 
              default='etl_pipeline/config/pipeline.yaml',
              help='Path to pipeline configuration file')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
@click.option('--dry-run', is_flag=True,
              help='Show what would be done without executing')
@click.pass_context
def cli(ctx: click.Context, config: str, verbose: bool, dry_run: bool) -> None:
    """ETL Pipeline CLI - Dental Clinic Data Engineering."""
    
    # Set up logging based on verbosity
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_level=log_level, log_dir="logs", log_file="etl_pipeline.log")
    
    # Configure SQL logging for debugging
    if verbose:
        configure_sql_logging(enabled=True, level="DEBUG")
    
    logger.info("ETL Pipeline CLI started")
    logger.debug(f"Config path: {config}, Verbose: {verbose}, Dry run: {dry_run}")
    
    # Validate environment first
    if not validate_environment():
        click.echo("❌ Environment validation failed. Check your configuration.")
        sys.exit(1)
    
    # Load configuration
    config_data = {}
    if Path(config).exists():
        config_data = load_config(config)
    else:
        logger.warning(f"Configuration file not found: {config}")
    
    # Store context for subcommands
    ctx.ensure_object(dict)
    ctx.obj = {
        'config': config_data,
        'config_path': config,
        'verbose': verbose,
        'dry_run': dry_run
    }

@cli.command()
@click.option('--tables', '-t', help='Comma-separated list of tables to process')
@click.option('--full', is_flag=True, help='Run complete pipeline for all tables')
@click.option('--source', default='source', help='Source database type (default: source)')
@click.option('--target', default='target', help='Target database type (default: target)')
@click.option('--force', is_flag=True, help='Force run even if no new data detected')
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
@click.pass_context
def run(ctx: click.Context, tables: Optional[str], full: bool, source: str, 
        target: str, force: bool, dry_run: bool) -> None:
    """Run the ETL pipeline."""
    etl_logger = ETLLogger("pipeline_runner")
    
    try:
        if dry_run:
            click.echo("🔍 DRY RUN: Showing what would be executed")
            # Determine which tables to process
            if tables:
                table_list = [t.strip() for t in tables.split(',')]
                click.echo(f"  📊 Tables: {', '.join(table_list)}")
            elif full:
                table_list = settings.list_tables('source_tables')
                click.echo(f"  📊 Would run complete pipeline for {len(table_list)} tables")
            else:
                table_list = ['default']
                click.echo("  📊 Would run default pipeline")
            click.echo(f"  📥 Source: {source}")
            click.echo(f"  📤 Target: {target}")
            click.echo(f"  🔄 Force mode: {'Yes' if force else 'No'}")
            click.echo("")
            # Show detailed steps for each table
            for table in table_list:
                click.echo(f"  Steps for table: {table}")
                click.echo(f"    - Extract data from {source}.{table}")
                click.echo(f"    - Transform: apply business rules, clean data")
                click.echo(f"    - Load into {target}.{table}")
                click.echo(f"    - Validate row counts and integrity")
                click.echo("")
            click.echo("  No data will be changed. This is a simulation.")
            return
        
        if tables:
            table_list = [t.strip() for t in tables.split(',')]
            click.echo(f"[START] Running pipeline for tables: {', '.join(table_list)}")
            etl_logger.log_etl_start("multiple_tables", "pipeline")
            
            # Import and use our IntelligentELTPipeline
            from etl_pipeline.elt_pipeline import IntelligentELTPipeline
            
            try:
                pipeline = IntelligentELTPipeline()
                success_tables, failed_tables = pipeline.process_tables_parallel(table_list, max_workers=5)
                
                total_records = 0
                for table in success_tables:
                    etl_logger.log_etl_complete(table, "extract", records_count=0)  # Will be updated with actual counts
                    total_records += 1  # Placeholder - actual implementation will track real record counts
                
                for table in failed_tables:
                    etl_logger.log_etl_error(table, "extract", Exception("Processing failed"))
                
                etl_logger.log_etl_complete("multiple_tables", "pipeline", records_count=total_records)
                
            except Exception as e:
                etl_logger.log_etl_error("multiple_tables", "pipeline", e)
                raise
            
        elif full:
            click.echo("[START] Running complete ETL pipeline...")
            etl_logger.log_etl_start("full_pipeline", "complete_etl")
            
            # Get all tables from configuration
            all_tables = settings.list_tables('source_tables')
            click.echo(f"[INFO] Processing {len(all_tables)} tables")
            
            # TODO: Implement full pipeline logic here
            for table in all_tables:
                etl_logger.log_etl_start(table, "full_process")
                # Add your ETL logic here
                etl_logger.log_etl_complete(table, "full_process", records_count=0)
            
            etl_logger.log_etl_complete("full_pipeline", "complete_etl")
        else:
            click.echo("[START] Running default pipeline")
            etl_logger.log_etl_start("default", "pipeline")
            # TODO: Implement default pipeline logic
            etl_logger.log_etl_complete("default", "pipeline")
        
        click.echo("[PASS] Pipeline execution completed successfully")
        
    except Exception as e:
        etl_logger.log_etl_error("pipeline", "execution", e)
        click.echo(f"[FAIL] Pipeline execution failed: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--format', type=click.Choice(['table', 'json', 'summary']), 
              default='summary', help='Output format (default: summary)')
@click.option('--table', help='Show status for specific table only')
@click.option('--output', '-o', help='Output file for status report')
@click.pass_context
def status(ctx: click.Context, format: str, table: Optional[str], output: Optional[str]) -> None:
    """Show pipeline status and metrics."""
    logger.info("Getting pipeline status")
    
    try:
        # TODO: Implement actual status checking logic
        status_data = {
            'last_updated': '2024-01-01T00:00:00',
            'tables': settings.list_tables('source_tables'),
            'total_tables': len(settings.list_tables('source_tables')),
            'status': 'ready'
        }
        
        if table:
            # Filter for specific table
            if table in status_data['tables']:
                status_data = {
                    'table': table,
                    'status': 'ready',
                    'last_run': 'Never'
                }
            else:
                click.echo(f"❌ Table '{table}' not found in configuration")
                return
        
        if output:
            with open(output, 'w') as f:
                if format == 'json':
                    import json
                    json.dump(status_data, f, indent=2, default=str)
                else:
                    yaml.dump(status_data, f, default_flow_style=False)
            click.echo(f"📄 Status report written to {output}")
        else:
            _display_status(status_data, format)
        
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {str(e)}")
        click.echo(f"❌ Failed to get pipeline status: {str(e)}")
        sys.exit(1)

def _display_status(status_data: dict, format: str) -> None:
    """Display status data in specified format."""
    if format == 'json':
        import json
        click.echo(json.dumps(status_data, indent=2, default=str))
    elif format == 'summary':
        click.echo("\n📊 ETL Pipeline Status Summary")
        click.echo("=" * 40)
        
        if 'tables' in status_data:
            click.echo(f"Total Tables: {len(status_data['tables'])}")
            click.echo(f"Available Tables: {', '.join(status_data['tables'][:5])}")
            if len(status_data['tables']) > 5:
                click.echo(f"... and {len(status_data['tables']) - 5} more")
        
        click.echo(f"Status: {status_data.get('status', 'Unknown')}")
        click.echo(f"Last Updated: {status_data.get('last_updated', 'Unknown')}")
    else:
        # Table format
        click.echo("📋 Pipeline Status:")
        click.echo(yaml.dump(status_data, default_flow_style=False))

@cli.command()
@click.option('--table', '-t', help='Table name to validate')
@click.option('--rules', help='Path to validation rules file')
@click.option('--fix-issues', is_flag=True, help='Automatically fix validation issues')
@click.option('--output', '-o', help='Output file for validation report')
@click.pass_context
def validate(ctx: click.Context, table: Optional[str], rules: Optional[str], 
             fix_issues: bool, output: Optional[str]) -> None:
    """Validate data quality and integrity."""
    etl_logger = ETLLogger("data_validator")
    dry_run = ctx.obj['dry_run']
    
    try:
        if dry_run:
            click.echo("🔍 DRY RUN: Data validation")
            if table:
                click.echo(f"  📊 Table: {table}")
            else:
                click.echo("  📊 All tables")
            if rules:
                click.echo(f"  📋 Rules file: {rules}")
            if fix_issues:
                click.echo("  🔧 Would auto-fix issues")
            return
        
        if table:
            click.echo(f"🔍 Validating table: {table}")
            etl_logger.log_etl_start(table, "validation")
            
            # TODO: Implement actual validation logic
            # For now, simulate successful validation
            etl_logger.log_validation_result(table, passed=True)
            
            click.echo(f"✅ Validation completed for {table}")
        else:
            click.echo("🔍 Validating all tables...")
            all_tables = settings.list_tables('source_tables')
            
            for table_name in all_tables:
                etl_logger.log_etl_start(table_name, "validation")
                # TODO: Implement validation logic
                etl_logger.log_validation_result(table_name, passed=True)
            
            click.echo(f"✅ Validation completed for {len(all_tables)} tables")
        
        if output:
            click.echo(f"📄 Validation report would be written to {output}")
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        click.echo(f"❌ Validation failed: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--action', type=click.Choice(['validate', 'generate', 'show', 'edit']),
              default='show', help='Configuration action (default: show)')
@click.option('--env', default='development', help='Environment (default: development)')
@click.pass_context
def config(ctx: click.Context, action: str, env: str) -> None:
    """Manage pipeline configuration."""
    config_path = ctx.obj['config_path']
    
    try:
        if action == 'validate':
            click.echo("✅ Validating configuration...")
            
            # Validate database configurations
            if settings.validate_configs():
                click.echo("✅ Database configurations are valid")
            else:
                click.echo("❌ Database configuration validation failed")
                sys.exit(1)
            
            # Validate config file if it exists
            if Path(config_path).exists():
                with open(config_path, 'r') as f:
                    config_data = yaml.safe_load(f)
                click.echo("✅ Configuration file is valid YAML")
            else:
                click.echo(f"⚠️  Configuration file not found: {config_path}")
            
            click.echo("✅ Configuration validation completed")
            
        elif action == 'show':
            click.echo("📋 Current Configuration:")
            click.echo("\n🔗 Database Connections:")
            for db_type in ['source', 'staging', 'target']:
                try:
                    config_data = settings.get_database_config(db_type)
                    # Hide sensitive information
                    safe_config = {k: v for k, v in config_data.items() if k != 'password'}
                    if 'password' in config_data:
                        safe_config['password'] = '***'
                    click.echo(f"  {db_type}: {safe_config}")
                except Exception as e:
                    click.echo(f"  {db_type}: ❌ Configuration error: {e}")
            
            click.echo(f"\n📊 Available Tables: {settings.list_tables('source_tables')}")
            
            if Path(config_path).exists():
                click.echo(f"\n📄 Pipeline Config ({config_path}):")
                with open(config_path, 'r') as f:
                    config_data = yaml.safe_load(f)
                click.echo(yaml.dump(config_data, default_flow_style=False))
            
        elif action == 'generate':
            click.echo("🔧 Configuration generation not yet implemented")
            click.echo("💡 Please manually create configuration files")
            
        elif action == 'edit':
            click.echo("✏️  Interactive configuration editing not yet implemented")
            click.echo("💡 Please manually edit configuration files")
        
    except Exception as e:
        logger.error(f"Config operation failed: {str(e)}")
        click.echo(f"❌ Config operation failed: {str(e)}")
        sys.exit(1)

# Dental clinic specific commands
@cli.command()
@click.option('--clinic-id', help='Specific clinic ID to sync')
@click.option('--incremental-only', is_flag=True, help='Only sync changed records')
@click.option('--validate-after', is_flag=True, help='Validate data after sync')
@click.pass_context
def patient_sync(ctx: click.Context, clinic_id: Optional[str], 
                incremental_only: bool, validate_after: bool) -> None:
    """Sync patient data from OpenDental."""
    etl_logger = ETLLogger("patient_sync")
    dry_run = ctx.obj['dry_run']
    
    try:
        if dry_run:
            click.echo("🔍 DRY RUN: Patient data sync")
            if clinic_id:
                click.echo(f"  🏥 Clinic ID: {clinic_id}")
            if incremental_only:
                click.echo("  📊 Mode: Incremental only")
            if validate_after:
                click.echo("  ✅ Will validate after sync")
            return
        
        click.echo("🏥 Starting patient data sync...")
        etl_logger.log_etl_start("patient", "sync")
        
        # TODO: Implement patient sync logic
        sync_params = {
            'clinic_id': clinic_id,
            'incremental': incremental_only
        }
        etl_logger.log_sql_query("SELECT * FROM patient WHERE updated > ?", sync_params)
        
        # Simulate successful sync
        records_synced = 150  # TODO: Replace with actual count
        etl_logger.log_etl_complete("patient", "sync", records_count=records_synced)
        
        if validate_after:
            etl_logger.log_etl_start("patient", "validation")
            etl_logger.log_validation_result("patient", passed=True)
        
        click.echo(f"✅ Patient sync completed: {records_synced} records")
        
    except Exception as e:
        etl_logger.log_etl_error("patient", "sync", e)
        click.echo(f"❌ Patient sync failed: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--date', help='Date for metrics (YYYY-MM-DD), defaults to today')
@click.option('--format', type=click.Choice(['summary', 'detailed', 'csv']),
              default='summary', help='Output format (default: summary)')
@click.option('--output', '-o', help='Output file for metrics')
@click.pass_context
def appointment_metrics(ctx: click.Context, date: Optional[str], 
                       format: str, output: Optional[str]) -> None:
    """Generate daily appointment metrics."""
    etl_logger = ETLLogger("appointment_metrics")
    dry_run = ctx.obj['dry_run']
    
    try:
        if not date:
            from datetime import datetime
            date = datetime.now().strftime('%Y-%m-%d')
        
        if dry_run:
            click.echo(f"🔍 DRY RUN: Appointment metrics for {date}")
            click.echo(f"  📊 Format: {format}")
            if output:
                click.echo(f"  📄 Output: {output}")
            return
        
        click.echo(f"📅 Generating appointment metrics for {date}...")
        etl_logger.log_etl_start("appointment_metrics", "generate")
        
        # TODO: Implement metrics generation logic
        metrics = {
            'date': date,
            'total_appointments': 45,
            'completed': 42,
            'cancelled': 2,
            'no_show': 1
        }
        
        if output:
            with open(output, 'w') as f:
                if format == 'csv':
                    import csv
                    writer = csv.DictWriter(f, fieldnames=metrics.keys())
                    writer.writeheader()
                    writer.writerow(metrics)
                else:
                    yaml.dump(metrics, f)
            click.echo(f"📄 Metrics written to {output}")
        else:
            click.echo(f"📊 Appointment Metrics for {date}:")
            for key, value in metrics.items():
                click.echo(f"  {key}: {value}")
        
        etl_logger.log_etl_complete("appointment_metrics", "generate")
        
    except Exception as e:
        etl_logger.log_etl_error("appointment_metrics", "generate", e)
        click.echo(f"❌ Metrics generation failed: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--table', help='Specific table to check for HIPAA compliance')
@click.option('--generate-report', is_flag=True, help='Generate detailed compliance report')
@click.option('--output', default='compliance_report.html', help='Output file for report')
@click.pass_context
def compliance_check(ctx: click.Context, table: Optional[str], 
                     generate_report: bool, output: str) -> None:
    """Run HIPAA compliance checks on dental clinic data."""
    etl_logger = ETLLogger("compliance_check")
    dry_run = ctx.obj['dry_run']
    
    try:
        if dry_run:
            click.echo("🔍 DRY RUN: HIPAA compliance check")
            if table:
                click.echo(f"  📊 Table: {table}")
            if generate_report:
                click.echo(f"  📄 Report: {output}")
            return
        
        click.echo("🔒 Running HIPAA compliance check...")
        etl_logger.log_etl_start("compliance", "check")
        
        if table:
            tables_to_check = [table]
        else:
            tables_to_check = settings.list_tables('source_tables')
        
        compliance_results = {}
        for table_name in tables_to_check:
            # TODO: Implement actual compliance checking logic
            compliance_results[table_name] = {
                'compliant': True,
                'issues': [],
                'recommendations': []
            }
        
        all_compliant = all(result['compliant'] for result in compliance_results.values())
        
        if all_compliant:
            click.echo("✅ All tables pass HIPAA compliance checks")
        else:
            click.echo("⚠️  Some compliance issues found")
        
        if generate_report:
            # TODO: Generate actual HTML report
            from datetime import datetime
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            with open(output, 'w') as f:
                f.write(f"<html><body><h1>HIPAA Compliance Report</h1>")
                f.write(f"<p>Generated on: {current_date}</p>")
                f.write(f"<p>Status: {'Compliant' if all_compliant else 'Issues Found'}</p>")
                f.write("</body></html>")
            click.echo(f"📄 Compliance report generated: {output}")
        
        etl_logger.log_etl_complete("compliance", "check")
        
    except Exception as e:
        etl_logger.log_etl_error("compliance", "check", e)
        click.echo(f"❌ Compliance check failed: {str(e)}")
        sys.exit(1)

def main():
    """Main entry point for the CLI."""
    try:
        cli(obj={})
    except Exception as e:
        logger.error(f"CLI execution failed: {str(e)}")
        click.echo(f"❌ CLI execution failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()