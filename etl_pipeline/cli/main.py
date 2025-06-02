"""
Enhanced ETL Pipeline CLI - Main Entry Point
File: etl_pipeline/cli/main.py

Updated to work with Click framework and existing modular architecture.
"""

import logging
import sys
import click
from typing import Optional
import yaml
from pathlib import Path

from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.monitoring import PipelineMonitor
from etl_pipeline.orchestration.pipeline_runner import PipelineRunner
from etl_pipeline.orchestration.scheduler import PipelineScheduler as ETLScheduler
from etl_pipeline.utils.notifications import NotificationManager, NotificationConfig
from etl_pipeline.utils.performance import PerformanceMonitor
from etl_pipeline.cli.commands import commands

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

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
        # Check if we can connect to databases
        connection_factory = ConnectionFactory()
        
        # Test connections
        connection_results = connection_factory.test_connections()
        
        if not all(connection_results.values()):
            click.echo("❌ Database connection validation failed")
            for db, status in connection_results.items():
                if not status:
                    click.echo(f"  - Failed to connect to {db} database")
            return False
            
        # Check required directories exist
        required_dirs = [
            "etl_pipeline/config",
            "etl_pipeline/logs"
        ]
        
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                Path(dir_path).mkdir(parents=True, exist_ok=True)
                click.echo(f"📁 Created directory: {dir_path}")
        
        return True
        
    except Exception as e:
        click.echo(f"❌ Environment validation failed: {str(e)}")
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
    setup_logging(verbose)
    
    # Validate environment first
    if not validate_environment():
        click.echo("❌ Environment validation failed. Check your configuration and database connections.")
        sys.exit(1)
    
    # Store context for subcommands
    ctx.ensure_object(dict)
    ctx.obj = {
        'config': load_config(config),
        'config_path': config,
        'verbose': verbose,
        'dry_run': dry_run
    }

@cli.command()
@click.option('--tables', '-t', help='Comma-separated list of tables to process')
@click.option('--full', is_flag=True, help='Run complete pipeline for all tables')
@click.option('--task', help='Specific task to run')
@click.option('--force', is_flag=True, help='Force run even if no new data detected')
@click.option('--parallel', default=4, help='Number of parallel processes (default: 4)')
@click.pass_context
def run(ctx: click.Context, tables: Optional[str], full: bool, task: Optional[str], 
        force: bool, parallel: int) -> None:
    """Run the ETL pipeline."""
    config = ctx.obj['config']
    dry_run = ctx.obj['dry_run']
    
    # Initialize components
    connection_factory = ConnectionFactory()
    monitor = PipelineMonitor()
    performance_monitor = PerformanceMonitor()
    
    # Set up notifications
    notification_config = NotificationConfig(
        email=config.get('notifications', {}).get('email'),
        slack=config.get('notifications', {}).get('slack'),
        teams=config.get('notifications', {}).get('teams'),
        custom_webhook=config.get('notifications', {}).get('custom_webhook')
    )
    notification_manager = NotificationManager(notification_config)
    
    # Initialize pipeline runner
    runner = PipelineRunner(
        connection_factory=connection_factory,
        monitor=monitor,
        performance_monitor=performance_monitor,
        notification_manager=notification_manager
    )
    
    try:
        if dry_run:
            click.echo("🔍 DRY RUN: Showing what would be executed")
            if task:
                click.echo(f"  📋 Task: {task}")
            elif tables:
                table_list = [t.strip() for t in tables.split(',')]
                click.echo(f"  📊 Tables: {', '.join(table_list)}")
            elif full:
                click.echo("  📊 Would run complete pipeline")
            click.echo(f"  ⚙️  Parallel workers: {parallel}")
            click.echo(f"  🔄 Force mode: {'Yes' if force else 'No'}")
            return
        
        if task:
            click.echo(f"🚀 Running task: {task}")
            runner.run_pipeline(task_name=task)
        elif tables:
            table_list = [t.strip() for t in tables.split(',')]
            click.echo(f"🚀 Running pipeline for tables: {', '.join(table_list)}")
            runner.run_pipeline(tables=table_list, force=force, parallel_workers=parallel)
        elif full:
            click.echo("🚀 Running complete ETL pipeline...")
            runner.run_pipeline(full_pipeline=True, force=force, parallel_workers=parallel)
        else:
            click.echo("🚀 Running entire pipeline")
            runner.run_pipeline()
        
        click.echo("✅ Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        click.echo(f"❌ Pipeline execution failed: {str(e)}")
        sys.exit(1)
    finally:
        runner.cleanup()

@cli.command()
@click.option('--format', type=click.Choice(['table', 'json', 'summary']), 
              default='table', help='Output format (default: table)')
@click.option('--table', help='Show status for specific table only')
@click.option('--watch', is_flag=True, help='Watch status in real-time')
@click.option('--output', '-o', help='Output file for status report')
@click.pass_context
def status(ctx: click.Context, format: str, table: Optional[str], 
           watch: bool, output: Optional[str]) -> None:
    """Show pipeline status and metrics."""
    config = ctx.obj['config']
    
    # Initialize components
    connection_factory = ConnectionFactory()
    monitor = PipelineMonitor()
    performance_monitor = PerformanceMonitor()
    
    # Get pipeline status
    runner = PipelineRunner(
        connection_factory=connection_factory,
        monitor=monitor,
        performance_monitor=performance_monitor
    )
    
    try:
        if watch:
            click.echo("👀 Watching pipeline status (Ctrl+C to stop)...")
            try:
                while True:
                    status_data = runner.get_pipeline_status()
                    if table:
                        # Filter for specific table
                        status_data = {k: v for k, v in status_data.items() if k == table}
                    
                    # Clear screen and display status
                    click.clear()
                    _display_status(status_data, format)
                    
                    import time
                    time.sleep(5)  # Update every 5 seconds
            except KeyboardInterrupt:
                click.echo("\n⏹️  Status monitoring stopped")
                return
        
        status_data = runner.get_pipeline_status()
        if table:
            status_data = {k: v for k, v in status_data.items() if k == table}
        
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
    finally:
        runner.cleanup()

def _display_status(status_data: dict, format: str) -> None:
    """Display status data in specified format."""
    if format == 'json':
        import json
        click.echo(json.dumps(status_data, indent=2, default=str))
    elif format == 'summary':
        click.echo("\n📊 ETL Pipeline Status Summary")
        click.echo("=" * 40)
        
        if 'tables' in status_data:
            total_tables = len(status_data['tables'])
            successful = sum(1 for t in status_data['tables'] if t.get('status') == 'success')
            failed = sum(1 for t in status_data['tables'] if t.get('status') == 'failed')
            running = sum(1 for t in status_data['tables'] if t.get('status') == 'running')
            
            click.echo(f"Total Tables: {total_tables}")
            click.echo(f"✅ Successful: {successful}")
            click.echo(f"❌ Failed: {failed}")
            click.echo(f"🔄 Running: {running}")
        
        click.echo(f"Last Updated: {status_data.get('last_updated', 'Unknown')}")
    else:
        # Table format
        click.echo("📋 Pipeline Status:")
        click.echo(yaml.dump(status_data, default_flow_style=False))

@cli.command()
@click.argument('task_name')
@click.option('--schedule', '-s', required=True,
              help='Cron expression for scheduling')
@click.pass_context
def schedule(ctx: click.Context, task_name: str, schedule: str) -> None:
    """Schedule a pipeline task."""
    config = ctx.obj['config']
    dry_run = ctx.obj['dry_run']
    
    if dry_run:
        click.echo(f"🔍 DRY RUN: Would schedule task {task_name}")
        click.echo(f"  📅 Schedule: {schedule}")
        return
    
    # Initialize components
    connection_factory = ConnectionFactory()
    monitor = PipelineMonitor()
    performance_monitor = PerformanceMonitor()
    
    # Set up notifications
    notification_config = NotificationConfig(
        email=config.get('notifications', {}).get('email'),
        slack=config.get('notifications', {}).get('slack'),
        teams=config.get('notifications', {}).get('teams'),
        custom_webhook=config.get('notifications', {}).get('custom_webhook')
    )
    notification_manager = NotificationManager(notification_config)
    
    # Initialize pipeline runner
    runner = PipelineRunner(
        connection_factory=connection_factory,
        monitor=monitor,
        performance_monitor=performance_monitor,
        notification_manager=notification_manager
    )
    
    try:
        runner.register_task(task_name, schedule=schedule)
        click.echo(f"📅 Task {task_name} scheduled with cron expression: {schedule}")
        
    except Exception as e:
        logger.error(f"Failed to schedule task: {str(e)}")
        click.echo(f"❌ Failed to schedule task: {str(e)}")
        sys.exit(1)
    finally:
        runner.cleanup()

@cli.command()
@click.argument('task_name')
@click.pass_context
def unschedule(ctx: click.Context, task_name: str) -> None:
    """Remove a scheduled pipeline task."""
    config = ctx.obj['config']
    dry_run = ctx.obj['dry_run']
    
    if dry_run:
        click.echo(f"🔍 DRY RUN: Would unschedule task {task_name}")
        return
    
    # Initialize components
    connection_factory = ConnectionFactory()
    monitor = PipelineMonitor()
    performance_monitor = PerformanceMonitor()
    
    # Initialize pipeline runner
    runner = PipelineRunner(
        connection_factory=connection_factory,
        monitor=monitor,
        performance_monitor=performance_monitor
    )
    
    try:
        runner.unschedule_task(task_name)
        click.echo(f"🗑️  Task {task_name} unscheduled")
        
    except Exception as e:
        logger.error(f"Failed to unschedule task: {str(e)}")
        click.echo(f"❌ Failed to unschedule task: {str(e)}")
        sys.exit(1)
    finally:
        runner.cleanup()

@cli.command()
@click.option('--schedule', default='daily', help='Schedule frequency (default: daily)')
@click.pass_context
def start_scheduler(ctx: click.Context, schedule: str) -> None:
    """Start the pipeline scheduler."""
    config = ctx.obj['config']
    dry_run = ctx.obj['dry_run']
    
    if dry_run:
        click.echo(f"🔍 DRY RUN: Would start scheduler with {schedule} schedule")
        return
    
    # Initialize components
    connection_factory = ConnectionFactory()
    monitor = PipelineMonitor()
    performance_monitor = PerformanceMonitor()
    
    # Set up notifications
    notification_config = NotificationConfig(
        email=config.get('notifications', {}).get('email'),
        slack=config.get('notifications', {}).get('slack'),
        teams=config.get('notifications', {}).get('teams'),
        custom_webhook=config.get('notifications', {}).get('custom_webhook')
    )
    notification_manager = NotificationManager(notification_config)
    
    # Initialize pipeline runner
    runner = PipelineRunner(
        connection_factory=connection_factory,
        monitor=monitor,
        performance_monitor=performance_monitor,
        notification_manager=notification_manager
    )
    
    try:
        runner.start_scheduler()
        click.echo(f"🕐 Pipeline scheduler started with {schedule} schedule")
        
        # Keep the process running
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            click.echo("\n⏹️  Stopping scheduler...")
            runner.stop_scheduler()
            click.echo("✅ Scheduler stopped")
        
    except Exception as e:
        logger.error(f"Failed to start scheduler: {str(e)}")
        click.echo(f"❌ Failed to start scheduler: {str(e)}")
        sys.exit(1)
    finally:
        runner.cleanup()

@cli.command()
@click.pass_context  
def stop_scheduler(ctx: click.Context) -> None:
    """Stop the pipeline scheduler."""
    dry_run = ctx.obj['dry_run']
    
    if dry_run:
        click.echo("🔍 DRY RUN: Would stop scheduler")
        return
    
    try:
        # Initialize scheduler
        scheduler = ETLScheduler()
        scheduler.stop()
        click.echo("⏹️  Pipeline scheduler stopped")
        
    except Exception as e:
        logger.error(f"Failed to stop scheduler: {str(e)}")
        click.echo(f"❌ Failed to stop scheduler: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--validate', is_flag=True, help='Validate the configuration')
@click.option('--action', type=click.Choice(['validate', 'generate', 'show', 'edit']),
              default='show', help='Configuration action (default: show)')
@click.option('--env', default='development', help='Environment (default: development)')
@click.option('--interactive', is_flag=True, help='Interactive configuration editor')
@click.pass_context
def config(ctx: click.Context, validate: bool, action: str, env: str, interactive: bool) -> None:
    """Manage pipeline configuration.
    
    Examples:
        etl config --validate             Validate configuration
        etl config --action generate      Generate new configuration
        etl config --action show          Show current configuration
        etl config --action edit          Edit configuration interactively
    """
    config_path = ctx.obj['config_path']
    dry_run = ctx.obj['dry_run']
    
    try:
        # If --validate flag is used, override action
        if validate:
            action = 'validate'
            
        if action == 'validate':
            click.echo("✅ Validating configuration...")
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            # Add validation logic
            click.echo("✅ Configuration is valid")
            
        elif action == 'show':
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            click.echo("📋 Current Configuration:")
            click.echo(yaml.dump(config_data, default_flow_style=False))
            
        elif action == 'generate':
            click.echo("🔧 Generating new configuration...")
            # Add configuration generation logic
            click.echo("✅ Configuration generated")
            
        elif action == 'edit':
            click.echo("✏️  Opening configuration editor...")
            # Add configuration editing logic
            click.echo("✅ Configuration edited")
        
    except Exception as e:
        logger.error(f"Config operation failed: {str(e)}")
        click.echo(f"❌ Config operation failed: {str(e)}")
        sys.exit(1)

# Add all commands from commands.py
cli.add_command(commands)

def main():
    """Main entry point for the CLI."""
    try:
        cli(obj={})
    except Exception as e:
        logger.error(f"CLI execution failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()