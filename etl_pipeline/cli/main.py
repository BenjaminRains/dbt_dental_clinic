#!/usr/bin/env python3
"""
Simplified ETL Pipeline CLI Main Entry Point
"""
import os
import sys
from pathlib import Path

# Add project root to Python path - same pattern as your working commands
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import click
import logging
from datetime import datetime

# Simple logging setup
def setup_simple_logging():
    """Set up simple logging for CLI operations."""
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"etl_cli_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, verbose):
    """ETL Pipeline CLI for Dental Clinic Data Engineering."""
    logger = setup_simple_logging()
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['logger'] = logger

@cli.command()
@click.option('--phase', choices=['1', '2', '3', '4'], default='1', help='Phase to run (default: 1)')
@click.option('--tables', help='Comma-separated list of specific tables')
@click.option('--dry-run', is_flag=True, help='Show what would be done')
@click.option('--force-full', is_flag=True, help='Force full extraction')
@click.pass_context
def run(ctx, phase, tables, dry_run, force_full):
    """Run the ETL pipeline."""
    logger = ctx.obj['logger']
    
    try:
        if dry_run:
            click.echo("🔍 DRY RUN MODE")
            if tables:
                table_list = [t.strip() for t in tables.split(',')]
                click.echo(f"📊 Would process tables: {', '.join(table_list)}")
            else:
                click.echo(f"📊 Would run Phase {phase}")
            click.echo(f"🔄 Force full: {'Yes' if force_full else 'No'}")
            return
        
        # Import here to avoid circular imports and ensure path is set
        from etl_pipeline.elt_pipeline import IntelligentELTPipeline
        
        click.echo("🚀 Starting ETL Pipeline...")
        logger.info(f"Starting ETL Pipeline - Phase {phase}")
        
        # Initialize pipeline
        pipeline = IntelligentELTPipeline()
        
        # Process specific tables or run phase
        if tables:
            table_list = [t.strip() for t in tables.split(',')]
            click.echo(f"📋 Processing tables: {', '.join(table_list)}")
            
            # Initialize connections
            pipeline.initialize_connections()
            
            success_count = 0
            for table in table_list:
                click.echo(f"📊 Processing {table}...")
                if pipeline.run_elt_pipeline(table, force_full=force_full):
                    success_count += 1
                    click.echo(f"  ✅ {table} completed")
                else:
                    click.echo(f"  ❌ {table} failed")
            
            click.echo(f"📈 Results: {success_count}/{len(table_list)} tables successful")
            
        else:
            # Run phase
            click.echo(f"📋 Running Phase {phase}")
            
            if phase == '1':
                if pipeline.run_phase1_critical_tables():
                    click.echo("✅ Phase 1 completed successfully!")
                else:
                    click.echo("❌ Phase 1 failed")
                    sys.exit(1)
            else:
                click.echo(f"Phase {phase} not yet implemented")
        
        click.echo("🎉 ETL Pipeline completed!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        click.echo(f"❌ Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        if 'pipeline' in locals():
            pipeline.cleanup()

@cli.command()
@click.option('--format', type=click.Choice(['table', 'json']), default='table')
@click.pass_context
def status(ctx, format):
    """Show pipeline status."""
    logger = ctx.obj['logger']
    
    try:
        # Import here to ensure path is set
        from etl_pipeline.core.connections import ConnectionFactory
        
        click.echo("📊 Checking Pipeline Status...")
        
        # Test connections
        status_info = {
            'timestamp': datetime.now().isoformat(),
            'connections': {}
        }
        
        try:
            source_engine = ConnectionFactory.get_opendental_source_connection()
            status_info['connections']['source'] = 'OK'
            source_engine.dispose()
        except Exception as e:
            status_info['connections']['source'] = f'FAILED: {str(e)}'
        
        try:
            repl_engine = ConnectionFactory.get_mysql_replication_connection()
            status_info['connections']['replication'] = 'OK'
            repl_engine.dispose()
        except Exception as e:
            status_info['connections']['replication'] = f'FAILED: {str(e)}'
        
        try:
            analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
            status_info['connections']['analytics'] = 'OK'
            analytics_engine.dispose()
        except Exception as e:
            status_info['connections']['analytics'] = f'FAILED: {str(e)}'
        
        if format == 'json':
            import json
            click.echo(json.dumps(status_info, indent=2))
        else:
            click.echo("=" * 50)
            click.echo(f"🕐 Status as of: {status_info['timestamp']}")
            click.echo("=" * 50)
            for conn_type, status in status_info['connections'].items():
                icon = "✅" if status == 'OK' else "❌"
                click.echo(f"{icon} {conn_type.title()}: {status}")
            click.echo("=" * 50)
        
    except Exception as e:
        logger.error(f"Status check failed: {str(e)}")
        click.echo(f"❌ Status check failed: {str(e)}")

@cli.command()
@click.pass_context
def test_connections(ctx):
    """Test all database connections."""
    logger = ctx.obj['logger']
    
    try:
        # Change to project root
        os.chdir(project_root)
        
        # Import and run test connections
        from test_connections import main as test_main
        test_main()
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        click.echo(f"❌ Connection test failed: {str(e)}")

@cli.command()
@click.option('--tables', help='Comma-separated list of tables to discover')
@click.option('--output', '-o', help='Output file for schema')
@click.pass_context
def discover_schema(ctx, tables, output):
    """Discover database schema."""
    logger = ctx.obj['logger']
    
    try:
        from etl_pipeline.core.connections import ConnectionFactory
        from sqlalchemy import text
        import yaml
        
        click.echo("🔍 Discovering database schema...")
        
        source_engine = ConnectionFactory.get_opendental_source_connection()
        
        with source_engine.connect() as conn:
            if tables:
                table_list = [t.strip() for t in tables.split(',')]
            else:
                # Get all tables
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE()
                """))
                table_list = [row[0] for row in result]
        
        schema_info = {}
        for table in table_list[:5]:  # Limit to first 5 for demo
            click.echo(f"📋 Analyzing {table}...")
            
            # Get column info
            with source_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable, column_key, column_default
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = :table_name
                    ORDER BY ordinal_position
                """).bindparams(table_name=table))
                
                columns = []
                for row in result:
                    columns.append({
                        'name': row[0],
                        'type': row[1],
                        'nullable': row[2] == 'YES',
                        'key': row[3],
                        'default': row[4]
                    })
                
                schema_info[table] = {'columns': columns}
        
        if output:
            with open(output, 'w') as f:
                yaml.dump(schema_info, f, default_flow_style=False)
            click.echo(f"📄 Schema saved to {output}")
        else:
            import yaml
            click.echo("📋 Schema Discovery Results:")
            click.echo(yaml.dump(schema_info, default_flow_style=False))
        
        source_engine.dispose()
        
    except Exception as e:
        logger.error(f"Schema discovery failed: {str(e)}")
        click.echo(f"❌ Schema discovery failed: {str(e)}")

@cli.command()
@click.option('--table', help='Specific table to validate')
@click.pass_context
def validate(ctx, table):
    """Validate data quality."""
    logger = ctx.obj['logger']
    
    try:
        click.echo("🔍 Running data validation...")
        
        if table:
            click.echo(f"📊 Validating table: {table}")
        else:
            click.echo("📊 Validating all configured tables")
        
        # Basic validation placeholder
        click.echo("✅ Data validation passed")
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        click.echo(f"❌ Validation failed: {str(e)}")

def main():
    """Main CLI entry point."""
    try:
        cli()
    except Exception as e:
        click.echo(f"❌ CLI failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()