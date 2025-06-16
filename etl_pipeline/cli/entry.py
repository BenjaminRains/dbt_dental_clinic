#!/usr/bin/env python3
"""
CLI entry point for the ETL pipeline.
"""
import os
import sys
import argparse
from pathlib import Path

# Ensure the project root is in Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='ETL Pipeline CLI')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1.0')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # ETL run command
    run_parser = subparsers.add_parser('run', help='Run the ETL pipeline')
    run_parser.add_argument('--phase', choices=['1', '2', '3', '4'], default='1',
                           help='Implementation phase to run')
    run_parser.add_argument('--tables', type=str, nargs='+',
                           help='Specific tables to process')
    run_parser.add_argument('--importance', choices=['critical', 'important', 'audit', 'reference'],
                           help='Process tables by importance level')
    run_parser.add_argument('--dry-run', action='store_true',
                           help='Show what would be processed without executing')
    run_parser.add_argument('--config', type=str, default='etl_pipeline/config/tables.yaml',
                           help='Path to table configuration file')
    run_parser.add_argument('--force-full', action='store_true',
                           help='Force full extraction for all tables')
    
    # Health check command
    health_parser = subparsers.add_parser('health', help='Run health checks')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show pipeline status')
    
    args = parser.parse_args()
    
    if args.command == 'run':
        run_etl_pipeline(args)
    elif args.command == 'health':
        run_health_checks()
    elif args.command == 'status':
        show_status()
    else:
        parser.print_help()

def run_etl_pipeline(args):
    """Run the ETL pipeline with given arguments."""
    try:
        from etl_pipeline.elt_pipeline import IntelligentELTPipeline
        import logging
        
        # Setup basic logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        print("🚀 Starting ETL pipeline...")
        
        # Initialize pipeline
        pipeline = IntelligentELTPipeline(config_path=args.config)
        
        if args.dry_run:
            print("📋 DRY RUN MODE - Showing what would be processed")
            
            if args.tables:
                tables = args.tables
            elif args.importance:
                tables = pipeline.get_tables_by_priority(args.importance)
            else:
                tables = pipeline.get_critical_tables()
            
            print(f"📊 Would process {len(tables)} tables: {', '.join(tables)}")
            
            for table in tables:
                config = pipeline.get_table_config(table)
                print(f"  - {table}: {config.get('table_importance')} "
                      f"({config.get('estimated_size_mb', 0):.1f} MB, "
                      f"{config.get('extraction_strategy')} extraction)")
            return
        
        # Initialize connections
        pipeline.initialize_connections()
        
        # Execute based on arguments
        if args.tables:
            print(f"📋 Processing specific tables: {', '.join(args.tables)}")
            success_count = 0
            for table in args.tables:
                if pipeline.run_elt_pipeline(table, force_full=args.force_full):
                    success_count += 1
            
            print(f"✅ Completed: {success_count}/{len(args.tables)} tables successful")
            
        elif args.importance:
            print(f"📋 Processing {args.importance} tables")
            results = pipeline.process_tables_by_priority([args.importance], force_full=args.force_full)
            
        elif args.phase == '1':
            if not pipeline.run_phase1_critical_tables():
                print("❌ Phase 1 failed")
                sys.exit(1)
                
        elif args.phase == '2':
            print("📋 Starting Phase 2: Critical + Important Tables")
            results = pipeline.process_tables_by_priority(['critical', 'important'], force_full=args.force_full)
            
        elif args.phase == '3':
            print("📋 Starting Phase 3: Critical + Important + Audit Tables")
            results = pipeline.process_tables_by_priority(['critical', 'important', 'audit'], force_full=args.force_full)
            
        elif args.phase == '4':
            print("📋 Starting Phase 4: All Tables")
            results = pipeline.process_tables_by_priority(force_full=args.force_full)
        
        print("✅ ETL Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        sys.exit(1)

def run_health_checks():
    """Run system health checks."""
    try:
        from etl_pipeline.core.connections import ConnectionFactory
        
        print("🏥 Running health checks...")
        
        # Test database connections
        print("🔍 Testing database connections...")
        
        try:
            source_engine = ConnectionFactory.get_opendental_source_connection()
            print("  ✅ OpenDental source connection: OK")
            source_engine.dispose()
        except Exception as e:
            print(f"  ❌ OpenDental source connection: FAILED - {e}")
        
        try:
            replication_engine = ConnectionFactory.get_mysql_replication_connection()
            print("  ✅ MySQL replication connection: OK")
            replication_engine.dispose()
        except Exception as e:
            print(f"  ❌ MySQL replication connection: FAILED - {e}")
        
        try:
            analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
            print("  ✅ PostgreSQL analytics connection: OK")
            analytics_engine.dispose()
        except Exception as e:
            print(f"  ❌ PostgreSQL analytics connection: FAILED - {e}")
        
        print("🏥 Health checks completed!")
        
    except Exception as e:
        print(f"❌ Health checks failed: {str(e)}")
        sys.exit(1)

def show_status():
    """Show pipeline status."""
    print("📊 Pipeline Status")
    print("==================")
    print("Feature not yet implemented.")
    print("Will show:")
    print("  - Last run times")
    print("  - Table processing status")
    print("  - Data freshness")
    print("  - Error counts")

if __name__ == "__main__":
    main()