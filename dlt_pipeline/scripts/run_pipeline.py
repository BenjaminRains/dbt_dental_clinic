#!/usr/bin/env python3
"""
Simplified dlt pipeline runner with two main functions:
1. Full sync of all/selected tables
2. Incremental sync of configured tables

This replaces the complex run_pipeline.py with a minimal implementation.
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

import dlt
from sources.opendental import opendental_source, opendental_incremental_source
from config.settings import (
    get_table_configs, 
    get_tables_by_importance, 
    get_incremental_tables,
    validate_environment
)
from utils.helpers import log_pipeline_results, create_simple_report, format_duration

def setup_logging():
    """Setup logging for pipeline execution"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/pipeline_{timestamp}.log'
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def create_pipeline(test_mode: bool = False) -> dlt.Pipeline:
    """Create and return dlt pipeline instance"""
    dataset_name = 'raw_test' if test_mode else 'raw'
    pipeline_name = 'opendental_etl_test' if test_mode else 'opendental_etl'
    
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination='postgres',
        dataset_name=dataset_name
    )

def run_full_sync(
    tables: list = None, 
    importance_filter: list = None,
    test_mode: bool = False
) -> bool:
    """
    Run full sync of specified tables or all tables.
    
    Args:
        tables: Specific table names to sync
        importance_filter: Filter by importance ['critical', 'important', 'audit', 'reference']
        test_mode: Use test dataset if True
    
    Returns:
        bool: True if successful
    """
    logger.info("="*60)
    logger.info("STARTING FULL SYNC")
    logger.info("="*60)
    
    start_time = datetime.now()
    
    try:
        # Create pipeline
        pipeline = create_pipeline(test_mode)
        
        # Create source with specified filters
        if tables:
            logger.info(f"Full sync of specific tables: {', '.join(tables)}")
            source = opendental_source(table_filter=tables)
        elif importance_filter:
            logger.info(f"Full sync of {importance_filter} importance tables")
            source = opendental_source(importance_filter=importance_filter)
        else:
            logger.info("Full sync of ALL configured tables")
            source = opendental_source()
        
        # Force full refresh by clearing relevant state
        if not test_mode:  # Only clear state in production mode
            logger.info("Clearing pipeline state to ensure full refresh...")
            pipeline.drop()  # This forces a complete refresh
            pipeline = create_pipeline(test_mode)  # Recreate pipeline
        
        # Run pipeline
        logger.info("Starting data extraction and loading...")
        load_info = pipeline.run(source)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Log results
        logger.info(f"✅ Full sync completed successfully in {format_duration(duration)}")
        log_pipeline_results(load_info, "Full Sync")
        
        # Create report
        if not test_mode:
            create_simple_report(pipeline, f"full_sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Full sync failed: {str(e)}")
        logger.exception("Full stack trace:")
        return False

def run_incremental_sync(test_mode: bool = False) -> bool:
    """
    Run incremental sync of all tables configured for incremental loading.
    
    Args:
        test_mode: Use test dataset if True
    
    Returns:
        bool: True if successful
    """
    logger.info("="*60)
    logger.info("STARTING INCREMENTAL SYNC")
    logger.info("="*60)
    
    start_time = datetime.now()
    
    try:
        # Create pipeline
        pipeline = create_pipeline(test_mode)
        
        # Get incremental tables
        incremental_tables = get_incremental_tables()
        
        if not incremental_tables:
            logger.warning("No tables configured for incremental loading")
            return True
        
        logger.info(f"Incremental sync of {len(incremental_tables)} tables: {', '.join(incremental_tables)}")
        
        # Create incremental source
        source = opendental_incremental_source()
        
        # Run pipeline
        logger.info("Starting incremental data extraction and loading...")
        load_info = pipeline.run(source)
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Log results
        logger.info(f"✅ Incremental sync completed successfully in {format_duration(duration)}")
        log_pipeline_results(load_info, "Incremental Sync")
        
        # Create report
        if not test_mode:
            create_simple_report(pipeline, f"incremental_sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Incremental sync failed: {str(e)}")
        logger.exception("Full stack trace:")
        return False

def main():
    global logger
    logger = setup_logging()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run OpenDental dlt ETL Pipeline')
    
    # Sync type (mutually exclusive)
    sync_group = parser.add_mutually_exclusive_group(required=True)
    sync_group.add_argument('--full', action='store_true', help='Run full sync')
    sync_group.add_argument('--incremental', action='store_true', help='Run incremental sync')
    
    # Full sync options
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync (for full sync only)')
    parser.add_argument('--importance', choices=['critical', 'important', 'audit', 'reference'], 
                       nargs='+', help='Filter by table importance (for full sync only)')
    
    # General options
    parser.add_argument('--test', action='store_true', help='Use test dataset (raw_test)')
    parser.add_argument('--validate-only', action='store_true', help='Only validate environment, don\'t run pipeline')
    
    args = parser.parse_args()
    
    # Validate environment
    logger.info("Validating environment...")
    if not validate_environment():
        logger.error("❌ Environment validation failed - check your .env file")
        sys.exit(1)
    
    logger.info("✅ Environment validation passed")
    
    # If validate-only, exit here
    if args.validate_only:
        logger.info("Environment validation completed successfully")
        sys.exit(0)
    
    # Log configuration info
    table_configs = get_table_configs()
    logger.info(f"Loaded configuration for {len(table_configs)} tables")
    
    # Show table counts by importance
    for importance in ['critical', 'important', 'audit', 'reference']:
        tables = get_tables_by_importance(importance)
        if tables:
            logger.info(f"  {importance.title()}: {len(tables)} tables")
    
    incremental_tables = get_incremental_tables()
    logger.info(f"  Incremental: {len(incremental_tables)} tables")
    
    # Run the appropriate sync
    success = False
    
    if args.full:
        # Validate arguments for full sync
        if args.tables and args.importance:
            logger.error("❌ Cannot specify both --tables and --importance")
            sys.exit(1)
        
        success = run_full_sync(
            tables=args.tables,
            importance_filter=args.importance,
            test_mode=args.test
        )
    
    elif args.incremental:
        # Validate arguments for incremental sync
        if args.tables or args.importance:
            logger.warning("--tables and --importance are ignored for incremental sync")
        
        success = run_incremental_sync(test_mode=args.test)
    
    # Exit with appropriate code
    if success:
        logger.info("🎉 Pipeline completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()