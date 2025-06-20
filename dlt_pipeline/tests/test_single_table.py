#!/usr/bin/env python3
"""
Single table testing script for dlt pipeline development.
Use this to test individual tables before running the full pipeline.

Usage:
    python scripts/test_single_table.py securitylog
    python scripts/test_single_table.py patient --full-sync
    python scripts/test_single_table.py appointment --incremental --limit 1000
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
from sources.opendental import opendental_source
from config.settings import get_table_configs, get_dlt_destination_config, validate_environment
from utils.helpers import log_pipeline_results, get_table_info, format_duration

def setup_logging():
    """Setup logging for test script"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/test_single_table_{timestamp}.log'
    
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

def validate_table_config(table_name: str, table_configs: dict) -> dict:
    """Validate that the table has proper configuration"""
    if table_name not in table_configs:
        raise ValueError(f"Table '{table_name}' not found in configuration")
    
    config = table_configs[table_name]
    
    # Log configuration details
    logger.info(f"Table Configuration for {table_name}:")
    logger.info(f"  Importance: {config.get('table_importance', 'N/A')}")
    logger.info(f"  Strategy: {config.get('extraction_strategy', 'N/A')}")
    logger.info(f"  Incremental Column: {config.get('incremental_column', 'N/A')}")
    
    return config

def test_source_connection(table_name: str):
    """Test that we can connect to source and detect the table"""
    logger.info(f"Testing source connection for table: {table_name}")
    
    try:
        # Create source with just this table
        source = opendental_source(table_filter=[table_name])
        
        # Check that the table is in the source
        resource_names = list(source.resources.keys())
        logger.info(f"Available resources: {resource_names}")
        
        if table_name not in resource_names:
            raise ValueError(f"Table {table_name} not found in source. Available: {resource_names}")
        
        logger.info(f"✅ Table {table_name} found in source")
        return source
        
    except Exception as e:
        logger.error(f"❌ Source connection test failed: {str(e)}")
        raise

def test_small_sample(table_name: str, source, limit: int = 10):
    """Test extracting a small sample of data"""
    logger.info(f"Testing small sample extraction: {limit} rows from {table_name}")
    
    try:
        # Get the resource
        resource = source.resources[table_name]
        
        # Extract small sample
        rows = []
        for i, row in enumerate(resource):
            if i >= limit:
                break
            rows.append(row)
        
        logger.info(f"✅ Successfully extracted {len(rows)} sample rows")
        
        if rows:
            logger.info("Sample row keys:")
            for key in list(rows[0].keys())[:10]:  # Show first 10 columns
                logger.info(f"  - {key}")
            if len(rows[0].keys()) > 10:
                logger.info(f"  ... and {len(rows[0].keys()) - 10} more columns")
        
        return rows
        
    except Exception as e:
        logger.error(f"❌ Sample extraction failed: {str(e)}")
        raise

def run_full_sync(table_name: str, pipeline):
    """Run a full sync of the table"""
    logger.info(f"Running FULL SYNC for table: {table_name}")
    
    start_time = datetime.now()
    
    try:
        # Create source with full table load
        source = opendental_source(table_filter=[table_name])
        
        # Force full refresh by dropping existing state for this table
        if table_name in pipeline.state.get('sources', {}).get('opendental', {}).get('resources', {}):
            logger.info(f"Clearing existing state for {table_name} to force full sync")
            del pipeline.state['sources']['opendental']['resources'][table_name]
            pipeline.state.save()
        
        # Run pipeline
        load_info = pipeline.run(source)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"✅ Full sync completed in {format_duration(duration)}")
        log_pipeline_results(load_info, f"Full Sync - {table_name}")
        
        return load_info
        
    except Exception as e:
        logger.error(f"❌ Full sync failed: {str(e)}")
        raise

def run_incremental_sync(table_name: str, pipeline):
    """Run an incremental sync of the table"""
    logger.info(f"Running INCREMENTAL SYNC for table: {table_name}")
    
    start_time = datetime.now()
    
    try:
        # Create source with incremental loading
        source = opendental_source(table_filter=[table_name])
        
        # Run pipeline (should pick up from last state)
        load_info = pipeline.run(source)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"✅ Incremental sync completed in {format_duration(duration)}")
        log_pipeline_results(load_info, f"Incremental Sync - {table_name}")
        
        return load_info
        
    except Exception as e:
        logger.error(f"❌ Incremental sync failed: {str(e)}")
        raise

def verify_data_in_destination(table_name: str, pipeline):
    """Verify data was loaded correctly in destination"""
    logger.info(f"Verifying data in destination for table: {table_name}")
    
    try:
        table_info = get_table_info(pipeline, table_name)
        
        if table_info is None:
            logger.error(f"❌ Table {table_name} not found in destination")
            return False
        
        logger.info(f"✅ Table verification:")
        logger.info(f"  Rows: {table_info['row_count']:,}")
        logger.info(f"  Columns: {table_info['column_count']}")
        logger.info(f"  Memory: {table_info['memory_usage_mb']:.1f} MB")
        logger.info(f"  Last Updated: {table_info['last_updated']}")
        
        # Show some column names
        columns = table_info['columns'][:10]
        logger.info(f"  Sample Columns: {', '.join(columns)}")
        if len(table_info['columns']) > 10:
            logger.info(f"    ... and {len(table_info['columns']) - 10} more")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Data verification failed: {str(e)}")
        return False

def check_incremental_state(table_name: str, pipeline):
    """Check the incremental state for the table"""
    logger.info(f"Checking incremental state for table: {table_name}")
    
    try:
        state = pipeline.state
        sources = state.get('sources', {})
        opendental_state = sources.get('opendental', {})
        resources = opendental_state.get('resources', {})
        table_state = resources.get(table_name, {})
        
        if not table_state:
            logger.info("No incremental state found (table may not use incremental loading)")
            return
        
        logger.info("Incremental State:")
        for key, value in table_state.items():
            logger.info(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"❌ Error checking incremental state: {str(e)}")

def main():
    global logger
    logger = setup_logging()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test single table in dlt pipeline')
    parser.add_argument('table_name', help='Name of the table to test')
    parser.add_argument('--full-sync', action='store_true', help='Run full sync (default)')
    parser.add_argument('--incremental', action='store_true', help='Run incremental sync')
    parser.add_argument('--both', action='store_true', help='Run both full and incremental sync')
    parser.add_argument('--sample-only', action='store_true', help='Only test small sample extraction')
    parser.add_argument('--limit', type=int, default=10, help='Number of rows for sample test')
    
    args = parser.parse_args()
    
    # Validate environment
    logger.info("Validating environment...")
    if not validate_environment():
        logger.error("❌ Environment validation failed")
        sys.exit(1)
    
    logger.info("✅ Environment validation passed")
    
    # Load table configurations
    logger.info("Loading table configurations...")
    table_configs = get_table_configs()
    
    # Validate table configuration
    try:
        config = validate_table_config(args.table_name, table_configs)
    except ValueError as e:
        logger.error(f"❌ {str(e)}")
        sys.exit(1)
    
    # Test source connection
    try:
        source = test_source_connection(args.table_name)
    except Exception:
        sys.exit(1)
    
    # Test small sample
    try:
        sample_rows = test_small_sample(args.table_name, source, args.limit)
    except Exception:
        sys.exit(1)
    
    # If sample-only, stop here
    if args.sample_only:
        logger.info("✅ Sample-only test completed successfully")
        sys.exit(0)
    
    # Create dlt pipeline
    logger.info("Creating dlt pipeline...")
    pipeline = dlt.pipeline(
        pipeline_name='opendental_etl_test',
        destination='postgres',
        dataset_name='raw_test'  # Use test schema
    )
    
    # Determine what to run
    run_full = args.full_sync or args.both or (not args.incremental)
    run_incremental = args.incremental or args.both
    
    # Run full sync if requested
    if run_full:
        try:
            load_info = run_full_sync(args.table_name, pipeline)
            verify_data_in_destination(args.table_name, pipeline)
            check_incremental_state(args.table_name, pipeline)
        except Exception:
            sys.exit(1)
    
    # Run incremental sync if requested
    if run_incremental:
        try:
            load_info = run_incremental_sync(args.table_name, pipeline)
            verify_data_in_destination(args.table_name, pipeline)
            check_incremental_state(args.table_name, pipeline)
        except Exception:
            sys.exit(1)
    
    logger.info("🎉 All tests completed successfully!")

if __name__ == "__main__":
    main()