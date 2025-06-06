#!/usr/bin/env python3
"""
Test script for Phase 1 implementation.
Tests the intelligent ETL pipeline with critical tables.
"""

import os
import sys
import logging
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from etl_pipeline.elt_pipeline import IntelligentELTPipeline
from dotenv import load_dotenv
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_phase1.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_configuration_loading():
    """Test that the intelligent pipeline can load configuration."""
    logger.info("[TEST] Testing configuration loading...")
    
    try:
        pipeline = IntelligentELTPipeline()
        
        # Test configuration methods
        critical_tables = pipeline.get_critical_tables()
        logger.info(f"[PASS] Found {len(critical_tables)} critical tables: {critical_tables}")
        
        # Test table configuration
        for table in critical_tables[:3]:  # Test first 3
            config = pipeline.get_table_config(table)
            logger.info(f"  [INFO] {table}: {config.get('table_importance')} "
                       f"({config.get('estimated_size_mb', 0):.1f} MB, "
                       f"{config.get('extraction_strategy')} extraction, "
                       f"batch size: {config.get('batch_size')})")
        
        return True
        
    except Exception as e:
        logger.error(f"[FAIL] Configuration loading failed: {str(e)}")
        return False

def test_database_connections():
    """Test database connectivity."""
    logger.info("[TEST] Testing database connections...")
    
    try:
        pipeline = IntelligentELTPipeline()
        pipeline.validate_database_names()
        pipeline.initialize_connections()
        
        # Test connections
        with pipeline.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            logger.info(f"[PASS] Source database connection: {result}")
        
        with pipeline.replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            logger.info(f"[PASS] Replication database connection: {result}")
            
        with pipeline.analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            logger.info(f"[PASS] Analytics database connection: {result}")
        
        pipeline.cleanup()
        return True
        
    except Exception as e:
        logger.error(f"[FAIL] Database connection failed: {str(e)}")
        return False

def test_dry_run_phase1():
    """Test dry run of Phase 1 critical tables."""
    logger.info("[TEST] Testing Phase 1 dry run...")
    
    try:
        pipeline = IntelligentELTPipeline()
        critical_tables = pipeline.get_critical_tables()
        
        logger.info(f"[INFO] Phase 1 would process {len(critical_tables)} critical tables:")
        
        total_size_mb = 0
        total_rows = 0
        
        for table in critical_tables:
            config = pipeline.get_table_config(table)
            size_mb = config.get('estimated_size_mb', 0)
            rows = config.get('estimated_rows', 0)
            
            total_size_mb += size_mb
            total_rows += rows
            
            logger.info(f"  [TARGET] {table}: "
                       f"{config.get('table_importance')} importance, "
                       f"{size_mb:.1f} MB, "
                       f"{rows:,} rows, "
                       f"{config.get('extraction_strategy')} extraction, "
                       f"batch size: {config.get('batch_size')}")
        
        logger.info(f"[STATS] Total Phase 1 scope: {total_size_mb:.1f} MB, {total_rows:,} rows")
        
        return True
        
    except Exception as e:
        logger.error(f"[FAIL] Dry run failed: {str(e)}")
        return False

def test_monitoring_configuration():
    """Test monitoring configuration for critical tables."""
    logger.info("[TEST] Testing monitoring configuration...")
    
    try:
        pipeline = IntelligentELTPipeline()
        critical_tables = pipeline.get_critical_tables()
        
        for table in critical_tables[:3]:  # Test first 3
            monitoring = pipeline.get_monitoring_config(table)
            logger.info(f"  [ALERT] {table} monitoring: "
                       f"alert_on_failure={monitoring.get('alert_on_failure')}, "
                       f"max_time={monitoring.get('max_extraction_time_minutes')}min, "
                       f"quality_threshold={monitoring.get('data_quality_threshold')}")
        
        return True
        
    except Exception as e:
        logger.error(f"[FAIL] Monitoring configuration test failed: {str(e)}")
        return False

def main():
    """Run all Phase 1 tests."""
    load_dotenv()
    
    logger.info("[START] Starting Phase 1 Implementation Tests")
    logger.info("=" * 60)
    
    tests = [
        ("Configuration Loading", test_configuration_loading),
        ("Database Connections", test_database_connections),
        ("Phase 1 Dry Run", test_dry_run_phase1),
        ("Monitoring Configuration", test_monitoring_configuration),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\n[RUN] Running test: {test_name}")
        logger.info("-" * 40)
        
        try:
            if test_func():
                logger.info(f"[PASS] {test_name}: PASSED")
                passed += 1
            else:
                logger.error(f"[FAIL] {test_name}: FAILED")
                failed += 1
        except Exception as e:
            logger.error(f"[FAIL] {test_name}: FAILED with exception: {str(e)}")
            failed += 1
    
    logger.info("\n" + "=" * 60)
    logger.info(f"[RESULTS] Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("[SUCCESS] All Phase 1 tests passed! Ready for implementation.")
        return True
    else:
        logger.error(f"[ERROR] {failed} tests failed. Fix issues before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 