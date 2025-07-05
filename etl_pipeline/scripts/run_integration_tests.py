#!/usr/bin/env python3
"""
Integration Test Runner Script

This script runs the integration tests in the correct order to test the ETL pipeline flow:
1. Setup test databases (if needed)
2. Run schema discovery tests (order=1) - test reading from source
3. Run MySQL replicator tests (order=2) - test replication to replication DB
4. Run Postgres schema tests (order=3) - test transformation to analytics DB

Usage:
    python scripts/run_integration_tests.py [--setup-only] [--skip-setup] [--order-only]

Requirements:
    - pytest-order package installed
    - Test databases configured and accessible
    - ETL_ENVIRONMENT=test set
"""

import os
import sys
import subprocess
import argparse
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command, description):
    """Run a command and log the result."""
    logger.info(f"Running: {description}")
    logger.info(f"Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"✅ {description} completed successfully")
        if result.stdout:
            logger.info(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed")
        logger.error(f"Error: {e}")
        if e.stdout:
            logger.error(f"stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        return False

def setup_test_databases():
    """Run the test database setup script."""
    setup_script = Path(__file__).parent.parent.parent / "setup_test_databases.py"
    
    if not setup_script.exists():
        logger.error(f"Setup script not found at: {setup_script}")
        return False
    
    logger.info("Setting up test databases...")
    return run_command(f"python {setup_script}", "Test database setup")

def run_integration_tests():
    """Run the integration tests in order."""
    # Change to the etl_pipeline directory
    etl_dir = Path(__file__).parent.parent
    os.chdir(etl_dir)
    
    # Run integration tests with ordering
    test_command = "pytest tests/integration/core/ -m integration -v --tb=short"
    logger.info("Running integration tests in order...")
    return run_command(test_command, "Integration tests")

def run_tests_by_order():
    """Run tests by specific order to ensure proper flow."""
    etl_dir = Path(__file__).parent.parent
    os.chdir(etl_dir)
    
    # Run tests in order using pytest-order
    logger.info("Running integration tests in specific order...")
    
    # Order 0: Configuration tests (prerequisites)
    logger.info("=== ORDER 0: Configuration Tests (Prerequisites) ===")
    if not run_command("pytest tests/integration/config/ -m integration -k 'order(0)' -v", "Order 0 tests"):
        return False
    
    # Order 1: Schema discovery tests (reading from source)
    logger.info("=== ORDER 1: Schema Discovery Tests (Source Database) ===")
    if not run_command("pytest tests/integration/core/ -m integration -k 'order(1)' -v", "Order 1 tests"):
        return False
    
    # Order 2: MySQL replicator tests (replication to replication DB)
    logger.info("=== ORDER 2: MySQL Replicator Tests (Replication Database) ===")
    if not run_command("pytest tests/integration/core/ -m integration -k 'order(2)' -v", "Order 2 tests"):
        return False
    
    # Order 3: Postgres schema tests (transformation to analytics DB)
    logger.info("=== ORDER 3: Postgres Schema Tests (Analytics Database) ===")
    if not run_command("pytest tests/integration/core/ -m integration -k 'order(3)' -v", "Order 3 tests"):
        return False
    
    # Order 4: Loader tests (data loading)
    logger.info("=== ORDER 4: Loader Tests (Data Loading) ===")
    if not run_command("pytest tests/integration/loaders/ -m integration -k 'order(4)' -v", "Order 4 tests"):
        return False
    
    # Order 5: Orchestration tests (pipeline orchestration)
    logger.info("=== ORDER 5: Orchestration Tests (Pipeline Orchestration) ===")
    if not run_command("pytest tests/integration/orchestration/ -m integration -k 'order(5)' -v", "Order 5 tests"):
        return False
    
    # Order 6: Monitoring tests (metrics collection)
    logger.info("=== ORDER 6: Monitoring Tests (Metrics Collection) ===")
    if not run_command("pytest tests/integration/monitoring/ -m integration -k 'order(6)' -v", "Order 6 tests"):
        return False
    
    return True

def main():
    """Main function to run integration tests."""
    parser = argparse.ArgumentParser(description="Run integration tests in correct order")
    parser.add_argument("--setup-only", action="store_true", help="Only run database setup")
    parser.add_argument("--skip-setup", action="store_true", help="Skip database setup")
    parser.add_argument("--order-only", action="store_true", help="Run tests by order only (no setup)")
    
    args = parser.parse_args()
    
    # Check environment
    if os.environ.get('ETL_ENVIRONMENT') != 'test':
        logger.warning("ETL_ENVIRONMENT is not set to 'test'. Setting it now...")
        os.environ['ETL_ENVIRONMENT'] = 'test'
    
    logger.info("Starting integration test runner...")
    logger.info(f"ETL_ENVIRONMENT: {os.environ.get('ETL_ENVIRONMENT')}")
    
    success = True
    
    if args.setup_only:
        # Only run setup
        success = setup_test_databases()
    elif args.order_only:
        # Only run tests by order
        success = run_tests_by_order()
    elif not args.skip_setup:
        # Run setup and then tests
        if setup_test_databases():
            success = run_tests_by_order()
        else:
            success = False
    else:
        # Skip setup, run tests normally
        success = run_integration_tests()
    
    if success:
        logger.info("🎉 All integration tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Integration tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 