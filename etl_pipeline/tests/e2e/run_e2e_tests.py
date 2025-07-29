#!/usr/bin/env python3
"""
E2E Test Runner

This script provides convenient ways to run the refactored E2E tests.
The tests are now organized into separate files for better maintainability.

Usage:
    python run_e2e_tests.py --help
    python run_e2e_tests.py --basic
    python run_e2e_tests.py --incremental
    python run_e2e_tests.py --copy-strategies
    python run_e2e_tests.py --validation
    python run_e2e_tests.py --all
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_tests(test_files, markers=None, verbose=False):
    """Run pytest for the specified test files."""
    cmd = ["pipenv", "run", "pytest"]
    
    # Add test files
    cmd.extend(test_files)
    
    # Add markers if specified
    if markers:
        for marker in markers:
            cmd.extend(["-m", marker])
    
    # Add verbose flag
    if verbose:
        cmd.append("-v")
    
    # Add other useful flags
    cmd.extend([
        "--tb=short",
        "--strict-markers",
        "--disable-warnings"
    ])
    
    print(f"Running command: {' '.join(cmd)}")
    print("-" * 80)
    
    try:
        result = subprocess.run(cmd, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Test execution failed with return code: {e.returncode}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run E2E tests for the ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_e2e_tests.py --basic                    # Run basic pipeline tests
    python run_e2e_tests.py --incremental             # Run incremental method tests
    python run_e2e_tests.py --copy-strategies         # Run copy strategy tests
    python run_e2e_tests.py --validation              # Run validation tests
    python run_e2e_tests.py --all                     # Run all tests
    python run_e2e_tests.py --basic --verbose         # Run with verbose output
        """
    )
    
    # Test category options
    parser.add_argument(
        "--basic",
        action="store_true",
        help="Run basic pipeline tests (patient, appointment, procedure data)"
    )
    
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Run incremental method tests (PostgresLoader, SimpleMySQLReplicator, Bulk)"
    )
    
    parser.add_argument(
        "--copy-strategies",
        action="store_true",
        help="Run copy strategy tests (Full, Incremental, Bulk, Upsert)"
    )
    
    parser.add_argument(
        "--validation",
        action="store_true",
        help="Run validation tests (UPSERT, Integrity, Data Quality)"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all E2E tests"
    )
    
    # General options
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Run tests with verbose output"
    )
    
    parser.add_argument(
        "--markers",
        nargs="+",
        help="Add pytest markers to filter tests (e.g., --markers e2e slow)"
    )
    
    args = parser.parse_args()
    
    # Determine which tests to run
    test_files = []
    
    if args.basic or args.all:
        test_files.append("test_basic_pipeline_e2e.py")
    
    if args.incremental or args.all:
        test_files.append("test_incremental_methods_e2e.py")
    
    if args.copy_strategies or args.all:
        test_files.append("test_copy_strategies_e2e.py")
    
    if args.validation or args.all:
        test_files.append("test_validation_e2e.py")
    
    # If no specific tests selected, show help
    if not test_files:
        parser.print_help()
        print("\nError: Please specify which tests to run.")
        sys.exit(1)
    
    # Change to the tests/e2e directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    print("E2E Test Runner")
    print("=" * 80)
    print(f"Running tests: {', '.join(test_files)}")
    if args.markers:
        print(f"Markers: {', '.join(args.markers)}")
    print()
    
    # Run the tests
    success = run_tests(test_files, args.markers, args.verbose)
    
    if success:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main() 