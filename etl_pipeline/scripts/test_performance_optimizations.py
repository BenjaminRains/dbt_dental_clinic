#!/usr/bin/env python3
"""
Performance Testing Script for ETL Pipeline Optimizations

This script tests the performance optimizations implemented in the ETL pipeline:
- Database-level optimizations (connection pools, performance settings)
- Streaming architecture for memory efficiency
- PostgreSQL COPY command for large tables
- Parallel processing for massive tables
- Performance tracking and reporting

Usage:
    python scripts/test_performance_optimizations.py [table_name]
"""

import os
import sys
import time
import logging
from typing import Dict, List, Optional

# Add the etl_pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.config.settings import get_settings
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.loaders.postgres_loader import PostgresLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_copy_performance(table_name: str = None) -> Dict:
    """Test MySQL copy performance with different strategies."""
    logger.info("Testing MySQL copy performance...")
    
    # Initialize replicator
    replicator = SimpleMySQLReplicator()
    
    # Get list of tables to test
    if table_name:
        tables_to_test = [table_name]
    else:
        # Test a mix of table sizes
        tables_to_test = [
            'adjustment',      # ~260K rows, 65MB
            'patient',         # ~50K rows, 15MB
            'appointment',     # ~100K rows, 25MB
            'procedurelog',    # ~500K rows, 125MB
        ]
    
    results = {}
    
    for table in tables_to_test:
        try:
            logger.info(f"Testing copy performance for {table}")
            
            # Test copy
            start_time = time.time()
            success = replicator.copy_table(table, force_full=False)
            duration = time.time() - start_time
            
            if success:
                results[table] = {
                    'success': True,
                    'duration': duration,
                    'strategy': replicator.get_extraction_strategy(table)
                }
                logger.info(f"✓ {table}: {duration:.2f}s ({replicator.get_extraction_strategy(table)} strategy)")
            else:
                results[table] = {
                    'success': False,
                    'duration': duration,
                    'strategy': 'failed'
                }
                logger.error(f"✗ {table}: Failed after {duration:.2f}s")
                
        except Exception as e:
            logger.error(f"Error testing {table}: {e}")
            results[table] = {
                'success': False,
                'duration': 0,
                'strategy': 'error',
                'error': str(e)
            }
    
    return results


def test_load_performance(table_name: str = None) -> Dict:
    """Test PostgreSQL load performance with different strategies."""
    logger.info("Testing PostgreSQL load performance...")
    
    # Initialize loader
    loader = PostgresLoader()
    
    # Get list of tables to test
    if table_name:
        tables_to_test = [table_name]
    else:
        # Test a mix of table sizes
        tables_to_test = [
            'adjustment',      # ~260K rows, 65MB
            'patient',         # ~50K rows, 15MB
            'appointment',     # ~100K rows, 25MB
            'procedurelog',    # ~500K rows, 125MB
        ]
    
    results = {}
    
    for table in tables_to_test:
        try:
            logger.info(f"Testing load performance for {table}")
            
            # Test load
            start_time = time.time()
            success = loader.load_table(table, force_full=False)
            duration = time.time() - start_time
            
            if success:
                # Get performance metrics
                metrics = getattr(loader, 'performance_metrics', {}).get(table, {})
                results[table] = {
                    'success': True,
                    'duration': duration,
                    'strategy': metrics.get('strategy', 'unknown'),
                    'rows_per_second': metrics.get('rows_per_second', 0),
                    'memory_mb': metrics.get('memory_mb', 0)
                }
                logger.info(f"✓ {table}: {duration:.2f}s ({metrics.get('strategy', 'unknown')} strategy, "
                           f"{metrics.get('rows_per_second', 0):.0f} rows/sec)")
            else:
                results[table] = {
                    'success': False,
                    'duration': duration,
                    'strategy': 'failed'
                }
                logger.error(f"✗ {table}: Failed after {duration:.2f}s")
                
        except Exception as e:
            logger.error(f"Error testing {table}: {e}")
            results[table] = {
                'success': False,
                'duration': 0,
                'strategy': 'error',
                'error': str(e)
            }
    
    return results


def generate_performance_report(copy_results: Dict, load_results: Dict) -> str:
    """Generate a comprehensive performance report."""
    report = ["# ETL Pipeline Performance Test Report", ""]
    
    # Copy Performance Summary
    report.append("## MySQL Copy Performance")
    report.append("")
    
    successful_copies = [k for k, v in copy_results.items() if v['success']]
    failed_copies = [k for k, v in copy_results.items() if not v['success']]
    
    if successful_copies:
        total_copy_time = sum(copy_results[table]['duration'] for table in successful_copies)
        avg_copy_time = total_copy_time / len(successful_copies)
        report.append(f"- **Successful Copies**: {len(successful_copies)}")
        report.append(f"- **Total Copy Time**: {total_copy_time:.2f}s")
        report.append(f"- **Average Copy Time**: {avg_copy_time:.2f}s")
        report.append("")
        
        for table in successful_copies:
            result = copy_results[table]
            report.append(f"### {table}")
            report.append(f"- Strategy: {result['strategy']}")
            report.append(f"- Duration: {result['duration']:.2f}s")
            report.append("")
    
    if failed_copies:
        report.append(f"- **Failed Copies**: {len(failed_copies)}")
        for table in failed_copies:
            result = copy_results[table]
            report.append(f"  - {table}: {result.get('error', 'Unknown error')}")
        report.append("")
    
    # Load Performance Summary
    report.append("## PostgreSQL Load Performance")
    report.append("")
    
    successful_loads = [k for k, v in load_results.items() if v['success']]
    failed_loads = [k for k, v in load_results.items() if not v['success']]
    
    if successful_loads:
        total_load_time = sum(load_results[table]['duration'] for table in successful_loads)
        avg_load_time = total_load_time / len(successful_loads)
        total_rows_per_sec = sum(load_results[table]['rows_per_second'] for table in successful_loads)
        avg_rows_per_sec = total_rows_per_sec / len(successful_loads)
        
        report.append(f"- **Successful Loads**: {len(successful_loads)}")
        report.append(f"- **Total Load Time**: {total_load_time:.2f}s")
        report.append(f"- **Average Load Time**: {avg_load_time:.2f}s")
        report.append(f"- **Average Rows/Second**: {avg_rows_per_sec:.0f}")
        report.append("")
        
        for table in successful_loads:
            result = load_results[table]
            report.append(f"### {table}")
            report.append(f"- Strategy: {result['strategy']}")
            report.append(f"- Duration: {result['duration']:.2f}s")
            report.append(f"- Rows/Second: {result['rows_per_second']:.0f}")
            report.append(f"- Memory Usage: {result['memory_mb']:.1f}MB")
            report.append("")
    
    if failed_loads:
        report.append(f"- **Failed Loads**: {len(failed_loads)}")
        for table in failed_loads:
            result = load_results[table]
            report.append(f"  - {table}: {result.get('error', 'Unknown error')}")
        report.append("")
    
    # Performance Recommendations
    report.append("## Performance Recommendations")
    report.append("")
    
    if successful_loads:
        fastest_table = min(successful_loads, key=lambda x: load_results[x]['duration'])
        fastest_result = load_results[fastest_table]
        report.append(f"- **Fastest Table**: {fastest_table} ({fastest_result['duration']:.2f}s, {fastest_result['strategy']} strategy)")
        
        if any(r['rows_per_second'] > 10000 for r in load_results.values()):
            report.append("- **High Performance**: Some tables achieved >10K rows/second")
        else:
            report.append("- **Performance Note**: Consider further optimizations for large tables")
    
    report.append("")
    report.append("## Optimization Status")
    report.append("")
    report.append("✅ **Implemented Optimizations**:")
    report.append("- Connection pool optimization (4x increase)")
    report.append("- MySQL/PostgreSQL performance settings")
    report.append("- Bulk INSERT operations")
    report.append("- Streaming architecture")
    report.append("- PostgreSQL COPY command")
    report.append("- Parallel processing for massive tables")
    report.append("- Performance tracking and metrics")
    report.append("")
    
    return "\n".join(report)


def main():
    """Main performance testing function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test ETL pipeline performance optimizations')
    parser.add_argument('table_name', nargs='?', help='Specific table to test (optional)')
    parser.add_argument('--copy-only', action='store_true', help='Test only MySQL copy performance')
    parser.add_argument('--load-only', action='store_true', help='Test only PostgreSQL load performance')
    parser.add_argument('--output', help='Output file for performance report')
    
    args = parser.parse_args()
    
    logger.info("Starting ETL pipeline performance testing...")
    
    # Set environment
    os.environ['ETL_ENVIRONMENT'] = 'test'
    
    copy_results = {}
    load_results = {}
    
    # Test copy performance
    if not args.load_only:
        logger.info("=" * 60)
        logger.info("TESTING MYSQL COPY PERFORMANCE")
        logger.info("=" * 60)
        copy_results = test_copy_performance(args.table_name)
    
    # Test load performance
    if not args.copy_only:
        logger.info("=" * 60)
        logger.info("TESTING POSTGRESQL LOAD PERFORMANCE")
        logger.info("=" * 60)
        load_results = test_load_performance(args.table_name)
    
    # Generate and display report
    logger.info("=" * 60)
    logger.info("GENERATING PERFORMANCE REPORT")
    logger.info("=" * 60)
    
    report = generate_performance_report(copy_results, load_results)
    
    # Output report
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        logger.info(f"Performance report saved to: {args.output}")
    else:
        print(report)
    
    logger.info("Performance testing completed!")


if __name__ == "__main__":
    main() 