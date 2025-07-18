#!/usr/bin/env python3
"""
Quick Connection Usage Check

This script provides a quick analysis of connection usage patterns in the ETL pipeline
for processing 400+ tables across 3 databases.

Usage:
    python etl_pipeline/scripts/quick_connection_check.py
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List

# Add etl_pipeline to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
# Removed find_latest_tables_config import - we only use tables.yml with metadata versioning

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_connection_patterns():
    """Analyze connection patterns for 400+ tables across 3 databases."""
    
    logger.info("Analyzing ETL pipeline connection patterns...")
    
    # Get settings
    settings = get_settings()
    
    # Get table configurations
    config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
    tables_config_path = os.path.join(config_dir, 'tables.yml')
    
    if not os.path.exists(tables_config_path):
        logger.error("No tables configuration found")
        return
    
    # Load table configurations
    import yaml
    with open(tables_config_path, 'r') as f:
        tables_config = yaml.safe_load(f)
    
    tables = list(tables_config.get('tables', {}).keys())
    total_tables = len(tables)
    
    logger.info(f"Found {total_tables} tables to process")
    
    # Analyze connection requirements
    analysis = {
        'total_tables': total_tables,
        'databases': {
            'source': 'OpenDental MySQL (Source)',
            'replication': 'MySQL Replication',
            'analytics': 'PostgreSQL Analytics'
        },
        'connection_patterns': {
            'per_table_connections': {
                'source': 1,  # One connection per table for source
                'replication': 1,  # One connection per table for target
                'analytics': 0  # Not used in SimpleMySQLReplicator
            },
            'total_connections_per_table': 2,  # Source + Replication
            'estimated_total_connections': total_tables * 2
        },
        'connection_pool_settings': {
            'default_pool_size': 5,
            'default_max_overflow': 10,
            'default_pool_timeout': 30,
            'default_pool_recycle': 1800
        },
        'potential_issues': [],
        'recommendations': []
    }
    
    # Analyze potential issues
    estimated_connections = analysis['connection_patterns']['estimated_total_connections']
    
    if estimated_connections > 100:
        analysis['potential_issues'].append(
            f"High connection count: {estimated_connections} connections for {total_tables} tables"
        )
    
    if total_tables > 200:
        analysis['potential_issues'].append(
            f"Large table count: {total_tables} tables may overwhelm connection pools"
        )
    
    # Check connection pool settings
    pool_size = analysis['connection_pool_settings']['default_pool_size']
    max_overflow = analysis['connection_pool_settings']['default_max_overflow']
    max_connections_per_pool = pool_size + max_overflow
    
    if estimated_connections > max_connections_per_pool * 3:  # 3 databases
        analysis['potential_issues'].append(
            f"Connection pool may be insufficient: {estimated_connections} estimated vs {max_connections_per_pool * 3} available"
        )
    
    # Generate recommendations
    if estimated_connections > 100:
        analysis['recommendations'].extend([
            "Consider using ConnectionManager for batch operations",
            "Implement connection reuse across table processing",
            "Use connection pooling at application level",
            "Consider processing tables in smaller batches"
        ])
    
    if total_tables > 200:
        analysis['recommendations'].extend([
            "Process tables in parallel with limited concurrency",
            "Use dedicated connection pools for each database",
            "Implement connection caching for frequently accessed databases",
            "Consider using connection factories with connection reuse"
        ])
    
    return analysis

def generate_report(analysis: Dict):
    """Generate a connection usage report."""
    
    report = []
    report.append("=" * 80)
    report.append("ETL PIPELINE CONNECTION USAGE ANALYSIS")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Overview
    report.append("OVERVIEW:")
    report.append(f"  Total tables to process: {analysis['total_tables']}")
    report.append(f"  Databases involved: {len(analysis['databases'])}")
    for db_type, db_name in analysis['databases'].items():
        report.append(f"    - {db_type}: {db_name}")
    report.append("")
    
    # Connection patterns
    patterns = analysis['connection_patterns']
    report.append("CONNECTION PATTERNS:")
    report.append(f"  Connections per table: {patterns['total_connections_per_table']}")
    report.append(f"  Estimated total connections: {patterns['estimated_total_connections']}")
    report.append("  Per database breakdown:")
    for db_type, count in patterns['per_table_connections'].items():
        report.append(f"    - {db_type}: {count} connection(s) per table")
    report.append("")
    
    # Connection pool settings
    pool_settings = analysis['connection_pool_settings']
    report.append("CONNECTION POOL SETTINGS:")
    report.append(f"  Default pool size: {pool_settings['default_pool_size']}")
    report.append(f"  Max overflow: {pool_settings['default_max_overflow']}")
    report.append(f"  Pool timeout: {pool_settings['default_pool_timeout']}s")
    report.append(f"  Pool recycle: {pool_settings['default_pool_recycle']}s")
    report.append(f"  Max connections per pool: {pool_settings['default_pool_size'] + pool_settings['default_max_overflow']}")
    report.append("")
    
    # Potential issues
    issues = analysis['potential_issues']
    if issues:
        report.append("POTENTIAL ISSUES:")
        for i, issue in enumerate(issues, 1):
            report.append(f"  {i}. {issue}")
        report.append("")
    
    # Recommendations
    recommendations = analysis['recommendations']
    if recommendations:
        report.append("RECOMMENDATIONS:")
        for i, rec in enumerate(recommendations, 1):
            report.append(f"  {i}. {rec}")
        report.append("")
    
    # Optimization strategies
    report.append("OPTIMIZATION STRATEGIES:")
    report.append("  1. Use ConnectionManager for batch operations")
    report.append("  2. Implement connection reuse across table processing")
    report.append("  3. Process tables in smaller batches")
    report.append("  4. Use dedicated connection pools for each database")
    report.append("  5. Implement connection caching")
    report.append("  6. Monitor connection usage with the analysis script")
    report.append("")
    
    report.append("=" * 80)
    
    return "\n".join(report)

def main():
    """Main function to run quick connection analysis."""
    
    logger.info("Starting quick connection usage analysis...")
    
    # Analyze connection patterns
    analysis = analyze_connection_patterns()
    
    if analysis is None:
        logger.error("Failed to analyze connection patterns")
        return
    
    # Generate report
    report = generate_report(analysis)
    
    # Print report
    print(report)
    
    # Save report to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"etl_pipeline/logs/quick_connection_analysis_{timestamp}.txt"
    
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, 'w') as f:
        f.write(report)
    
    logger.info(f"Quick connection analysis report saved to: {report_file}")
    
    return analysis

if __name__ == "__main__":
    main() 