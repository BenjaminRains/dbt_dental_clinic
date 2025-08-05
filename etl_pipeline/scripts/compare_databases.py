#!/usr/bin/env python3
"""
Database Comparison Script for ETL Pipeline

This script connects to all three databases and compares their tables:
- opendental (source MySQL)
- opendental_replication (replication MySQL) 
- opendental_analytics (target PostgreSQL)

It creates pandas DataFrames for analysis and comparison.
"""

import sys
import os
import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import text
import logging
from datetime import datetime

# Add the etl_pipeline directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config import get_settings
from etl_pipeline.config.logging import setup_run_logging, get_logger

# Set up logging for this analysis
log_file_path = setup_run_logging(
    log_level="INFO",
    log_dir="logs/analysis",
    run_id="database_comparison"
)
logger = get_logger(__name__)
logger.info(f"Starting database comparison analysis - Log file: {log_file_path}")


def get_mysql_tables(engine, database_name):
    """Get all tables from MySQL database with metadata."""
    query = """
    SELECT 
        TABLE_NAME as table_name,
        TABLE_ROWS as row_count,
        ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS size_mb,
        TABLE_COMMENT as comment,
        ENGINE as engine,
        TABLE_COLLATION as collation
    FROM information_schema.TABLES 
    WHERE TABLE_SCHEMA = :database_name
    ORDER BY TABLE_NAME
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"database_name": database_name})
            rows = result.fetchall()
            
        df = pd.DataFrame(rows, columns=[
            'table_name', 'row_count', 'size_mb', 'comment', 
            'engine', 'collation'
        ])
        df['database'] = database_name
        df['database_type'] = 'mysql'
        
        logger.info(f"Retrieved {len(df)} tables from {database_name}")
        return df
        
    except Exception as e:
        logger.error(f"Error getting tables from {database_name}: {e}")
        return pd.DataFrame()


def get_postgresql_tables(engine, database_name):
    """Get all tables from PostgreSQL database with metadata."""
    query = """
    SELECT 
        schemaname as schema_name,
        tablename as table_name,
        COALESCE(reltuples::bigint, 0) as row_count,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size_pretty,
        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
    FROM pg_tables pt
    LEFT JOIN pg_class pc ON pc.relname = pt.tablename
    WHERE schemaname = 'raw'
    ORDER BY tablename
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            
        df = pd.DataFrame(rows, columns=[
            'schema_name', 'table_name', 'row_count', 'size_pretty', 'size_bytes'
        ])
        
        # Convert size_bytes to MB
        df['size_mb'] = df['size_bytes'].apply(lambda x: round(x / 1024 / 1024, 2) if x else 0)
        
        # Add database info
        df['database'] = database_name
        df['database_type'] = 'postgresql'
        df['comment'] = ''
        df['engine'] = 'postgresql'
        df['collation'] = ''
        
        logger.info(f"Retrieved {len(df)} tables from {database_name}")
        return df
        
    except Exception as e:
        logger.error(f"Error getting tables from {database_name}: {e}")
        return pd.DataFrame()





def get_actual_row_counts(engine, database_name, table_names):
    """Get actual row counts for multiple tables."""
    results = {}
    
    if database_name == 'opendental_analytics':
        # PostgreSQL - get counts for all tables in raw schema
        for table_name in table_names:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}"))
                    count = result.scalar()
                    results[table_name] = count
            except Exception as e:
                logger.warning(f"Could not get row count for {table_name}: {e}")
                results[table_name] = 0
    else:
        # MySQL - get counts for all tables
        for table_name in table_names:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {database_name}.{table_name}"))
                    count = result.scalar()
                    results[table_name] = count
            except Exception as e:
                logger.warning(f"Could not get row count for {table_name}: {e}")
                results[table_name] = 0
    
    return results


def get_table_columns(engine, database_name, table_name, schema_name=None):
    """Get column information for a specific table."""
    if database_name == 'opendental_analytics':
        # PostgreSQL
        if schema_name:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_schema = :schema_name 
            AND table_name = :table_name
            ORDER BY ordinal_position
            """
            params = {"schema_name": schema_name, "table_name": table_name}
        else:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = :table_name
            ORDER BY ordinal_position
            """
            params = {"table_name": table_name}
    else:
        # MySQL
        query = """
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            COLUMN_DEFAULT as column_default,
            CHARACTER_MAXIMUM_LENGTH as character_maximum_length
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = :database_name 
        AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """
        params = {"database_name": database_name, "table_name": table_name}
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            
        return pd.DataFrame(rows, columns=[
            'column_name', 'data_type', 'is_nullable', 
            'column_default', 'character_maximum_length'
        ])
        
    except Exception as e:
        logger.error(f"Error getting columns for {database_name}.{table_name}: {e}")
        return pd.DataFrame()


def create_comparison_report(source_df, replication_df, analytics_df, source_engine, replication_engine, analytics_engine):
    """Create a comprehensive comparison report of all tables across databases."""
    
    # Get all unique table names
    all_tables = set()
    if not source_df.empty:
        all_tables.update(source_df['table_name'].tolist())
    if not replication_df.empty:
        all_tables.update(replication_df['table_name'].tolist())
    if not analytics_df.empty:
        all_tables.update(analytics_df['table_name'].tolist())
    
    all_tables = sorted(all_tables)
    
    # Get actual row counts for all tables
    logger.info("Getting actual row counts for all tables...")
    source_counts = get_actual_row_counts(source_engine, 'opendental', all_tables)
    replication_counts = get_actual_row_counts(replication_engine, 'opendental_replication', all_tables)
    analytics_counts = get_actual_row_counts(analytics_engine, 'opendental_analytics', all_tables)
    
    # Create comparison DataFrame
    comparison_data = []
    
    for table_name in all_tables:
        # Check if table exists in each database
        source_exists = table_name in source_df['table_name'].values if not source_df.empty else False
        replication_exists = table_name in replication_df['table_name'].values if not replication_df.empty else False
        analytics_exists = table_name in analytics_df['table_name'].values if not analytics_df.empty else False
        
        # Get metadata
        source_meta = source_df[source_df['table_name'] == table_name].iloc[0] if source_exists else None
        replication_meta = replication_df[replication_df['table_name'] == table_name].iloc[0] if replication_exists else None
        analytics_meta = analytics_df[analytics_df['table_name'] == table_name].iloc[0] if analytics_exists else None
        
        # Get row counts
        source_count = source_counts.get(table_name, 0) if source_exists else None
        replication_count = replication_counts.get(table_name, 0) if replication_exists else None
        analytics_count = analytics_counts.get(table_name, 0) if analytics_exists else None
        
        # Get sizes
        source_size = source_meta['size_mb'] if source_meta is not None else None
        replication_size = replication_meta['size_mb'] if replication_meta is not None else None
        analytics_size = analytics_meta['size_mb'] if analytics_meta is not None else None
        
        comparison_data.append({
            'table_name': table_name,
            'source_exists': source_exists,
            'replication_exists': replication_exists,
            'analytics_exists': analytics_exists,
            'source_count': source_count,
            'replication_count': replication_count,
            'analytics_count': analytics_count,
            'source_size_mb': source_size,
            'replication_size_mb': replication_size,
            'analytics_size_mb': analytics_size,
            'status': _get_table_status(source_exists, replication_exists, analytics_exists, 
                                      source_count, replication_count, analytics_count)
        })
    
    return pd.DataFrame(comparison_data)


def _get_table_status(source_exists, replication_exists, analytics_exists, source_count, replication_count, analytics_count):
    """Determine the status of a table across databases."""
    if not source_exists:
        return "MISSING_SOURCE"
    elif not replication_exists:
        return "MISSING_REPLICATION"
    elif not analytics_exists:
        return "MISSING_ANALYTICS"
    elif source_count is None or replication_count is None or analytics_count is None:
        return "COUNT_ERROR"
    elif source_count == replication_count == analytics_count:
        return "SYNCED"
    elif source_count != replication_count:
        return "REPLICATION_MISMATCH"
    elif replication_count != analytics_count:
        return "ANALYTICS_MISMATCH"
    else:
        return "UNKNOWN"


def compare_table_counts(source_engine, replication_engine, analytics_engine, table_name, schema_name=None):
    """
    Compare row counts for a specific table across all three databases.
    
    Args:
        source_engine: MySQL engine for source database
        replication_engine: MySQL engine for replication database  
        analytics_engine: PostgreSQL engine for analytics database
        table_name: Name of the table to compare
        schema_name: Schema name for PostgreSQL (optional)
        
    Returns:
        DataFrame with comparison results
    """
    logger.info(f"Comparing table counts for: {table_name}")
    
    results = []
    
    try:
        # Source database (MySQL)
        with source_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as row_count FROM opendental.{table_name}"))
            source_count = result.scalar()
            results.append({
                'database': 'opendental (source)',
                'table_name': table_name,
                'row_count': source_count,
                'database_type': 'mysql'
            })
            logger.info(f"Source database {table_name}: {source_count} rows")
            
    except Exception as e:
        logger.error(f"Error counting rows in source database for {table_name}: {e}")
        results.append({
            'database': 'opendental (source)',
            'table_name': table_name,
            'row_count': None,
            'database_type': 'mysql',
            'error': str(e)
        })
    
    try:
        # Replication database (MySQL)
        with replication_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as row_count FROM opendental_replication.{table_name}"))
            replication_count = result.scalar()
            results.append({
                'database': 'opendental_replication',
                'table_name': table_name,
                'row_count': replication_count,
                'database_type': 'mysql'
            })
            logger.info(f"Replication database {table_name}: {replication_count} rows")
            
    except Exception as e:
        logger.error(f"Error counting rows in replication database for {table_name}: {e}")
        results.append({
            'database': 'opendental_replication',
            'table_name': table_name,
            'row_count': None,
            'database_type': 'mysql',
            'error': str(e)
        })
    
    try:
        # Analytics database (PostgreSQL)
        if schema_name:
            query = f"SELECT COUNT(*) as row_count FROM {schema_name}.{table_name}"
        else:
            query = f"SELECT COUNT(*) as row_count FROM raw.{table_name}"
            
        with analytics_engine.connect() as conn:
            result = conn.execute(text(query))
            analytics_count = result.scalar()
            results.append({
                'database': f'opendental_analytics ({schema_name or "raw"})',
                'table_name': table_name,
                'row_count': analytics_count,
                'database_type': 'postgresql'
            })
            logger.info(f"Analytics database {table_name}: {analytics_count} rows")
            
    except Exception as e:
        logger.error(f"Error counting rows in analytics database for {table_name}: {e}")
        results.append({
            'database': f'opendental_analytics ({schema_name or "raw"})',
            'table_name': table_name,
            'row_count': None,
            'database_type': 'postgresql',
            'error': str(e)
        })
    
    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Add comparison analysis
    if len(df) == 3 and df['row_count'].notna().all():
        source_count = df[df['database'] == 'opendental (source)']['row_count'].iloc[0]
        replication_count = df[df['database'] == 'opendental_replication']['row_count'].iloc[0]
        analytics_count = df[df['database'].str.contains('opendental_analytics')]['row_count'].iloc[0]
        
        # Check for data loss
        if replication_count < source_count:
            logger.warning(f"Data loss detected in replication: {source_count - replication_count} rows missing")
        if analytics_count < replication_count:
            logger.warning(f"Data loss detected in analytics: {replication_count - analytics_count} rows missing")
        if analytics_count < source_count:
            logger.warning(f"Data loss detected in pipeline: {source_count - analytics_count} rows missing")
            
        # Add summary
        print(f"\n📊 Table Count Comparison for '{table_name}':")
        print(f"Source (opendental): {source_count:,} rows")
        print(f"Replication (opendental_replication): {replication_count:,} rows")
        print(f"Analytics (opendental_analytics.{schema_name or 'raw'}): {analytics_count:,} rows")
        
        if source_count == replication_count == analytics_count:
            print("✅ All databases have the same row count")
        else:
            print("⚠️  Row count differences detected")
    
    return df


def main():
    """Main analysis function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Compare databases and tables across ETL pipeline. Use --list-tables to view all tables, --table to compare specific table counts, or --interactive for interactive mode.')
    parser.add_argument('--table', '-t', type=str, help='Specific table name to compare')
    parser.add_argument('--schema', '-s', type=str, default='raw', 
                       help='Schema name for PostgreSQL table (default: raw)')
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Run in interactive mode (default if no table specified)')
    parser.add_argument('--list-tables', '-l', action='store_true',
                       help='List all tables from all databases')
    
    args = parser.parse_args()
    
    logger.info("Starting database comparison analysis...")
    
    try:
        # Get settings for connections
        settings = get_settings()
        
        # Create connections to all three databases
        logger.info("Creating database connections...")
        
        # Get connection configurations from settings
        source_config = settings.get_source_connection_config()
        replication_config = settings.get_replication_connection_config()
        analytics_config = settings.get_analytics_raw_connection_config()
        
        # Create engines using ConnectionFactory
        source_engine = ConnectionFactory.create_mysql_engine(**source_config)
        replication_engine = ConnectionFactory.create_mysql_engine(**replication_config)
        analytics_engine = ConnectionFactory.create_postgres_engine(**analytics_config)
        
        # Get tables from each database
        source_df = get_mysql_tables(source_engine, 'opendental')
        replication_df = get_mysql_tables(replication_engine, 'opendental_replication')
        analytics_df = get_postgresql_tables(analytics_engine, 'opendental_analytics')
        
        # Combine all dataframes
        all_tables_df = pd.concat([source_df, replication_df, analytics_df], ignore_index=True)
        
        # Simple summary
        print(f"\n📊 Database Export Complete")
        print(f"Source (opendental): {len(source_df)} tables")
        print(f"Replication (opendental_replication): {len(replication_df)} tables")
        print(f"Analytics (opendental_analytics): {len(analytics_df)} tables")
        print(f"📝 Log file: {log_file_path}")
        
        # If list-tables option is provided, display comprehensive comparison report
        if args.list_tables:
            print(f"\n📋 Database Comparison Report")
            print(f"=" * 80)
            
            # Create comprehensive comparison report
            comparison_df = create_comparison_report(source_df, replication_df, analytics_df, 
                                                   source_engine, replication_engine, analytics_engine)
            
            # Display summary statistics
            total_tables = len(comparison_df)
            synced_tables = len(comparison_df[comparison_df['status'] == 'SYNCED'])
            missing_replication = len(comparison_df[comparison_df['status'] == 'MISSING_REPLICATION'])
            missing_analytics = len(comparison_df[comparison_df['status'] == 'MISSING_ANALYTICS'])
            replication_mismatch = len(comparison_df[comparison_df['status'] == 'REPLICATION_MISMATCH'])
            analytics_mismatch = len(comparison_df[comparison_df['status'] == 'ANALYTICS_MISMATCH'])
            
            print(f"\n📊 Summary Statistics:")
            print(f"Total tables analyzed: {total_tables}")
            print(f"✅ Fully synced: {synced_tables}")
            print(f"⚠️  Missing in replication: {missing_replication}")
            print(f"⚠️  Missing in analytics: {missing_analytics}")
            print(f"⚠️  Replication count mismatch: {replication_mismatch}")
            print(f"⚠️  Analytics count mismatch: {analytics_mismatch}")
            
            # Display detailed comparison table
            print(f"\n📋 Detailed Table Comparison:")
            print(f"{'Table Name':<25} {'Source':<10} {'Replication':<12} {'Analytics':<10} {'Status':<20}")
            print("-" * 85)
            
            for _, row in comparison_df.iterrows():
                source_count = f"{row['source_count']:,}" if row['source_count'] is not None else "N/A"
                replication_count = f"{row['replication_count']:,}" if row['replication_count'] is not None else "N/A"
                analytics_count = f"{row['analytics_count']:,}" if row['analytics_count'] is not None else "N/A"
                
                # Add status indicators
                status_icon = {
                    'SYNCED': '✅',
                    'MISSING_REPLICATION': '⚠️',
                    'MISSING_ANALYTICS': '⚠️',
                    'REPLICATION_MISMATCH': '❌',
                    'ANALYTICS_MISMATCH': '❌',
                    'MISSING_SOURCE': '❌',
                    'COUNT_ERROR': '❌',
                    'UNKNOWN': '❓'
                }.get(row['status'], '❓')
                
                print(f"{row['table_name']:<25} {source_count:<10} {replication_count:<12} {analytics_count:<10} {status_icon} {row['status']:<15}")
            
            # Show tables with issues
            issues_df = comparison_df[comparison_df['status'] != 'SYNCED']
            if not issues_df.empty:
                print(f"\n🚨 Tables with Issues:")
                for _, row in issues_df.iterrows():
                    print(f"  • {row['table_name']}: {row['status']}")
                    if row['source_count'] != row['replication_count']:
                        diff = abs(row['source_count'] - row['replication_count']) if row['source_count'] and row['replication_count'] else 0
                        print(f"    Source: {row['source_count']:,} | Replication: {row['replication_count']:,} | Difference: {diff:,}")
                    if row['replication_count'] != row['analytics_count']:
                        diff = abs(row['replication_count'] - row['analytics_count']) if row['replication_count'] and row['analytics_count'] else 0
                        print(f"    Replication: {row['replication_count']:,} | Analytics: {row['analytics_count']:,} | Difference: {diff:,}")
            
            # Show size comparison for synced tables
            synced_df = comparison_df[comparison_df['status'] == 'SYNCED']
            if not synced_df.empty:
                print(f"\n💾 Size Comparison (Synced Tables):")
                print(f"{'Table Name':<25} {'Source (MB)':<12} {'Replication (MB)':<15} {'Analytics (MB)':<13}")
                print("-" * 70)
                for _, row in synced_df.iterrows():
                    source_size = f"{row['source_size_mb']:.2f}" if row['source_size_mb'] is not None else "N/A"
                    replication_size = f"{row['replication_size_mb']:.2f}" if row['replication_size_mb'] is not None else "N/A"
                    analytics_size = f"{row['analytics_size_mb']:.2f}" if row['analytics_size_mb'] is not None else "N/A"
                    print(f"{row['table_name']:<25} {source_size:<12} {replication_size:<15} {analytics_size:<13}")
            
            return
        
        # If specific table is provided, compare it
        if args.table:
            table_name = args.table.strip().lower()
            schema_name = args.schema if args.schema != 'raw' else None
            
            print(f"\n🔍 Comparing table: {table_name}")
            comparison_df = compare_table_counts(source_engine, replication_engine, analytics_engine, table_name, schema_name)
            
            # Log the comparison results
            logger.info(f"Table count comparison completed for '{table_name}'")
            logger.info(f"Comparison results: {comparison_df.to_dict('records')}")
            
            return
        
        # Interactive mode (default if no table specified)
        if args.interactive or not args.table:
            print(f"\n🔍 Interactive Table Count Comparison")
            print(f"Enter a table name to compare counts across all databases (or 'quit' to exit):")
            print(f"Format: table_name or schema.table_name (e.g., 'patient' or 'staging.patient')")
            
            while True:
                table_input = input("\nEnter table name: ").strip().lower()
                
                if table_input.lower() in ['quit', 'exit', 'q']:
                    break
                    
                if not table_input:
                    print("Please enter a valid table name")
                    continue
                
                # Parse schema and table name
                if '.' in table_input:
                    schema_name, table_name = table_input.split('.', 1)
                else:
                    schema_name = None
                    table_name = table_input
                
                # Perform table count comparison
                comparison_df = compare_table_counts(source_engine, replication_engine, analytics_engine, table_name, schema_name)
                
                # Log the comparison results
                logger.info(f"Table count comparison completed for '{table_input}'")
                logger.info(f"Comparison results: {comparison_df.to_dict('records')}")
        
        logger.info(f"Database export completed - {len(source_df)} source, {len(replication_df)} replication, {len(analytics_df)} analytics tables")
        
    except Exception as e:
        logger.error(f"Error during database comparison: {e}")
        raise


if __name__ == "__main__":
    main() 