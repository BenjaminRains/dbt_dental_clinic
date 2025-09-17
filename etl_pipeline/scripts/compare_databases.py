#!/usr/bin/env python3
"""
Database Comparison Script for ETL Pipeline

This script connects to all three databases and compares their tables:
- opendental (source MySQL)
- opendental_replication (replication MySQL) 
- opendental_analytics (target PostgreSQL)

It creates pandas DataFrames for analysis and comparison.

NEW FEATURES:
- Percentage difference analysis between source and analytics databases
- Configurable threshold (default: 3%) for identifying significant differences
- Detailed logging of tables with differences above threshold
- Summary statistics including top differences and distribution analysis
- Command line option to customize threshold: --threshold <percentage>
"""

import sys
import os
import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import text
import logging
from datetime import datetime
import time

# Add the etl_pipeline directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config import get_settings
from etl_pipeline.config.logging import setup_run_logging, get_logger

# Create logs/compare_databases directory if it doesn't exist
import os
from pathlib import Path
from datetime import datetime

log_dir = Path(__file__).parent.parent / "logs" / "compare_databases"
log_dir.mkdir(parents=True, exist_ok=True)

# Generate datetime-based run ID
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
run_id = f"compare_{current_datetime}"

# Set up logging for this analysis
log_file_path = setup_run_logging(
    log_level="INFO",
    log_dir="logs/compare_databases",
    run_id=run_id
)
logger = get_logger(__name__)
logger.info(f"Starting database comparison analysis - Log file: {log_file_path}")


def calculate_percentage_difference(source_count, analytics_count, threshold=3.0):
    """
    Calculate percentage difference between source and analytics row counts.
    
    Args:
        source_count: Row count from source database
        analytics_count: Row count from analytics database
        threshold: Percentage threshold for significant differences (default: 3.0)
        
    Returns:
        Tuple of (percentage_difference, is_significant)
    """
    if source_count is None or analytics_count is None:
        return None, False
    
    if source_count == 0:
        if analytics_count == 0:
            return 0.0, False
        else:
            return float('inf'), True
    
    percentage_diff = abs(analytics_count - source_count) / source_count * 100
    is_significant = percentage_diff > threshold
    
    return round(percentage_diff, 2), is_significant


def log_percentage_differences(comparison_df, threshold=3.0):
    """
    Log tables with percentage differences above the threshold.
    
    Args:
        comparison_df: DataFrame with comparison results
        threshold: Percentage threshold for significant differences (default: 3.0)
    """
    logger.info(f"Analyzing percentage differences with {threshold}% threshold...")
    
    # Filter for tables that exist in both source and analytics
    valid_comparisons = comparison_df[
        (comparison_df['source_exists'] == True) & 
        (comparison_df['analytics_exists'] == True) &
        (comparison_df['source_count'].notna()) & 
        (comparison_df['analytics_count'].notna())
    ].copy()
    
    if valid_comparisons.empty:
        logger.info("No valid comparisons found for percentage difference analysis")
        return None, None
    
    # Calculate percentage differences
    percentage_data = []
    significant_differences = []
    
    for _, row in valid_comparisons.iterrows():
        source_count = row['source_count']
        analytics_count = row['analytics_count']
        
        percentage_diff, is_significant = calculate_percentage_difference(source_count, analytics_count)
        
        if percentage_diff is not None:
            percentage_data.append({
                'table_name': row['table_name'],
                'source_count': source_count,
                'analytics_count': analytics_count,
                'percentage_difference': percentage_diff,
                'is_significant': is_significant,
                'status': row['status']
            })
            
            if is_significant:
                significant_differences.append({
                    'table_name': row['table_name'],
                    'source_count': source_count,
                    'analytics_count': analytics_count,
                    'percentage_difference': percentage_diff,
                    'status': row['status']
                })
    
    # Log summary statistics
    total_tables = len(percentage_data)
    significant_count = len(significant_differences)
    
    logger.info(f"Percentage difference analysis complete:")
    logger.info(f"  Total tables analyzed: {total_tables}")
    logger.info(f"  Tables with >{threshold}% difference: {significant_count}")
    logger.info(f"  Tables within {threshold}% threshold: {total_tables - significant_count}")
    
    # Log significant differences
    if significant_differences:
        logger.warning(f"Tables with >{threshold}% difference from source:")
        for diff in sorted(significant_differences, key=lambda x: x['percentage_difference'], reverse=True):
            logger.warning(f"  {diff['table_name']}: {diff['percentage_difference']}% "
                         f"(Source: {diff['source_count']:,}, Analytics: {diff['analytics_count']:,}) "
                         f"Status: {diff['status']}")
    else:
        logger.info(f"All tables are within {threshold}% threshold of source database")
    
    # Log detailed percentage differences for all tables
    logger.info("Detailed percentage differences for all tables:")
    for diff in sorted(percentage_data, key=lambda x: x['percentage_difference'], reverse=True):
        logger.info(f"  {diff['table_name']}: {diff['percentage_difference']}% "
                   f"(Source: {diff['source_count']:,}, Analytics: {diff['analytics_count']:,}) "
                   f"Status: {diff['status']}")
    
    return percentage_data, significant_differences


def print_percentage_difference_summary(percentage_data, significant_differences, threshold=3.0):
    """
    Print a summary of percentage differences for console output.
    
    Args:
        percentage_data: List of percentage difference data for all tables
        significant_differences: List of tables with significant differences
        threshold: Percentage threshold used
    """
    if not percentage_data:
        print(f"\n📊 No percentage difference data available")
        return
    
    total_tables = len(percentage_data)
    significant_count = len(significant_differences)
    
    print(f"\n📊 Percentage Difference Summary (Source vs Analytics):")
    print(f"  Total tables analyzed: {total_tables}")
    print(f"  Tables with >{threshold}% difference: {significant_count}")
    print(f"  Tables within {threshold}% threshold: {total_tables - significant_count}")
    
    if significant_differences:
        print(f"\n⚠️  Tables with >{threshold}% difference from source:")
        for diff in sorted(significant_differences, key=lambda x: x['percentage_difference'], reverse=True):
            print(f"  {diff['table_name']}: {diff['percentage_difference']}% "
                 f"(Source: {diff['source_count']:,}, Analytics: {diff['analytics_count']:,})")
        
        # Show top 5 largest differences
        print(f"\n🔝 Top 5 Largest Differences:")
        top_5 = sorted(significant_differences, key=lambda x: x['percentage_difference'], reverse=True)[:5]
        for i, diff in enumerate(top_5, 1):
            print(f"  {i}. {diff['table_name']}: {diff['percentage_difference']}% "
                 f"(Source: {diff['source_count']:,}, Analytics: {diff['analytics_count']:,})")
    else:
        print(f"\n✅ All tables are within {threshold}% threshold of source database")
    
    # Show distribution of differences
    if percentage_data:
        differences = [diff['percentage_difference'] for diff in percentage_data if diff['percentage_difference'] is not None]
        if differences:
            avg_diff = sum(differences) / len(differences)
            max_diff = max(differences)
            min_diff = min(differences)
            print(f"\n📈 Difference Statistics:")
            print(f"  Average difference: {avg_diff:.2f}%")
            print(f"  Maximum difference: {max_diff:.2f}%")
            print(f"  Minimum difference: {min_diff:.2f}%")


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
    """
    Get exact row counts for multiple tables with timeout protection.
    
    Args:
        engine: Database engine
        database_name: Name of the database
        table_names: List of table names to count
    
    Returns:
        Dict mapping table names to row counts
    """
    results = {}
    
    if database_name == 'opendental_analytics':
        # PostgreSQL - get exact counts for all tables in raw schema
        for table_name in table_names:
            try:
                with engine.connect() as conn:
                    # Set PostgreSQL timeouts
                    conn.execute(text("SET statement_timeout = '300s'"))
                    conn.execute(text("SET lock_timeout = '60s'"))
                    
                    # Use exact COUNT(*) for all tables
                    result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}"))
                    count = result.scalar()
                    results[table_name] = count
                    
            except Exception as e:
                logger.warning(f"Could not get row count for {table_name}: {e}")
                results[table_name] = 0
    else:
        # MySQL - get exact counts for all tables
        for table_name in table_names:
            try:
                with engine.connect() as conn:
                    # Set MySQL timeouts
                    conn.execute(text("SET SESSION net_read_timeout = 300"))
                    conn.execute(text("SET SESSION wait_timeout = 600"))
                    conn.execute(text("SET SESSION interactive_timeout = 600"))
                    
                    # Use exact COUNT(*) for all tables
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


def check_duplicate_keys(engine, database_name, table_name, schema_name=None):
    """
    Check for duplicate primary keys and other potential duplicates in a table.
    
    Args:
        engine: Database engine
        database_name: Name of the database
        table_name: Name of the table to check
        schema_name: Schema name for PostgreSQL (optional)
        
    Returns:
        Dictionary with duplicate analysis results
    """
    logger.info(f"Checking duplicates for {database_name}.{table_name}")
    
    results = {
        'database': database_name,
        'table_name': table_name,
        'duplicate_primary_keys': [],
        'duplicate_records': [],
        'total_duplicates': 0,
        'error': None
    }
    
    try:
        # Get primary key column name
        if database_name == 'opendental_analytics':
            # PostgreSQL
            if schema_name:
                pk_query = """
                SELECT column_name 
                FROM information_schema.key_column_usage 
                WHERE table_schema = :schema_name 
                AND table_name = :table_name 
                AND constraint_name LIKE '%_pkey'
                """
                params = {"schema_name": schema_name, "table_name": table_name}
            else:
                pk_query = """
                SELECT column_name 
                FROM information_schema.key_column_usage 
                WHERE table_schema = 'raw' 
                AND table_name = :table_name 
                AND constraint_name LIKE '%_pkey'
                """
                params = {"table_name": table_name}
        else:
            # MySQL
            pk_query = """
            SELECT COLUMN_NAME 
            FROM information_schema.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = :database_name 
            AND TABLE_NAME = :table_name 
            AND CONSTRAINT_NAME = 'PRIMARY'
            """
            params = {"database_name": database_name, "table_name": table_name}
        
        with engine.connect() as conn:
            pk_result = conn.execute(text(pk_query), params)
            pk_columns = [row[0] for row in pk_result.fetchall()]
        
        if not pk_columns:
            logger.warning(f"No primary key found for {database_name}.{table_name}")
            results['error'] = "No primary key found"
            return results
        
        # Check for duplicate primary keys
        pk_column = pk_columns[0]  # Use first primary key column
        
        # Ensure proper quoting for PostgreSQL
        if database_name == 'opendental_analytics':
            quoted_pk_column = f'"{pk_column}"'
        else:
            quoted_pk_column = pk_column
        
        if database_name == 'opendental_analytics':
            if schema_name:
                duplicate_pk_query = f"""
                SELECT {quoted_pk_column}, COUNT(*) as duplicate_count
                FROM {schema_name}.{table_name}
                GROUP BY {quoted_pk_column}
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
                """
            else:
                duplicate_pk_query = f"""
                SELECT {quoted_pk_column}, COUNT(*) as duplicate_count
                FROM raw.{table_name}
                GROUP BY {quoted_pk_column}
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
                """
        else:
            duplicate_pk_query = f"""
            SELECT {quoted_pk_column}, COUNT(*) as duplicate_count
            FROM {database_name}.{table_name}
            GROUP BY {quoted_pk_column}
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            """
        
        with engine.connect() as conn:
            duplicate_result = conn.execute(text(duplicate_pk_query))
            duplicate_pks = duplicate_result.fetchall()
        
        # Store duplicate primary keys
        for pk_value, count in duplicate_pks:
            results['duplicate_primary_keys'].append({
                'primary_key_value': pk_value,
                'duplicate_count': count
            })
        
        # Check for duplicate records based on primary key only
        # Use only the primary key column for duplicate checking
        if database_name == 'opendental_analytics':
            key_fields = [f'"{pk_column}"']
        else:
            key_fields = [pk_column]
        
        # Check if all key fields exist in the table
        if database_name == 'opendental_analytics':
            if schema_name:
                columns_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema_name 
                AND table_name = :table_name
                """
                params = {"schema_name": schema_name, "table_name": table_name}
            else:
                columns_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'raw' 
                AND table_name = :table_name
                """
                params = {"table_name": table_name}
        else:
            columns_query = f"""
            SELECT COLUMN_NAME 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = :database_name 
            AND TABLE_NAME = :table_name
            """
            params = {"database_name": database_name, "table_name": table_name}
        
        with engine.connect() as conn:
            columns_result = conn.execute(text(columns_query), params)
            available_columns = [row[0] for row in columns_result.fetchall()]
        
        # Filter key fields to only those that exist in the table
        existing_key_fields = [field for field in key_fields if field in available_columns]
        
        if len(existing_key_fields) > 1:
            # Check for duplicate records based on multiple fields
            key_fields_str = ', '.join(existing_key_fields)
            
            if database_name == 'opendental_analytics':
                if schema_name:
                    duplicate_records_query = f"""
                    SELECT {key_fields_str}, COUNT(*) as duplicate_count
                    FROM {schema_name}.{table_name}
                    GROUP BY {key_fields_str}
                    HAVING COUNT(*) > 1
                    ORDER BY duplicate_count DESC
                    LIMIT 10
                    """
                else:
                    duplicate_records_query = f"""
                    SELECT {key_fields_str}, COUNT(*) as duplicate_count
                    FROM raw.{table_name}
                    GROUP BY {key_fields_str}
                    HAVING COUNT(*) > 1
                    ORDER BY duplicate_count DESC
                    LIMIT 10
                    """
            else:
                duplicate_records_query = f"""
                SELECT {key_fields_str}, COUNT(*) as duplicate_count
                FROM {database_name}.{table_name}
                GROUP BY {key_fields_str}
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
                LIMIT 10
                """
            
            with engine.connect() as conn:
                duplicate_records_result = conn.execute(text(duplicate_records_query))
                duplicate_records = duplicate_records_result.fetchall()
            
            # Store duplicate records
            for record in duplicate_records:
                key_values = record[:-1]  # All but the last (count)
                count = record[-1]
                results['duplicate_records'].append({
                    'key_values': key_values,
                    'duplicate_count': count
                })
        
        # Calculate total duplicates
        total_pk_duplicates = sum(item['duplicate_count'] for item in results['duplicate_primary_keys'])
        total_record_duplicates = sum(item['duplicate_count'] for item in results['duplicate_records'])
        results['total_duplicates'] = total_pk_duplicates + total_record_duplicates
        
        logger.info(f"Duplicate check completed for {database_name}.{table_name}: {results['total_duplicates']} total duplicates")
        
    except Exception as e:
        logger.error(f"Error checking duplicates for {database_name}.{table_name}: {e}")
        results['error'] = str(e)
    
    return results


def compare_table_counts(source_engine, replication_engine, analytics_engine, table_name, schema_name=None):
    """
    Compare exact row counts for a specific table across all three databases.
    
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
    start_time = time.time()
    
    try:
        # Source database (MySQL)
        with source_engine.connect() as conn:
            # Set MySQL timeouts
            conn.execute(text("SET SESSION net_read_timeout = 300"))
            conn.execute(text("SET SESSION wait_timeout = 600"))
            conn.execute(text("SET SESSION interactive_timeout = 600"))
            
            # Use exact COUNT(*) for all tables
            result = conn.execute(text(f"SELECT COUNT(*) as row_count FROM opendental.{table_name}"))
            source_count = result.scalar()
            
            results.append({
                'database': 'opendental (source)',
                'table_name': table_name,
                'row_count': source_count,
                'database_type': 'mysql'
            })
            logger.info(f"Source database {table_name}: {source_count:,} rows")
            
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
            # Set MySQL timeouts
            conn.execute(text("SET SESSION net_read_timeout = 300"))
            conn.execute(text("SET SESSION wait_timeout = 600"))
            conn.execute(text("SET SESSION interactive_timeout = 600"))
            
            # Use exact COUNT(*) for all tables
            result = conn.execute(text(f"SELECT COUNT(*) as row_count FROM opendental_replication.{table_name}"))
            replication_count = result.scalar()
            
            results.append({
                'database': 'opendental_replication',
                'table_name': table_name,
                'row_count': replication_count,
                'database_type': 'mysql'
            })
            logger.info(f"Replication database {table_name}: {replication_count:,} rows")
            
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
        with analytics_engine.connect() as conn:
            # Set PostgreSQL timeouts
            conn.execute(text("SET statement_timeout = '300s'"))
            conn.execute(text("SET lock_timeout = '60s'"))
            
            if schema_name:
                table_ref = f"{schema_name}.{table_name}"
            else:
                table_ref = f"raw.{table_name}"
            
            # Use exact COUNT(*) for all tables
            result = conn.execute(text(f"SELECT COUNT(*) as row_count FROM {table_ref}"))
            analytics_count = result.scalar()
            
            results.append({
                'database': f'opendental_analytics ({schema_name or "raw"})',
                'table_name': table_name,
                'row_count': analytics_count,
                'database_type': 'postgresql'
            })
            logger.info(f"Analytics database {table_name}: {analytics_count:,} rows")
            
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
            
        # Add summary with timing
        duration = time.time() - start_time
        summary_msg = f"\n📊 Table Count Comparison for '{table_name}' (completed in {duration:.2f}s):\nSource (opendental): {source_count:,} rows\nReplication (opendental_replication): {replication_count:,} rows\nAnalytics (opendental_analytics.{schema_name or 'raw'}): {analytics_count:,} rows"
        print(summary_msg)
        logger.info(summary_msg)
        
        # Calculate and display percentage differences
        if source_count is not None and analytics_count is not None:
            percentage_diff, is_significant = calculate_percentage_difference(source_count, analytics_count, threshold=3.0)
            if percentage_diff is not None:
                if is_significant:
                    diff_msg = f"⚠️  Source vs Analytics: {percentage_diff}% difference (>3% threshold)"
                    print(diff_msg)
                    logger.warning(diff_msg)
                else:
                    diff_msg = f"✅ Source vs Analytics: {percentage_diff}% difference (within 3% threshold)"
                    print(diff_msg)
                    logger.info(diff_msg)
        
        if source_count == replication_count == analytics_count:
            success_msg = "✅ All databases have the same row count"
            print(success_msg)
            logger.info(success_msg)
        else:
            warning_msg = "⚠️  Row count differences detected"
            print(warning_msg)
            logger.warning(warning_msg)
    
    return df


def check_duplicates_for_table(source_engine, replication_engine, analytics_engine, table_name, schema_name=None):
    """
    Check for duplicates across all three databases for a specific table.
    
    Args:
        source_engine: Source database engine
        replication_engine: Replication database engine
        analytics_engine: Analytics database engine
        table_name: Name of the table to check
        schema_name: Schema name for analytics database (optional)
    """
    logger.info(f"Checking duplicates for table: {table_name}")
    
    # Check duplicates in source database
    source_results = check_duplicate_keys(source_engine, 'opendental', table_name)
    if source_results['error']:
        logger.error(f"Error checking source duplicates: {source_results['error']}")
    else:
        logger.info(f"Source duplicates: {source_results['total_duplicates']} total")
        if source_results['duplicate_primary_keys']:
            logger.warning(f"Source duplicate primary keys: {len(source_results['duplicate_primary_keys'])} instances")
        if source_results['duplicate_records']:
            logger.warning(f"Source duplicate records: {len(source_results['duplicate_records'])} instances")
    
    # Check duplicates in replication database
    replication_results = check_duplicate_keys(replication_engine, 'opendental_replication', table_name)
    if replication_results['error']:
        logger.error(f"Error checking replication duplicates: {replication_results['error']}")
    else:
        logger.info(f"Replication duplicates: {replication_results['total_duplicates']} total")
        if replication_results['duplicate_primary_keys']:
            logger.warning(f"Replication duplicate primary keys: {len(replication_results['duplicate_primary_keys'])} instances")
        if replication_results['duplicate_records']:
            logger.warning(f"Replication duplicate records: {len(replication_results['duplicate_records'])} instances")
    
    # Check duplicates in analytics database
    analytics_results = check_duplicate_keys(analytics_engine, 'opendental_analytics', table_name, schema_name)
    if analytics_results['error']:
        logger.error(f"Error checking analytics duplicates: {analytics_results['error']}")
    else:
        logger.info(f"Analytics duplicates: {analytics_results['total_duplicates']} total")
        if analytics_results['duplicate_primary_keys']:
            logger.warning(f"Analytics duplicate primary keys: {len(analytics_results['duplicate_primary_keys'])} instances")
        if analytics_results['duplicate_records']:
            logger.warning(f"Analytics duplicate records: {len(analytics_results['duplicate_records'])} instances")
    
    # Summary
    total_source_duplicates = source_results.get('total_duplicates', 0)
    total_replication_duplicates = replication_results.get('total_duplicates', 0)
    total_analytics_duplicates = analytics_results.get('total_duplicates', 0)
    
    print(f"\n📊 Duplicate Analysis Summary for '{table_name}':")
    print(f"Source (opendental): {total_source_duplicates} duplicates")
    print(f"Replication (opendental_replication): {total_replication_duplicates} duplicates")
    print(f"Analytics (opendental_analytics.{schema_name or 'raw'}): {total_analytics_duplicates} duplicates")
    
    if total_source_duplicates > 0 or total_replication_duplicates > 0 or total_analytics_duplicates > 0:
        print("⚠️  Duplicates detected in one or more databases")
    else:
        print("✅ No duplicates found in any database")


def compare_tracking_tables(source_engine, replication_engine, analytics_engine):
    """
    Compare ETL tracking tables across databases to diagnose pipeline issues.
    
    Args:
        source_engine: MySQL engine for source database
        replication_engine: MySQL engine for replication database  
        analytics_engine: PostgreSQL engine for analytics database
        
    Returns:
        DataFrame with tracking table comparison results
    """
    logger.info("Comparing ETL tracking tables across databases...")
    
    results = []
    
    # Check etl_copy_status table (source -> replication tracking)
    logger.info("Checking etl_copy_status table...")
    
    try:
        # Source database etl_copy_status
        with source_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, last_copied, last_primary_value, primary_column_name, rows_copied, copy_status, _updated_at
                FROM opendental.etl_copy_status 
                ORDER BY _updated_at DESC
                LIMIT 20
            """))
            source_copy_status = result.fetchall()
            
            for row in source_copy_status:
                results.append({
                    'database': 'opendental (source)',
                    'table_name': row[0],
                    'tracking_type': 'copy_status',
                    'last_time': row[1],
                    'last_primary_value': row[2],
                    'primary_column_name': row[3],
                    'rows_count': row[4],
                    'status': row[5],
                    'tracking_time': row[6]
                })
                
    except Exception as e:
        logger.error(f"Error getting source etl_copy_status: {e}")
        results.append({
            'database': 'opendental (source)',
            'table_name': 'N/A',
            'tracking_type': 'copy_status',
            'last_time': None,
            'rows_count': None,
            'status': 'ERROR',
            'tracking_time': None,
            'error': str(e)
        })
    
    try:
        # Replication database etl_copy_status
        with replication_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, last_copied, last_primary_value, primary_column_name, rows_copied, copy_status, _updated_at
                FROM opendental_replication.etl_copy_status 
                ORDER BY _updated_at DESC
                LIMIT 20
            """))
            replication_copy_status = result.fetchall()
            
            for row in replication_copy_status:
                results.append({
                    'database': 'opendental_replication',
                    'table_name': row[0],
                    'tracking_type': 'copy_status',
                    'last_time': row[1],
                    'last_primary_value': row[2],
                    'primary_column_name': row[3],
                    'rows_count': row[4],
                    'status': row[5],
                    'tracking_time': row[6]
                })
                
    except Exception as e:
        logger.error(f"Error getting replication etl_copy_status: {e}")
        results.append({
            'database': 'opendental_replication',
            'table_name': 'N/A',
            'tracking_type': 'copy_status',
            'last_time': None,
            'rows_count': None,
            'status': 'ERROR',
            'tracking_time': None,
            'error': str(e)
        })
    
    # Check etl_load_status table (replication -> analytics tracking)
    logger.info("Checking etl_load_status table...")
    
    try:
        # Analytics database etl_load_status
        with analytics_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, last_loaded, last_primary_value, primary_column_name, rows_loaded, load_status, _loaded_at
                FROM raw.etl_load_status 
                ORDER BY _loaded_at DESC
                LIMIT 20
            """))
            analytics_load_status = result.fetchall()
            
            for row in analytics_load_status:
                results.append({
                    'database': 'opendental_analytics',
                    'table_name': row[0],
                    'tracking_type': 'load_status',
                    'last_loaded': row[1],
                    'last_primary_value': row[2],
                    'primary_column_name': row[3],
                    'rows_count': row[4],
                    'status': row[5],
                    'tracking_time': row[6]
                })
                
    except Exception as e:
        logger.error(f"Error getting analytics etl_load_status: {e}")
        results.append({
            'database': 'opendental_analytics',
            'table_name': 'N/A',
            'tracking_type': 'load_status',
            'last_loaded': None,
            'last_primary_value': None,
            'primary_column_name': None,
            'rows_count': None,
            'status': 'ERROR',
            'tracking_time': None,
            'error': str(e)
        })
    
    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Print summary
    print(f"\n📊 ETL Tracking Tables Comparison:")
    print(f"Total tracking records found: {len(df)}")
    
    if not df.empty:
        # Group by table name and show latest status for each
        print(f"\n🔍 Latest Tracking Status by Table:")
        
        # For copy status (source -> replication)
        copy_status = df[df['tracking_type'] == 'copy_status']
        if not copy_status.empty:
            print(f"\n📋 Copy Status (Source -> Replication):")
            for _, row in copy_status.iterrows():
                print(f"  {row['table_name']}: {row['status']} at {row['tracking_time']} ({row['rows_count']} rows)")
                if row['last_time']:
                    print(f"    Last copied: {row['last_time']}")
                if row['last_primary_value']:
                    print(f"    Last primary value: {row['last_primary_value']} ({row['primary_column_name']})")
        
        # For load status (replication -> analytics)
        load_status = df[df['tracking_type'] == 'load_status']
        if not load_status.empty:
            print(f"\n📋 Load Status (Replication -> Analytics):")
            for _, row in load_status.iterrows():
                print(f"  {row['table_name']}: {row['status']} at {row['tracking_time']} ({row['rows_count']} rows)")
                if row['last_loaded']:
                    print(f"    Last loaded: {row['last_loaded']}")
                if row['last_primary_value']:
                    print(f"    Last primary value: {row['last_primary_value']} ({row['primary_column_name']})")
    
    return df


def get_tracking_details_for_table(source_engine, replication_engine, analytics_engine, table_name):
    """
    Get detailed tracking information for a specific table.
    
    Args:
        source_engine: MySQL engine for source database
        replication_engine: MySQL engine for replication database  
        analytics_engine: PostgreSQL engine for analytics database
        table_name: Name of the table to check
        
    Returns:
        Dictionary with detailed tracking information
    """
    logger.info(f"Getting detailed tracking information for table: {table_name}")
    
    details = {
        'table_name': table_name,
        'copy_status': {},
        'load_status': {},
        'errors': []
    }
    
    # Get copy status from source
    try:
        with source_engine.connect() as conn:
            # First check if the table exists
            table_exists = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'opendental' AND table_name = 'etl_copy_status'
            """)).scalar()
            
            if table_exists:
                result = conn.execute(text("""
                    SELECT table_name, last_copied, last_primary_value, primary_column_name, rows_copied, copy_status, _updated_at
                    FROM opendental.etl_copy_status 
                    WHERE table_name = :table_name
                    ORDER BY _updated_at DESC
                    LIMIT 1
                """), {"table_name": table_name})
                row = result.fetchone()
                
                if row:
                    details['copy_status']['source'] = {
                        'last_copy_time': row[1],  # last_copied
                        'last_primary_value': row[2],
                        'primary_column_name': row[3],
                        'rows_copied': row[4],
                        'copy_status': row[5],
                        '_copied_at': row[6]  # _updated_at
                    }
                else:
                    details['copy_status']['source'] = None
            else:
                details['copy_status']['source'] = None
                logger.info(f"Source database does not have etl_copy_status table")
                
    except Exception as e:
        error_msg = f"Error getting source copy status for {table_name}: {e}"
        logger.error(error_msg)
        details['errors'].append(error_msg)
    
    # Get copy status from replication
    try:
        with replication_engine.connect() as conn:
            # First check if the table exists
            table_exists = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'opendental_replication' AND table_name = 'etl_copy_status'
            """)).scalar()
            
            if table_exists:
                # Check the actual column structure
                columns_result = conn.execute(text("""
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = 'opendental_replication' AND TABLE_NAME = 'etl_copy_status'
                    ORDER BY ORDINAL_POSITION
                """))
                columns = [row[0] for row in columns_result.fetchall()]
                
                # Check which columns are actually available
                available_columns = set(columns)
                
                # Use the correct column names based on actual table structure from initialize_etl_tracking_tables.py
                if 'last_copied' in available_columns and '_updated_at' in available_columns:
                    result = conn.execute(text("""
                        SELECT table_name, last_copied, last_primary_value, primary_column_name, rows_copied, copy_status, _updated_at
                        FROM opendental_replication.etl_copy_status 
                        WHERE table_name = :table_name
                        ORDER BY _updated_at DESC
                        LIMIT 1
                    """), {"table_name": table_name})
                elif 'last_copied' in available_columns and '_created_at' in available_columns:
                    result = conn.execute(text("""
                        SELECT table_name, last_copied, last_primary_value, primary_column_name, rows_copied, copy_status, _created_at
                        FROM opendental_replication.etl_copy_status 
                        WHERE table_name = :table_name
                        ORDER BY _created_at DESC
                        LIMIT 1
                    """), {"table_name": table_name})
                else:
                    # Fallback to basic structure
                    logger.warning(f"Replication etl_copy_status table has unexpected structure: {columns}")
                    # Try to get at least basic info
                    if 'table_name' in available_columns and 'rows_copied' in available_columns:
                        result = conn.execute(text("""
                            SELECT table_name, rows_copied, copy_status
                            FROM opendental_replication.etl_copy_status 
                            WHERE table_name = :table_name
                            LIMIT 1
                        """), {"table_name": table_name})
                    else:
                        logger.error(f"Cannot query replication etl_copy_status - unexpected columns: {columns}")
                        details['copy_status']['replication'] = None
                        return details
                
                row = result.fetchone()
                
                if row:
                    # Handle different column structures based on what's available
                    if 'last_copied' in available_columns and '_updated_at' in available_columns:
                        details['copy_status']['replication'] = {
                            'last_copy_time': row[1],  # last_copied
                            'last_primary_value': row[2],
                            'primary_column_name': row[3],
                            'rows_copied': row[4],
                            'copy_status': row[5],
                            '_copied_at': row[6]  # _updated_at
                        }
                    elif 'last_copied' in available_columns and '_created_at' in available_columns:
                        details['copy_status']['replication'] = {
                            'last_copy_time': row[1],  # last_copied
                            'last_primary_value': row[2],
                            'primary_column_name': row[3],
                            'rows_copied': row[4],
                            'copy_status': row[5],
                            '_copied_at': row[6]  # _created_at
                        }
                    else:
                        # Basic structure without timestamp columns
                        details['copy_status']['replication'] = {
                            'last_copy_time': None,
                            'last_primary_value': row[1],
                            'primary_column_name': row[2],
                            'rows_copied': row[3],
                            'copy_status': row[4],
                            '_copied_at': None
                        }
                else:
                    details['copy_status']['replication'] = None
            else:
                details['copy_status']['replication'] = None
                logger.info(f"Replication database does not have etl_copy_status table")
                
    except Exception as e:
        error_msg = f"Error getting replication copy status for {table_name}: {e}"
        logger.error(error_msg)
        details['errors'].append(error_msg)
    
    # Get load status from analytics
    try:
        with analytics_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, last_loaded, last_primary_value, primary_column_name, rows_loaded, load_status, _loaded_at
                FROM raw.etl_load_status 
                WHERE table_name = :table_name
                ORDER BY _loaded_at DESC
                LIMIT 1
            """), {"table_name": table_name})
            row = result.fetchone()
            
            if row:
                details['load_status']['analytics'] = {
                    'last_loaded': row[1],
                    'last_primary_value': row[2],
                    'primary_column_name': row[3],
                    'rows_loaded': row[4],
                    'load_status': row[5],
                    '_loaded_at': row[6]
                }
            else:
                details['load_status']['analytics'] = None
                
    except Exception as e:
        error_msg = f"Error getting analytics load status for {table_name}: {e}"
        logger.error(error_msg)
        details['errors'].append(error_msg)
    
    # Print detailed information
    print(f"\n🔍 Detailed Tracking Information for '{table_name}':")
    
    # Copy status
    print(f"\n📋 Copy Status (Source -> Replication):")
    if 'source' in details['copy_status'] and details['copy_status']['source']:
        source_copy = details['copy_status']['source']
        print(f"  Source: {source_copy['copy_status']} at {source_copy['_copied_at']} ({source_copy['rows_copied']} rows)")
        if source_copy['last_copy_time']:
            print(f"    Last copied: {source_copy['last_copy_time']}")
        if source_copy['last_primary_value']:
            print(f"    Last primary value: {source_copy['last_primary_value']} ({source_copy['primary_column_name']})")
    else:
        print(f"  Source: No copy status found (table may not exist or no records for this table)")
    
    if 'replication' in details['copy_status'] and details['copy_status']['replication']:
        repl_copy = details['copy_status']['replication']
        print(f"  Replication: {repl_copy['copy_status']} at {repl_copy['_copied_at']} ({repl_copy['rows_copied']} rows)")
        if repl_copy['last_copy_time']:
            print(f"    Last copied: {repl_copy['last_copy_time']}")
        if repl_copy['last_primary_value']:
            print(f"    Last primary value: {repl_copy['last_primary_value']} ({repl_copy['primary_column_name']})")
    else:
        print(f"  Replication: No copy status found (table may not exist or no records for this table)")
    
    # Load status
    print(f"\n📋 Load Status (Replication -> Analytics):")
    if 'analytics' in details['load_status'] and details['load_status']['analytics']:
        analytics_load = details['load_status']['analytics']
        print(f"  Analytics: {analytics_load['load_status']} at {analytics_load['_loaded_at']} ({analytics_load['rows_loaded']} rows)")
        if analytics_load['last_loaded']:
            print(f"    Last loaded: {analytics_load['last_loaded']}")
        if analytics_load['last_primary_value']:
            print(f"    Last primary value: {analytics_load['last_primary_value']} ({analytics_load['primary_column_name']})")
    else:
        print(f"  Analytics: No load status found (no tracking records for this table)")
    
    # Summary
    has_copy_status = any(details['copy_status'].values())
    has_load_status = any(details['load_status'].values())
    
    if not has_copy_status and not has_load_status:
        print(f"\n⚠️  No tracking information found for '{table_name}'")
        print(f"   This means the table may not have been processed by the ETL pipeline yet")
        print(f"   or tracking tables are not set up for this table")
    
    # Errors
    if details['errors']:
        print(f"\n❌ Errors:")
        for error in details['errors']:
            print(f"  {error}")
    
    return details


def compare_all_tables(source_engine, replication_engine, analytics_engine, source_df, replication_df, analytics_df, threshold=3.0):
    """
    Compare all tables across all three databases.
    
    Args:
        source_engine: MySQL engine for source database
        replication_engine: MySQL engine for replication database  
        analytics_engine: PostgreSQL engine for analytics database
        source_df: DataFrame with source database tables
        replication_df: DataFrame with replication database tables
        analytics_df: DataFrame with analytics database tables
        
    Returns:
        DataFrame with comparison results for all tables
    """
    logger.info("Starting comparison of all tables across databases...")
    
    # Get all unique table names
    all_tables = set()
    if not source_df.empty:
        all_tables.update(source_df['table_name'].tolist())
    if not replication_df.empty:
        all_tables.update(replication_df['table_name'].tolist())
    if not analytics_df.empty:
        all_tables.update(analytics_df['table_name'].tolist())
    
    all_tables = sorted(all_tables)
    total_tables = len(all_tables)
    
    print(f"\n🔍 Comparing {total_tables} tables across all databases...")
    logger.info(f"Comparing {total_tables} tables across all databases")
    
    # Create comparison DataFrame
    comparison_data = []
    processed_count = 0
    
    for table_name in all_tables:
        processed_count += 1
        if processed_count % 10 == 0:
            print(f"Progress: {processed_count}/{total_tables} tables processed...")
        
        # Check if table exists in each database
        source_exists = table_name in source_df['table_name'].values if not source_df.empty else False
        replication_exists = table_name in replication_df['table_name'].values if not replication_df.empty else False
        analytics_exists = table_name in analytics_df['table_name'].values if not analytics_df.empty else False
        
        # Get metadata
        source_meta = source_df[source_df['table_name'] == table_name].iloc[0] if source_exists else None
        replication_meta = replication_df[replication_df['table_name'] == table_name].iloc[0] if replication_exists else None
        analytics_meta = analytics_df[analytics_df['table_name'] == table_name].iloc[0] if analytics_exists else None
        
        # Get row counts with timeout protection
        source_count = None
        replication_count = None
        analytics_count = None
        
        try:
            if source_exists:
                with source_engine.connect() as conn:
                    conn.execute(text("SET SESSION net_read_timeout = 300"))
                    conn.execute(text("SET SESSION wait_timeout = 600"))
                    conn.execute(text("SET SESSION interactive_timeout = 600"))
                    result = conn.execute(text(f"SELECT COUNT(*) FROM opendental.{table_name}"))
                    source_count = result.scalar()
        except Exception as e:
            logger.warning(f"Could not get row count for source {table_name}: {e}")
        
        try:
            if replication_exists:
                with replication_engine.connect() as conn:
                    conn.execute(text("SET SESSION net_read_timeout = 300"))
                    conn.execute(text("SET SESSION wait_timeout = 600"))
                    conn.execute(text("SET SESSION interactive_timeout = 600"))
                    result = conn.execute(text(f"SELECT COUNT(*) FROM opendental_replication.{table_name}"))
                    replication_count = result.scalar()
        except Exception as e:
            logger.warning(f"Could not get row count for replication {table_name}: {e}")
        
        try:
            if analytics_exists:
                with analytics_engine.connect() as conn:
                    conn.execute(text("SET statement_timeout = '300s'"))
                    conn.execute(text("SET lock_timeout = '60s'"))
                    result = conn.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}"))
                    analytics_count = result.scalar()
        except Exception as e:
            logger.warning(f"Could not get row count for analytics {table_name}: {e}")
        
        # Get sizes
        source_size = source_meta['size_mb'] if source_meta is not None else None
        replication_size = replication_meta['size_mb'] if replication_meta is not None else None
        analytics_size = analytics_meta['size_mb'] if analytics_meta is not None else None
        
        # Determine status
        status = _get_table_status(source_exists, replication_exists, analytics_exists, 
                                  source_count, replication_count, analytics_count)
        
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
            'status': status
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Print summary statistics
    print(f"\n📊 All Tables Comparison Complete!")
    print(f"Total tables processed: {len(comparison_df)}")
    
    # Status breakdown
    status_counts = comparison_df['status'].value_counts()
    print(f"\n📈 Status Breakdown:")
    for status, count in status_counts.items():
        print(f"  {status}: {count} tables")
    
    # Show tables with issues
    issues_df = comparison_df[comparison_df['status'] != 'SYNCED']
    if not issues_df.empty:
        print(f"\n⚠️  Tables with Issues ({len(issues_df)}):")
        for _, row in issues_df.iterrows():
            print(f"  {row['table_name']}: {row['status']}")
            if row['source_count'] is not None and row['replication_count'] is not None and row['analytics_count'] is not None:
                print(f"    Counts - Source: {row['source_count']:,}, Replication: {row['replication_count']:,}, Analytics: {row['analytics_count']:,}")
    
    # Show perfectly synced tables
    synced_df = comparison_df[comparison_df['status'] == 'SYNCED']
    if not synced_df.empty:
        print(f"\n✅ Perfectly Synced Tables ({len(synced_df)}):")
        for _, row in synced_df.head(10).iterrows():  # Show first 10
            print(f"  {row['table_name']}: {row['source_count']:,} rows")
        if len(synced_df) > 10:
            print(f"  ... and {len(synced_df) - 10} more")
    
    # Add percentage difference analysis
    print(f"\n📊 Percentage Difference Analysis (Source vs Analytics):")
    percentage_data, significant_differences = log_percentage_differences(comparison_df, threshold=threshold)
    
    if percentage_data and significant_differences:
        print_percentage_difference_summary(percentage_data, significant_differences, threshold=threshold)
    
    logger.info(f"All tables comparison completed - {len(comparison_df)} tables processed")
    return comparison_df


def main():
    """Main analysis function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Compare databases and tables across ETL pipeline. By default, compares all tables. Use --table to compare specific table, --interactive for interactive mode, or --list-tables to view all tables.')
    parser.add_argument('--table', '-t', type=str, help='Specific table name to compare')
    parser.add_argument('--schema', '-s', type=str, default='raw', 
                       help='Schema name for PostgreSQL table (default: raw)')
    parser.add_argument('--interactive', '-i', action='store_true', 
                       help='Run in interactive mode')
    parser.add_argument('--list-tables', '-l', action='store_true',
                       help='List all tables from all databases')
    parser.add_argument('--tracking', action='store_true',
                       help='Compare ETL tracking tables across databases')
    parser.add_argument('--tracking-details', type=str,
                       help='Get detailed tracking information for a specific table')
    parser.add_argument('--threshold', type=float, default=3.0,
                       help='Percentage threshold for significant differences (default: 3.0)')
    
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
        summary_msg = f"\n📊 Database Export Complete\nSource (opendental): {len(source_df)} tables\nReplication (opendental_replication): {len(replication_df)} tables\nAnalytics (opendental_analytics): {len(analytics_df)} tables\n📝 Log file: {log_file_path}"
        print(summary_msg)
        logger.info(summary_msg)
        
        # If tracking comparison is requested
        if args.tracking:
            logger.info(f"\n🔍 Comparing ETL tracking tables across databases")
            tracking_df = compare_tracking_tables(source_engine, replication_engine, analytics_engine)
            logger.info(f"Tracking table comparison completed")
            return
        
        # If detailed tracking information is requested for a specific table
        if args.tracking_details:
            table_name = args.tracking_details
            logger.info(f"\n🔍 Getting detailed tracking information for table: {table_name}")
            tracking_details = get_tracking_details_for_table(source_engine, replication_engine, analytics_engine, table_name)
            logger.info(f"Detailed tracking information completed for '{table_name}'")
            return
        
        # If specific table is provided, compare it
        if args.table:
            logger.info(f"\n🔍 Comparing table: {args.table}")
            
            # Parse schema and table name
            if '.' in args.table:
                schema_name, table_name = args.table.split('.', 1)
            else:
                schema_name = args.schema
                table_name = args.table
            
            # Perform table count comparison with exact counts
            comparison_df = compare_table_counts(
                source_engine, replication_engine, analytics_engine, 
                table_name, schema_name
            )
            
            # Log the comparison results
            logger.info(f"Table count comparison completed for '{args.table}'")
            logger.info(f"Comparison results: {comparison_df.to_dict('records')}")
            
            # Check for duplicates
            logger.info(f"\n🔍 Checking for duplicates in {table_name}")
            check_duplicates_for_table(source_engine, replication_engine, analytics_engine, table_name, schema_name)
            
            # Get detailed tracking information for this table
            logger.info(f"\n🔍 Getting tracking information for {table_name}")
            tracking_details = get_tracking_details_for_table(source_engine, replication_engine, analytics_engine, table_name)
            logger.info(f"Tracking information completed for '{table_name}'")
            
            return
        
        # Interactive mode (only if explicitly requested)
        if args.interactive:
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
        
        # If no specific table was provided and not in interactive mode, compare all tables
        elif not args.table:
            logger.info(f"\n🔍 Comparing all tables across all databases")
            compare_all_tables(source_engine, replication_engine, analytics_engine, source_df, replication_df, analytics_df)
        
        logger.info(f"Database export completed - {len(source_df)} source, {len(replication_df)} replication, {len(analytics_df)} analytics tables")
        
    except Exception as e:
        logger.error(f"Error during database comparison: {e}")
        raise


if __name__ == "__main__":
    main() 