#!/usr/bin/env python3
"""
Clean up old or incorrectly named ETL tracking tables.

This script identifies and removes any ETL tracking tables that don't match
the expected schema or naming conventions.

Expected Tracking Tables:
- MySQL Replication: etl_copy_status
- PostgreSQL Analytics: raw.etl_load_status, raw.etl_transform_status

Usage:
    python scripts/cleanup_old_tracking_tables.py
"""

import sys
import os
from pathlib import Path
from sqlalchemy import text, inspect

# Add the etl_pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.config import get_settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config.settings import DatabaseType, PostgresSchema

logger = get_logger(__name__)

def get_mysql_tracking_tables(replication_engine):
    """Get all tracking-related tables in MySQL replication database."""
    try:
        with replication_engine.connect() as conn:
            # Get all tables that might be tracking tables, excluding system tables and business data
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                AND table_type = 'BASE TABLE'
                AND (
                    table_name LIKE '%etl%'
                    OR table_name LIKE '%tracking%'
                    OR table_name LIKE '%status%'
                    OR table_name LIKE '%copy%'
                    OR table_name LIKE '%load%'
                    OR table_name LIKE '%transform%'
                )
                AND table_name NOT IN (
                    'replication_connection_status',
                    'replication_applier_status',
                    'replication_applier_status_by_coordinator',
                    'replication_applier_status_by_worker',
                    'log_status',
                    'status_by_account',
                    'status_by_host',
                    'status_by_thread',
                    'status_by_user',
                    'global_status',
                    'session_status',
                    'tls_channel_status',
                    'keyring_component_status',
                    'session_ssl_status',
                    'claimtracking',
                    'ehrlabresultscopyto'
                )
            """))
            
            tracking_tables = list(set([row[0] for row in result.fetchall()]))  # Remove duplicates
            logger.info(f"Found {len(tracking_tables)} potential tracking tables in MySQL: {tracking_tables}")
            return tracking_tables
            
    except Exception as e:
        logger.error(f"Error getting MySQL tracking tables: {str(e)}")
        return []

def get_postgresql_tracking_tables(analytics_engine, analytics_schema):
    """Get all tracking-related tables in PostgreSQL analytics database."""
    try:
        with analytics_engine.connect() as conn:
            # Get all tables that might be tracking tables, excluding system tables and business data
            result = conn.execute(text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{analytics_schema}'
                AND table_type = 'BASE TABLE'
                AND (
                    table_name LIKE '%etl%'
                    OR table_name LIKE '%tracking%'
                    OR table_name LIKE '%status%'
                    OR table_name LIKE '%copy%'
                    OR table_name LIKE '%load%'
                    OR table_name LIKE '%transform%'
                )
                AND table_name NOT LIKE 'pg_%'
                AND table_name NOT LIKE 'information_schema%'
                AND table_name NOT IN (
                    'claimtracking',
                    'ehrlabresultscopyto'
                )
            """))
            
            tracking_tables = list(set([row[0] for row in result.fetchall()]))  # Remove duplicates
            logger.info(f"Found {len(tracking_tables)} potential tracking tables in PostgreSQL {analytics_schema}: {tracking_tables}")
            return tracking_tables
            
    except Exception as e:
        logger.error(f"Error getting PostgreSQL tracking tables: {str(e)}")
        return []

def check_table_schema_mysql(replication_engine, table_name):
    """Check if a MySQL table has the expected tracking schema."""
    try:
        with replication_engine.connect() as conn:
            # First check if table exists
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = '{table_name}'
            """)).scalar()
            
            if result == 0:
                logger.warning(f"Table {table_name} does not exist - skipping")
                return False, "Table does not exist"
            
            # Get table structure
            result = conn.execute(text(f"""
                DESCRIBE {table_name}
            """))
            
            columns = [row[0] for row in result.fetchall()]
            logger.debug(f"Table {table_name} columns: {columns}")
            
            # Check if it has the expected columns for etl_copy_status
            expected_columns = ['id', 'table_name', 'last_copied', 'rows_copied', 'copy_status', '_created_at', '_updated_at']
            
            if table_name == 'etl_copy_status':
                # This is the correct table, check if schema is correct
                missing_columns = [col for col in expected_columns if col not in columns]
                if missing_columns:
                    logger.warning(f"Table {table_name} is missing expected columns: {missing_columns}")
                    return False, f"Missing columns: {missing_columns}"
                return True, "Correct schema"
            else:
                # This is an old or incorrectly named table
                return False, "Incorrect table name"
                
    except Exception as e:
        logger.error(f"Error checking MySQL table schema for {table_name}: {str(e)}")
        return False, f"Error: {str(e)}"

def check_table_schema_postgresql(analytics_engine, analytics_schema, table_name):
    """Check if a PostgreSQL table has the expected tracking schema."""
    try:
        with analytics_engine.connect() as conn:
            # First check if table exists
            result = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{analytics_schema}' 
                AND table_name = '{table_name}'
            """)).scalar()
            
            if result == 0:
                logger.warning(f"Table {analytics_schema}.{table_name} does not exist - skipping")
                return False, "Table does not exist"
            
            # Get table structure
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{analytics_schema}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """))
            
            columns = [row[0] for row in result.fetchall()]
            logger.debug(f"Table {analytics_schema}.{table_name} columns: {columns}")
            
            # Check if it has the expected columns for tracking tables
            if table_name == 'etl_load_status':
                expected_columns = ['id', 'table_name', 'rows_loaded', 'load_status', '_loaded_at', '_transformed_at']
                missing_columns = [col for col in expected_columns if col not in columns]
                if missing_columns:
                    logger.warning(f"Table {analytics_schema}.{table_name} is missing expected columns: {missing_columns}")
                    return False, f"Missing columns: {missing_columns}"
                return True, "Correct schema"
                
            elif table_name == 'etl_transform_status':
                expected_columns = ['id', 'table_name', 'last_transformed', 'rows_transformed', 'transformation_status', '_loaded_at', '_created_at', '_updated_at']
                missing_columns = [col for col in expected_columns if col not in columns]
                if missing_columns:
                    logger.warning(f"Table {analytics_schema}.{table_name} is missing expected columns: {missing_columns}")
                    return False, f"Missing columns: {missing_columns}"
                return True, "Correct schema"
                
            else:
                # This is an old or incorrectly named table
                return False, "Incorrect table name"
                
    except Exception as e:
        logger.error(f"Error checking PostgreSQL table schema for {analytics_schema}.{table_name}: {str(e)}")
        return False, f"Error: {str(e)}"

def drop_mysql_table(replication_engine, table_name):
    """Drop a MySQL table."""
    try:
        with replication_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
            logger.info(f"Dropped MySQL table: {table_name}")
            return True
    except Exception as e:
        logger.error(f"Error dropping MySQL table {table_name}: {str(e)}")
        return False

def drop_postgresql_table(analytics_engine, analytics_schema, table_name):
    """Drop a PostgreSQL table."""
    try:
        with analytics_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {analytics_schema}.{table_name} CASCADE"))
            conn.commit()
            logger.info(f"Dropped PostgreSQL table: {analytics_schema}.{table_name}")
            return True
    except Exception as e:
        logger.error(f"Error dropping PostgreSQL table {analytics_schema}.{table_name}: {str(e)}")
        return False

def analyze_mysql_tracking_tables():
    """Analyze MySQL tracking tables and return tables to be dropped."""
    logger.info("=== Analyzing MySQL Replication Tracking Tables ===")
    
    replication_engine = None
    try:
        settings = get_settings()
        replication_engine = ConnectionFactory.get_replication_connection(settings)
        
        # Get all potential tracking tables
        tracking_tables = get_mysql_tracking_tables(replication_engine)
        
        if not tracking_tables:
            logger.info("No potential tracking tables found in MySQL replication database")
            return []
        
        tables_to_drop = []
        tables_to_keep = []
        
        # Check each table
        for table_name in tracking_tables:
            is_correct, reason = check_table_schema_mysql(replication_engine, table_name)
            
            if is_correct:
                logger.info(f"Table {table_name} has correct schema - keeping")
                tables_to_keep.append(table_name)
            else:
                logger.warning(f"Table {table_name} has incorrect schema: {reason} - will drop")
                tables_to_drop.append(table_name)
        
        return tables_to_drop
        
    except Exception as e:
        logger.error(f"Error analyzing MySQL tracking tables: {str(e)}")
        return []
    finally:
        if replication_engine:
            replication_engine.dispose()

def analyze_postgresql_tracking_tables():
    """Analyze PostgreSQL tracking tables and return tables to be dropped."""
    logger.info("=== Analyzing PostgreSQL Analytics Tracking Tables ===")
    
    analytics_engine = None
    try:
        settings = get_settings()
        analytics_engine = ConnectionFactory.get_analytics_connection(settings)
        
        # Get analytics schema
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        analytics_schema = analytics_config.get('schema', 'raw')
        
        # Get all potential tracking tables
        tracking_tables = get_postgresql_tracking_tables(analytics_engine, analytics_schema)
        
        if not tracking_tables:
            logger.info(f"No potential tracking tables found in PostgreSQL {analytics_schema} schema")
            return []
        
        tables_to_drop = []
        tables_to_keep = []
        
        # Check each table
        for table_name in tracking_tables:
            is_correct, reason = check_table_schema_postgresql(analytics_engine, analytics_schema, table_name)
            
            if is_correct:
                logger.info(f"Table {analytics_schema}.{table_name} has correct schema - keeping")
                tables_to_keep.append(f"{analytics_schema}.{table_name}")
            else:
                logger.warning(f"Table {analytics_schema}.{table_name} has incorrect schema: {reason} - will drop")
                tables_to_drop.append(f"{analytics_schema}.{table_name}")
        
        return tables_to_drop
        
    except Exception as e:
        logger.error(f"Error analyzing PostgreSQL tracking tables: {str(e)}")
        return []
    finally:
        if analytics_engine:
            analytics_engine.dispose()

def cleanup_mysql_tracking_tables(tables_to_drop):
    """Clean up old or incorrect MySQL tracking tables."""
    logger.info("=== Cleaning up MySQL Replication Tracking Tables ===")
    
    if not tables_to_drop:
        logger.info("No incorrect MySQL tracking tables to drop")
        return True
    
    replication_engine = None
    try:
        settings = get_settings()
        replication_engine = ConnectionFactory.get_replication_connection(settings)
        
        # Drop incorrect tables
        logger.info(f"Dropping {len(tables_to_drop)} incorrect MySQL tracking tables: {tables_to_drop}")
        for table_name in tables_to_drop:
            drop_mysql_table(replication_engine, table_name)
        
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up MySQL tracking tables: {str(e)}")
        return False
    finally:
        if replication_engine:
            replication_engine.dispose()

def cleanup_postgresql_tracking_tables(tables_to_drop):
    """Clean up old or incorrect PostgreSQL tracking tables."""
    logger.info("=== Cleaning up PostgreSQL Analytics Tracking Tables ===")
    
    if not tables_to_drop:
        logger.info("No incorrect PostgreSQL tracking tables to drop")
        return True
    
    analytics_engine = None
    try:
        settings = get_settings()
        analytics_engine = ConnectionFactory.get_analytics_connection(settings)
        
        # Drop incorrect tables
        logger.info(f"Dropping {len(tables_to_drop)} incorrect PostgreSQL tracking tables: {tables_to_drop}")
        for table_name in tables_to_drop:
            # Extract schema and table name from full name
            schema, table = table_name.split('.', 1)
            drop_postgresql_table(analytics_engine, schema, table)
        
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up PostgreSQL tracking tables: {str(e)}")
        return False
    finally:
        if analytics_engine:
            analytics_engine.dispose()

def get_user_confirmation(tables_to_drop):
    """Get user confirmation before dropping tables."""
    if not tables_to_drop:
        print("\n✅ No tables need to be dropped. All tracking tables have correct schemas.")
        return True
    
    print("\n" + "="*80)
    print("🗑️  TABLES TO BE DROPPED")
    print("="*80)
    
    for i, table_name in enumerate(tables_to_drop, 1):
        print(f"{i:2d}. {table_name}")
    
    print("\n" + "="*80)
    print("⚠️  WARNING: This action will permanently delete the tables listed above.")
    print("   Make sure you have backups if needed.")
    print("="*80)
    
    while True:
        response = input("\nDo you want to proceed with dropping these tables? (y/n): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter 'y' or 'n'.")

def main():
    """Main function to clean up old tracking tables."""
    logger.info("Starting ETL tracking tables cleanup...")
    
    # Analyze tables to be dropped
    mysql_tables_to_drop = analyze_mysql_tracking_tables()
    postgresql_tables_to_drop = analyze_postgresql_tracking_tables()
    
    # Combine all tables to be dropped
    all_tables_to_drop = mysql_tables_to_drop + postgresql_tables_to_drop
    
    # Get user confirmation
    if not get_user_confirmation(all_tables_to_drop):
        logger.info("Cleanup cancelled by user.")
        return True
    
    # Proceed with cleanup
    logger.info("Proceeding with cleanup...")
    
    # Clean up MySQL replication tracking tables
    mysql_success = cleanup_mysql_tracking_tables(mysql_tables_to_drop)
    
    # Clean up PostgreSQL analytics tracking tables
    postgresql_success = cleanup_postgresql_tracking_tables(postgresql_tables_to_drop)
    
    # Summary
    if mysql_success and postgresql_success:
        logger.info("=== ETL Tracking Tables Cleanup Completed Successfully ===")
        logger.info("Expected tracking tables:")
        logger.info("  MySQL Replication: etl_copy_status")
        logger.info("  PostgreSQL Analytics: raw.etl_load_status, raw.etl_transform_status")
        return True
    else:
        logger.error("=== ETL Tracking Tables Cleanup Failed ===")
        if not mysql_success:
            logger.error("MySQL replication tracking cleanup failed")
        if not postgresql_success:
            logger.error("PostgreSQL analytics tracking cleanup failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 