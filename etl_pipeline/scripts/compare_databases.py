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
        0 as row_count,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size_pretty,
        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
    FROM pg_tables 
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


def main():
    """Main analysis function."""
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
        
        # Create simple output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("database_data") / f"export_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save raw database data to CSV files
        source_df.to_csv(output_dir / "opendental_tables.csv", index=False)
        replication_df.to_csv(output_dir / "opendental_replication_tables.csv", index=False)
        analytics_df.to_csv(output_dir / "opendental_analytics_tables.csv", index=False)
        
        # Simple summary
        print(f"\n📊 Database Export Complete")
        print(f"Source (opendental): {len(source_df)} tables")
        print(f"Replication (opendental_replication): {len(replication_df)} tables")
        print(f"Analytics (opendental_analytics): {len(analytics_df)} tables")
        print(f"📁 Data saved to: {output_dir}")
        print(f"📝 Log file: {log_file_path}")
        
        logger.info(f"Database export completed - {len(source_df)} source, {len(replication_df)} replication, {len(analytics_df)} analytics tables")
        
    except Exception as e:
        logger.error(f"Error during database comparison: {e}")
        raise


if __name__ == "__main__":
    main() 