#!/usr/bin/env python3
"""
Initialize tracking tables for both MySQL replication and PostgreSQL loading.

This script uses the ETL pipeline infrastructure (Settings and connections)
to create missing tracking records in both the MySQL replication database
and the PostgreSQL analytics database.

Usage:
    python scripts/initialize_etl_tracking_tables.py
"""

import sys
import os
import yaml
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

# Add the etl_pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.config import get_settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config.settings import DatabaseType, PostgresSchema

logger = get_logger(__name__)

def load_tables_config():
    """Load the tables configuration file."""
    try:
        config_path = Path(__file__).parent.parent / "etl_pipeline" / "config" / "tables.yml"
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading tables config: {str(e)}")
        return {}

def get_tables_with_incremental_columns(config):
    """Get tables that have incremental_columns defined."""
    tables_with_incremental = []
    if 'tables' in config:
        for table_name, table_config in config['tables'].items():
            if 'incremental_columns' in table_config and table_config['incremental_columns']:
                tables_with_incremental.append(table_name)
    return tables_with_incremental

def create_mysql_tracking_tables(replication_engine, replication_database):
    """Create MySQL tracking table structure if it doesn't exist with primary column support."""
    try:
        with replication_engine.connect() as conn:
            # Create enhanced etl_copy_status table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS etl_copy_status (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_copied TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:01',
                last_primary_value VARCHAR(255) NULL,
                primary_column_name VARCHAR(255) NULL,
                rows_copied INT DEFAULT 0,
                copy_status VARCHAR(50) DEFAULT 'pending',
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_table_sql))
            
            # Create indexes with proper MySQL syntax and error handling
            try:
                conn.execute(text("""
                CREATE INDEX idx_etl_copy_status_table_name 
                ON etl_copy_status(table_name);
                """))
                logger.debug("Created index idx_etl_copy_status_table_name")
            except Exception as e:
                if "Duplicate key name" in str(e) or "already exists" in str(e):
                    logger.debug("Index idx_etl_copy_status_table_name already exists")
                else:
                    logger.warning(f"Could not create index idx_etl_copy_status_table_name: {str(e)}")
            
            try:
                conn.execute(text("""
                CREATE INDEX idx_etl_copy_status_last_copied 
                ON etl_copy_status(last_copied);
                """))
                logger.debug("Created index idx_etl_copy_status_last_copied")
            except Exception as e:
                if "Duplicate key name" in str(e) or "already exists" in str(e):
                    logger.debug("Index idx_etl_copy_status_last_copied already exists")
                else:
                    logger.warning(f"Could not create index idx_etl_copy_status_last_copied: {str(e)}")
            
            try:
                conn.execute(text("""
                CREATE INDEX idx_etl_copy_status_primary_value 
                ON etl_copy_status(last_primary_value);
                """))
                logger.debug("Created index idx_etl_copy_status_primary_value")
            except Exception as e:
                if "Duplicate key name" in str(e) or "already exists" in str(e):
                    logger.debug("Index idx_etl_copy_status_primary_value already exists")
                else:
                    logger.warning(f"Could not create index idx_etl_copy_status_primary_value: {str(e)}")
            
            conn.commit()
            logger.info(f"MySQL tracking tables created/verified successfully in {replication_database} with primary column support")
            return True
            
    except Exception as e:
        logger.error(f"Error creating MySQL tracking tables: {str(e)}")
        return False

def create_postgresql_tracking_tables(analytics_engine, analytics_schema):
    """Create PostgreSQL tracking table structures with primary column support."""
    try:
        with analytics_engine.connect() as conn:
            # Check if tables exist first
            result = conn.execute(text(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{analytics_schema}' 
                AND table_name IN ('etl_load_status', 'etl_transform_status')
            """))
            existing_tables = [row[0] for row in result.fetchall()]
            
            if existing_tables:
                logger.info(f"Found existing tracking tables in {analytics_schema}: {existing_tables}")
                logger.info("Tables already exist - skipping creation")
                return True
            
            logger.info(f"Creating tracking tables in {analytics_schema} with primary column support")
            
            # Create enhanced etl_load_status table with primary column support
            create_load_status_sql = f"""
            CREATE TABLE {analytics_schema}.etl_load_status (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_loaded TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:01',
                last_primary_value VARCHAR(255) NULL,
                primary_column_name VARCHAR(255) NULL,
                rows_loaded INTEGER DEFAULT 0,
                load_status VARCHAR(50) DEFAULT 'pending',
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(text(create_load_status_sql))
            
            # Create enhanced etl_transform_status table with primary column support
            create_transform_status_sql = f"""
            CREATE TABLE {analytics_schema}.etl_transform_status (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_transformed TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:01',
                last_primary_value VARCHAR(255) NULL,
                primary_column_name VARCHAR(255) NULL,
                rows_transformed INTEGER DEFAULT 0,
                transform_status VARCHAR(50) DEFAULT 'pending',
                _transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(text(create_transform_status_sql))
            
            # Create indexes in the same transaction to avoid isolation issues
            logger.info("Creating indexes for tracking tables")
            
            # Indexes for etl_load_status
            conn.execute(text(f"""
                CREATE INDEX idx_etl_load_status_table_name 
                ON {analytics_schema}.etl_load_status (table_name)
            """))
            
            conn.execute(text(f"""
                CREATE INDEX idx_etl_load_status_primary_value 
                ON {analytics_schema}.etl_load_status (last_primary_value)
            """))
            
            conn.execute(text(f"""
                CREATE INDEX idx_etl_load_status_load_status 
                ON {analytics_schema}.etl_load_status (load_status)
            """))
            
            # Indexes for etl_transform_status
            conn.execute(text(f"""
                CREATE INDEX idx_etl_transform_status_table_name 
                ON {analytics_schema}.etl_transform_status (table_name)
            """))
            
            conn.execute(text(f"""
                CREATE INDEX idx_etl_transform_status_primary_value 
                ON {analytics_schema}.etl_transform_status (last_primary_value)
            """))
            
            conn.execute(text(f"""
                CREATE INDEX idx_etl_transform_status_transform_status 
                ON {analytics_schema}.etl_transform_status (transform_status)
            """))
            
            # Commit the transaction
            conn.commit()
            logger.info(f"Successfully created {analytics_schema}.etl_load_status and {analytics_schema}.etl_transform_status with primary column support")
            return True
            
    except Exception as e:
        logger.error(f"Error creating PostgreSQL tracking tables: {e}")
        return False

def create_mysql_tracking_records(replication_engine, tables_config):
    """Create initial tracking records for all tables in MySQL replication database."""
    try:
        # Get replication database info from settings
        settings = get_settings()
        replication_config = settings.get_database_config(DatabaseType.REPLICATION)
        replication_database = replication_config.get('database', 'opendental_replication')
        
        logger.info(f"Connecting to MySQL replication database: {replication_database}")
        
        # First, ensure tracking tables exist
        if not create_mysql_tracking_tables(replication_engine, replication_database):
            logger.error("Failed to create MySQL tracking tables")
            return False
        
        with replication_engine.connect() as conn:
            for table_name in tables_config['tables']:
                # Skip temporary/backup tables
                if table_name.startswith('temp') and 'bak' in table_name:
                    logger.info(f"Skipping temporary/backup table: {table_name}")
                    continue
                    
                # Check if record already exists
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM etl_copy_status 
                    WHERE table_name = :table_name
                """), {"table_name": table_name}).scalar()
                
                if result == 0:
                    conn.execute(text("""
                        INSERT INTO etl_copy_status (
                            table_name, last_copied, last_primary_value, primary_column_name,
                            rows_copied, copy_status, _created_at, _updated_at
                        ) VALUES (
                            :table_name, '1970-01-01 00:00:01', NULL, NULL,
                            0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                    """), {"table_name": table_name})
                    logger.info(f"Created tracking record for {table_name}")
                else:
                    logger.info(f"Tracking record already exists for {table_name}")
                    
            # Commit the transaction BEFORE the connection closes
            conn.commit()
            logger.info("Successfully committed MySQL tracking records to database")
                    
        return True
                    
    except Exception as e:
        logger.error(f"Error creating MySQL tracking records: {e}")
        raise

def create_postgresql_tracking_records(tables_with_incremental):
    """Create missing tracking records in the PostgreSQL analytics database."""
    
    analytics_engine = None
    try:
        # Get settings and connections using ETL pipeline infrastructure
        settings = get_settings()
        analytics_engine = ConnectionFactory.get_analytics_connection(settings)
        
        # Get analytics database info from settings using proper enum
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        analytics_schema = analytics_config.get('schema', 'raw')
        
        logger.info(f"Connecting to PostgreSQL analytics database with schema: {analytics_schema}")
        
        # First, ensure tracking tables exist
        if not create_postgresql_tracking_tables(analytics_engine, analytics_schema):
            logger.error("Failed to create PostgreSQL tracking tables")
            return False
        
        # Check which tables need tracking records
        missing_tables = []
        with analytics_engine.connect() as conn:
            for table_name in tables_with_incremental:
                # Skip temporary/backup tables
                if table_name.startswith('temp') and 'bak' in table_name:
                    logger.info(f"Skipping temporary/backup table: {table_name}")
                    continue
                    
                # Check if tracking record exists
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {analytics_schema}.etl_load_status 
                    WHERE table_name = :table_name
                """), {"table_name": table_name}).scalar()
                
                if result == 0:
                    missing_tables.append(table_name)
        
        if not missing_tables:
            logger.info("All PostgreSQL tracking records already exist")
            return True
        
        logger.info(f"Found {len(missing_tables)} tables missing PostgreSQL tracking records: {missing_tables}")
        
        # Create tracking records for missing tables
        created_count = 0
        with analytics_engine.connect() as conn:
            for table_name in missing_tables:
                try:
                    conn.execute(text(f"""
                        INSERT INTO {analytics_schema}.etl_load_status (
                            table_name, last_loaded, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at, _created_at, _updated_at
                        ) VALUES (
                            :table_name, '1970-01-01 00:00:01', NULL, NULL,
                            0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                    """), {"table_name": table_name})
                    
                    logger.info(f"Created PostgreSQL tracking record for {table_name}")
                    created_count += 1
                    
                except Exception as e:
                    logger.error(f"Error creating PostgreSQL tracking record for {table_name}: {str(e)}")
            
            # Commit the transaction BEFORE the connection closes
            conn.commit()
            logger.info(f"Successfully created {created_count} PostgreSQL tracking records")
        
        # Verify the created records
        logger.info("Verification - Created PostgreSQL tracking records:")
        with analytics_engine.connect() as conn:
            for table_name in missing_tables[:5]:  # Show first 5 for verification
                result = conn.execute(text(f"""
                    SELECT table_name, last_loaded, rows_loaded, load_status, _created_at
                    FROM {analytics_schema}.etl_load_status 
                    WHERE table_name = :table_name
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    logger.info(f"  {result.table_name}: {result.load_status} status, {result.rows_loaded} rows, created {result._created_at}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating PostgreSQL tracking records: {str(e)}")
        return False
    finally:
        # Properly dispose of the engine to clean up connection pools
        if analytics_engine:
            analytics_engine.dispose()
            logger.debug("PostgreSQL analytics engine disposed")

def main():
    """Main function to initialize tracking tables and records."""
    logger.info("Starting tracking records initialization...")
    
    # Load configuration
    config = load_tables_config()
    if not config:
        logger.error("Failed to load tables configuration")
        return False
    
    # Get tables with incremental columns
    tables_with_incremental = get_tables_with_incremental_columns(config)
    logger.info(f"Found {len(tables_with_incremental)} tables with incremental columns")
    
    # Initialize MySQL replication tracking
    logger.info("=== Initializing MySQL Replication Tracking Records ===")
    # Get settings and connections using ETL pipeline infrastructure
    settings = get_settings()
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    mysql_success = create_mysql_tracking_records(replication_engine, config)
    
    # Initialize PostgreSQL analytics tracking
    logger.info("=== Initializing PostgreSQL Analytics Tracking Records ===")
    postgresql_success = create_postgresql_tracking_records(tables_with_incremental)
    
    # Summary
    if mysql_success and postgresql_success:
        logger.info("=== Tracking Records Initialization Completed Successfully ===")
        return True
    else:
        logger.error("=== Tracking Records Initialization Failed ===")
        if not mysql_success:
            logger.error("MySQL replication tracking initialization failed")
        if not postgresql_success:
            logger.error("PostgreSQL analytics tracking initialization failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 