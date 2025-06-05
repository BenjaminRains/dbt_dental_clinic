"""
Main ETL pipeline implementation.
Handles the extraction and loading of data from OpenDental to analytics.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from typing import Dict, List, Optional
from etl_pipeline.core.connections import ConnectionFactory
from dotenv import load_dotenv
import re
from mysql_replicator import ExactMySQLReplicator
from etl_pipeline.core.metrics import MetricsCollector
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("elt_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ELTPipeline:
    def __init__(self):
        """Initialize the ETL pipeline."""
        # Load environment variables
        self.source_db = os.getenv('SOURCE_MYSQL_DB')
        self.replication_db = os.getenv('REPLICATION_MYSQL_DB')
        self.analytics_db = os.getenv('ANALYTICS_POSTGRES_DB')
        self.analytics_schema = os.getenv('ANALYTICS_POSTGRES_SCHEMA', 'raw')
        
        # Validate required environment variables
        missing_dbs = []
        if not self.source_db:
            missing_dbs.append('SOURCE_MYSQL_DB')
        if not self.replication_db:
            missing_dbs.append('REPLICATION_MYSQL_DB')
        if not self.analytics_db:
            missing_dbs.append('ANALYTICS_POSTGRES_DB')
        
        if missing_dbs:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_dbs)}")
        
        # Initialize connections
        self.source_engine = None
        self.replication_engine = None
        self.analytics_engine = None
        
        # Initialize metrics
        self.metrics = MetricsCollector()
        
        # Initialize tracking tables
        self.ensure_tracking_tables()
    
    def validate_database_names(self):
        """Validate that all required database names are set."""
        missing_dbs = []
        
        if not self.source_db:
            missing_dbs.append('SOURCE_MYSQL_DB')
        if not self.replication_db:
            missing_dbs.append('REPLICATION_MYSQL_DB')
        if not self.analytics_db:
            missing_dbs.append('ANALYTICS_POSTGRES_DB')
        
        if missing_dbs:
            error_msg = f"Missing required database names: {', '.join(missing_dbs)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Source database: {self.source_db}")
        logger.info(f"Replication database: {self.replication_db}")
        logger.info(f"Analytics database: {self.analytics_db}")
        logger.info(f"PostgreSQL schema: {self.analytics_schema}")
    
    def initialize_connections(self):
        """Initialize database connections with proper error handling."""
        try:
            self.source_engine = ConnectionFactory.get_source_connection()
            self.replication_engine = ConnectionFactory.get_replication_connection()
            self.analytics_engine = ConnectionFactory.get_analytics_connection()
            
            # Initialize the MySQL replicator
            self.mysql_replicator = ExactMySQLReplicator(
                source_engine=self.source_engine,
                target_engine=self.replication_engine,
                source_db=self.source_db,
                target_db=self.replication_db
            )
            
            logger.info("Successfully initialized all database connections")
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {str(e)}")
            self.cleanup()
            raise
    
    def cleanup(self):
        """Clean up database connections."""
        try:
            if self.source_engine:
                self.source_engine.dispose()
                self.source_engine = None
                logger.info("Closed source database connection")
            
            if self.replication_engine:
                self.replication_engine.dispose()
                self.replication_engine = None
                logger.info("Closed replication database connection")
            
            if self.analytics_engine:
                self.analytics_engine.dispose()
                self.analytics_engine = None
                logger.info("Closed analytics database connection")
        except Exception as e:
            logger.error(f"Error during connection cleanup: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are cleaned up."""
        self.cleanup()
    
    def ensure_tracking_tables(self):
        """Create necessary tracking tables in replication and analytics databases."""
        try:
            # Create extract tracking table in replication MySQL
            with self.replication_engine.begin() as conn:
                conn.execute(text(f"USE {self.replication_db}"))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_extract_status (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_extracted TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                        rows_extracted INTEGER DEFAULT 0,
                        extraction_status VARCHAR(50) DEFAULT 'pending',
                        schema_hash VARCHAR(32),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """))
            
            # Create load tracking table in PostgreSQL
            with self.analytics_engine.begin() as conn:
                # Ensure schema exists
                conn.execute(text(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.analytics_schema}
                """))
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.analytics_schema}.etl_load_status (
                        id SERIAL PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_loaded TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                        rows_loaded INTEGER DEFAULT 0,
                        load_status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
            
            logger.info("Tracking tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tracking tables: {str(e)}")
            raise
    
    def extract_to_replication(self, table_name: str, force_full: bool = False) -> bool:
        """
        EXTRACT phase using exact MySQL replication.
        This performs a pure extract-load without any transformations.
        """
        try:
            # Get last extraction info
            with self.replication_engine.connect() as conn:
                conn.execute(text(f"USE {self.replication_db}"))
                
                query = text("""
                    SELECT last_extracted, schema_hash 
                    FROM etl_extract_status 
                    WHERE table_name = :table_name
                """)
                result = conn.execute(query.bindparams(table_name=table_name))
                row = result.fetchone()
                
                last_extracted = row[0] if row else None
                stored_schema_hash = row[1] if row and len(row) > 1 else None
            
            # Check if schema has changed
            current_schema_hash = self.mysql_replicator.get_schema_hash(table_name)
            schema_changed = stored_schema_hash != current_schema_hash
            
            if schema_changed:
                logger.info(f"Schema change detected for {table_name}, forcing full extraction")
                force_full = True
            
            # Perform extraction
            rows_extracted = self.mysql_replicator.extract_table_data(
                table_name=table_name,
                last_extracted=last_extracted,
                force_full=force_full
            )
            
            # Verify extraction
            if not self.mysql_replicator.verify_extraction(table_name):
                logger.error(f"Extraction verification failed for {table_name}")
                return False
            
            # Update extraction status
            with self.replication_engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl_extract_status 
                    (table_name, last_extracted, rows_extracted, extraction_status, schema_hash)
                    VALUES (:table_name, :now, :rows, 'success', :schema_hash)
                    ON DUPLICATE KEY UPDATE 
                        last_extracted = :now,
                        rows_extracted = :rows,
                        extraction_status = 'success',
                        schema_hash = :schema_hash,
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=rows_extracted,
                    schema_hash=current_schema_hash
                ))
            
            logger.info(f"Successfully extracted {rows_extracted} rows for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in extract phase for {table_name}: {str(e)}")
            return False
    
    def load_to_analytics(self, table_name: str) -> bool:
        """
        Load data from replication MySQL to PostgreSQL analytics.
        Applies basic type conversions for PostgreSQL compatibility.
        """
        try:
            # Read from replication MySQL
            with self.replication_engine.connect() as conn:
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            
            if df.empty:
                logger.info(f"No data to load for {table_name}")
                return True
            
            # Apply basic PostgreSQL compatibility conversions
            df_clean = self.apply_basic_postgres_conversions(df, table_name)
            
            # Load to PostgreSQL analytics
            with self.analytics_engine.begin() as conn:
                df_clean.to_sql(
                    table_name,  # Clean name: raw.patient, raw.appointment
                    conn,
                    schema=self.analytics_schema,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
            
            # Update load status using PostgreSQL syntax
            with self.analytics_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.analytics_schema}.etl_load_status 
                    (table_name, last_loaded, rows_loaded, load_status)
                    VALUES (:table_name, :now, :rows, 'success')
                    ON CONFLICT (table_name) DO UPDATE SET
                        last_loaded = EXCLUDED.last_loaded,
                        rows_loaded = EXCLUDED.rows_loaded,
                        load_status = EXCLUDED.load_status,
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=len(df_clean)
                ))
            
            logger.info(f"Successfully loaded {len(df_clean)} rows to analytics for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in load phase for {table_name}: {str(e)}")
            return False
    
    def apply_basic_postgres_conversions(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Apply basic type conversions for PostgreSQL compatibility.
        Only handles technical transformations, no business logic.
        """
        try:
            df_clean = df.copy()
            
            # Convert MySQL zero dates to NULL
            for col in df_clean.columns:
                if df_clean[col].dtype == 'datetime64[ns]':
                    df_clean[col] = df_clean[col].replace(pd.Timestamp('1969-12-31 19:00:00'), pd.NaT)
            
            # Convert boolean columns
            for col in df_clean.columns:
                if df_clean[col].dtype == 'int64' and df_clean[col].isin([0, 1]).all():
                    df_clean[col] = df_clean[col].astype('bool')
            
            # Convert text columns to string
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    df_clean[col] = df_clean[col].astype('string')
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error applying PostgreSQL conversions for {table_name}: {str(e)}")
            raise
    
    def run_elt_pipeline(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ETL pipeline for a table.
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full extraction
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Initialize connections if needed
            if not all([self.source_engine, self.replication_engine, self.analytics_engine]):
                self.initialize_connections()
            
            # Extract to replication
            if not self.extract_to_replication(table_name, force_full):
                logger.error(f"Extraction failed for {table_name}")
                return False
            
            # Load to analytics
            if not self.load_to_analytics(table_name):
                logger.error(f"Loading failed for {table_name}")
                return False
            
            logger.info(f"Successfully completed ETL pipeline for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in ETL pipeline for {table_name}: {str(e)}")
            return False
        finally:
            self.cleanup()

def main():
    """Main entry point for the ETL pipeline."""
    try:
        pipeline = ELTPipeline()
        pipeline.validate_database_names()
        
        # Example usage
        tables = ['patient', 'appointment', 'treatment']
        for table in tables:
            pipeline.run_elt_pipeline(table)
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()