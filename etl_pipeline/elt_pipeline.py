import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from typing import Dict, List, Optional
from connection_factory import get_source_connection, get_staging_connection, get_target_connection
from dotenv import load_dotenv
import re

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
        """Initialize the ELT pipeline with all necessary connections."""
        self.source_engine = None
        self.target_mysql_engine = None  # Local MySQL target
        self.target_postgres_engine = None  # PostgreSQL for transformations
        
        # Database names and schemas - FIXED variable names
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        self.target_mysql_db = os.getenv('STAGING_MYSQL_DB')  # Fixed from 'STAGING_MYSQL_DATABASE'
        self.target_postgres_db = os.getenv('TARGET_POSTGRES_DB')  # Fixed from 'POSTGRES_DB'
        self.target_schema = os.getenv('TARGET_POSTGRES_SCHEMA', 'analytics')  # Fixed from 'POSTGRES_SCHEMA'
        
        # Transform file paths
        self.analysis_base_path = os.path.join(os.path.dirname(__file__), '..', 'analysis')
        
        # Validate database names
        self.validate_database_names()
        
        # Initialize connections
        self.initialize_connections()
        
        # Initialize tracking tables
        self.ensure_tracking_tables()
    
    def validate_database_names(self):
        """Validate that all required database names are set."""
        missing_dbs = []
        
        if not self.source_db:
            missing_dbs.append('OPENDENTAL_SOURCE_DB')
        if not self.target_mysql_db:
            missing_dbs.append('STAGING_MYSQL_DB')  # Fixed variable name
        if not self.target_postgres_db:
            missing_dbs.append('TARGET_POSTGRES_DB')  # Fixed variable name
        
        if missing_dbs:
            error_msg = f"Missing required database names: {', '.join(missing_dbs)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Source database: {self.source_db}")
        logger.info(f"Target MySQL database: {self.target_mysql_db}")
        logger.info(f"Target PostgreSQL database: {self.target_postgres_db}")
        logger.info(f"PostgreSQL schema: {self.target_schema}")
    
    def initialize_connections(self):
        """Initialize database connections with proper error handling."""
        try:
            self.source_engine = get_source_connection(readonly=True)
            self.target_mysql_engine = get_staging_connection()  # This is actually the target MySQL
            self.target_postgres_engine = get_target_connection()
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
            
            if self.target_mysql_engine:
                self.target_mysql_engine.dispose()
                self.target_mysql_engine = None
                logger.info("Closed target MySQL database connection")
            
            if self.target_postgres_engine:
                self.target_postgres_engine.dispose()
                self.target_postgres_engine = None
                logger.info("Closed target PostgreSQL database connection")
        except Exception as e:
            logger.error(f"Error during connection cleanup: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are cleaned up."""
        self.cleanup()
    
    def ensure_tracking_tables(self):
        """Create necessary tracking tables in target MySQL and PostgreSQL databases."""
        try:
            # Create extract tracking table in target MySQL
            with self.target_mysql_engine.begin() as conn:
                conn.execute(text(f"USE {self.target_mysql_db}"))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_extract_status (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_extracted TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                        rows_extracted INTEGER DEFAULT 0,
                        extraction_status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """))
            
            # Create transform tracking table in PostgreSQL
            with self.target_postgres_engine.begin() as conn:
                # Ensure schema exists
                conn.execute(text(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.target_schema}
                """))
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.target_schema}.etl_transform_status (
                        id SERIAL PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_transformed TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                        rows_transformed INTEGER DEFAULT 0,
                        transformation_status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
            
            logger.info("Tracking tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tracking tables: {str(e)}")
            raise
    
    def get_mysql_ddl_path(self, table_name: str) -> str:
        """Get the path to the MySQL DDL file for a table."""
        return os.path.join(self.analysis_base_path, table_name, f"{table_name}_ddl.sql")
    
    def get_postgres_transform_path(self, table_name: str) -> str:
        """Get the path to the PostgreSQL transformation file for a table."""
        return os.path.join(self.analysis_base_path, table_name, f"{table_name}_pg_ddl.sql")
    
    def read_file(self, file_path: str) -> str:
        """Read a file and return its contents."""
        try:
            with open(file_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}")
            return None
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return None
    
    def extract_to_target_mysql(self, table_name: str, force_full: bool = False) -> bool:
        """
        EXTRACT phase: Extract data from source MySQL to target MySQL.
        This performs a pure extract-load without any transformations.
        """
        try:
            # Get last extraction time
            with self.target_mysql_engine.connect() as conn:
                conn.execute(text(f"USE {self.target_mysql_db}"))
                
                query = text("""
                    SELECT last_extracted 
                    FROM etl_extract_status 
                    WHERE table_name = :table_name
                """)
                result = conn.execute(query.bindparams(table_name=table_name))
                last_extracted = result.scalar()
            
            # Build extraction query with incremental logic
            if force_full or not last_extracted:
                where_clause = "1=1"
                logger.info(f"Performing full extraction for {table_name}")
            else:
                # Use a generic incremental extraction based on any timestamp column
                where_clause = """
                    EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_schema = :db_name 
                        AND table_name = :table_name 
                        AND data_type IN ('datetime', 'timestamp')
                        AND column_name IN (
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = :db_name 
                            AND table_name = :table_name 
                            AND data_type IN ('datetime', 'timestamp')
                        )
                        AND table_name.column_name > :last_extracted
                    )
                """
                logger.info(f"Performing incremental extraction for {table_name} since {last_extracted}")
            
            # Create target table using MySQL DDL if it doesn't exist
            self.create_target_mysql_table(table_name)
            
            # Perform direct table copy using MySQL's INSERT ... SELECT
            with self.source_engine.connect() as source_conn, self.target_mysql_engine.connect() as target_conn:
                # Use the source database
                source_conn.execute(text(f"USE {self.source_db}"))
                target_conn.execute(text(f"USE {self.target_mysql_db}"))
                
                # Get column names from source table
                result = source_conn.execute(text(f"""
                    SELECT GROUP_CONCAT(column_name) 
                    FROM information_schema.columns 
                    WHERE table_schema = :db_name 
                    AND table_name = :table_name
                    ORDER BY ordinal_position
                """).bindparams(db_name=self.source_db, table_name=table_name))
                columns = result.scalar()
                
                if not columns:
                    raise ValueError(f"No columns found for table {table_name}")
                
                # Truncate target table
                target_conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                
                # Copy data directly using INSERT ... SELECT
                copy_query = f"""
                    INSERT INTO {self.target_mysql_db}.{table_name} ({columns})
                    SELECT {columns}
                    FROM {self.source_db}.{table_name}
                    WHERE {where_clause}
                """
                
                params = {
                    'db_name': self.source_db,
                    'table_name': table_name,
                    'last_extracted': last_extracted
                } if not force_full and last_extracted else {
                    'db_name': self.source_db,
                    'table_name': table_name
                }
                
                result = target_conn.execute(text(copy_query).bindparams(**params))
                rows_copied = result.rowcount
            
            # Update extraction status
            with self.target_mysql_engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl_extract_status 
                    (table_name, last_extracted, rows_extracted, extraction_status)
                    VALUES (:table_name, :now, :rows, 'success')
                    ON DUPLICATE KEY UPDATE 
                        last_extracted = :now,
                        rows_extracted = :rows,
                        extraction_status = 'success',
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=rows_copied
                ))
            
            logger.info(f"Successfully extracted {rows_copied} rows for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting {table_name}: {str(e)}")
            return False
    
    def create_target_mysql_table(self, table_name: str) -> bool:
        """
        Create target MySQL table using the MySQL DDL file.
        This preserves the exact MySQL architecture.
        """
        try:
            mysql_ddl_path = self.get_mysql_ddl_path(table_name)
            mysql_ddl = self.read_file(mysql_ddl_path)
            
            if not mysql_ddl:
                logger.error(f"Could not read MySQL DDL for {table_name}")
                return False
            
            # Execute the MySQL DDL (it should create the table exactly as in source)
            with self.target_mysql_engine.begin() as conn:
                conn.execute(text(f"USE {self.target_mysql_db}"))
                
                # Drop table if exists and recreate
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.execute(text(mysql_ddl))
            
            logger.info(f"Created target MySQL table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating target MySQL table {table_name}: {str(e)}")
            return False
    
    def load_to_postgres(self, table_name: str) -> bool:
        """
        LOAD phase: Load data from target MySQL to PostgreSQL.
        This is a simple data movement with basic type conversion.
        """
        try:
            # Read data from target MySQL
            with self.target_mysql_engine.connect() as conn:
                conn.execute(text(f"USE {self.target_mysql_db}"))
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            
            if df.empty:
                logger.info(f"No data to load for {table_name}")
                return True
            
            # Apply basic type conversions for PostgreSQL compatibility
            df_clean = self.apply_basic_postgres_conversions(df, table_name)
            
            # Create raw PostgreSQL table (simple structure for data loading)
            self.create_postgres_raw_table(table_name, df_clean)
            
            # Load to PostgreSQL raw area
            with self.target_postgres_engine.begin() as conn:
                df_clean.to_sql(
                    f"{table_name}_raw",
                    conn,
                    schema=self.target_schema,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
            
            logger.info(f"Successfully loaded {len(df_clean)} rows to PostgreSQL for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading {table_name} to PostgreSQL: {str(e)}")
            return False
    
    def transform_in_postgres(self, table_name: str) -> bool:
        """
        TRANSFORM phase: Apply business logic transformations using PostgreSQL DDL files.
        This uses the transformation SQL from the analysis/{table_name}_pg_ddl.sql files.
        """
        try:
            # Read the PostgreSQL transformation file
            pg_transform_path = self.get_postgres_transform_path(table_name)
            pg_transform_sql = self.read_file(pg_transform_path)
            
            if not pg_transform_sql:
                logger.error(f"Could not read PostgreSQL transformation file for {table_name}")
                return False
            
            # Extract the transformation logic from the DDL file
            # The DDL file should contain the final table structure and any views/functions
            transformation_sql = self.extract_transformation_logic(pg_transform_sql, table_name)
            
            # Execute the transformation in PostgreSQL
            with self.target_postgres_engine.begin() as conn:
                # Drop existing transformed table if it exists
                conn.execute(text(f"""
                    DROP TABLE IF EXISTS {self.target_schema}.{table_name} CASCADE
                """))
                
                # Execute the PostgreSQL DDL/transformation
                conn.execute(text(pg_transform_sql))
                
                # If there's custom transformation logic, execute it
                if transformation_sql:
                    conn.execute(text(transformation_sql))
                
                # Get row count for tracking
                try:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM {self.target_schema}.{table_name}
                    """))
                    row_count = result.scalar()
                except:
                    # If the table doesn't exist or is a view, set row count to 0
                    row_count = 0
            
            # Update transform status
            with self.target_postgres_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.target_schema}.etl_transform_status 
                    (table_name, last_transformed, rows_transformed, transformation_status)
                    VALUES (:table_name, :now, :rows, 'success')
                    ON CONFLICT (table_name) 
                    DO UPDATE SET 
                        last_transformed = :now,
                        rows_transformed = :rows,
                        transformation_status = 'success',
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=row_count
                ))
            
            logger.info(f"Successfully transformed {row_count} rows for {self.target_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error transforming {table_name}: {str(e)}")
            
            # Update transform status with error
            try:
                with self.target_postgres_engine.begin() as conn:
                    conn.execute(text(f"""
                        INSERT INTO {self.target_schema}.etl_transform_status 
                        (table_name, transformation_status)
                        VALUES (:table_name, 'error')
                        ON CONFLICT (table_name) 
                        DO UPDATE SET 
                            transformation_status = 'error',
                            updated_at = CURRENT_TIMESTAMP
                    """).bindparams(table_name=table_name))
            except Exception as status_error:
                logger.error(f"Error updating transform status: {str(status_error)}")
            
            return False
    
    def extract_transformation_logic(self, pg_ddl_content: str, table_name: str) -> str:
        """
        Extract any custom transformation logic from the PostgreSQL DDL file.
        This method can be extended to parse specific transformation patterns.
        """
        # Look for any INSERT or SELECT statements that might be transformation logic
        transformation_patterns = [
            r'INSERT INTO.*?SELECT.*?FROM.*?;',
            r'CREATE OR REPLACE VIEW.*?AS\s+SELECT.*?;',
            r'-- TRANSFORM:.*?;'
        ]
        
        transformations = []
        for pattern in transformation_patterns:
            matches = re.findall(pattern, pg_ddl_content, re.DOTALL | re.IGNORECASE)
            transformations.extend(matches)
        
        if transformations:
            return '\n'.join(transformations)
        
        # If no explicit transformations found, create a simple INSERT from raw table
        return f"""
            INSERT INTO {self.target_schema}.{table_name}
            SELECT * FROM {self.target_schema}.{table_name}_raw;
        """
    
    def run_elt_pipeline(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ELT pipeline for a table with proper separation of concerns.
        
        E - Extract from source MySQL to target MySQL (preserving architecture)
        L - Load from target MySQL to PostgreSQL raw area
        T - Transform using PostgreSQL DDL files and SQL
        """
        logger.info(f"Starting ELT pipeline for {table_name}")
        
        # EXTRACT phase
        logger.info(f"[EXTRACT] Processing {table_name}")
        if not self.extract_to_target_mysql(table_name, force_full):
            logger.error(f"Extract phase failed for {table_name}")
            return False
        
        # LOAD phase
        logger.info(f"[LOAD] Processing {table_name}")
        if not self.load_to_postgres(table_name):
            logger.error(f"Load phase failed for {table_name}")
            return False
        
        # TRANSFORM phase
        logger.info(f"[TRANSFORM] Processing {table_name}")
        if not self.transform_in_postgres(table_name):
            logger.error(f"Transform phase failed for {table_name}")
            return False
        
        logger.info(f"Successfully completed ELT pipeline for {table_name}")
        return True
    
    # Helper methods
    
    def apply_basic_postgres_conversions(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Apply basic type conversions for PostgreSQL compatibility.
        Only essential conversions - no business logic.
        """
        try:
            df = df.copy()
            
            for col in df.columns:
                # Handle OpenDental's zero dates
                if df[col].dtype == 'object':
                    # Check if this might be a date column
                    if any(str(val).startswith(('0000-00-00', '0001-01-01')) for val in df[col].dropna().head() if val):
                        zero_date_patterns = ['0000-00-00', '0000-00-00 00:00:00', '0001-01-01', '0001-01-01 00:00:00']
                        for pattern in zero_date_patterns:
                            df.loc[df[col] == pattern, col] = None
                        
                        # Try to convert to datetime
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Handle text fields - replace empty strings with None for PostgreSQL
                if df[col].dtype == 'object':
                    df[col] = df[col].replace(['', 'nan', 'None'], None)
            
            return df
            
        except Exception as e:
            logger.error(f"Error applying basic conversions to {table_name}: {str(e)}")
            return df
    
    def create_postgres_raw_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        Create a simple raw table in PostgreSQL for data loading.
        This is just for initial data loading, not the final structure.
        """
        try:
            with self.target_postgres_engine.begin() as conn:
                # Let pandas infer the structure for the raw table
                df.head(0).to_sql(
                    f"{table_name}_raw",
                    conn,
                    schema=self.target_schema,
                    if_exists='replace',
                    index=False
                )
            
            logger.info(f"Created PostgreSQL raw table for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating PostgreSQL raw table for {table_name}: {str(e)}")
            return False


def main():
    """Main entry point for the ELT pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ELT Pipeline for OpenDental Data')
    parser.add_argument('--tables', nargs='+', help='Specific tables to process')
    parser.add_argument('--full-sync', action='store_true', help='Force full sync for all tables')
    parser.add_argument('--extract-only', action='store_true', help='Run only extract phase')
    parser.add_argument('--load-only', action='store_true', help='Run only load phase')
    parser.add_argument('--transform-only', action='store_true', help='Run only transform phase')
    args = parser.parse_args()
    
    # Use context manager to ensure proper cleanup
    with ELTPipeline() as pipeline:
        try:
            # Get list of tables to process
            if args.tables:
                tables_to_process = args.tables
            else:
                # Get all tables from source
                with pipeline.source_engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = :db_name
                        AND table_type = 'BASE TABLE'
                    """).bindparams(db_name=os.getenv('OPENDENTAL_SOURCE_DB')))
                    tables_to_process = [row[0] for row in result]
            
            # Process each table
            results = {'success': 0, 'failed': 0}
            
            for table in tables_to_process:
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing table: {table}")
                logger.info(f"{'='*50}")
                
                success = False
                
                # Run specific phases based on arguments
                if args.extract_only:
                    success = pipeline.extract_to_target_mysql(table, args.full_sync)
                elif args.load_only:
                    success = pipeline.load_to_postgres(table)
                elif args.transform_only:
                    success = pipeline.transform_in_postgres(table)
                else:
                    # Run full ELT pipeline
                    success = pipeline.run_elt_pipeline(table, args.full_sync)
                
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
            
            # Print summary
            logger.info(f"\n{'='*50}")
            logger.info("PIPELINE SUMMARY")
            logger.info(f"{'='*50}")
            logger.info(f"Tables processed: {len(tables_to_process)}")
            logger.info(f"Successful: {results['success']}")
            logger.info(f"Failed: {results['failed']}")
            logger.info(f"Success rate: {results['success']/len(tables_to_process)*100:.1f}%")
            
            return results['failed'] == 0
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)