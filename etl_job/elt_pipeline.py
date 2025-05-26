import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from typing import Dict, List, Optional
from connection_factory import get_source_connection, get_target_connection
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
        self.target_engine = None
        
        # Database names and schemas
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        self.target_db = os.getenv('POSTGRES_DATABASE')
        self.target_schema = os.getenv('POSTGRES_SCHEMA', 'analytics')
        self.raw_schema = os.getenv('POSTGRES_RAW_SCHEMA', 'raw')
        
        # DDL file paths
        self.ddl_base_path = os.path.join(os.path.dirname(__file__), '..', 'analysis')
        
        # Validate database names
        self.validate_database_names()
        
        # Initialize connections
        self.initialize_connections()
        
        # Initialize tracking tables and schemas
        self.ensure_schemas_exist()
        self.ensure_tracking_tables()
    
    def validate_database_names(self):
        """Validate that all required database names are set."""
        missing_dbs = []
        
        if not self.source_db:
            missing_dbs.append('OPENDENTAL_SOURCE_DB')
        if not self.target_db:
            missing_dbs.append('POSTGRES_DATABASE')
        
        if missing_dbs:
            error_msg = f"Missing required database names: {', '.join(missing_dbs)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Source database: {self.source_db}")
        logger.info(f"Target database: {self.target_db}")
        logger.info(f"Analytics schema: {self.target_schema}")
        logger.info(f"Raw schema: {self.raw_schema}")
    
    def initialize_connections(self):
        """Initialize database connections with proper error handling."""
        try:
            self.source_engine = get_source_connection(readonly=True)
            self.target_engine = get_target_connection()
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
            
            if self.target_engine:
                self.target_engine.dispose()
                self.target_engine = None
                logger.info("Closed target database connection")
        except Exception as e:
            logger.error(f"Error during connection cleanup: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are cleaned up."""
        self.cleanup()
    
    def ensure_schemas_exist(self):
        """Ensure both analytics and raw schemas exist in PostgreSQL."""
        try:
            with self.target_engine.begin() as conn:
                # Create raw schema for raw data
                conn.execute(text(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.raw_schema}
                """))
                
                # Create analytics schema for transformed data
                conn.execute(text(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.target_schema}
                """))
                
                logger.info(f"Ensured schemas {self.raw_schema} and {self.target_schema} exist")
                return True
        except Exception as e:
            logger.error(f"Error ensuring schemas exist: {str(e)}")
            return False
    
    def ensure_tracking_tables(self):
        """Create necessary tracking tables in target database."""
        try:
            # Create target tracking tables (for Load and Transform phases)
            with self.target_engine.begin() as conn:
                # Load tracking (raw data)
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.target_schema}.etl_load_status (
                        id SERIAL PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_loaded TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                        rows_loaded INTEGER DEFAULT 0,
                        load_status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Transform tracking (analytics data)
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
    
    def get_ddl_path(self, table_name: str, is_postgres: bool = False) -> str:
        """Get the path to the DDL file for a table."""
        suffix = '_pg_ddl.sql' if is_postgres else '_ddl.sql'
        return os.path.join(self.ddl_base_path, table_name, f"{table_name}{suffix}")
    
    def read_ddl_file(self, table_name: str, is_postgres: bool = False) -> str:
        """Read the DDL file for a table."""
        try:
            ddl_path = self.get_ddl_path(table_name, is_postgres)
            with open(ddl_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            logger.warning(f"DDL file not found for {table_name} ({'PostgreSQL' if is_postgres else 'MySQL'})")
            return None
        except Exception as e:
            logger.error(f"Error reading DDL file for {table_name}: {str(e)}")
            return None
    
    def convert_mysql_to_pg_ddl(self, mysql_ddl: str) -> str:
        """Convert MySQL DDL to PostgreSQL DDL with proper type mapping."""
        if not mysql_ddl:
            return None
            
        # Type mappings for OpenDental
        type_mappings = {
            'bigint(20)': 'bigint',
            'tinyint(1)': 'boolean',
            'tinyint(3)': 'smallint',
            'tinyint(4)': 'smallint',
            'int(11)': 'integer',
            'varchar(255)': 'character varying(255)',
            'text': 'text',
            'datetime': 'timestamp without time zone',
            'timestamp': 'timestamp without time zone',
            'char(1)': 'character(1)',
            'decimal(10,2)': 'numeric(10,2)',
            'float': 'real',
            'double': 'double precision',
            'date': 'date',
            'time': 'time without time zone',
            'blob': 'bytea',
            'longblob': 'bytea',
            'mediumblob': 'bytea',
            'tinyblob': 'bytea',
            'enum': 'text',
            'set': 'text'
        }
        
        # Replace MySQL types with PostgreSQL types
        for mysql_type, pg_type in type_mappings.items():
            mysql_ddl = mysql_ddl.replace(mysql_type, pg_type)
        
        # Remove MySQL-specific syntax
        mysql_ddl = mysql_ddl.replace('ENGINE=MyISAM', '')
        mysql_ddl = mysql_ddl.replace('AUTO_INCREMENT', '')
        mysql_ddl = mysql_ddl.replace('COLLATE utf8mb3_uca1400_ai_ci', '')
        mysql_ddl = mysql_ddl.replace('COLLATE utf8mb3_general_ci', '')
        mysql_ddl = mysql_ddl.replace('COLLATE utf8mb4_general_ci', '')
        
        # Convert MySQL index syntax to PostgreSQL
        mysql_ddl = re.sub(r'KEY `([^`]+)` \(`([^`]+)`\)', r'INDEX \1 (\2)', mysql_ddl)
        
        return mysql_ddl
    
    def create_raw_table(self, table_name: str) -> bool:
        """Create raw table in PostgreSQL using the MySQL DDL as reference."""
        try:
            # Read the MySQL DDL file
            mysql_ddl = self.read_ddl_file(table_name, is_postgres=False)
            if not mysql_ddl:
                logger.error(f"Could not read MySQL DDL for {table_name}")
                return False
            
            # Convert MySQL DDL to PostgreSQL DDL
            pg_ddl = self.convert_mysql_to_pg_ddl(mysql_ddl)
            if not pg_ddl:
                logger.error(f"Could not convert MySQL DDL to PostgreSQL for {table_name}")
                return False
            
            # Extract the column definitions
            column_defs = re.search(r'CREATE TABLE.*?\((.*?)\)', pg_ddl, re.DOTALL)
            if not column_defs:
                logger.error(f"Could not extract column definitions from DDL for {table_name}")
                return False
            
            # Create the raw table
            with self.target_engine.begin() as conn:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.raw_schema}.{table_name} (
                        {column_defs.group(1)}
                    )
                """))
            
            logger.info(f"Created raw table {self.raw_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating raw table {table_name}: {str(e)}")
            return False
    
    def create_transform_table(self, table_name: str) -> bool:
        """Create transformed table in PostgreSQL using the target DDL."""
        try:
            # Read the PostgreSQL DDL file
            pg_ddl = self.read_ddl_file(table_name, is_postgres=True)
            if not pg_ddl:
                logger.error(f"Could not read PostgreSQL DDL for {table_name}")
                return False
            
            # Extract the column definitions
            column_defs = re.search(r'CREATE TABLE.*?\((.*?)\)', pg_ddl, re.DOTALL)
            if not column_defs:
                logger.error(f"Could not extract column definitions from DDL for {table_name}")
                return False
            
            # Create the transformed table
            with self.target_engine.begin() as conn:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.target_schema}.{table_name} (
                        {column_defs.group(1)}
                    )
                """))
            
            logger.info(f"Created transform table {self.target_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating transform table {table_name}: {str(e)}")
            return False
    
    def extract_and_load_raw(self, table_name: str, force_full: bool = False) -> bool:
        """
        EXTRACT and LOAD phase: Extract data from source MySQL and load directly to PostgreSQL raw schema.
        Only basic type conversions are applied for PostgreSQL compatibility.
        """
        try:
            # Get last extraction time
            with self.target_engine.connect() as conn:
                query = text(f"""
                    SELECT last_loaded 
                    FROM {self.target_schema}.etl_load_status 
                    WHERE table_name = :table_name
                """)
                result = conn.execute(query.bindparams(table_name=table_name))
                last_loaded = result.scalar()
            
            # Build extraction query with incremental logic
            if force_full or not last_loaded:
                where_clause = "1=1"
                logger.info(f"Performing full extraction for {table_name}")
            else:
                # OpenDental incremental extraction logic
                where_clause = """
                    (DateTStamp > :last_loaded OR DateTStamp IS NULL)
                    OR (SecDateTEdit > :last_loaded OR SecDateTEdit IS NULL)
                """
                logger.info(f"Performing incremental extraction for {table_name} since {last_loaded}")
            
            # Extract data from source
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                query = f"SELECT * FROM {table_name} WHERE {where_clause}"
                df = pd.read_sql(
                    query,
                    conn,
                    params={'last_loaded': last_loaded} if not force_full and last_loaded else {}
                )
            
            if df.empty:
                logger.info(f"No new data to extract for {table_name}")
                return True
            
            # Get source schema for basic type mapping
            schema = self.get_table_schema(table_name)
            if not schema:
                logger.error(f"Could not get schema for {table_name}")
                return False
            
            # Apply ONLY basic type conversions needed for PostgreSQL compatibility
            df_clean = self.apply_basic_type_conversions(df, schema, table_name)
            
            # Create raw table if it doesn't exist
            if not self.create_raw_table(table_name):
                logger.error(f"Failed to create raw table for {table_name}")
                return False
            
            # Load raw data to PostgreSQL
            with self.target_engine.begin() as conn:
                df_clean.to_sql(
                    table_name,
                    conn,
                    schema=self.raw_schema,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
            
            # Update load status
            with self.target_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.target_schema}.etl_load_status 
                    (table_name, last_loaded, rows_loaded, load_status)
                    VALUES (:table_name, :now, :rows, 'success')
                    ON CONFLICT (table_name) 
                    DO UPDATE SET 
                        last_loaded = :now,
                        rows_loaded = :rows,
                        load_status = 'success',
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=len(df_clean)
                ))
            
            logger.info(f"Successfully loaded {len(df_clean)} rows to {self.raw_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting and loading {table_name}: {str(e)}")
            return False
    
    def transform_in_postgres(self, table_name: str) -> bool:
        """
        TRANSFORM phase: Apply business logic transformations using SQL in PostgreSQL.
        Uses the target DDL structure for the transformed table.
        """
        try:
            # Get transformation SQL for this table
            transformation_sql = self.get_transformation_sql(table_name)
            
            # Execute transformation in PostgreSQL
            with self.target_engine.begin() as conn:
                # Drop existing analytics table if it exists
                conn.execute(text(f"""
                    DROP TABLE IF EXISTS {self.target_schema}.{table_name}
                """))
                
                # Create transformed table using target DDL
                if not self.create_transform_table(table_name):
                    logger.error(f"Failed to create transform table for {table_name}")
                    return False
                
                # Insert transformed data
                conn.execute(text(f"""
                    INSERT INTO {self.target_schema}.{table_name}
                    {transformation_sql}
                """))
                
                # Get row count for tracking
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.target_schema}.{table_name}
                """))
                row_count = result.scalar()
            
            # Update transform status
            with self.target_engine.begin() as conn:
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
                with self.target_engine.begin() as conn:
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
    
    def run_elt_pipeline(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ELT pipeline for a table with proper separation of concerns.
        
        E+L - Extract from MySQL and load directly to PostgreSQL raw schema
        T - Transform using SQL in PostgreSQL (raw schema -> analytics schema)
        """
        logger.info(f"Starting ELT pipeline for {table_name}")
        
        # EXTRACT and LOAD phase
        logger.info(f"[EXTRACT+LOAD] Processing {table_name}")
        if not self.extract_and_load_raw(table_name, force_full):
            logger.error(f"Extract+Load phase failed for {table_name}")
            return False
        
        # TRANSFORM phase
        logger.info(f"[TRANSFORM] Processing {table_name}")
        if not self.transform_in_postgres(table_name):
            logger.error(f"Transform phase failed for {table_name}")
            return False
        
        logger.info(f"Successfully completed ELT pipeline for {table_name}")
        return True
    
    # Helper methods (kept from original with minimal changes)
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema for a table from MySQL."""
        try:
            schema_query = text("""
                SELECT 
                    COLUMN_NAME, 
                    COLUMN_TYPE, 
                    IS_NULLABLE,
                    COLUMN_DEFAULT, 
                    COLUMN_KEY, 
                    EXTRA
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :db_name
                AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
            """)
            
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                result = conn.execute(
                    schema_query.bindparams(
                        db_name=self.source_db,
                        table_name=table_name
                    )
                )
                return {row[0]: row[1] for row in result}
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            return {}
    
    def apply_basic_type_conversions(self, df: pd.DataFrame, schema: Dict[str, str], table_name: str) -> pd.DataFrame:
        """Apply basic type conversions to the DataFrame based on the schema."""
        for col_name, col_type in schema.items():
            if col_type.startswith('varchar'):
                try:
                    length = col_type.split('(')[1].split(')')[0]
                    df[col_name] = df[col_name].astype(f'str:{length}')
                except (IndexError, ValueError):
                    df[col_name] = df[col_name].astype('str')
            elif col_type.startswith('int'):
                df[col_name] = df[col_name].astype('int')
            elif col_type.startswith('float'):
                df[col_name] = df[col_name].astype('float')
            elif col_type.startswith('date'):
                df[col_name] = pd.to_datetime(df[col_name]).dt.strftime('%Y-%m-%d')
            elif col_type.startswith('time'):
                df[col_name] = pd.to_datetime(df[col_name]).dt.strftime('%H:%M:%S')
            elif col_type.startswith('datetime'):
                df[col_name] = pd.to_datetime(df[col_name])
            elif col_type.startswith('decimal'):
                try:
                    precision = col_type.split('(')[1].split(')')[0]
                    df[col_name] = df[col_name].astype(f'float:{precision}')
                except (IndexError, ValueError):
                    df[col_name] = df[col_name].astype('float')
            elif col_type.startswith('text'):
                df[col_name] = df[col_name].astype('str')
            elif col_type.startswith('boolean'):
                df[col_name] = df[col_name].astype('bool')
            elif col_type.startswith('set(') or col_type.startswith('enum('):
                df[col_name] = df[col_name].astype('str')
            else:
                logger.warning(f"Unknown type {col_type}, defaulting to str for {col_name}")
                df[col_name] = df[col_name].astype('str')
        return df
    
    def get_transformation_sql(self, table_name: str) -> str:
        """
        Return SQL transformations for each table.
        All business logic transformations are defined here as SQL.
        """
        
        # Base case - no transformations, just copy raw data
        base_sql = f"SELECT * FROM {self.raw_schema}.{table_name}"
        
        # Table-specific transformations using PostgreSQL SQL
        transformations = {
            'patient': f"""
                SELECT *,
                    -- Combine first and last name
                    CASE 
                        WHEN fname IS NOT NULL AND lname IS NOT NULL 
                        THEN CONCAT(fname, ' ', lname)
                        ELSE COALESCE(fname, lname)
                    END as full_name,
                    
                    -- Calculate age from birth date
                    CASE 
                        WHEN birthdate IS NOT NULL AND birthdate > '1900-01-01'::date
                        THEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate))
                        ELSE NULL
                    END as age_years,
                    
                    -- Age category
                    CASE 
                        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) < 18 THEN 'Minor'
                        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) BETWEEN 18 AND 64 THEN 'Adult'
                        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) >= 65 THEN 'Senior'
                        ELSE 'Unknown'
                    END as age_category,
                    
                    -- Clean phone numbers (example transformation)
                    REGEXP_REPLACE(
                        COALESCE(hmphn, ''), 
                        '[^0-9]', '', 'g'
                    ) as home_phone_clean,
                    
                    -- Data quality flags
                    CASE 
                        WHEN fname IS NULL OR lname IS NULL THEN true 
                        ELSE false 
                    END as missing_name_flag,
                    
                    CASE 
                        WHEN birthdate IS NULL OR birthdate <= '1900-01-01'::date THEN true 
                        ELSE false 
                    END as invalid_birthdate_flag
                    
                FROM {self.raw_schema}.patient
            """,
            
            'appointment': f"""
                SELECT *,
                    -- Calculate appointment duration in minutes
                    CASE 
                        WHEN aptdatetime IS NOT NULL AND aptendtime IS NOT NULL
                        THEN EXTRACT(EPOCH FROM (aptendtime - aptdatetime)) / 60
                        ELSE NULL
                    END as duration_minutes,
                    
                    -- Day of week
                    TO_CHAR(aptdatetime, 'Day') as day_of_week,
                    
                    -- Hour of day
                    EXTRACT(HOUR FROM aptdatetime) as hour_of_day,
                    
                    -- Business hours flag
                    CASE 
                        WHEN EXTRACT(HOUR FROM aptdatetime) BETWEEN 8 AND 17 
                        AND EXTRACT(DOW FROM aptdatetime) BETWEEN 1 AND 5
                        THEN true
                        ELSE false
                    END as is_business_hours,
                    
                    -- Appointment status description
                    CASE aptstatus
                        WHEN 1 THEN 'Scheduled'
                        WHEN 2 THEN 'Complete'
                        WHEN 3 THEN 'Broken'
                        WHEN 4 THEN 'Cancelled'
                        WHEN 5 THEN 'Unscheduled'
                        ELSE 'Unknown'
                    END as status_description,
                    
                    -- Late appointment flag (if arrived after scheduled time)
                    CASE 
                        WHEN datetimarrive > aptdatetime THEN true
                        ELSE false
                    END as arrived_late,
                    
                    -- No-show flag
                    CASE 
                        WHEN aptstatus = 3 AND datetimarrive IS NULL THEN true
                        ELSE false
                    END as no_show_flag
                    
                FROM {self.raw_schema}.appointment
            """,
            
            'procedurelog': f"""
                SELECT *,
                    -- Procedure date parts for analytics
                    EXTRACT(YEAR FROM procdate) as proc_year,
                    EXTRACT(MONTH FROM procdate) as proc_month,
                    EXTRACT(DOW FROM procdate) as proc_day_of_week,
                    
                    -- Fee categories
                    CASE 
                        WHEN procfee = 0 THEN 'No Charge'
                        WHEN procfee BETWEEN 0.01 AND 100 THEN 'Low Fee'
                        WHEN procfee BETWEEN 100.01 AND 500 THEN 'Medium Fee'
                        WHEN procfee > 500 THEN 'High Fee'
                        ELSE 'Unknown'
                    END as fee_category,
                    
                    -- Procedure status
                    CASE procstatus
                        WHEN 0 THEN 'Treatment Planned'
                        WHEN 1 THEN 'Complete'
                        WHEN 2 THEN 'Existing Current'
                        WHEN 3 THEN 'Existing Other'
                        WHEN 4 THEN 'Referred Out'
                        WHEN 5 THEN 'Cancelled'
                        WHEN 6 THEN 'Condition'
                        WHEN 7 THEN 'Existing Referred Out'
                        ELSE 'Unknown'
                    END as status_description,
                    
                    -- Insurance vs cash procedure
                    CASE 
                        WHEN claimnum > 0 THEN 'Insurance'
                        ELSE 'Cash/Self-Pay'
                    END as payment_type
                    
                FROM {self.raw_schema}.procedurelog
            """
        }
        
        return transformations.get(table_name, base_sql)


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
                    success = pipeline.extract_and_load_raw(table, args.full_sync)
                elif args.load_only:
                    success = pipeline.extract_and_load_raw(table, args.full_sync)
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