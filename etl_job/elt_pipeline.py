import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from typing import Dict, List, Optional
from connection_factory import get_source_connection, get_staging_connection, get_target_connection
from dotenv import load_dotenv

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
        self.staging_engine = None
        self.target_engine = None
        
        # Database names and schemas
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        self.staging_db = os.getenv('STAGING_MYSQL_DATABASE')
        self.target_db = os.getenv('POSTGRES_DATABASE')
        self.target_schema = os.getenv('POSTGRES_SCHEMA', 'analytics')
        self.raw_schema = os.getenv('POSTGRES_RAW_SCHEMA', 'raw')
        
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
        if not self.staging_db:
            missing_dbs.append('STAGING_MYSQL_DATABASE')
        if not self.target_db:
            missing_dbs.append('POSTGRES_DATABASE')
        
        if missing_dbs:
            error_msg = f"Missing required database names: {', '.join(missing_dbs)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Source database: {self.source_db}")
        logger.info(f"Staging database: {self.staging_db}")
        logger.info(f"Target database: {self.target_db}")
        logger.info(f"Analytics schema: {self.target_schema}")
        logger.info(f"Raw schema: {self.raw_schema}")
    
    def initialize_connections(self):
        """Initialize database connections with proper error handling."""
        try:
            self.source_engine = get_source_connection(readonly=True)
            self.staging_engine = get_staging_connection()
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
            
            if self.staging_engine:
                self.staging_engine.dispose()
                self.staging_engine = None
                logger.info("Closed staging database connection")
            
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
        """Create necessary tracking tables in staging and target databases."""
        try:
            # Create staging tracking table (for Extract phase)
            with self.staging_engine.begin() as conn:
                conn.execute(text(f"USE {self.staging_db}"))
                
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
    
    def extract_to_staging(self, table_name: str, force_full: bool = False) -> bool:
        """
        EXTRACT phase: Extract data from source MySQL to staging MySQL.
        This is purely for incremental extraction logic and temporary storage.
        """
        try:
            # Get last extraction time
            with self.staging_engine.connect() as conn:
                conn.execute(text(f"USE {self.staging_db}"))
                
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
                # OpenDental incremental extraction logic
                where_clause = """
                    (DateTStamp > :last_extracted OR DateTStamp IS NULL)
                    OR (SecDateTEdit > :last_extracted OR SecDateTEdit IS NULL)
                """
                logger.info(f"Performing incremental extraction for {table_name} since {last_extracted}")
            
            # Extract data from source
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                query = f"SELECT * FROM {table_name} WHERE {where_clause}"
                df = pd.read_sql(
                    query,
                    conn,
                    params={'last_extracted': last_extracted} if not force_full and last_extracted else {}
                )
            
            if df.empty:
                logger.info(f"No new data to extract for {table_name}")
                return True
            
            # Store in staging for temporary processing
            with self.staging_engine.begin() as conn:
                conn.execute(text(f"USE {self.staging_db}"))
                
                # Create/replace staging table
                df.to_sql(
                    f"staging_{table_name}",
                    conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
            
            # Update extraction status
            with self.staging_engine.begin() as conn:
                conn.execute(text(f"USE {self.staging_db}"))
                
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
                    rows=len(df)
                ))
            
            logger.info(f"Successfully extracted {len(df)} rows for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting {table_name}: {str(e)}")
            return False
    
    def load_raw_to_postgres(self, table_name: str) -> bool:
        """
        LOAD phase: Load raw data from staging to PostgreSQL raw schema.
        Minimal transformations - only type conversion for PostgreSQL compatibility.
        """
        try:
            # Read from staging
            with self.staging_engine.connect() as conn:
                conn.execute(text(f"USE {self.staging_db}"))
                df = pd.read_sql(f"SELECT * FROM staging_{table_name}", conn)
            
            if df.empty:
                logger.info(f"No data to load for {table_name}")
                return True
            
            # Get source schema for basic type mapping
            schema = self.get_table_schema(table_name)
            if not schema:
                logger.error(f"Could not get schema for {table_name}")
                return False
            
            # Apply ONLY basic type conversions needed for PostgreSQL compatibility
            df_clean = self.apply_basic_type_conversions(df, schema, table_name)
            
            # Create raw table if it doesn't exist
            if not self.create_raw_table(table_name, schema):
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
            logger.error(f"Error loading raw data for {table_name}: {str(e)}")
            return False
    
    def apply_basic_type_conversions(self, df: pd.DataFrame, schema: Dict[str, str], table_name: str) -> pd.DataFrame:
        """
        Apply ONLY basic type conversions needed for PostgreSQL compatibility.
        No business logic transformations - those happen in PostgreSQL.
        """
        try:
            df = df.copy()
            
            for col in df.columns:
                if col not in schema:
                    continue
                
                mysql_type = schema[col]
                try:
                    # Handle OpenDental's zero dates (PostgreSQL compatibility)
                    if any(t in mysql_type.lower() for t in ['datetime', 'timestamp', 'date']):
                        if df[col].dtype == 'object':
                            # Replace OpenDental zero dates with NULL
                            zero_date_patterns = ['0000-00-00', '0000-00-00 00:00:00', '0001-01-01', '0001-01-01 00:00:00']
                            for pattern in zero_date_patterns:
                                df.loc[df[col] == pattern, col] = None
                        
                        # Convert to datetime
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    # Handle TIME type (convert OpenDental nanoseconds to time string)
                    elif 'time' in mysql_type.lower() and not ('datetime' in mysql_type.lower() or 'timestamp' in mysql_type.lower()):
                        def convert_time_for_postgres(val):
                            if pd.isna(val) or val == '' or val == 0:
                                return None
                            
                            try:
                                if isinstance(val, (int, float)) and val > 86400:
                                    seconds = val / 1000000000  # OpenDental nanoseconds
                                else:
                                    seconds = float(val)
                                
                                hours = int(seconds // 3600)
                                minutes = int((seconds % 3600) // 60)
                                secs = int(seconds % 60)
                                
                                return f"{hours:02d}:{minutes:02d}:{secs:02d}"
                            except (ValueError, TypeError):
                                return None
                        
                        df[col] = df[col].apply(convert_time_for_postgres)
                    
                    # Handle numeric types
                    elif self.is_numeric_type(mysql_type):
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Handle text types (clean up NaN values)
                    elif any(t in mysql_type.lower() for t in ['varchar', 'char', 'text']):
                        df[col] = df[col].astype(str).replace(['nan', 'None', ''], None)
                
                except Exception as e:
                    logger.warning(f"Error converting column '{col}' in {table_name}: {str(e)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error applying basic type conversions to {table_name}: {str(e)}")
            raise
    
    def transform_in_postgres(self, table_name: str) -> bool:
        """
        TRANSFORM phase: Apply business logic transformations using SQL in PostgreSQL.
        All transformations happen in PostgreSQL using SQL.
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
                
                # Create transformed table using SQL
                conn.execute(text(f"""
                    CREATE TABLE {self.target_schema}.{table_name} AS
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
    
    def run_elt_pipeline(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ELT pipeline for a table with proper separation of concerns.
        
        E - Extract from MySQL to staging MySQL (incremental logic)
        L - Load raw data from staging to PostgreSQL raw schema
        T - Transform using SQL in PostgreSQL (raw schema -> analytics schema)
        """
        logger.info(f"Starting ELT pipeline for {table_name}")
        
        # EXTRACT phase
        logger.info(f"[EXTRACT] Processing {table_name}")
        if not self.extract_to_staging(table_name, force_full):
            logger.error(f"Extract phase failed for {table_name}")
            return False
        
        # LOAD phase  
        logger.info(f"[LOAD] Processing {table_name}")
        if not self.load_raw_to_postgres(table_name):
            logger.error(f"Load phase failed for {table_name}")
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
    
    def create_raw_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create raw table in PostgreSQL with proper type mapping."""
        try:
            create_sql_parts = [f'CREATE TABLE IF NOT EXISTS {self.raw_schema}."{table_name}" (']
            
            for col_name, col_type in schema.items():
                pg_type = self.map_mysql_to_postgres_type(col_type, col_name)
                create_sql_parts.append(f'    "{col_name}" {pg_type},')
            
            # Remove trailing comma and close
            create_sql_parts[-1] = create_sql_parts[-1].rstrip(',')
            create_sql_parts.append(");")
            create_sql = "\n".join(create_sql_parts)
            
            # Execute creation
            with self.target_engine.begin() as conn:
                conn.execute(text(create_sql))
            
            logger.info(f"Created raw table {self.raw_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating raw table {table_name}: {str(e)}")
            return False
    
    def map_mysql_to_postgres_type(self, mysql_type: str, column_name: str = '') -> str:
        """Map MySQL types to PostgreSQL types with OpenDental-specific handling."""
        try:
            type_lower = str(mysql_type).lower()
            
            # Handle OpenDental's boolean convention
            if type_lower == 'tinyint(1)' or self.is_bool_column(column_name, mysql_type):
                return 'BOOLEAN'
            elif type_lower.startswith('tinyint'):
                return 'SMALLINT'
            elif type_lower.startswith('smallint'):
                return 'SMALLINT'
            elif type_lower.startswith('mediumint'):
                return 'INTEGER'
            elif type_lower.startswith('int'):
                return 'INTEGER'
            elif type_lower.startswith('bigint'):
                return 'BIGINT'
            
            # Handle SET and ENUM types
            elif type_lower.startswith('set(') or type_lower.startswith('enum('):
                return 'TEXT'
            
            # String types
            elif type_lower.startswith('varchar') or type_lower.startswith('char'):
                try:
                    length = type_lower.split('(')[1].split(')')[0]
                    return f'VARCHAR({length})'
                except (IndexError, ValueError):
                    return 'VARCHAR(255)'
            elif type_lower.startswith('text'):
                return 'TEXT'
            
            # Date/time types
            elif type_lower.startswith('datetime') or type_lower.startswith('timestamp'):
                return 'TIMESTAMP'
            elif type_lower.startswith('date'):
                return 'DATE'
            elif type_lower.startswith('time'):
                return 'TIME'
            
            # Numeric types
            elif type_lower.startswith('decimal') or type_lower.startswith('numeric'):
                try:
                    precision = type_lower.split('(')[1].split(')')[0]
                    return f'NUMERIC({precision})'
                except (IndexError, ValueError):
                    return 'NUMERIC(10,2)'
            elif type_lower.startswith('float'):
                return 'REAL'
            elif type_lower.startswith('double'):
                return 'DOUBLE PRECISION'
            
            # Binary types
            elif type_lower.startswith('blob') or type_lower.startswith('binary'):
                return 'BYTEA'
            
            else:
                logger.warning(f"Unknown type {mysql_type}, defaulting to TEXT")
                return 'TEXT'
            
        except Exception as e:
            logger.error(f"Error mapping type {mysql_type}: {str(e)}")
            return 'TEXT'
    
    def is_bool_column(self, column_name: str, column_type: str) -> bool:
        """Determine if a column should be treated as boolean based on OpenDental conventions."""
        if str(column_type).lower() == 'tinyint(1)':
            return True
        
        bool_patterns = [
            'is_', 'has_', 'show', 'use', 'allow', 'enable', 'disable',
            'visible', 'active', 'hidden', 'locked', 'no', 'can', 
            'is', 'has', 'flag', 'bool', 'boolean', 'timelocked'
        ]
        
        column_lower = column_name.lower()
        known_boolean_columns = [
            'timelocked', 'isnewpatient', 'ishygiene', 'isdeleted', 'isreverse',
            'isestimate', 'isinsurance', 'isactive', 'iscomplete'
        ]
        
        if column_lower in known_boolean_columns:
            return True
        
        for pattern in bool_patterns:
            if (column_lower.startswith(pattern) or 
                f"_{pattern}" in column_lower or 
                column_lower.endswith(f"_{pattern}")):
                return True
            
        return False
    
    def is_numeric_type(self, mysql_type: str) -> bool:
        """Check if MySQL type is numeric."""
        type_lower = mysql_type.lower()
        return any(t in type_lower for t in ['int', 'decimal', 'float', 'double', 'numeric'])


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
                    success = pipeline.extract_to_staging(table, args.full_sync)
                elif args.load_only:
                    success = pipeline.load_raw_to_postgres(table)
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