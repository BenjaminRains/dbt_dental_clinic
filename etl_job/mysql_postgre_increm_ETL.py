import os
import pandas as pd
from sqlalchemy import create_engine, text, quoted_name
from sqlalchemy.engine.url import URL
import pymysql
from dotenv import load_dotenv
import logging
import argparse
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError, OperationalError
from pandas.errors import EmptyDataError
from sqlalchemy.engine import Connection
from connection_factory import get_source_connection, get_target_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_incremental.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get database connections using the factory
try:
    source_engine = get_source_connection(readonly=True)
    target_engine = get_target_connection()
    logger.info("Successfully initialized database connections")
except Exception as e:
    logger.error(f"Failed to initialize database connections: {str(e)}")
    raise

class TableConfig:
    """Configuration for table-specific ETL settings."""
    
    def __init__(self, 
                 table_name: str,
                 created_at_column: Optional[str] = None,
                 updated_at_column: Optional[str] = None,
                 primary_key_column: Optional[str] = None,
                 batch_size: int = 10000,
                 max_retries: int = 3,
                 retry_delay: int = 300):
        self.table_name = table_name
        self.created_at_column = created_at_column
        self.updated_at_column = updated_at_column
        self.primary_key_column = primary_key_column
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay

class ETLConfig:
    """Global ETL configuration settings."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLConfig, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance
    
    def initialize(self):
        """Initialize configuration with environment variables."""
        self.chunk_size = int(os.getenv('ETL_CHUNK_SIZE', '50000'))
        self.sub_chunk_size = int(os.getenv('ETL_SUB_CHUNK_SIZE', '10000'))
        self.max_retries = int(os.getenv('ETL_MAX_RETRIES', '2'))
        self.tolerance = float(os.getenv('ETL_TOLERANCE', '0.001'))
        self.small_table_threshold = int(os.getenv('ETL_SMALL_TABLE_THRESHOLD', '10000'))

def validate_env_vars() -> bool:
    """Validate all required environment variables are set."""
    required_vars = [
        'OPENDENTAL_SOURCE_USER',
        'OPENDENTAL_SOURCE_PW',
        'OPENDENTAL_SOURCE_HOST',
        'OPENDENTAL_SOURCE_PORT',
        'OPENDENTAL_SOURCE_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'POSTGRES_HOST',
        'POSTGRES_PORT',
        'POSTGRES_DATABASE'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return False
    return True

def validate_connections() -> bool:
    """Validate database connections before starting ETL process."""
    try:
        # Test source connection
        with source_engine.connect() as source_conn:
            source_conn.execute(text("SELECT 1"))
            logger.info(f"[OK] Source MySQL connection successful")
            
        # Test target connection
        with target_engine.connect() as target_conn:
            target_conn.execute(text("SELECT 1"))
            logger.info(f"[OK] Target PostgreSQL connection successful")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Connection validation failed: {str(e)}")
        return False

def convert_data_types(df: pd.DataFrame, table_name: str = None) -> Tuple[pd.DataFrame, List[str]]:
    """Convert DataFrame types to be compatible with PostgreSQL, handling OpenDental specifics."""
    df = df.copy()
    warnings = []
    
    # Get PostgreSQL column types for validation
    pg_column_types = {}
    try:
        with target_engine.connect() as conn:
            query = text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = :table_name
            """)
            result = conn.execute(query.bindparams(table_name=table_name))
            pg_column_types = {row[0]: row[1].lower() for row in result}
    except Exception as e:
        logger.warning(f"Could not get PostgreSQL schema for {table_name}: {str(e)}")
    
    for col in df.columns:
        if col not in pg_column_types:
            continue
            
        target_type = pg_column_types[col]
        try:
            # Handle TIME type specifically (common in OpenDental)
            if target_type == 'time without time zone':
                def convert_to_time_string(val):
                    if pd.isna(val) or val == '' or val == 0:
                        return None
                    
                    try:
                        # Handle nanoseconds format from OpenDental
                        if isinstance(val, (int, float)) and val > 86400:
                            seconds = val / 1000000000  # Convert nanoseconds to seconds
                        else:
                            seconds = float(val)
                        
                        hours = int(seconds // 3600)
                        minutes = int((seconds % 3600) // 60)
                        secs = int(seconds % 60)
                        
                        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
                    except (ValueError, TypeError):
                        return None
                
                df[col] = df[col].apply(convert_to_time_string)
                logger.info(f"Converted column {col} to time format")
                
            # Boolean type - handle OpenDental's tinyint(1) conventions
            elif target_type == 'boolean':
                bool_values = df[col].map(lambda x: bool(x) if pd.notnull(x) else None)
                df[col] = pd.Series(bool_values, index=df.index, dtype="boolean")
                
            # Integer types - handle OpenDental's various int formats
            elif target_type in ('smallint', 'integer', 'bigint'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            # Numeric/Decimal types
            elif target_type in ('numeric', 'decimal', 'real', 'double precision'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            # Timestamp/Date types - critical for OpenDental date handling
            elif target_type in ('timestamp without time zone', 'timestamp with time zone', 'timestamp', 'date'):
                if df[col].dtype == 'object':
                    # Handle OpenDental's zero dates and invalid dates
                    zero_date_patterns = ['0000-00-00', '0000-00-00 00:00:00', '0001-01-01', '0001-01-01 00:00:00']
                    for pattern in zero_date_patterns:
                        df.loc[df[col] == pattern, col] = None
                    
                    # Track zero date replacements
                    zero_date_mask = df[col].astype(str).str.contains(r'^0+[-/]0+[-/]0+', regex=True, na=False)
                    if zero_date_mask.any():
                        zero_count = zero_date_mask.sum()
                        warnings.append(f"Column '{col}' had {zero_count} zero dates replaced with NULL")
                        df.loc[zero_date_mask, col] = None
                
                # Convert to datetime
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Track conversion issues
                if df[col].isna().all() and len(df) > 0:
                    warnings.append(f"Column '{col}' has all invalid datetime values")
            
            # Text types - handle OpenDental's text encoding
            elif target_type in ('text', 'varchar', 'char'):
                df[col] = df[col].astype(str).replace(['nan', 'None', ''], None)
                
        except Exception as e:
            warnings.append(f"Error converting column '{col}' to {target_type}: {str(e)}")
            
    return df, warnings

def is_bool_column(column_name: str, column_type: str) -> bool:
    """Determine if a column should be treated as boolean based on OpenDental conventions."""
    # Check for tinyint(1) which is typically boolean in MySQL/MariaDB
    if str(column_type).lower() == 'tinyint(1)':
        return True
        
    # OpenDental-specific boolean patterns
    bool_patterns = [
        'is_', 'has_', 'show', 'use', 'allow', 'enable', 'disable',
        'visible', 'active', 'hidden', 'locked', 'no', 'can', 
        'is', 'has', 'flag', 'bool', 'boolean', 'timelocked'
    ]
    
    column_lower = column_name.lower()
    
    # OpenDental-specific boolean column names
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

def get_metadata_columns(table_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Get created_at, updated_at, and primary key columns for OpenDental tables."""
    try:
        with source_engine.connect() as conn:
            # Get datetime columns
            datetime_query = text("""
                SELECT column_name, column_type
                FROM information_schema.columns 
                WHERE table_schema = :db_name 
                AND table_name = :table_name 
                AND (data_type = 'datetime' OR data_type = 'timestamp')
                ORDER BY ordinal_position
            """)
            datetime_result = conn.execute(datetime_query.bindparams(
                db_name=os.getenv('OPENDENTAL_SOURCE_DB'),
                table_name=table_name
            ))
            datetime_columns = [(row[0], row[1]) for row in datetime_result]
            
            # Get primary key
            pk_query = text("""
                SELECT column_name 
                FROM information_schema.key_column_usage
                WHERE table_schema = :db_name
                AND table_name = :table_name
                AND constraint_name = 'PRIMARY'
                LIMIT 1
            """)
            pk_result = conn.execute(pk_query.bindparams(
                db_name=os.getenv('OPENDENTAL_SOURCE_DB'),
                table_name=table_name
            ))
            primary_key = pk_result.scalar()
            
        # OpenDental common patterns for created_at columns
        created_at_patterns = [
            'DateEntry', 'SecDateTEntry', 'CreatedDate', 'InsertDate', 'dateentry'
        ]
        
        # OpenDental common patterns for updated_at columns  
        updated_at_patterns = [
            'DateTStamp', 'SecDateTEdit', 'ModifiedDate', 'datetimestamp', 'datetstamp'
        ]
        
        created_at = None
        updated_at = None
        
        # Find created_at column
        for col_name, col_type in datetime_columns:
            for pattern in created_at_patterns:
                if pattern.lower() == col_name.lower():
                    created_at = col_name
                    break
            if created_at:
                break
        
        # Find updated_at column
        for col_name, col_type in datetime_columns:
            for pattern in updated_at_patterns:
                if pattern.lower() == col_name.lower():
                    updated_at = col_name
                    break
            if updated_at:
                break
        
        # If no specific updated_at found, use the first datetime column
        if not updated_at and datetime_columns:
            updated_at = datetime_columns[0][0]
            
        return created_at, updated_at, primary_key
        
    except Exception as e:
        logger.error(f"Error getting metadata columns for {table_name}: {str(e)}")
        return None, None, None

def get_all_tables() -> Dict[str, TableConfig]:
    """Fetch all tables from MySQL and create TableConfig objects."""
    try:
        with source_engine.connect() as conn:
            # Get all tables from the database
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :db_name
                AND table_type = 'BASE TABLE'
            """)
            result = conn.execute(query.bindparams(db_name=os.getenv('OPENDENTAL_SOURCE_DB')))
            tables = [row[0] for row in result]
            
            # Create TableConfig for each table
            table_configs = {}
            for table in tables:
                try:
                    created_at, updated_at, primary_key = get_metadata_columns(table)
                    
                    # Only include tables that have at least one timestamp column or primary key
                    if created_at or updated_at or primary_key:
                        table_configs[table] = TableConfig(
                            table_name=table,
                            created_at_column=created_at,
                            updated_at_column=updated_at,
                            primary_key_column=primary_key
                        )
                        logger.info(f"Configured table {table}: created_at={created_at}, updated_at={updated_at}, pk={primary_key}")
                    else:
                        logger.warning(f"Skipping table {table}: no timestamp or primary key columns found")
                        
                except Exception as e:
                    logger.error(f"Error configuring table {table}: {str(e)}")
                    continue
            
            logger.info(f"Configured {len(table_configs)} tables for incremental sync")
            return table_configs
            
    except Exception as e:
        logger.error(f"Error fetching tables: {str(e)}")
        return {}

def ensure_target_table_exists(table_name: str) -> bool:
    """Ensure target table exists in PostgreSQL with proper schema."""
    try:
        # Check if table exists
        with target_engine.connect() as conn:
            exists_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = :table_name
                )
            """)
            table_exists = conn.execute(exists_query.bindparams(table_name=table_name)).scalar()
            
        if table_exists:
            logger.info(f"Target table {table_name} already exists")
            return True
            
        # Create the table by copying structure from MySQL
        logger.info(f"Creating target table {table_name}")
        return create_target_table_from_source(table_name)
        
    except Exception as e:
        logger.error(f"Error ensuring target table {table_name}: {str(e)}")
        return False

def create_target_table_from_source(table_name: str) -> bool:
    """Create PostgreSQL table based on MySQL schema with proper type mapping."""
    try:
        # Get MySQL table schema
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
        
        schema_df = pd.read_sql(
            schema_query, 
            source_engine,
            params={
                'db_name': os.getenv('OPENDENTAL_SOURCE_DB'),
                'table_name': table_name
            }
        )
        
        if len(schema_df) == 0:
            logger.warning(f"No schema found for {table_name}")
            return False
        
        # Build CREATE TABLE statement
        create_sql_parts = [f'CREATE TABLE IF NOT EXISTS "{table_name}" (']
        primary_keys = []
        
        for _, row in schema_df.iterrows():
            col_name = row['COLUMN_NAME']
            col_type = map_mysql_to_postgres_type(row['COLUMN_TYPE'], col_name)
            
            # For analytics, allow NULLs except for primary keys
            if row['COLUMN_KEY'] == 'PRI':
                nullable = "NOT NULL"
                primary_keys.append(f'"{col_name}"')
            else:
                nullable = ""  # Allow NULL by default
            
            # Handle auto increment
            if 'auto_increment' in str(row['EXTRA']).lower():
                if 'int' in str(row['COLUMN_TYPE']).lower():
                    col_type = 'SERIAL'
                    nullable = ""
            
            # Handle default values
            default = ""
            if pd.notna(row['COLUMN_DEFAULT']):
                if col_type == 'BOOLEAN':
                    if row['COLUMN_DEFAULT'] in ('1', 1, 'TRUE', 'true', 'True'):
                        default = "DEFAULT TRUE"
                    else:
                        default = "DEFAULT FALSE"
                elif 'timestamp' in col_type.lower():
                    if 'current_timestamp' in str(row['COLUMN_DEFAULT']).lower():
                        default = "DEFAULT CURRENT_TIMESTAMP"
                elif is_numeric_type(row['COLUMN_TYPE']):
                    default = f"DEFAULT {row['COLUMN_DEFAULT']}"
            
            create_sql_parts.append(f'    "{col_name}" {col_type} {nullable} {default},')
        
        # Add primary key constraint
        if primary_keys:
            primary_key_line = f"    PRIMARY KEY ({', '.join(primary_keys)})"
            create_sql_parts.append(primary_key_line)
        else:
            create_sql_parts[-1] = create_sql_parts[-1].rstrip(',')
        
        create_sql_parts.append(");")
        create_sql = "\n".join(create_sql_parts)
        
        # Execute creation
        with target_engine.begin() as conn:
            conn.execute(text(create_sql))
            
        logger.info(f"Created table {table_name} in PostgreSQL")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {str(e)}")
        return False

def map_mysql_to_postgres_type(mysql_type: str, column_name: str = '') -> str:
    """Map MySQL types to PostgreSQL types with OpenDental-specific handling."""
    try:
        type_lower = str(mysql_type).lower()
        
        # Handle OpenDental's boolean convention
        if type_lower == 'tinyint(1)' or is_bool_column(column_name, mysql_type):
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
            
        # Handle SET and ENUM types common in OpenDental
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
            
        # Date/time types - critical for OpenDental
        elif type_lower.startswith('datetime'):
            return 'TIMESTAMP'
        elif type_lower.startswith('timestamp'):
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

def is_numeric_type(mysql_type):
    """Check if MySQL type is numeric."""
    type_lower = mysql_type.lower()
    return any(t in type_lower for t in ['int', 'decimal', 'float', 'double', 'numeric'])

def get_last_sync_time(table_name: str) -> Optional[datetime]:
    """Get the last successful sync time for a table."""
    try:
        with target_engine.connect() as conn:
            query = text("""
                SELECT last_modified 
                FROM etl_sync_status 
                WHERE table_name = :table_name
            """)
            result = conn.execute(query.bindparams(table_name=table_name))
            return result.scalar()
    except Exception as e:
        logger.error(f"Error getting last sync time for {table_name}: {str(e)}")
        return None

def update_sync_time(table_name: str, sync_time: datetime, rows_processed: int) -> bool:
    """Update the last sync time for a table."""
    try:
        with target_engine.begin() as conn:
            query = text("""
                INSERT INTO etl_sync_status (table_name, last_modified, sync_status, rows_processed)
                VALUES (:table_name, :sync_time, 'success', :rows_processed)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_modified = :sync_time,
                    sync_status = 'success',
                    rows_processed = :rows_processed
            """)
            conn.execute(query.bindparams(
                table_name=table_name,
                sync_time=sync_time,
                rows_processed=rows_processed
            ))
            return True
    except Exception as e:
        logger.error(f"Error updating sync time for {table_name}: {str(e)}")
        return False

def get_changed_records(table_config: TableConfig, last_sync: Optional[datetime]) -> pd.DataFrame:
    """Get records that have changed since last sync with proper OpenDental handling."""
    try:
        # Build WHERE clause based on available metadata columns
        where_clauses = []
        
        if table_config.updated_at_column:
            where_clauses.append(f"`{table_config.updated_at_column}` > :last_sync")
        if table_config.created_at_column and table_config.created_at_column != table_config.updated_at_column:
            where_clauses.append(f"`{table_config.created_at_column}` > :last_sync")
        
        if not where_clauses:
            # If no timestamp columns, get all records (full sync)
            where_clause = "1=1"
            logger.warning(f"No timestamp columns found for {table_config.table_name}, performing full sync")
        else:
            where_clause = " OR ".join(where_clauses)
        
        # Add metadata columns for tracking
        metadata_columns = []
        if table_config.created_at_column:
            metadata_columns.append(f"`{table_config.created_at_column}` as _created_at")
        else:
            metadata_columns.append("NULL as _created_at")
            
        if table_config.updated_at_column:
            metadata_columns.append(f"`{table_config.updated_at_column}` as _updated_at")
        else:
            metadata_columns.append("NULL as _updated_at")
        
        metadata_select = ", ".join(metadata_columns)
        
        # Order by the most appropriate column for consistent results
        order_column = table_config.updated_at_column or table_config.created_at_column or table_config.primary_key_column
        order_clause = f"ORDER BY `{order_column}`" if order_column else ""
        
        query = f"""
            SELECT 
                *,
                CURRENT_TIMESTAMP as _loaded_at,
                {metadata_select}
            FROM `{table_config.table_name}`
            WHERE {where_clause}
            {order_clause}
        """
        
        with source_engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={'last_sync': last_sync or datetime(1970, 1, 1)}
            )
            return df
            
    except Exception as e:
        logger.error(f"Error getting changed records for {table_config.table_name}: {str(e)}")
        raise

def sync_table_incremental(table_name: str, table_configs: Dict[str, TableConfig]) -> bool:
    """Perform incremental sync for a specific table with data transformations."""
    if table_name not in table_configs:
        logger.error(f"No configuration found for table {table_name}")
        return False
        
    config = table_configs[table_name]
    
    try:
        # Ensure target table exists
        if not ensure_target_table_exists(table_name):
            logger.error(f"Failed to ensure target table exists for {table_name}")
            return False
        
        # Get last sync time
        last_sync = get_last_sync_time(table_name)
        logger.info(f"Last sync for {table_name}: {last_sync}")
        
        # Get changed records
        df = get_changed_records(config, last_sync)
        
        if df.empty:
            logger.info(f"No changes found for {table_name}")
            return True
        
        logger.info(f"Found {len(df)} changed records for {table_name}")
        
        # Apply data transformations (this is the key addition!)
        transformed_df, warnings = convert_data_types(df, table_name)
        
        if warnings:
            for warning in warnings:
                logger.warning(f"{table_name}: {warning}")
        
        # Process in batches
        total_rows = len(transformed_df)
        processed_rows = 0
        
        for i in range(0, total_rows, config.batch_size):
            batch = transformed_df.iloc[i:i + config.batch_size]
            
            # Load to PostgreSQL with proper transaction handling
            with target_engine.begin() as conn:
                batch.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            processed_rows += len(batch)
            logger.info(f"Processed {processed_rows}/{total_rows} rows for {table_name}")
        
        # Update sync time
        current_time = datetime.now()
        update_sync_time(table_name, current_time, processed_rows)
        
        logger.info(f"Successfully synced {processed_rows} records for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error syncing {table_name}: {str(e)}")
        return False

def ensure_tracking_table_exists() -> bool:
    """Create ETL tracking table if it doesn't exist."""
    try:
        with target_engine.begin() as conn:
            create_query = text("""
                CREATE TABLE IF NOT EXISTS etl_sync_status (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL UNIQUE,
                    last_modified TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00',
                    sync_status VARCHAR(50) DEFAULT 'pending',
                    rows_processed INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(create_query)
            logger.info("ETL tracking table ensured")
            return True
    except Exception as e:
        logger.error(f"Error creating tracking table: {str(e)}")
        return False

def main():
    """Main ETL process with incremental sync and data transformations."""
    parser = argparse.ArgumentParser(description='Incremental ETL from MySQL to PostgreSQL with transformations')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync')
    parser.add_argument('--full-sync', action='store_true', help='Force full sync for all tables')
    args = parser.parse_args()
    
    # Validate environment variables first
    if not validate_env_vars():
        logger.error("Missing required environment variables. Exiting.")
        return
    
    # Initialize database connections
    try:
        global source_engine, target_engine
        source_engine = get_source_connection(readonly=True)
        target_engine = get_target_connection()
        logger.info("Successfully initialized database connections")
    except Exception as e:
        logger.error(f"Failed to initialize database connections: {str(e)}")
        return
    
    # Validate connections
    if not validate_connections():
        logger.error("Connection validation failed. Exiting.")
        return
    
    # Ensure tracking table exists
    if not ensure_tracking_table_exists():
        logger.error("Failed to create tracking table. Exiting.")
        return
    
    # Get table configurations
    table_configs = get_all_tables()
    if not table_configs:
        logger.error("No tables configured for sync. Exiting.")
        return
    
    # Determine tables to sync
    tables_to_sync = args.tables if args.tables else list(table_configs.keys())
    
    # Filter to only include configured tables
    tables_to_sync = [t for t in tables_to_sync if t in table_configs]
    
    if not tables_to_sync:
        logger.error("No valid tables to sync. Exiting.")
        return
    
    logger.info(f"Starting incremental sync for {len(tables_to_sync)} tables")
    
    # Track results
    results = {'success': 0, 'failed': 0, 'total_rows': 0}
    
    try:
        for table in tables_to_sync:
            logger.info(f"\n--- Starting sync for {table} ---")
            
            if args.full_sync:
                # Clear last sync time to force full sync
                try:
                    with target_engine.begin() as conn:
                        conn.execute(text(
                            "UPDATE etl_sync_status SET last_modified = '1970-01-01 00:00:00' WHERE table_name = :table"
                        ).bindparams(table=table))
                    logger.info(f"Cleared sync timestamp for {table} (full sync mode)")
                except Exception as e:
                    logger.warning(f"Could not clear sync timestamp for {table}: {str(e)}")
            
            if sync_table_incremental(table, table_configs):
                results['success'] += 1
                logger.info(f"✓ Successfully synced {table}")
            else:
                results['failed'] += 1
                logger.error(f"✗ Failed to sync {table}")
        
        # Summary
        logger.info(f"\n=== SYNC SUMMARY ===")
        logger.info(f"Tables processed: {len(tables_to_sync)}")
        logger.info(f"Successful: {results['success']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info(f"Success rate: {results['success']/len(tables_to_sync)*100:.1f}%")
        
    except Exception as e:
        logger.error(f"Unexpected error during sync process: {str(e)}")
        return False
    finally:
        # Cleanup connections
        try:
            source_engine.dispose()
            target_engine.dispose()
            logger.info("Successfully closed database connections")
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")
    
    return True

if __name__ == "__main__":
    main()