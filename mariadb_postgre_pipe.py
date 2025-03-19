import os
import pandas as pd
from sqlalchemy import create_engine, text, quoted_name
from sqlalchemy.engine.url import URL
import pymysql
from dotenv import load_dotenv
import logging
import argparse
from typing import Dict, List, Tuple
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError, OperationalError
from pandas.errors import EmptyDataError
from sqlalchemy.engine import Connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MariaDB Connection Credentials (using MariaDB localhost root configuration)
mariadb_engine = create_engine(
    f"mysql+pymysql://{os.getenv('MARIADB_ROOT_USER')}:{os.getenv('MARIADB_ROOT_PASSWORD')}@"
    f"{os.getenv('MARIADB_ROOT_HOST')}:{os.getenv('MARIADB_ROOT_PORT')}/"
    f"{os.getenv('DBT_MYSQL_DATABASE')}"
)

# PostgreSQL Connection Credentials
pg_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DATABASE')}"  # Changed from POSTGRES_DB to match .env.template
)

# Add connection validation
def validate_connections() -> bool:
    """Validate database connections before starting ETL process."""
    try:
        # Test MariaDB connection
        with mariadb_engine.connect() as maria_conn:
            maria_conn.execute(text("SELECT 1"))
            logger.info(
                f"[OK] MariaDB connection successful: "
                f"{os.getenv('MARIADB_ROOT_HOST')}:{os.getenv('MARIADB_ROOT_PORT')}"
            )
            
        # Test PostgreSQL connection
        with pg_engine.connect() as postgres_conn:
            postgres_conn.execute(text("SELECT 1"))
            logger.info(
                f"[OK] PostgreSQL connection successful: "
                f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
            )
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Connection validation failed: {str(e)}")
        return False

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataFrame types to be compatible with PostgreSQL."""
    # Make a copy of the dataframe to avoid SettingWithCopyWarning
    df = df.copy()
    
    for col in df.columns:
        # Handle problematic data types
        if df[col].dtype == 'object':
            # Convert boolean-like values
            if set(df[col].dropna().unique()).issubset({True, False, 'True', 'False', 1, 0, '1', '0'}):
                # The issue is with the astype('boolean') not working properly
                # Use pd.array with BooleanDtype explicitly
                bool_map = {'True': True, 'False': False, '1': True, '0': False}
                
                # Convert to Python bool values first
                bool_values = []
                for val in df[col]:
                    if pd.isna(val):
                        bool_values.append(pd.NA)
                    elif val in (True, 1, '1', 'True'):
                        bool_values.append(True)
                    else:
                        bool_values.append(False)
                        
                # Create a boolean array with explicit dtype
                df.loc[:, col] = pd.array(bool_values, dtype="boolean")
            else:
                # Convert None to NULL for strings
                df.loc[:, col] = df[col].astype(str).replace('None', None)
                
                # Handle MySQL zero dates and minimum dates in string columns
                if isinstance(df[col], pd.Series):
                    df.loc[:, col] = df[col].replace('0000-00-00 00:00:00', None)
                    df.loc[:, col] = df[col].replace('0000-00-00', None)
                    # Handle minimum dates (0001-01-01)
                    df.loc[:, col] = df[col].replace('0001-01-01 00:00:00', None)
                    df.loc[:, col] = df[col].replace('0001-01-01', None)
            
        # Handle binary data
        elif 'datetime' in str(df[col].dtype):
            # Ensure proper datetime format
            df.loc[:, col] = pd.to_datetime(df[col], errors='coerce')
            
            # Handle minimum date values in datetime columns
            min_date = pd.Timestamp('0001-01-01')
            df.loc[:, col] = df.loc[:, col].apply(lambda x: None if x == min_date else x)
            
        # Handle numeric conversions
        elif 'int' in str(df[col].dtype) or 'float' in str(df[col].dtype):
            # Handle NaN values
            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def get_postgres_type(pandas_dtype):
    """Map pandas dtype to PostgreSQL type."""
    dtype_str = str(pandas_dtype)
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE PRECISION'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        return 'TEXT'

def validate_schema(table_name: str, df: pd.DataFrame) -> bool:
    """Improved schema validation with better case handling."""
    try:
        safe_table = sanitize_identifier(table_name)
        
        # Check if the table exists first
        check_table_exists = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """)
        
        with pg_engine.connect() as conn:
            table_exists = conn.execute(
                check_table_exists.bindparams(table_name=str(safe_table))
            ).scalar()
        
        if not table_exists:
            # Table doesn't exist, create it first
            logger.info(f"Table {table_name} doesn't exist in PostgreSQL. Creating it first.")
            if not create_empty_target_table(table_name):
                logger.error(f"Failed to create table {table_name} in PostgreSQL.")
                return False
        
        # Create case-insensitive column mappings
        df_columns = {col.lower(): col for col in df.columns}
        
        # Get existing columns with proper case handling
        with pg_engine.connect() as conn:
            query = text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = :table_name
            """)
            result = conn.execute(query.bindparams(table_name=str(safe_table)))
            pg_columns = pd.DataFrame(result.fetchall(), columns=['column_name', 'data_type'])
        
        pg_column_map = {col.lower(): col for col in pg_columns['column_name']}
        
        # Handle missing columns in both directions
        source_cols = set(df_columns.keys())
        target_cols = set(pg_column_map.keys())
        
        missing_in_target = source_cols - target_cols
        missing_in_source = target_cols - source_cols
        
        if missing_in_source:
            logger.warning(
                f"Columns in target but not in source: {missing_in_source}. "
                "These will be set to NULL for new rows."
            )
        
        # Add missing columns to target
        if missing_in_target:
            with pg_engine.begin() as conn:
                for col_lower in missing_in_target:
                    orig_col = df_columns[col_lower]
                    safe_col = sanitize_identifier(orig_col)
                    col_type = get_postgres_type(df[orig_col].dtype)
                    
                    try:
                        # Handle PostgreSQL reserved keywords
                        alter_query = text(
                            "ALTER TABLE :table ADD COLUMN :column :type"
                        ).bindparams(
                            table=safe_table,
                            column=safe_col,
                            type=col_type
                        )
                        conn.execute(alter_query)
                        logger.info(f"Added column {safe_col} ({col_type})")
                    except SQLAlchemyError as e:
                        logger.warning(f"Could not add column {safe_col}: {str(e)}")
                        # Continue with other columns even if one fails
        
        return True
    except Exception as e:
        logger.error(f"Schema validation error for {table_name}: {str(e)}")
        return False

def map_type(mysql_type: str) -> str:
    """Map MySQL types to PostgreSQL types with improved error handling."""
    try:
        type_lower = str(mysql_type).lower()
        
        # Handle SET and ENUM types specially - convert to TEXT
        if type_lower.startswith('set(') or type_lower.startswith('enum('):
            return 'TEXT'
            
        # Integer types
        if 'tinyint(1)' in type_lower:
            return 'BOOLEAN'
        elif 'int' in type_lower:
            return 'INTEGER'
            
        # String types
        elif 'varchar' in type_lower or 'char' in type_lower:
            try:
                length = type_lower.split('(')[1].split(')')[0]
                return f'VARCHAR({length})'
            except (IndexError, ValueError):
                logger.warning(
                    f"Could not extract length for {mysql_type}, "
                    "defaulting to VARCHAR(255)"
                )
                return 'VARCHAR(255)'
                
        elif 'text' in type_lower:
            return 'TEXT'
            
        # Date/time types
        elif 'datetime' in type_lower or 'timestamp' in type_lower:
            return 'TIMESTAMP'
        elif 'date' in type_lower:
            return 'DATE'
        elif 'time' in type_lower:
            return 'TIME'
            
        # Numeric types
        elif 'decimal' in type_lower or 'numeric' in type_lower:
            try:
                precision = type_lower.split('(')[1].split(')')[0]
                return f'NUMERIC({precision})'
            except (IndexError, ValueError):
                logger.warning(
                    f"Could not extract precision for {mysql_type}, "
                    "defaulting to NUMERIC(10,2)"
                )
                return 'NUMERIC(10,2)'
                
        elif 'float' in type_lower:
            return 'REAL'
        elif 'double' in type_lower:
            return 'DOUBLE PRECISION'
            
        # Binary types
        elif 'blob' in type_lower or 'binary' in type_lower:
            return 'BYTEA'
            
        # Default
        else:
            logger.warning(f"Unknown type {mysql_type}, defaulting to TEXT")
            return 'TEXT'
            
    except Exception as e:
        logger.error(f"Error mapping type {mysql_type}: {str(e)}")
        return 'TEXT'  # Safe fallback

def is_numeric_type(mysql_type):
    """Check if MySQL type is numeric."""
    type_lower = mysql_type.lower()
    return any(t in type_lower for t in ['int', 'decimal', 'float', 'double', 'numeric'])

def create_empty_target_table(table_name: str) -> bool:
    """Create PostgreSQL table with proper type mappings and SQL injection protection."""
    try:
        safe_table = sanitize_identifier(table_name)
        
        # Get MariaDB table schema using parameterized query
        schema_query = text("""
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :db_name
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION;
        """)
        
        schema_df = pd.read_sql(
            schema_query, 
            mariadb_engine,
            params={
                'db_name': os.getenv('DBT_MYSQL_DATABASE'),
                'table_name': str(safe_table)
            }
        )
        
        if len(schema_df) == 0:
            logger.warning(f"No schema found for {safe_table}")
            return False
        
        # Build CREATE TABLE statement with proper sanitization
        create_sql_parts = [f"CREATE TABLE IF NOT EXISTS {safe_table} ("]
        primary_keys = []
        
        for _, row in schema_df.iterrows():
            try:
                col_name = sanitize_identifier(row['COLUMN_NAME'])
                col_type = map_type(row['COLUMN_TYPE'])
                nullable = "NULL" if row['IS_NULLABLE'] == 'YES' else "NOT NULL"
                
                # Handle default values properly
                default = ""
                if pd.notna(row['COLUMN_DEFAULT']):
                    # Handle timestamp default values - these often cause issues
                    if ('datetime' in str(row['COLUMN_TYPE']).lower() or 
                        'timestamp' in str(row['COLUMN_TYPE']).lower() or 
                        'date' in str(row['COLUMN_TYPE']).lower()):
                        # Use PostgreSQL standard default for dates
                        default = "DEFAULT '1970-01-01 00:00:00'::timestamp"
                    elif row['COLUMN_DEFAULT'] == 'CURRENT_TIMESTAMP':
                        default = "DEFAULT CURRENT_TIMESTAMP"
                    elif is_numeric_type(row['COLUMN_TYPE']):
                        default = f"DEFAULT {row['COLUMN_DEFAULT']}"
                    else:
                        # Handle other string defaults - remove extra quotes
                        clean_default = row['COLUMN_DEFAULT'].strip("'")
                        # Additional safety check to prevent syntax errors
                        if all(c.isprintable() for c in clean_default):
                            default = f"DEFAULT '{clean_default}'"
                        else:
                            # If non-printable characters, use empty string
                            default = "DEFAULT ''"
                            logger.warning(f"Non-printable characters in default value for {row['COLUMN_NAME']}, using empty string")
                
                # Handle auto increment
                if 'auto_increment' in str(row['EXTRA']).lower():
                    if 'int' in str(row['COLUMN_TYPE']).lower():
                        col_type = 'SERIAL'
                        nullable = ""  # SERIAL implies NOT NULL
                        default = ""   # SERIAL handles default
                
                # Track primary keys
                if row['COLUMN_KEY'] == 'PRI':
                    primary_keys.append(str(col_name))
                
                create_sql_parts.append(
                    f"    {col_name} {col_type} {nullable} {default},"
                )
                
            except ValueError as e:
                logger.error(f"Error processing column {row['COLUMN_NAME']}: {str(e)}")
                return False
        
        # Add primary key constraint
        if primary_keys:
            create_sql_parts.append(
                f"    PRIMARY KEY ({', '.join(primary_keys)}),"
            )
        
        # Finalize statement
        create_sql_parts[-1] = create_sql_parts[-1].rstrip(',')
        create_sql_parts.append(");")
        create_sql = "\n".join(create_sql_parts)
        
        # Execute creation
        with pg_engine.begin() as conn:
            conn.execute(text(create_sql))
            
        logger.info(f"Created table {safe_table} in PostgreSQL")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table {safe_table}: {str(e)}")
        return False

def fetch_table_names() -> List[str]:
    """Fetch all table names from MariaDB."""
    try:
        with mariadb_engine.connect() as maria_conn:
            result = maria_conn.execute(text("SHOW TABLES;"))
            tables = [row[0] for row in result]
            logger.info(f"[INFO] Found {len(tables)} tables in MariaDB")
            return tables
    except Exception as e:
        logger.error(f"Error fetching table names: {str(e)}")
        return []

def ensure_tracking_table_exists() -> bool:
    """Create and verify tracking table with proper transaction handling."""
    try:
        with pg_engine.begin() as conn:
            # Check if table exists
            exists_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'etl_sync_status'
                );
            """)
            table_exists = conn.execute(exists_query).scalar()
            
            if not table_exists:
                # Create tracking table
                create_query = text("""
                    CREATE TABLE etl_sync_status (
                        id SERIAL PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL,
                        last_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        sync_status VARCHAR(50),
                        rows_processed INTEGER,
                        UNIQUE(table_name)
                    );
                """)
                conn.execute(create_query)
                logger.info("Created ETL tracking table")
                
                # Get list of tables to track
                mariadb_tables = fetch_table_names()
                
                if not mariadb_tables:
                    logger.warning("No tables found in MariaDB to track")
                    return True
                
                # Insert tables in same transaction
                for table in mariadb_tables:
                    insert_query = text("""
                        INSERT INTO etl_sync_status 
                        (table_name, last_modified, sync_status, rows_processed)
                        VALUES (:table_name, '1970-01-01 00:00:00', 'pending', 0)
                        ON CONFLICT (table_name) DO NOTHING;
                    """)
                    conn.execute(insert_query.bindparams(table_name=table))
                    logger.debug(f"Added table {table} to tracking")
            
            return True
            
    except SQLAlchemyError as e:
        logger.error(f"Database error creating tracking table: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error creating tracking table: {str(e)}")
        return False

def fetch_last_sync(table_name: str) -> str:
    try:
        # First check if the tracking table exists
        with pg_engine.connect() as postgres_conn:
            check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'etl_sync_status'
            );
            """
            table_exists = postgres_conn.execute(text(check_query)).scalar()
            
            if not table_exists:
                # Create the table on-the-fly if missing
                logger.warning("ETL tracking table missing. Creating it now.")
                ensure_tracking_table_exists()
                return "1970-01-01 00:00:00"  # Default to full sync
            
            # Now try to get the sync timestamp
            query = """
            SELECT last_modified 
            FROM etl_sync_status 
            WHERE table_name = :table_name;
            """
            result = postgres_conn.execute(text(query).bindparams(table_name=table_name))
            last_sync = result.scalar()
            
            if last_sync is None:
                # Add the table to tracking if it's missing
                insert_query = """
                INSERT INTO etl_sync_status (table_name, last_modified, sync_status, rows_processed)
                VALUES (:table_name, '1970-01-01 00:00:00', 'pending', 0)
                ON CONFLICT (table_name) DO NOTHING;
                """
                postgres_conn.execute(text(insert_query).bindparams(table_name=table_name))
                return "1970-01-01 00:00:00"
                
            return last_sync
    except Exception as e:
        logger.error(f"Error fetching last sync for {table_name}: {str(e)}")
        return "1970-01-01 00:00:00"  # Default to full sync on error

def update_sync_timestamp(table_name: str, rows_processed: int = 0) -> bool:
    """Update the sync status for a table after successful sync."""
    try:
        with pg_engine.connect() as postgres_conn:
            query = """
            UPDATE etl_sync_status 
            SET last_modified = CURRENT_TIMESTAMP,
                sync_status = 'success',
                rows_processed = :rows_processed
            WHERE table_name = :table_name;
            """
            postgres_conn.execute(
                text(query).bindparams(
                    table_name=table_name,
                    rows_processed=rows_processed
                )
            )
            logger.info(f"Updated sync status for {table_name}")
            return True
    except Exception as e:
        logger.error(f"Error updating sync status for {table_name}: {str(e)}")
        return False

def update_sync_failure(table_name: str, error_message: str) -> bool:
    """Record a sync failure in the tracking table."""
    try:
        with pg_engine.connect() as postgres_conn:
            query = """
            UPDATE etl_sync_status 
            SET last_modified = CURRENT_TIMESTAMP,
                sync_status = 'failed',
                rows_processed = 0
            WHERE table_name = :table_name;
            """
            postgres_conn.execute(
                text(query).bindparams(
                    table_name=table_name
                )
            )
            logger.info(f"Recorded sync failure for {table_name}")
            return True
    except Exception as e:
        logger.error(f"Error recording sync failure for {table_name}: {str(e)}")
        return False

def sanitize_identifier(identifier: str) -> quoted_name:
    """
    Safely quote and sanitize SQL identifiers (table names, column names).
    Returns SQLAlchemy quoted_name object that handles escaping properly.
    """
    if not identifier or not isinstance(identifier, str):
        raise ValueError(f"Invalid identifier: {identifier}")
    
    # Remove any dangerous characters but preserve case
    clean_identifier = ''.join(c for c in identifier if c.isalnum() or c == '_')
    if not clean_identifier:
        raise ValueError(f"Invalid identifier after cleaning: {identifier}")
    
    # Return a properly quoted identifier
    return quoted_name(clean_identifier, quote=True)

def validate_env_vars() -> bool:
    """Validate all required environment variables are set."""
    required_vars = [
        'MARIADB_ROOT_USER',
        'MARIADB_ROOT_PASSWORD',
        'MARIADB_ROOT_HOST',
        'MARIADB_ROOT_PORT',
        'DBT_MYSQL_DATABASE',
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

class ETLConfig:
    """Configuration settings for ETL process."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLConfig, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance
    
    def initialize(self):
        """Initialize configuration with environment variables."""
        self.chunk_size = int(os.getenv('ETL_CHUNK_SIZE', '100000'))
        self.sub_chunk_size = int(os.getenv('ETL_SUB_CHUNK_SIZE', '10000'))
        self.max_retries = int(os.getenv('ETL_MAX_RETRIES', '3'))
        self.tolerance = float(os.getenv('ETL_TOLERANCE', '0.001'))
        self.small_table_threshold = int(os.getenv('ETL_SMALL_TABLE_THRESHOLD', '10000'))

def main(specific_tables=None):
    """Main ETL process with improved configuration and validation."""
    config = ETLConfig()  # Single initialization using singleton pattern
    
    # Validate environment variables
    if not validate_env_vars():
        return
    
    # Validate connections
    if not validate_connections():
        return

    # Ensure tracking table exists in PostgreSQL
    if not ensure_tracking_table_exists():
        logger.error("Failed to create/verify tracking table. Exiting.")
        return
    
    # First attempt
    tables = specific_tables if specific_tables else fetch_table_names()
    failed_tables = []
    results = {'success': [], 'failure': [], 'quality_issues': []}
    
    logger.info(f"Starting direct sync for {len(tables)} tables")
    
    # Add better progress tracking
    total = len(tables)
    logger.info(f"Starting sync for {total} tables")
    
    for i, table in enumerate(tables, 1):
        logger.info(f"Processing table {i}/{total} ({i/total*100:.1f}%): {table}")
        try:
            if sync_table_directly(table):
                if verify_data_quality(table):
                    results['success'].append(table)
                else:
                    results['quality_issues'].append(table)
                    failed_tables.append(table)
            else:
                results['failure'].append(table)
                failed_tables.append(table)
        except Exception as e:
            logger.error(f"Error syncing {table}: {str(e)}")
            results['failure'].append(table)
            failed_tables.append(table)
        
        # Log intermediate success rate every 10% progress
        if i % max(1, total // 10) == 0:
            current_success_rate = len(results['success']) / i * 100
            logger.info(f"Current success rate: {current_success_rate:.2f}%")
    
    # Retry mechanism
    if failed_tables:
        logger.info(f"Attempting to retry {len(failed_tables)} failed tables")
        
        for retry in range(1, config.max_retries + 1):
            if not failed_tables:
                break
                
            logger.info(f"Retry attempt {retry}/{config.max_retries}")
            still_failed = []
            
            for table in failed_tables:
                try:
                    logger.info(f"Retrying sync for {table}")
                    if sync_table_directly(table):
                        if verify_data_quality(table):
                            logger.info(f"✅ Successfully synced {table} on retry {retry}")
                            results['success'].append(table)
                            if table in results['failure']:
                                results['failure'].remove(table)
                            if table in results['quality_issues']:
                                results['quality_issues'].remove(table)
                        else:
                            still_failed.append(table)
                    else:
                        still_failed.append(table)
                except Exception as e:
                    logger.error(f"Error on retry {retry} for {table}: {str(e)}")
                    still_failed.append(table)
            
            failed_tables = still_failed
            
            if not failed_tables:
                logger.info("✅ All retries successful!")
                break

    # Close all database connections
    mariadb_engine.dispose()
    pg_engine.dispose()

    # Log summary
    logger.info("\n======== Execution Summary ========")
    logger.info(f"Successful tables: {len(results['success'])}")
    logger.info(f"Tables with quality issues: {len(results['quality_issues'])}")
    logger.info(f"Failed tables: {len(results['failure'])}")

    if results['quality_issues']:
        logger.warning("\n⚠️ Tables with Quality Issues:")
        for table in results['quality_issues']:
            logger.warning(f"- {table}")

    if results['failure']:
        logger.error("\n❌ Failed Tables:")
        for table in results['failure']:
            logger.error(f"- {table}")
            
    # Log success percentage
    total = len(tables)
    success_rate = len(results['success']) / total * 100 if total > 0 else 0
    logger.info(f"\nOverall success rate: {success_rate:.2f}% ({len(results['success'])}/{total})")

def sync_table_directly(table_name: str) -> bool:
    """
    Performs direct incremental sync of a table from MariaDB to PostgreSQL.
    Uses proper SQL sanitization and transaction management.
    """
    try:
        # Initialize config
        config = ETLConfig()  # Get singleton instance
        
        logger.info(f"Starting direct sync for table: {table_name}")
        
        # Get last sync timestamp
        last_sync = fetch_last_sync(table_name)
        
        # Check if the table has a last_modified column
        check_column_query = text("""
            SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = :db_name
            AND TABLE_NAME = :table_name 
            AND COLUMN_NAME = 'last_modified';
        """)
        
        with mariadb_engine.connect() as maria_conn:
            has_last_modified = maria_conn.execute(
                check_column_query.bindparams(
                    db_name=os.getenv('DBT_MYSQL_DATABASE'),
                    table_name=table_name
                )
            ).scalar() > 0
        
        # Check if table is empty
        count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")
        with mariadb_engine.connect() as maria_conn:
            row_count = maria_conn.execute(count_query).scalar()
            
        if row_count == 0:
            logger.info(f"Table {table_name} has no data in source. Creating empty table.")
            create_empty_target_table(table_name)
            update_sync_timestamp(table_name)
            return True
            
        # Get primary key for incremental sync
        pk_column = get_primary_key_safely(table_name)
        
        # Construct select query based on sync strategy
        safe_table = str(sanitize_identifier(table_name))
        if has_last_modified:
            # Use %s for MariaDB parameter binding instead of named parameters
            select_query = f"SELECT * FROM {safe_table} WHERE last_modified > %s"
            params = (last_sync,)
            logger.info(
                f"Performing incremental sync for {table_name} "
                f"using last_modified > {last_sync}"
            )
        else:
            if pk_column:
                # Check if target table exists and get max ID
                check_pg_table_query = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = :table_name
                    );
                """)
                
                with pg_engine.connect() as postgres_conn:
                    pg_table_exists = postgres_conn.execute(
                        check_pg_table_query.bindparams(table_name=table_name)
                    ).scalar()
                
                if pg_table_exists:
                    safe_pk = str(sanitize_identifier(pk_column))
                    # Ensure column name is properly quoted for PostgreSQL
                    max_id_query = text(f'SELECT MAX("{pk_column}") FROM {safe_table}')
                    with pg_engine.connect() as postgres_conn:
                        max_id = postgres_conn.execute(max_id_query).scalar() or 0
                    
                    # Use %s for MariaDB parameter binding instead of named parameters
                    select_query = f'SELECT * FROM {safe_table} WHERE {pk_column} > %s'
                    params = (max_id,)
                    logger.info(
                        f"Performing incremental sync for {table_name} "
                        f"using primary key {pk_column} > {max_id}"
                    )
                else:
                    select_query = f"SELECT * FROM {safe_table}"
                    params = {}
                    logger.info(f"Target table doesn't exist. Full sync for {table_name}")
            else:
                select_query = f"SELECT * FROM {safe_table}"
                params = {}
                logger.info(f"No incremental capability. Full sync for {table_name}")
        
        # Execute query with chunking
        total_rows = 0
        chunk_number = 0
        
        # Get DataFrame iterator
        df_iterator = pd.read_sql(
            select_query, 
            mariadb_engine, 
            params=params,
            chunksize=config.chunk_size
        )
        
        # Process each chunk
        for chunk_df in df_iterator:
            if not isinstance(chunk_df, pd.DataFrame):
                raise ValueError(f"Expected DataFrame but got {type(chunk_df)}")
                
            if len(chunk_df) == 0:
                continue
                
            # For first chunk, setup the operation type
            if_exists = "append"
            if chunk_number == 0:
                if not has_last_modified and not pk_column:
                    if_exists = "replace"
                    logger.info(f"Performing full replace for table {table_name}")
                
            rows_in_chunk = len(chunk_df)
            total_rows += rows_in_chunk
            
            # Process data types for PostgreSQL compatibility
            chunk_df = convert_data_types(chunk_df)
            
            # Validate schema before writing
            if chunk_number == 0:
                validate_schema(table_name, chunk_df)
            
            # Use proper SQLAlchemy connection for to_sql to avoid warnings
            with pg_engine.connect() as sqlalchemy_connection:
                # Start a transaction
                with sqlalchemy_connection.begin():
                    chunk_df.to_sql(
                        name=table_name,
                        con=sqlalchemy_connection,
                        if_exists=if_exists,
                        index=False,
                        chunksize=config.sub_chunk_size,
                        method='multi'  # Use multi-row insert to improve performance
                    )
            logger.info(f"Processed chunk {chunk_number + 1} ({rows_in_chunk} rows)")
            chunk_number += 1
        
        # Update sync status with row count
        update_sync_timestamp(table_name, total_rows)
        return True
        
    except SQLAlchemyError as e:
        error_message = str(e)
        logger.error(f"Database error syncing {table_name}: {error_message}")
        
        # If we have a date/time field overflow error, try to sync the table again
        if "DatetimeFieldOverflow" in error_message or "date/time field value out of range" in error_message:
            logger.info(f"Retrying sync for {table_name}")
            # The convert_data_types function will now handle zero dates better on next attempt
            try:
                return sync_table_directly(table_name)
            except Exception as retry_error:
                logger.error(f"Retry failed for {table_name}: {str(retry_error)}")
        
        update_sync_failure(table_name, error_message)
        return False
        
    except pd.io.sql.DatabaseError as e:
        error_message = str(e)
        logger.error(f"Pandas database error syncing {table_name}: {error_message}")
        
        # If we have a date/time field overflow error, try to sync the table again
        if "DatetimeFieldOverflow" in error_message or "date/time field value out of range" in error_message:
            logger.info(f"Retrying sync for {table_name}")
            # The convert_data_types function will now handle zero dates better on next attempt
            try:
                return sync_table_directly(table_name)
            except Exception as retry_error:
                logger.error(f"Retry failed for {table_name}: {str(retry_error)}")
        
        update_sync_failure(table_name, error_message)
        return False
        
    except ValueError as e:
        logger.error(f"Value error in sync process: {str(e)}")
        update_sync_failure(table_name, str(e))
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error syncing {table_name}: {str(e)}")
        update_sync_failure(table_name, str(e))
        return False

def verify_data_quality(table_name: str) -> bool:
    """
    Performs comprehensive data quality checks with proper SQL sanitization.
    """
    try:
        # Initialize config
        config = ETLConfig()  # Get singleton instance
        
        # Get sanitized table name
        safe_table = sanitize_identifier(table_name)
        
        # First check if target table exists in PostgreSQL
        check_table_exists_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """)
        
        with pg_engine.connect() as postgres_conn:
            table_exists = postgres_conn.execute(
                check_table_exists_query.bindparams(table_name=table_name)
            ).scalar()
            
        if not table_exists:
            logger.warning(f"Table {table_name} does not exist in PostgreSQL. Skipping data quality check.")
            return False
        
        # Check row counts in source and target - use different quoting styles
        # MariaDB uses backticks for identifiers
        maria_count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")
        # PostgreSQL uses double quotes for identifiers
        pg_count_query = text(f'SELECT COUNT(*) FROM "{table_name}"')
        
        with mariadb_engine.connect() as maria_conn:
            source_count = maria_conn.execute(maria_count_query).scalar()
        
        with pg_engine.connect() as postgres_conn:
            target_count = postgres_conn.execute(pg_count_query).scalar()
        
        logger.info(f"Data quality check for {table_name}:")
        logger.info(f"Source count: {source_count}, Target count: {target_count}")
        
        if source_count == 0:
            logger.info(f"Source table {table_name} is empty.")
            return True
            
        if target_count == 0:
            logger.warning(
                f"Warning: Target table {table_name} is empty but source has "
                f"{source_count} rows."
            )
            return False
            
        # For small tables, require exact match
        if source_count < config.small_table_threshold:
            if source_count != target_count:
                logger.error(
                    f"Row count mismatch for small table {table_name}: "
                    f"Source={source_count}, Target={target_count}. "
                    f"Small tables must match exactly."
                )
                return False
            logger.info(f"Small table {table_name} row counts match exactly")
        
        # For larger tables, use configured tolerance
        else:
            difference = abs(source_count - target_count)
            ratio = target_count / source_count if source_count > 0 else 0
            
            if ratio < (1 - config.tolerance):
                logger.error(
                    f"Row count mismatch for large table {table_name}: "
                    f"Source={source_count}, Target={target_count}, "
                    f"Difference={difference} rows ({(1-ratio)*100:.3f}% deviation). "
                    f"Maximum allowed deviation is {config.tolerance*100:.1f}%"
                )
                return False
            else:
                logger.info(
                    f"Large table {table_name} row counts within tolerance: "
                    f"Difference={difference} rows ({(1-ratio)*100:.3f}% deviation)"
                )

        # Check for null primary keys
        pk_query = text("""
            SELECT COLUMN_NAME 
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :db_name
            AND TABLE_NAME = :table_name
            AND CONSTRAINT_NAME = 'PRIMARY';
        """)
        
        with mariadb_engine.connect() as maria_conn:
            pk_result = maria_conn.execute(
                pk_query.bindparams(
                    db_name=os.getenv('DBT_MYSQL_DATABASE'),
                    table_name=table_name
                )
            )
            pk_column = pk_result.scalar()
        
        if pk_column:
            safe_pk = sanitize_identifier(pk_column)
            # Use proper quoting for each database
            null_check_query = text(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{pk_column}" IS NULL')
            
            with pg_engine.connect() as postgres_conn:
                null_pk_count = postgres_conn.execute(null_check_query).scalar()
                
            if null_pk_count > 0:
                logger.warning(
                    f"Warning: Found {null_pk_count} rows with NULL primary key "
                    f"in {table_name}"
                )
                return False
        
        logger.info(f"Data quality verification passed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error verifying data quality for {table_name}: {str(e)}")
        return False

def get_primary_key_safely(table_name: str) -> str:
    """
    Safely get the primary key for a table, handling potential errors.
    Returns None if no primary key is found or if an error occurs.
    """
    try:
        # Standard method using information_schema
        pk_query = text("""
            SELECT COLUMN_NAME 
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :db_name
            AND TABLE_NAME = :table_name
            AND CONSTRAINT_NAME = 'PRIMARY'
            LIMIT 1;
        """)
        
        with mariadb_engine.connect() as maria_conn:
            pk_result = maria_conn.execute(
                pk_query.bindparams(
                    db_name=os.getenv('DBT_MYSQL_DATABASE'),
                    table_name=table_name
                )
            )
            pk_column = pk_result.scalar()
            
        return pk_column
    except Exception as e:
        logger.warning(f"Could not get primary key for {table_name} using standard method: {str(e)}")
        
        # Fallback: try alternative method without using SHOW KEYS
        try:
            alt_pk_query = text("""
                SELECT c.COLUMN_NAME
                FROM information_schema.TABLE_CONSTRAINTS tc
                JOIN information_schema.KEY_COLUMN_USAGE c ON
                    c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                WHERE 
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_SCHEMA = :db_name
                    AND tc.TABLE_NAME = :table_name
                LIMIT 1;
            """)
            
            with mariadb_engine.connect() as maria_conn:
                pk_result = maria_conn.execute(
                    alt_pk_query.bindparams(
                        db_name=os.getenv('DBT_MYSQL_DATABASE'),
                        table_name=table_name
                    )
                )
                pk_column = pk_result.scalar()
                
            return pk_column
        except Exception as alt_e:
            logger.warning(f"Could not get primary key for {table_name} using alternative method: {str(alt_e)}")
            return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MariaDB to PostgreSQL ETL pipeline')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync (space-separated)')
    
    args = parser.parse_args()
    
    main(specific_tables=args.tables)