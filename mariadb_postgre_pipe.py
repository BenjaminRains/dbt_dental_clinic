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

# Modify the convert_data_types function to properly handle time columns in the apptview table

def convert_data_types(df: pd.DataFrame, table_name: str = None) -> pd.DataFrame:
    """Convert DataFrame types to be compatible with PostgreSQL."""
    df = df.copy()
    
    # Get PostgreSQL column types
    pg_column_types = {}
    try:
        with pg_engine.connect() as conn:
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
            # Handle TIME type specifically
            if target_type == 'time without time zone':
                def convert_to_time_string(val):
                    if pd.isna(val) or val == '' or val == 0:
                        return None
                    
                    try:
                        # Handle nanoseconds format
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
                
            # Boolean type
            elif target_type == 'boolean':
                bool_values = df[col].map(lambda x: bool(x) if pd.notnull(x) else None)
                df[col] = pd.Series(bool_values, index=df.index, dtype="boolean")
                
            # Integer types
            elif target_type in ('smallint', 'integer', 'bigint'):
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
                
            # Numeric/Decimal types
            elif target_type in ('numeric', 'decimal', 'real', 'double precision'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            # Timestamp/Date types
            elif target_type in ('timestamp', 'date'):
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
            # Text types (no conversion needed)
            elif target_type in ('text', 'varchar', 'char'):
                df[col] = df[col].astype(str).replace('nan', None)
                
        except Exception as e:
            logger.error(f"Error converting column {col} to {target_type}: {str(e)}")
            
    return df

def is_bool_column(column_name: str, column_type: str) -> bool:
    """Determine if a column should be treated as boolean based on name and type."""
    # Check for tinyint(1) which is typically boolean in MySQL/MariaDB
    if str(column_type).lower() == 'tinyint(1)':
        return True
        
    # Check for column names that typically indicate boolean values
    bool_patterns = [
        'is_', 'has_', 'show', 'use', 'allow', 'enable', 'disable',
        'visible', 'active', 'hidden', 'locked', 'no', 'can', 
        'is', 'has', 'flag', 'bool', 'boolean', 'timelocked'
    ]
    
    column_lower = column_name.lower()
    
    # Direct exact matches for known boolean column names
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


def get_postgres_type(pandas_dtype, column_name: str = '') -> str:
    """Map pandas dtype to PostgreSQL type."""
    dtype_str = str(pandas_dtype)
    if 'bool' in dtype_str:
        return 'BOOLEAN'
    elif column_name and is_bool_column(column_name, ''):  # Pass empty string as column_type since we don't have it here
        return 'BOOLEAN'
    elif 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE PRECISION'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def validate_schema(table_name: str, df: pd.DataFrame) -> bool:
    """Improved schema validation with better case handling."""
    try:
        # Check if the table exists first
        check_table_exists = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            );
        """)
        
        with pg_engine.connect() as conn:
            table_exists = conn.execute(
                check_table_exists.bindparams(table_name=table_name)
            ).scalar()
        
        if not table_exists:
            # Table doesn't exist, create it first
            logger.info(f"Table {table_name} doesn't exist in PostgreSQL. Creating it first.")
            if not create_empty_target_table(table_name):
                logger.error(f"Failed to create table {table_name} in PostgreSQL.")
                return False
            return True  # Return early since we've just created the table
        
        # Create case-insensitive column mappings
        df_columns = {col.lower(): col for col in df.columns}
        
        # Get existing columns with proper case handling
        with pg_engine.connect() as conn:
            query = text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = :table_name
            """)
            result = conn.execute(query.bindparams(table_name=table_name))
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
                    col_type = get_postgres_type(df[orig_col].dtype, orig_col)
                    
                    try:
                        # Handle PostgreSQL reserved keywords with proper quoting
                        alter_query = f'ALTER TABLE "{table_name}" ADD COLUMN "{orig_col}" {col_type}'
                        conn.execute(text(alter_query))
                        logger.info(f"Added column {orig_col} ({col_type})")
                    except SQLAlchemyError as e:
                        logger.warning(f"Could not add column {orig_col}: {str(e)}")
                        # Continue with other columns even if one fails
        
        return True
    except Exception as e:
        logger.error(f"Schema validation error for {table_name}: {str(e)}")
        return False


def map_type(mysql_type: str, column_name: str = '') -> str:
    """Map MySQL types to PostgreSQL types with more precise type mapping."""
    try:
        type_lower = str(mysql_type).lower()
        
        # Specific integer type mappings
        if type_lower == 'tinyint(1)':
            # Only tinyint(1) should be boolean (MySQL convention)
            return 'BOOLEAN'
        elif type_lower.startswith('tinyint'):
            return 'SMALLINT'  # PostgreSQL's smallest integer type
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
            
        # Default fallback
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

def create_secondary_indexes(table_name: str) -> bool:
    """Create all secondary indexes from MariaDB to PostgreSQL."""
    try:
        # Get indexes from MariaDB using a more reliable query
        index_query = text("""
            SELECT 
                INDEX_NAME as Key_name,
                COLUMN_NAME as Column_name,
                NON_UNIQUE,
                SEQ_IN_INDEX
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE TABLE_SCHEMA = :db_name 
            AND TABLE_NAME = :table_name 
            AND INDEX_NAME != 'PRIMARY'
            ORDER BY INDEX_NAME, SEQ_IN_INDEX;
        """)
        
        with mariadb_engine.connect() as maria_conn:
            result = maria_conn.execute(
                index_query.bindparams(
                    db_name=os.getenv('DBT_MYSQL_DATABASE'),
                    table_name=table_name
                )
            )
            indexes = result.fetchall()
        
        if not indexes:
            logger.info(f"No secondary indexes found for {table_name}")
            return True
            
        # Group indexes by key name
        index_groups = {}
        for idx in indexes:
            key_name = idx[0]  # Key_name
            column_name = idx[1]  # Column_name
            non_unique = idx[2]  # NON_UNIQUE flag
            
            if key_name not in index_groups:
                index_groups[key_name] = {
                    'columns': [],
                    'non_unique': non_unique
                }
                
            index_groups[key_name]['columns'].append(column_name)
        
        # Create each index
        with pg_engine.begin() as conn:
            for key_name, index_info in index_groups.items():
                # Use a naming convention for PostgreSQL indexes
                index_name = f"{table_name}_{key_name}"
                # Properly quote column names
                column_list = ", ".join([f'"{col}"' for col in index_info['columns']])
                
                # Determine if index should be unique
                unique_clause = "" if index_info['non_unique'] else "UNIQUE"
                
                # Determine index type
                if len(index_info['columns']) == 1 and ('date' in index_info['columns'][0].lower() or 'time' in index_info['columns'][0].lower()):
                    # Check if table is large (over 1 million rows)
                    count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")
                    with mariadb_engine.connect() as maria_conn:
                        row_count = maria_conn.execute(count_query).scalar()
                    
                    if row_count and row_count > 1000000:
                        # Use BRIN index for date columns on large tables
                        create_index_sql = f"""
                        CREATE {unique_clause} INDEX IF NOT EXISTS "{index_name}" 
                        ON "{table_name}" USING BRIN ({column_list});
                        """
                    else:
                        # Standard B-tree index
                        create_index_sql = f"""
                        CREATE {unique_clause} INDEX IF NOT EXISTS "{index_name}" 
                        ON "{table_name}" ({column_list});
                        """
                else:
                    # Standard B-tree index
                    create_index_sql = f"""
                    CREATE {unique_clause} INDEX IF NOT EXISTS "{index_name}" 
                    ON "{table_name}" ({column_list});
                    """
                
                try:
                    logger.info(f"Creating index with SQL: {create_index_sql}")
                    conn.execute(text(create_index_sql))
                    logger.info(f"Created index {key_name} on {table_name}")
                except Exception as idx_error:
                    logger.warning(f"Error creating index {key_name}: {str(idx_error)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating secondary indexes for {table_name}: {str(e)}")
        return False

# Fix for the create_empty_target_table function - specifically the default values handling for timestamps

def create_empty_target_table(table_name: str) -> bool:
    """Create PostgreSQL table with relaxed constraints for analytics."""
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
                'table_name': table_name  # Use original table name here
            }
        )
        
        if len(schema_df) == 0:
            logger.warning(f"No schema found for {table_name}")
            return False
        
        # Build CREATE TABLE statement with proper sanitization
        create_sql_parts = [f'CREATE TABLE IF NOT EXISTS "{table_name}" (']
        primary_keys = []
        
        for _, row in schema_df.iterrows():
            try:
                col_name = row['COLUMN_NAME']  # Keep original column name
                col_type = map_type(row['COLUMN_TYPE'], col_name)
                
                # For analytics, we might want to relax NOT NULL constraints
                # to avoid data loading issues:
                if row['COLUMN_KEY'] == 'PRI':
                    # Primary keys must be NOT NULL
                    nullable = "NOT NULL"
                elif 'auto_increment' in str(row['EXTRA']).lower():
                    # Auto-increment columns should also be NOT NULL
                    nullable = "NOT NULL"
                else:
                    # For analytics purposes, make all other columns nullable
                    nullable = "NULL"
                
                # Handle default values properly
                default = ""
                if pd.notna(row['COLUMN_DEFAULT']):
                    # Handle boolean defaults
                    if col_type == 'BOOLEAN':
                        # Handle default values for boolean columns
                        if row['COLUMN_DEFAULT'] in ('1', 1, 'TRUE', 'true', 'True', 'Y', 'y', 'Yes'):
                            default = "DEFAULT TRUE"
                        else:
                            default = "DEFAULT FALSE"
                    # Handle DATE column type specifically
                    elif col_type == 'DATE':
                        # For DATE columns, add a more robust default value handling
                        if row['COLUMN_DEFAULT'] == '' or row['COLUMN_DEFAULT'] == "''" or row['COLUMN_DEFAULT'] is None:
                            default = "DEFAULT '0001-01-01'::date"
                        elif row['COLUMN_DEFAULT'] == '0001-01-01' or row['COLUMN_DEFAULT'] == "'0001-01-01'":
                            default = "DEFAULT '0001-01-01'::date"
                        else:
                            clean_default = str(row['COLUMN_DEFAULT']).strip("'")
                            default = f"DEFAULT '{clean_default}'::date"
                    # Handle timestamp default values
                    elif ('datetime' in str(row['COLUMN_TYPE']).lower() or 
                        'timestamp' in str(row['COLUMN_TYPE']).lower()):
                        if 'current_timestamp' in str(row['COLUMN_DEFAULT']).lower() or row['COLUMN_DEFAULT'] == 'CURRENT_TIMESTAMP':
                            default = "DEFAULT CURRENT_TIMESTAMP"
                        elif row['COLUMN_DEFAULT'] == '0001-01-01 00:00:00' or pd.isna(row['COLUMN_DEFAULT']):
                            default = "DEFAULT '0001-01-01 00:00:00'::timestamp"
                        else:
                            clean_default = str(row['COLUMN_DEFAULT']).strip("'")
                            default = f"DEFAULT '{clean_default}'::timestamp"
                    elif 'date' in str(row['COLUMN_TYPE']).lower() and col_type != 'DATE':
                        if row['COLUMN_DEFAULT'] == '0001-01-01' or pd.isna(row['COLUMN_DEFAULT']):
                            default = "DEFAULT '0001-01-01'::date"
                        else:
                            clean_default = str(row['COLUMN_DEFAULT']).strip("'")
                            default = f"DEFAULT '{clean_default}'::date"
                    elif is_numeric_type(row['COLUMN_TYPE']):
                        default = f"DEFAULT {row['COLUMN_DEFAULT']}"
                    else:
                        # Handle other string defaults - remove extra quotes
                        clean_default = str(row['COLUMN_DEFAULT']).strip("'")
                        # Additional safety check to prevent syntax errors
                        if all(c.isprintable() for c in clean_default):
                            default = f"DEFAULT '{clean_default}'"
                        else:
                            # If non-printable characters, use empty string
                            default = "DEFAULT ''"
                            logger.warning(f"Non-printable characters in default value for {row['COLUMN_NAME']}, using empty string")
                elif col_type == 'BOOLEAN':
                    # Provide a safe default for boolean columns even when there's no default in the source
                    default = "DEFAULT FALSE"
                
                # Handle auto increment - IMPORTANT FIX HERE
                auto_increment = False
                if 'auto_increment' in str(row['EXTRA']).lower():
                    if 'int' in str(row['COLUMN_TYPE']).lower():
                        col_type = 'SERIAL'
                        nullable = ""  # SERIAL implies NOT NULL
                        default = ""   # SERIAL handles default 
                        auto_increment = True
                
                # Track primary keys
                if row['COLUMN_KEY'] == 'PRI':
                    primary_keys.append(f'"{col_name}"')
                    
                    # Make sure primary key is NOT NULL even if auto_increment is handled separately
                    if not auto_increment and nullable != "NOT NULL":
                        nullable = "NOT NULL"
                
                # Special handling for TIME columns
                if col_type == 'TIME':
                    # For TIME columns, we want to preserve the NULL default behavior
                    # from MariaDB exactly as it is
                    if row['COLUMN_DEFAULT'] is None or pd.isna(row['COLUMN_DEFAULT']) or str(row['COLUMN_DEFAULT']).lower() == 'null':
                        default = "DEFAULT NULL"  # Explicitly set DEFAULT NULL
                    else:
                        clean_default = str(row['COLUMN_DEFAULT']).strip("'")
                        default = f"DEFAULT '{clean_default}'"
                
                create_sql_parts.append(
                    f'    "{col_name}" {col_type} {nullable} {default},'
                )
                
            except ValueError as e:
                logger.error(f"Error processing column {row['COLUMN_NAME']}: {str(e)}")
                return False
        
        # Add primary key constraint
        if primary_keys:
            primary_key_line = f"    PRIMARY KEY ({', '.join(primary_keys)})"
            create_sql_parts.append(primary_key_line)
        else:
            # Remove trailing comma from the last column definition
            create_sql_parts[-1] = create_sql_parts[-1].rstrip(',')
        
        # Finalize statement
        create_sql_parts.append(");")
        create_sql = "\n".join(create_sql_parts)
        
        logger.info(f"SQL for table creation: \n{create_sql}")
        
        # Drop the table first if it exists
        drop_sql = f'DROP TABLE IF EXISTS "{table_name}";'
        
        # Execute creation
        with pg_engine.begin() as conn:
            conn.execute(text(drop_sql))
            conn.execute(text(create_sql))
            
        # Now we need to create the secondary indexes
        create_indexes_result = create_secondary_indexes(table_name)
        if create_indexes_result:
            logger.info(f"Created table {table_name} with indexes in PostgreSQL")
        else:
            logger.warning(f"Created table {table_name} but failed to create some indexes")
            
        return True
        
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {str(e)}")
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
    
    # Instead of filtering out characters, preserve the original identifier
    # but wrapped in SQLAlchemy's quoted_name to ensure proper quoting
    return quoted_name(identifier, quote=True)

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
    
    # Force max_retries to 0 to eliminate retries completely
    config.max_retries = 0
    
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
    
    # Process tables
    tables = specific_tables if specific_tables else fetch_table_names()
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
            else:
                results['failure'].append(table)
        except Exception as e:
            logger.error(f"Error syncing {table}: {str(e)}")
            results['failure'].append(table)
        
        # Log intermediate success rate every 10% progress
        if i % max(1, total // 10) == 0:
            current_success_rate = len(results['success']) / i * 100
            logger.info(f"Current success rate: {current_success_rate:.2f}%")
    
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
        logger.error("\nFAILED Tables:")
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
        
        # IMPORTANT: Force table recreation for analytics
        logger.info(f"Analytics mode: Forcing recreation of table {table_name}")
        if not create_empty_target_table(table_name):
            logger.error(f"Failed to create table structure for {table_name}")
            return False
        
        # Check if table is empty
        count_query = text(f"SELECT COUNT(*) FROM `{table_name}`")
        with mariadb_engine.connect() as maria_conn:
            row_count = maria_conn.execute(count_query).scalar()
            
        if row_count == 0:
            logger.info(f"Table {table_name} has no data in source. Empty table created.")
            update_sync_timestamp(table_name)
            return True
            
        # For analytics database, always do a full sync
        select_query = f"SELECT * FROM `{table_name}`"
        params = {}
        logger.info(f"Analytics mode: Performing full sync for {table_name}")
        
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
                
            # Always use append mode since we created an empty table
            if_exists = "append"
            
            rows_in_chunk = len(chunk_df)
            total_rows += rows_in_chunk
            
            # Add special handling for date columns with empty strings
            for column in chunk_df.columns:
                if 'date' in column.lower():
                    # Replace empty strings with NULL for date columns
                    chunk_df.loc[chunk_df[column] == '', column] = None
                    # For columns that should never be NULL, use a default date
                    if column == 'DateAdverseReaction':
                        chunk_df.loc[chunk_df[column].isnull(), column] = '0001-01-01'
            
            # Process data types for PostgreSQL compatibility
            chunk_df = convert_data_types(chunk_df, table_name)
            
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
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error syncing {table_name}: {error_message}")
        update_sync_failure(table_name, error_message)
        return False
    

def verify_data_quality(table_name: str) -> bool:
    """
    Performs comprehensive data quality checks with proper SQL sanitization.
    """
    try:
        # Initialize config
        config = ETLConfig()  # Get singleton instance
        
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

def get_not_null_columns(table_name: str) -> List[str]:
    """Get columns with NOT NULL constraints in the target schema."""
    try:
        # Get schema from MariaDB
        schema_query = text("""
            SELECT COLUMN_NAME, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :db_name
            AND TABLE_NAME = :table_name
            AND IS_NULLABLE = 'NO';
        """)
        
        schema_df = pd.read_sql(
            schema_query, 
            mariadb_engine,
            params={
                'db_name': os.getenv('DBT_MYSQL_DATABASE'),
                'table_name': table_name
            }
        )
        
        return schema_df['COLUMN_NAME'].tolist() if not schema_df.empty else []
    except Exception as e:
        logger.error(f"Error getting NOT NULL columns for {table_name}: {str(e)}")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MariaDB to PostgreSQL ETL pipeline')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync (space-separated)')
    
    args = parser.parse_args()
    
    main(specific_tables=args.tables)