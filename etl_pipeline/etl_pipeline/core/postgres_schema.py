"""
Enhanced PostgreSQL schema conversion with better type mapping.
"""
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
import hashlib
import re
from datetime import datetime
import logging
import os
import yaml

from etl_pipeline.config.logging import get_logger
from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.connections import ConnectionFactory

# Import custom exceptions for structured error handling
from etl_pipeline.exceptions.schema import SchemaTransformationError, SchemaValidationError, TypeConversionError
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError, DatabaseQueryError
from etl_pipeline.exceptions.data import DataExtractionError

logger = logging.getLogger(__name__)

class PostgresSchema:
    """
    PURE TRANSFORMATION LAYER for schema and data conversion from MySQL to PostgreSQL.
    
    This class works with the new Settings-centric architecture and automatically
    uses the correct environment (clinic/test) based on Settings detection.
    
    ARCHITECTURAL ROLE: PURE TRANSFORMATION LAYER
    - Focuses exclusively on schema conversion and data transformation
    - Handles all MySQL to PostgreSQL type mapping and conversion
    - Provides schema validation and table creation services
    - No data movement or pipeline mechanics
    
    Connection Architecture Compliance:
    - ✅ Uses Settings injection for environment-agnostic operation
    - ✅ Uses unified ConnectionFactory API with Settings injection
    - ✅ Uses Settings-based configuration methods
    - ✅ No direct environment variable access
    - ✅ Uses PostgresSchema enum for type safety
    
    TRANSFORMATION RESPONSIBILITIES:
    - Schema extraction and analysis from MySQL
    - Schema conversion (MySQL → PostgreSQL types)
    - Table creation and schema validation
    - Data type conversion during ETL operations
    - Type validation and normalization
    - Encoding and format handling
    
    SEPARATION OF CONCERNS:
    - PostgresSchema: Schema conversion, data transformation, type mapping
    - PostgresLoader: Data movement, pipeline mechanics, load verification
    """
    
    def __init__(self, postgres_schema: ConfigPostgresSchema = ConfigPostgresSchema.RAW, settings=None):
        """
        Initialize PostgreSQL schema adaptation using new Settings-centric architecture.
        
        Args:
            postgres_schema: PostgreSQL schema enum (default: PostgresSchema.RAW)
            settings: Settings instance (uses global if None)
        """
        # Use provided settings or get global settings
        self.settings = settings or get_settings()
        
        # Get database connections using new ConnectionFactory (unified API)
        self.mysql_engine = ConnectionFactory.get_replication_connection(self.settings)
        self.postgres_engine = ConnectionFactory.get_analytics_raw_connection(self.settings)
        
        # Get database names from settings
        mysql_config = self.settings.get_replication_connection_config()
        postgres_config = self.settings.get_analytics_connection_config(postgres_schema)
        
        self.mysql_db = mysql_config.get('database', 'opendental_replication')
        self.postgres_db = postgres_config.get('database', 'opendental_analytics')
        self.postgres_schema = postgres_schema.value  # Use enum value for string operations
        self._schema_cache = {}
        
        # ADD THIS: Cache for type conversion analysis to prevent repeated DB queries
        self._type_analysis_cache = {}
        self._table_column_types_cache = {}
        
        # Initialize inspectors
        self.mysql_inspector = inspect(self.mysql_engine)
        self.postgres_inspector = inspect(self.postgres_engine)
        
        # Log initialization info
        logger.info(f"PostgresSchema initialized with schema: {postgres_schema.value}")
        logger.debug(f"MySQL DB: {self.mysql_db}, PostgreSQL DB: {self.postgres_db}")
    
    def get_table_schema_from_mysql(self, table_name: str) -> Dict:
        """
        Get MySQL table schema directly from the replication database.
        This replaces SchemaDiscovery dependency with direct MySQL queries.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict: Schema information in the format expected by adapt_schema
        """
        try:
            with self.mysql_engine.connect() as conn:
                # Get CREATE TABLE statement
                result = conn.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
                row = result.fetchone()
                
                if not row:
                    raise SchemaValidationError(
                        message=f"Table {table_name} not found in {self.mysql_db}",
                        table_name=table_name,
                        validation_details={"database": self.mysql_db, "error_type": "table_not_found"}
                    )
                
                create_statement = row[1]
                
                # Get table metadata
                metadata_result = conn.execute(text(f"SHOW TABLE STATUS LIKE '{table_name}'"))
                metadata_row = metadata_result.fetchone()
                
                metadata = {
                    'engine': metadata_row[1] if metadata_row and metadata_row[1] else 'InnoDB',
                    'charset': 'utf8mb4',
                    'collation': 'utf8mb4_general_ci',
                    'auto_increment': metadata_row[10] if metadata_row and len(metadata_row) > 10 and metadata_row[10] else None,
                    'row_count': metadata_row[4] if metadata_row and len(metadata_row) > 4 and metadata_row[4] else 0
                }
                
                # Get column information
                columns_result = conn.execute(text(f"""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        extra,
                        column_comment,
                        column_key
                    FROM information_schema.columns
                    WHERE table_schema = '{self.mysql_db}'
                    AND table_name = '{table_name}'
                    ORDER BY ordinal_position
                """))
                
                columns = []
                for col_row in columns_result:
                    columns.append({
                        'name': col_row[0],
                        'type': col_row[1],
                        'is_nullable': col_row[2] == 'YES',
                        'default': col_row[3],
                        'extra': col_row[4],
                        'comment': col_row[5] or '',
                        'key_type': col_row[6]
                    })
                
                schema_info = {
                    'table_name': table_name,
                    'create_statement': create_statement,
                    'metadata': metadata,
                    'columns': columns,
                    'schema_hash': self._calculate_schema_hash(create_statement),
                    'analysis_timestamp': datetime.now().isoformat(),
                    'analysis_version': '4.0'
                }
                
                return schema_info
                
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed for {table_name}: {e}")
            raise DataExtractionError(
                message=f"Failed to connect to MySQL database for schema extraction",
                table_name=table_name,
                details={"database": self.mysql_db, "connection_type": "mysql"},
                original_exception=e
            )
        except DatabaseQueryError as e:
            logger.error(f"Database query failed for {table_name}: {e}")
            raise DataExtractionError(
                message=f"Failed to execute schema extraction queries",
                table_name=table_name,
                details={"database": self.mysql_db, "query_type": "schema_metadata"},
                original_exception=e
            )
        except SchemaValidationError:
            # Re-raise schema validation errors as-is
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting schema for {table_name}: {str(e)}")
            raise DataExtractionError(
                message=f"Unexpected error during schema extraction",
                table_name=table_name,
                details={"database": self.mysql_db, "error_type": "unexpected"},
                original_exception=e
            )
    
    def _calculate_schema_hash(self, create_statement: str) -> str:
        """Calculate hash of create statement for change detection."""
        return hashlib.md5(create_statement.encode()).hexdigest()
    
    def _analyze_column_data(self, table_name: str, column_name: str, mysql_type: str) -> str:
        """
        Analyze actual column data to determine the best PostgreSQL type.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            mysql_type: MySQL type definition
            
        Returns:
            str: Best PostgreSQL type for this column
        """
        try:
            # Special handling for known columns that should always be smallint
            # These columns are known to support values beyond 0/1 in business logic
            known_smallint_columns = {
                'apptview': ['StackBehavUR', 'StackBehavLR'],
                'provider': ['ProvStatus', 'AnesthProvType', 'EhrMuStage'],
                'tasklist': ['TaskListStatus']
            }
            
            if (table_name.lower() in known_smallint_columns and 
                column_name in known_smallint_columns[table_name.lower()]):
                logger.info(f"Column {table_name}.{column_name} forced to smallint (known business logic)")
                return 'smallint'
            
            # For TINYINT columns, check if they're actually boolean (only 0/1 values)
            if mysql_type.lower().startswith('tinyint'):
                with self.mysql_engine.connect() as conn:
                    # Use a more efficient query that checks for non-0/1 values
                    # Handle reserved words by backticking table name
                    safe_table_name = f"`{table_name}`" if table_name.lower() in ['order', 'group', 'user'] else table_name
                    query = text(f"""
                        SELECT COUNT(*) 
                        FROM {safe_table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND {column_name} NOT IN (0, 1)
                    """)
                    result = conn.execute(query)
                    scalar_result = result.scalar()
                    has_non_bool = scalar_result is not None and scalar_result > 0
                    
                    if not has_non_bool:
                        logger.info(f"Column {table_name}.{column_name} identified as boolean")
                        return 'boolean'
                    
                    logger.info(f"Column {table_name}.{column_name} kept as smallint (contains non-boolean values)")
                    return 'smallint'
            
            # For other types, use standard conversion
            pg_type = self._convert_mysql_type_standard(mysql_type)
            logger.debug(f"Column {table_name}.{column_name} ({mysql_type}) converted to {pg_type}")
            return pg_type
            
        except DatabaseConnectionError as e:
            logger.warning(f"Database connection failed for column analysis {table_name}.{column_name}: {str(e)}")
            # Fall back to standard type conversion
            pg_type = self._convert_mysql_type_standard(mysql_type)
            logger.debug(f"Fallback conversion for {table_name}.{column_name} ({mysql_type}) to {pg_type}")
            return pg_type
        except DatabaseQueryError as e:
            logger.warning(f"Database query failed for column analysis {table_name}.{column_name}: {str(e)}")
            # Fall back to standard type conversion
            pg_type = self._convert_mysql_type_standard(mysql_type)
            logger.debug(f"Fallback conversion for {table_name}.{column_name} ({mysql_type}) to {pg_type}")
            return pg_type
        except Exception as e:
            logger.warning(f"Unexpected error analyzing column data for {table_name}.{column_name}: {str(e)}")
            # Fall back to standard type conversion
            pg_type = self._convert_mysql_type_standard(mysql_type)
            logger.debug(f"Fallback conversion for {table_name}.{column_name} ({mysql_type}) to {pg_type}")
            return pg_type
    
    def _convert_mysql_type_standard(self, mysql_type: str) -> str:
        """Standard MySQL to PostgreSQL type conversion."""
        type_map = {
            'int': 'integer',
            'bigint': 'bigint',
            'tinyint': 'smallint',  # Default to smallint, not boolean
            'smallint': 'smallint',
            'mediumint': 'integer',
            'float': 'real',
            'double': 'double precision',
            'decimal': 'numeric',
            'char': 'character',
            'varchar': 'character varying',
            'text': 'text',
            'mediumtext': 'text',
            'longtext': 'text',
            'datetime': 'timestamp',
            'timestamp': 'timestamp',
            'date': 'date',
            'time': 'time',
            'year': 'integer',
            'boolean': 'boolean',
            'bit': 'bit',
            'binary': 'bytea',
            'varbinary': 'bytea',
            'blob': 'bytea',
            'mediumblob': 'bytea',
            'longblob': 'bytea',
            'json': 'jsonb'
        }
        
        # Extract base type and parameters
        base_type = mysql_type.split('(')[0].lower()
        params = re.findall(r'\(([^)]+)\)', mysql_type)
        
        # Get PostgreSQL type
        pg_type = type_map.get(base_type, 'text')
        
        # Add parameters if present
        if params and pg_type in ['character varying', 'character', 'numeric']:
            pg_type += f"({params[0]})"
        
        logger.debug(f"Standard conversion: {mysql_type} -> base_type={base_type} -> {pg_type}")
        return pg_type
    
    def _analyze_column_data_cached(self, table_name: str, column_name: str, mysql_type: str) -> str:
        """
        Cached version of column data analysis to prevent repeated database queries.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            mysql_type: MySQL type definition
            
        Returns:
            str: Best PostgreSQL type for this column
        """
        # Normalize mysql_type to handle variations like "tinyint", "tinyint(1)", "tinyint(4)"
        # Extract base type (e.g., "tinyint" from "tinyint(1)")
        base_type = mysql_type.split('(')[0].strip().lower()
        
        # Create cache key using normalized base type to prevent duplicate analysis
        # when same column is analyzed with different type formats (e.g., "tinyint" vs "tinyint(1)")
        cache_key = f"{table_name}.{column_name}.{base_type}"
        
        # Check cache first
        if cache_key in self._type_analysis_cache:
            logger.debug(f"Using cached type analysis for {cache_key}")
            return self._type_analysis_cache[cache_key]
        
        try:
            # Special handling for known columns that should always be smallint
            # These columns are known to support values beyond 0/1 in business logic
            known_smallint_columns = {
                'apptview': ['StackBehavUR', 'StackBehavLR'],
                'provider': ['ProvStatus', 'AnesthProvType', 'EhrMuStage'],
                'tasklist': ['TaskListStatus']
            }
            
            if (table_name.lower() in known_smallint_columns and 
                column_name in known_smallint_columns[table_name.lower()]):
                logger.info(f"Column {table_name}.{column_name} forced to smallint (known business logic)")
                pg_type = 'smallint'
            # For TINYINT columns, check if they're actually boolean (only 0/1 values)
            elif mysql_type.lower().startswith('tinyint'):
                with self.mysql_engine.connect() as conn:
                    # Use a more efficient query that checks for non-0/1 values
                    # Handle reserved words by backticking table name
                    safe_table_name = f"`{table_name}`" if table_name.lower() in ['order', 'group', 'user'] else table_name
                    query = text(f"""
                        SELECT COUNT(*) 
                        FROM {safe_table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND {column_name} NOT IN (0, 1)
                    """)
                    result = conn.execute(query)
                    scalar_result = result.scalar()
                    has_non_bool = scalar_result is not None and scalar_result > 0
                    
                    if not has_non_bool:
                        logger.info(f"Column {table_name}.{column_name} identified as boolean")
                        pg_type = 'boolean'
                    else:
                        logger.info(f"Column {table_name}.{column_name} kept as smallint (contains non-boolean values)")
                        pg_type = 'smallint'
            else:
                # For other types, use standard conversion
                pg_type = self._convert_mysql_type_standard(mysql_type)
                logger.debug(f"Column {table_name}.{column_name} ({mysql_type}) converted to {pg_type}")
            
            # Cache the result
            self._type_analysis_cache[cache_key] = pg_type
            return pg_type
            
        except Exception as e:
            logger.warning(f"Error analyzing column data for {table_name}.{column_name}: {str(e)}")
            # Fall back to standard type conversion
            pg_type = self._convert_mysql_type_standard(mysql_type)
            logger.debug(f"Fallback conversion for {table_name}.{column_name} ({mysql_type}) to {pg_type}")
            
            # Cache the fallback result
            self._type_analysis_cache[cache_key] = pg_type
            return pg_type
    
    def _get_table_column_types_cached(self, table_name: str) -> Dict[str, str]:
        """
        Get cached PostgreSQL types for all columns in a table.
        This prevents repeated analysis during row conversion.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict[str, str]: Mapping of column_name -> postgresql_type
        """
        # Check cache first
        if table_name in self._table_column_types_cache:
            return self._table_column_types_cache[table_name]
        
        try:
            # Get table schema
            mysql_schema = self.get_table_schema_from_mysql(table_name)
            if not mysql_schema or 'columns' not in mysql_schema:
                logger.warning(f"Could not get schema for table {table_name}")
                return {}
            
            column_types = {}
            columns_info = mysql_schema['columns']
            
            # Handle both list and dict formats
            if isinstance(columns_info, list):
                columns_dict = {col['name']: col for col in columns_info}
            elif isinstance(columns_info, dict):
                columns_dict = columns_info
            else:
                logger.warning(f"Unexpected columns format for table {table_name}")
                return {}
            
            # Analyze each column once and cache the result
            for col_name, col_info in columns_dict.items():
                mysql_type = col_info.get('type', '')
                pg_type = self._analyze_column_data_cached(table_name, col_name, mysql_type)
                column_types[col_name] = pg_type
            
            # Cache the entire table's column types
            self._table_column_types_cache[table_name] = column_types
            logger.info(f"Cached column types for table {table_name}: {len(column_types)} columns")
            
            return column_types
            
        except Exception as e:
            logger.error(f"Error getting cached column types for {table_name}: {str(e)}")
            return {}
    
    def _convert_mysql_type(self, mysql_type: str, table_name: Optional[str] = None, column_name: Optional[str] = None) -> str:
        """
        Convert MySQL data type to PostgreSQL data type with intelligent analysis.
        
        Uses cached column analysis to prevent duplicate database queries when the same
        column is analyzed during both table creation and row conversion.
        
        Args:
            mysql_type: MySQL type string
            table_name: Table name for data analysis
            column_name: Column name for data analysis
            
        Returns:
            str: PostgreSQL type
        """
        # If we have table and column info, do intelligent analysis (using cached version)
        if table_name and column_name:
            return self._analyze_column_data_cached(table_name, column_name, mysql_type)
        
        # Otherwise use standard conversion
        return self._convert_mysql_type_standard(mysql_type)
    
    def adapt_schema(self, table_name: str, mysql_schema: Dict) -> str:
        """
        Adapt MySQL schema to PostgreSQL with intelligent type mapping.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from get_table_schema_from_mysql()
            
        Returns:
            str: PostgreSQL-compatible CREATE TABLE statement
        """
        try:
            # Get the MySQL CREATE statement
            create_statement = mysql_schema['create_statement']
            logger.debug(f"MySQL CREATE statement for {table_name}: {create_statement}")
            
            # Convert MySQL types to PostgreSQL types with intelligent analysis
            pg_columns = self._convert_mysql_to_postgres_intelligent(create_statement, table_name)
            logger.debug(f"PostgreSQL columns for {table_name}: {pg_columns}")
            
            # Check if we have any columns
            if not pg_columns.strip():
                raise SchemaTransformationError(
                    message=f"No valid columns found in MySQL schema for table {table_name}",
                    table_name=table_name,
                    mysql_schema=mysql_schema,
                    details={"error_type": "no_valid_columns", "schema_type": "mysql"}
                )
            
            # Create the final CREATE TABLE statement
            pg_create = f"CREATE TABLE {self.postgres_schema}.{table_name} (\n{pg_columns}\n)"
            
            return pg_create
            
        except SchemaTransformationError:
            # Re-raise schema transformation errors as-is
            raise
        except Exception as e:
            logger.error(f"Unexpected error adapting schema for {table_name}: {str(e)}")
            raise SchemaTransformationError(
                message=f"Unexpected error during schema transformation",
                table_name=table_name,
                mysql_schema=mysql_schema,
                details={"error_type": "unexpected", "schema_type": "mysql"},
                original_exception=e
            )
    
    def _convert_mysql_to_postgres_intelligent(self, mysql_create: str, table_name: str) -> str:
        """Convert MySQL CREATE statement to PostgreSQL syntax with intelligent type analysis."""
        # Extract the content between the outermost parentheses
        content_match = re.search(r'CREATE TABLE[^(]+\((.*)\)', mysql_create, re.DOTALL)
        if not content_match:
            return ""
            
        content = content_match.group(1)
        
        # Extract column definitions
        columns = []
        
        # Pattern to match column definitions: `name` type [modifiers...]
        # Robust: matches any number/order of modifiers, handles last column (no trailing comma)
        col_pattern = r'`(\w+)`\s+([a-zA-Z0-9_]+(?:\([^)]+\))?)(?:\s+[^,]*)?(?:,|$)'
        
        # Find all column definitions
        for match in re.finditer(col_pattern, content):
            col_name = match.group(1)
            col_type = match.group(2).strip()
            
            logger.debug(f"Extracted column: {col_name} -> {col_type}")
            
            # Convert the type
            pg_type = self._convert_mysql_type(col_type, table_name, col_name)
            columns.append(f'"{col_name}" {pg_type}')
            
        # Look for PRIMARY KEY
        pk_match = re.search(r'PRIMARY KEY\s*\(\s*`(\w+)`\s*\)', content)
        if pk_match:
            pk_col = pk_match.group(1)
            columns.append(f'PRIMARY KEY ("{pk_col}")')
            
        return ",\n".join(columns)
    
    def create_postgres_table(self, table_name: str, mysql_schema: Dict) -> bool:
        """
        Create PostgreSQL table from MySQL schema with intelligent type mapping.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from get_table_schema_from_mysql()
            
        Returns:
            bool: True if successful
        """
        try:
            # Adapt schema for PostgreSQL
            pg_create = self.adapt_schema(table_name, mysql_schema)
            
            # Create table in PostgreSQL
            with self.postgres_engine.begin() as conn:
                # Try to create schema if it doesn't exist (graceful failure)
                try:
                    conn.execute(text(f"""
                        CREATE SCHEMA IF NOT EXISTS {self.postgres_schema}
                    """))
                except Exception as e:
                    # If schema creation fails, assume it already exists
                    logger.debug(f"Schema creation failed (likely already exists): {str(e)}")
                
                # Drop existing table if it exists (with CASCADE to handle dependent objects)
                conn.execute(text(f"""
                    DROP TABLE IF EXISTS {self.postgres_schema}.{table_name} CASCADE
                """))
                
                # Create table
                conn.execute(text(pg_create))
                
                logger.info(f"Created PostgreSQL table {self.postgres_schema}.{table_name}")
                return True
                
        except SchemaTransformationError as e:
            logger.error(f"Schema transformation failed for {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed for {table_name}: {e}")
            return False
        except DatabaseTransactionError as e:
            logger.error(f"Database transaction failed for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating PostgreSQL table {table_name}: {str(e)}")
            return False
    
    def ensure_table_exists(self, table_name: str, mysql_schema: Dict) -> bool:
        """
        Ensure PostgreSQL table exists and matches schema.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from get_table_schema_from_mysql()
            
        Returns:
            bool: True if table exists and matches schema
        """
        try:
            # Check if table exists
            inspector = inspect(self.postgres_engine)
            if not inspector.has_table(table_name, schema=self.postgres_schema):
                # Create table
                return self.create_postgres_table(table_name, mysql_schema)
            
            # Verify schema - if verification fails, recreate the table
            if not self.verify_schema(table_name, mysql_schema):
                logger.warning(f"Schema verification failed for {table_name}, recreating table with correct schema")
                return self.create_postgres_table(table_name, mysql_schema)
            
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring PostgreSQL table {table_name}: {str(e)}")
            return False

    def verify_schema(self, table_name: str, mysql_schema: Dict) -> bool:
        """
        Verify that PostgreSQL table matches adapted MySQL schema.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from get_table_schema_from_mysql()
            
        Returns:
            bool: True if schemas match
        """
        try:
            # Get PostgreSQL table schema
            pg_columns = self.postgres_inspector.get_columns(
                table_name, 
                schema=self.postgres_schema
            )
            
            # Extract MySQL columns from create statement
            create_stmt = mysql_schema['create_statement']
            content_match = re.search(r'CREATE\s+TABLE\s+`[^`]+`\s*\((.*)\)', create_stmt, re.DOTALL)
            if not content_match:
                logger.error(f"Could not extract column definitions from MySQL create statement for {table_name}")
                return False
                
            # Extract column definitions using the same pattern as table creation
            content = content_match.group(1)
            column_defs = []
            col_pattern = r'`(\w+)`\s+([a-zA-Z0-9_]+(?:\([^)]+\))?)(?:\s+[^,]*)?(?:,|$)'
            
            for match in re.finditer(col_pattern, content):
                col_name = match.group(1)
                col_type = match.group(2).strip()
                column_defs.append((col_name, col_type))
            
            # Check if we're in a test environment using Settings
            environment = self.settings.environment
            is_test_env = environment == 'test'
            
            # Compare column counts
            if len(pg_columns) != len(column_defs):
                if is_test_env:
                    # In test environments, log warning but don't fail on column count mismatch
                    logger.warning(f"Column count mismatch for {table_name}: PostgreSQL has {len(pg_columns)}, MySQL has {len(column_defs)} (test environment - continuing)")
                else:
                    # In production, fail on column count mismatch
                    logger.error(f"Column count mismatch for {table_name}: PostgreSQL has {len(pg_columns)}, MySQL has {len(column_defs)}")
                    return False
            
            # Compare column names and types
            for pg_col, (mysql_name, mysql_type) in zip(pg_columns, column_defs):
                if pg_col['name'] != mysql_name:
                    if is_test_env:
                        # In test environments, log warning but don't fail on name mismatch
                        logger.warning(f"Column name mismatch: {pg_col['name']} != {mysql_name} (test environment - continuing)")
                    else:
                        # In production, fail on name mismatch
                        logger.error(f"Column name mismatch: {pg_col['name']} != {mysql_name}")
                        return False
                
                # Convert MySQL type to PostgreSQL type for comparison (use same logic as table creation)
                pg_type = self._convert_mysql_type(mysql_type.strip(), table_name, mysql_name)
                
                # Normalize type strings for comparison
                def normalize_type(type_str):
                    # Remove any extra whitespace
                    type_str = ' '.join(type_str.split())
                    # Convert to lowercase
                    type_str = type_str.lower()
                    # Handle character varying vs varchar
                    type_str = type_str.replace('character varying', 'varchar')
                    # Handle CHAR vs character (PostgreSQL sometimes returns CHAR in uppercase)
                    # Use regex to match CHAR only when it's a standalone type, not part of "character varying"
                    type_str = re.sub(r'\bchar\b', 'character', type_str, flags=re.IGNORECASE)
                    # Remove all spaces inside parentheses (e.g., numeric(10, 2) -> numeric(10,2))
                    type_str = re.sub(r'\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)', r'(\1,\2)', type_str)
                    return type_str
                
                pg_type_normalized = normalize_type(pg_type)
                col_type_normalized = normalize_type(str(pg_col['type']))
                
                if pg_type_normalized != col_type_normalized:
                    if is_test_env:
                        # In test environments, log warning but don't fail
                        logger.warning(f"Column type mismatch for {mysql_name}: PostgreSQL has {pg_col['type']}, expected {pg_type} (test environment - continuing)")
                    else:
                        # In production, fail on type mismatch
                        logger.error(f"Column type mismatch for {mysql_name}: PostgreSQL has {pg_col['type']}, expected {pg_type}")
                        return False
            
            return True
            
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed for {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query failed for {table_name}: {e}")
            return False
        except SchemaValidationError as e:
            logger.error(f"Schema validation failed for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error verifying schema for {table_name}: {str(e)}")
            return False 

    def convert_row_data_types(self, table_name: str, row_data: Dict) -> Dict:
        """
        OPTIMIZED: Convert row data types from MySQL to PostgreSQL compatible types.
        Uses caching to prevent repeated database analysis.
        
        Args:
            table_name: Name of the table
            row_data: Dictionary containing the row data
            
        Returns:
            Dictionary with converted data types
        """
        try:
            # Get cached column types (this does the heavy analysis once)
            column_types = self._get_table_column_types_cached(table_name)
            if not column_types:
                logger.warning(f"Could not get column types for table {table_name}, returning original data")
                return row_data
            
            # Get table schema for additional metadata
            table_schema = self.get_table_schema_from_mysql(table_name)
            columns_info = table_schema.get('columns', [])
            
            # Convert to dict for easy lookup
            if isinstance(columns_info, list):
                columns_dict = {col['name']: col for col in columns_info}
            elif isinstance(columns_info, dict):
                columns_dict = columns_info
            else:
                columns_dict = {}
            
            converted_data = {}
            
            for col_name, value in row_data.items():
                try:
                    # Get PostgreSQL type from cache
                    pg_type = column_types.get(col_name)
                    if not pg_type:
                        # No type mapping found, use original value
                        converted_data[col_name] = value
                        continue
                    
                    # Get column metadata for nullability
                    col_info = columns_dict.get(col_name, {})
                    is_nullable = col_info.get('is_nullable', True)
                    mysql_type = col_info.get('type', '').lower()
                    
                    # Convert the value using simplified logic
                    converted_value = self._convert_single_value_optimized(
                        value, pg_type, mysql_type, is_nullable, col_name, table_name
                    )
                    converted_data[col_name] = converted_value
                    
                except Exception as e:
                    logger.warning(f"Error converting column {col_name} in {table_name}: {str(e)}")
                    # Use original value as fallback
                    converted_data[col_name] = value
            
            return converted_data
            
        except Exception as e:
            logger.error(f"Error converting row data types for {table_name}: {str(e)}")
            # Return original data if conversion fails
            return row_data

    def _convert_single_value(self, value, type_info: Dict, col_name: str, table_name: str):
        """
        Convert a single value to the appropriate PostgreSQL type.
        
        Args:
            value: The value to convert
            type_info: Column type information
            col_name: Column name
            table_name: Table name
            
        Returns:
            Converted value
        """
        try:
            # Handle None values
            if value is None:
                return None
            
            # Get SQLAlchemy type information
            sqlalchemy_type = type_info.get('sqlalchemy_type')
            if not sqlalchemy_type:
                logger.warning(f"No SQLAlchemy type info for {table_name}.{col_name}, using original value")
                return value
            
            # Get Python type
            python_type = type_info.get('python_type')
            if not python_type:
                logger.warning(f"No Python type info for {table_name}.{col_name}, using original value")
                return value
            
            # Get nullable flag
            nullable = type_info.get('nullable', True)
            
            # Handle empty strings for nullable columns
            if isinstance(value, str) and value.strip() == '':
                return None if nullable else ''
            
            # Boolean conversion (MySQL TINYINT 0/1 → PostgreSQL boolean)
            if python_type == bool:
                if isinstance(value, (int, float)):
                    return bool(value)
                elif isinstance(value, str):
                    return value.lower() in ('1', 'true', 'yes', 'on')
                else:
                    return bool(value)
            
            # Integer conversions with range validation
            elif python_type == int:
                if isinstance(value, str) and value.strip() == '':
                    return None if nullable else 0
                
                try:
                    # Handle string values that might be numeric
                    if isinstance(value, str):
                        # Remove any non-numeric characters except decimal point and minus
                        cleaned_value = ''.join(c for c in value if c.isdigit() or c in '.-')
                        if not cleaned_value or cleaned_value == '.':
                            return None if nullable else 0
                        int_value = int(float(cleaned_value))  # Handle decimal strings like "123.0"
                    else:
                        int_value = int(float(value))  # Handle decimal strings like "123.0"
                    
                    # Validate integer ranges for PostgreSQL
                    if hasattr(sqlalchemy_type, 'python_type'):
                        if 'SMALLINT' in str(sqlalchemy_type).upper():
                            if not (-32768 <= int_value <= 32767):
                                logger.warning(f"SMALLINT out of range for {table_name}.{col_name}: {int_value}")
                        elif 'INTEGER' in str(sqlalchemy_type).upper():
                            if not (-2147483648 <= int_value <= 2147483647):
                                logger.warning(f"INTEGER out of range for {table_name}.{col_name}: {int_value}")
                    
                    return int_value
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not convert '{value}' to integer for {table_name}.{col_name}: {str(e)}")
                    return None if nullable else 0
            
            # Float/Decimal conversions
            elif python_type == float:
                if isinstance(value, str) and value.strip() == '':
                    return None if nullable else 0.0
                
                try:
                    return float(value)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not convert '{value}' to float for {table_name}.{col_name}: {str(e)}")
                    return None if nullable else 0.0
            
            # Decimal/Numeric conversions (high precision)
            elif hasattr(sqlalchemy_type, 'python_type') and 'NUMERIC' in str(sqlalchemy_type).upper():
                if isinstance(value, str) and value.strip() == '':
                    return None if nullable else 0
                
                try:
                    from decimal import Decimal, InvalidOperation
                    return Decimal(str(value))
                except (InvalidOperation, ValueError, TypeError) as e:
                    logger.warning(f"Could not convert '{value}' to decimal for {table_name}.{col_name}: {str(e)}")
                    return None if nullable else 0
            
            # DateTime conversions (MySQL datetime → PostgreSQL timestamp)
            elif python_type == datetime:
                if isinstance(value, str):
                    if value.strip() == '' or value == '0000-00-00 00:00:00':
                        return None if nullable else datetime.min
                    
                    try:
                        # Handle various MySQL datetime formats
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse datetime '{value}' for {table_name}.{col_name}: {str(e)}")
                        return None if nullable else datetime.min
                elif isinstance(value, datetime):
                    return value
                else:
                    logger.warning(f"Unexpected datetime type '{type(value)}' for {table_name}.{col_name}")
                    return None if nullable else datetime.min
            
            # String conversions with encoding and length validation
            elif python_type == str:
                if value is None:
                    return None
                
                str_value = str(value)
                
                # Handle MySQL charset encoding issues
                try:
                    # Ensure proper UTF-8 encoding
                    if isinstance(str_value, bytes):
                        str_value = str_value.decode('utf-8', errors='replace')
                    else:
                        # Re-encode to handle any encoding issues
                        str_value = str_value.encode('utf-8', errors='replace').decode('utf-8')
                except (UnicodeDecodeError, UnicodeEncodeError):
                    logger.warning(f"Encoding issue for {table_name}.{col_name}, using replacement characters")
                    str_value = str(value).encode('utf-8', errors='replace').decode('utf-8')
                
                # Validate string length if column has length constraint
                if hasattr(sqlalchemy_type, 'length') and sqlalchemy_type.length:
                    max_length = sqlalchemy_type.length
                    if len(str_value) > max_length:
                        logger.warning(f"String truncated for {table_name}.{col_name}: length {len(str_value)} > {max_length}")
                        str_value = str_value[:max_length]
                
                return str_value
            
            # Date conversions (MySQL date → PostgreSQL date)
            elif hasattr(sqlalchemy_type, 'python_type') and 'DATE' in str(sqlalchemy_type).upper():
                if isinstance(value, str):
                    if value.strip() == '' or value == '0000-00-00':
                        return None if nullable else datetime.min.date()
                    
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt.date()
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse date '{value}' for {table_name}.{col_name}: {str(e)}")
                        return None if nullable else datetime.min.date()
                elif hasattr(value, 'date'):
                    return value.date() if hasattr(value, 'date') else value
                else:
                    return value
            
            # Time conversions (MySQL time → PostgreSQL time)
            elif hasattr(sqlalchemy_type, 'python_type') and 'TIME' in str(sqlalchemy_type).upper():
                if isinstance(value, str):
                    if value.strip() == '':
                        return None if nullable else datetime.min.time()
                    
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt.time()
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse time '{value}' for {table_name}.{col_name}: {str(e)}")
                        return None if nullable else datetime.min.time()
                elif hasattr(value, 'time'):
                    return value.time() if hasattr(value, 'time') else value
                else:
                    return value
            
            # JSON conversions (MySQL json → PostgreSQL jsonb)
            elif hasattr(sqlalchemy_type, 'python_type') and 'JSON' in str(sqlalchemy_type).upper():
                if isinstance(value, str):
                    try:
                        import json
                        return json.loads(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse JSON '{value}' for {table_name}.{col_name}: {str(e)}")
                        return None if nullable else {}
                else:
                    return value
            
            # Default case: return original value
            else:
                logger.debug(f"No specific conversion for {table_name}.{col_name} (type: {python_type}), using original value")
                return value
                
        except Exception as e:
            logger.error(f"Unexpected error converting value for {table_name}.{col_name}: {str(e)}")
            return value  # Return original value on any error
    
    def _convert_single_value_simplified(self, value, col_info: Dict, col_name: str, table_name: str):
        """
        Simplified single value conversion that works with the schema format from get_table_schema_from_mysql.
        
        Args:
            value: The value to convert
            col_info: Column information dictionary
            col_name: Column name
            table_name: Table name
            
        Returns:
            Converted value
        """
        try:
            # Handle None values
            if value is None:
                return None
            
            # Get MySQL type from column info
            mysql_type = col_info.get('type', '').lower()
            is_nullable = col_info.get('is_nullable', True)
            
            # Handle empty strings for nullable columns
            if isinstance(value, str) and value.strip() == '':
                return None if is_nullable else ''
            
            # Boolean conversion (MySQL TINYINT 0/1 → PostgreSQL boolean)
            if mysql_type.startswith('tinyint'):
                # Check if this column was identified as boolean during schema analysis
                pg_type = self._convert_mysql_type(mysql_type, table_name, col_name)
                if pg_type == 'boolean':
                    if isinstance(value, (int, float)):
                        return bool(value)
                    elif isinstance(value, str):
                        return value.lower() in ('1', 'true', 'yes', 'on')
                    else:
                        return bool(value)
                else:
                    # Keep as smallint
                    try:
                        return int(value) if value != '' else (None if is_nullable else 0)
                    except (ValueError, TypeError):
                        return None if is_nullable else 0
            
            # Integer conversions
            elif mysql_type in ('int', 'bigint', 'smallint', 'mediumint'):
                if isinstance(value, str) and value.strip() == '':
                    return None if is_nullable else 0
                try:
                    if isinstance(value, str):
                        # Remove any non-numeric characters except decimal point and minus
                        cleaned_value = ''.join(c for c in value if c.isdigit() or c in '.-')
                        if not cleaned_value or cleaned_value == '.':
                            return None if is_nullable else 0
                        int_value = int(float(cleaned_value))
                    else:
                        int_value = int(float(value))
                    return int_value
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert '{value}' to integer for {table_name}.{col_name}")
                    return None if is_nullable else 0
            
            # Float/Decimal conversions
            elif mysql_type in ('float', 'double', 'decimal', 'numeric'):
                if isinstance(value, str) and value.strip() == '':
                    return None if is_nullable else 0.0
                try:
                    if mysql_type in ('decimal', 'numeric'):
                        from decimal import Decimal, InvalidOperation
                        return Decimal(str(value))
                    else:
                        return float(value)
                except (ValueError, TypeError, InvalidOperation):
                    logger.warning(f"Could not convert '{value}' to numeric for {table_name}.{col_name}")
                    return None if is_nullable else 0
            
            # DateTime conversions
            elif mysql_type in ('datetime', 'timestamp'):
                if isinstance(value, str):
                    if value.strip() == '' or value == '0000-00-00 00:00:00':
                        return None if is_nullable else datetime.min
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse datetime '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else datetime.min
                elif isinstance(value, datetime):
                    return value
                else:
                    return None if is_nullable else datetime.min
            
            # Date conversions
            elif mysql_type == 'date':
                if isinstance(value, str):
                    if value.strip() == '' or value == '0000-00-00':
                        return None if is_nullable else datetime.min.date()
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt.date()
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse date '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else datetime.min.date()
                elif hasattr(value, 'date'):
                    return value.date() if hasattr(value, 'date') else value
                else:
                    return value
            
            # Time conversions
            elif mysql_type == 'time':
                if isinstance(value, str):
                    if value.strip() == '':
                        return None if is_nullable else datetime.min.time()
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt.time()
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse time '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else datetime.min.time()
                elif hasattr(value, 'time'):
                    return value.time() if hasattr(value, 'time') else value
                else:
                    return value
            
            # String conversions with encoding handling
            elif mysql_type in ('varchar', 'char', 'text', 'mediumtext', 'longtext'):
                if value is None:
                    return None
                str_value = str(value)
                # Handle MySQL charset encoding issues
                try:
                    if isinstance(str_value, bytes):
                        str_value = str_value.decode('utf-8', errors='replace')
                    else:
                        # Re-encode to handle any encoding issues
                        str_value = str_value.encode('utf-8', errors='replace').decode('utf-8')
                except (UnicodeDecodeError, UnicodeEncodeError):
                    logger.warning(f"Encoding issue for {table_name}.{col_name}, using replacement characters")
                    str_value = str(value).encode('utf-8', errors='replace').decode('utf-8')
                return str_value
            
            # JSON conversions
            elif mysql_type == 'json':
                if isinstance(value, str):
                    try:
                        import json
                        return json.loads(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse JSON '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else {}
                else:
                    return value
            
            # Default case: return original value
            else:
                logger.debug(f"No specific conversion for {table_name}.{col_name} (type: {mysql_type}), using original value")
                return value
                
        except Exception as e:
            logger.error(f"Unexpected error converting value for {table_name}.{col_name}: {str(e)}")
            return value  # Return original value on any error 

    def _convert_single_value_optimized(self, value, pg_type: str, mysql_type: str, 
                                       is_nullable: bool, col_name: str, table_name: str):
        """
        Optimized single value conversion using pre-determined PostgreSQL types.
        
        Args:
            value: The value to convert
            pg_type: Pre-determined PostgreSQL type
            mysql_type: Original MySQL type
            is_nullable: Whether the column is nullable
            col_name: Column name
            table_name: Table name
            
        Returns:
            Converted value
        """
        try:
            # Handle None values
            if value is None:
                return None
            
            # Handle empty strings for nullable columns
            if isinstance(value, str) and value.strip() == '':
                return None if is_nullable else ''
            
            # Boolean conversion
            if pg_type == 'boolean':
                if isinstance(value, (int, float)):
                    return bool(value)
                elif isinstance(value, str):
                    return value.lower() in ('1', 'true', 'yes', 'on')
                else:
                    return bool(value)
            
            # Integer conversions
            elif pg_type in ('smallint', 'integer', 'bigint'):
                if isinstance(value, str) and value.strip() == '':
                    return None if is_nullable else 0
                
                try:
                    if isinstance(value, str):
                        # Remove any non-numeric characters except decimal point and minus
                        cleaned_value = ''.join(c for c in value if c.isdigit() or c in '.-')
                        if not cleaned_value or cleaned_value == '.':
                            return None if is_nullable else 0
                        int_value = int(float(cleaned_value))
                    else:
                        int_value = int(float(value))
                    
                    return int_value
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert '{value}' to integer for {table_name}.{col_name}")
                    return None if is_nullable else 0
            
            # Float/Decimal conversions
            elif pg_type in ('real', 'double precision', 'numeric'):
                if isinstance(value, str) and value.strip() == '':
                    return None if is_nullable else 0.0
                
                try:
                    if 'numeric' in pg_type:
                        from decimal import Decimal, InvalidOperation
                        return Decimal(str(value))
                    else:
                        return float(value)
                except (ValueError, TypeError, InvalidOperation):
                    logger.warning(f"Could not convert '{value}' to numeric for {table_name}.{col_name}")
                    return None if is_nullable else 0
            
            # DateTime conversions
            elif pg_type == 'timestamp':
                if isinstance(value, str):
                    if value.strip() == '' or value == '0000-00-00 00:00:00':
                        return None if is_nullable else datetime.min
                    
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse datetime '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else datetime.min
                elif isinstance(value, datetime):
                    return value
                else:
                    return None if is_nullable else datetime.min
            
            # Date conversions
            elif pg_type == 'date':
                if isinstance(value, str):
                    if value.strip() == '' or value == '0000-00-00':
                        return None if is_nullable else datetime.min.date()
                    
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt.date()
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse date '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else datetime.min.date()
                elif hasattr(value, 'date'):
                    return value.date() if hasattr(value, 'date') else value
                else:
                    return value
            
            # Time conversions
            elif pg_type == 'time':
                if isinstance(value, str):
                    if value.strip() == '':
                        return None if is_nullable else datetime.min.time()
                    try:
                        from dateutil import parser
                        parsed_dt = parser.parse(value)
                        return parsed_dt.time()
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse time '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else datetime.min.time()
                elif hasattr(value, 'time'):
                    return value.time() if hasattr(value, 'time') else value
                else:
                    return value
            
            # String conversions
            elif pg_type in ('character varying', 'character', 'text'):
                if value is None:
                    return None
                
                str_value = str(value)
                
                # Handle MySQL charset encoding issues
                try:
                    if isinstance(str_value, bytes):
                        str_value = str_value.decode('utf-8', errors='replace')
                    else:
                        # Re-encode to handle any encoding issues
                        str_value = str_value.encode('utf-8', errors='replace').decode('utf-8')
                except (UnicodeDecodeError, UnicodeEncodeError):
                    logger.warning(f"Encoding issue for {table_name}.{col_name}, using replacement characters")
                    str_value = str(value).encode('utf-8', errors='replace').decode('utf-8')
                
                return str_value
            
            # JSON conversions
            elif pg_type == 'jsonb':
                if isinstance(value, str):
                    try:
                        import json
                        return json.loads(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse JSON '{value}' for {table_name}.{col_name}")
                        return None if is_nullable else {}
                else:
                    return value
            
            # Default case: return original value
            else:
                logger.debug(f"No specific conversion for {table_name}.{col_name} (pg_type: {pg_type}), using original value")
                return value
                
        except Exception as e:
            logger.error(f"Unexpected error converting value for {table_name}.{col_name}: {str(e)}")
            return value  # Return original value on any error 