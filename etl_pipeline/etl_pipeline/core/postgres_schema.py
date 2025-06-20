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

from etl_pipeline.core.logger import get_logger
from etl_pipeline.config.settings import settings

logger = logging.getLogger(__name__)

class PostgresSchema:
    """Handles schema adaptation from MySQL to PostgreSQL with improved type mapping."""
    
    def __init__(self, mysql_engine: Engine, postgres_engine: Engine, mysql_db: str, postgres_db: str, postgres_schema: str = 'raw'):
        """
        Initialize PostgreSQL schema adaptation.
        
        Args:
            mysql_engine: MySQL replication database engine
            postgres_engine: PostgreSQL analytics database engine
            mysql_db: MySQL database name
            postgres_db: PostgreSQL database name
            postgres_schema: PostgreSQL schema name
        """
        self.mysql_engine = mysql_engine
        self.postgres_engine = postgres_engine
        self.mysql_db = mysql_db
        self.postgres_db = postgres_db
        self.postgres_schema = postgres_schema
        self._schema_cache = {}
        
        # Initialize inspectors
        self.mysql_inspector = inspect(mysql_engine)
        self.postgres_inspector = inspect(postgres_engine)
    
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
            # For TINYINT columns, check if they're actually boolean (only 0/1 values)
            if mysql_type.lower().startswith('tinyint'):
                with self.mysql_engine.connect() as conn:
                    # Use a more efficient query that checks for non-0/1 values
                    query = text(f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE {column_name} IS NOT NULL 
                        AND {column_name} NOT IN (0, 1)
                        LIMIT 1
                    """)
                    result = conn.execute(query)
                    has_non_bool = result.scalar() > 0
                    
                    if not has_non_bool:
                        logger.info(f"Column {table_name}.{column_name} identified as boolean")
                        return 'boolean'
                    
                    logger.info(f"Column {table_name}.{column_name} kept as smallint (contains non-boolean values)")
                    return 'smallint'
            
        except Exception as e:
            logger.warning(f"Could not analyze column data for {table_name}.{column_name}: {str(e)}")
        
        # Fall back to standard type conversion
        return self._convert_mysql_type_standard(mysql_type)
    
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
        
        return pg_type
    
    def _convert_mysql_type(self, mysql_type: str, table_name: str = None, column_name: str = None) -> str:
        """
        Convert MySQL data type to PostgreSQL data type with intelligent analysis.
        
        Args:
            mysql_type: MySQL type string
            table_name: Table name for data analysis
            column_name: Column name for data analysis
            
        Returns:
            str: PostgreSQL type
        """
        # If we have table and column info, do intelligent analysis
        if table_name and column_name:
            return self._analyze_column_data(table_name, column_name, mysql_type)
        
        # Otherwise use standard conversion
        return self._convert_mysql_type_standard(mysql_type)
    
    def adapt_schema(self, table_name: str, mysql_schema: Dict) -> str:
        """
        Adapt MySQL schema to PostgreSQL with intelligent type mapping.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from SchemaDiscovery
            
        Returns:
            str: PostgreSQL-compatible CREATE TABLE statement
        """
        try:
            # Get the MySQL CREATE statement
            create_statement = mysql_schema['create_statement']
            
            # Convert MySQL types to PostgreSQL types with intelligent analysis
            pg_columns = self._convert_mysql_to_postgres_intelligent(create_statement, table_name)
            
            # Create the final CREATE TABLE statement
            pg_create = f"CREATE TABLE {self.postgres_schema}.{table_name} (\n{pg_columns}\n)"
            
            return pg_create
            
        except Exception as e:
            logger.error(f"Error adapting schema for {table_name}: {str(e)}")
            raise
    
    def _convert_mysql_to_postgres_intelligent(self, mysql_create: str, table_name: str) -> str:
        """Convert MySQL CREATE statement to PostgreSQL syntax with intelligent type analysis."""
        # Extract the content between the outermost parentheses
        content_match = re.search(r'CREATE TABLE[^(]+\((.*)\)', mysql_create, re.DOTALL)
        if not content_match:
            return ""
            
        content = content_match.group(1)
        
        # Extract column definitions
        columns = []
        
        # Pattern to match column definitions: `name` type [NOT NULL] [AUTO_INCREMENT]
        col_pattern = r'`(\w+)`\s+([a-zA-Z][a-zA-Z0-9_\s]*(?:\([^)]+\))?)(?:\s+(?:NOT NULL|AUTO_INCREMENT))?(?:,|$)'
        
        # Find all column definitions
        for match in re.finditer(col_pattern, content):
            col_name = match.group(1)
            col_type = match.group(2).strip()
            
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
            mysql_schema: MySQL schema information from SchemaDiscovery
            
        Returns:
            bool: True if successful
        """
        try:
            # Adapt schema for PostgreSQL
            pg_create = self.adapt_schema(table_name, mysql_schema)
            
            # Create table in PostgreSQL
            with self.postgres_engine.begin() as conn:
                # Ensure schema exists
                conn.execute(text(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.postgres_schema}
                """))
                
                # Drop existing table if it exists
                conn.execute(text(f"""
                    DROP TABLE IF EXISTS {self.postgres_schema}.{table_name}
                """))
                
                # Create table
                conn.execute(text(pg_create))
                
                logger.info(f"Created PostgreSQL table {self.postgres_schema}.{table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error creating PostgreSQL table {table_name}: {str(e)}")
            return False
    
    def verify_schema(self, table_name: str, mysql_schema: Dict) -> bool:
        """
        Verify that PostgreSQL table matches adapted MySQL schema.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from SchemaDiscovery
            
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
                
            # Extract column definitions
            content = content_match.group(1)
            column_defs = re.findall(r'`([^`]+)`\s+([^,]+)(?:,|$)', content)
            
            # Compare column counts
            if len(pg_columns) != len(column_defs):
                logger.error(f"Column count mismatch for {table_name}: PostgreSQL has {len(pg_columns)}, MySQL has {len(column_defs)}")
                return False
            
            # Compare column names and types
            for pg_col, (mysql_name, mysql_type) in zip(pg_columns, column_defs):
                if pg_col['name'] != mysql_name:
                    logger.error(f"Column name mismatch: {pg_col['name']} != {mysql_name}")
                    return False
                
                # Convert MySQL type to PostgreSQL type for comparison
                pg_type = self._convert_mysql_type(mysql_type.strip(), table_name, mysql_name)
                
                # Normalize type strings for comparison
                def normalize_type(type_str):
                    # Remove any extra whitespace
                    type_str = ' '.join(type_str.split())
                    # Convert to lowercase
                    type_str = type_str.lower()
                    # Handle character varying vs varchar
                    type_str = type_str.replace('character varying', 'varchar')
                    return type_str
                
                pg_type_normalized = normalize_type(pg_type)
                col_type_normalized = normalize_type(str(pg_col['type']))
                
                if pg_type_normalized != col_type_normalized:
                    logger.error(f"Column type mismatch for {mysql_name}: PostgreSQL has {pg_col['type']}, expected {pg_type}")
                    return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error verifying schema for {table_name}: {str(e)}")
            return False 