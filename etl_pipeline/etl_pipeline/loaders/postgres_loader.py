"""
DEPRECATION NOTICE - REFACTORING IN PROGRESS
============================================

This file is part of the ETL Pipeline Schema Analysis Refactoring Plan.
See: docs/refactor_remove_schema_discovery_from_etl.md

PLANNED CHANGES:
- Will update for simplified tables.yml configuration structure
- Will remove three-section YAML support (source_tables, staging_tables, target_tables)
- Will use new get_table_config() method from settings
- Will integrate with enhanced PostgresSchema for configuration
- Will maintain current data movement functionality

TIMELINE: Phase 4 of refactoring plan
STATUS: Configuration update in progress

PostgreSQL loader implementation for loading data from MySQL replication to PostgreSQL analytics.

DATA FLOW ARCHITECTURE
======================

ARCHITECTURAL ROLE:
This is a PURE DATA MOVEMENT LAYER that moves data from MySQL replication to PostgreSQL raw schema.
It focuses exclusively on data movement and delegates all transformation logic to PostgresSchema.

DATA FLOW:
MySQL OpenDental (Source Database)
    ↓
SimpleMySQLReplicator (MySQL to MySQL copy)
    ↓
opendental_replication (MySQL Replication Database)
    ↓
PostgresLoader (ETL Data Movement Layer)
    ├── Data Extraction (with incremental support)
    ├── Data Loading (bulk/chunked operations)
    └── Load Verification (row count validation)
    ↓
PostgresSchema (Transformation Layer)
    ├── Schema Conversion (MySQL → PostgreSQL types)
    ├── Data Type Conversion (MySQL → PostgreSQL values)
    └── Type Validation and Normalization
    ↓
opendental_analytics.raw (PostgreSQL Raw Schema)

KEY ARCHITECTURAL PRINCIPLES:

1. PURE DATA MOVEMENT LAYER
   - Purpose: Move data from MySQL replication to PostgreSQL raw schema
   - Transformation: DELEGATED - all transformation logic handled by PostgresSchema
   - No Business Logic: No field mappings, calculations, or business rules

2. TRANSFORMATION DELEGATION
   - Schema Conversion: PostgresSchema.adapt_schema() and create_postgres_table()
   - Data Type Conversion: PostgresSchema.convert_row_data_types()
   - Type Validation: PostgresSchema.verify_schema()

3. INCREMENTAL LOADING SUPPORT
   - Strategy: Time-based incremental loading using incremental_columns from tables.yml
   - Fallback: Full load when incremental not possible
   - Tracking: Load status tracking in etl_load_status table

4. PERFORMANCE OPTIMIZATION
   - Chunked Loading: Memory-efficient processing for large tables
   - Bulk Operations: Optimized INSERT statements
   - Connection Management: Proper transaction handling

5. DATA INTEGRITY
   - Schema Validation: Ensure PostgreSQL table structure matches MySQL
   - Row Count Verification: Validate data completeness
   - Error Handling: Graceful failure with detailed logging

WHAT POSTGRESLOADER SHOULD NOT DO:
- Business Logic Transformations (field name changes, calculations)
- Data Quality Rules (complex validation, business-specific checks)
- Analytics Preparation (data enrichment, metadata standardization)
- Schema Analysis or Conversion (delegated to PostgresSchema)
- Data Type Conversions (delegated to PostgresSchema)
- Table Creation or Modification (delegated to PostgresSchema)

WHAT POSTGRESLOADER SHOULD DO:
- Data Movement Operations (extraction, loading, verification)
- Configuration Integration (tables.yml, PostgresSchema)
- Pipeline Mechanics (incremental loading, chunking, transaction management)
- Query Building for Data Extraction
- Load Verification and Error Handling

This creates a clean separation where:
- SimpleMySQLReplicator = MySQL to MySQL data movement
- PostgresLoader = MySQL to PostgreSQL data movement (pure movement)
- PostgresSchema = Schema conversion and data transformation
- dbt models = Business Logic & Analytics

SIMPLIFIED VERSION - CORE LOADING FUNCTIONALITY ONLY
===================================================
This PostgresLoader has been simplified to focus only on core loading operations,
removing over-engineered schema analysis, metadata methods, and redundant functionality.

Core Functionality:
- load_table(): Standard table loading with incremental support
- load_table_chunked(): Chunked loading for large tables
- verify_load(): Basic load verification
- Schema integration with PostgresSchema for table creation

Removed Components:
- Schema analysis methods (use PostgresSchema.get_table_schema_from_mysql() instead)
- Metadata methods (grants, triggers, views, dependencies)
- Redundant table information methods
- Methods that duplicate existing functionality

What's Kept:
- Core loading logic (load_table, load_table_chunked)
- Integration with PostgresSchema for schema conversion
- Incremental loading support
- Load verification
- Error handling and logging

This simplified version focuses on the actual ETL operations needed by the pipeline.
"""
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
import os
import yaml

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config.settings import Settings
# Removed find_latest_tables_config import - we only use tables.yml with metadata versioning

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseTransactionError, DatabaseQueryError
from ..exceptions.data import DataLoadingError
from ..exceptions.configuration import ConfigurationError

logger = get_logger(__name__)

class PostgresLoader:
    """
    PURE DATA MOVEMENT LAYER for loading data from MySQL replication to PostgreSQL analytics.
    
    This class integrates with SimpleMySQLReplicator and uses static configuration
    from tables.yml, eliminating the need for dynamic SchemaDiscovery during ETL operations.
    
    ARCHITECTURAL ROLE: PURE DATA MOVEMENT LAYER
    - Focuses exclusively on data extraction, loading, and pipeline mechanics
    - Delegates all transformation logic to PostgresSchema
    - Handles incremental loading, chunking, and load verification
    - Manages database connections and transaction handling
    - No schema analysis, conversion, or data transformation
    
    DATA MOVEMENT RESPONSIBILITIES:
    - Data extraction from MySQL replication database
    - Data loading to PostgreSQL analytics database
    - Incremental loading logic and query building
    - Chunked processing for large datasets
    - Load verification and transaction management
    - Configuration management (tables.yml)
    
    DELEGATION TO POSTGRESSCHEMA:
    - Schema extraction: schema_adapter.get_table_schema_from_mysql()
    - Table creation: schema_adapter.ensure_table_exists()
    - Data transformation: schema_adapter.convert_row_data_types()
    
    SEPARATION OF CONCERNS:
    - PostgresLoader: Data movement, pipeline mechanics, load verification
    - PostgresSchema: Schema conversion, data transformation, type mapping
    """
    
    def __init__(self, tables_config_path: Optional[str] = None, use_test_environment: bool = False, settings: Optional[Settings] = None):
        """
        Initialize the PostgreSQL loader using new settings-centric architecture.
        
        Args:
            tables_config_path: Path to tables.yml (uses default if None)
            use_test_environment: Whether to use test environment connections (default: False)
            settings: Optional Settings instance to use (for test injection)
        """
        try:
            # Use injected settings if provided
            if settings is not None:
                self.settings = settings
                logger.info("Using injected Settings instance for database connections")
            elif use_test_environment:
                from etl_pipeline.config import create_test_settings
                self.settings = create_test_settings()
                logger.info("Using test environment settings for database connections")
            else:
                self.settings = get_settings()
            
            # Set tables config path
            if tables_config_path is None:
                config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
                tables_config_path = os.path.join(config_dir, 'tables.yml')
            self.tables_config_path = tables_config_path
            logger.info(f"PostgresLoader using tables config: {self.tables_config_path}")
            
            # Load table configuration
            self.table_configs = self._load_configuration()
            
            # Get database connections using unified interface with Settings injection
            self.replication_engine = ConnectionFactory.get_replication_connection(self.settings)
            self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(self.settings)
            print(f"[ETL DEBUG] PostgresLoader analytics_engine id: {id(self.analytics_engine)}")
            
            # Log which environment we're using
            environment = self.settings.environment.upper()
            logger.info(f"PostgresLoader initialized with {environment} environment connections")
            
            # Get database configurations from settings
            replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
            analytics_config = self.settings.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
            
            # Use actual database names from settings (supports both production and test environments)
            self.replication_db = replication_config.get('database', 'opendental_replication')
            self.analytics_db = analytics_config.get('database', 'opendental_analytics')
            self.analytics_schema = analytics_config.get('schema', 'raw')
            
            # Initialize schema adapter using new constructor
            # Convert string schema to enum for PostgresSchema constructor
            schema_enum = ConfigPostgresSchema.RAW  # Default to RAW
            if self.analytics_schema == 'raw':
                schema_enum = ConfigPostgresSchema.RAW
            elif self.analytics_schema == 'staging':
                schema_enum = ConfigPostgresSchema.STAGING
            elif self.analytics_schema == 'intermediate':
                schema_enum = ConfigPostgresSchema.INTERMEDIATE
            elif self.analytics_schema == 'marts':
                schema_enum = ConfigPostgresSchema.MARTS
            
            self.schema_adapter = PostgresSchema(
                postgres_schema=schema_enum,
                settings=self.settings
            )
            
            self.target_schema = analytics_config.get('schema', 'raw')
            self.staging_schema = replication_config.get('schema', 'raw')
            
            logger.info(f"PostgresLoader initialized with {len(self.table_configs)} table configurations")
            
        except ConfigurationError as e:
            logger.error(f"Configuration error in PostgresLoader initialization: {e}")
            raise
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error in PostgresLoader initialization: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in PostgresLoader initialization: {str(e)}")
            raise
    
    def _load_configuration(self) -> Dict:
        """Load table configuration from tables.yml."""
        try:
            with open(self.tables_config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            tables = config.get('tables', {})
            logger.info(f"Loaded configuration for {len(tables)} tables from {self.tables_config_path}")
            return tables
            
        except FileNotFoundError:
            raise ConfigurationError(
                message=f"Configuration file not found: {self.tables_config_path}",
                config_file=self.tables_config_path,
                details={"error_type": "file_not_found"}
            )
        except Exception as e:
            raise ConfigurationError(
                message=f"Failed to load configuration from {self.tables_config_path}",
                config_file=self.tables_config_path,
                details={"error": str(e)}
            )
    
    def get_table_config(self, table_name: str) -> Dict:
        """Get table configuration."""
        config = self.table_configs.get(table_name, {})
        if not config:
            logger.warning(f"No configuration found for table {table_name}")
            return {}
        return config
    
    def load_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Load table from MySQL replication to PostgreSQL analytics using pure SQLAlchemy.
        This approach avoids pandas' connection handling issues entirely.
        
        Args:
            table_name: Name of the table to load
            force_full: Whether to force a full load instead of incremental
            
        Returns:
            bool: True if successful
        """
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get MySQL schema from replication database
            mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
            
            # Create or verify PostgreSQL table
            if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                return False
            
            # Get incremental columns from configuration
            incremental_columns = table_config.get('incremental_columns', [])
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Build query to get data
            query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # Use SQLAlchemy connections directly - more reliable than pandas integration
            with self.replication_engine.connect() as source_conn:
                # Execute query and fetch results
                result = source_conn.execute(text(query))
                
                # Get column names from result
                column_names = list(result.keys())
                
                # Fetch all rows (for incremental loads, this should be manageable)
                rows = result.fetchall()
                
                if not rows:
                    logger.info(f"No new data to load for {table_name}")
                    return True
                
                logger.info(f"Fetched {len(rows)} rows from {table_name}")
            
            # Prepare data for PostgreSQL insertion with type conversion
            rows_data = []
            for row in rows:
                row_dict = dict(zip(column_names, row))
                converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                rows_data.append(converted_row)
            
            # Handle PostgreSQL insertion with proper transaction management
            with self.analytics_engine.begin() as target_conn:
                if force_full:
                    # For full load, truncate table first
                    target_conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    logger.info(f"Truncated table {self.analytics_schema}.{table_name} for full load")
                
                # Use bulk insert for better performance
                if rows_data:
                    # Create parameterized insert statement
                    columns = ', '.join([f'"{col}"' for col in column_names])
                    placeholders = ', '.join([f':{col}' for col in column_names])
                    
                    insert_sql = f"""
                        INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    
                    # Execute bulk insert
                    target_conn.execute(text(insert_sql), rows_data)
                    
                    logger.info(f"Loaded {len(rows_data)} rows to {self.analytics_schema}.{table_name} using {'full' if force_full else 'incremental'} strategy")
            
            return True
                
        except DataLoadingError as e:
            logger.error(f"Data loading failed for {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed for {table_name}: {e}")
            return False
        except DatabaseTransactionError as e:
            logger.error(f"Database transaction failed for {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query failed for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error loading table {table_name}: {str(e)}")
            return False
    
    def load_table_chunked(self, table_name: str, force_full: bool = False, 
                          chunk_size: int = 10000) -> bool:
        """
        Load table in chunks for memory efficiency with large datasets.
        
        Args:
            table_name: Name of the table to load
            force_full: Whether to force a full load instead of incremental
            chunk_size: Number of rows to process in each chunk
            
        Returns:
            bool: True if successful
        """
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get MySQL schema from replication database
            mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
            
            # Create or verify PostgreSQL table
            if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                return False
            
            # Get incremental columns from configuration
            incremental_columns = table_config.get('incremental_columns', [])
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Check if analytics table exists and has sufficient data for incremental loading
            if not force_full and incremental_columns:
                try:
                    # Check if analytics table exists
                    inspector = inspect(self.analytics_engine)
                    table_exists = inspector.has_table(table_name, schema=self.analytics_schema)
                    
                    if not table_exists:
                        logger.info(f"Analytics table {table_name} does not exist. Performing full sync.")
                        force_full = True
                    else:
                        # Check row counts
                        with self.replication_engine.connect() as source_conn:
                            replication_count = source_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                        
                        with self.analytics_engine.connect() as target_conn:
                            analytics_count = target_conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                        
                        replication_count = replication_count or 0
                        analytics_count = analytics_count or 0
                        
                        # If analytics has less than 50% of replication data, force full load
                        if replication_count > 0 and analytics_count < (replication_count * 0.5):
                            logger.warning(f"Analytics table {table_name} has {analytics_count} rows vs {replication_count} in replication. Performing full sync.")
                            force_full = True
                        else:
                            logger.info(f"Analytics table {table_name} has {analytics_count} rows vs {replication_count} in replication. Using incremental load.")
                        
                except Exception as e:
                    logger.warning(f"Could not check data completeness for {table_name}, proceeding with incremental: {str(e)}")
            
            # First, get total count
            count_query = self._build_count_query(table_name, incremental_columns, force_full)
            
            with self.replication_engine.connect() as source_conn:
                total_rows = source_conn.execute(text(count_query)).scalar()
            
            if total_rows is None or total_rows == 0:
                logger.info(f"No new data to load for {table_name}")
                return True
            
            # Ensure total_rows is an integer
            total_rows = int(total_rows)
            
            logger.info(f"Loading {total_rows} rows from {table_name} in chunks of {chunk_size}")
            
            # Process in chunks
            total_loaded = 0
            chunk_num = 0
            
            # Handle full load truncation
            if force_full:
                with self.analytics_engine.begin() as target_conn:
                    target_conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    logger.info(f"Truncated table {self.analytics_schema}.{table_name} for full load")
            
            # Build base query once (outside the loop)
            base_query = self._build_load_query(table_name, incremental_columns, force_full)
            
            while total_loaded < total_rows:
                chunk_num += 1
                
                # Build chunked query using the pre-built base query
                chunked_query = f"{base_query} LIMIT {chunk_size} OFFSET {total_loaded}"
                
                # Load chunk
                with self.replication_engine.connect() as source_conn:
                    result = source_conn.execute(text(chunked_query))
                    column_names = list(result.keys())
                    rows = result.fetchall()
                    
                    if not rows:
                        break
                    
                    # Prepare chunk data with type conversion
                    rows_data = []
                    for row in rows:
                        row_dict = dict(zip(column_names, row))
                        converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                        rows_data.append(converted_row)
                
                # Insert chunk
                with self.analytics_engine.begin() as target_conn:
                    columns = ', '.join([f'"{col}"' for col in column_names])
                    placeholders = ', '.join([f':{col}' for col in column_names])
                    
                    insert_sql = f"""
                        INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    
                    target_conn.execute(text(insert_sql), rows_data)
                
                chunk_rows = len(rows)
                total_loaded += chunk_rows
                
                logger.info(f"Loaded chunk {chunk_num}: {total_loaded}/{total_rows} rows ({(total_loaded/total_rows)*100:.1f}%)")
                
                if chunk_rows < chunk_size:
                    break
            
            logger.info(f"Successfully loaded {total_loaded} rows to {self.analytics_schema}.{table_name}")
            return True
            
        except DataLoadingError as e:
            logger.error(f"Data loading failed in chunked load for table {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed in chunked load for table {table_name}: {e}")
            return False
        except DatabaseTransactionError as e:
            logger.error(f"Database transaction failed in chunked load for table {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query failed in chunked load for table {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in chunked load for table {table_name}: {str(e)}")
            return False
    
    def verify_load(self, table_name: str) -> bool:
        """
        Verify that the load was successful.
        
        Args:
            table_name: Name of the table
            
        Returns:
            bool: True if verification passed
        """
        try:
            # Get row counts from correct schemas
            with self.replication_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {self.replication_db}.{table_name}")).scalar()
            
            with self.analytics_engine.connect() as conn:
                target_count = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
            
            source_count = source_count or 0
            target_count = target_count or 0
            
            logger.info(f"Row count verification for {table_name}: source={source_count}, target={target_count}")
            
            if source_count != target_count:
                logger.error(f"Row count mismatch for {table_name}: source={source_count}, target={target_count}")
                return False
            
            logger.info(f"Load verification passed for {table_name}")
            return True
            
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during load verification for {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query failed during load verification for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during load verification for {table_name}: {str(e)}")
            return False
    

    
    def _build_load_query(self, table_name: str, incremental_columns: List[str], force_full: bool = False) -> str:
        """
        Build query to get data from MySQL.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns
            force_full: Whether to force a full load instead of incremental
            
        Returns:
            str: SQL query
        """
        # Check if we're in test environment using Settings instead of direct env access
        is_test_environment = self.settings.environment.lower() == 'test'
        
        # Only use column-specific SELECT in test environments
        if is_test_environment:
            # Get PostgreSQL column list to ensure we only select columns that exist in target
            try:
                inspector = inspect(self.analytics_engine)
                pg_columns = inspector.get_columns(table_name, schema=self.analytics_schema)
                pg_column_names = [col['name'] for col in pg_columns]
                
                # Filter to only include columns that exist in PostgreSQL table
                # This handles cases where MySQL has more columns than PostgreSQL (like in test environments)
                available_columns = []
                for col_name in pg_column_names:
                    # Check if column exists in MySQL replication table
                    # For now, we'll assume all PostgreSQL columns exist in MySQL
                    # In a more robust implementation, we could check MySQL schema too
                    available_columns.append(col_name)
                
                if not available_columns:
                    logger.error(f"No matching columns found for table {table_name}")
                    return f"SELECT 1 FROM {table_name} WHERE 1=0"  # Return empty result
                
                columns_clause = ", ".join(available_columns)
                
            except Exception as e:
                logger.warning(f"Could not get PostgreSQL columns for {table_name}, using SELECT *: {str(e)}")
                columns_clause = "*"
        else:
            # Production environment - use SELECT * as originally designed
            columns_clause = "*"
        
        # If force_full is True, return full table query
        if force_full:
            logger.info(f"Building full load query for {table_name}")
            return f"SELECT {columns_clause} FROM {table_name}"
            
        # Get last load timestamp
        last_load = self._get_last_load(table_name)
        
        if last_load and incremental_columns:
            # Build incremental condition
            conditions = []
            for col in incremental_columns:
                if is_test_environment and col not in pg_column_names:
                    # Only include if column exists in PostgreSQL (test environment only)
                    continue
                conditions.append(f"{col} > '{last_load}'")
            
            if conditions:
                where_clause = " AND ".join(conditions)
                query = f"SELECT {columns_clause} FROM {table_name} WHERE {where_clause}"
                logger.info(f"Building incremental query for {table_name} since {last_load}")
                return query
        
        logger.info(f"No incremental columns or last load timestamp found for {table_name}, using full query")
        return f"SELECT {columns_clause} FROM {table_name}"
    
    def _build_count_query(self, table_name: str, incremental_columns: List[str], force_full: bool = False) -> str:
        """
        Build count query for determining total rows to load.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns
            force_full: Whether to force a full load instead of incremental
            
        Returns:
            str: SQL count query
        """
        if force_full:
            return f"SELECT COUNT(*) FROM {table_name}"
        
        # Get last load timestamp
        last_load = self._get_last_load(table_name)
        
        if last_load and incremental_columns:
            conditions = []
            for col in incremental_columns:
                conditions.append(f"{col} > '{last_load}'")
            
            where_clause = " AND ".join(conditions)
            return f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
        
        return f"SELECT COUNT(*) FROM {table_name}"
    
    def _get_last_load(self, table_name: str) -> Optional[datetime]:
        """
        Get timestamp of last successful load.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[datetime]: Last load timestamp if available
        """
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT MAX(last_loaded)
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                """), {"table_name": table_name}).scalar()
                
                return result
                
        except Exception as e:
            logger.error(f"Error getting last load for {table_name}: {str(e)}")
            return None
    
 