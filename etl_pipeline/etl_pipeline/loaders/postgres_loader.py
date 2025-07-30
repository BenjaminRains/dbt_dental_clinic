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
from datetime import datetime, date
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
            # In test environment, only create analytics connection (replication not needed for tracking tests)
            if use_test_environment:
                self.replication_engine = None
                logger.info("Skipping replication connection in test environment")
            else:
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
            
            # Skip schema adapter creation in test environment (not needed for unit tests)
            if use_test_environment:
                self.schema_adapter = None
                logger.info("Skipping PostgresSchema adapter creation in test environment")
            else:
                self.schema_adapter = PostgresSchema(
                    postgres_schema=schema_enum,
                    settings=self.settings
                )
            
            self.target_schema = analytics_config.get('schema', 'raw')
            self.staging_schema = replication_config.get('schema', 'raw')
            
            # Validate tracking tables exist (only in production, not in test environment)
            if not use_test_environment:
                if not self._validate_tracking_tables_exist():
                    logger.error("PostgreSQL tracking tables not found. Run initialize_etl_tracking_tables.py to create them.")
                    raise ConfigurationError(
                        message="PostgreSQL tracking tables not found",
                        details={"error_type": "missing_tracking_tables", "solution": "Run initialize_etl_tracking_tables.py"}
                    )
            else:
                logger.info("Skipping tracking table validation in test environment")
            
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
    
    def _ensure_tracking_record_exists(self, table_name: str) -> bool:
        """Ensure a tracking record exists for the table with primary column support."""
        try:
            with self.analytics_engine.connect() as conn:
                # Check if record exists
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.analytics_schema}.etl_load_status 
                    WHERE table_name = :table_name
                """), {"table_name": table_name}).scalar()
                
                if result == 0:
                    # Create initial tracking record with primary column support
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_loaded, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at, _created_at, _updated_at
                        ) VALUES (
                            :table_name, '2024-01-01 00:00:00', NULL, NULL,
                            0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                    """), {"table_name": table_name})
                    conn.commit()
                    logger.info(f"Created initial tracking record for {table_name} with primary column support")
                
                return True
                
        except Exception as e:
            logger.error(f"Error ensuring tracking record for {table_name}: {str(e)}")
            return False

    def _update_load_status(self, table_name: str, rows_loaded: int, 
                           load_status: str = 'success', 
                           last_primary_value: Optional[str] = None,
                           primary_column_name: Optional[str] = None) -> bool:
        """Create or update load tracking record after successful PostgreSQL load with primary column support."""
        try:
            with self.analytics_engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.analytics_schema}.etl_load_status (
                        table_name, last_loaded, last_primary_value, primary_column_name,
                        rows_loaded, load_status, _loaded_at, _created_at, _updated_at
                    ) VALUES (
                        :table_name, CURRENT_TIMESTAMP, :last_primary_value, :primary_column_name,
                        :rows_loaded, :load_status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (table_name) DO UPDATE SET
                        last_loaded = CURRENT_TIMESTAMP,
                        last_primary_value = :last_primary_value,
                        primary_column_name = :primary_column_name,
                        rows_loaded = :rows_loaded,
                        load_status = :load_status,
                        _updated_at = CURRENT_TIMESTAMP
                """), {
                    "table_name": table_name,
                    "last_primary_value": last_primary_value,
                    "primary_column_name": primary_column_name,
                    "rows_loaded": rows_loaded,
                    "load_status": load_status
                })
                conn.commit()
                logger.info(f"Updated load status for {table_name}: {rows_loaded} rows, {load_status}, primary_value={last_primary_value}")
                return True
                
        except Exception as e:
            logger.error(f"Error updating load status for {table_name}: {str(e)}")
            return False

    def bulk_insert_optimized(self, table_name: str, rows_data: List[Dict], chunk_size: int = 100000) -> bool:  # Increased from 50000 to 100000
        """Optimized bulk INSERT using executemany with larger chunks."""
        try:
            if not rows_data:
                logger.info(f"No data to insert for {table_name}")
                return True
            
            # Process in optimized chunks
            total_inserted = 0
            for i in range(0, len(rows_data), chunk_size):
                chunk = rows_data[i:i + chunk_size]
                
                # Build optimized INSERT statement
                columns = ', '.join([f'"{col}"' for col in chunk[0].keys()])
                placeholders = ', '.join([f':{col}' for col in chunk[0].keys()])
                
                insert_sql = f"""
                    INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                    VALUES ({placeholders})
                """
                
                # Use executemany for bulk operation
                with self.analytics_engine.begin() as conn:
                    conn.execute(text(insert_sql), chunk)
                
                total_inserted += len(chunk)
                logger.debug(f"Bulk inserted {len(chunk)} rows for {table_name} (total: {total_inserted})")
            
            logger.info(f"Successfully bulk inserted {total_inserted} rows for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in bulk insert for {table_name}: {str(e)}")
            return False

    def stream_mysql_data(self, table_name: str, query: str, chunk_size: int = 100000):  # Increased from 50000 to 100000
        """Stream data from MySQL without loading into memory."""
        # In test environment, return empty generator since replication_engine is None
        if self.replication_engine is None:
            logger.info(f"Skipping MySQL data streaming for {table_name} in test environment")
            return
        
        try:
            with self.replication_engine.connect() as conn:
                # Use server-side cursor for streaming
                result = conn.execution_options(stream_results=True).execute(text(query))
                
                while True:
                    try:
                        chunk = result.fetchmany(chunk_size)
                        if not chunk:
                            break
                            
                        # Convert to list of dicts
                        column_names = list(result.keys())
                        chunk_dicts = [self._convert_sqlalchemy_row_to_dict(row, column_names) for row in chunk]
                        
                        yield chunk_dicts
                        
                    except Exception as e:
                        logger.error(f"Error fetching chunk for {table_name}: {e}")
                        raise
                        
        except Exception as e:
            logger.error(f"Error in streaming for {table_name}: {e}")
            raise
        finally:
            # Ensure result is closed
            if 'result' in locals():
                result.close()

    def load_table_streaming(self, table_name: str, force_full: bool = False) -> bool:
        """Streaming version of load_table for memory-efficient processing."""
        import psutil
        import time
        
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
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
            
            # Build query once
            query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # Handle full load truncation
            if force_full:
                with self.analytics_engine.begin() as target_conn:
                    target_conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    logger.info(f"Truncated table {self.analytics_schema}.{table_name} for full load")
            
            # Stream and process in chunks
            total_processed = 0
            for chunk in self.stream_mysql_data(table_name, query):
                # Convert types for entire chunk
                converted_chunk = [
                    self.schema_adapter.convert_row_data_types(table_name, row) 
                    for row in chunk
                ]
                
                # Bulk insert chunk
                self.bulk_insert_optimized(table_name, converted_chunk)
                
                total_processed += len(chunk)
                current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                logger.info(f"Processed {total_processed} rows for {table_name}, Memory: {current_memory:.1f}MB")
            
            end_time = time.time()
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            duration = end_time - start_time
            
            logger.info(f"Streaming load completed: {total_processed} rows in {duration:.2f}s, "
                       f"Memory: {initial_memory:.1f}MB → {final_memory:.1f}MB")
            
            # Update load status
            if total_processed > 0:
                self._update_load_status(table_name, total_processed)
            
            return True
            
        except Exception as e:
            logger.error(f"Streaming load failed for {table_name}: {e}")
            return False
    
    def _convert_sqlalchemy_row_to_dict(self, row, column_names: List[str]) -> Dict:
        """
        Convert SQLAlchemy Row object to dictionary consistently.
        
        Args:
            row: SQLAlchemy Row object or similar
            column_names: List of column names as fallback
            
        Returns:
            Dict: Converted row data
        """
        try:
            # Method 1: Modern SQLAlchemy (1.4+)
            if hasattr(row, '_mapping'):
                return dict(row._mapping)
            
            # Method 2: Named tuple
            elif hasattr(row, '_asdict'):
                return row._asdict()
            
            # Method 3: Check if it's already a dictionary
            elif isinstance(row, dict):
                return row
            
            # Method 4: Older SQLAlchemy Row objects
            elif hasattr(row, 'keys') and callable(row.keys):
                return dict(zip(row.keys(), row))
            
            # Method 5: Fallback for tuple/list with improved error handling
            else:
                # Convert SQLAlchemy Row object to proper dictionary
                if hasattr(row, '_mapping'):
                    # Modern SQLAlchemy (1.4+)
                    return dict(row._mapping)
                elif hasattr(row, '_asdict'):
                    # Named tuple
                    return row._asdict()
                elif hasattr(row, 'keys') and callable(row.keys):
                    # Older SQLAlchemy Row objects
                    return dict(zip(row.keys(), row))
                else:
                    # Fallback for tuple/list
                    return dict(zip(column_names, row))
                
        except Exception as e:
            logger.warning(f"Error converting row to dict: {str(e)}, using fallback")
            # Ultimate fallback with improved logic
            try:
                # Convert SQLAlchemy Row object to proper dictionary
                if hasattr(row, '_mapping'):
                    # Modern SQLAlchemy (1.4+)
                    return dict(row._mapping)
                elif hasattr(row, '_asdict'):
                    # Named tuple
                    return row._asdict()
                elif hasattr(row, 'keys') and callable(row.keys):
                    # Older SQLAlchemy Row objects
                    return dict(zip(row.keys(), row))
                else:
                    # Fallback for tuple/list
                    return dict(zip(column_names, row))
            except Exception as e2:
                logger.error(f"Fallback row conversion also failed: {str(e2)}")
                return {}

    def _get_last_load_time_max(self, table_name: str, incremental_columns: List[str]) -> Optional[datetime]:
        """Get the maximum last load time across all incremental columns."""
        try:
            timestamps = []
            
            with self.analytics_engine.connect() as conn:
                for column in incremental_columns:
                    # Get last load time for this specific column
                    result = conn.execute(text(f"""
                        SELECT last_loaded
                        FROM {self.analytics_schema}.etl_load_status
                        WHERE table_name = :table_name
                        AND load_status = 'success'
                        ORDER BY last_loaded DESC
                        LIMIT 1
                    """), {"table_name": table_name}).scalar()
                    
                    if result:
                        timestamps.append(result)
            
            return max(timestamps) if timestamps else None
            
        except Exception as e:
            logger.error(f"Error getting max last load time for {table_name}: {str(e)}")
            return None
    
    def _build_improved_load_query_max(self, table_name: str, incremental_columns: List[str], 
                                      force_full: bool = False, incremental_strategy: str = 'or_logic') -> str:
        """Build query with improved incremental logic using maximum timestamp across all columns."""
        
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')
        
        # Data quality validation
        valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
        
        if force_full or not valid_columns:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # Get maximum last load time across all columns
        last_load = self._get_last_load_time_max(table_name, valid_columns)
        if not last_load:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # Build conditions based on incremental strategy
        conditions = []
        for col in valid_columns:
            conditions.append(f"{col} > '{last_load}'")
        
        if incremental_strategy == 'or_logic':
            where_clause = " OR ".join(conditions)
        elif incremental_strategy == 'and_logic':
            where_clause = " AND ".join(conditions)
        elif incremental_strategy == 'single_column':
            # Use only the first column (primary incremental column)
            if valid_columns:
                where_clause = f"{valid_columns[0]} > '{last_load}'"
            else:
                where_clause = "1=0"  # No valid columns
        else:
            # Default to OR logic
            where_clause = " OR ".join(conditions)
        
        return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
    
    def _filter_valid_incremental_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """
        Filter out columns with data quality issues.
        
        Args:
            table_name: Name of the table
            columns: List of incremental columns to validate
            
        Returns:
            List[str]: Filtered list of valid incremental columns
        """
        if not columns:
            return []
        
        # In test environment, skip data quality validation since replication_engine is None
        if self.replication_engine is None:
            logger.info(f"Skipping data quality validation for {table_name} in test environment")
            return columns
        
        valid_columns = []
        
        try:
            # Get replication database name from settings
            replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
            replication_db = replication_config.get('database', 'opendental_replication')
            
            with self.replication_engine.connect() as conn:
                for column in columns:
                    # Check for data quality issues by sampling the column
                    sample_query = f"""
                        SELECT MIN({column}), MAX({column}), COUNT(*)
                        FROM `{replication_db}`.`{table_name}`
                        WHERE {column} IS NOT NULL
                        LIMIT 1000
                    """
                    
                    result = conn.execute(text(sample_query))
                    row = result.fetchone()
                    
                    if row and row[0] and row[1]:
                        min_date = row[0]
                        max_date = row[1]
                        count = row[2]
                        
                        # Skip columns with obviously bad dates
                        if isinstance(min_date, (datetime, date)):
                            if min_date.year < 2000 or max_date.year > 2030:
                                logger.warning(f"Column {column} in {table_name} has bad date range: {min_date} to {max_date}")
                                continue
                        
                        # Skip columns with too many NULL values (indicating poor data quality)
                        if count < 100:  # Less than 100 non-null values in sample
                            logger.warning(f"Column {column} in {table_name} has poor data quality: only {count} non-null values")
                            continue
                        
                        valid_columns.append(column)
                        logger.debug(f"Column {column} in {table_name} passed data quality validation")
                    else:
                        logger.warning(f"Column {column} in {table_name} has no valid data")
                        
        except Exception as e:
            logger.warning(f"Could not validate data quality for columns in {table_name}: {str(e)}")
            # If validation fails, return original columns (fail open)
            return columns
        
        return valid_columns
    
    def _get_last_load_time(self, table_name: str) -> Optional[datetime]:
        """Get last load time for incremental loading."""
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT last_loaded
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    ORDER BY last_loaded DESC
                    LIMIT 1
                """), {"table_name": table_name}).scalar()
                
                return result
        except Exception as e:
            logger.error(f"Error getting last load time for {table_name}: {str(e)}")
            return None
    
    def load_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Load table from MySQL replication to PostgreSQL analytics using pure SQLAlchemy.
        This approach avoids pandas' connection handling issues entirely.
        
        ENHANCED WITH AUTOMATIC STRATEGY SELECTION:
        - Small tables (< 50MB): Standard loading
        - Medium tables (50-200MB): Streaming loading  
        - Large tables (200-500MB): Chunked loading
        - Very large tables (> 500MB): COPY command
        - Massive tables (> 1M rows): Parallel processing
        
        Args:
            table_name: Name of the table to load
            force_full: Whether to force a full load instead of incremental
            
        Returns:
            bool: True if successful
        """
        import time
        import psutil
        
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Get table configuration for strategy selection
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            estimated_size_mb = table_config.get('estimated_size_mb', 0)
            estimated_rows = table_config.get('estimated_rows', 0)
            
            # Enhanced strategy selection with parallel processing
            if estimated_rows > 1_000_000:
                logger.info(f"Using parallel processing for massive table {table_name} ({estimated_rows:,} rows)")
                strategy = "parallel"
                success = self.load_table_parallel(table_name, force_full)
            elif estimated_size_mb > 500:
                logger.info(f"Using COPY command for very large table {table_name} ({estimated_size_mb}MB)")
                strategy = "copy_csv"
                success = self.load_table_copy_csv(table_name, force_full)
            elif estimated_size_mb > 200:
                logger.info(f"Using chunked loading for large table {table_name} ({estimated_size_mb}MB)")
                strategy = "chunked"
                success = self.load_table_chunked(table_name, force_full, chunk_size=100000)  # Increased from 50000 to 100000
            elif estimated_size_mb > 50:
                logger.info(f"Using streaming loading for medium table {table_name} ({estimated_size_mb}MB)")
                strategy = "streaming"
                success = self.load_table_streaming(table_name, force_full)
            else:
                logger.info(f"Using standard loading for small table {table_name} ({estimated_size_mb}MB)")
                strategy = "standard"
                success = self.load_table_standard(table_name, force_full)
            
            # Track performance metrics
            if success:
                end_time = time.time()
                final_memory = psutil.Process().memory_info().rss / 1024 / 1024
                duration = end_time - start_time
                memory_used = final_memory - initial_memory
                
                # Get actual rows processed from tracking
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                    rows_processed = result or 0
                
                self.track_performance_metrics(table_name, strategy, duration, memory_used, rows_processed)
            
            return success
                
        except Exception as e:
            logger.error(f"Error in load_table for {table_name}: {str(e)}")
            return False

    def load_table_standard(self, table_name: str, force_full: bool = False) -> bool:
        """
        Standard table loading method (original implementation).
        Used for small tables that can be loaded entirely into memory.
        """
        try:
            # Ensure tracking record exists before loading
            if not self._ensure_tracking_record_exists(table_name):
                logger.error(f"Failed to ensure tracking record for {table_name}")
                return False
            
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            
            # Get incremental strategy from configuration
            incremental_strategy = self._get_incremental_strategy(table_config)
            
            # Log the strategy being used
            self._log_incremental_strategy(table_name, primary_column, incremental_columns, incremental_strategy)
            
            # Get MySQL schema from replication database
            mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
            
            # Create or verify PostgreSQL table
            if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                return False
            
            # Apply data quality validation to incremental columns
            if incremental_columns:
                valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
                if len(valid_columns) != len(incremental_columns):
                    logger.warning(f"Data quality validation filtered {len(incremental_columns) - len(valid_columns)} columns for {table_name}")
                incremental_columns = valid_columns
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Validate incremental integrity before loading
            if not force_full and incremental_columns:
                last_load = self._get_last_load_time_max(table_name, incremental_columns)
                if last_load:
                    self._validate_incremental_integrity(table_name, incremental_columns, last_load)
            
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
                row_dict = self._convert_sqlalchemy_row_to_dict(row, column_names)
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
                    # Use UPSERT for incremental loads to handle duplicate keys
                    if force_full:
                        # For full loads, use simple INSERT (table is truncated)
                        columns = ', '.join([f'"{col}"' for col in column_names])
                        placeholders = ', '.join([f':{col}' for col in column_names])
                        
                        insert_sql = f"""
                            INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                            VALUES ({placeholders})
                        """
                    else:
                        # For incremental loads, use UPSERT to handle duplicate keys
                        insert_sql = self._build_upsert_sql(table_name, column_names)
                    
                    # Execute bulk insert/upsert
                    target_conn.execute(text(insert_sql), rows_data)
                    
                    logger.info(f"Loaded {len(rows_data)} rows to {self.analytics_schema}.{table_name} using {'full' if force_full else 'incremental'} strategy")
            
            # After successful load, update tracking record with primary column value
            if rows_data:
                # Get the maximum value of the primary column from loaded data
                last_primary_value = None
                if primary_column and primary_column != 'none':
                    # Extract the maximum value of the primary column from loaded data
                    primary_values = [row.get(primary_column) for row in rows_data if row.get(primary_column) is not None]
                    if primary_values:
                        last_primary_value = str(max(primary_values))
                
                self._update_load_status(
                    table_name, len(rows_data), 'success', last_primary_value, primary_column
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error in standard load for {table_name}: {str(e)}")
            # Update tracking record with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False
    
    def load_table_chunked(self, table_name: str, force_full: bool = False, 
                          chunk_size: int = 50000) -> bool:
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
            # Ensure tracking record exists before loading
            if not self._ensure_tracking_record_exists(table_name):
                logger.error(f"Failed to ensure tracking record for {table_name}")
                return False
            
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            
            # Get incremental strategy from configuration
            incremental_strategy = self._get_incremental_strategy(table_config)
            
            # Log the strategy being used
            self._log_incremental_strategy(table_name, primary_column, incremental_columns, incremental_strategy)
            
            # Get MySQL schema from replication database
            mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
            
            # Create or verify PostgreSQL table
            if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                return False
            
            # Apply data quality validation to incremental columns
            if incremental_columns:
                valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
                if len(valid_columns) != len(incremental_columns):
                    logger.warning(f"Data quality validation filtered {len(incremental_columns) - len(valid_columns)} columns for {table_name}")
                incremental_columns = valid_columns
            
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
                logger.info(f"DEBUG: Count query result for {table_name}: {total_rows} rows")
            
            if total_rows is None or total_rows == 0:
                logger.info(f"No new data to load for {table_name}")
                return True
            
            # Ensure total_rows is an integer
            total_rows = int(total_rows)
            
            logger.info(f"Loading {total_rows} rows from {table_name} in chunks of {chunk_size}")
            
            # Process in chunks
            total_loaded = 0
            chunk_num = 0
            all_primary_values = []  # Track primary column values across all chunks
            
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
                        row_dict = self._convert_sqlalchemy_row_to_dict(row, column_names)
                        converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                        rows_data.append(converted_row)
                        
                        # Track primary column values if available
                        if primary_column and primary_column != 'none':
                            primary_value = converted_row.get(primary_column)
                            if primary_value is not None:
                                all_primary_values.append(primary_value)
                
                # Insert chunk
                with self.analytics_engine.begin() as target_conn:
                    # Use UPSERT for incremental loads to handle duplicate keys
                    if force_full:
                        # For full loads, use simple INSERT (table is truncated)
                        columns = ', '.join([f'"{col}"' for col in column_names])
                        placeholders = ', '.join([f':{col}' for col in column_names])
                        
                        insert_sql = f"""
                            INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                            VALUES ({placeholders})
                        """
                    else:
                        # For incremental loads, use UPSERT to handle duplicate keys
                        insert_sql = self._build_upsert_sql(table_name, column_names)
                    
                    target_conn.execute(text(insert_sql), rows_data)
                
                chunk_rows = len(rows)
                total_loaded += chunk_rows
                
                logger.info(f"Loaded chunk {chunk_num}: {total_loaded}/{total_rows} rows ({(total_loaded/total_rows)*100:.1f}%)")
                
                if chunk_rows < chunk_size:
                    break
            
            # After successful load, update tracking record with primary column value
            if total_loaded > 0:
                # Get the maximum value of the primary column from loaded data
                last_primary_value = None
                if primary_column and primary_column != 'none' and all_primary_values:
                    last_primary_value = str(max(all_primary_values))
                
                self._update_load_status(
                    table_name, total_loaded, 'success', last_primary_value, primary_column
                )
            
            logger.info(f"Successfully loaded {total_loaded} rows to {self.analytics_schema}.{table_name}")
            return True
            
        except DataLoadingError as e:
            logger.error(f"Data loading failed in chunked load for table {table_name}: {e}")
            # Update tracking record with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed in chunked load for table {table_name}: {e}")
            # Update tracking record with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False
        except DatabaseTransactionError as e:
            logger.error(f"Database transaction failed in chunked load for table {table_name}: {e}")
            # Update tracking record with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query failed in chunked load for table {table_name}: {e}")
            # Update tracking record with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
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
    

    
    def _build_enhanced_load_query(self, table_name: str, incremental_columns: List[str], 
                                  primary_column: Optional[str] = None,
                                  force_full: bool = False, incremental_strategy: str = 'or_logic') -> str:
        """Build query with enhanced incremental logic supporting primary column and strategy configuration."""
        
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')
        
        # Data quality validation
        valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
        
        if force_full or not valid_columns:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # Use primary column logic if available
        if primary_column and primary_column != 'none':
            last_primary_value = self._get_last_primary_value(table_name)
            if last_primary_value:
                # Use primary column for incremental logic
                return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {primary_column} > '{last_primary_value}'"
            else:
                # No primary value found, use timestamp fallback
                last_load = self._get_last_load_time(table_name)
                if last_load:
                    return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {primary_column} > '{last_load}'"
                else:
                    return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        else:
            # Use multi-column logic with strategy-based logic
            last_load = self._get_last_load_time_max(table_name, valid_columns)
            if not last_load:
                return f"SELECT * FROM `{replication_db}`.`{table_name}`"
            
            # Build conditions based on incremental strategy
            conditions = []
            for col in valid_columns:
                conditions.append(f"{col} > '{last_load}'")
            
            if incremental_strategy == 'or_logic':
                where_clause = " OR ".join(conditions)
            elif incremental_strategy == 'and_logic':
                where_clause = " AND ".join(conditions)
            elif incremental_strategy == 'single_column':
                # Use only the first column (primary incremental column)
                if valid_columns:
                    where_clause = f"{valid_columns[0]} > '{last_load}'"
                else:
                    where_clause = "1=0"  # No valid columns
            else:
                # Default to OR logic
                where_clause = " OR ".join(conditions)
            
            return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"

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
        # Get configuration and strategy
        table_config = self.get_table_config(table_name)
        primary_column = self._get_primary_incremental_column(table_config) if table_config else None
        incremental_strategy = self._get_incremental_strategy(table_config) if table_config else 'or_logic'
        
        # Log the strategy being used
        self._log_incremental_strategy(table_name, primary_column, incremental_columns, incremental_strategy)
        
        return self._build_enhanced_load_query(table_name, incremental_columns, primary_column, force_full, incremental_strategy)
    
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
        # Get configuration and strategy
        table_config = self.get_table_config(table_name)
        incremental_strategy = self._get_incremental_strategy(table_config) if table_config else 'or_logic'
        
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')
        
        if force_full:
            query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
            logger.info(f"DEBUG: Force full count query for {table_name}: {query}")
            return query
        
        # Get maximum last load timestamp across all columns
        last_load = self._get_last_load_time_max(table_name, incremental_columns)
        
        if last_load and incremental_columns:
            conditions = []
            for col in incremental_columns:
                conditions.append(f"{col} > '{last_load}'")
            
            # Use strategy-based logic
            if incremental_strategy == 'or_logic':
                where_clause = " OR ".join(conditions)
            elif incremental_strategy == 'and_logic':
                where_clause = " AND ".join(conditions)
            elif incremental_strategy == 'single_column':
                # Use only the first column
                if incremental_columns:
                    where_clause = f"{incremental_columns[0]} > '{last_load}'"
                else:
                    where_clause = "1=0"
            else:
                # Default to OR logic
                where_clause = " OR ".join(conditions)
            
            query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
            logger.info(f"DEBUG: Incremental count query for {table_name} ({incremental_strategy}): {query}")
            return query
        
        query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
        logger.info(f"DEBUG: Default count query for {table_name}: {query}")
        return query
    
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
    
    def _build_upsert_sql(self, table_name: str, column_names: List[str]) -> str:
        """
        Build PostgreSQL UPSERT SQL with dynamic primary key handling.
        
        PostgreSQL uses: INSERT ... ON CONFLICT DO UPDATE
        with explicit conflict target and EXCLUDED table reference.
        
        Args:
            table_name: Name of the table
            column_names: List of column names to insert
            
        Returns:
            str: PostgreSQL UPSERT SQL statement
        """
        # Get primary key from table configuration
        table_config = self.get_table_config(table_name)
        primary_key = table_config.get('primary_key', 'id')
        
        # Build column lists
        columns = ', '.join([f'"{col}"' for col in column_names])
        placeholders = ', '.join([f':{col}' for col in column_names])
        
        # Build UPDATE clause (exclude primary key from updates)
        update_columns = [f'"{col}" = EXCLUDED."{col}"' 
                         for col in column_names if col != primary_key]
        update_clause = ', '.join(update_columns) if update_columns else 'updated_at = CURRENT_TIMESTAMP'
        
        return f"""
            INSERT INTO {self.analytics_schema}.{table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ("{primary_key}") DO UPDATE SET
                {update_clause}
        """
 
    def _validate_incremental_integrity(self, table_name: str, incremental_columns: List[str], last_load: datetime) -> bool:
        """
        Validate that incremental logic isn't missing records.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns
            last_load: Last load timestamp
            
        Returns:
            bool: True if validation passes
        """
        try:
            if not incremental_columns or not last_load:
                return True  # Skip validation if no incremental columns or last load
            
            with self.replication_engine.connect() as conn:
                # Check if any records exist between last_load and now that aren't captured
                conditions = []
                for col in incremental_columns:
                    conditions.append(f"{col} > '{last_load}'")
                
                validation_query = f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE {' OR '.join(conditions)}
                """
                
                result = conn.execute(text(validation_query))
                total_new_records = result.scalar() or 0
                
                if total_new_records > 0:
                    logger.info(f"Incremental integrity validation for {table_name}: {total_new_records} new records found since {last_load}")
                else:
                    logger.info(f"Incremental integrity validation for {table_name}: No new records found since {last_load}")
                
                return True
                
        except Exception as e:
            logger.warning(f"Incremental integrity validation failed for {table_name}: {str(e)}")
            return False

    def _validate_data_completeness(self, table_name: str, expected_count: int, actual_count: int) -> bool:
        """
        Validate that the expected number of records were loaded.
        
        Args:
            table_name: Name of the table
            expected_count: Expected number of records
            actual_count: Actual number of records loaded
            
        Returns:
            bool: True if validation passes
        """
        if actual_count < expected_count * 0.9:  # Allow 10% variance
            logger.warning(f"Data completeness check failed for {table_name}: expected {expected_count}, got {actual_count}")
            return False
        
        logger.info(f"Data completeness validated for {table_name}: {actual_count} records")
        return True
 
    def load_table_copy_csv(self, table_name: str, force_full: bool = False) -> bool:
        """Use PostgreSQL COPY command with CSV for maximum speed."""
        import tempfile
        import csv
        import os
        
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
            
            # Create temporary CSV file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csvfile:
                # Stream from MySQL directly to CSV
                with self.replication_engine.connect() as source_conn:
                    result = source_conn.execution_options(stream_results=True).execute(text(query))
                    
                    writer = csv.writer(csvfile)
                    column_names = list(result.keys())
                    
                    # Write header
                    writer.writerow(column_names)
                    
                    # Stream rows directly to CSV
                    chunk_size = 50000
                    total_rows = 0
                    while True:
                        chunk = result.fetchmany(chunk_size)
                        if not chunk:
                            break
                        writer.writerows(chunk)
                        total_rows += len(chunk)
                        logger.debug(f"Wrote {len(chunk)} rows to CSV for {table_name} (total: {total_rows})")
                
                csv_path = csvfile.name
            
            try:
                # Use PostgreSQL COPY command
                with self.analytics_engine.begin() as target_conn:
                    if force_full:
                        target_conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    
                    copy_sql = f"""
                        COPY {self.analytics_schema}.{table_name} ({','.join([f'"{col}"' for col in column_names])})
                        FROM '{csv_path}'
                        WITH (FORMAT csv, HEADER true, DELIMITER ',')
                    """
                    target_conn.execute(text(copy_sql))
                    
                logger.info(f"Successfully loaded {table_name} using COPY command: {total_rows} rows")
                
                # Update load status
                if total_rows > 0:
                    self._update_load_status(table_name, total_rows)
                
                return True
                
            finally:
                # Clean up temp file
                if os.path.exists(csv_path):
                    os.unlink(csv_path)
                    
        except Exception as e:
            logger.error(f"Error in COPY load for {table_name}: {str(e)}")
            return False
 
    def load_table_parallel(self, table_name: str, force_full: bool = False) -> bool:
        """Parallel chunk processing for massive tables."""
        import os
        from concurrent.futures import ThreadPoolExecutor
        import time
        
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get incremental columns from configuration
            incremental_columns = table_config.get('incremental_columns', [])
            
            # Get total row count
            count_query = self._build_count_query(table_name, incremental_columns, force_full)
            with self.replication_engine.connect() as conn:
                total_rows = conn.execute(text(count_query)).scalar() or 0
            
            if total_rows < 100000:
                # Not worth parallelizing small tables
                logger.info(f"Table {table_name} has only {total_rows} rows, using streaming instead of parallel")
                return self.load_table_streaming(table_name, force_full)
            
            # Calculate optimal chunk size and worker count
            cpu_count = os.cpu_count() or 4
            chunk_size = max(100000, total_rows // (cpu_count * 2))  # Increased from 50000 to 100000
            
            # Create chunks
            chunks = [(i * chunk_size, min((i + 1) * chunk_size, total_rows)) 
                      for i in range(0, total_rows, chunk_size)]
            
            logger.info(f"Processing {table_name} in {len(chunks)} parallel chunks with {cpu_count} workers")
            
            # Process chunks in parallel
            with ThreadPoolExecutor(max_workers=cpu_count) as executor:
                futures = [
                    executor.submit(self._process_chunk_parallel, table_name, start, end, force_full, incremental_columns)
                    for start, end in chunks
                ]
                
                # Wait for all chunks to complete
                results = [future.result() for future in futures]
            
            success = all(results)
            if success:
                logger.info(f"Successfully processed {table_name} in parallel: {total_rows} rows")
                self._update_load_status(table_name, total_rows)
            
            return success
            
        except Exception as e:
            logger.error(f"Error in parallel load for {table_name}: {str(e)}")
            return False

    def _process_chunk_parallel(self, table_name: str, start: int, end: int, force_full: bool, incremental_columns: List[str]) -> bool:
        """Process a single chunk in parallel."""
        from etl_pipeline.core.connections import ConnectionFactory
        
        try:
            # Each thread gets its own connections
            replication_engine = ConnectionFactory.get_replication_connection(self.settings)
            analytics_engine = ConnectionFactory.get_analytics_raw_connection(self.settings)
            
            # Build chunk query
            base_query = self._build_load_query(table_name, incremental_columns, force_full)
            chunk_query = f"{base_query} LIMIT {end - start} OFFSET {start}"
            
            # Process chunk
            with replication_engine.connect() as source_conn:
                result = source_conn.execute(text(chunk_query))
                column_names = list(result.keys())
                rows = result.fetchall()
                
                # Convert types
                rows_data = []
                for row in rows:
                    row_dict = self._convert_sqlalchemy_row_to_dict(row, column_names)
                    converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                    rows_data.append(converted_row)
            
            # Bulk insert chunk
            with analytics_engine.begin() as target_conn:
                columns = ', '.join([f'"{col}"' for col in column_names])
                placeholders = ', '.join([f':{col}' for col in column_names])
                
                insert_sql = f"""
                    INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                    VALUES ({placeholders})
                """
                target_conn.execute(text(insert_sql), rows_data)
            
            logger.debug(f"Completed chunk {start}-{end} for {table_name}: {len(rows_data)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Failed chunk {start}-{end} for {table_name}: {e}")
            return False

    def track_performance_metrics(self, table_name: str, strategy: str, duration: float, memory_mb: float, rows_processed: int):
        """Track performance metrics for different strategies."""
        if not hasattr(self, 'performance_metrics'):
            self.performance_metrics = {}
        
        self.performance_metrics[table_name] = {
            'strategy': strategy,
            'duration': duration,
            'memory_mb': memory_mb,
            'rows_processed': rows_processed,
            'rows_per_second': rows_processed / duration if duration > 0 else 0,
            'timestamp': datetime.now()
        }
        
        logger.info(f"Performance metrics for {table_name}: {strategy} strategy, "
                   f"{rows_processed} rows in {duration:.2f}s ({rows_processed / duration:.0f} rows/sec), "
                   f"Memory: {memory_mb:.1f}MB")

    def get_performance_report(self) -> str:
        """Generate a comprehensive performance report."""
        if not hasattr(self, 'performance_metrics') or not self.performance_metrics:
            return "No performance metrics available."
        
        report = ["# ETL Performance Report", ""]
        
        for table_name, metrics in self.performance_metrics.items():
            report.append(f"## {table_name}")
            report.append(f"- Strategy: {metrics['strategy']}")
            report.append(f"- Duration: {metrics['duration']:.2f}s")
            report.append(f"- Rows Processed: {metrics['rows_processed']:,}")
            report.append(f"- Rows/Second: {metrics['rows_per_second']:.0f}")
            report.append(f"- Memory Usage: {metrics['memory_mb']:.1f}MB")
            report.append("")
        
        return "\n".join(report)
 
    def _get_last_primary_value(self, table_name: str) -> Optional[str]:
        """Get the last primary column value for incremental loading."""
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT last_primary_value, primary_column_name
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    ORDER BY last_loaded DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    last_primary_value, primary_column_name = result
                    logger.debug(f"Retrieved last primary value for {table_name}: {last_primary_value} (column: {primary_column_name})")
                    return last_primary_value
                return None
                
        except Exception as e:
            logger.error(f"Error getting last primary value for {table_name}: {str(e)}")
            return None

    def _get_primary_incremental_column(self, config: Dict) -> Optional[str]:
        """Get the primary incremental column from configuration with fallback logic."""
        primary_column = config.get('primary_incremental_column')
        
        # Check if primary column is valid (not null, 'none', or empty)
        if primary_column and primary_column != 'none' and primary_column.strip():
            return primary_column
        
        # Fallback: if no primary column specified, return None to use multi-column logic
        return None

    def _get_incremental_strategy(self, config: Dict) -> str:
        """Get the incremental strategy from table configuration."""
        return config.get('incremental_strategy', 'or_logic')

    def _log_incremental_strategy(self, table_name: str, primary_column: Optional[str], incremental_columns: List[str], strategy: str):
        """Log which incremental strategy is being used."""
        if primary_column and primary_column != 'none':
            logger.info(f"Table {table_name}: Using {strategy} logic with primary incremental column '{primary_column}' for optimized incremental loading")
        else:
            logger.info(f"Table {table_name}: Using {strategy} logic with multi-column incremental logic: {incremental_columns}")
 
    def _validate_tracking_tables_exist(self) -> bool:
        """Validate that PostgreSQL tracking tables exist in analytics database."""
        try:
            with self.analytics_engine.connect() as conn:
                # Check if etl_load_status table exists
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.analytics_schema}' 
                    AND table_name = 'etl_load_status'
                """)).scalar()
                
                if result == 0:
                    logger.error(f"PostgreSQL tracking table '{self.analytics_schema}.etl_load_status' not found")
                    return False
                
                # Check if etl_transform_status table exists
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.analytics_schema}' 
                    AND table_name = 'etl_transform_status'
                """)).scalar()
                
                if result == 0:
                    logger.error(f"PostgreSQL tracking table '{self.analytics_schema}.etl_transform_status' not found")
                    return False
                
                # Check if tables have the expected structure with primary column support
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.analytics_schema}' 
                    AND table_name = 'etl_load_status' 
                    AND column_name IN ('last_primary_value', 'primary_column_name')
                """)).scalar()
                
                if result < 2:
                    logger.error(f"PostgreSQL tracking table '{self.analytics_schema}.etl_load_status' missing primary column support columns")
                    return False
                
                result = conn.execute(text(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.analytics_schema}' 
                    AND table_name = 'etl_transform_status' 
                    AND column_name IN ('last_primary_value', 'primary_column_name')
                """)).scalar()
                
                if result < 2:
                    logger.error(f"PostgreSQL tracking table '{self.analytics_schema}.etl_transform_status' missing primary column support columns")
                    return False
                
                logger.info("PostgreSQL tracking tables validated successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error validating PostgreSQL tracking tables: {str(e)}")
            return False
 