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
from typing import Dict, List, Optional, Any, Tuple, Union
from sqlalchemy import text, inspect, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
import os
import yaml
import time
import psutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Method usage tracking imports
import sys
from pathlib import Path
scripts_path = Path(__file__).parent.parent.parent / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
from method_tracker import track_method, save_tracking_report, print_tracking_report # type: ignore

logger = get_logger(__name__)

class SchemaCache:
    """
    Cache for MySQL schema information to avoid repeated expensive queries.
    
    This class caches schema analysis results to improve performance
    when loading multiple tables or retrying failed operations.
    """
    
    def __init__(self):
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 3600  # 1 hour TTL
    
    def get_cached_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get cached schema for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Cached schema dict or None if not cached/expired
        """
        if table_name not in self._cache:
            return None
        
        # Check if cache is still valid
        timestamp = self._cache_timestamps.get(table_name, 0)
        if time.time() - timestamp > self._cache_ttl:
            # Cache expired, remove entry
            del self._cache[table_name]
            if table_name in self._cache_timestamps:
                del self._cache_timestamps[table_name]
            return None
        
        return self._cache[table_name]
    
    def cache_schema(self, table_name: str, schema: Dict) -> None:
        """
        Cache schema for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema dictionary to cache
        """
        self._cache[table_name] = schema
        self._cache_timestamps[table_name] = time.time()
        logger.debug(f"Cached schema for table {table_name}")
    
    def invalidate_cache(self, table_name: Optional[str] = None) -> None:
        """
        Invalidate cache for a specific table or all tables.
        
        Args:
            table_name: Name of the table to invalidate, or None for all tables
        """
        if table_name:
            if table_name in self._cache:
                del self._cache[table_name]
            if table_name in self._cache_timestamps:
                del self._cache_timestamps[table_name]
            logger.debug(f"Invalidated cache for table {table_name}")
        else:
            self._cache.clear()
            self._cache_timestamps.clear()
            logger.debug("Invalidated all schema cache entries")
    
    def get_cache_stats(self) -> Dict:
        """
        Get cache statistics.
        
        Returns:
            Dict with cache statistics
        """
        return {
            'cached_tables': len(self._cache),
            'cache_size': sum(len(str(schema)) for schema in self._cache.values()),
            'oldest_entry': min(self._cache_timestamps.values()) if self._cache_timestamps else None,
            'newest_entry': max(self._cache_timestamps.values()) if self._cache_timestamps else None
        }

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
            
            # IMPROVED: Initialize schema cache to avoid repeated expensive schema analysis
            self.schema_cache = SchemaCache()
            logger.info("Initialized schema cache for performance optimization")
            
            # Get database connections using unified interface with Settings injection
            # In test environment, only create analytics connection (replication not needed for tracking tests)
            if use_test_environment:
                self.replication_engine = ConnectionFactory.get_source_connection(self.settings)
                logger.info("Using source database as replication database in test environment")
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
            
            # Validate tracking tables exist (with warning in test environment)
            if not self._validate_tracking_tables_exist():
                if use_test_environment:
                    logger.warning("PostgreSQL tracking tables not found in test environment. Run initialize_etl_tracking_tables.py to create them.")
                else:
                    logger.error("PostgreSQL tracking tables not found. Run initialize_etl_tracking_tables.py to create them.")
                    raise ConfigurationError(
                        message="PostgreSQL tracking tables not found",
                        details={"error_type": "missing_tracking_tables", "solution": "Run initialize_etl_tracking_tables.py"}
                    )
            else:
                if use_test_environment:
                    logger.info("PostgreSQL tracking tables validated in test environment")
                else:
                    logger.info("PostgreSQL tracking tables validated in production environment")
            
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
    
    def _get_cached_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get MySQL schema with caching to avoid repeated expensive queries.
        
        IMPROVED: Uses schema cache to avoid repeated expensive schema analysis.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Cached schema dict or None if not available
        """
        if self.schema_adapter is None:
            logger.warning("Schema adapter not available in test environment, using mock schema")
            # Return a mock schema for test environment to allow loading to proceed
            return {
                'columns': [
                    {'name': 'id', 'type': 'int', 'nullable': False},
                    {'name': 'name', 'type': 'varchar', 'nullable': True},
                    {'name': 'created_at', 'type': 'datetime', 'nullable': True},
                    {'name': 'updated_at', 'type': 'datetime', 'nullable': True}
                ],
                'primary_key': ['id'],
                'table_name': table_name
            }
        
        # Try to get from cache first
        cached_schema = self.schema_cache.get_cached_schema(table_name)
        if cached_schema is not None:
            logger.debug(f"Using cached schema for table {table_name}")
            return cached_schema
        
        # If not in cache, get from schema adapter and cache it
        try:
            logger.debug(f"Fetching schema for table {table_name} from MySQL")
            mysql_schema = self.schema_adapter.get_table_schema_from_mysql(table_name)
            
            # Cache the schema for future use
            self.schema_cache.cache_schema(table_name, mysql_schema)
            logger.debug(f"Cached schema for table {table_name}")
            
            return mysql_schema
            
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            return None
    
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
                    # Note: Only using columns that actually exist in the table schema
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at
                        ) VALUES (
                            :table_name, NULL, NULL,
                            0, 'pending', CURRENT_TIMESTAMP
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
        """
        Create or update load tracking record after successful PostgreSQL load with primary column support.
        
        ENHANCED: Only update timestamp when rows are actually loaded to prevent false "up to date" status.
        """
        try:
            with self.analytics_engine.connect() as conn:
                # Ensure primary_column_name is always populated from config when available
                if not primary_column_name:
                    try:
                        table_config = self.get_table_config(table_name)
                        primary_column_from_config = self._get_primary_incremental_column(table_config) if table_config else None
                        if primary_column_from_config:
                            primary_column_name = primary_column_from_config
                            logger.debug(f"Using primary_column_name from config for {table_name}: {primary_column_name}")
                    except Exception as cfg_err:
                        logger.warning(f"Could not resolve primary_column_name from config for {table_name}: {str(cfg_err)}")

                # Optional write-time fallback: if 0 rows loaded and last_primary_value is NULL but analytics has data,
                # compute MAX(primary_column) now to persist a correct high-water mark immediately
                if rows_loaded == 0 and last_primary_value is None and primary_column_name:
                    try:
                        computed_max = conn.execute(text(f"""
                            SELECT MAX("{primary_column_name}")
                            FROM {self.analytics_schema}.{table_name}
                        """)).scalar()
                        if computed_max is not None:
                            last_primary_value = str(computed_max)
                            logger.info(f"Write-time computed last_primary_value for {table_name}: {last_primary_value}")
                    except Exception as compute_err:
                        logger.warning(f"Could not compute write-time MAX({primary_column_name}) for {table_name}: {str(compute_err)}")
                # Use simplified schema - audit columns are not needed
                if rows_loaded > 0:
                    # Only update timestamp when rows are actually loaded
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at
                        ) VALUES (
                            :table_name, :last_primary_value, :primary_column_name,
                            :rows_loaded, :load_status, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (table_name) DO UPDATE SET
                            last_primary_value = :last_primary_value,
                            primary_column_name = :primary_column_name,
                            rows_loaded = :rows_loaded,
                            load_status = :load_status,
                            _loaded_at = CURRENT_TIMESTAMP
                    """), {
                        "table_name": table_name,
                        "last_primary_value": last_primary_value,
                        "primary_column_name": primary_column_name,
                        "rows_loaded": rows_loaded,
                        "load_status": load_status
                    })
                else:
                    # When no rows are loaded, only update the status but keep the existing timestamp
                    conn.execute(text(f"""
                        INSERT INTO {self.analytics_schema}.etl_load_status (
                            table_name, last_primary_value, primary_column_name,
                            rows_loaded, load_status, _loaded_at
                        ) VALUES (
                            :table_name, :last_primary_value, :primary_column_name,
                            :rows_loaded, :load_status, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (table_name) DO UPDATE SET
                            last_primary_value = :last_primary_value,
                            primary_column_name = :primary_column_name,
                            rows_loaded = :rows_loaded,
                            load_status = :load_status,
                            _loaded_at = CURRENT_TIMESTAMP
                    """), {
                        "table_name": table_name,
                        "last_primary_value": last_primary_value,
                        "primary_column_name": primary_column_name,
                        "rows_loaded": rows_loaded,
                        "load_status": load_status
                    })
                
                conn.commit()
                logger.info(f"Tracking table updated: {self.analytics_schema}.etl_load_status for {table_name}")
                logger.info(f"Updated load status for {table_name}: {rows_loaded} rows, {load_status}, primary_value={last_primary_value}")
                return True
                
        except Exception as e:
            logger.error(f"Error updating load status for {table_name}: {str(e)}")
            return False

    def bulk_insert_optimized(self, table_name: str, rows_data: List[Dict], chunk_size: int = 25000, use_upsert: bool = True) -> bool:  # Further reduced from 50000 to 25000 to prevent timeouts
        """
        Optimized bulk INSERT using executemany with strategy-based chunk sizes.
        
        PHASE 1 IMPROVEMENT: Uses optimized batch sizes based on insert strategy:
        - simple_insert: 50,000 rows (no conflicts, larger batches OK)
        - optimized_upsert: 5,000 rows (reduce conflict overhead)
        - copy_csv: 100,000 rows (COPY can handle large batches)
        """
        try:
            if not rows_data:
                logger.info(f"No data to insert for {table_name}")
                return True
            
            # Determine optimal batch size based on strategy
            if use_upsert:
                # For UPSERT operations, use smaller batches to reduce conflict overhead
                optimal_batch_size = min(chunk_size, 5000)  # Cap at 5K for UPSERT
                logger.debug(f"Using UPSERT-optimized batch size: {optimal_batch_size}")
            else:
                # For simple INSERT, can use larger batches
                optimal_batch_size = min(chunk_size, 50000)  # Cap at 50K for INSERT
                logger.debug(f"Using INSERT-optimized batch size: {optimal_batch_size}")
            
            # Process in optimized chunks
            total_inserted = 0
            for i in range(0, len(rows_data), optimal_batch_size):
                chunk = rows_data[i:i + optimal_batch_size]
                
                # Build optimized INSERT statement
                columns = ', '.join([f'"{col}"' for col in chunk[0].keys()])
                placeholders = ', '.join([f':{col}' for col in chunk[0].keys()])
                
                if use_upsert:
                    # Use UPSERT to handle duplicate keys
                    insert_sql = self._build_upsert_sql(table_name, list(chunk[0].keys()))
                    logger.debug(f"Using UPSERT for {table_name} chunk {i//optimal_batch_size + 1} with {len(chunk)} rows")
                else:
                    # Use simple INSERT (for full loads where table is truncated)
                    insert_sql = f"""
                        INSERT INTO {self.analytics_schema}.{table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    logger.debug(f"Using simple INSERT for {table_name} chunk {i//optimal_batch_size + 1} with {len(chunk)} rows")
                
                # Use executemany for bulk operation
                try:
                    with self.analytics_engine.begin() as conn:
                        conn.execute(text(insert_sql), chunk)
                except Exception as e:
                    if "duplicate key" in str(e).lower() or "unique_violation" in str(e).lower():
                        logger.error(f"Duplicate key violation in {table_name} chunk {i//optimal_batch_size + 1}: {str(e)}")
                        logger.error(f"First few rows in chunk: {chunk[:3] if chunk else 'No data'}")
                        raise
                    else:
                        raise
                
                total_inserted += len(chunk)
                logger.debug(f"Bulk inserted {len(chunk)} rows for {table_name} (total: {total_inserted})")
            
            logger.info(f"Successfully bulk inserted {total_inserted} rows for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in bulk insert for {table_name}: {str(e)}")
            return False

    def stream_mysql_data(self, table_name: str, query: str, chunk_size: int = 25000):  # Further reduced from 50000 to 25000 to prevent timeouts
        """Stream data from MySQL without loading into memory with connection retry logic."""
        # In test environment, we use source database as replication database
        if self.replication_engine is None:
            logger.error(f"No MySQL engine available for {table_name}")
            return
        
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                with self.replication_engine.connect() as conn:
                    # Set connection timeout and performance settings
                    conn.execute(text("SET SESSION net_read_timeout = 300"))  # 5 minutes
                    conn.execute(text("SET SESSION net_write_timeout = 300"))  # 5 minutes
                    conn.execute(text("SET SESSION wait_timeout = 600"))  # 10 minutes
                    
                    # Use server-side cursor for streaming
                    result = conn.execution_options(stream_results=True).execute(text(query))
                    
                    chunk_count = 0
                    while True:
                        try:
                            chunk = result.fetchmany(chunk_size)
                            if not chunk:
                                break
                                
                            # Convert to list of dicts
                            column_names = list(result.keys())
                            chunk_dicts = [self._convert_sqlalchemy_row_to_dict(row, column_names) for row in chunk]
                            
                            chunk_count += 1
                            logger.debug(f"Fetched chunk {chunk_count} for {table_name} with {len(chunk_dicts)} rows")
                            
                            yield chunk_dicts
                            
                        except Exception as e:
                            logger.error(f"Error fetching chunk {chunk_count + 1} for {table_name}: {e}")
                            raise
                            
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"MySQL connection attempt {attempt + 1} failed for {table_name}: {e}")
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"All {max_retries} connection attempts failed for {table_name}: {e}")
                    raise
            finally:
                # Ensure result is closed
                if 'result' in locals():
                    try:
                        result.close()
                    except Exception as e:
                        logger.debug(f"Error closing result: {e}")

    @track_method
    def load_table_streaming(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]:
        """Streaming version of load_table for memory-efficient processing.
        
        Returns:
            Tuple[bool, Dict]: (success, metadata_dict)
        """
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'streaming',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'No configuration found'
                }
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            
            # IMPROVED: Get MySQL schema with caching to avoid repeated expensive queries
            mysql_schema = self._get_cached_schema(table_name)
            if mysql_schema is None:
                logger.error(f"Failed to get schema for table {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'streaming',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'Failed to get schema'
                }
            
            # Create or verify PostgreSQL table
            if self.schema_adapter is not None:
                if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                    return False, {
                        'rows_loaded': 0,
                        'strategy_used': 'streaming',
                        'duration': 0.0,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': None,
                        'memory_used_mb': 0,
                        'error': 'Failed to ensure table exists'
                    }
            else:
                logger.info(f"Skipping table creation in test environment for {table_name}")
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Build query once
            query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # Handle full load truncation
            if force_full:
                logger.info(f"Truncating table {table_name} for full refresh")
                with self.analytics_engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    conn.commit()
            
            # Stream data from MySQL to PostgreSQL
            rows_loaded = 0
            batch_size = 10000
            
            try:
                for batch_data in self.stream_mysql_data(table_name, query, batch_size):
                    if batch_data:
                        # Convert data types for the batch
                        converted_batch = []
                        for row in batch_data:
                            if self.schema_adapter is not None:
                                converted_row = self.schema_adapter.convert_row_data_types(table_name, row)
                            else:
                                # In test environment, use test-specific data type conversion
                                converted_row = self._convert_data_types_for_test_environment(table_name, row)
                            converted_batch.append(converted_row)
                        
                        # Insert batch - Always use UPSERT for tables with primary keys to prevent duplicate key violations
                        table_config = self.get_table_config(table_name)
                        primary_key = table_config.get('primary_key')
                        use_upsert = True if primary_key and primary_key != 'id' else not force_full
                        if self.bulk_insert_optimized(table_name, converted_batch, use_upsert=use_upsert):
                            rows_loaded += len(converted_batch)
                        else:
                            logger.error(f"Failed to insert batch for {table_name}")
                            return False, {
                                'rows_loaded': rows_loaded,
                                'strategy_used': 'streaming',
                                'duration': time.time() - start_time,
                                'force_full_applied': force_full,
                                'primary_column': primary_column,
                                'last_primary_value': None,
                                'memory_used_mb': 0,
                                'error': 'Failed to insert batch'
                            }
                
                # Update load status with hybrid approach for tables with primary keys
                table_config = self.get_table_config(table_name)
                primary_key = table_config.get('primary_key')
                
                if primary_key and primary_key != 'id':
                    # Use hybrid approach for tables with primary keys
                    logger.info(f"Using hybrid load status update for {table_name} with primary key {primary_key}")
                    
                    # Get the last timestamp and primary key from the loaded data
                    last_timestamp = None
                    last_primary_value = None
                    
                    # Get the last timestamp from incremental columns
                    incremental_columns = table_config.get('incremental_columns', [])
                    if incremental_columns:
                        last_timestamp = self._get_loaded_at_time_max(table_name, incremental_columns)
                    
                    # Get the last datetime value from the incremental column
                    if rows_loaded > 0:
                        # Query the analytics database to get the max datetime value from the primary incremental column
                        try:
                            with self.analytics_engine.connect() as conn:
                                # Get the primary incremental column from table config
                                primary_incremental_column = table_config.get('primary_incremental_column')
                                if primary_incremental_column and primary_incremental_column != 'none':
                                    result = conn.execute(text(f"""
                                        SELECT MAX("{primary_incremental_column}") 
                                        FROM {self.analytics_schema}.{table_name}
                                        WHERE "{primary_incremental_column}" IS NOT NULL
                                    """)).scalar()
                                    if result:
                                        last_primary_value = str(result)
                                        logger.info(f"Got max datetime value for {table_name}: {last_primary_value} from column {primary_incremental_column}")
                                    else:
                                        logger.warning(f"No datetime values found in {primary_incremental_column} for {table_name}")
                                else:
                                    logger.warning(f"No primary incremental column configured for {table_name}")
                        except Exception as e:
                            logger.warning(f"Could not get last datetime value for {table_name}: {str(e)}")
                    
                    self._update_load_status_hybrid(
                        table_name, rows_loaded, 'success', 
                        last_timestamp, last_primary_value, primary_key
                    )
                else:
                    # Use standard approach for tables without primary keys
                    last_primary_value = None
                    if primary_column and primary_column != 'none':
                        last_primary_value = self._get_last_primary_value(table_name)
                    
                    self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
                
                # Calculate performance metrics
                end_time = time.time()
                final_memory = psutil.Process().memory_info().rss / 1024 / 1024
                duration = end_time - start_time
                memory_used = final_memory - initial_memory
                
                logger.info(f"Successfully streamed {table_name} ({rows_loaded:,} rows) in {duration:.2f}s")
                
                return True, {
                    'rows_loaded': rows_loaded,
                    'strategy_used': 'streaming',
                    'duration': duration,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': last_primary_value,
                    'memory_used_mb': memory_used,
                    'incremental_columns': incremental_columns
                }
                
            except Exception as e:
                logger.error(f"Error during streaming for {table_name}: {str(e)}")
                # Update tracking with failure status
                self._update_load_status(table_name, rows_loaded, 'failed', None, primary_column)
                return False, {
                    'rows_loaded': rows_loaded,
                    'strategy_used': 'streaming',
                    'duration': time.time() - start_time,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': f'Streaming error: {str(e)}'
                }
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Error in load_table_streaming for {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False, {
                'rows_loaded': 0,
                'strategy_used': 'streaming',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'error': str(e)
            }
    
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

    def _get_loaded_at_time_max(self, table_name: str, incremental_columns: List[str]) -> Optional[datetime]:
        """Get the maximum last load time across all incremental columns."""
        try:
            # Get the last primary value from the tracking table
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT last_primary_value, primary_column_name
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    AND last_primary_value IS NOT NULL
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result and result[0]:
                    last_primary_value = result[0]
                    primary_column_name = result[1]
                    
                    logger.info(f"DEBUG: Found last_primary_value for {table_name}: '{last_primary_value}' (column: {primary_column_name})")
                    
                    # Convert the primary value to datetime if it's a timestamp
                    try:
                        # Try to parse as datetime
                        if isinstance(last_primary_value, str):
                            # Handle different datetime formats
                            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                                try:
                                    parsed_datetime = datetime.strptime(last_primary_value, fmt)
                                    logger.info(f"DEBUG: Successfully parsed '{last_primary_value}' as datetime: {parsed_datetime}")
                                    return parsed_datetime
                                except ValueError:
                                    continue
                        
                        # If it's already a datetime object
                        if isinstance(last_primary_value, datetime):
                            logger.info(f"DEBUG: Using existing datetime object: {last_primary_value}")
                            return last_primary_value
                            
                    except Exception as e:
                        logger.warning(f"Could not parse last_primary_value '{last_primary_value}' as datetime for {table_name}: {str(e)}")
                        return None
                else:
                    logger.info(f"DEBUG: No last_primary_value found for {table_name} in etl_load_status table")
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting max last load time for {table_name}: {str(e)}")
            return None

    def _get_hybrid_incremental_info(self, table_name: str) -> Tuple[Optional[datetime], Optional[int]]:
        """
        Get hybrid incremental information using both timestamp and primary key.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Tuple[Optional[datetime], Optional[int]]: (last_timestamp, last_primary_value)
        """
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT last_primary_value, primary_column_name, last_loaded
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    last_primary_value = result[0]
                    primary_column_name = result[1]
                    last_loaded = result[2]
                    
                    # If last_primary_value is empty but we have last_loaded, use that
                    if not last_primary_value and last_loaded:
                        logger.info(f"Using last_loaded timestamp for {table_name} since last_primary_value is empty: {last_loaded}")
                        return last_loaded, None
                    
                    # If we have a valid last_primary_value, process it
                    if last_primary_value:
                        # Parse timestamp and primary key
                        last_timestamp = None
                        last_primary_id = None
                        
                        try:
                            if isinstance(last_primary_value, str):
                                # Check if it's a combined format: "timestamp|primary_key"
                                if '|' in last_primary_value:
                                    parts = last_primary_value.split('|', 1)
                                    if len(parts) == 2:
                                        timestamp_str, primary_key_str = parts
                                        
                                        # Parse timestamp
                                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                                            try:
                                                last_timestamp = datetime.strptime(timestamp_str, fmt)
                                                break
                                            except ValueError:
                                                continue
                                        
                                        # Parse primary key
                                        try:
                                            last_primary_id = int(primary_key_str)
                                        except (ValueError, TypeError):
                                            logger.warning(f"Could not parse primary key '{primary_key_str}' for {table_name}")
                                        
                                        logger.info(f"Parsed hybrid format for {table_name}: timestamp={last_timestamp}, primary_id={last_primary_id}")
                                        return last_timestamp, last_primary_id
                                
                                # If not combined format, try to parse as individual values
                                # Try to parse as datetime first
                                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                                    try:
                                        last_timestamp = datetime.strptime(last_primary_value, fmt)
                                        break
                                    except ValueError:
                                        continue
                                
                                # If not a datetime, try to parse as integer (primary key)
                                if last_timestamp is None:
                                    try:
                                        last_primary_id = int(last_primary_value)
                                    except (ValueError, TypeError):
                                        pass
                            
                            # If it's already a datetime object
                            elif isinstance(last_primary_value, datetime):
                                last_timestamp = last_primary_value
                                
                        except Exception as e:
                            logger.warning(f"Could not parse last_primary_value '{last_primary_value}' for {table_name}: {str(e)}")
                        
                        logger.info(f"DEBUG: Hybrid incremental info for {table_name}: timestamp={last_timestamp}, primary_id={last_primary_id}")
                        return last_timestamp, last_primary_id
                    else:
                        # No valid last_primary_value found
                        logger.info(f"No valid last_primary_value found for {table_name}")
                        return None, None
                else:
                    logger.info(f"DEBUG: No hybrid incremental info found for {table_name}")
                    return None, None
            
        except Exception as e:
            logger.error(f"Error getting hybrid incremental info for {table_name}: {str(e)}")
            return None, None

    def _build_hybrid_incremental_query(self, table_name: str, incremental_columns: List[str], 
                                       force_full: bool = False) -> str:
        """
        Build hybrid incremental query using both timestamp and primary key.
        
        This approach prevents duplicate key violations by ensuring we only load records that are:
        1. New (higher primary key than last loaded)
        2. Updated (same or lower primary key but newer timestamp)
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns (e.g., ['SecDateTEdit'])
            force_full: Whether to force a full load
            
        Returns:
            str: SQL query for incremental loading
        """
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')
        
        if force_full:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # Get table configuration
        table_config = self.get_table_config(table_name)
        primary_key = table_config.get('primary_key', 'id')
        
        # Get hybrid incremental info
        last_timestamp, last_primary_id = self._get_hybrid_incremental_info(table_name)
        
        # If no incremental info found, check if the table has data in analytics
        if not last_timestamp and not last_primary_id:
            # Check if the table has any data in the analytics database
            try:
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                    if result and result > 0:
                        # Table has data, get the maximum primary key value
                        max_result = conn.execute(text(f"""
                            SELECT MAX("{primary_key}") 
                            FROM {self.analytics_schema}.{table_name}
                            WHERE "{primary_key}" IS NOT NULL
                        """)).scalar()
                        if max_result:
                            last_primary_id = int(max_result)
                            logger.info(f"No incremental info found for {table_name}, but table has {result} rows. Using max {primary_key}: {last_primary_id}")
                        else:
                            logger.info(f"No incremental info found for {table_name}, but table has {result} rows. No valid {primary_key} values found.")
                    else:
                        # Table is empty, perform full load
                        logger.info(f"No incremental info found for {table_name} and table is empty, using full load")
                        return f"SELECT * FROM `{replication_db}`.`{table_name}`"
            except Exception as e:
                logger.warning(f"Error checking analytics table data for {table_name}: {str(e)}")
                # Fall back to full load if we can't check
                logger.info(f"No incremental info found for {table_name}, using full load")
                return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # If still no incremental info, perform full load
        if not last_timestamp and not last_primary_id:
            logger.info(f"No incremental info found for {table_name}, using full load")
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # Build hybrid conditions
        conditions = []
        
        # Condition 1: New records (higher primary key)
        if last_primary_id is not None:
            conditions.append(f"{primary_key} > {last_primary_id}")
        
        # Condition 2: Updated records (same or lower primary key but newer timestamp)
        if last_timestamp is not None and incremental_columns:
            timestamp_conditions = []
            for col in incremental_columns:
                timestamp_conditions.append(f"{col} > '{last_timestamp}'")
            
            if timestamp_conditions:
                if last_primary_id is not None:
                    # For updated records, we want same or lower primary key but newer timestamp
                    timestamp_condition = f"({' OR '.join(timestamp_conditions)}) AND {primary_key} <= {last_primary_id}"
                else:
                    # No primary key info, just use timestamp
                    timestamp_condition = f"({' OR '.join(timestamp_conditions)})"
                
                conditions.append(f"({timestamp_condition})")
        
        if not conditions:
            logger.warning(f"No valid conditions for hybrid incremental query for {table_name}")
            return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE 1=0"  # No records
        
        # Combine conditions with OR
        where_clause = " OR ".join(conditions)
        
        query = f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
        logger.info(f"Hybrid incremental query for {table_name}: {query}")
        
        return query
    
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
            # Get the actual database name from the connection instead of settings
            # This handles the case where test environment uses source database as replication
            with self.replication_engine.connect() as conn:
                # Get the current database name from the connection
                result = conn.execute(text("SELECT DATABASE()"))
                actual_db = result.scalar()
                logger.debug(f"Actual database for {table_name}: {actual_db}")
                
                for column in columns:
                    # Check for data quality issues by sampling the column
                    sample_query = f"""
                        SELECT MIN({column}), MAX({column}), COUNT(*)
                        FROM `{actual_db}`.`{table_name}`
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
    
    def _get_loaded_at_time(self, table_name: str) -> Optional[datetime]:
        """Get last load time from analytics database's etl_load_status table."""
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT _loaded_at
                    FROM {self.analytics_schema}.etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                """), {"table_name": table_name}).scalar()
                
                return result
        except Exception as e:
            logger.error(f"Error getting last load time for {table_name}: {str(e)}")
            return None

    def _get_analytics_row_count(self, table_name: str) -> int:
        """Get the current row count of a table in the analytics database."""
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                return result or 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name} in analytics: {str(e)}")
            return 0

    def _get_incremental_cutoff_time(self, table_name: str) -> Optional[datetime]:
        """
        Get the cutoff time for incremental loading.
        
        ENHANCED: Use the analytics load time as cutoff for incremental loading from replication.
        This ensures analytics loads all data that was copied to replication since the last analytics load.
        """
        try:
            # Get _loaded_at from analytics DB (this is the correct cutoff for incremental loads)
            _loaded_at = self._get_loaded_at_time(table_name)
            
            if _loaded_at:
                logger.info(f"Using analytics load time as cutoff for {table_name}: {_loaded_at}")
                return _loaded_at
            else:
                logger.info(f"No previous load found for {table_name}, performing full load")
                return None
                
        except Exception as e:
            logger.error(f"Error getting incremental cutoff time for {table_name}: {str(e)}")
            return None
    
    @track_method
    def load_table(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]:
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
            Tuple[bool, Dict]: (success, metadata_dict)
            metadata_dict contains:
                - rows_loaded: int
                - strategy_used: str ('standard', 'streaming', 'chunked', 'copy_csv', 'parallel')
                - duration: float (seconds)
                - force_full_applied: bool
                - primary_column: Optional[str]
                - last_primary_value: Optional[str]
                - memory_used_mb: float
        """
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Get table configuration for strategy selection
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'error',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'No configuration found'
                }
            
            estimated_size_mb = table_config.get('estimated_size_mb', 0)
            estimated_rows = table_config.get('estimated_rows', 0)
            primary_column = self._get_primary_incremental_column(table_config)
            
            # Enhanced strategy selection with parallel processing
            if estimated_rows > 1_000_000:
                logger.info(f"Using parallel processing for massive table {table_name} ({estimated_rows:,} rows)")
                strategy = "parallel"
                success, metadata = self.load_table_parallel(table_name, force_full)
            elif estimated_size_mb > 500:
                logger.info(f"Using COPY command for very large table {table_name} ({estimated_size_mb}MB)")
                strategy = "copy_csv"
                success, metadata = self.load_table_copy_csv(table_name, force_full)
            elif estimated_size_mb > 200:
                logger.info(f"Using chunked loading for large table {table_name} ({estimated_size_mb}MB)")
                strategy = "chunked"
                success, metadata = self.load_table_chunked(table_name, force_full, chunk_size=25000)
            elif estimated_size_mb > 50:
                logger.info(f"Using streaming loading for medium table {table_name} ({estimated_size_mb}MB)")
                strategy = "streaming"
                success, metadata = self.load_table_streaming(table_name, force_full)
            else:
                logger.info(f"Using standard loading for small table {table_name} ({estimated_size_mb}MB)")
                strategy = "standard"
                success, metadata = self.load_table_standard(table_name, force_full)
            
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
                
                # REMOVED: track_performance_metrics() call - Replaced by unified _track_performance_optimized() in SimpleMySQLReplicator
                
                # Return enhanced metadata
                return True, {
                    'rows_loaded': rows_processed,
                    'strategy_used': strategy,
                    'duration': duration,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': metadata.get('last_primary_value'),
                    'memory_used_mb': memory_used,
                    'estimated_size_mb': estimated_size_mb,
                    'estimated_rows': estimated_rows
                }
            else:
                # Return failure metadata
                end_time = time.time()
                duration = end_time - start_time
                final_memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_used = final_memory - initial_memory
                
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': strategy,
                    'duration': duration,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': memory_used,
                    'error': metadata.get('error', 'Load operation failed')
                }
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Error in load_table for {table_name}: {str(e)}")
            return False, {
                'rows_loaded': 0,
                'strategy_used': 'error',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'error': str(e)
            }

    @track_method
    def load_table_standard(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]:
        """
        Standard table loading method (original implementation).
        Used for small tables that can be loaded entirely into memory.
        
        Returns:
            Tuple[bool, Dict]: (success, metadata_dict)
        """
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Ensure tracking record exists before loading
            if not self._ensure_tracking_record_exists(table_name):
                logger.error(f"Failed to ensure tracking record for {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'standard',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'Failed to ensure tracking record'
                }
            
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'standard',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'No configuration found'
                }
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            
            # Get incremental strategy from configuration
            incremental_strategy = self._get_incremental_strategy(table_config)
            
            # Log the strategy being used
            self._log_incremental_strategy(table_name, primary_column, incremental_columns, incremental_strategy)
            
            # IMPROVED: Get MySQL schema with caching to avoid repeated expensive queries
            mysql_schema = self._get_cached_schema(table_name)
            if mysql_schema is None:
                logger.error(f"Failed to get schema for table {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'standard',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'Failed to get schema'
                }
            
            # Create or verify PostgreSQL table
            if self.schema_adapter is not None:
                if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                    return False, {
                        'rows_loaded': 0,
                        'strategy_used': 'standard',
                        'duration': 0.0,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': None,
                        'memory_used_mb': 0,
                        'error': 'Failed to ensure table exists'
                    }
            else:
                logger.info(f"Skipping table creation in test environment for {table_name}")
            
            # Apply data quality validation to incremental columns
            valid_incremental_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not valid_incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no valid incremental columns, treating as full refresh")
                force_full = True
            
            # Build query once
            query = self._build_load_query(table_name, valid_incremental_columns, force_full)
            
            # Handle full load truncation
            if force_full:
                logger.info(f"Truncating table {table_name} for full refresh")
                with self.analytics_engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    conn.commit()
            
            # Execute the load
            rows_loaded = 0
            with self.replication_engine.connect() as source_conn:
                with self.analytics_engine.connect() as target_conn:
                    # Execute query and fetch results
                    result = source_conn.execute(text(query))
                    
                    # Get column names for data conversion
                    column_names = list(result.keys())
                    
                    # Process results in batches
                    batch_size = 10000
                    batch = []
                    
                    for row in result:
                        # Convert row to dict and apply data type conversion
                        row_dict = self._convert_sqlalchemy_row_to_dict(row, column_names)
                        if self.schema_adapter is not None:
                            converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                        else:
                            # In test environment, use test-specific data type conversion
                            converted_row = self._convert_data_types_for_test_environment(table_name, row_dict)
                        batch.append(converted_row)
                        
                        if len(batch) >= batch_size:
                            # Insert batch
                            if self.bulk_insert_optimized(table_name, batch, use_upsert=not force_full):
                                rows_loaded += len(batch)
                            else:
                                logger.error(f"Failed to insert batch for {table_name}")
                                return False, {
                                    'rows_loaded': rows_loaded,
                                    'strategy_used': 'standard',
                                    'duration': time.time() - start_time,
                                    'force_full_applied': force_full,
                                    'primary_column': primary_column,
                                    'last_primary_value': None,
                                    'memory_used_mb': 0,
                                    'error': 'Failed to insert batch'
                                }
                            batch = []
                    
                    # Insert remaining rows
                    if batch:
                        if self.bulk_insert_optimized(table_name, batch, use_upsert=not force_full):
                            rows_loaded += len(batch)
                        else:
                            logger.error(f"Failed to insert final batch for {table_name}")
                            return False, {
                                'rows_loaded': rows_loaded,
                                'strategy_used': 'standard',
                                'duration': time.time() - start_time,
                                'force_full_applied': force_full,
                                'primary_column': primary_column,
                                'last_primary_value': None,
                                'memory_used_mb': 0,
                                'error': 'Failed to insert final batch'
                            }
            
            # Update load status with hybrid approach for tables with primary keys
            table_config = self.get_table_config(table_name)
            primary_key = table_config.get('primary_key')
            
            if primary_key and primary_key != 'id':
                # Use hybrid approach for tables with primary keys
                logger.info(f"Using hybrid load status update for {table_name} with primary key {primary_key}")
                
                # Get the last timestamp and primary key from the loaded data
                last_timestamp = None
                last_primary_value = None
                
                # Get the last timestamp from incremental columns
                incremental_columns = table_config.get('incremental_columns', [])
                if incremental_columns:
                    last_timestamp = self._get_loaded_at_time_max(table_name, incremental_columns)
                
                # Get the last primary value from the incremental column
                # Query the analytics database to get the max value from the primary incremental column
                try:
                    with self.analytics_engine.connect() as conn:
                        # Get the primary incremental column from table config
                        primary_incremental_column = table_config.get('primary_incremental_column')
                        if primary_incremental_column and primary_incremental_column != 'none':
                            result = conn.execute(text(f"""
                                SELECT MAX("{primary_incremental_column}") 
                                FROM {self.analytics_schema}.{table_name}
                                WHERE "{primary_incremental_column}" IS NOT NULL
                            """)).scalar()
                            if result:
                                # Check if this is an integer column and handle accordingly
                                if self._is_integer_column(table_name, primary_incremental_column):
                                    last_primary_value = str(result)  # Keep as string for storage
                                    logger.info(f"Got max integer value for {table_name}: {last_primary_value} from column {primary_incremental_column}")
                                else:
                                    last_primary_value = str(result)
                                    logger.info(f"Got max datetime value for {table_name}: {last_primary_value} from column {primary_incremental_column}")
                            else:
                                logger.warning(f"No values found in {primary_incremental_column} for {table_name}")
                        else:
                            logger.warning(f"No primary incremental column configured for {table_name}")
                except Exception as e:
                    logger.warning(f"Could not get last primary value for {table_name}: {str(e)}")
                
                self._update_load_status_hybrid(
                    table_name, rows_loaded, 'success', 
                    last_timestamp, last_primary_value, primary_key
                )
            else:
                # Use standard approach for tables without primary keys
                last_primary_value = None
                if primary_column and primary_column != 'none':
                    last_primary_value = self._get_last_primary_value(table_name)
                
                self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
            
            # Calculate performance metrics
            end_time = time.time()
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            duration = end_time - start_time
            memory_used = final_memory - initial_memory
            
            logger.info(f"Successfully loaded {table_name} ({rows_loaded:,} rows) in {duration:.2f}s")
            
            return True, {
                'rows_loaded': rows_loaded,
                'strategy_used': 'standard',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': primary_column,
                'last_primary_value': last_primary_value,
                'memory_used_mb': memory_used,
                'incremental_columns': valid_incremental_columns,
                'incremental_strategy': incremental_strategy
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Error in load_table_standard for {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False, {
                'rows_loaded': 0,
                'strategy_used': 'standard',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'error': str(e)
            }
    
    @track_method
    def load_table_chunked(self, table_name: str, force_full: bool = False, 
                          chunk_size: int = 25000) -> Tuple[bool, Dict]:  # Further reduced from 50000 to 25000 to prevent timeouts
        """
        Chunked loading method for large tables with Phase 1 fixes.
        
        PHASE 1 IMPROVEMENTS:
        - Proper pagination using LIMIT/OFFSET to prevent data duplication
        - Pre-processing validation to check for new data before loading
        - Strategy-based insert optimization with appropriate batch sizes
        - Enhanced error handling and progress tracking
        
        Returns:
            Tuple[bool, Dict]: (success, metadata_dict)
        """
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'chunked',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'No configuration found'
                }
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            incremental_strategy = table_config.get('incremental_strategy', 'or_logic')
            
            # Log incremental strategy decision
            self._log_incremental_strategy(table_name, primary_column, incremental_columns, incremental_strategy)
            
            # PHASE 1 FIX 1: Pre-processing validation
            # ENHANCED: Skip validation if analytics needs updating has already been determined
            # The validation was incorrectly checking for "new data since last load" when replication
            # already contains all data from previous runs
            should_proceed, validation_result = self._validate_incremental_load(table_name, incremental_columns, force_full)
            if not should_proceed:
                logger.info(f"Skipping load for {table_name}: {validation_result}")
                # FIXED: Return False for no new data scenario to indicate failure
                # This prevents incorrect tracking table updates
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'chunked',
                    'duration': time.time() - start_time,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'skipped_reason': validation_result,
                    'error': f'No new data found: {validation_result}'
                }
            
            # Get expected row count for strategy selection
            expected_rows = 0
            if isinstance(validation_result, int):
                expected_rows = validation_result
            else:
                expected_rows = table_config.get('estimated_rows', 0)
            
            # PHASE 1 FIX 2: Strategy-based insert optimization
            insert_strategy = self._choose_insert_strategy(force_full, expected_rows)
            # Use simple batch size calculation instead of removed _get_optimized_batch_size()
            BATCH_SIZES = {
                'simple_insert': 50_000,    # No conflicts, larger batches OK
                'optimized_upsert': 5_000,  # Reduce conflict overhead
                'copy_csv': 100_000         # COPY can handle large batches
            }
            optimized_batch_size = BATCH_SIZES.get(insert_strategy, 25_000)  # Default fallback
            
            logger.info(f"Using {insert_strategy} strategy with {optimized_batch_size} batch size for {table_name}")
            
            # IMPROVED: Get MySQL schema with caching to avoid repeated expensive queries
            mysql_schema = self._get_cached_schema(table_name)
            if mysql_schema is None:
                logger.error(f"Failed to get schema for table {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'chunked',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'Failed to get schema'
                }
            
            # Create or verify PostgreSQL table
            if self.schema_adapter is not None:
                if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                    return False, {
                        'rows_loaded': 0,
                        'strategy_used': 'chunked',
                        'duration': 0.0,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': None,
                        'memory_used_mb': 0,
                        'error': 'Failed to ensure table exists'
                    }
            else:
                logger.info(f"Skipping table creation in test environment for {table_name}")
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Handle full load truncation
            if force_full:
                logger.info(f"Truncating table {table_name} for full refresh")
                with self.analytics_engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    conn.commit()
            
            # Build base query for paginated processing
            base_query = self._build_enhanced_load_query(table_name, incremental_columns, primary_column, force_full, incremental_strategy)
            logger.info(f"Built base query for {table_name}: {base_query[:100]}...")
            
            # PHASE 1 FIX 3: Enhanced validation with safety thresholds
            max_allowed_rows = expected_rows * 3 if expected_rows > 0 else float('inf')  # Allow up to 3x the expected rows
            
            # Process in chunks using proper pagination
            rows_loaded = 0
            chunk_count = 0
            
            try:
                # PHASE 1 FIX 4: Use paginated streaming to prevent duplication
                first_chunk_processed = False
                for chunk_data in self.stream_mysql_data_paginated(table_name, base_query, optimized_batch_size):
                    if chunk_data:
                        first_chunk_processed = True
                        # VALIDATION: Check if we're loading too many rows
                        if expected_rows > 0 and rows_loaded + len(chunk_data) > max_allowed_rows:
                            logger.error(f"STOPPING LOAD: {table_name} has exceeded maximum allowed rows. "
                                       f"Loaded: {rows_loaded + len(chunk_data)}, Expected: {expected_rows}, "
                                       f"Max allowed: {max_allowed_rows}")
                            return False, {
                                'rows_loaded': rows_loaded,
                                'strategy_used': 'chunked',
                                'duration': time.time() - start_time,
                                'force_full_applied': force_full,
                                'primary_column': primary_column,
                                'last_primary_value': None,
                                'memory_used_mb': 0,
                                'chunk_count': chunk_count,
                                'error': f'Exceeded maximum allowed rows: {rows_loaded + len(chunk_data)} > {max_allowed_rows}'
                            }
                        
                        # Convert data types for the chunk
                        converted_chunk = []
                        for row in chunk_data:
                            if self.schema_adapter is not None:
                                converted_row = self.schema_adapter.convert_row_data_types(table_name, row)
                            else:
                                # In test environment, use test-specific data type conversion
                                converted_row = self._convert_data_types_for_test_environment(table_name, row)
                            converted_chunk.append(converted_row)
                        
                        # Determine upsert based on table structure, not strategy
                        primary_key = table_config.get('primary_key')
                        has_real_primary_key = bool(primary_key) and primary_key != 'id'
                        use_upsert = (not force_full) and has_real_primary_key
                        
                        logger.debug(f"Using {'UPSERT' if use_upsert else 'INSERT'} for {table_name} (force_full={force_full}, has_pk={has_real_primary_key})")
                        
                        if self.bulk_insert_optimized(table_name, converted_chunk, use_upsert=use_upsert):
                            rows_loaded += len(converted_chunk)
                            chunk_count += 1
                            
                            # Log progress with validation info
                            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                            progress_msg = f"Chunk {chunk_count}: {len(converted_chunk)} rows, Total: {rows_loaded:,}, Memory: {current_memory:.1f}MB"
                            if expected_rows > 0:
                                progress_pct = (rows_loaded / expected_rows) * 100
                                progress_msg += f", Progress: {progress_pct:.1f}%"
                            logger.info(progress_msg)
                        else:
                            logger.error(f"Failed to insert chunk {chunk_count} for {table_name}")
                            return False, {
                                'rows_loaded': rows_loaded,
                                'strategy_used': 'chunked',
                                'duration': time.time() - start_time,
                                'force_full_applied': force_full,
                                'primary_column': primary_column,
                                'last_primary_value': None,
                                'memory_used_mb': 0,
                                'error': f'Failed to insert chunk {chunk_count}'
                            }
                
                # HYBRID FIX: If no chunks were processed but analytics needs updating,
                # fall back to full load from replication
                if not first_chunk_processed and not force_full:
                    # Check if analytics needs updating
                    needs_updating, _, _, _ = self._check_analytics_needs_updating(table_name)
                    if needs_updating:
                        logger.warning(f"Incremental query returned 0 rows for {table_name}, but analytics needs updating. Falling back to full load from replication.")
                        # Build full load query
                        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
                        replication_db = replication_config.get('database', 'opendental_replication')
                        full_query = f"SELECT * FROM `{replication_db}`.`{table_name}`"
                        
                        # Process full load in chunks
                        for chunk_data in self.stream_mysql_data_paginated(table_name, full_query, optimized_batch_size):
                            if chunk_data:
                                # Convert data types for the chunk
                                converted_chunk = []
                                for row in chunk_data:
                                    if self.schema_adapter is not None:
                                        converted_row = self.schema_adapter.convert_row_data_types(table_name, row)
                                    else:
                                        # In test environment, use test-specific data type conversion
                                        converted_row = self._convert_data_types_for_test_environment(table_name, row)
                                    converted_chunk.append(converted_row)
                                
                                # Determine upsert based on table structure, not strategy
                                primary_key = table_config.get('primary_key')
                                has_real_primary_key = bool(primary_key) and primary_key != 'id'
                                use_upsert = (not force_full) and has_real_primary_key
                                
                                logger.debug(f"Using {'UPSERT' if use_upsert else 'INSERT'} for {table_name} (force_full={force_full}, has_pk={has_real_primary_key})")
                                
                                if self.bulk_insert_optimized(table_name, converted_chunk, use_upsert=use_upsert):
                                    rows_loaded += len(converted_chunk)
                                    chunk_count += 1
                                    
                                    # Log progress
                                    current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                                    progress_msg = f"Full load chunk {chunk_count}: {len(converted_chunk)} rows, Total: {rows_loaded:,}, Memory: {current_memory:.1f}MB"
                                    logger.info(progress_msg)
                                else:
                                    logger.error(f"Failed to insert full load chunk {chunk_count} for {table_name}")
                                    return False, {
                                        'rows_loaded': rows_loaded,
                                        'strategy_used': 'chunked',
                                        'duration': time.time() - start_time,
                                        'force_full_applied': force_full,
                                        'primary_column': primary_column,
                                        'last_primary_value': None,
                                        'memory_used_mb': 0,
                                        'error': f'Failed to insert full load chunk {chunk_count}'
                                    }
                
                # Update load status with hybrid approach for tables with primary keys
                table_config = self.get_table_config(table_name)
                primary_key = table_config.get('primary_key')
                
                if primary_key and primary_key != 'id':
                    # Use hybrid approach for tables with primary keys
                    logger.info(f"Using hybrid load status update for {table_name} with primary key {primary_key}")
                    
                    # Get the last timestamp and primary key from the loaded data
                    last_timestamp = None
                    last_primary_value = None
                    
                    # Get the last timestamp from incremental columns
                    incremental_columns = table_config.get('incremental_columns', [])
                    if incremental_columns:
                        last_timestamp = self._get_loaded_at_time_max(table_name, incremental_columns)
                    
                    # Get the last primary value from the incremental column
                    if rows_loaded > 0:
                        # Query the analytics database to get the max value from the primary incremental column
                        try:
                            with self.analytics_engine.connect() as conn:
                                # Get the primary incremental column from table config
                                primary_incremental_column = table_config.get('primary_incremental_column')
                                if primary_incremental_column and primary_incremental_column != 'none':
                                    result = conn.execute(text(f"""
                                        SELECT MAX("{primary_incremental_column}") 
                                        FROM {self.analytics_schema}.{table_name}
                                        WHERE "{primary_incremental_column}" IS NOT NULL
                                    """)).scalar()
                                    if result:
                                        # Check if this is an integer column and handle accordingly
                                        if self._is_integer_column(table_name, primary_incremental_column):
                                            last_primary_value = str(result)  # Keep as string for storage
                                            logger.info(f"Got max integer value for {table_name}: {last_primary_value} from column {primary_incremental_column}")
                                        else:
                                            last_primary_value = str(result)
                                            logger.info(f"Got max datetime value for {table_name}: {last_primary_value} from column {primary_incremental_column}")
                                    else:
                                        logger.warning(f"No values found in {primary_incremental_column} for {table_name}")
                                else:
                                    logger.warning(f"No primary incremental column configured for {table_name}")
                        except Exception as e:
                            logger.warning(f"Could not get last primary value for {table_name}: {str(e)}")
                    
                    self._update_load_status_hybrid(
                        table_name, rows_loaded, 'success', 
                        last_timestamp, last_primary_value, primary_key
                    )
                else:
                    # Use standard approach for tables without primary keys
                    last_primary_value = None
                    if primary_column and primary_column != 'none':
                        last_primary_value = self._get_last_primary_value(table_name)
                    
                    self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
                
                # Calculate performance metrics
                end_time = time.time()
                final_memory = psutil.Process().memory_info().rss / 1024 / 1024
                duration = end_time - start_time
                memory_used = final_memory - initial_memory
                
                logger.info(f"Successfully loaded {table_name} in {chunk_count} chunks "
                          f"({rows_loaded:,} rows) in {duration:.2f}s using {insert_strategy} strategy")
                
                return True, {
                    'rows_loaded': rows_loaded,
                    'strategy_used': 'chunked',
                    'duration': duration,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': last_primary_value,
                    'memory_used_mb': memory_used,
                    'chunk_count': chunk_count,
                    'chunk_size': optimized_batch_size,
                    'insert_strategy': insert_strategy,
                    'incremental_columns': incremental_columns
                }
                
            except Exception as e:
                logger.error(f"Error during chunked loading for {table_name}: {str(e)}")
                return False, {
                    'rows_loaded': rows_loaded,
                    'strategy_used': 'chunked',
                    'duration': time.time() - start_time,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'chunk_count': chunk_count,
                    'error': f'Chunked loading error: {str(e)}'
                }
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Error in load_table_chunked for {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False, {
                'rows_loaded': 0,
                'strategy_used': 'chunked',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'error': str(e)
            }
    
    @track_method
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
        """Build enhanced load query with improved incremental logic."""
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')

        # ENHANCED: Respect the incremental_strategy configuration
        # Don't override incremental_columns - use what was passed in
        table_config = self.get_table_config(table_name)
        primary_incremental_column = table_config.get('primary_incremental_column') if table_config else None
        
        # Validate incremental columns
        if not incremental_columns:
            logger.warning(f"No incremental columns provided for {table_name}, falling back to primary column")
            if primary_incremental_column:
                incremental_columns = [primary_incremental_column]
            else:
                logger.error(f"No incremental columns available for {table_name}")
                return f"SELECT * FROM `{replication_db}`.`{table_name}`"

        if force_full:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"

        # Get last analytics load time to determine what's new
        last_analytics_load = self._get_loaded_at_time(table_name)
        
        # FIXED: Handle integer primary keys correctly
        # Get the last primary value for integer primary keys
        last_primary_value = self._get_last_primary_value(table_name)
        
        # If no previous load record exists, check if the table has data in analytics
        if not last_analytics_load and not last_primary_value:
            # Check if the table has any data in the analytics database
            try:
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}")).scalar()
                    if result and result > 0:
                        # Table has data, get the maximum primary key value
                        if primary_incremental_column:
                            max_result = conn.execute(text(f"""
                                SELECT MAX("{primary_incremental_column}") 
                                FROM {self.analytics_schema}.{table_name}
                                WHERE "{primary_incremental_column}" IS NOT NULL
                            """)).scalar()
                            if max_result:
                                last_primary_value = str(max_result)
                                logger.info(f"No previous load record found for {table_name}, but table has {result} rows. Using max {primary_incremental_column}: {last_primary_value}")
                            else:
                                logger.info(f"No previous load record found for {table_name}, but table has {result} rows. No valid {primary_incremental_column} values found.")
                        else:
                            logger.info(f"No previous load record found for {table_name}, but table has {result} rows. No primary incremental column configured.")
                    else:
                        # Table is empty, perform full load
                        logger.info(f"No previous analytics load found for {table_name} and table is empty, performing full load")
                        return f"SELECT * FROM `{replication_db}`.`{table_name}`"
            except Exception as e:
                logger.warning(f"Error checking analytics table data for {table_name}: {str(e)}")
                # Fall back to full load if we can't check
                logger.info(f"No previous analytics load found for {table_name}, performing full load")
                return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # If still no previous load info, perform full load
        if not last_analytics_load and not last_primary_value:
            logger.info(f"No previous analytics load found for {table_name}, performing full load")
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # ENHANCED: Implement proper multi-column incremental logic based on strategy
        if incremental_strategy == 'or_logic':
            # Use OR logic for multiple columns
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    conditions.append(f"(`{col}` > '{last_analytics_load}' AND `{col}` != '0001-01-01 00:00:00')")
                # If no valid analytics load time, skip timestamp conditions
            where_clause = " OR ".join(conditions) if conditions else "1=1"  # Default to all records if no conditions
        elif incremental_strategy == 'and_logic':
            # Use AND logic for multiple columns
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    conditions.append(f"(`{col}` > '{last_analytics_load}' AND `{col}` != '0001-01-01 00:00:00')")
                # If no valid analytics load time, skip timestamp conditions
            where_clause = " AND ".join(conditions) if conditions else "1=1"  # Default to all records if no conditions
        elif incremental_strategy == 'single_column':
            # Use only the primary incremental column
            if primary_incremental_column and primary_incremental_column in incremental_columns:
                if last_primary_value and self._is_integer_column(table_name, primary_incremental_column):
                    # Handle integer primary key columns
                    where_clause = f"`{primary_incremental_column}` > {last_primary_value}"
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    where_clause = f"`{primary_incremental_column}` > '{last_analytics_load}' AND `{primary_incremental_column}` != '0001-01-01 00:00:00'"
                else:
                    # No valid conditions, use full load
                    where_clause = "1=1"
            else:
                # Fallback to first column
                if last_primary_value and self._is_integer_column(table_name, incremental_columns[0]):
                    where_clause = f"`{incremental_columns[0]}` > {last_primary_value}"
                elif last_analytics_load:
                    where_clause = f"`{incremental_columns[0]}` > '{last_analytics_load}' AND `{incremental_columns[0]}` != '0001-01-01 00:00:00'"
                else:
                    # No valid conditions, use full load
                    where_clause = "1=1"
        else:
            # Default to OR logic
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                elif last_analytics_load:
                    # Handle timestamp columns only if we have a valid analytics load time
                    conditions.append(f"(`{col}` > '{last_analytics_load}' AND `{col}` != '0001-01-01 00:00:00')")
                # If no valid analytics load time, skip timestamp conditions
            where_clause = " OR ".join(conditions) if conditions else "1=1"  # Default to all records if no conditions

        logger.info(f"Using {incremental_strategy} strategy for {table_name} with columns: {incremental_columns}")
        logger.info(f"Analytics load time: {last_analytics_load}")
        logger.info(f"Last primary value: {last_primary_value}")
        logger.info(f"Where clause: {where_clause}")
        
        return f"SELECT * FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
    
    @track_method
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
        
        logger.info(f"DEBUG: Building count query for {table_name} with force_full={force_full}, incremental_columns={incremental_columns}, strategy={incremental_strategy}")
        
        # Get the actual database name from the connection instead of settings
        # This handles the case where test environment uses source database as replication
        try:
            with self.replication_engine.connect() as conn:
                result = conn.execute(text("SELECT DATABASE()"))
                actual_db = result.scalar()
                logger.debug(f"Building count query for {table_name} using database: {actual_db}")
        except Exception as e:
            logger.warning(f"Could not get actual database name, using settings fallback: {e}")
            # Fallback to settings if connection fails
            replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
            actual_db = replication_config.get('database', 'opendental_replication')
        
        if force_full:
            query = f"SELECT COUNT(*) FROM `{actual_db}`.`{table_name}`"
            logger.info(f"DEBUG: Force full count query for {table_name}: {query}")
            return query
        
        # Get last load time from analytics (correct cutoff for incremental loads)
        last_load = self._get_loaded_at_time(table_name)
        last_primary_value = self._get_last_primary_value(table_name)
        
        logger.info(f"DEBUG: Last load time for {table_name}: {last_load}")
        logger.info(f"DEBUG: Last primary value for {table_name}: {last_primary_value}")
        
        # If no previous successful load, check if there's any data in replication that needs loading
        if last_load is None and last_primary_value is None:
            # Check if replication has any data for this table
            replication_has_data_query = f"SELECT COUNT(*) FROM `{actual_db}`.`{table_name}`"
            logger.info(f"DEBUG: No previous load found for {table_name}, checking if replication has data: {replication_has_data_query}")
            return replication_has_data_query
        
        if last_load and incremental_columns:
            # FIXED: Handle integer primary keys correctly
            table_config = self.get_table_config(table_name)
            primary_incremental_column = table_config.get('primary_incremental_column') if table_config else None
            last_primary_value = self._get_last_primary_value(table_name)
            
            conditions = []
            for col in incremental_columns:
                if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                    # Handle integer primary key columns
                    conditions.append(f"`{col}` > {last_primary_value}")
                else:
                    # Handle timestamp columns
                    conditions.append(f"({col} > '{last_load}' AND {col} != '0001-01-01 00:00:00')")
            
            # Use strategy-based logic
            if incremental_strategy == 'or_logic':
                where_clause = " OR ".join(conditions)
            elif incremental_strategy == 'and_logic':
                where_clause = " AND ".join(conditions)
            elif incremental_strategy == 'single_column':
                # Use only the primary incremental column
                if primary_incremental_column and primary_incremental_column in incremental_columns:
                    if last_primary_value and self._is_integer_column(table_name, primary_incremental_column):
                        where_clause = f"`{primary_incremental_column}` > {last_primary_value}"
                    else:
                        where_clause = f"({primary_incremental_column} > '{last_load}' AND {primary_incremental_column} != '0001-01-01 00:00:00')"
                else:
                    # Fallback to first column
                    if incremental_columns:
                        if last_primary_value and self._is_integer_column(table_name, incremental_columns[0]):
                            where_clause = f"`{incremental_columns[0]}` > {last_primary_value}"
                        else:
                            where_clause = f"({incremental_columns[0]} > '{last_load}' AND {incremental_columns[0]} != '0001-01-01 00:00:00')"
                    else:
                        where_clause = "1=0"
            else:
                # Default to OR logic
                where_clause = " OR ".join(conditions)
            
            query = f"SELECT COUNT(*) FROM `{actual_db}`.`{table_name}` WHERE {where_clause}"
            logger.info(f"DEBUG: Incremental count query for {table_name} ({incremental_strategy}): {query}")
            return query
        
        query = f"SELECT COUNT(*) FROM `{actual_db}`.`{table_name}`"
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
                    SELECT MAX(_loaded_at)
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
        """
        # Get primary key from table configuration
        table_config = self.get_table_config(table_name)
        primary_key = table_config.get('primary_key', 'id')
        logger.debug(f"Building UPSERT for {table_name} with primary key: {primary_key}")
        columns = ', '.join([f'"{col}"' for col in column_names])
        placeholders = ', '.join([f':{col}' for col in column_names])
        # Update all columns except the primary key
        update_columns = [f'"{col}" = EXCLUDED."{col}"' for col in column_names if col != primary_key]
        update_clause = ', '.join(update_columns) if update_columns else ''
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
 
    @track_method
    def load_table_copy_csv(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]:
        """
        Load table using PostgreSQL COPY command for maximum performance.
        
        Returns:
            Tuple[bool, Dict]: (success, metadata_dict)
        """
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'copy_csv',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'No configuration found'
                }
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            
            # IMPROVED: Get MySQL schema with caching to avoid repeated expensive queries
            mysql_schema = self._get_cached_schema(table_name)
            if mysql_schema is None:
                logger.error(f"Failed to get schema for table {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'copy_csv',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'Failed to get schema'
                }
            
            # Create or verify PostgreSQL table
            if self.schema_adapter is not None:
                if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                    return False, {
                        'rows_loaded': 0,
                        'strategy_used': 'copy_csv',
                        'duration': 0.0,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': None,
                        'memory_used_mb': 0,
                        'error': 'Failed to ensure table exists'
                    }
            else:
                logger.info(f"Skipping table creation in test environment for {table_name}")
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Handle full load truncation
            if force_full:
                logger.info(f"Truncating table {table_name} for full refresh")
                with self.analytics_engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    conn.commit()
            
            # Build query for data extraction
            query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # Create temporary CSV file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                temp_file_path = temp_file.name
                
                try:
                    # Export data from MySQL to CSV
                    rows_exported = 0
                    with self.replication_engine.connect() as source_conn:
                        result = source_conn.execute(text(query))
                        column_names = list(result.keys())
                        
                        # HYBRID FIX: If incremental query returns 0 rows but analytics needs updating,
                        # fall back to full load from replication
                        initial_rows = result.fetchall()
                        if len(initial_rows) == 0:
                            # Check if analytics needs updating
                            needs_updating, _, _, _ = self._check_analytics_needs_updating(table_name)
                            if needs_updating and not force_full:
                                logger.warning(f"Incremental query returned 0 rows for {table_name}, but analytics needs updating. Falling back to full load from replication.")
                                # Build full load query
                                replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
                                replication_db = replication_config.get('database', 'opendental_replication')
                                full_query = f"SELECT * FROM `{replication_db}`.`{table_name}`"
                                result = source_conn.execute(text(full_query))
                                column_names = list(result.keys())
                                initial_rows = result.fetchall()
                        
                        # Write CSV header
                        temp_file.write(','.join([f'"{col}"' for col in column_names]) + '\n')
                        
                        # Write data rows
                        for row in initial_rows:
                            # Convert row to dict and apply data type conversion
                            row_dict = self._convert_sqlalchemy_row_to_dict(row, column_names)
                            if self.schema_adapter is not None:
                                converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                            else:
                                # In test environment, use test-specific data type conversion
                                converted_row = self._convert_data_types_for_test_environment(table_name, row_dict)
                            
                            # Format CSV row
                            csv_row = []
                            for col in column_names:
                                value = converted_row.get(col)
                                if value is None:
                                    csv_row.append('')
                                else:
                                    # Escape quotes and wrap in quotes
                                    str_value = str(value).replace('"', '""')
                                    csv_row.append(f'"{str_value}"')
                            
                            temp_file.write(','.join(csv_row) + '\n')
                            rows_exported += 1
                    
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                    
                    # Import CSV to PostgreSQL using COPY
                    rows_loaded = 0
                    with self.analytics_engine.connect() as target_conn:
                        with open(temp_file_path, 'r') as csv_file:
                            # Skip header row
                            next(csv_file)
                            
                            # Use COPY command for fast import
                            copy_sql = f"""
                                COPY {self.analytics_schema}.{table_name} 
                                FROM STDIN 
                                WITH (FORMAT csv, HEADER false)
                            """
                            
                            with target_conn.connection.cursor() as cursor:
                                cursor.copy_expert(copy_sql, csv_file)
                                rows_loaded = cursor.rowcount
                    
                    # Update load status
                    last_primary_value = None
                    if primary_column and primary_column != 'none':
                        last_primary_value = self._get_last_primary_value(table_name)
                    
                    self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
                    
                    # Calculate performance metrics
                    end_time = time.time()
                    final_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    duration = end_time - start_time
                    memory_used = final_memory - initial_memory
                    
                    logger.info(f"Successfully loaded {table_name} via COPY CSV "
                              f"({rows_loaded:,} rows) in {duration:.2f}s")
                    
                    return True, {
                        'rows_loaded': rows_loaded,
                        'strategy_used': 'copy_csv',
                        'duration': duration,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': last_primary_value,
                        'memory_used_mb': memory_used,
                        'rows_exported': rows_exported,
                        'incremental_columns': incremental_columns
                    }
                    
                except Exception as e:
                    logger.error(f"Error during COPY CSV loading for {table_name}: {str(e)}")
                    return False, {
                        'rows_loaded': 0,
                        'strategy_used': 'copy_csv',
                        'duration': time.time() - start_time,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': None,
                        'memory_used_mb': 0,
                        'error': f'COPY CSV error: {str(e)}'
                    }
                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(temp_file_path)
                    except Exception as e:
                        logger.warning(f"Could not delete temporary file {temp_file_path}: {e}")
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Error in load_table_copy_csv for {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False, {
                'rows_loaded': 0,
                'strategy_used': 'copy_csv',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'error': str(e)
            }
 
    @track_method
    def load_table_parallel(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]:
        """
        Parallel loading method for massive tables.
        
        Returns:
            Tuple[bool, Dict]: (success, metadata_dict)
        """
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'parallel',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'No configuration found'
                }
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            
            # IMPROVED: Get MySQL schema with caching to avoid repeated expensive queries
            mysql_schema = self._get_cached_schema(table_name)
            if mysql_schema is None:
                logger.error(f"Failed to get schema for table {table_name}")
                return False, {
                    'rows_loaded': 0,
                    'strategy_used': 'parallel',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'error': 'Failed to get schema'
                }
            
            # Create or verify PostgreSQL table
            if self.schema_adapter is not None:
                if not self.schema_adapter.ensure_table_exists(table_name, mysql_schema):
                    return False, {
                        'rows_loaded': 0,
                        'strategy_used': 'parallel',
                        'duration': 0.0,
                        'force_full_applied': force_full,
                        'primary_column': primary_column,
                        'last_primary_value': None,
                        'memory_used_mb': 0,
                        'error': 'Failed to ensure table exists'
                    }
            else:
                logger.info(f"Skipping table creation in test environment for {table_name}")
            
            # Check if this is a table without incremental columns (needs full refresh)
            if not incremental_columns and not force_full:
                logger.info(f"Table {table_name} has no incremental columns, treating as full refresh")
                force_full = True
            
            # Handle full load truncation
            if force_full:
                logger.info(f"Truncating table {table_name} for full refresh")
                with self.analytics_engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    conn.commit()
            
            # Get total row count for parallel processing
            count_query = self._build_count_query(table_name, incremental_columns, force_full)
            with self.replication_engine.connect() as conn:
                total_rows = conn.execute(text(count_query)).scalar() or 0
            
            if total_rows == 0:
                logger.info(f"No data to load for {table_name}")
                return True, {
                    'rows_loaded': 0,
                    'strategy_used': 'parallel',
                    'duration': time.time() - start_time,
                    'force_full_applied': force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': 0,
                    'total_rows': total_rows
                }
            
            # Calculate chunk size and number of workers
            chunk_size = 50000  # Larger chunks for parallel processing
            num_workers = min(4, (total_rows // chunk_size) + 1)  # Max 4 workers
            
            logger.info(f"Processing {total_rows:,} rows in {num_workers} parallel chunks of {chunk_size:,}")
            
            # Process chunks in parallel
            rows_loaded = 0
            chunk_count = 0
            lock = threading.Lock()
            
            def process_chunk(chunk_start: int, chunk_end: int, chunk_id: int) -> Tuple[bool, int]:
                """Process a single chunk of data."""
                try:
                    # Build chunk query
                    base_query = self._build_load_query(table_name, incremental_columns, force_full)
                    chunk_query = f"{base_query} LIMIT {chunk_end - chunk_start} OFFSET {chunk_start}"
                    
                    # Process chunk
                    chunk_rows = 0
                    with self.replication_engine.connect() as source_conn:
                        result = source_conn.execute(text(chunk_query))
                        column_names = list(result.keys())
                        
                        # Process rows in batches
                        batch_size = 10000
                        batch = []
                        
                        for row in result:
                            # Convert row to dict and apply data type conversion
                            row_dict = self._convert_sqlalchemy_row_to_dict(row, column_names)
                            if self.schema_adapter is not None:
                                converted_row = self.schema_adapter.convert_row_data_types(table_name, row_dict)
                            else:
                                # In test environment, use test-specific data type conversion
                                converted_row = self._convert_data_types_for_test_environment(table_name, row_dict)
                            batch.append(converted_row)
                            
                            if len(batch) >= batch_size:
                                # Insert batch
                                if self.bulk_insert_optimized(table_name, batch, use_upsert=not force_full):
                                    chunk_rows += len(batch)
                                else:
                                    logger.error(f"Failed to insert batch in chunk {chunk_id}")
                                    return False, 0
                                batch = []
                        
                        # Insert remaining rows
                        if batch:
                            if self.bulk_insert_optimized(table_name, batch, use_upsert=not force_full):
                                chunk_rows += len(batch)
                            else:
                                logger.error(f"Failed to insert final batch in chunk {chunk_id}")
                                return False, 0
                    
                    with lock:
                        nonlocal rows_loaded, chunk_count
                        rows_loaded += chunk_rows
                        chunk_count += 1
                        logger.info(f"Chunk {chunk_id}: {chunk_rows:,} rows, "
                                  f"Total: {rows_loaded:,}/{total_rows:,}")
                    
                    return True, chunk_rows
                    
                except Exception as e:
                    logger.error(f"Error in chunk {chunk_id}: {str(e)}")
                    return False, 0
            
            # Execute parallel processing
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Submit chunk tasks
                futures = []
                for i in range(num_workers):
                    chunk_start = i * chunk_size
                    chunk_end = min((i + 1) * chunk_size, total_rows)
                    if chunk_start < total_rows:
                        future = executor.submit(process_chunk, chunk_start, chunk_end, i + 1)
                        futures.append(future)
                
                # Wait for all chunks to complete
                success_count = 0
                for future in as_completed(futures):
                    try:
                        success, chunk_rows = future.result()
                        if success:
                            success_count += 1
                        else:
                            logger.error("Chunk processing failed")
                            return False, {
                                'rows_loaded': rows_loaded,
                                'strategy_used': 'parallel',
                                'duration': time.time() - start_time,
                                'force_full_applied': force_full,
                                'primary_column': primary_column,
                                'last_primary_value': None,
                                'memory_used_mb': 0,
                                'error': 'Chunk processing failed'
                            }
                    except Exception as e:
                        logger.error(f"Error in parallel processing: {str(e)}")
                        return False, {
                            'rows_loaded': rows_loaded,
                            'strategy_used': 'parallel',
                            'duration': time.time() - start_time,
                            'force_full_applied': force_full,
                            'primary_column': primary_column,
                            'last_primary_value': None,
                            'memory_used_mb': 0,
                            'error': f'Parallel processing error: {str(e)}'
                        }
            
            # Update load status
            last_primary_value = None
            if primary_column and primary_column != 'none':
                last_primary_value = self._get_last_primary_value(table_name)
            
            self._update_load_status(table_name, rows_loaded, 'success', last_primary_value, primary_column)
            
            # Calculate performance metrics
            end_time = time.time()
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            duration = end_time - start_time
            memory_used = final_memory - initial_memory
            
            logger.info(f"Successfully loaded {table_name} in parallel "
                      f"({rows_loaded:,} rows, {chunk_count} chunks) in {duration:.2f}s")
            
            return True, {
                'rows_loaded': rows_loaded,
                'strategy_used': 'parallel',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': primary_column,
                'last_primary_value': last_primary_value,
                'memory_used_mb': memory_used,
                'chunk_count': chunk_count,
                'num_workers': num_workers,
                'total_rows': total_rows,
                'incremental_columns': incremental_columns
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Error in load_table_parallel for {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_load_status(table_name, 0, 'failed', None, None)
            return False, {
                'rows_loaded': 0,
                'strategy_used': 'parallel',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'error': str(e)
            }

    # REMOVED: track_performance_metrics() - Replaced by unified _track_performance_optimized() in SimpleMySQLReplicator

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
                    ORDER BY _loaded_at DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    last_primary_value, primary_column_name = result
                    logger.debug(f"Retrieved last primary value for {table_name}: {last_primary_value} (column: {primary_column_name})")
                    
                    # NEW: If tracking has NULL but we know the primary column, compute from analytics data
                    if last_primary_value is None and primary_column_name:
                        try:
                            computed_max = conn.execute(text(f"""
                                SELECT MAX("{primary_column_name}")
                                FROM {self.analytics_schema}.{table_name}
                            """)).scalar()
                            if computed_max is not None:
                                last_primary_value = str(computed_max)
                                logger.info(f"Computed last_primary_value for {table_name} from analytics data: {last_primary_value}")
                            else:
                                logger.info(f"Analytics table {self.analytics_schema}.{table_name} is empty; leaving last_primary_value as NULL")
                        except Exception as compute_err:
                            logger.warning(f"Could not compute MAX({primary_column_name}) for {table_name}: {str(compute_err)}")

                    # If this is an integer column, ensure we return a proper integer value
                    if primary_column_name and self._is_integer_column(table_name, primary_column_name):
                        try:
                            # Try to convert to integer to validate it's a proper integer
                            int_value = int(last_primary_value)
                            return str(int_value)  # Return as string for consistency
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid integer value for {table_name}.{primary_column_name}: {last_primary_value}")
                            return None
                    
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
    
    def _is_integer_column(self, table_name: str, column_name: str) -> bool:
        """Check if a column is an integer type based on table configuration."""
        try:
            table_config = self.get_table_config(table_name)
            if not table_config:
                return False
            
            # Check if this is the primary key and if it's likely an integer
            primary_key = table_config.get('primary_key')
            primary_incremental_column = table_config.get('primary_incremental_column')
            
            # Check if this column is either the primary key or the primary incremental column
            if column_name == primary_key or column_name == primary_incremental_column:
                # Common integer primary key patterns
                integer_primary_patterns = [
                    'Num', 'ID', 'Id', 'id'
                ]
                return any(pattern in column_name for pattern in integer_primary_patterns)
            
            # For non-primary keys, check if it's explicitly marked as integer
            # This is a simplified check - in a real implementation, you might want to
            # query the database schema to get the actual column type
            return False
            
        except Exception as e:
            logger.warning(f"Error checking if column {column_name} is integer for table {table_name}: {str(e)}")
            return False

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
    
    def _convert_data_types_for_test_environment(self, table_name: str, row_data: Dict) -> Dict:
        """
        Convert data types for test environment when schema_adapter is None.
        This handles the specific case where MySQL integer boolean values need to be converted to PostgreSQL boolean.
        
        Args:
            table_name: Name of the table
            row_data: Dictionary containing the row data
            
        Returns:
            Dict: Row data with converted types
        """
        converted_row = row_data.copy()
        
        # Define boolean fields that need conversion from integer to boolean
        # This matches the pattern used in test_data_manager.py
        boolean_fields_by_table = {
            'patient': ['PatStatus', 'Gender', 'Position', 'Premed', 
                       'PlannedIsDone', 'PreferConfirmMethod', 'PreferContactMethod', 
                       'PreferRecallMethod', 'TxtMsgOk', 'HasSuperBilling', 
                       'HasSignedTil', 'ShortCodeOptIn', 'PreferContactConfidential', 
                       'CanadianEligibilityCode'],
            'appointment': ['AptStatus'],
            # Add other tables as needed
        }
        
        # Get boolean fields for this table
        boolean_fields = boolean_fields_by_table.get(table_name, [])
        
        # Convert integer boolean values to PostgreSQL boolean
        for field in boolean_fields:
            if field in converted_row and isinstance(converted_row[field], int):
                converted_row[field] = bool(converted_row[field])
        
        return converted_row

    def stream_mysql_data_paginated(self, table_name: str, base_query: str, chunk_size: int = 25000):
        """
        Stream data from MySQL using proper LIMIT/OFFSET pagination to prevent duplication.
        
        This method addresses the pagination bug by explicitly adding LIMIT/OFFSET clauses
        to ensure each chunk processes a unique subset of data.
        
        Args:
            table_name: Name of the table being processed
            base_query: Base SQL query without LIMIT/OFFSET
            chunk_size: Number of rows per chunk
            
        Yields:
            List[Dict]: Chunks of data as dictionaries
        """
        if self.replication_engine is None:
            logger.error(f"No MySQL engine available for {table_name}")
            return
        
        max_retries = 3
        retry_delay = 5  # seconds
        offset = 0
        
        for attempt in range(max_retries):
            try:
                with self.replication_engine.connect() as conn:
                    # Set connection timeout and performance settings
                    conn.execute(text("SET SESSION net_read_timeout = 300"))  # 5 minutes
                    conn.execute(text("SET SESSION net_write_timeout = 300"))  # 5 minutes
                    conn.execute(text("SET SESSION wait_timeout = 600"))  # 10 minutes
                    
                    chunk_count = 0
                    while True:
                        # FIXED: Add proper LIMIT/OFFSET pagination
                        chunk_query = f"{base_query} LIMIT {chunk_size} OFFSET {offset}"
                        
                        try:
                            # Execute the paginated query
                            result = conn.execute(text(chunk_query))
                            chunk_data = result.fetchall()
                            
                            if not chunk_data:
                                logger.info(f"No more data for {table_name} at offset {offset}")
                                break
                            
                            # Convert to list of dicts
                            column_names = list(result.keys())
                            chunk_dicts = [self._convert_sqlalchemy_row_to_dict(row, column_names) for row in chunk_data]
                            
                            chunk_count += 1
                            logger.debug(f"Fetched chunk {chunk_count} for {table_name} with {len(chunk_dicts)} rows (offset: {offset})")
                            
                            yield chunk_dicts
                            
                            # Move to next offset
                            offset += chunk_size
                            
                            # Safety check: if we get fewer rows than chunk_size, we're done
                            if len(chunk_data) < chunk_size:
                                logger.info(f"Final chunk for {table_name}: {len(chunk_data)} rows (less than chunk_size {chunk_size})")
                                break
                                
                        except Exception as e:
                            logger.error(f"Error fetching chunk {chunk_count + 1} for {table_name} at offset {offset}: {e}")
                            raise
                    
                    # Success - break out of retry loop
                    break
                            
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"MySQL connection attempt {attempt + 1} failed for {table_name}: {e}")
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"All {max_retries} connection attempts failed for {table_name}: {e}")
                    raise

    def _validate_incremental_load(self, table_name: str, incremental_columns: List[str], force_full: bool = False) -> Tuple[bool, Union[str, int]]:
        """
        Validate incremental load by checking if new data exists before processing.
        
        ENHANCED: Skip validation when analytics needs updating has already been determined.
        The validation was incorrectly checking for "new data since last load" when replication
        already contains all data from previous runs.
        
        Args:
            table_name: Name of the table to validate
            incremental_columns: List of incremental columns
            force_full: Whether this is a forced full load
            
        Returns:
            Tuple[bool, Union[str, int]]: (should_proceed, reason_or_count)
        """
        if force_full:
            logger.info(f"Force full load for {table_name}, skipping incremental validation")
            return True, "force_full"
        
        if not incremental_columns:
            logger.info(f"No incremental columns for {table_name}, treating as full load")
            return True, "no_incremental_columns"
        
        # ENHANCED: Check if analytics needs updating before doing validation
        # If analytics needs updating, skip the validation that checks for "new data since last load"
        needs_updating, replication_primary, analytics_primary, force_full_load_recommended = self._check_analytics_needs_updating(table_name)
        if needs_updating:
            logger.info(f"Analytics needs updating for {table_name}, skipping validation check")
            return True, "analytics_needs_updating"
        
        try:
            # Build count query to check for new data
            count_query = self._build_count_query(table_name, incremental_columns, force_full=False)
            logger.debug(f"Validation count query for {table_name}: {count_query}")
            
            with self.replication_engine.connect() as conn:
                result = conn.execute(text(count_query))
                new_row_count = result.scalar()
            
            if new_row_count == 0:
                logger.info(f"No new data for {table_name}, skipping load")
                return False, "no_new_data"
            
            logger.info(f"Found {new_row_count:,} new rows for {table_name}")
            return True, new_row_count
            
        except Exception as e:
            logger.error(f"Error validating incremental load for {table_name}: {e}")
            # On validation error, proceed with load but log the issue
            return True, f"validation_error: {str(e)}"

    def _choose_insert_strategy(self, force_full: bool, row_count: int) -> str:
        """
        Choose the appropriate insert strategy based on load type and data volume.
        
        Args:
            force_full: Whether this is a forced full load
            row_count: Estimated number of rows to be loaded
            
        Returns:
            str: Strategy name ('simple_insert', 'optimized_upsert', 'copy_csv')
        """
        if force_full:
            # Use simple INSERT for truncated tables (no conflicts possible)
            logger.info(f"Using simple INSERT strategy for full load")
            return "simple_insert"
        elif row_count > 1_000_000:
            # Use COPY for massive datasets
            logger.info(f"Using COPY CSV strategy for large dataset ({row_count:,} rows)")
            return "copy_csv"
        else:
            # Use optimized UPSERT with smaller batches
            logger.info(f"Using optimized UPSERT strategy for incremental load ({row_count:,} rows)")
            return "optimized_upsert"

    def _get_last_copy_time_from_replication(self, table_name: str) -> Optional[datetime]:
        """Get last copy time from replication database's etl_copy_status table."""
        try:
            with self.replication_engine.connect() as conn:
                # Use the replication database name explicitly
                replication_db = self.replication_engine.url.database
                result = conn.execute(text(f"""
                    SELECT last_copied
                    FROM `{replication_db}`.etl_copy_status
                    WHERE table_name = :table_name
                    AND copy_status = 'success'
                    ORDER BY last_copied DESC
                    LIMIT 1
                """), {"table_name": table_name}).scalar()
                return result
        except Exception as e:
            logger.error(f"Error getting last copy time from replication for {table_name}: {str(e)}")
            return None

    def _get_last_primary_value_from_replication(self, table_name: str) -> Tuple[Optional[datetime], Optional[str]]:
        """Get last primary_value and primary_column_name from replication database's etl_copy_status table."""
        try:
            with self.replication_engine.connect() as conn:
                # Use the replication database name explicitly
                replication_db = self.replication_engine.url.database
                result = conn.execute(text(f"""
                    SELECT last_primary_value, primary_column_name
                    FROM `{replication_db}`.etl_copy_status
                    WHERE table_name = :table_name
                    AND copy_status = 'success'
                    ORDER BY last_copied DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    return result[0], result[1]  # last_primary_value, primary_column_name
                return None, None
        except Exception as e:
            logger.error(f"Error getting last primary_value from replication for {table_name}: {str(e)}")
            return None, None
    
    def _check_analytics_needs_updating(self, table_name: str) -> Tuple[bool, Optional[str], Optional[str], bool]:
        """
        Check if analytics database needs updating from replication database.
        
        ENHANCED: Check for actual new data in replication since last analytics load,
        not just timestamp comparison.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            Tuple[bool, Optional[str], Optional[str], bool]: (needs_updating, replication_primary_value, analytics_primary_value, force_full_load_recommended)
        """
        try:
            # Get last copy time from replication database
            replication_copy_time = self._get_last_copy_time_from_replication(table_name)
            
            # Get last load time from analytics database
            analytics_load_time = self._get_loaded_at_time(table_name)
            
            # Get current row count in analytics table
            analytics_row_count = self._get_analytics_row_count(table_name)
            
            if not replication_copy_time:
                logger.info(f"No replication data found for {table_name}, analytics cannot be updated")
                return False, None, None, False
            
            # Check if analytics table is empty but has a non-initial load time
            force_full_load_recommended = False
            if analytics_row_count == 0 and analytics_load_time is not None:
                logger.info(f"Analytics table {table_name} is empty but has load timestamp {analytics_load_time}, recommending full load")
                force_full_load_recommended = True
            
            if analytics_load_time is None:
                # Analytics has never been loaded, check if replication has data
                logger.info(f"Analytics has never been loaded for {table_name}, checking if replication has data")
                
                # Check if replication has any data for this table
                try:
                    with self.replication_engine.connect() as conn:
                        # Use the replication database name explicitly
                        replication_db = self.replication_engine.url.database
                        result = conn.execute(text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"))
                        replication_row_count = result.scalar()
                        
                    if replication_row_count > 0:
                        logger.info(f"Replication has {replication_row_count} rows for {table_name}, analytics needs full sync")
                        return True, str(replication_copy_time), None, True
                    else:
                        logger.info(f"Replication has no data for {table_name}, nothing to load")
                        return False, str(replication_copy_time), None, False
                except Exception as e:
                    logger.error(f"Error checking replication data for {table_name}: {str(e)}")
                    return False, str(replication_copy_time), None, False
            
            # ENHANCED: Check if there's new data in replication since last analytics load
            # Instead of just comparing timestamps, check if there are actually new records
            try:
                with self.replication_engine.connect() as conn:
                    # Use the replication database name explicitly
                    replication_db = self.replication_engine.url.database
                    
                    # Get table configuration for incremental columns
                    table_config = self.get_table_config(table_name)
                    incremental_columns = table_config.get('incremental_columns', []) if table_config else []
                    
                    # ENHANCED: Check for actual new records in replication since last analytics load
                    # Don't rely on replication copy time comparison - check actual data timestamps
                    if incremental_columns and analytics_load_time:
                        # Get incremental strategy from table configuration
                        incremental_strategy = table_config.get('incremental_strategy', 'or_logic')
                        
                        # FIXED: Check if there are new records in replication since last analytics load
                        # The issue is that records with timestamps older than analytics_load_time might still be new
                        # if they weren't in analytics before. We need to check if replication has more records than analytics.
                        
                        # First, check if replication has more records than analytics (this catches all new records regardless of timestamp)
                        replication_count_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
                        replication_count = conn.execute(text(replication_count_query)).scalar()
                        
                        if replication_count > analytics_row_count:
                            logger.info(f"Replication has {replication_count} rows vs analytics {analytics_row_count} rows for {table_name}, analytics needs updating")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        
                        # If row counts are the same, check for records with timestamps newer than analytics load time
                        # FIXED: Handle integer primary keys correctly
                        primary_incremental_column = table_config.get('primary_incremental_column')
                        last_primary_value = self._get_last_primary_value(table_name)
                        
                        conditions = []
                        for col in incremental_columns:
                            if col == primary_incremental_column and last_primary_value and self._is_integer_column(table_name, col):
                                # Handle integer primary key columns
                                conditions.append(f"`{col}` > {last_primary_value}")
                            else:
                                # Handle timestamp columns
                                conditions.append(f"({col} > '{analytics_load_time}' AND {col} != '0001-01-01 00:00:00')")
                        
                        # Use strategy-based logic
                        if incremental_strategy == 'or_logic':
                            where_clause = " OR ".join(conditions)
                        elif incremental_strategy == 'and_logic':
                            where_clause = " AND ".join(conditions)
                        elif incremental_strategy == 'single_column':
                            # Use only the primary incremental column
                            if primary_incremental_column and primary_incremental_column in incremental_columns:
                                if last_primary_value and self._is_integer_column(table_name, primary_incremental_column):
                                    where_clause = f"`{primary_incremental_column}` > {last_primary_value}"
                                else:
                                    where_clause = f"({primary_incremental_column} > '{analytics_load_time}' AND {primary_incremental_column} != '0001-01-01 00:00:00')"
                            else:
                                # Fallback to first column
                                if last_primary_value and self._is_integer_column(table_name, incremental_columns[0]):
                                    where_clause = f"`{incremental_columns[0]}` > {last_primary_value}"
                                else:
                                    where_clause = f"({incremental_columns[0]} > '{analytics_load_time}' AND {incremental_columns[0]} != '0001-01-01 00:00:00')"
                        else:
                            # Default to OR logic
                            where_clause = " OR ".join(conditions)
                        
                        new_records_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}` WHERE {where_clause}"
                        
                        result = conn.execute(text(new_records_query))
                        new_records_count = result.scalar()
                        
                        if new_records_count > 0:
                            logger.info(f"Found {new_records_count} new records in replication for {table_name} since last analytics load")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        else:
                            logger.info(f"No new records found in replication for {table_name} since last analytics load")
                            return False, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                    
                    # Fallback: Only use replication copy time comparison if no incremental columns available
                    if replication_copy_time and analytics_load_time and replication_copy_time > analytics_load_time:
                        logger.info(f"Replication copy time ({replication_copy_time}) is newer than analytics load time ({analytics_load_time}), but no incremental columns configured. Checking for any data differences.")
                        # Check if replication has more rows than analytics
                        replication_count_query = f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`"
                        analytics_count_query = f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}"
                        
                        replication_count = conn.execute(text(replication_count_query)).scalar()
                        analytics_count = self._get_analytics_row_count(table_name)
                        
                        if replication_count > analytics_count:
                            logger.info(f"Replication has {replication_count} rows vs analytics {analytics_count} rows for {table_name}, analytics needs updating")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        else:
                            logger.info(f"Replication and analytics have same row count for {table_name} ({replication_count}), no update needed")
                            return False, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                    
                    else:
                        # Fallback to timestamp comparison if no incremental columns or no analytics load time
                        if replication_copy_time > analytics_load_time or force_full_load_recommended:
                            logger.info(f"Analytics needs updating for {table_name}: "
                                       f"replication_copy_time={replication_copy_time}, analytics_load_time={analytics_load_time}, "
                                       f"analytics_row_count={analytics_row_count}, force_full_load_recommended={force_full_load_recommended}")
                            return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                        else:
                            logger.info(f"Analytics is up to date for {table_name}: "
                                       f"replication_copy_time={replication_copy_time}, analytics_load_time={analytics_load_time}")
                            return False, str(replication_copy_time), str(analytics_load_time), False
                            
            except Exception as e:
                logger.error(f"Error checking for new records in replication for {table_name}: {str(e)}")
                # Fallback to timestamp comparison
                if replication_copy_time > analytics_load_time or force_full_load_recommended:
                    return True, str(replication_copy_time), str(analytics_load_time), force_full_load_recommended
                else:
                    return False, str(replication_copy_time), str(analytics_load_time), False
                
        except Exception as e:
            logger.error(f"Error checking if analytics needs updating for {table_name}: {str(e)}")
            return False, None, None, False

    def _update_load_status_hybrid(self, table_name: str, rows_loaded: int, 
                                  load_status: str = 'success', 
                                  last_timestamp: Optional[datetime] = None,
                                  last_primary_value: Optional[str] = None,
                                  primary_column_name: Optional[str] = None) -> bool:
        """
        Update load status with hybrid information (both timestamp and primary key).
        
        Args:
            table_name: Name of the table
            rows_loaded: Number of rows loaded
            load_status: Status of the load ('success', 'failed', etc.)
            last_timestamp: Last timestamp processed
            last_primary_value: Last primary key value processed
            primary_column_name: Name of the primary key column
            
        Returns:
            bool: True if update was successful
        """
        try:
            # Get table configuration
            table_config = self.get_table_config(table_name)
            primary_key = table_config.get('primary_key', 'id')
            
            # Store both timestamp and primary key information in a combined format
            # Format: "timestamp|primary_key" or just "timestamp" if no primary key
            stored_value = None
            stored_column = 'hybrid'
            
            if last_timestamp and last_primary_value:
                # Store both timestamp and primary key
                timestamp_str = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                stored_value = f"{timestamp_str}|{last_primary_value}"
                logger.info(f"Storing hybrid value for {table_name}: timestamp={timestamp_str}, primary_key={last_primary_value}")
            elif last_timestamp:
                # Store only timestamp
                stored_value = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                stored_column = 'timestamp'
                logger.info(f"Storing timestamp-only value for {table_name}: {stored_value}")
            elif last_primary_value:
                # Store only primary key
                stored_value = str(last_primary_value)
                stored_column = primary_column_name or primary_key
                logger.info(f"Storing primary-key-only value for {table_name}: {stored_value}")
            else:
                # Get current timestamp as fallback
                stored_value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                stored_column = 'timestamp'
                logger.info(f"Storing fallback timestamp for {table_name}: {stored_value}")
            
            # Update the tracking table
            with self.analytics_engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.analytics_schema}.etl_load_status 
                    (table_name, rows_loaded, load_status, last_primary_value, primary_column_name, _loaded_at)
                    VALUES (:table_name, :rows_loaded, :load_status, :last_primary_value, :primary_column_name, NOW())
                    ON CONFLICT (table_name) DO UPDATE SET
                        last_primary_value = :last_primary_value,
                        primary_column_name = :primary_column_name,
                        rows_loaded = :rows_loaded,
                        load_status = :load_status,
                        _loaded_at = NOW()
                """), {
                    "table_name": table_name,
                    "rows_loaded": rows_loaded,
                    "load_status": load_status,
                    "last_primary_value": stored_value,
                    "primary_column_name": stored_column
                })
                conn.commit()
            
            logger.info(f"Updated hybrid load status for {table_name}: {stored_value} ({stored_column})")
            return True
            
        except Exception as e:
            logger.error(f"Error updating hybrid load status for {table_name}: {str(e)}")
            return False

    def _build_improved_load_query_max(self, table_name: str, incremental_columns: List[str], 
                                      force_full: bool = False, incremental_strategy: str = 'or_logic') -> str:
        """
        Build query with improved incremental logic using maximum timestamp across all columns.
        
        ENHANCED: Now uses hybrid approach for tables with primary keys to prevent duplicate key violations.
        """
        # Get table configuration
        table_config = self.get_table_config(table_name)
        primary_key = table_config.get('primary_key')
        
        # Use hybrid approach for tables with primary keys
        if primary_key and primary_key != 'id':
            logger.info(f"Using hybrid incremental approach for {table_name} with primary key {primary_key}")
            return self._build_hybrid_incremental_query(table_name, incremental_columns, force_full)
        
        # Fall back to original timestamp-only approach for tables without primary keys
        logger.info(f"Using timestamp-only incremental approach for {table_name}")
        
        # Get replication database name from settings
        replication_config = self.settings.get_database_config(DatabaseType.REPLICATION)
        replication_db = replication_config.get('database', 'opendental_replication')
        
        # Data quality validation
        valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
        
        if force_full or not valid_columns:
            return f"SELECT * FROM `{replication_db}`.`{table_name}`"
        
        # Get maximum last load time across all columns
        last_load = self._get_loaded_at_time_max(table_name, valid_columns)
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
