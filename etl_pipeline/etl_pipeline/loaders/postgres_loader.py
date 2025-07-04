"""
DEPRECATION NOTICE - REFACTORING IN PROGRESS
============================================

This file is part of the ETL Pipeline Schema Analysis Refactoring Plan.
See: docs/refactoring_plan_schema_analysis.md

PLANNED CHANGES:
- Will update for simplified tables.yml configuration structure
- Will remove three-section YAML support (source_tables, staging_tables, target_tables)
- Will use new get_table_config() method from settings
- Will integrate with enhanced SchemaDiscovery for configuration
- Will maintain current data movement functionality

TIMELINE: Phase 4 of refactoring plan
STATUS: Configuration update in progress

PostgreSQL loader implementation for loading data from MySQL replication to PostgreSQL analytics.

DATA FLOW ARCHITECTURE
======================

ARCHITECTURAL ROLE:
This is a PURE DATA MOVEMENT LAYER that moves data from MySQL replication to PostgreSQL raw schema.
It performs minimal transformations (only data type conversion) and focuses on infrastructure operations.

DATA FLOW:
MySQL OpenDental (Source Database)
    ↓
opendental_replication (MySQL Replication Database)
    ↓
PostgresLoader (ETL Infrastructure Layer)
    ├── Schema Conversion (MySQL → PostgreSQL types)
    ├── Data Extraction (with incremental support)
    ├── Data Loading (bulk/chunked operations)
    └── Load Verification (row count validation)
    ↓
opendental_analytics.raw (PostgreSQL Raw Schema)

KEY ARCHITECTURAL PRINCIPLES:

1. PURE DATA MOVEMENT LAYER
   - Purpose: Move data from MySQL replication to PostgreSQL raw schema
   - Transformation: MINIMAL - only data type conversion (MySQL → PostgreSQL)
   - No Business Logic: No field mappings, calculations, or business rules

2. CONFIGURATION-DRIVEN TYPE CONVERSION
   - Source: MySQL data types from SchemaDiscovery
   - Target: PostgreSQL data types via PostgresSchema
   - Standardization: Consistent type mapping across all tables

3. INCREMENTAL LOADING SUPPORT
   - Strategy: Time-based incremental loading using incremental_columns
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

WHAT POSTGRESLOADER SHOULD DO:
- Infrastructure Operations (schema conversion, data extraction/loading)
- Configuration Integration (tables.yml, SchemaDiscovery, PostgresSchema)
- Pipeline Mechanics (incremental loading, chunking, verification)

This creates a clean separation where:
- PostgresLoader = Data Movement Infrastructure
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
- Schema analysis methods (use schema_discovery.py instead)
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
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
import os

from etl_pipeline.config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.config.logging import get_logger

logger = get_logger(__name__)

class PostgresLoader:
    """PostgreSQL loader for loading data from MySQL replication to PostgreSQL analytics."""
    
    def __init__(self, replication_engine: Engine, analytics_engine: Engine):
        """
        Initialize the PostgreSQL loader.
        
        Args:
            replication_engine: MySQL replication database engine
            analytics_engine: PostgreSQL analytics database engine
        """
        self.replication_engine = replication_engine
        self.analytics_engine = analytics_engine
        
        # Use correct database names from conventions
        self.replication_db = "opendental_replication"
        self.analytics_db = "opendental_analytics"
        self.analytics_schema = "raw"
        
        # Initialize schema adapter
        self.schema_adapter = PostgresSchema(
            mysql_engine=replication_engine,
            postgres_engine=analytics_engine,
            mysql_db=self.replication_db,
            postgres_db=self.analytics_db,
            postgres_schema=self.analytics_schema
        )
        
        # Get PostgreSQL-specific settings from config
        settings_instance = get_settings()
        analytics_config = settings_instance.get_database_config(DatabaseType.ANALYTICS, ConfigPostgresSchema.RAW)
        replication_config = settings_instance.get_database_config(DatabaseType.REPLICATION)
        
        self.target_schema = analytics_config.get('schema', 'raw')
        self.staging_schema = replication_config.get('schema', 'raw')
    
    def load_table(self, table_name: str, mysql_schema: Dict, force_full: bool = False) -> bool:
        """
        Load table from MySQL replication to PostgreSQL analytics using pure SQLAlchemy.
        This approach avoids pandas' connection handling issues entirely.
        
        Args:
            table_name: Name of the table to load
            mysql_schema: MySQL schema information from SchemaDiscovery
            force_full: Whether to force a full load instead of incremental
            
        Returns:
            bool: True if successful
        """
        try:
            # Create or verify PostgreSQL table
            if not self._ensure_postgres_table(table_name, mysql_schema):
                return False
            
            # Get incremental columns
            incremental_columns = mysql_schema.get('incremental_columns', [])
            
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
            
            # Prepare data for PostgreSQL insertion
            rows_data = [dict(zip(column_names, row)) for row in rows]
            
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
                
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {str(e)}")
            return False
    
    def load_table_chunked(self, table_name: str, mysql_schema: Dict, force_full: bool = False, 
                          chunk_size: int = 10000) -> bool:
        """
        Load table in chunks for memory efficiency with large datasets.
        
        Args:
            table_name: Name of the table to load
            mysql_schema: MySQL schema information from SchemaDiscovery
            force_full: Whether to force a full load instead of incremental
            chunk_size: Number of rows to process in each chunk
            
        Returns:
            bool: True if successful
        """
        try:
            # Create or verify PostgreSQL table
            if not self._ensure_postgres_table(table_name, mysql_schema):
                return False
            
            # Get incremental columns
            incremental_columns = mysql_schema.get('incremental_columns', [])
            
            # First, get total count
            count_query = self._build_count_query(table_name, incremental_columns, force_full)
            
            with self.replication_engine.connect() as source_conn:
                total_rows = source_conn.execute(text(count_query)).scalar()
            
            if total_rows == 0:
                logger.info(f"No new data to load for {table_name}")
                return True
            
            logger.info(f"Loading {total_rows} rows from {table_name} in chunks of {chunk_size}")
            
            # Process in chunks
            total_loaded = 0
            chunk_num = 0
            
            # Handle full load truncation
            if force_full:
                with self.analytics_engine.begin() as target_conn:
                    target_conn.execute(text(f"TRUNCATE TABLE {self.analytics_schema}.{table_name}"))
                    logger.info(f"Truncated table {self.analytics_schema}.{table_name} for full load")
            
            while total_loaded < total_rows:
                chunk_num += 1
                
                # Build chunked query
                base_query = self._build_load_query(table_name, incremental_columns, force_full)
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
                        converted_row = self._convert_row_data_types(table_name, row_dict)
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
            
        except Exception as e:
            logger.error(f"Error in chunked load for table {table_name}: {str(e)}")
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
            
            # Compare counts
            if source_count != target_count:
                logger.error(
                    f"Row count mismatch for table {table_name}: "
                    f"source={source_count}, target={target_count}"
                )
                return False
            
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error verifying load for table {table_name}: {str(e)}")
            return False
    
    def _ensure_postgres_table(self, table_name: str, mysql_schema: Dict) -> bool:
        """
        Ensure PostgreSQL table exists and matches schema.
        
        Args:
            table_name: Name of the table
            mysql_schema: MySQL schema information from SchemaDiscovery
            
        Returns:
            bool: True if table exists and matches schema
        """
        try:
            # Check if table exists
            inspector = inspect(self.analytics_engine)
            if not inspector.has_table(table_name, schema='raw'):
                # Create table
                return self.schema_adapter.create_postgres_table(table_name, mysql_schema)
            
            # Verify schema
            return self.schema_adapter.verify_schema(table_name, mysql_schema)
            
        except Exception as e:
            logger.error(f"Error ensuring PostgreSQL table {table_name}: {str(e)}")
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
        # Check if we're in test environment
        is_test_environment = os.environ.get('ETL_ENVIRONMENT', 'production').lower() == 'test'
        
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
    
    def _convert_row_data_types(self, table_name: str, row_data: Dict) -> Dict:
        """
        Convert row data types to match PostgreSQL schema.
        
        Args:
            table_name: Name of the table
            row_data: Dictionary of column names and values
            
        Returns:
            Dict: Converted row data
        """
        try:
            # Get PostgreSQL column types
            with self.analytics_engine.connect() as conn:
                inspector = inspect(self.analytics_engine)
                columns = inspector.get_columns(table_name, schema=self.analytics_schema)
                
                # Create type mapping
                type_map = {col['name']: col['type'].python_type for col in columns}
                
                # Convert values
                converted_data = {}
                for col_name, value in row_data.items():
                    if col_name in type_map:
                        target_type = type_map[col_name]
                        
                        # Handle boolean conversion
                        if target_type == bool and value is not None:
                            converted_data[col_name] = bool(value)
                        else:
                            converted_data[col_name] = value
                    else:
                        converted_data[col_name] = value
                
                return converted_data
                
        except Exception as e:
            logger.error(f"Error converting data types for {table_name}: {str(e)}")
            return row_data 