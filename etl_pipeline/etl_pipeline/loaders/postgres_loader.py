"""
PostgreSQL loader implementation for loading data from MySQL replication to PostgreSQL analytics.

WELL-IMPLEMENTED BUT OVER-ENGINEERED - NEEDS SIMPLIFICATION
==========================================================
This PostgresLoader is a concrete implementation with good core functionality
but suffers from over-engineering due to inheriting from the bloated BaseLoader interface.

Current Status:
- Core loading logic is well-implemented and production-ready
- Handles MySQL to PostgreSQL data transfer with schema conversion
- Supports incremental loading, chunked processing, and data validation
- Integrates properly with PostgresSchema and Settings classes
- Implements 20+ methods from BaseLoader (many unnecessary)

Problems:
- Over-engineered due to BaseLoader inheritance
- Many methods duplicate functionality from schema_discovery.py
- Complex interface for what should be simple loading operations
- Redundant schema analysis methods (get_table_schema, has_schema_changed, etc.)
- Unnecessary metadata methods (get_table_grants, get_table_triggers, etc.)

What's Good:
- Core load_table() method is well-implemented
- Proper error handling and logging throughout
- Integration with PostgresSchema for schema conversion
- Support for incremental and chunked loading
- Type conversion between MySQL and PostgreSQL
- Load verification and status tracking

What Should Be Removed:
- Schema analysis methods (use schema_discovery.py instead)
- Metadata methods (grants, triggers, views, dependencies)
- Redundant table information methods
- Methods that duplicate existing functionality

Recommendations:
- Keep core loading methods (load_table, verify_load, update_load_status)
- Remove redundant schema analysis methods
- Simplify interface to focus on actual ETL operations
- Maintain integration with PostgresSchema and Settings
- Consider creating a simplified loader interface

TODO: Simplify interface by removing redundant methods
TODO: Focus on core loading operations only
TODO: Remove schema analysis methods (use schema_discovery.py)
TODO: Create streamlined loader interface
TODO: Maintain core functionality while reducing complexity
"""
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
import pandas as pd

from .base_loader import BaseLoader
from etl_pipeline.config.settings import settings
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.core.logger import get_logger

logger = get_logger(__name__)

class PostgresLoader(BaseLoader):
    """PostgreSQL loader for loading data from MySQL replication to PostgreSQL analytics."""
    
    def __init__(self, replication_engine: Engine, analytics_engine: Engine):
        """
        Initialize the PostgreSQL loader.
        
        Args:
            replication_engine: MySQL replication database engine
            analytics_engine: PostgreSQL analytics database engine
        """
        super().__init__(replication_engine, analytics_engine)
        
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
        analytics_config = settings.get_database_config('analytics')
        replication_config = settings.get_database_config('replication')
        
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
        # If force_full is True, return full table query
        if force_full:
            logger.info(f"Building full load query for {table_name}")
            return f"SELECT * FROM {table_name}"
            
        # Get last load timestamp
        last_load = self._get_last_load(table_name)
        
        if last_load and incremental_columns:
            # Build incremental condition
            conditions = []
            for col in incremental_columns:
                conditions.append(f"{col} > '{last_load}'")
            
            where_clause = " AND ".join(conditions)
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"
            logger.info(f"Building incremental query for {table_name} since {last_load}")
            return query
        
        logger.info(f"No incremental columns or last load timestamp found for {table_name}, using full query")
        return f"SELECT * FROM {table_name}"
    
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
    
    def _update_load_status_internal(self, conn, table_name: str, rows_loaded: int, 
                                   status: str = 'success') -> None:
        """
        Update load status in the same transaction.
        
        Args:
            conn: Database connection
            table_name: Name of the table
            rows_loaded: Number of rows loaded
            status: Load status
        """
        schema_hash = self.get_table_schema(table_name)['schema_hash']
        
        conn.execute(
            text(f"""
                INSERT INTO {self.analytics_schema}.etl_transform_status 
                (table_name, last_transformed, rows_transformed, transformation_status, schema_hash)
                VALUES (:table_name, CURRENT_TIMESTAMP, :rows, :status, :schema_hash)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_transformed = CURRENT_TIMESTAMP,
                    rows_transformed = :rows,
                    transformation_status = :status,
                    schema_hash = :schema_hash,
                    updated_at = CURRENT_TIMESTAMP
            """),
            {
                "table_name": table_name,
                "rows": rows_loaded,
                "status": status,
                "schema_hash": schema_hash
            }
        )
    
    def update_load_status(self, table_name: str, rows_loaded: int, 
                          status: str = 'success') -> None:
        """
        Update the load status for a table.
        
        Args:
            table_name: Name of the table
            rows_loaded: Number of rows loaded
            status: Load status ('success', 'failed', etc.)
        """
        try:
            with self.analytics_engine.begin() as conn:
                self._update_load_status_internal(conn, table_name, rows_loaded, status)
                
        except SQLAlchemyError as e:
            logger.error(f"Error updating load status for table {table_name}: {str(e)}")
            raise
    
    def get_incremental_column(self, table_name: str) -> Optional[str]:
        """
        Get the best column for incremental loading.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[str]: Name of the incremental column, or None if not found
        """
        try:
            # Use schema discovery to find incremental columns
            incremental_columns = self.schema_adapter.get_incremental_columns(table_name)
            
            # Return the highest priority column if available
            if incremental_columns:
                return incremental_columns[0]['column_name']
            
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting incremental column for table {table_name}: {str(e)}")
            return None
    
    def get_last_loaded(self, table_name: str) -> Optional[datetime]:
        """
        Get the last load timestamp for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[datetime]: Last load timestamp, or None if not found
        """
        try:
            query = f"""
            SELECT last_transformed
            FROM {self.analytics_schema}.etl_transform_status
            WHERE table_name = :table_name
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                row = result.fetchone()
                
                return row[0] if row else None
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting last load timestamp for table {table_name}: {str(e)}")
            return None
    
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
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the complete schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict[str, Any]: Complete schema information
        """
        try:
            return self.schema_adapter.get_table_schema(table_name)
        except SQLAlchemyError as e:
            logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            return {}
    
    def has_schema_changed(self, table_name: str, stored_hash: str) -> bool:
        """
        Check if the table schema has changed.
        
        Args:
            table_name: Name of the table
            stored_hash: Previously stored schema hash
            
        Returns:
            bool: True if schema has changed
        """
        try:
            current_schema = self.get_table_schema(table_name)
            return current_schema['schema_hash'] != stored_hash
            
        except SQLAlchemyError as e:
            logger.error(f"Error checking schema change for table {table_name}: {str(e)}")
            return True
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict[str, Any]: Table metadata
        """
        try:
            return {
                "row_count": self.get_table_row_count(table_name),
                "size_bytes": self.get_table_size(table_name),
                "columns": self.get_table_columns(table_name),
                "primary_key": self.get_table_primary_key(table_name),
                "indexes": self.get_table_indexes(table_name),
                "foreign_keys": self.get_table_foreign_keys(table_name),
                "constraints": self.get_table_constraints(table_name),
                "partitions": self.get_table_partitions(table_name),
                "grants": self.get_table_grants(table_name),
                "triggers": self.get_table_triggers(table_name),
                "views": self.get_table_views(table_name),
                "dependencies": self.get_table_dependencies(table_name)
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting metadata for table {table_name}: {str(e)}")
            return {}
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Number of rows
        """
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.fetchone()[0]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting row count for table {table_name}: {str(e)}")
            return 0
    
    def get_table_size(self, table_name: str) -> int:
        """
        Get the size of a table in bytes.
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Table size in bytes
        """
        try:
            query = """
            SELECT pg_total_relation_size(:table_name)
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return result.fetchone()[0]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting size for table {table_name}: {str(e)}")
            return 0
    
    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the indexes for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of index information
        """
        try:
            return self.schema_adapter.get_indexes(table_name)
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting indexes for table {table_name}: {str(e)}")
            return []
    
    def get_table_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the constraints for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of constraint information
        """
        try:
            return self.schema_adapter.get_unique_constraints(table_name)
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting constraints for table {table_name}: {str(e)}")
            return []
    
    def get_table_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the foreign keys for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of foreign key information
        """
        try:
            return self.schema_adapter.get_foreign_keys(table_name)
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting foreign keys for table {table_name}: {str(e)}")
            return []
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of column information
        """
        try:
            return self.schema_adapter.get_columns(table_name)
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting columns for table {table_name}: {str(e)}")
            return []
    
    def get_table_primary_key(self, table_name: str) -> Optional[List[str]]:
        """
        Get the primary key columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[List[str]]: List of primary key column names, or None if no primary key
        """
        try:
            return self.schema_adapter.get_pk_constraint(table_name)['constrained_columns']
            
        except SQLAlchemyError as e:
            logger.error(f"Error getting primary key for table {table_name}: {str(e)}")
            return None
    
    def get_table_partitions(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get the partitions for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[List[Dict[str, Any]]]: List of partition information, or None if not partitioned
        """
        try:
            query = """
            SELECT *
            FROM pg_partitions
            WHERE tablename = :table_name
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                partitions = result.fetchall()
                
                return [dict(row) for row in partitions] if partitions else None
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting partitions for table {table_name}: {str(e)}")
            return None
    
    def get_table_grants(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the grants for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of grant information
        """
        try:
            query = """
            SELECT *
            FROM information_schema.role_table_grants
            WHERE table_name = :table_name
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return [dict(row) for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting grants for table {table_name}: {str(e)}")
            return []
    
    def get_table_triggers(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the triggers for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of trigger information
        """
        try:
            query = """
            SELECT *
            FROM information_schema.triggers
            WHERE event_object_table = :table_name
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return [dict(row) for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting triggers for table {table_name}: {str(e)}")
            return []
    
    def get_table_views(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the views that reference a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of view information
        """
        try:
            query = """
            SELECT DISTINCT v.viewname
            FROM pg_views v
            JOIN pg_depend d ON d.refobjid = v.oid
            JOIN pg_class c ON c.oid = d.objid
            WHERE c.relname = :table_name
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return [dict(row) for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting views for table {table_name}: {str(e)}")
            return []
    
    def get_table_dependencies(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get the dependencies for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict[str, Any]]: List of dependency information
        """
        try:
            query = """
            SELECT
                c.relname as dependent_table,
                a.attname as dependent_column,
                c2.relname as referenced_table,
                a2.attname as referenced_column
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_class c2 ON c2.oid = con.confrelid
            JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = c.oid
            JOIN pg_attribute a2 ON a2.attnum = ANY(con.confkey) AND a2.attrelid = c2.oid
            WHERE c2.relname = :table_name
            """
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return [dict(row) for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting dependencies for table {table_name}: {str(e)}")
            return []

    def _build_load_query_with_limit(self, table_name: str, incremental_columns: List[str], 
                                    force_full: bool = False, limit: int = None) -> str:
        """
        Enhanced query builder with optional limit for memory management.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns
            force_full: Whether to force a full load instead of incremental
            limit: Optional limit for large datasets
            
        Returns:
            str: SQL query
        """
        base_query = self._build_load_query(table_name, incremental_columns, force_full)
        
        if limit and limit > 0:
            base_query += f" LIMIT {limit}"
        
        return base_query

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