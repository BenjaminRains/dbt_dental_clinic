"""
PostgreSQL loader implementation for loading data from MySQL staging to PostgreSQL target.
"""
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .base_loader import BaseLoader
from etl_pipeline.config import DatabaseConfig, PipelineConfig

logger = logging.getLogger(__name__)

class PostgresLoader(BaseLoader):
    """PostgreSQL loader implementation."""
    
    def __init__(self, source_engine: Engine, target_engine: Engine):
        super().__init__(source_engine, target_engine)
        self.inspector = inspect(target_engine)
        self.db_config = DatabaseConfig()
        self.pipeline_config = PipelineConfig()
        
        # Get PostgreSQL-specific settings from config
        self.target_schema = self.pipeline_config.get_connection_config('target').get('schema', 'public')
        self.staging_schema = self.pipeline_config.get_connection_config('staging').get('schema', 'staging')
    
    def load_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Load data from MySQL staging to PostgreSQL target.
        
        Args:
            table_name: Name of the table to load
            force_full: Whether to force a full load
            
        Returns:
            bool: True if load was successful
        """
        try:
            # Get last load timestamp
            last_loaded = None if force_full else self.get_last_loaded(table_name)
            
            # Get incremental column
            incremental_column = self.get_incremental_column(table_name)
            
            # Build load query with schema names
            if last_loaded and incremental_column:
                query = f"""
                INSERT INTO {self.target_schema}.{table_name}
                SELECT *
                FROM {self.staging_schema}.{table_name}
                WHERE {incremental_column} > :last_loaded
                """
                params = {"last_loaded": last_loaded}
            else:
                query = f"""
                INSERT INTO {self.target_schema}.{table_name}
                SELECT *
                FROM {self.staging_schema}.{table_name}
                """
                params = {}
            
            # Execute load
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query), params)
                rows_loaded = result.rowcount
            
            # Update load status
            self.update_load_status(table_name, rows_loaded)
            
            # Verify load
            if not self.verify_load(table_name):
                raise Exception(f"Load verification failed for table {table_name}")
            
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error loading table {table_name}: {str(e)}")
            self.update_load_status(table_name, 0, status='failed')
            return False
    
    def get_incremental_column(self, table_name: str) -> Optional[str]:
        """
        Get the best column for incremental loading.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[str]: Name of the incremental column, or None if not found
        """
        try:
            # Get table columns
            columns = self.get_table_columns(table_name)
            
            # Look for timestamp columns
            timestamp_columns = [
                col['name'] for col in columns 
                if col['type'].lower() in ('timestamp', 'timestamptz')
            ]
            
            # Look for date columns
            date_columns = [
                col['name'] for col in columns 
                if col['type'].lower() == 'date'
            ]
            
            # Look for ID columns
            id_columns = [
                col['name'] for col in columns 
                if col['name'].lower().endswith('_id') and 
                col['type'].lower() in ('integer', 'bigint')
            ]
            
            # Return first found column in order of preference
            if timestamp_columns:
                return timestamp_columns[0]
            elif date_columns:
                return date_columns[0]
            elif id_columns:
                return id_columns[0]
            
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
            SELECT last_loaded
            FROM {self.target_schema}.etl_load_status
            WHERE table_name = :table_name
            """
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                row = result.fetchone()
                
                return row[0] if row else None
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting last load timestamp for table {table_name}: {str(e)}")
            return None
    
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
            query = f"""
            INSERT INTO {self.target_schema}.etl_load_status (
                table_name,
                last_loaded,
                rows_loaded,
                status,
                schema_hash
            ) VALUES (
                :table_name,
                :last_loaded,
                :rows_loaded,
                :status,
                :schema_hash
            ) ON CONFLICT (table_name) DO UPDATE SET
                last_loaded = EXCLUDED.last_loaded,
                rows_loaded = EXCLUDED.rows_loaded,
                status = EXCLUDED.status,
                schema_hash = EXCLUDED.schema_hash
            """
            
            schema_hash = self.get_table_schema(table_name)['hash']
            
            with self.target_engine.connect() as conn:
                conn.execute(
                    text(query),
                    {
                        "table_name": table_name,
                        "last_loaded": datetime.now(),
                        "rows_loaded": rows_loaded,
                        "status": status,
                        "schema_hash": schema_hash
                    }
                )
                conn.commit()
                
        except SQLAlchemyError as e:
            logger.error(f"Error updating load status for table {table_name}: {str(e)}")
    
    def verify_load(self, table_name: str) -> bool:
        """
        Verify that the load was successful.
        
        Args:
            table_name: Name of the table
            
        Returns:
            bool: True if verification passed
        """
        try:
            # Get row counts
            source_count = self.get_table_row_count(table_name)
            target_count = self.get_table_row_count(table_name)
            
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
        Get the schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict[str, Any]: Schema information
        """
        try:
            # Get CREATE TABLE statement
            with self.target_engine.connect() as conn:
                result = conn.execute(text(f"SELECT pg_get_ddl('{table_name}')"))
                create_stmt = result.fetchone()[0]
            
            # Calculate schema hash
            schema_hash = hashlib.md5(create_stmt.encode()).hexdigest()
            
            return {
                "create_statement": create_stmt,
                "hash": schema_hash
            }
            
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
            return current_schema['hash'] != stored_hash
            
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
            with self.target_engine.connect() as conn:
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
            
            with self.target_engine.connect() as conn:
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
            return self.inspector.get_indexes(table_name)
            
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
            return self.inspector.get_unique_constraints(table_name)
            
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
            return self.inspector.get_foreign_keys(table_name)
            
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
            return self.inspector.get_columns(table_name)
            
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
            return self.inspector.get_pk_constraint(table_name)['constrained_columns']
            
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
            
            with self.target_engine.connect() as conn:
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
            
            with self.target_engine.connect() as conn:
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
            
            with self.target_engine.connect() as conn:
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
            
            with self.target_engine.connect() as conn:
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
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return [dict(row) for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting dependencies for table {table_name}: {str(e)}")
            return [] 