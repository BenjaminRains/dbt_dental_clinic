"""
Schema discovery module for MySQL schema replication and change detection.
Handles schema extraction, comparison, and change detection.
"""
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
import hashlib
import re
from datetime import datetime

# Use the new logger architecture
from etl_pipeline.core.logger import get_logger
from etl_pipeline.config.settings import settings

logger = get_logger(__name__)

class SchemaDiscovery:
    """Handles MySQL schema discovery and replication."""
    
    def __init__(self, source_engine: Engine, target_engine: Engine, source_db: str, target_db: str):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db = source_db
        self.target_db = target_db
        self._schema_cache = {}
        # Use the new settings system instead of PipelineConfig
        self.settings = settings
    
    def get_table_schema(self, table_name: str) -> Dict:
        """
        Get the complete schema information for a table.
        Includes CREATE TABLE statement, indexes, constraints, and detailed column information.
        """
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                # Get CREATE TABLE statement
                result = conn.execute(text(f"SHOW CREATE TABLE {table_name}"))
                row = result.fetchone()
                
                if not row:
                    raise ValueError(f"Table {table_name} not found in {self.source_db}")
                
                create_statement = row[1]
                
                # Get table metadata
                metadata = self._get_table_metadata(conn, table_name)
                
                # Get indexes
                indexes = self._get_table_indexes(conn, table_name)
                
                # Get foreign keys
                foreign_keys = self._get_foreign_keys(conn, table_name)
                
                # Get detailed column information
                columns = self._get_detailed_columns(conn, table_name)
                
                schema_info = {
                    'table_name': table_name,
                    'create_statement': create_statement,
                    'metadata': metadata,
                    'indexes': indexes,
                    'foreign_keys': foreign_keys,
                    'columns': columns,
                    'schema_hash': self._calculate_schema_hash(create_statement)
                }
                
                # Cache the schema info
                self._schema_cache[table_name] = schema_info
                
                return schema_info
                
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            raise
    
    def _get_table_metadata(self, conn, table_name: str) -> Dict:
        """Get table metadata including engine, charset, and row count."""
        result = conn.execute(text(f"SHOW TABLE STATUS LIKE '{table_name}'"))
        row = result.fetchone()
        
        if not row:
            return {
                'engine': 'InnoDB',
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_general_ci',
                'auto_increment': None,
                'row_count': 0
            }
        
        return {
            'engine': row.Engine or 'InnoDB',
            'charset': getattr(row, 'Collation', 'utf8mb4_general_ci').split('_')[0] if hasattr(row, 'Collation') else 'utf8mb4',
            'collation': getattr(row, 'Collation', 'utf8mb4_general_ci') if hasattr(row, 'Collation') else 'utf8mb4_general_ci',
            'auto_increment': getattr(row, 'Auto_increment', None) if hasattr(row, 'Auto_increment') else None,
            'row_count': getattr(row, 'Rows', 0) if hasattr(row, 'Rows') else 0
        }
    
    def _get_table_indexes(self, conn, table_name: str) -> List[Dict]:
        """Get all indexes for a table."""
        query = text("""
            SELECT 
                index_name,
                GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
                non_unique,
                index_type
            FROM information_schema.statistics
            WHERE table_schema = :db_name
            AND table_name = :table_name
            GROUP BY index_name, non_unique, index_type
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        indexes = []
        for row in result:
            indexes.append({
                'name': row.index_name,
                'columns': row.columns.split(','),
                'is_unique': not row.non_unique,
                'type': row.index_type
            })
        
        return indexes
    
    def _get_foreign_keys(self, conn, table_name: str) -> List[Dict]:
        """Get all foreign key constraints for a table."""
        query = text("""
            SELECT 
                constraint_name,
                column_name,
                referenced_table_name,
                referenced_column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = :db_name
            AND table_name = :table_name
            AND referenced_table_name IS NOT NULL
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        foreign_keys = []
        for row in result:
            foreign_keys.append({
                'name': row.constraint_name,
                'column': row.column_name,
                'referenced_table': row.referenced_table_name,
                'referenced_column': row.referenced_column_name
            })
        
        return foreign_keys
    
    def _calculate_schema_hash(self, create_statement: str) -> str:
        """Calculate a hash of the schema for change detection."""
        # Normalize the CREATE statement by removing variable parts
        normalized = re.sub(r'AUTO_INCREMENT=\d+', 'AUTO_INCREMENT=1', create_statement)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def has_schema_changed(self, table_name: str, stored_hash: str) -> bool:
        """Check if the table schema has changed."""
        try:
            current_schema = self.get_table_schema(table_name)
            current_hash = current_schema['schema_hash']
            return current_hash != stored_hash
        except Exception as e:
            logger.error(f"Error checking schema changes for {table_name}: {str(e)}")
            return True  # Assume changed if we can't determine
    
    def replicate_schema(self, table_name: str, drop_if_exists: bool = True) -> bool:
        """
        Replicate the source table schema to the target database.
        Preserves all MySQL-specific features.
        """
        try:
            schema_info = self.get_table_schema(table_name)
            create_statement = schema_info['create_statement']
            
            # Modify the CREATE statement for the target database
            target_create_statement = self._adapt_create_statement_for_target(
                create_statement, table_name
            )
            
            with self.target_engine.begin() as conn:
                conn.execute(text(f"USE {self.target_db}"))
                
                # Drop table if it exists and requested
                if drop_if_exists:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    logger.info(f"Dropped existing table {table_name} in target")
                
                # Create the exact replica
                conn.execute(text(target_create_statement))
                
                logger.info(f"Created exact replica of {table_name} in {self.target_db}")
                
                # Verify the table was created successfully
                verify_result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                if not verify_result.fetchone():
                    raise Exception(f"Table {table_name} was not created successfully")
                
                return True
                
        except Exception as e:
            logger.error(f"Error replicating schema for {table_name}: {str(e)}")
            return False
    
    def _adapt_create_statement_for_target(self, create_statement: str, table_name: str) -> str:
        """
        Adapt the CREATE statement for the target database.
        Ensures compatibility while preserving the exact structure.
        """
        # Replace the table name to ensure it's unqualified
        pattern = r'CREATE TABLE `?[^`\s]*`?\.`?' + re.escape(table_name) + r'`?'
        replacement = f'CREATE TABLE `{table_name}`'
        adapted = re.sub(pattern, replacement, create_statement, flags=re.IGNORECASE)
        
        # Also handle simple cases without database qualification
        pattern2 = r'CREATE TABLE `?' + re.escape(table_name) + r'`?'
        replacement2 = f'CREATE TABLE `{table_name}`'
        adapted = re.sub(pattern2, replacement2, adapted, flags=re.IGNORECASE)
        
        # Reset AUTO_INCREMENT to 1 for clean start
        adapted = re.sub(r'AUTO_INCREMENT=\d+', 'AUTO_INCREMENT=1', adapted)
        
        # Ensure we're using a compatible storage engine (usually InnoDB)
        if 'ENGINE=MyISAM' in adapted:
            adapted = adapted.replace('ENGINE=MyISAM', 'ENGINE=InnoDB')
            logger.info(f"Converted MyISAM to InnoDB for table {table_name}")
        
        return adapted
    
    def get_incremental_columns(self, table_name: str) -> List[Dict]:
        """
        Find suitable columns for incremental extraction.
        Returns a list of columns ordered by their suitability for incremental extraction.
        """
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                query = text("""
                    SELECT 
                        column_name,
                        data_type,
                        column_default,
                        extra,
                        column_comment
                    FROM information_schema.columns 
                    WHERE table_schema = :db_name 
                    AND table_name = :table_name
                    AND (
                        data_type IN ('datetime', 'timestamp')
                        OR (data_type = 'int' AND extra LIKE '%auto_increment%')
                        OR column_name REGEXP '(date|time|modified|updated|created)'
                    )
                    ORDER BY 
                        CASE 
                            WHEN column_name REGEXP '(modified|updated)' THEN 1
                            WHEN column_name REGEXP '(created|inserted)' THEN 2
                            WHEN data_type IN ('datetime', 'timestamp') THEN 3
                            ELSE 4
                        END,
                        column_name
                """)
                
                result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
                
                incremental_columns = []
                for row in result:
                    incremental_columns.append({
                        'column_name': row.column_name,
                        'data_type': row.data_type,
                        'default': row.column_default,
                        'extra': row.extra,
                        'comment': row.column_comment,
                        'priority': self._get_column_priority(row.column_name, row.data_type, row.extra)
                    })
                
                return sorted(incremental_columns, key=lambda x: x['priority'])
                
        except Exception as e:
            logger.error(f"Error finding incremental columns for {table_name}: {str(e)}")
            return []
    
    def _get_column_priority(self, column_name: str, data_type: str, extra: str) -> int:
        """Assign priority for incremental extraction columns."""
        name_lower = column_name.lower()
        
        # Highest priority: explicit modification tracking
        if any(keyword in name_lower for keyword in ['datemodified', 'modified', 'updated', 'lastmodified']):
            return 1
        
        # High priority: creation tracking  
        if any(keyword in name_lower for keyword in ['datecreated', 'created', 'inserted', 'dateinserted']):
            return 2
        
        # Medium priority: timestamp columns
        if data_type in ['timestamp'] and 'DEFAULT CURRENT_TIMESTAMP' in (extra or ''):
            return 3
        
        # Lower priority: other datetime columns
        if data_type in ['datetime', 'timestamp']:
            return 4
        
        # Lowest priority: auto increment (for append-only tables)
        if 'auto_increment' in (extra or '').lower():
            return 5
        
        return 10
    
    def _get_detailed_columns(self, conn, table_name: str) -> List[Dict]:
        """Get detailed information about all columns in a table."""
        query = text("""
            SELECT 
                COLUMN_NAME,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_KEY,
                COLUMN_DEFAULT,
                EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = :db_name
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        columns = []
        for row in result:
            columns.append({
                'name': row.COLUMN_NAME,
                'type': row.COLUMN_TYPE,
                'is_nullable': row.IS_NULLABLE == 'YES',
                'key_type': row.COLUMN_KEY,  # PRI, MUL, etc.
                'default': row.COLUMN_DEFAULT,
                'extra': row.EXTRA
            })
        
        return columns

    def discover_all_tables(self) -> List[str]:
        """Discover all tables in the source database."""
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :db_name 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                
                result = conn.execute(query.bindparams(db_name=self.source_db))
                return [row.table_name for row in result]
                
        except Exception as e:
            logger.error(f"Error discovering tables: {str(e)}")
            return []

    def get_table_size_info(self, table_name: str) -> Dict:
        """Get size and row count information for a table."""
        try:
            with self.source_engine.connect() as conn:
                query = text("""
                    SELECT 
                        table_rows,
                        data_length,
                        index_length,
                        (data_length + index_length) as total_size
                    FROM information_schema.tables
                    WHERE table_schema = :db_name
                    AND table_name = :table_name
                """)
                
                result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
                row = result.fetchone()
                
                if row:
                    return {
                        'row_count': row.table_rows or 0,
                        'data_size_bytes': row.data_length or 0,
                        'index_size_bytes': row.index_length or 0,
                        'total_size_bytes': row.total_size or 0,
                        'data_size_mb': round((row.data_length or 0) / (1024 * 1024), 2),
                        'index_size_mb': round((row.index_length or 0) / (1024 * 1024), 2),
                        'total_size_mb': round((row.total_size or 0) / (1024 * 1024), 2)
                    }
                else:
                    return {
                        'row_count': 0,
                        'data_size_bytes': 0,
                        'index_size_bytes': 0,
                        'total_size_bytes': 0,
                        'data_size_mb': 0.0,
                        'index_size_mb': 0.0,
                        'total_size_mb': 0.0
                    }
                    
        except Exception as e:
            logger.error(f"Error getting size info for {table_name}: {str(e)}")
            return {}