"""
Schema discovery module for MySQL schema replication.
Handles schema extraction, comparison, and change detection between MySQL databases.

SCHEMA DISCOVERY AND ANALYSIS - WORKS WITH mysql_replicator.py
============================================================
This SchemaDiscovery class provides the intelligence layer for MySQL-to-MySQL
table replication, working in conjunction with mysql_replicator.py.

What This Module Does:
- Discovers and analyzes MySQL table schemas in detail
- Detects schema changes using hash-based comparison
- Provides comprehensive schema information (indexes, foreign keys, metadata)
- Identifies columns suitable for incremental loading
- Extracts table size information and performance metrics
- Supports exact schema replication between MySQL databases

Integration with mysql_replicator.py:
- schema_discovery.py: "What to copy" (analysis, change detection)
- mysql_replicator.py: "How to copy" (actual data copying, schema creation)

Typical Workflow:
1. SchemaDiscovery analyzes source table schema
2. Detects if schema has changed since last replication
3. Provides schema information to mysql_replicator.py
4. mysql_replicator.py creates exact replica and copies data

Key Features:
- Schema change detection for incremental ETL operations
- Complete schema analysis (CREATE statements, indexes, constraints)
- Table metadata extraction (engine, charset, row counts)
- Foreign key and index discovery
- Column-level analysis for incremental loading
- Size information for performance optimization

Dependencies:
- Works with OpenDental source database (MySQL)
- Works with MySQL replication database (MySQL)
- Uses Settings class for configuration
- Integrates with ETL pipeline for change detection
- Provides data to mysql_replicator.py for execution

Note: This is NOT PostgreSQL conversion - that's handled by postgres_schema.py
"""
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
import hashlib
import re
from datetime import datetime
import time

# Use the new logger architecture
from etl_pipeline.config.logging import get_logger
from etl_pipeline.config.settings import settings

logger = get_logger(__name__)

class SchemaDiscovery:
    """Handles MySQL schema discovery and replication between MySQL databases."""
    
    def __init__(self, source_engine: Engine, target_engine: Engine, source_db: str, target_db: str):
        """
        Initialize schema discovery for MySQL-to-MySQL replication.
        
        Args:
            source_engine: Source MySQL database engine (OpenDental)
            target_engine: Target MySQL database engine (Replication)
            source_db: Source database name
            target_db: Target database name
        """
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db = source_db
        self.target_db = target_db
        self._schema_cache = {}
        # Use the new settings system instead of PipelineConfig
        self.settings = settings
        
        # Initialize inspectors
        self.source_inspector = inspect(source_engine)
        self.target_inspector = inspect(target_engine)
    
    def get_table_schema(self, table_name: str) -> Dict:
        """
        Get the complete schema information for a table.
        Includes CREATE TABLE statement, indexes, constraints, and detailed column information.
        """
        try:
            logger.info(f"Getting schema information for {table_name}...")
            start_time = time.time()
            
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
                
                # Get indexes (only if needed)
                indexes = self._get_table_indexes(conn, table_name)
                
                # Get foreign keys (only if needed)
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
                
                elapsed = time.time() - start_time
                logger.info(f"Completed schema discovery for {table_name} in {elapsed:.2f}s")
                
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
    
    def replicate_schema(self, source_table: str, target_table: str = None, drop_if_exists: bool = True) -> bool:
        """
        Replicate a table's schema to the target database.
        
        Args:
            source_table: Name of the source table to replicate
            target_table: Name to use for the target table (defaults to source_table)
            drop_if_exists: Whether to drop the target table if it exists
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use source table name as target if not specified
            target_table = target_table or source_table
            
            # Get the CREATE TABLE statement from source
            create_statement = self.get_table_schema(source_table)['create_statement']
            
            # Modify the CREATE statement for the target database
            target_create_statement = self._adapt_create_statement_for_target(
                create_statement, target_table
            )
            
            with self.target_engine.begin() as conn:
                conn.execute(text(f"USE {self.target_db}"))
                
                # Drop table if it exists and requested
                if drop_if_exists:
                    conn.execute(text(f"DROP TABLE IF EXISTS `{target_table}`"))
                    logger.info(f"Dropped existing table {target_table} in target")
                
                # Create the exact replica
                conn.execute(text(target_create_statement))
                
                logger.info(f"Created exact replica of {source_table} as {target_table} in {self.target_db}")
                
                # Verify the table was created successfully
                verify_result = conn.execute(text(f"SHOW TABLES LIKE '{target_table}'"))
                if not verify_result.fetchone():
                    raise Exception(f"Table {target_table} was not created successfully")
                
                return True
                
        except Exception as e:
            logger.error(f"Error replicating schema for {source_table}: {str(e)}")
            return False
    
    def _adapt_create_statement_for_target(self, create_statement: str, target_table: str) -> str:
        """
        Adapt the CREATE statement for the target database.
        Ensures compatibility while preserving the exact structure.
        """
        # First, extract the table definition part (everything after CREATE TABLE)
        parts = create_statement.split('CREATE TABLE', 1)
        if len(parts) != 2:
            raise ValueError("Invalid CREATE TABLE statement")
            
        # Get the table definition part and remove any existing table name
        table_def = parts[1].strip()
        table_def = re.sub(r'^`?\w+`?\s+', '', table_def)
        
        # Create the new statement with the target table name
        return f"CREATE TABLE `{target_table}` {table_def}"
    
    def get_incremental_columns(self, table_name: str) -> List[Dict]:
        """
        Get all columns that can be used for incremental loading.
        All columns are considered equally for incremental loading.
        """
        try:
            columns = self._get_detailed_columns(self.source_engine.connect(), table_name)
            
            # All columns are considered for incremental loading
            incremental_columns = []
            for col in columns:
                incremental_columns.append({
                    'column_name': col['name'],
                    'data_type': col['type'],
                    'default': col['default'],
                    'extra': col['extra'],
                    'comment': col.get('comment', ''),
                    'priority': 1  # All columns have equal priority
                })
            
            return incremental_columns
            
        except Exception as e:
            logger.error(f"Error getting incremental columns for {table_name}: {str(e)}")
            return []
    
    def _get_detailed_columns(self, conn, table_name: str) -> List[Dict]:
        """Get detailed information about all columns in a table."""
        query = text("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                extra,
                column_comment,
                column_key
            FROM information_schema.columns
            WHERE table_schema = :db_name
            AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        
        result = conn.execute(query.bindparams(db_name=self.source_db, table_name=table_name))
        
        columns = []
        for row in result:
            columns.append({
                'name': row.column_name,
                'type': row.data_type,
                'is_nullable': row.is_nullable == 'YES',
                'default': row.column_default,
                'extra': row.extra,
                'comment': row.column_comment,
                'key_type': row.column_key
            })
        
        return columns
    
    def discover_all_tables(self) -> List[str]:
        """Get a list of all tables in the source database."""
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                result = conn.execute(text("SHOW TABLES"))
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error discovering tables: {str(e)}")
            return []
    
    def get_table_size_info(self, table_name: str) -> Dict:
        """Get size information for a table."""
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                # Get table size information
                result = conn.execute(text(f"""
                    SELECT 
                        table_rows as row_count,
                        data_length as data_size_bytes,
                        index_length as index_size_bytes,
                        data_length + index_length as total_size_bytes
                    FROM information_schema.tables
                    WHERE table_schema = :db_name
                    AND table_name = :table_name
                """).bindparams(db_name=self.source_db, table_name=table_name))
                
                row = result.fetchone()
                if not row:
                    return {
                        'row_count': 0,
                        'data_size_bytes': 0,
                        'index_size_bytes': 0,
                        'total_size_bytes': 0,
                        'data_size_mb': 0,
                        'index_size_mb': 0,
                        'total_size_mb': 0
                    }
                
                # Convert bytes to MB
                data_size_mb = row.data_size_bytes / (1024 * 1024)
                index_size_mb = row.index_size_bytes / (1024 * 1024)
                total_size_mb = row.total_size_bytes / (1024 * 1024)
                
                return {
                    'row_count': row.row_count,
                    'data_size_bytes': row.data_size_bytes,
                    'index_size_bytes': row.index_size_bytes,
                    'total_size_bytes': row.total_size_bytes,
                    'data_size_mb': round(data_size_mb, 2),
                    'index_size_mb': round(index_size_mb, 2),
                    'total_size_mb': round(total_size_mb, 2)
                }
                
        except Exception as e:
            logger.error(f"Error getting size info for {table_name}: {str(e)}")
            return {
                'row_count': 0,
                'data_size_bytes': 0,
                'index_size_bytes': 0,
                'total_size_bytes': 0,
                'data_size_mb': 0,
                'index_size_mb': 0,
                'total_size_mb': 0
            }