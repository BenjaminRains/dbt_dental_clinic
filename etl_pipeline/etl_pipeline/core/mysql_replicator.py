"""
MySQL replicator focused on exact table replication and schema management.

NOTE: This is NOT actual MySQL database replication!
====================================================
This file is MISNAMED and MISLEADING. It does NOT perform:
- Real-time MySQL replication
- Change data capture
- Continuous database synchronization
- MySQL's built-in replication features

What it ACTUALLY does:
- Copy individual tables between MySQL databases
- Recreate exact table schemas
- One-time data migration
- Table-level copying utility

Better names would be:
- mysql_schema_replicator.py

**Files to Update:**
- `etl_pipeline/orchestration/pipeline_orchestrator.py` (Line 82)
- `etl_pipeline/orchestration/table_processor.py` (Line 103)
- `tests/core/test_mysql_replicator.py` (Line 24)

TODO: Fix import statements in dependent files
TODO: Rename this file to accurately reflect its functionality: mysql_schema_replicator.py
TODO: Check alignment with schema_discovery.py 
TODO: Add tests
"""
import os
import re
import hashlib
import json
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import time

logger = logging.getLogger(__name__)

class ExactMySQLReplicator:
    """
    Creates exact MySQL table replicas with focus on schema and data integrity.
    """
    
    def __init__(self, source_engine: Engine, target_engine: Engine, source_db: str, target_db: str):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db = source_db
        self.target_db = target_db
        self.inspector = inspect(source_engine)
        
        # Cache for schema information
        self._schema_cache = {}
        
        # Timeout settings
        self.query_timeout = 300  # 5 minutes max per query
        self.max_batch_size = 10000  # Reasonable batch size
        
    def create_exact_replica(self, table_name: str) -> bool:
        """Create an exact replica of a table in the target database."""
        try:
            # Get source schema
            source_schema = self.get_exact_table_schema(table_name, self.source_engine)
            if not source_schema:
                logger.error(f"Could not get source schema for {table_name}")
                return False
            
            # Create target table
            with self.target_engine.connect() as conn:
                # Drop if exists
                conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
                
                # Create table with exact schema
                create_stmt = self._adapt_create_statement_for_target(
                    source_schema['create_statement'],
                    table_name
                )
                conn.execute(text(create_stmt))
                logger.info(f"Recreated table {table_name} in target")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating exact target table {table_name}: {str(e)}")
            return False

    def copy_table_data(self, table_name: str) -> bool:
        """Copy data from source to target table."""
        try:
            # Get row count
            with self.source_engine.connect() as conn:
                row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{table_name}`")
                ).scalar()
            
            logger.info(f"[COPY] Estimated rows: {row_count:,}")
            
            # Truncate target
            with self.target_engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE `{table_name}`"))
                logger.info(f"[COPY] Truncated target table {table_name}")
            
            # Copy data
            if row_count <= self.max_batch_size:
                return self._copy_direct(table_name, row_count)
            else:
                return self._copy_chunked(table_name, row_count)
                
        except Exception as e:
            logger.error(f"Error copying data for {table_name}: {str(e)}")
            return False

    def verify_exact_replica(self, table_name: str) -> bool:
        """Verify that the target table is an exact replica of the source table."""
        try:
            # Get source schema
            source_schema = self.get_exact_table_schema(table_name, self.source_engine)
            if not source_schema:
                logger.error(f"[VERIFY] Could not get source schema for {table_name}")
                return False
                
            # Get target schema
            target_schema = self.get_exact_table_schema(table_name, self.target_engine)
            if not target_schema:
                logger.error(f"[VERIFY] Could not get target schema for {table_name}")
                return False
            
            # Compare schema hashes
            if source_schema['schema_hash'] != target_schema['schema_hash']:
                logger.error(f"[VERIFY] Schema mismatch for {table_name}")
                logger.error(f"[VERIFY] Source hash: {source_schema['schema_hash']}")
                logger.error(f"[VERIFY] Target hash: {target_schema['schema_hash']}")
                logger.error(f"[VERIFY] Source schema:\n{source_schema['create_statement']}")
                logger.error(f"[VERIFY] Target schema:\n{target_schema['create_statement']}")
                logger.error(f"[VERIFY] Normalized source schema:\n{source_schema['normalized_schema']}")
                logger.error(f"[VERIFY] Normalized target schema:\n{target_schema['normalized_schema']}")
                return False
            
            # Compare row counts
            source_count = self._get_row_count(table_name, self.source_engine)
            target_count = self._get_row_count(table_name, self.target_engine)
            
            if source_count != target_count:
                logger.error(f"[VERIFY] Row count mismatch for {table_name}")
                logger.error(f"[VERIFY] Source count: {source_count}")
                logger.error(f"[VERIFY] Target count: {target_count}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"[VERIFY] Error verifying replica for {table_name}: {str(e)}")
            return False

    def _copy_direct(self, table_name: str, row_count: int) -> bool:
        """Direct table copy for smaller tables."""
        try:
            with self.source_engine.connect() as source_conn, \
                 self.target_engine.connect() as target_conn:
                
                # Copy all data
                copy_query = text(f"SELECT * FROM `{table_name}`")
                result = source_conn.execute(copy_query)
                rows = result.fetchall()
                
                if not rows:
                    logger.info(f"[COPY] No data to copy from {table_name}")
                    return True
                
                # Insert data
                columns = result.keys()
                placeholders = ', '.join([':' + col for col in columns])
                insert_query = text(f"""
                    INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in columns)})
                    VALUES ({placeholders})
                """)
                
                with target_conn.begin():
                    target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
                
                rows_copied = len(rows)
                logger.info(f"[COPY] Direct copy completed: {rows_copied:,} rows")
                return True
                
        except Exception as e:
            logger.error(f"[COPY] Error in direct copy: {str(e)}")
            return False

    def _copy_chunked(self, table_name: str, row_count: int) -> bool:
        """Copy table in chunks using primary key ranges."""
        try:
            with self.source_engine.connect() as source_conn, \
                 self.target_engine.connect() as target_conn:
                
                # Get primary key columns
                pk_columns = self._get_primary_key_columns(table_name)
                
                if pk_columns:
                    return self._copy_with_pk_chunking(source_conn, target_conn, table_name)
                else:
                    return self._copy_with_limit_offset(source_conn, target_conn, table_name)
                    
        except Exception as e:
            logger.error(f"[COPY] Error in chunked copy: {str(e)}")
            return False

    def _copy_with_pk_chunking(self, source_conn, target_conn, table_name: str) -> bool:
        """Copy table using primary key ranges for efficient chunking."""
        try:
            # Get primary key range
            pk_columns = self._get_primary_key_columns(table_name)
            if not pk_columns:
                logger.error(f"[COPY] No primary key found for {table_name}")
                return False
                
            pk_list = ', '.join(f'`{col}`' for col in pk_columns)
            
            # Get min and max PK values
            range_query = text(f"""
                SELECT 
                    MIN({pk_list}) as min_pk,
                    MAX({pk_list}) as max_pk
                FROM `{table_name}`
            """)
            
            result = source_conn.execute(range_query)
            row = result.fetchone()
            
            if not row:
                logger.info(f"[COPY] No data to copy from {table_name}")
                return True
                
            min_pk = row.min_pk
            max_pk = row.max_pk
            
            logger.info(f"[COPY] Primary key range: {min_pk} to {max_pk}")
            
            # Copy in chunks
            total_copied = 0
            chunk_size = self.max_batch_size
            
            while min_pk <= max_pk:
                # Get chunk of data
                chunk_query = text(f"""
                    SELECT * FROM `{table_name}`
                    WHERE {pk_list} >= :min_pk
                    ORDER BY {pk_list}
                    LIMIT :chunk_size
                """)
                
                result = source_conn.execute(chunk_query, {
                    'min_pk': min_pk,
                    'chunk_size': chunk_size
                })
                
                rows = result.fetchall()
                if not rows:
                    break
                    
                # Insert chunk
                columns = result.keys()
                placeholders = ', '.join([':' + col for col in columns])
                insert_query = text(f"""
                    INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in columns)})
                    VALUES ({placeholders})
                """)
                
                with target_conn.begin():
                    target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
                
                chunk_rows = len(rows)
                total_copied += chunk_rows
                
                # Update min_pk for next chunk
                last_row = rows[-1]
                min_pk = getattr(last_row, pk_columns[0]) + 1
                
                logger.info(f"[COPY] Chunk: copied {total_copied:,} rows")
            
            logger.info(f"[COPY] Completed chunked copy: {total_copied:,} total rows")
            return True
            
        except Exception as e:
            logger.error(f"[COPY] Error in chunked copy: {str(e)}")
            return False

    def _copy_with_limit_offset(self, source_conn, target_conn, table_name: str) -> bool:
        """Copy table using LIMIT/OFFSET for tables without primary key."""
        try:
            total_copied = 0
            offset = 0
            chunk_count = 0
            
            logger.info(f"[COPY] Using LIMIT/OFFSET copy for {table_name}")
            
            while True:
                chunk_count += 1
                
                # Copy chunk
                chunk_query = text(f"""
                    SELECT * FROM `{table_name}`
                    LIMIT :batch_size OFFSET :offset
                """)
                
                result = source_conn.execute(chunk_query, {
                    'batch_size': self.max_batch_size,
                    'offset': offset
                })
                
                rows = result.fetchall()
                if not rows:
                    break
                
                # Insert chunk
                columns = result.keys()
                placeholders = ', '.join([':' + col for col in columns])
                insert_query = text(f"""
                    INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in columns)})
                    VALUES ({placeholders})
                """)
                
                with target_conn.begin():
                    target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
                
                chunk_rows = len(rows)
                total_copied += chunk_rows
                offset += chunk_rows
                
                logger.info(f"[COPY] Chunk {chunk_count}: copied {total_copied:,} rows")
                
                if chunk_rows < self.max_batch_size:
                    break
            
            logger.info(f"[COPY] Completed LIMIT/OFFSET copy: {total_copied:,} total rows")
            return True
            
        except Exception as e:
            logger.error(f"[COPY] Error in LIMIT/OFFSET copy: {str(e)}")
            return False

    def _verify_data_integrity(self, source_conn, target_conn, table_name: str, pk_columns: List[str]) -> bool:
        """Verify data integrity using primary key checksums."""
        try:
            # Get checksums for each chunk
            pk_list = ', '.join(pk_columns)
            checksum_query = text(f"""
                SELECT 
                    MD5(GROUP_CONCAT({pk_list} ORDER BY {pk_list})) as chunk_hash,
                    COUNT(*) as row_count
                FROM {table_name}
                GROUP BY FLOOR(ROW_NUMBER() OVER (ORDER BY {pk_list}) / 1000)
            """)
            
            source_result = source_conn.execute(checksum_query)
            target_result = target_conn.execute(checksum_query)
            
            source_checksums = {row.chunk_hash: row.row_count for row in source_result}
            target_checksums = {row.chunk_hash: row.row_count for row in target_result}
            
            if source_checksums != target_checksums:
                logger.error(f"[VERIFY] Data integrity check failed for {table_name}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"[VERIFY] Error in data integrity check: {str(e)}")
            return False

    def _get_primary_key_columns(self, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        try:
            return self.inspector.get_pk_constraint(table_name)['constrained_columns']
        except Exception:
            return []

    def _get_row_count(self, table_name: str, engine) -> int:
        """Get the row count for a table."""
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
                return result if result is not None else 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {str(e)}")
            return 0

    def get_exact_table_schema(self, table_name: str, engine) -> Optional[Dict[str, Any]]:
        """Get the exact schema of a table including its CREATE statement."""
        try:
            with engine.connect() as conn:
                # Get CREATE TABLE statement
                result = conn.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
                row = result.fetchone()
                
                if not row:
                    logger.error(f"Could not get CREATE TABLE statement for {table_name}")
                    return None
                
                # SHOW CREATE TABLE returns two columns: Table and Create Table
                create_stmt = row[1]  # Get the Create Table column
                
                # Normalize the CREATE statement
                normalized = self._normalize_create_statement(create_stmt)
                
                # Calculate hash of normalized schema
                schema_hash = hashlib.md5(normalized.encode()).hexdigest()
                
                return {
                    'table_name': table_name,
                    'create_statement': create_stmt,
                    'normalized_schema': normalized,
                    'schema_hash': schema_hash
                }
                
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            return None

    def _get_table_metadata(self, conn, table_name: str) -> Dict:
        """Get additional table metadata."""
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

    def _normalize_create_statement(self, create_statement: str) -> str:
        """Normalize CREATE TABLE statement by removing MySQL-specific differences."""
        # Remove display widths from integer types
        normalized = re.sub(r'(tinyint|smallint|int|bigint)\(\d+\)', r'\1', create_statement)
        
        # Remove engine specification
        normalized = re.sub(r'ENGINE=\w+', '', normalized)
        
        # Remove charset and collation
        normalized = re.sub(r'DEFAULT CHARSET=\w+', '', normalized)
        normalized = re.sub(r'COLLATE \w+', '', normalized)
        normalized = re.sub(r'COLLATE=\w+', '', normalized)
        
        # Remove auto increment
        normalized = re.sub(r'AUTO_INCREMENT=\d+', '', normalized)
        
        # Remove other non-essential parts
        normalized = re.sub(r'ROW_FORMAT=\w+', '', normalized)
        normalized = re.sub(r'KEY_BLOCK_SIZE=\d+', '', normalized)
        normalized = re.sub(r'COMMENT=\'[^\']*\'', '', normalized)
        
        # Normalize whitespace and quotes
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        normalized = re.sub(r'`', '', normalized)
        
        # Sort column definitions to ensure consistent order
        if 'CREATE TABLE' in normalized:
            table_parts = normalized.split('(', 1)
            if len(table_parts) > 1:
                table_name = table_parts[0]
                columns = table_parts[1].rstrip(')').split(',')
                # Sort columns but keep PRIMARY KEY at the end
                pk_cols = [col for col in columns if 'PRIMARY KEY' in col]
                other_cols = [col for col in columns if 'PRIMARY KEY' not in col]
                sorted_cols = sorted(other_cols) + pk_cols
                normalized = f"{table_name}({','.join(sorted_cols)})"
        
        # Remove any trailing collation or charset
        normalized = re.sub(r'COLLATE=\w+$', '', normalized)
        normalized = re.sub(r'CHARSET=\w+$', '', normalized)
        normalized = re.sub(r'COLLATE \w+$', '', normalized)
        normalized = re.sub(r'CHARSET \w+$', '', normalized)
        
        # Remove any trailing parentheses that might be left after removing collation
        normalized = re.sub(r'\)$', '', normalized)
        
        return normalized.strip()

    def _calculate_schema_hash(self, create_statement: str) -> str:
        """Calculate a hash of the schema for change detection."""
        normalized = self._normalize_create_statement(create_statement)
        return hashlib.md5(normalized.encode()).hexdigest()

    def _adapt_create_statement_for_target(self, create_statement: str, table_name: str) -> str:
        """Adapt the CREATE statement for the target database."""
        # Remove database name if present
        pattern = r'CREATE TABLE `?[^`\s]*`?\.`?' + re.escape(table_name) + r'`?'
        replacement = f'CREATE TABLE `{table_name}`'
        adapted = re.sub(pattern, replacement, create_statement, flags=re.IGNORECASE)
        
        # Ensure table name is properly quoted
        pattern2 = r'CREATE TABLE `?' + re.escape(table_name) + r'`?'
        replacement2 = f'CREATE TABLE `{table_name}`'
        adapted = re.sub(pattern2, replacement2, adapted, flags=re.IGNORECASE)
        
        # Remove display widths from integer types
        adapted = re.sub(r'(tinyint|smallint|int|bigint)\(\d+\)', r'\1', adapted)
        
        # Standardize engine and charset
        adapted = re.sub(r'ENGINE=\w+', 'ENGINE=InnoDB', adapted)
        adapted = re.sub(r'DEFAULT CHARSET=\w+', 'DEFAULT CHARSET=utf8mb4', adapted)
        adapted = re.sub(r'COLLATE \w+', 'COLLATE utf8mb4_0900_ai_ci', adapted)
        
        return adapted 