import os
import re
import hashlib
import json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

class ExactMySQLReplicator:
    """
    Creates exact MySQL table replicas by using SHOW CREATE TABLE
    and preserving all MySQL-specific features.
    """
    
    def __init__(self, source_engine, target_engine, source_db: str, target_db: str):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.source_db = source_db
        self.target_db = target_db
        
        # Cache for schema information
        self._schema_cache = {}
        
    def get_exact_table_schema(self, table_name: str) -> Dict:
        """
        Get the exact table schema using SHOW CREATE TABLE.
        This preserves all MySQL-specific syntax and features.
        """
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text(f"USE {self.source_db}"))
                
                # Get the exact CREATE TABLE statement
                result = conn.execute(text(f"SHOW CREATE TABLE {table_name}"))
                row = result.fetchone()
                
                if not row:
                    raise ValueError(f"Table {table_name} not found in {self.source_db}")
                
                create_statement = row[1]  # Second column contains the CREATE TABLE statement
                
                # Get additional metadata
                table_info = self._get_table_metadata(conn, table_name)
                
                # Get incremental extraction column candidates
                incremental_columns = self._find_incremental_columns(conn, table_name)
                
                schema_info = {
                    'table_name': table_name,
                    'create_statement': create_statement,
                    'engine': table_info['engine'],
                    'charset': table_info['charset'],
                    'collation': table_info['collation'],
                    'auto_increment': table_info['auto_increment'],
                    'row_count': table_info['row_count'],
                    'incremental_columns': incremental_columns,
                    'schema_hash': self._calculate_schema_hash(create_statement)
                }
                
                # Cache the schema info
                self._schema_cache[table_name] = schema_info
                
                logger.info(f"Retrieved exact schema for {table_name}: "
                          f"Engine={table_info['engine']}, Charset={table_info['charset']}, "
                          f"Rows={table_info['row_count']}")
                
                return schema_info
                
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            raise
    
    def _get_table_metadata(self, conn, table_name: str) -> Dict:
        """Get additional table metadata."""
        # Get table status information
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
    
    def _find_incremental_columns(self, conn, table_name: str) -> List[Dict]:
        """Find suitable columns for incremental extraction."""
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
        
        return incremental_columns
    
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
    
    def _calculate_schema_hash(self, create_statement: str) -> str:
        """Calculate a hash of the schema for change detection."""
        # Normalize the CREATE statement by removing variable parts
        normalized = re.sub(r'AUTO_INCREMENT=\d+', 'AUTO_INCREMENT=1', create_statement)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def create_exact_target_table(self, table_name: str, drop_if_exists: bool = True) -> bool:
        """
        Create an exact replica of the source table in the target database.
        """
        try:
            schema_info = self.get_exact_table_schema(table_name)
            create_statement = schema_info['create_statement']
            
            # Modify the CREATE statement for the target database
            target_create_statement = self._adapt_create_statement_for_target(
                create_statement, table_name
            )
            
            with self.target_engine.begin() as conn:
                conn.execute(text(f"USE {self.target_db}"))
                
                # Check if table already exists
                check_table_result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                table_exists = check_table_result.fetchone() is not None
                
                if table_exists and not drop_if_exists:
                    logger.info(f"Table {table_name} already exists in target, skipping creation for incremental extraction")
                    return True
                elif drop_if_exists:
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
            logger.error(f"Error creating exact target table {table_name}: {str(e)}")
            return False
    
    def _adapt_create_statement_for_target(self, create_statement: str, table_name: str) -> str:
        """
        Adapt the CREATE statement for the target database.
        This ensures compatibility while preserving the exact structure.
        """
        # Replace the table name to ensure it's unqualified
        # This handles cases where the source CREATE statement might include database name
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
    
    def get_best_incremental_column(self, table_name: str) -> Optional[str]:
        """Get the best column for incremental extraction."""
        if table_name not in self._schema_cache:
            self.get_exact_table_schema(table_name)
        
        incremental_columns = self._schema_cache[table_name]['incremental_columns']
        
        if not incremental_columns:
            return None
        
        # Return the column with the highest priority (lowest priority number)
        best_column = min(incremental_columns, key=lambda x: x['priority'])
        return best_column['column_name']
    
    def extract_table_data(self, table_name: str, last_extracted: Optional[datetime] = None, 
                          force_full: bool = False, batch_size: int = 10000) -> int:
        """
        Extract data from source to target table with chunked processing for large tables.
        """
        try:
            # Ensure target table exists and is exact replica
            if not self.create_exact_target_table(table_name):
                return 0
            
            # Determine extraction strategy
            incremental_column = self.get_best_incremental_column(table_name)
            
            if force_full or not last_extracted or not incremental_column:
                return self._extract_full_table(table_name, batch_size)
            else:
                return self._extract_incremental(table_name, incremental_column, last_extracted, batch_size)
                
        except Exception as e:
            logger.error(f"Error extracting data for {table_name}: {str(e)}")
            return 0
    
    def extract_incremental_data(self, table_name: str, incremental_column: str, 
                               last_extracted: Optional[datetime], batch_size: int = 10000) -> int:
        """
        Extract incremental data using a specific column.
        This method is called by the intelligent pipeline when it has already determined
        the best incremental column to use.
        """
        try:
            # For incremental extraction, only create table if it doesn't exist (don't drop existing data)
            if not self.create_exact_target_table(table_name, drop_if_exists=False):
                return 0
                
            if not last_extracted:
                logger.info(f"No last_extracted timestamp, performing full extraction for {table_name}")
                return self._extract_full_table(table_name, batch_size)
            
            return self._extract_incremental(table_name, incremental_column, last_extracted, batch_size)
                
        except Exception as e:
            logger.error(f"Error in incremental extraction for {table_name}: {str(e)}")
            return 0
    
    def _extract_full_table(self, table_name: str, batch_size: int) -> int:
        """Extract all data from source table."""
        logger.info(f"Performing full extraction for {table_name}")
        
        with self.source_engine.connect() as source_conn, \
             self.target_engine.connect() as target_conn:
            
            source_conn.execute(text(f"USE {self.source_db}"))
            target_conn.execute(text(f"USE {self.target_db}"))
            
            # Get total row count
            count_result = source_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_rows = count_result.scalar()
            
            if total_rows == 0:
                logger.info(f"No data to extract from {table_name}")
                return 0
            
            # Truncate target table
            target_conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            
            # Extract in chunks if table is large
            if total_rows > batch_size:
                return self._extract_in_chunks(source_conn, target_conn, table_name, total_rows, batch_size)
            else:
                # Small table - extract all at once
                return self._extract_direct_copy(source_conn, target_conn, table_name)
    
    def _extract_incremental(self, table_name: str, incremental_column: str, 
                           last_extracted: datetime, batch_size: int) -> int:
        """Extract only new/modified data based on incremental column using secure two-step approach."""
        logger.info(f"Performing incremental extraction for {table_name} using {incremental_column} > {last_extracted}")
        
        # Step 1: Extract data from source (using readonly connection)
        with self.source_engine.connect() as source_conn:
            source_conn.execute(text(f"USE {self.source_db}"))
            
            # Check if there are new records
            count_query = text(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE {incremental_column} > :last_extracted
            """)
            count_result = source_conn.execute(count_query.bindparams(last_extracted=last_extracted))
            new_rows = count_result.scalar()
            
            if new_rows == 0:
                logger.info(f"No new data to extract from {table_name}")
                return 0
            
            # Extract new data from source
            extract_query = text(f"""
                SELECT * FROM {table_name}
                WHERE {incremental_column} > :last_extracted
                ORDER BY {incremental_column}
            """)
            
            result = source_conn.execute(extract_query.bindparams(last_extracted=last_extracted))
            rows = result.fetchall()
            columns = result.keys()
        
        # Step 2: Insert into target (using replication connection)
        if rows:
            with self.target_engine.connect() as target_conn:
                target_conn.execute(text(f"USE {self.target_db}"))
                
                # Build parameterized INSERT statement with ON DUPLICATE KEY UPDATE for upsert behavior
                placeholders = ', '.join([':' + col for col in columns])
                update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns])
                insert_query = text(f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """)
                
                # Execute bulk insert
                target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
                target_conn.commit()  # CRITICAL: Commit the transaction
                rows_inserted = len(rows)
                
                logger.info(f"Extracted {rows_inserted} new/modified rows from {table_name}")
                return rows_inserted
        
        return 0
    
    def _extract_in_chunks(self, source_conn, target_conn, table_name: str, 
                          total_rows: int, batch_size: int) -> int:
        """Extract large table in chunks using secure two-step approach."""
        logger.info(f"Extracting {total_rows} rows from {table_name} in chunks of {batch_size}")
        
        # Get primary key column for chunking
        pk_query = text("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = :db_name
            AND table_name = :table_name
            AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
            LIMIT 1
        """)
        
        pk_result = source_conn.execute(pk_query.bindparams(
            db_name=self.source_db, 
            table_name=table_name
        ))
        pk_column = pk_result.scalar()
        
        if not pk_column:
            # No primary key - fall back to direct copy
            logger.warning(f"No primary key found for {table_name}, using direct copy")
            return self._extract_direct_copy_secure(table_name)
        
        total_extracted = 0
        offset = 0
        
        while offset < total_rows:
            # Step 1: Extract chunk from source (readonly connection)
            chunk_query = text(f"""
                SELECT * FROM {table_name}
                ORDER BY {pk_column}
                LIMIT {batch_size} OFFSET {offset}
            """)
            
            result = source_conn.execute(chunk_query)
            rows = result.fetchall()
            
            if not rows:
                break  # No more data
            
            # Step 2: Insert chunk into target (replication connection)  
            columns = result.keys()
            placeholders = ', '.join([':' + col for col in columns])
            insert_query = text(f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
            """)
            
            target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
            target_conn.commit()  # CRITICAL: Commit each chunk
            chunk_rows = len(rows)
            total_extracted += chunk_rows
            offset += batch_size
            
            logger.info(f"Extracted chunk: {total_extracted}/{total_rows} rows")
            
            if chunk_rows < batch_size:
                break  # Last chunk
        
        return total_extracted
    
    def _extract_direct_copy(self, source_conn, target_conn, table_name: str) -> int:
        """Direct table copy for smaller tables using secure two-step approach."""
        # Step 1: Extract all data from source (readonly connection)
        extract_query = text(f"SELECT * FROM {table_name}")
        result = source_conn.execute(extract_query)
        rows = result.fetchall()
        
        if not rows:
            logger.info(f"No data to copy from {table_name}")
            return 0
        
        # Step 2: Insert into target (replication connection)
        columns = result.keys()
        placeholders = ', '.join([':' + col for col in columns])
        insert_query = text(f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
        """)
        
        target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
        target_conn.commit()  # CRITICAL: Commit the transaction
        rows_copied = len(rows)
        
        logger.info(f"Direct copy completed: {rows_copied} rows")
        return rows_copied
    
    def _extract_direct_copy_secure(self, table_name: str) -> int:
        """Secure direct table copy using separate connections for source and target."""
        # Step 1: Extract all data from source (readonly connection)
        with self.source_engine.connect() as source_conn:
            source_conn.execute(text(f"USE {self.source_db}"))
            extract_query = text(f"SELECT * FROM {table_name}")
            result = source_conn.execute(extract_query)
            rows = result.fetchall()
            columns = result.keys()
        
        if not rows:
            logger.info(f"No data to copy from {table_name}")
            return 0
        
        # Step 2: Insert into target (replication connection)
        with self.target_engine.connect() as target_conn:
            target_conn.execute(text(f"USE {self.target_db}"))
            placeholders = ', '.join([':' + col for col in columns])
            insert_query = text(f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
            """)
            
            target_conn.execute(insert_query, [dict(row._mapping) for row in rows])
            target_conn.commit()  # CRITICAL: Commit the transaction
            rows_copied = len(rows)
            
            logger.info(f"Secure direct copy completed: {rows_copied} rows")
            return rows_copied
    
    
        try:
            with self.source_engine.connect() as source_conn, \
                 self.target_engine.connect() as target_conn:
                
                source_conn.execute(text(f"USE {self.source_db}"))
                target_conn.execute(text(f"USE {self.target_db}"))
                
                # Get row counts
                source_count = source_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                target_count = target_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                
                # Always log the row counts for tracking purposes
                logger.info(f"Row count tracking - {table_name}: source MySQL ({self.source_db})={source_count:,}, target MySQL ({self.target_db})={target_count:,}")
                
                # For incremental extractions, don't fail on row count mismatches
                if is_incremental:
                    logger.info(f"Incremental extraction verification skipped for {table_name} - row count comparison not applicable for incremental loads")
                    return True
                
                # For full extractions, verify row counts match
                # Exact match - perfect!
                if source_count == target_count:
                    logger.info(f"Full extraction verified: {table_name} has {target_count} rows in both source MySQL ({self.source_db}) and target MySQL ({self.target_db})")
                    return True
                
                # For large tables (>100K rows), allow small discrepancies due to live database changes
                if target_count > 100000:
                    difference = abs(source_count - target_count)
                    tolerance_rows = max(10, int(target_count * 0.001))  # 0.1% or 10 rows, whichever is larger
                    
                    if difference <= tolerance_rows:
                        logger.info(f"Full extraction verified with tolerance: {table_name} has {target_count} rows in target MySQL ({self.target_db}) vs {source_count} in source MySQL ({self.source_db}) - difference: {difference} within tolerance: {tolerance_rows}")
                        return True
                    else:
                        logger.error(f"Full extraction verification failed: {table_name} has {source_count} rows in source MySQL ({self.source_db}) but {target_count} in target MySQL ({self.target_db}) - difference: {difference} exceeds tolerance: {tolerance_rows}")
                        return False
                else:
                    # Small tables must match exactly
                    logger.error(f"Full extraction verification failed: {table_name} has {source_count} rows in source MySQL ({self.source_db}) but {target_count} in target MySQL ({self.target_db})")
                    return False
                    
        except Exception as e:
            logger.error(f"Error verifying extraction for {table_name}: {str(e)}")
            return False
    
    def verify_extraction(self, table_name: str, is_incremental: bool = False) -> bool:
        """Verify that the extraction was successful by comparing row counts with tolerance for live databases."""
    
    def has_schema_changed(self, table_name: str, stored_hash: str) -> bool:
        """Check if the source table schema has changed."""
        try:
            current_schema = self.get_exact_table_schema(table_name)
            current_hash = current_schema['schema_hash']
            return current_hash != stored_hash
        except Exception as e:
            logger.error(f"Error checking schema changes for {table_name}: {str(e)}")
            return True  # Assume changed if we can't determine
    
    def get_schema_hash(self, table_name: str) -> str:
        """Get the current schema hash for a table."""
        schema_info = self.get_exact_table_schema(table_name)
        return schema_info['schema_hash'] 