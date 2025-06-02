"""
Incremental extraction module for managing watermarks and change detection.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class IncrementalExtractor:
    """Handles incremental extraction with watermark management."""
    
    def __init__(self, source_engine: Engine, target_engine: Engine):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self._watermark_cache = {}
    
    def get_watermark(self, table_name: str) -> Optional[datetime]:
        """
        Get the current watermark for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Optional[datetime]: Current watermark timestamp, or None if not found
        """
        try:
            if table_name in self._watermark_cache:
                return self._watermark_cache[table_name]
            
            query = """
            SELECT watermark_timestamp
            FROM etl_watermarks
            WHERE table_name = :table_name
            """
            
            with self.target_engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                row = result.fetchone()
                
                watermark = row[0] if row else None
                self._watermark_cache[table_name] = watermark
                
                return watermark
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting watermark for table {table_name}: {str(e)}")
            return None
    
    def update_watermark(self, table_name: str, timestamp: datetime) -> bool:
        """
        Update the watermark for a table.
        
        Args:
            table_name: Name of the table
            timestamp: New watermark timestamp
            
        Returns:
            bool: True if update was successful
        """
        try:
            query = """
            INSERT INTO etl_watermarks (table_name, watermark_timestamp)
            VALUES (:table_name, :timestamp)
            ON CONFLICT (table_name) 
            DO UPDATE SET 
                watermark_timestamp = :timestamp,
                updated_at = CURRENT_TIMESTAMP
            """
            
            with self.target_engine.connect() as conn:
                conn.execute(text(query), {
                    "table_name": table_name,
                    "timestamp": timestamp
                })
                
            self._watermark_cache[table_name] = timestamp
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error updating watermark for table {table_name}: {str(e)}")
            return False
    
    def detect_changes(self, table_name: str, watermark_column: str) -> Dict[str, Any]:
        """
        Detect changes in a table since the last watermark.
        
        Args:
            table_name: Name of the table
            watermark_column: Column to use for change detection
            
        Returns:
            Dict[str, Any]: Change detection results
        """
        try:
            watermark = self.get_watermark(table_name)
            
            # Get change statistics
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN {watermark_column} > :watermark THEN 1 END) as changed_rows,
                MIN({watermark_column}) as min_timestamp,
                MAX({watermark_column}) as max_timestamp
            FROM {table_name}
            WHERE {watermark_column} > :watermark
            """
            
            with self.source_engine.connect() as conn:
                result = conn.execute(text(query), {
                    "watermark": watermark or datetime.min
                })
                stats = dict(result.fetchone())
            
            return {
                "has_changes": stats["changed_rows"] > 0,
                "total_rows": stats["total_rows"],
                "changed_rows": stats["changed_rows"],
                "min_timestamp": stats["min_timestamp"],
                "max_timestamp": stats["max_timestamp"]
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Error detecting changes for table {table_name}: {str(e)}")
            return {
                "has_changes": False,
                "error": str(e)
            }
    
    def get_changed_records(self, table_name: str, watermark_column: str, 
                          batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Get records that have changed since the last watermark.
        
        Args:
            table_name: Name of the table
            watermark_column: Column to use for change detection
            batch_size: Number of records to fetch per batch
            
        Returns:
            List[Dict[str, Any]]: List of changed records
        """
        try:
            watermark = self.get_watermark(table_name)
            
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE {watermark_column} > :watermark
            ORDER BY {watermark_column}
            LIMIT :batch_size
            """
            
            with self.source_engine.connect() as conn:
                result = conn.execute(text(query), {
                    "watermark": watermark or datetime.min,
                    "batch_size": batch_size
                })
                return [dict(row) for row in result.fetchall()]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting changed records for table {table_name}: {str(e)}")
            return []
    
    def ensure_watermark_table(self) -> bool:
        """
        Ensure the watermark tracking table exists.
        
        Returns:
            bool: True if table exists or was created successfully
        """
        try:
            query = """
            CREATE TABLE IF NOT EXISTS etl_watermarks (
                table_name VARCHAR(100) PRIMARY KEY,
                watermark_timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            with self.target_engine.connect() as conn:
                conn.execute(text(query))
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error creating watermark table: {str(e)}")
            return False 