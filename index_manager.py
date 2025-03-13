"""
Index Manager for dbt validation process

A comprehensive tool for managing database indexes:
- Create new indexes
- Drop existing indexes
- Update indexes
- Monitor index usage

Usage:
    python index_manager.py database_name [--action {create,drop,show,show-custom,restore-system}] [--table table_name]
"""

import logging
import sys
import argparse
from typing import List, Optional
from scripts.index_configs import TREATMENT_JOURNEY_INDEXES, SYSTEM_INDEXES
from src.db_config import VALID_DATABASES
from src.connections.factory import ConnectionFactory
import re

# Define constants for base queries
BASE_SHOW_INDEXES_QUERY = """
SELECT 
    TABLE_NAME,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS COLUMNS
FROM INFORMATION_SCHEMA.STATISTICS 
WHERE TABLE_SCHEMA = DATABASE()
"""
GROUP_BY_ORDER_QUERY = " GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME, INDEX_NAME"

def sanitize_table_name(table_name: str) -> str:
    """
    Very basic sanitization: wrap the table name in backticks.
    (Assumes the table name does not contain backticks already.)
    """
    return f"`{table_name}`" if not table_name.startswith("`") else table_name

class IndexManager:
    """Manages database indexes for the ML pipeline."""
    
    def __init__(self, database_name: str):
        if database_name not in VALID_DATABASES:
            raise ValueError(f"Invalid database name. Must be one of: {', '.join(VALID_DATABASES)}")
        self.database_name = database_name
        self.logger = self._setup_logging()
        self.connection = self._create_connection()
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for index management."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(self.__class__.__name__)
    
    def _create_connection(self):
        """Create a MariaDB connection using the factory (requires root privileges)."""
        try:
            conn = ConnectionFactory.create_connection(
                connection_type='local_mariadb',
                database=self.database_name,
                use_root=True  # Index management requires root privileges
            )
            return conn.connect()
        except Exception as err:
            self.logger.error(f"Failed to connect to database: {err}")
            raise
    
    def show_indexes(self, table_name: Optional[str] = None) -> None:
        """Display all indexes for the current database."""
        query = BASE_SHOW_INDEXES_QUERY
        if table_name:
            sanitized = sanitize_table_name(table_name)
            query += f" AND TABLE_NAME = {sanitized}"
        query += GROUP_BY_ORDER_QUERY
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                indexes = cursor.fetchall()
                if not indexes:
                    self.logger.info("No indexes found.")
                else:
                    for table, index, columns in indexes:
                        self.logger.info(f"Table: {table}, Index: {index}, Columns: {columns}")
        except Exception as err:
            self.logger.error(f"Error showing indexes: {err}")
    
    def show_custom_indexes(self, table_name: Optional[str] = None) -> None:
        """Display only custom indexes (those with a lowercase idx_ prefix)."""
        query = BASE_SHOW_INDEXES_QUERY + " AND BINARY INDEX_NAME LIKE 'idx_%'"
        if table_name:
            sanitized = sanitize_table_name(table_name)
            query += f" AND TABLE_NAME = {sanitized}"
        query += GROUP_BY_ORDER_QUERY
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                indexes = cursor.fetchall()
                if not indexes:
                    self.logger.info("No custom indexes found.")
                else:
                    self.logger.info("Custom indexes (idx_ prefix):")
                    for table, index, columns in indexes:
                        self.logger.info(f"  - Table: {table}, Index: {index}, Columns: {columns}")
        except Exception as err:
            self.logger.error(f"Error showing custom indexes: {err}")
    
    def drop_indexes(self, pattern: str = 'idx_%') -> None:
        """
        Drop indexes matching the given pattern.
        (By default, drops indexes with names starting with 'idx_' with case-sensitive matching.)
        """
        try:
            with self.connection.cursor() as cursor:
                # Use parameterized query for pattern
                query = """
                SELECT 
                    TABLE_NAME,
                    INDEX_NAME 
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = DATABASE()
                AND BINARY INDEX_NAME LIKE %s
                GROUP BY TABLE_NAME, INDEX_NAME
                """
                cursor.execute(query, (pattern,))
                existing_indexes = cursor.fetchall()
                
                for table_name, index_name in existing_indexes:
                    try:
                        self.logger.info(f"Dropping index {index_name} from {table_name}")
                        drop_query = f"DROP INDEX {index_name} ON {table_name}"
                        cursor.execute(drop_query)
                    except Exception as err:
                        self.logger.error(f"Failed to drop index {index_name} on {table_name}: {err}")
                self.connection.commit()
        except Exception as err:
            self.logger.error(f"Error dropping indexes: {err}")
    
    def create_indexes(self, indexes: List[str]) -> None:
        """Create new indexes using the provided list of index creation SQL statements."""
        try:
            with self.connection.cursor() as cursor:
                for index_sql in indexes:
                    try:
                        self.logger.info(f"Creating index: {index_sql}")
                        cursor.execute(index_sql)
                    except Exception as err:
                        # Duplicate key error in MariaDB is error code 1061
                        if hasattr(err, 'errno') and err.errno == 1061:
                            self.logger.info(f"Index already exists: {index_sql}")
                        else:
                            self.logger.error(f"Failed to create index: {err}")
                self.connection.commit()
        except Exception as err:
            self.logger.error(f"Error creating indexes: {err}")
    
    def restore_system_indexes(self) -> None:
        """Restore system indexes that were accidentally dropped."""
        self.logger.info("Restoring system indexes...")
        self.create_indexes(SYSTEM_INDEXES)
        self.logger.info("System indexes restored.")

def main():
    parser = argparse.ArgumentParser(description='Manage database indexes for the ML pipeline')
    parser.add_argument('database_name', help='Name of the database. Valid options: ' + ", ".join(VALID_DATABASES))
    parser.add_argument('--action', 
                        choices=['create', 'drop', 'show', 'show-custom', 'restore-system'],
                        default='show', 
                        help='Action to perform')
    parser.add_argument('--table', help='Specific table to show indexes for')
    
    args = parser.parse_args()
    
    try:
        manager = IndexManager(args.database_name)
        if args.action == 'show':
            manager.show_indexes(args.table)
        elif args.action == 'show-custom':
            manager.show_custom_indexes(args.table)
        elif args.action == 'drop':
            manager.drop_indexes()
        elif args.action == 'create':
            manager.create_indexes(TREATMENT_JOURNEY_INDEXES)
        elif args.action == 'restore-system':
            manager.restore_system_indexes()
    except Exception as e:
        logging.error(f"Error during index management: {e}")
        sys.exit(1)
    finally:
        if 'manager' in locals() and hasattr(manager, 'connection'):
            try:
                manager.connection.close()
            except Exception as e:
                logging.error(f"Error closing connection: {e}")

if __name__ == "__main__":
    main()
