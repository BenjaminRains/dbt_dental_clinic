import os
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
import logging
import argparse
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError, OperationalError
from pandas.errors import EmptyDataError
from connection_factory import get_source_connection, get_target_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_incremental.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class TableConfig:
    """Configuration for table-specific ETL settings."""
    
    def __init__(self, 
                 table_name: str,
                 created_at_column: str,
                 updated_at_column: str,
                 batch_size: int = 10000,
                 max_retries: int = 3,
                 retry_delay: int = 300):
        self.table_name = table_name
        self.created_at_column = created_at_column
        self.updated_at_column = updated_at_column
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay

def verify_table_access(table_name: str, conn) -> bool:
    """Verify read-only access and connection for a table."""
    try:
        # Test read-only access with a simple SELECT
        query = text(f"SELECT 1 FROM {table_name} LIMIT 1")
        conn.execute(query)
        
        # Get connection user
        user_query = text("SELECT CURRENT_USER()")
        user = conn.execute(user_query).scalar()
        
        logger.info(f"Read-only access verified successfully")
        logger.info(f"Connected as user: {user}")
        return True
    except Exception as e:
        logger.error(f"Failed to verify access for table {table_name}: {str(e)}")
        return False

def get_metadata_columns(table_name: str, conn) -> Tuple[Optional[str], Optional[str]]:
    """Get the appropriate created_at and updated_at columns for a table."""
    try:
        # Common patterns for created_at columns
        created_at_patterns = [
            'DateEntry', 'SecDateTEntry', 'CreatedDate', 'InsertDate'
        ]
        
        # Common patterns for updated_at columns
        updated_at_patterns = [
            'DateTStamp', 'SecDateTEdit', 'ModifiedDate'
        ]
        
        # Get all datetime columns for the table
        query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'opendental' 
            AND table_name = :table_name 
            AND (data_type = 'datetime' OR data_type = 'timestamp')
        """)
        result = conn.execute(query.bindparams(table_name=table_name))
        datetime_columns = [row[0] for row in result]
        
        # Find created_at column
        created_at = None
        for pattern in created_at_patterns:
            if pattern in datetime_columns:
                created_at = pattern
                break
        
        # Find updated_at column
        updated_at = None
        for pattern in updated_at_patterns:
            if pattern in datetime_columns:
                updated_at = pattern
                break
        
        # If no specific columns found, use the most recent datetime column for updated_at
        if not updated_at and datetime_columns:
            updated_at = datetime_columns[0]
        
        return created_at, updated_at
    except Exception as e:
        logger.error(f"Error getting metadata columns for {table_name}: {str(e)}")
        return None, None

def get_all_tables() -> Dict[str, TableConfig]:
    """Fetch all tables from MySQL and create TableConfig objects."""
    try:
        with get_source_connection().connect() as conn:
            # Get all tables from the database
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'opendental'
            """)
            result = conn.execute(query)
            tables = [row[0] for row in result]
            
            # Create TableConfig for each table
            table_configs = {}
            for table in tables:
                try:
                    # Phase 1: Initial discovery
                    created_at, updated_at = get_metadata_columns(table, conn)
                    
                    if created_at or updated_at:
                        table_configs[table] = TableConfig(
                            table_name=table,
                            created_at_column=created_at,
                            updated_at_column=updated_at
                        )
                        logger.info(f"Added table {table} with created_at={created_at}, updated_at={updated_at}")
                        
                        # Phase 2: Verification
                        logger.info(f"\nTable: {table}")
                        logger.info(f"Created at column: {created_at}")
                        logger.info(f"Updated at column: {updated_at}")
                        
                        if verify_table_access(table, conn):
                            logger.info(f"Verified metadata columns - Created: {created_at}, Updated: {updated_at}")
                        else:
                            logger.warning(f"Skipping table {table} due to access verification failure")
                            del table_configs[table]
                            
                except Exception as e:
                    logger.error(f"Error configuring table {table}: {str(e)}")
                    continue
            
            logger.info(f"Found {len(table_configs)} tables in the database")
            return table_configs
    except Exception as e:
        logger.error(f"Error fetching tables: {str(e)}")
        return {}

def get_changed_records(table_config: TableConfig, last_sync: Optional[datetime]) -> pd.DataFrame:
    """Get records that have changed since last sync."""
    try:
        # Build the WHERE clause based on available metadata columns
        where_clauses = []
        if table_config.updated_at_column:
            where_clauses.append(f"{table_config.updated_at_column} > :last_sync")
        if table_config.created_at_column:
            where_clauses.append(f"{table_config.created_at_column} > :last_sync")
        
        where_clause = " OR ".join(where_clauses)
        
        query = f"""
            SELECT 
                *,
                CURRENT_TIMESTAMP as _loaded_at,
                {table_config.created_at_column or 'NULL'} as _created_at,
                {table_config.updated_at_column or 'NULL'} as _updated_at
            FROM {table_config.table_name}
            WHERE {where_clause}
            ORDER BY {table_config.updated_at_column or table_config.created_at_column}
        """
        
        with get_source_connection().connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={'last_sync': last_sync or '1970-01-01 00:00:00'}
            )
            return df
    except Exception as e:
        logger.error(f"Error getting changed records for {table_config.table_name}: {str(e)}")
        raise

# Initialize TABLE_CONFIGS with all tables from the database
TABLE_CONFIGS = get_all_tables()

def get_last_sync_time(table_name: str) -> Optional[datetime]:
    """Get the last successful sync time for a table."""
    try:
        with get_target_connection().connect() as conn:
            query = text("""
                SELECT last_modified 
                FROM etl_sync_status 
                WHERE table_name = :table_name
            """)
            result = conn.execute(query.bindparams(table_name=table_name))
            last_sync = result.scalar()
            return last_sync
    except Exception as e:
        logger.error(f"Error getting last sync time for {table_name}: {str(e)}")
        return None

def update_sync_time(table_name: str, sync_time: datetime, rows_processed: int) -> bool:
    """Update the last sync time for a table."""
    try:
        with get_target_connection().connect() as conn:
            query = text("""
                INSERT INTO etl_sync_status (table_name, last_modified, sync_status, rows_processed)
                VALUES (:table_name, :sync_time, 'success', :rows_processed)
                ON CONFLICT (table_name) 
                DO UPDATE SET 
                    last_modified = :sync_time,
                    sync_status = 'success',
                    rows_processed = :rows_processed
            """)
            conn.execute(query.bindparams(
                table_name=table_name,
                sync_time=sync_time,
                rows_processed=rows_processed
            ))
            return True
    except Exception as e:
        logger.error(f"Error updating sync time for {table_name}: {str(e)}")
        return False

def sync_table_incremental(table_name: str) -> bool:
    """Perform incremental sync for a specific table."""
    if table_name not in TABLE_CONFIGS:
        logger.error(f"No configuration found for table {table_name}")
        return False
        
    config = TABLE_CONFIGS[table_name]
    last_sync = get_last_sync_time(table_name)
    
    try:
        # Get changed records
        df = get_changed_records(config, last_sync)
        
        if df.empty:
            logger.info(f"No changes found for {table_name}")
            return True
            
        # Process in batches
        total_rows = len(df)
        processed_rows = 0
        
        for i in range(0, total_rows, config.batch_size):
            batch = df.iloc[i:i + config.batch_size]
            
            # Load to PostgreSQL
            with get_target_connection().connect() as conn:
                batch.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            processed_rows += len(batch)
            logger.info(f"Processed {processed_rows}/{total_rows} rows for {table_name}")
        
        # Update sync time
        current_time = datetime.now()
        update_sync_time(table_name, current_time, processed_rows)
        
        return True
        
    except Exception as e:
        logger.error(f"Error syncing {table_name}: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Incremental ETL from MySQL to PostgreSQL')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync')
    args = parser.parse_args()
    
    tables_to_sync = args.tables if args.tables else TABLE_CONFIGS.keys()
    
    for table in tables_to_sync:
        logger.info(f"Starting incremental sync for {table}")
        if sync_table_incremental(table):
            logger.info(f"Successfully synced {table}")
        else:
            logger.error(f"Failed to sync {table}")

if __name__ == "__main__":
    main() 