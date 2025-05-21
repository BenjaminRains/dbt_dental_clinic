import os
import pandas as pd
from sqlalchemy import create_engine, text, quoted_name
from sqlalchemy.engine.url import URL
import pymysql
from dotenv import load_dotenv
import logging
import argparse
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError, OperationalError
from pandas.errors import EmptyDataError
from sqlalchemy.engine import Connection

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

# Database connections
mariadb_engine = create_engine(
    f"mysql+pymysql://{os.getenv('MARIADB_ROOT_USER')}:{os.getenv('MARIADB_ROOT_PASSWORD')}@"
    f"{os.getenv('MARIADB_ROOT_HOST')}:{os.getenv('MARIADB_ROOT_PORT')}/"
    f"{os.getenv('DBT_MYSQL_DATABASE')}"
)

pg_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DATABASE')}"
)

class TableConfig:
    """Configuration for table-specific ETL settings."""
    
    def __init__(self, 
                 table_name: str,
                 change_column: str,
                 batch_size: int = 10000,
                 max_retries: int = 3,
                 retry_delay: int = 300):
        self.table_name = table_name
        self.change_column = change_column
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay

# Table configurations
TABLE_CONFIGS = {
    'appointment': TableConfig('appointment', 'AptDateTime'),
    'procedurelog': TableConfig('procedurelog', 'ProcDate'),
    'payment': TableConfig('payment', 'PayDate'),
    'claim': TableConfig('claim', 'DateSent'),
    'commlog': TableConfig('commlog', 'CommDateTime'),
    'entrylog': TableConfig('entrylog', 'EntryDateTime'),
    'patient': TableConfig('patient', 'DateFirstVisit'),
    'insplan': TableConfig('insplan', 'DateEffective'),
    'provider': TableConfig('provider', 'DateTerm'),
    'feesched': TableConfig('feesched', 'DateStart'),
    'definition': TableConfig('definition', 'DateTStamp'),
    'preference': TableConfig('preference', 'DateTStamp'),
    'securitylog': TableConfig('securitylog', 'LogDateTime')
}

def get_last_sync_time(table_name: str) -> Optional[datetime]:
    """Get the last successful sync time for a table."""
    try:
        with pg_engine.connect() as conn:
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
        with pg_engine.connect() as conn:
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

def get_changed_records(table_config: TableConfig, last_sync: Optional[datetime]) -> pd.DataFrame:
    """Get records that have changed since last sync."""
    try:
        query = f"""
            SELECT * FROM {table_config.table_name}
            WHERE {table_config.change_column} > :last_sync
            ORDER BY {table_config.change_column}
        """
        
        with mariadb_engine.connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={'last_sync': last_sync or '1970-01-01 00:00:00'}
            )
            return df
    except Exception as e:
        logger.error(f"Error getting changed records for {table_config.table_name}: {str(e)}")
        raise

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
            
            # Convert data types
            processed_batch, warnings = convert_data_types(batch, table_name)
            
            # Load to PostgreSQL
            with pg_engine.connect() as conn:
                processed_batch.to_sql(
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
    parser = argparse.ArgumentParser(description='Incremental ETL from MariaDB to PostgreSQL')
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