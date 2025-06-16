
"""
Intelligent ETL pipeline implementation.
Handles the extraction and loading of data from OpenDental to analytics
using data-driven configuration and priority-based processing.
"""

import os
import sys
from pathlib import Path

# Add project root to Python path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent  # Go up two levels from etl_pipeline/elt_pipeline.py
sys.path.insert(0, str(project_root))

# Now continue with your existing imports
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from etl_pipeline.core.connections import ConnectionFactory
from dotenv import load_dotenv
import re
from etl_pipeline.mysql_replicator import ExactMySQLReplicator
from etl_pipeline.core.metrics import MetricsCollector
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Rest of your existing code continues unchanged...

# Create logs directory if it doesn't exist
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)

# Generate unique log filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"elt_pipeline_{timestamp}.log"
log_file_path = os.path.join(logs_dir, log_filename)

# Configure handlers
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
file_handler.flush = lambda: file_handler.stream.flush()  # Force immediate flush
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler],
    force=True  # Override any existing logging configuration
)

print(f"ETL Pipeline logging to: {log_file_path}")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class IntelligentELTPipeline:
    def __init__(self, config_path: str = 'etl_pipeline/config/tables.yaml'):
        """
        Initialize the intelligent ETL pipeline (Phase 1: Configuration Only).
        
        This phase loads configuration and validates environment without creating
        database connections. Call initialize_connections() for database operations.
        """
        # Load environment variables using improved naming with fallbacks
        self.opendental_source_engine = None
        self.mysql_replication_engine = None
        self.postgres_analytics_engine = None
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB') or os.getenv('SOURCE_MYSQL_DB') or "opendental_source"
        self.replication_db = os.getenv('MYSQL_REPLICATION_DB') or os.getenv('REPLICATION_MYSQL_DB') or "opendental_replication"
        self.analytics_db = os.getenv('POSTGRES_ANALYTICS_DB') or os.getenv('ANALYTICS_POSTGRES_DB') or "opendental_analytics"
        self.analytics_schema = os.getenv('POSTGRES_ANALYTICS_SCHEMA') or os.getenv('ANALYTICS_POSTGRES_SCHEMA', 'raw')
        
        # Validate required environment variables
        missing_dbs = []
        if not self.source_db:
            missing_dbs.append('OPENDENTAL_SOURCE_DB')
        if not self.replication_db:
            missing_dbs.append('MYSQL_REPLICATION_DB')
        if not self.analytics_db:
            missing_dbs.append('POSTGRES_ANALYTICS_DB')
        
        if missing_dbs:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_dbs)}")
        
        # Load intelligent configuration
        self.config_path = config_path
        self.table_config = self.load_table_configuration()
        
        # Initialize connection state (Phase 2 will populate these)
        self.mysql_replicator = None
        
        # Initialize metrics
        self.metrics = MetricsCollector()
        
        # Track initialization state
        self.connections_initialized = False
        self.tracking_tables_created = False
        
        logger.info(f"Initialized intelligent ETL pipeline with {len(self.table_config.get('source_tables', {}))} table configurations")
        logger.info("Call initialize_connections() to enable database operations")
    
    def load_table_configuration(self) -> Dict:
        """Load intelligent table configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Loaded configuration for {len(config.get('source_tables', {}))} tables")
            return config
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {str(e)}")
            raise
    
    def get_tables_by_priority(self, importance_level: str) -> List[str]:
        """Get list of tables by importance level."""
        tables = []
        source_tables = self.table_config.get('source_tables', {})
        
        for table_name, config in source_tables.items():
            if config.get('table_importance') == importance_level:
                tables.append(table_name)
        
        logger.info(f"Found {len(tables)} tables with importance level: {importance_level}")
        return tables
    
    def get_critical_tables(self) -> List[str]:
        """Get the top 5 critical tables for Phase 1 implementation."""
        critical_tables = self.get_tables_by_priority('critical')
        
        # Sort by estimated size (descending) to get the largest critical tables first
        source_tables = self.table_config.get('source_tables', {})
        critical_with_size = [
            (table, source_tables[table].get('estimated_size_mb', 0))
            for table in critical_tables
        ]
        critical_with_size.sort(key=lambda x: x[1], reverse=True)
        
        # Return top 5 for Phase 1
        top_5_critical = [table for table, _ in critical_with_size[:5]]
        
        logger.info(f"Phase 1 critical tables: {top_5_critical}")
        return top_5_critical
    
    def get_table_config(self, table_name: str) -> Dict:
        """Get configuration for a specific table."""
        source_tables = self.table_config.get('source_tables', {})
        config = source_tables.get(table_name, {})
        
        if not config:
            logger.warning(f"No configuration found for table: {table_name}")
            # Return default configuration
            return {
                'incremental_column': None,
                'batch_size': 5000,
                'extraction_strategy': 'full_table',
                'table_importance': 'reference',
                'monitoring': {
                    'alert_on_failure': False,
                    'max_extraction_time_minutes': 10,
                    'data_quality_threshold': 0.95
                }
            }
        
        return config
    
    def should_use_incremental(self, table_name: str) -> bool:
        """Check if table supports incremental extraction."""
        config = self.get_table_config(table_name)
        return config.get('extraction_strategy') == 'incremental'
    
    def get_incremental_column(self, table_name: str) -> Optional[str]:
        """Get the timestamp column for incremental extraction."""
        config = self.get_table_config(table_name)
        return config.get('incremental_column')
    
    def get_batch_size(self, table_name: str) -> int:
        """Get intelligent batch size for table."""
        config = self.get_table_config(table_name)
        return config.get('batch_size', 5000)
    
    def get_monitoring_config(self, table_name: str) -> Dict:
        """Get monitoring configuration for table."""
        config = self.get_table_config(table_name)
        return config.get('monitoring', {
            'alert_on_failure': False,
            'max_extraction_time_minutes': 10,
            'data_quality_threshold': 0.95
        })
    
    def validate_database_names(self):
        """Validate that all required database names are set."""
        missing_dbs = []
        
        if not self.source_db:
            missing_dbs.append('OPENDENTAL_SOURCE_DB')
        if not self.replication_db:
            missing_dbs.append('MYSQL_REPLICATION_DB')
        if not self.analytics_db:
            missing_dbs.append('POSTGRES_ANALYTICS_DB')
        
        if missing_dbs:
            error_msg = f"Missing required database names: {', '.join(missing_dbs)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Source database: {self.source_db}")
        logger.info(f"Replication database: {self.replication_db}")
        logger.info(f"Analytics database: {self.analytics_db}")
        logger.info(f"PostgreSQL schema: {self.analytics_schema}")
    
    def initialize_connections(self):
        """
        Initialize database connections and setup tracking tables (Phase 2).
        
        This creates all database connections and ensures tracking tables exist.
        Must be called before any database operations.
        """
        if self.connections_initialized:
            logger.debug("Database connections already initialized")
            return
            
        try:
            logger.info("Initializing database connections...")
            
            # Create database connections
            self.opendental_source_engine = ConnectionFactory.get_opendental_source_connection()
            self.mysql_replication_engine = ConnectionFactory.get_mysql_replication_connection()
            self.postgres_analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
            
            # Initialize the MySQL replicator
            self.mysql_replicator = ExactMySQLReplicator(
                source_engine=self.opendental_source_engine,
                target_engine=self.mysql_replication_engine,
                source_db=self.source_db,
                target_db=self.replication_db
            )
            
            # Now that connections exist, create tracking tables
            self.ensure_tracking_tables()
            
            # Mark as successfully initialized
            self.connections_initialized = True
            
            logger.info("Successfully initialized all database connections and tracking tables")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {str(e)}")
            self.cleanup()
            raise
    
    def cleanup(self):
        """Clean up database connections and reset state."""
        try:
            if self.opendental_source_engine:
                self.opendental_source_engine.dispose()
                self.opendental_source_engine = None
                logger.info("Closed source database connection")
            
            if self.mysql_replication_engine:
                self.mysql_replication_engine.dispose()
                self.mysql_replication_engine = None
                logger.info("Closed replication database connection")
            
            if self.postgres_analytics_engine:
                self.postgres_analytics_engine.dispose()
                self.postgres_analytics_engine = None
                logger.info("Closed analytics database connection")
            
            # Reset state
            self.mysql_replicator = None
            self.connections_initialized = False
            self.tracking_tables_created = False
            
        except Exception as e:
            logger.error(f"Error during connection cleanup: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are cleaned up."""
        self.cleanup()
    
    def ensure_tracking_tables(self):
        """
        Create necessary tracking tables in replication and analytics databases.
        
        This method requires database connections to be initialized first.
        """
        if self.tracking_tables_created:
            logger.debug("Tracking tables already created")
            return
            
        if not self._connections_available():
            raise RuntimeError("Database connections must be initialized before creating tracking tables. Call initialize_connections() first.")
        
        try:
            logger.info("Creating tracking tables...")
            
            # Create extract tracking table in replication MySQL
            with self.mysql_replication_engine.begin() as conn:
                conn.execute(text(f"USE {self.replication_db}"))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_extract_status (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_extracted TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:01',
                        rows_extracted INTEGER DEFAULT 0,
                        extraction_status VARCHAR(50) DEFAULT 'pending',
                        schema_hash VARCHAR(32),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """))
            
            # Create load tracking table in PostgreSQL
            with self.postgres_analytics_engine.begin() as conn:
                # Ensure schema exists
                conn.execute(text(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.analytics_schema}
                """))
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.analytics_schema}.etl_load_status (
                        id SERIAL PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL UNIQUE,
                        last_loaded TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:01',
                        rows_loaded INTEGER DEFAULT 0,
                        load_status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
            
            self.tracking_tables_created = True
            logger.info("Tracking tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tracking tables: {str(e)}")
            raise
    
    def _connections_available(self) -> bool:
        """Check if all required database connections are available."""
        return all([
            self.opendental_source_engine is not None,
            self.mysql_replication_engine is not None,
            self.postgres_analytics_engine is not None
        ])
    
    def _require_connections(self):
        """Ensure database connections are available, or raise an error."""
        if not self._connections_available():
            raise RuntimeError(
                "Database connections required but not available. "
                "Call initialize_connections() before database operations."
            )
    
    def extract_to_replication(self, table_name: str, force_full: bool = False) -> bool:
        """
        Intelligent EXTRACT phase using configuration-driven approach.
        Uses smart batching and incremental extraction when available.
        """
        self._require_connections()  # Ensure database connections are available
        
        start_time = time.time()
        table_config = self.get_table_config(table_name)
        monitoring = self.get_monitoring_config(table_name)
        
        try:
            # Get last extraction info
            with self.mysql_replication_engine.connect() as conn:
                conn.execute(text(f"USE {self.replication_db}"))
                
                query = text("""
                    SELECT last_extracted, schema_hash 
                    FROM etl_extract_status 
                    WHERE table_name = :table_name
                """)
                result = conn.execute(query.bindparams(table_name=table_name))
                row = result.fetchone()
                
                last_extracted = row[0] if row else None
                stored_schema_hash = row[1] if row and len(row) > 1 else None
            
            # Check if schema has changed
            current_schema_hash = self.mysql_replicator.get_schema_hash(table_name)
            schema_changed = stored_schema_hash != current_schema_hash
            
            if schema_changed:
                logger.info(f"Schema change detected for {table_name}, forcing full extraction")
                force_full = True
            
            # Determine extraction strategy
            use_incremental = self.should_use_incremental(table_name) and not force_full and last_extracted
            batch_size = self.get_batch_size(table_name)
            
            logger.info(f"Extracting {table_name} - Strategy: {'incremental' if use_incremental else 'full'}, Batch size: {batch_size}")
            
            # Perform intelligent extraction
            if use_incremental:
                rows_extracted = self.mysql_replicator.extract_incremental_data(
                    table_name=table_name,
                    incremental_column=self.get_incremental_column(table_name),
                    last_extracted=last_extracted,
                    batch_size=batch_size
                )
            else:
                rows_extracted = self.mysql_replicator.extract_table_data(
                    table_name=table_name,
                    last_extracted=last_extracted,
                    force_full=force_full,
                    batch_size=batch_size
                )
            
            # Check processing time against SLA
            processing_time = (time.time() - start_time) / 60  # Convert to minutes
            max_time = monitoring.get('max_extraction_time_minutes', 10)
            
            if processing_time > max_time:
                logger.warning(f"{table_name} extraction took {processing_time:.2f} minutes (SLA: {max_time} minutes)")
                if monitoring.get('alert_on_failure', False):
                    self.send_monitoring_alert(table_name, 'TIME_EXCEEDED', f"Extraction time: {processing_time:.2f}min")
            
            # Verify extraction with intelligent tolerance
            if not self.mysql_replicator.verify_extraction(table_name, is_incremental=use_incremental):
                logger.error(f"Extraction verification failed for {table_name}")
                if monitoring.get('alert_on_failure', False):
                    # Get actual counts for better error message
                    try:
                        with self.opendental_source_engine.connect() as conn:
                            conn.execute(text(f"USE {self.source_db}"))
                            source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                        with self.mysql_replication_engine.connect() as conn:
                            conn.execute(text(f"USE {self.replication_db}"))
                            target_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                        
                        difference = abs(source_count - target_count)
                        alert_msg = f"Verification failed: source={source_count:,}, target={target_count:,}, difference={difference:,} rows"
                        
                        # For large tables, explain tolerance policy
                        if target_count > 100000:
                            tolerance = max(10, int(target_count * 0.001))
                            alert_msg += f" (tolerance for large tables: {tolerance:,} rows)"
                            
                    except Exception:
                        alert_msg = "Data verification failed"
                        
                    self.send_monitoring_alert(table_name, 'VERIFICATION_FAILED', alert_msg)
                return False
            
            # Update extraction status
            with self.mysql_replication_engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl_extract_status 
                    (table_name, last_extracted, rows_extracted, extraction_status, schema_hash)
                    VALUES (:table_name, :now, :rows, 'success', :schema_hash)
                    ON DUPLICATE KEY UPDATE 
                        last_extracted = :now,
                        rows_extracted = :rows,
                        extraction_status = 'success',
                        schema_hash = :schema_hash,
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=rows_extracted,
                    schema_hash=current_schema_hash
                ))
            
            importance = table_config.get('table_importance', 'reference')
            logger.info(f"[PASS] Successfully extracted {rows_extracted} rows for {table_name} ({importance}) in {processing_time:.2f} minutes")
            return True
            
        except Exception as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"[FAIL] Error in extract phase for {table_name} after {processing_time:.2f} minutes: {str(e)}")
            
            # Send alert for critical tables
            if monitoring.get('alert_on_failure', False):
                self.send_monitoring_alert(table_name, 'EXTRACTION_FAILED', str(e))
            
            return False
    
    def send_monitoring_alert(self, table_name: str, alert_type: str, message: str):
        """Send monitoring alert for critical issues."""
        table_config = self.get_table_config(table_name)
        importance = table_config.get('table_importance', 'reference')
        
        # Log the alert (in a real system, this would integrate with monitoring tools)
        alert_msg = f"[FAIL] ALERT [{alert_type}] - Table: {table_name} ({importance}) - {message}"
        logger.error(alert_msg)
        
        # TODO: Integrate with actual monitoring systems (PagerDuty, Slack, etc.)
        # For now, just log at ERROR level for critical tables
        if importance == 'critical':
            logger.critical(alert_msg)
    
    def get_table_row_count_from_target(self, table_name: str) -> int:
        """Get actual row count from target replication database."""
        try:
            if not self._connections_available():
                return 0
                
            with self.mysql_replication_engine.connect() as conn:
                conn.execute(text(f"USE {self.replication_db}"))
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                return int(count) if count is not None else 0
                
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name}: {str(e)}")
            return 0
    
    def load_to_analytics(self, table_name: str, is_incremental: bool = False) -> bool:
        """
        Intelligent LOAD phase with data quality validation.
        Applies PostgreSQL conversions and monitors data quality.
        Supports both full replacement and incremental loading strategies.
        """
        self._require_connections()  # Ensure database connections are available
        
        start_time = time.time()
        monitoring = self.get_monitoring_config(table_name)
        
        try:
            # Read from replication MySQL - for incremental loads, only get recent data
            with self.mysql_replication_engine.connect() as conn:
                if is_incremental:
                    # For incremental loads, only load data added since the last analytics load
                    # Get the last loaded timestamp from analytics tracking
                    with self.postgres_analytics_engine.connect() as analytics_conn:
                        last_loaded_query = f"""
                            SELECT last_loaded FROM {self.analytics_schema}.etl_load_status 
                            WHERE table_name = :table_name
                        """
                        result = analytics_conn.execute(text(last_loaded_query).bindparams(table_name=table_name))
                        row = result.fetchone()
                        last_loaded = row[0] if row else datetime(1970, 1, 1)
                    
                    # Get incremental column for filtering
                    incremental_column = self.get_incremental_column(table_name)
                    if incremental_column:
                        incremental_query = f"""
                            SELECT * FROM {table_name} 
                            WHERE {incremental_column} > %(last_loaded)s
                            ORDER BY {incremental_column}
                        """
                        df = pd.read_sql(incremental_query, conn, params={'last_loaded': last_loaded})
                        source_count = len(df)
                        logger.info(f"Loading {source_count} incremental rows from {table_name} since {last_loaded}")
                    else:
                        # No incremental column - skip incremental loading to avoid duplication
                        logger.warning(f"No incremental column found for {table_name}, skipping incremental load to prevent duplication")
                        return True
                else:
                    # For full loads, get all data
                    source_count_query = f"SELECT COUNT(*) FROM {table_name}"
                    source_count = conn.execute(text(source_count_query)).scalar()
                    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                    logger.info(f"Loading {source_count} total rows from {table_name} (full load)")
                
                if source_count == 0:
                    logger.info(f"No data to load for {table_name}")
                    return True
            
            # Apply basic PostgreSQL compatibility conversions
            df_clean = self.apply_basic_postgres_conversions(df, table_name)
            
            # Data quality validation
            quality_score = len(df_clean) / source_count if source_count > 0 else 1.0
            quality_threshold = monitoring.get('data_quality_threshold', 0.95)
            
            if quality_score < quality_threshold:
                error_msg = f"Data quality below threshold: {quality_score:.3f} < {quality_threshold}"
                logger.error(f"[FAIL] {table_name} - {error_msg}")
                if monitoring.get('alert_on_failure', False):
                    self.send_monitoring_alert(table_name, 'DATA_QUALITY_FAILED', error_msg)
                return False
            
            # Determine loading strategy based on extraction type
            if_exists_strategy = 'append' if is_incremental else 'replace'
            logger.info(f"Loading {table_name} to analytics using strategy: {if_exists_strategy}")
            
            # Note: For production incremental loads, consider implementing UPSERT logic
            # to handle potential duplicates if the same incremental data is loaded multiple times
            
            # Load to PostgreSQL analytics with intelligent batching
            batch_size = min(self.get_batch_size(table_name), 10000)  # Cap at 10k for memory
            
            with self.postgres_analytics_engine.begin() as conn:
                df_clean.to_sql(
                    table_name,
                    conn,
                    schema=self.analytics_schema,
                    if_exists=if_exists_strategy,
                    index=False,
                    method='multi',
                    chunksize=batch_size
                )
            
            # Verify load success
            with self.postgres_analytics_engine.connect() as conn:
                target_count_query = f"SELECT COUNT(*) FROM {self.analytics_schema}.{table_name}"
                target_count = conn.execute(text(target_count_query)).scalar()
                
                # For incremental loads, verify the data was appended correctly
                if is_incremental:
                    # For incremental, we just verify that the new data was loaded
                    if target_count < source_count:
                        error_msg = f"Incremental load verification failed: expected at least {source_count} new rows, but analytics table only has {target_count} total rows"
                        logger.error(f"[FAIL] {table_name} - {error_msg}")
                        if monitoring.get('alert_on_failure', False):
                            self.send_monitoring_alert(table_name, 'INCREMENTAL_LOAD_FAILED', error_msg)
                        return False
                    logger.info(f"Incremental load verified: {source_count} new rows appended to analytics (total: {target_count})")
                else:
                    # For full loads, verify the complete data quality
                    final_quality_score = target_count / source_count if source_count > 0 else 1.0
                    
                    if final_quality_score < quality_threshold:
                        error_msg = f"Full load quality check failed: {final_quality_score:.3f} < {quality_threshold}"
                        logger.error(f"[FAIL] {table_name} - {error_msg}")
                        if monitoring.get('alert_on_failure', False):
                            self.send_monitoring_alert(table_name, 'LOAD_QUALITY_FAILED', error_msg)
                        return False
                    logger.info(f"Full load verified: {target_count} rows loaded with quality score {final_quality_score:.3f}")
            
            # Update load status using PostgreSQL syntax
            with self.postgres_analytics_engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.analytics_schema}.etl_load_status 
                    (table_name, last_loaded, rows_loaded, load_status)
                    VALUES (:table_name, :now, :rows, 'success')
                    ON CONFLICT (table_name) DO UPDATE SET
                        last_loaded = EXCLUDED.last_loaded,
                        rows_loaded = EXCLUDED.rows_loaded,
                        load_status = EXCLUDED.load_status,
                        updated_at = CURRENT_TIMESTAMP
                """).bindparams(
                    table_name=table_name,
                    now=datetime.now(),
                    rows=target_count
                ))
            
            processing_time = (time.time() - start_time) / 60
            table_config = self.get_table_config(table_name)
            importance = table_config.get('table_importance', 'reference')
            load_type = "incremental" if is_incremental else "full"
            
            if is_incremental:
                logger.info(f"[PASS] Successfully loaded {source_count} new rows to analytics for {table_name} ({importance}) - {load_type} load in {processing_time:.2f} minutes (total: {target_count} rows)")
            else:
                logger.info(f"[PASS] Successfully loaded {target_count} rows to analytics for {table_name} ({importance}) - {load_type} load with quality: {final_quality_score:.3f} in {processing_time:.2f} minutes")
            return True
            
        except Exception as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"[FAIL] Error in load phase for {table_name} after {processing_time:.2f} minutes: {str(e)}")
            
            if monitoring.get('alert_on_failure', False):
                self.send_monitoring_alert(table_name, 'LOAD_FAILED', str(e))
            
            return False
    
    def apply_basic_postgres_conversions(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Apply basic type conversions for PostgreSQL compatibility.
        Only handles technical transformations, no business logic.
        """
        try:
            df_clean = df.copy()
            
            # Convert MySQL zero dates to NULL
            for col in df_clean.columns:
                if df_clean[col].dtype == 'datetime64[ns]':
                    df_clean[col] = df_clean[col].replace(pd.Timestamp('1969-12-31 19:00:00'), pd.NaT)
            
            # Convert boolean columns
            for col in df_clean.columns:
                if df_clean[col].dtype == 'int64' and df_clean[col].isin([0, 1]).all():
                    df_clean[col] = df_clean[col].astype('bool')
            
            # Convert text columns to string
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    df_clean[col] = df_clean[col].astype('string')
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error applying PostgreSQL conversions for {table_name}: {str(e)}")
            raise
    
    def run_elt_pipeline(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete intelligent ETL pipeline for a single table.
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full extraction
            
        Returns:
            bool: True if successful, False otherwise
        """
        start_time = time.time()
        table_config = self.get_table_config(table_name)
        importance = table_config.get('table_importance', 'reference')
        
        try:
            # Ensure connections are initialized
            if not self.connections_initialized:
                self.initialize_connections()
            
            logger.info(f"[START] Starting ETL pipeline for {table_name} ({importance})")
            
            # Determine extraction strategy (same logic as in extract_to_replication)
            with self.mysql_replication_engine.connect() as conn:
                conn.execute(text(f"USE {self.replication_db}"))
                query = text("SELECT last_extracted FROM etl_extract_status WHERE table_name = :table_name")
                result = conn.execute(query.bindparams(table_name=table_name))
                row = result.fetchone()
                last_extracted = row[0] if row else None
            
            use_incremental = self.should_use_incremental(table_name) and not force_full and last_extracted
            
            # Extract to replication
            if not self.extract_to_replication(table_name, force_full):
                logger.error(f"[FAIL] Extraction failed for {table_name}")
                return False
            
            # Load to analytics with extraction type information
            if not self.load_to_analytics(table_name, is_incremental=use_incremental):
                logger.error(f"[FAIL] Loading failed for {table_name}")
                return False
            
            total_time = (time.time() - start_time) / 60
            logger.info(f"[PASS] Successfully completed ETL pipeline for {table_name} ({importance}) in {total_time:.2f} minutes")
            return True
            
        except Exception as e:
            total_time = (time.time() - start_time) / 60
            logger.error(f"[FAIL] Error in ETL pipeline for {table_name} after {total_time:.2f} minutes: {str(e)}")
            return False
    
    def process_tables_by_priority(self, importance_levels: List[str] = None, max_workers: int = 5, force_full: bool = False) -> Dict[str, List[str]]:
        """
        Process tables by priority with intelligent parallelization.
        
        Args:
            importance_levels: List of importance levels to process (default: all)
            max_workers: Maximum number of parallel workers for critical tables
            force_full: Whether to force full extraction for all tables
            
        Returns:
            Dict with success/failure lists for each importance level
        """
        if importance_levels is None:
            importance_levels = ['critical', 'important', 'audit', 'reference']
        
        results = {}
        
        for importance in importance_levels:
            tables = self.get_tables_by_priority(importance)
            if not tables:
                logger.info(f"No tables found for importance level: {importance}")
                continue
                
            logger.info(f"[START] Processing {len(tables)} {importance} tables")
            
            if importance == 'critical' and len(tables) > 1:
                # Process critical tables in parallel for speed
                success_tables, failed_tables = self.process_tables_parallel(tables, max_workers, force_full)
            else:
                # Process other tables sequentially to manage resources
                success_tables, failed_tables = self.process_tables_sequential(tables, force_full)
            
            results[importance] = {
                'success': success_tables,
                'failed': failed_tables,
                'total': len(tables)
            }
            
            logger.info(f"[PASS] {importance.capitalize()} tables: {len(success_tables)}/{len(tables)} successful")
            
            # Stop processing if critical tables failed
            if importance == 'critical' and failed_tables:
                logger.error(f"[FAIL] Critical table failures detected. Stopping pipeline.")
                break
        
        return results
    
    def process_tables_parallel(self, tables: List[str], max_workers: int = 5, force_full: bool = False) -> Tuple[List[str], List[str]]:
        """Process tables in parallel using ThreadPoolExecutor."""
        success_tables = []
        failed_tables = []
        
        logger.info(f"[START] Processing {len(tables)} tables in parallel (max workers: {max_workers})")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks with force_full parameter
            future_to_table = {
                executor.submit(self.run_elt_pipeline, table, force_full): table 
                for table in tables
            }
            
            # Process completed tasks
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    success = future.result()
                    if success:
                        success_tables.append(table)
                    else:
                        failed_tables.append(table)
                except Exception as e:
                    logger.error(f"[FAIL] Exception in parallel processing for {table}: {str(e)}")
                    failed_tables.append(table)
        
        return success_tables, failed_tables
    
    def process_tables_sequential(self, tables: List[str], force_full: bool = False) -> Tuple[List[str], List[str]]:
        """Process tables sequentially."""
        success_tables = []
        failed_tables = []
        
        logger.info(f"[SEQ] Processing {len(tables)} tables sequentially")
        
        for table in tables:
            try:
                success = self.run_elt_pipeline(table, force_full)
                if success:
                    success_tables.append(table)
                else:
                    failed_tables.append(table)
            except Exception as e:
                logger.error(f"[FAIL] Exception in sequential processing for {table}: {str(e)}")
                failed_tables.append(table)
        
        return success_tables, failed_tables
    
    def run_phase1_critical_tables(self) -> bool:
        """
        Run Phase 1 implementation: Process top 5 critical tables.
        This is the entry point for Phase 1 testing.
        """
        logger.info("[START] Starting Phase 1: Critical Tables Processing")
        
        # Get top 5 critical tables
        critical_tables = self.get_critical_tables()
        
        if not critical_tables:
            logger.error("[FAIL] No critical tables found for Phase 1")
            return False
        
        logger.info(f"[INFO] Phase 1 tables: {critical_tables}")
        
        # Process critical tables with parallel execution  
        success_tables, failed_tables = self.process_tables_parallel(critical_tables, max_workers=5)
        
        # Report results
        success_rate = len(success_tables) / len(critical_tables) * 100
        logger.info(f"[INFO] Phase 1 Results: {len(success_tables)}/{len(critical_tables)} successful ({success_rate:.1f}%)")
        
        if failed_tables:
            logger.error(f"[FAIL] Failed tables: {failed_tables}")
            return False
        
        logger.info("[PASS] Phase 1 completed successfully!")
        return True

def main():
    """Main entry point for the intelligent ETL pipeline."""
    import argparse
    
    # Log the start of the ETL run
    logger.info("=" * 80)
    logger.info("INTELLIGENT ETL PIPELINE - RUN STARTED")
    logger.info("=" * 80)
    logger.info(f"Log file: {log_file_path}")
    
    parser = argparse.ArgumentParser(description='Intelligent OpenDental ETL Pipeline')
    parser.add_argument('--phase', choices=['1', '2', '3', '4'], default='1',
                       help='Implementation phase to run (default: 1)')
    parser.add_argument('--tables', type=str, nargs='+',
                       help='Specific tables to process')
    parser.add_argument('--importance', choices=['critical', 'important', 'audit', 'reference'],
                       help='Process tables by importance level')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be processed without executing')
    parser.add_argument('--config', type=str, default='etl_pipeline/config/tables.yaml',
                       help='Path to table configuration file')
    
    args = parser.parse_args()
    logger.info(f"Arguments: {vars(args)}")
    
    try:
        # Initialize intelligent pipeline
        pipeline = IntelligentELTPipeline(config_path=args.config)
        pipeline.validate_database_names()
        pipeline.initialize_connections()
        
        if args.dry_run:
            logger.info("[TEST] DRY RUN MODE - Showing what would be processed")
            
            if args.tables:
                tables = args.tables
            elif args.importance:
                tables = pipeline.get_tables_by_priority(args.importance)
            else:
                # Default to Phase 1 critical tables
                tables = pipeline.get_critical_tables()
            
            logger.info(f"[INFO] Would process {len(tables)} tables: {tables}")
            
            for table in tables:
                config = pipeline.get_table_config(table)
                logger.info(f"  - {table}: {config.get('table_importance')} "
                           f"({config.get('estimated_size_mb', 0):.1f} MB, "
                           f"{config.get('extraction_strategy')} extraction)")
            return
        
        # Execute based on arguments
        if args.tables:
            # Process specific tables
            logger.info(f"[START] Processing specific tables: {args.tables}")
            success_count = 0
            for table in args.tables:
                if pipeline.run_elt_pipeline(table):
                    success_count += 1
            
            logger.info(f"[PASS] Completed: {success_count}/{len(args.tables)} tables successful")
            
        elif args.importance:
            # Process by importance level
            logger.info(f"[START] Processing {args.importance} tables")
            results = pipeline.process_tables_by_priority([args.importance])
            
        elif args.phase == '1':
            # Phase 1: Critical tables only
            if not pipeline.run_phase1_critical_tables():
                logger.error("[FAIL] Phase 1 failed")
                sys.exit(1)
                
        elif args.phase == '2':
            # Phase 2: Critical + Important tables
            logger.info("[START] Starting Phase 2: Critical + Important Tables")
            results = pipeline.process_tables_by_priority(['critical', 'important'])
            
        elif args.phase == '3':
            # Phase 3: Critical + Important + Audit tables
            logger.info("[START] Starting Phase 3: Critical + Important + Audit Tables")
            results = pipeline.process_tables_by_priority(['critical', 'important', 'audit'])
            
        elif args.phase == '4':
            # Phase 4: All tables
            logger.info("[START] Starting Phase 4: All Tables")
            results = pipeline.process_tables_by_priority()
        
        logger.info("[PASS] ETL Pipeline completed successfully!")
        logger.info("=" * 80)
        logger.info("INTELLIGENT ETL PIPELINE - RUN COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"[FAIL] Pipeline failed: {str(e)}")
        logger.error("=" * 80)
        logger.error("INTELLIGENT ETL PIPELINE - RUN FAILED")
        logger.error("=" * 80)
        sys.exit(1)
    finally:
        if 'pipeline' in locals():
            pipeline.cleanup()
        logger.info(f"Run completed. Full logs available in: {log_file_path}")

if __name__ == "__main__":
    main()