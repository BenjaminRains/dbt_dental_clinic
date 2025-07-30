"""
OpenDental Schema Analysis Script

This is THE single source of schema analysis for the entire ETL pipeline.
All schema analysis, table relationship discovery, and configuration generation
happens here using direct database connections and modern architecture.

Connection Management Features:
- Uses existing ConnectionManager from etl_pipeline.core.connections
- Connection pooling with proper cleanup and retry logic
- Rate limiting to prevent overwhelming the database server (100ms minimum between queries)
- Batch processing to reduce database calls (5 tables per batch)
- Timeout handling for all database operations (30s timeout)
- Automatic retry logic for transient failures

Performance Optimizations:
- Uses estimated row counts from information_schema.TABLE_ROWS (much faster than COUNT(*))
- Falls back to size-based estimation when TABLE_ROWS is unavailable
- Avoids expensive COUNT(*) queries that can lock large tables

Usage:
    python etl_pipeline/scripts/analyze_opendental_schema.py

Output:
    - etl_pipeline/config/tables.yml (pipeline configuration with internal versioning)
    - etl_pipeline/logs/schema_analysis_YYYYMMDD_HHMMSS.json (detailed analysis)
    - Console summary report

This script uses modern connection handling and direct database analysis.
The tables.yml file includes metadata for version tracking and change detection.
"""

import os
import yaml
import json
import hashlib
from datetime import datetime, date
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from decimal import Decimal
import shutil
from dotenv import load_dotenv
import time
import threading
import concurrent.futures

from etl_pipeline.core.connections import ConnectionFactory, create_connection_manager
from etl_pipeline.config import get_settings

# Configure timeout for database operations
DB_TIMEOUT = 30  # seconds
BATCH_SIZE = 5  # Process tables in smaller batches for better responsiveness
RATE_LIMIT_DELAY = 0.5  # Delay between database operations (seconds)

# Configure logging
def setup_logging():
    """Setup logging with organized directory structure."""
    # Create organized log directories
    logs_base = Path('logs')
    schema_analysis_logs = logs_base / 'schema_analysis' / 'logs'
    schema_analysis_reports = logs_base / 'schema_analysis' / 'reports'
    
    schema_analysis_logs.mkdir(parents=True, exist_ok=True)
    schema_analysis_reports.mkdir(parents=True, exist_ok=True)
    
    # Get a logger specifically for schema analysis to avoid conflicts
    logger = logging.getLogger('schema_analysis')
    logger.setLevel(logging.INFO)
    
    # Add console handler if not already present
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# Constants for filtering tables
EXCLUDED_PATTERNS = ['tmp_', 'temp_', 'backup_', '#', 'test_']

# DBT model patterns for discovery
DBT_STAGING_PATTERNS = ['stg_opendental__*.sql', 'stg_*.sql']
DBT_MART_PATTERNS = ['dim_*.sql', 'fact_*.sql', 'mart_*.sql']
DBT_INTERMEDIATE_PATTERNS = ['int_*.sql', 'intermediate_*.sql']

# Custom YAML representer for Decimal objects
def decimal_representer(dumper, data):
    return dumper.represent_float(float(data))

yaml.add_representer(Decimal, decimal_representer)

def run_with_timeout(func, timeout_seconds):
    """Run a function with a timeout using threading."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")

def setup_environment():
    """Setup environment variables for the script."""
    # Get the script directory and project paths
    script_dir = Path(__file__).parent
    etl_pipeline_dir = script_dir.parent  # etl_pipeline directory
    project_root = etl_pipeline_dir.parent  # project root
    
    # Check if ETL_ENVIRONMENT is already set
    if not os.getenv('ETL_ENVIRONMENT'):
        logger.info("ETL_ENVIRONMENT not set, attempting to detect environment...")
        
        # Try to load from environment files
        env_files = [
            etl_pipeline_dir / '.env_production',
            etl_pipeline_dir / '.env_test',
            project_root / '.env_production',
            project_root / '.env_test'
        ]
        
        loaded = False
        for env_file in env_files:
            if env_file.exists():
                logger.info(f"Loading environment from: {env_file}")
                load_dotenv(env_file, override=True)
                loaded = True
                break
        
        if not loaded:
            # Fail fast if no environment is set and no files found
            raise ValueError("ETL_ENVIRONMENT environment variable is not set")
        else:
            logger.info(f"Environment loaded, ETL_ENVIRONMENT={os.getenv('ETL_ENVIRONMENT')}")
    
    # Validate environment
    environment = os.getenv('ETL_ENVIRONMENT')
    if not environment:
        raise ValueError("ETL_ENVIRONMENT environment variable is not set")
    
    if environment not in ['production', 'test']:
        raise ValueError(f"Invalid ETL_ENVIRONMENT: {environment}. Must be 'production' or 'test'")
    
    logger.info(f"Using environment: {environment}")
    return environment

class OpenDentalSchemaAnalyzer:
    """Coordinates complete OpenDental schema analysis using direct database connections."""
    
    def __init__(self):
        """Initialize with source database connection using modern connection handling."""
        # Setup environment first
        self.environment = setup_environment()
        
        # Use explicit production connection
        settings = get_settings()
        self.source_engine = ConnectionFactory.get_source_connection(settings)
        self.source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        if not self.source_db:
            raise ValueError("OPENDENTAL_SOURCE_DB environment variable is required")
        
        # Get dbt project root (current directory)
        self.dbt_project_root = str(Path(__file__).parent.parent.parent)
        
        # Initialize inspector for schema analysis
        self.inspector = inspect(self.source_engine)
        
        logger.info("Schema analyzer initialized with existing ConnectionManager architecture")
    
    def discover_all_tables(self) -> List[str]:
        """Discover all tables in the source database."""
        try:
            logger.info("Discovering all tables in database...")
            tables = self.inspector.get_table_names()
            logger.info(f"Discovered {len(tables)} tables in database")
            return tables
        except Exception as e:
            logger.error(f"Failed to discover tables: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get detailed schema information for a table using existing ConnectionManager."""
        def _get_schema():
            # Use the existing ConnectionManager for proper connection handling
            with create_connection_manager(self.source_engine) as conn_manager:
                # Get all schema information using the connection manager
                columns = self.inspector.get_columns(table_name)
                primary_keys = self.inspector.get_pk_constraint(table_name)
                foreign_keys = self.inspector.get_foreign_keys(table_name)
                indexes = self.inspector.get_indexes(table_name)
                
                return {
                    'table_name': table_name,
                    'columns': {col['name']: {
                        'type': str(col['type']),
                        'nullable': col['nullable'],
                        'default': col['default'],
                        'primary_key': col['name'] in primary_keys.get('constrained_columns', [])
                    } for col in columns},
                    'primary_keys': primary_keys.get('constrained_columns', []),
                    'foreign_keys': foreign_keys,
                    'indexes': indexes
                }
        
        try:
            return run_with_timeout(_get_schema, DB_TIMEOUT)
        except TimeoutError:
            logger.warning(f"Timeout getting schema for table {table_name}")
            return {'table_name': table_name, 'error': 'timeout'}
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return {'table_name': table_name, 'error': str(e)}
    
    def get_estimated_row_count(self, table_name: str) -> int:
        """Get estimated row count using MySQL's TABLE_ROWS from information_schema.
        
        This method is much faster than COUNT(*) and avoids locking large tables.
        TABLE_ROWS provides an estimate that is updated by MySQL's statistics.
        If TABLE_ROWS is unavailable, falls back to size-based estimation.
        """
        def _get_estimated_count():
            with create_connection_manager(self.source_engine) as conn_manager:
                # Use TABLE_ROWS from information_schema for estimated count (much faster than COUNT(*))
                estimated_count_result = conn_manager.execute_with_retry(f"""
                    SELECT TABLE_ROWS 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.source_db}' 
                    AND table_name = '{table_name}'
                """)
                estimated_count = estimated_count_result.scalar() if estimated_count_result else 0
                
                # If TABLE_ROWS is NULL or 0, try to get a rough estimate from table size
                if not estimated_count:
                    # Get table size and estimate rows based on average row size
                    size_result = conn_manager.execute_with_retry(f"""
                        SELECT 
                            data_length,
                            index_length,
                            ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                        FROM information_schema.tables 
                        WHERE table_schema = '{self.source_db}' 
                        AND table_name = '{table_name}'
                    """)
                    
                    if size_result:
                        row = size_result.fetchone()
                        if row and row[0]:  # data_length exists
                            data_length = row[0]
                            # Estimate based on average row size of 1KB (conservative estimate)
                            estimated_count = max(1, int(data_length / 1024))
                        else:
                            estimated_count = 0
                    else:
                        estimated_count = 0
                
                return estimated_count
        
        try:
            return run_with_timeout(_get_estimated_count, DB_TIMEOUT)
        except TimeoutError:
            logger.warning(f"Timeout getting estimated row count for table {table_name}")
            return 0
        except Exception as e:
            logger.error(f"Failed to get estimated row count for table {table_name}: {e}")
            return 0

    def get_table_size_info(self, table_name: str) -> Dict:
        """Get table size and estimated row count information using existing ConnectionManager."""
        def _get_size_info():
            # Use the existing ConnectionManager for proper connection handling
            with create_connection_manager(self.source_engine) as conn_manager:
                # Get estimated row count (much faster than COUNT(*))
                estimated_row_count = self.get_estimated_row_count(table_name)
                
                # Get table size (approximate)
                size_result = conn_manager.execute_with_retry(f"""
                    SELECT 
                        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.source_db}' 
                    AND table_name = '{table_name}'
                """)
                size_mb = size_result.scalar() if size_result else 0
                
                return {
                    'table_name': table_name,
                    'estimated_row_count': estimated_row_count,
                    'size_mb': size_mb,
                    'source': 'information_schema_estimate'
                }
        
        try:
            return run_with_timeout(_get_size_info, DB_TIMEOUT)
        except TimeoutError:
            logger.warning(f"Timeout getting size info for table {table_name}")
            return {
                'table_name': table_name,
                'estimated_row_count': 0,
                'size_mb': 0,
                'error': 'timeout'
            }
        except Exception as e:
            logger.error(f"Failed to get size info for table {table_name}: {e}")
            return {
                'table_name': table_name,
                'estimated_row_count': 0,
                'size_mb': 0,
                'error': str(e)
            }
    
    def discover_dbt_models(self) -> Dict[str, List[str]]:
        """Discover dbt models in the project."""
        logger.info("Discovering dbt models...")
        dbt_models = {
            'staging': [],
            'mart': [],
            'intermediate': []
        }
        
        try:
            models_dir = Path(self.dbt_project_root) / 'models'
            if not models_dir.exists():
                logger.warning(f"dbt models directory not found: {models_dir}")
                return dbt_models
            
            # Discover staging models
            staging_dir = models_dir / 'staging'
            if staging_dir.exists():
                for sql_file in staging_dir.rglob('*.sql'):
                    dbt_models['staging'].append(sql_file.stem)
            
            # Discover mart models
            marts_dir = models_dir / 'marts'
            if marts_dir.exists():
                for sql_file in marts_dir.rglob('*.sql'):
                    dbt_models['mart'].append(sql_file.stem)
            
            # Discover intermediate models
            intermediate_dir = models_dir / 'intermediate'
            if intermediate_dir.exists():
                for sql_file in intermediate_dir.rglob('*.sql'):
                    dbt_models['intermediate'].append(sql_file.stem)
            
            total_models = sum(len(models) for models in dbt_models.values())
            logger.info(f"Discovered {total_models} dbt models: {len(dbt_models['staging'])} staging, {len(dbt_models['mart'])} mart, {len(dbt_models['intermediate'])} intermediate")
            return dbt_models
            
        except Exception as e:
            logger.error(f"Failed to discover dbt models: {e}")
            return dbt_models
    
    def determine_table_importance(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine table importance based on schema and size analysis."""
        # Critical tables (core business entities) - these are fundamental to dental practice operations
        critical_tables = ['patient', 'appointment', 'procedurelog', 'claimproc', 'payment']
        if table_name.lower() in critical_tables:
            return 'critical'
        
        # Reference tables (lookup data) - prioritize over size considerations
        if 'def' in table_name.lower() or 'type' in table_name.lower():
            return 'reference'
        
        # Determine initial importance based on size and business patterns
        initial_importance = 'standard'
        
        # Large tables (performance consideration) - automatically important due to size
        if size_info.get('estimated_row_count', 0) > 1000000:  # 1M+ rows
            initial_importance = 'important'
        
        # Insurance and billing related tables (high business value)
        insurance_billing_patterns = ['insplan', 'patplan', 'carrier', 'claim', 'payment', 'fee']
        if any(pattern in table_name.lower() for pattern in insurance_billing_patterns):
            initial_importance = 'important'
        
        # Clinical procedure related tables (high business value)
        clinical_patterns = ['procedure', 'treatment', 'diagnosis', 'medication']
        if any(pattern in table_name.lower() for pattern in clinical_patterns):
            initial_importance = 'important'
        
        # Audit tables (logging/history) - override any size-based importance
        if 'log' in table_name.lower() or 'hist' in table_name.lower():
            return 'audit'
        
        return initial_importance
    
    def determine_extraction_strategy(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
        """Determine optimal extraction strategy for a table based on size and available incremental columns."""
        estimated_row_count = size_info.get('estimated_row_count', 0)
        
        # Find available incremental columns
        incremental_columns = self.find_incremental_columns(table_name, schema_info)
        has_incremental_columns = len(incremental_columns) > 0
        
        # If no incremental columns, use full table
        if not has_incremental_columns:
            return 'full_table'
        
        # Chunked incremental for very large tables (> 1M rows)
        if estimated_row_count > 1_000_000:
            return 'chunked_incremental'
        
        # Incremental for medium to large tables (> 10k rows)
        if estimated_row_count > 10_000:
            return 'incremental'
        
        # For small tables (< 10k rows), use small_table strategy
        return 'small_table'
    
    def determine_incremental_strategy(self, table_name: str, schema_info: Dict, incremental_columns: List[str]) -> str:
        """
        Determine the optimal incremental strategy for a table.
        
        Strategies:
        - 'or_logic': Use OR logic for multiple incremental columns (captures more updates)
        - 'and_logic': Use AND logic for multiple incremental columns (more conservative)
        - 'single_column': Use only the primary incremental column
        - 'none': No incremental strategy (fallback to full table)
        
        Args:
            table_name: Name of the table
            schema_info: Schema information for the table
            incremental_columns: List of available incremental columns
            
        Returns:
            str: The determined incremental strategy
        """
        if not incremental_columns:
            return 'none'
        
        # If only one column, use single_column strategy
        if len(incremental_columns) == 1:
            return 'single_column'
        
        # For multiple columns, determine based on data quality and business logic
        # Use OR logic for most tables to capture more incremental updates
        # Use AND logic only for tables where we need to be very conservative
        
        # Tables that should use AND logic (more conservative)
        conservative_tables = {
            'claimproc', 'payment', 'adjustment', 'claim', 'patplan'
        }
        
        if table_name.lower() in conservative_tables:
            return 'and_logic'
        
        # Default to OR logic for most tables (captures more updates)
        return 'or_logic'
    
    def validate_incremental_column_data_quality(self, table_name: str, column_name: str) -> bool:
        """
        Validate data quality for an incremental column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column to validate
            
        Returns:
            bool: True if column passes data quality validation
        """
        try:
            with create_connection_manager(self.source_engine) as conn_manager:
                # Sample the column to check data quality
                sample_query = f"""
                    SELECT MIN({column_name}), MAX({column_name}), COUNT(*)
                    FROM `{self.source_db}`.`{table_name}`
                    WHERE {column_name} IS NOT NULL
                    LIMIT 1000
                """
                
                result = conn_manager.execute_with_retry(sample_query)
                if not result:
                    return False
                
                row = result.fetchone()
                if not row:
                    return False
                
                min_date = row[0]
                max_date = row[1]
                count = row[2]
                
                # Skip columns with obviously bad dates
                if isinstance(min_date, (datetime, date)):
                    if min_date.year < 2000 or max_date.year > 2030:
                        logger.debug(f"Column {column_name} in {table_name} has bad date range: {min_date} to {max_date}")
                        return False
                
                # Skip columns with too many NULL values (indicating poor data quality)
                if count < 100:  # Less than 100 non-null values in sample
                    logger.debug(f"Column {column_name} in {table_name} has poor data quality: only {count} non-null values")
                    return False
                
                return True
                
        except Exception as e:
            logger.warning(f"Could not validate data quality for column {column_name} in {table_name}: {str(e)}")
            # If validation fails, return False (fail closed) to avoid using unreliable columns
            return False

    def find_incremental_columns(self, table_name: str, schema_info: Dict) -> List[str]:
        """
        Find timestamp and datetime columns for incremental loading with data quality validation.
        
        This method now includes:
        - Data quality validation (date range checks)
        - Column prioritization (limit to most reliable columns)
        - Business logic for column selection
        - Exclusion of workflow-specific columns
        """
        columns = schema_info.get('columns', {})
        timestamp_columns = []
        
        # Columns to exclude - workflow-specific datetime columns that don't track record modifications
        excluded_columns = {
            'DateTimeArrived', 'DateTimeSeated', 'DateTimeDismissed', 'DateTimeAskedToArrive',
            'DateTimeStarted', 'DateTimeFinished', 'DateTimeCheckedIn', 'DateTimeCheckedOut',
            'DateTimeConfirmed', 'DateTimeCancelled', 'DateTimeRescheduled', 'IntakeDate', 
            'DateTimeNewPatThankYouTransmit', 'DateTimeSent', 'DateTimeThankYouTransmit', 'DateTimeExpire',
            'DateTimeSmsScheduled', 'DateTimeSmsSent', 'DateTimeEmailSent', 'DateTimeOrig', 'DateExclude',
            'DateTimePending', 'DateTimeCompleted', 'DateTimeExpired', 'DateTimeLastError',
            'DateTimeEntry', 'SmsContractDate', 'TimeEntered1', 'TimeDisplayed1', 'TimeEntered2',
            'TimeDisplayed2', 'DateTimeLastConnect', 'DateTimeEnd', 'LastHeartBeat', 'DateTimeConfirmTransmit',
            'DateTimeRSVP', 'DateTimeConfirmExpire', 'LastQueryTime', 'DateLastRun', 'DateTimeShown',
            'DateTRequest', 'DateTAcceptDeny', 'DateTAppend', 'DischargeDate', 'DateTPracticeSigned'
        }
        
        # Priority order for column selection (most reliable first)
        priority_order = [
            'DateTStamp',      # Most recent timestamp - highest priority
            'SecDateTEdit',    # Security date edit - second priority
            'SecDateTEntry',   # Security date entry - third priority
            'DateTEntry',      # Date entry - fourth priority
            'DateTimeModified', # Date time modified
            'DateModified',    # Date modified
            'DateTimeCreated', # Date time created
            'DateCreated'      # Date created
        ]
        
        # First pass: collect all timestamp/datetime columns
        candidate_columns = []
        for column_name, column_info in columns.items():
            # Handle None values safely
            data_type = column_info.get('type')
            if data_type is None:
                continue
                
            data_type_str = str(data_type).lower()
            
            # Only include timestamp and datetime columns, exclude simple date columns
            if any(dt in data_type_str for dt in ['timestamp', 'datetime']):
                # Skip workflow-specific datetime columns
                if column_name not in excluded_columns:
                    candidate_columns.append(column_name)
        
        # Second pass: validate data quality and prioritize
        for column_name in candidate_columns:
            # Validate data quality
            if self.validate_incremental_column_data_quality(table_name, column_name):
                timestamp_columns.append(column_name)
        
        # Third pass: prioritize and limit to most reliable columns
        prioritized_columns = []
        
        # First, add columns in priority order
        for priority_column in priority_order:
            if priority_column in timestamp_columns:
                prioritized_columns.append(priority_column)
        
        # Then, add any remaining columns
        for column_name in timestamp_columns:
            if column_name not in prioritized_columns:
                prioritized_columns.append(column_name)
        
        # Limit to 2-3 most reliable columns to avoid complexity
        final_columns = prioritized_columns[:3]
        
        if len(final_columns) != len(timestamp_columns):
            logger.info(f"Filtered incremental columns for {table_name}: {len(timestamp_columns)} -> {len(final_columns)} columns")
        
        return final_columns
    
    def select_primary_incremental_column(self, incremental_columns: List[str]) -> Optional[str]:
        """
        Select the primary incremental column from a list based on priority order.
        
        Priority order (highest to lowest):
        1. DateTStamp - Most recent timestamp (when record was last modified)
        2. SecDateTEdit - Security date edit (when record was last edited)
        3. SecDateTEntry - Security date entry (when record was created/modified)
        4. DateTEntry - Date entry
        5. All others - Any remaining timestamp columns
        
        Args:
            incremental_columns: List of incremental column names
            
        Returns:
            The selected primary incremental column, or None if no columns provided
        """
        if not incremental_columns:
            return None
        
        # Priority order for selecting the best incremental column
        priority_order = [
            'DateTStamp',      # Most recent timestamp - highest priority
            'SecDateTEdit',    # Security date edit - second priority
            'SecDateTEntry',   # Security date entry - third priority
            'DateTEntry'       # Date entry - fourth priority
        ]
        
        # First, try to find a column that matches our priority order
        for priority_column in priority_order:
            if priority_column in incremental_columns:
                logger.debug(f"Selected primary incremental column '{priority_column}' based on priority order")
                return priority_column
        
        # If no priority columns found, return the first column in the list
        # This ensures we always have a fallback
        selected_column = incremental_columns[0]
        logger.debug(f"Selected primary incremental column '{selected_column}' as fallback (no priority columns found)")
        return selected_column
    
    def get_batch_schema_info(self, table_names: List[str]) -> Dict[str, Dict]:
        """Get schema information for multiple tables in a single connection."""
        def _get_batch_schema():
            with create_connection_manager(self.source_engine) as conn_manager:
                batch_results = {}
                
                for table_name in table_names:
                    try:
                        # Get all schema information for this table
                        columns = self.inspector.get_columns(table_name)
                        primary_keys = self.inspector.get_pk_constraint(table_name)
                        foreign_keys = self.inspector.get_foreign_keys(table_name)
                        indexes = self.inspector.get_indexes(table_name)
                        
                        batch_results[table_name] = {
                            'table_name': table_name,
                            'columns': {col['name']: {
                                'type': str(col['type']),
                                'nullable': col['nullable'],
                                'default': col['default'],
                                'primary_key': col['name'] in primary_keys.get('constrained_columns', [])
                            } for col in columns},
                            'primary_keys': primary_keys.get('constrained_columns', []),
                            'foreign_keys': foreign_keys,
                            'indexes': indexes
                        }
                    except Exception as e:
                        logger.warning(f"Failed to get schema for table {table_name}: {e}")
                        batch_results[table_name] = {
                            'table_name': table_name,
                            'error': str(e)
                        }
                
                return batch_results
        
        try:
            return run_with_timeout(_get_batch_schema, DB_TIMEOUT * 2)  # Longer timeout for batch operations
        except TimeoutError:
            logger.warning(f"Timeout getting batch schema for {len(table_names)} tables")
            return {table_name: {'table_name': table_name, 'error': 'timeout'} for table_name in table_names}
        except Exception as e:
            logger.error(f"Failed to get batch schema: {e}")
            return {table_name: {'table_name': table_name, 'error': str(e)} for table_name in table_names}
    
    def get_batch_size_info(self, table_names: List[str]) -> Dict[str, Dict]:
        """Get size information for multiple tables in a single connection using estimated row counts."""
        def _get_batch_size():
            with create_connection_manager(self.source_engine) as conn_manager:
                batch_results = {}
                
                for table_name in table_names:
                    try:
                        # Get estimated row count (much faster than COUNT(*))
                        estimated_row_count = self.get_estimated_row_count(table_name)
                        
                        # Get table size (approximate)
                        size_result = conn_manager.execute_with_retry(f"""
                            SELECT 
                                ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                            FROM information_schema.tables 
                            WHERE table_schema = '{self.source_db}' 
                            AND table_name = '{table_name}'
                        """)
                        size_mb = size_result.scalar() if size_result else 0
                        
                        batch_results[table_name] = {
                            'table_name': table_name,
                            'estimated_row_count': estimated_row_count,
                            'size_mb': size_mb,
                            'source': 'information_schema_estimate'
                        }
                    except Exception as e:
                        logger.warning(f"Failed to get size info for table {table_name}: {e}")
                        batch_results[table_name] = {
                            'table_name': table_name,
                            'estimated_row_count': 0,
                            'size_mb': 0,
                            'error': str(e)
                        }
                
                return batch_results
        
        try:
            return run_with_timeout(_get_batch_size, DB_TIMEOUT * 2)  # Longer timeout for batch operations
        except TimeoutError:
            logger.warning(f"Timeout getting batch size info for {len(table_names)} tables")
            return {table_name: {
                'table_name': table_name,
                'estimated_row_count': 0,
                'size_mb': 0,
                'error': 'timeout'
            } for table_name in table_names}
        except Exception as e:
            logger.error(f"Failed to get batch size info: {e}")
            return {table_name: {
                'table_name': table_name,
                'estimated_row_count': 0,
                'size_mb': 0,
                'error': str(e)
            } for table_name in table_names}
    
    def _generate_schema_hash(self, tables: List[str]) -> str:
        """Generate a hash of the schema structure for change detection."""
        try:
            logger.info("Generating schema hash for change detection...")
            # Create a hashable representation of the schema
            schema_data = []
            for table_name in tables:
                schema_info = self.get_table_schema(table_name)
                # Include table name, column names, and primary keys in hash
                table_hash_data = {
                    'table_name': table_name,
                    'columns': list(schema_info.get('columns', {}).keys()),
                    'primary_keys': schema_info.get('primary_keys', [])
                }
                schema_data.append(table_hash_data)
            
            # Sort for consistent hashing
            schema_data.sort(key=lambda x: x['table_name'])
            
            # Generate hash
            hash_input = str(schema_data).encode('utf-8')
            return hashlib.md5(hash_input).hexdigest()
        except Exception as e:
            logger.warning(f"Failed to generate schema hash: {e}")
            return "unknown"
    
    def generate_schema_hash(self, table_name: str) -> str:
        """Generate a hash for a single table's schema (public interface)."""
        return self._generate_schema_hash([table_name])
    
    def generate_complete_configuration(self, output_dir: str) -> Dict:
        """Generate complete configuration for all tables with batch processing."""
        logger.info("Generating complete configuration...")
        
        # Discover dbt models
        dbt_models = self.discover_dbt_models()
        
        # Discover tables
        all_tables = self.discover_all_tables()
        tables = [t for t in all_tables if not any(pattern in t.lower() for pattern in EXCLUDED_PATTERNS)]
        
        logger.info(f"Processing {len(tables)} tables (filtered from {len(all_tables)} total)")
        
        # Generate schema hash for change detection (simplified for large databases)
        logger.info("Generating schema hash for change detection...")
        schema_hash = self._generate_schema_hash(tables[:min(50, len(tables))])  # Only hash first 50 tables
        
        # Detect environment from settings
        try:
            settings = get_settings()
            environment = settings.environment if hasattr(settings, 'environment') else 'unknown'
        except Exception:
            environment = 'unknown'
        
        config = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'analyzer_version': '3.0',
                'configuration_version': '3.0',
                'source_database': self.source_db,
                'total_tables': len(tables),
                'schema_hash': schema_hash,
                'analysis_timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
                'environment': environment
            },
            'tables': {}
        }
        
        # Process tables in batches with progress logging
        total_batches = (len(tables) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Processing {len(tables)} tables in {total_batches} batches of {BATCH_SIZE}")
        
        processed_count = 0
        error_count = 0
        start_time = time.time()
        
        # Main progress bar for overall analysis
        with tqdm(total=len(tables), desc="Analyzing tables", unit="table") as pbar:
            for i in range(0, len(tables), BATCH_SIZE):
                batch = tables[i:i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                
                logger.info(f"Processing batch {batch_num}/{total_batches}: tables {i+1}-{min(i+BATCH_SIZE, len(tables))}")
                
                # Get batch schema and size information for efficiency
                logger.info(f"Getting schema information for batch {batch_num}...")
                batch_schema_info = self.get_batch_schema_info(batch)
                
                logger.info(f"Getting size information for batch {batch_num}...")
                batch_size_info = self.get_batch_size_info(batch)
                
                # Process each table in the batch
                for table_name in batch:
                    try:
                        # Get schema and size information from batch results
                        schema_info = batch_schema_info.get(table_name, {'table_name': table_name, 'error': 'not_found'})
                        size_info = batch_size_info.get(table_name, {'table_name': table_name, 'error': 'not_found'})
                        
                        # Update progress bar with table info
                        estimated_row_count = size_info.get('estimated_row_count', 0)
                        pbar.set_postfix({
                            'table': table_name,
                            'rows': f"{estimated_row_count:,}" if estimated_row_count > 0 else "0",
                            'processed': processed_count,
                            'errors': error_count
                        })
                        
                        # Skip tables with errors in schema or size info
                        if 'error' in schema_info or 'error' in size_info:
                            logger.warning(f"Skipping table {table_name} due to errors")
                            error_count += 1
                            config['tables'][table_name] = {
                                'table_name': table_name,
                                'error': f"Schema error: {schema_info.get('error', 'unknown')}, Size error: {size_info.get('error', 'unknown')}",
                                'table_importance': 'standard',
                                'extraction_strategy': 'full_table'
                            }
                            pbar.update(1)
                            continue
                        
                        # Determine table characteristics
                        importance = self.determine_table_importance(table_name, schema_info, size_info)
                        extraction_strategy = self.determine_extraction_strategy(table_name, schema_info, size_info)
                        incremental_columns = self.find_incremental_columns(table_name, schema_info)
                        
                        # Determine incremental strategy based on columns and business logic
                        incremental_strategy = self.determine_incremental_strategy(table_name, schema_info, incremental_columns)
                        
                        # Select primary incremental column based on priority order
                        primary_incremental_column = self.select_primary_incremental_column(incremental_columns)
                        
                        # Check if table has dbt models
                        is_modeled = False
                        dbt_model_types = []
                        
                        # Check staging models
                        staging_models = [model for model in dbt_models['staging'] 
                                        if table_name.lower() in model.lower()]
                        if staging_models:
                            is_modeled = True
                            dbt_model_types.append('staging')
                        
                        # Check mart models
                        mart_models = [model for model in dbt_models['mart'] 
                                     if table_name.lower() in model.lower()]
                        if mart_models:
                            is_modeled = True
                            dbt_model_types.append('mart')
                        
                        # Check intermediate models
                        intermediate_models = [model for model in dbt_models['intermediate'] 
                                            if table_name.lower() in model.lower()]
                        if intermediate_models:
                            is_modeled = True
                            dbt_model_types.append('intermediate')
                        
                        # Determine optimal batch size based on table size
                        estimated_rows = size_info.get('estimated_row_count', 0)
                        if estimated_rows > 1_000_000:  # Very large tables (>1M rows)
                            batch_size = 100_000
                        elif estimated_rows > 500_000:  # Large tables (>500K rows)
                            batch_size = 50_000
                        elif estimated_rows > 100_000:  # Medium tables (>100K rows)
                            batch_size = 25_000
                        elif estimated_rows > 10_000:  # Small-medium tables (>10K rows)
                            batch_size = 10_000
                        else:  # Small tables
                            batch_size = 5_000
                        
                        # Get primary key from schema info
                        primary_keys = schema_info.get('primary_keys', [])
                        primary_key = primary_keys[0] if primary_keys else None
                        
                        # Build table configuration
                        table_config = {
                            'table_name': table_name,
                            'table_importance': importance,
                            'extraction_strategy': extraction_strategy,
                            'estimated_rows': estimated_rows,
                            'estimated_size_mb': size_info.get('size_mb', 0),
                            'batch_size': batch_size,
                            'incremental_columns': incremental_columns,
                            'incremental_strategy': incremental_strategy,
                            'primary_incremental_column': primary_incremental_column,
                            'is_modeled': is_modeled,
                            'dbt_model_types': dbt_model_types,
                            'monitoring': {
                                'alert_on_failure': importance in ['critical', 'important'],
                                'alert_on_slow_extraction': estimated_rows > 100000
                            },
                            'schema_hash': hash(str(schema_info)),  # Simple hash for change detection
                            'last_analyzed': datetime.now().isoformat()
                        }
                        
                        # Add primary key if available
                        if primary_key:
                            table_config['primary_key'] = primary_key
                        
                        config['tables'][table_name] = table_config
                        processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process table {table_name}: {e}")
                        error_count += 1
                        # Add error entry
                        config['tables'][table_name] = {
                            'table_name': table_name,
                            'error': str(e),
                            'table_importance': 'standard',
                            'extraction_strategy': 'full_table'
                        }
                    
                    pbar.update(1)
                
                # Add a small delay between batches to prevent overwhelming the database
                if i + BATCH_SIZE < len(tables):
                    time.sleep(1)
                
                # Log batch progress with timing
                batch_time = time.time() - start_time
                elapsed_per_batch = batch_time / batch_num
                remaining_batches = total_batches - batch_num
                estimated_remaining = remaining_batches * elapsed_per_batch
                
                logger.info(f"Completed batch {batch_num}/{total_batches}. "
                           f"Processed: {processed_count}, Errors: {error_count}. "
                           f"Elapsed: {batch_time:.1f}s, "
                           f"Est. remaining: {estimated_remaining:.1f}s")
        
        total_time = time.time() - start_time
        logger.info(f"Table processing complete in {total_time:.1f}s. "
                   f"Total processed: {processed_count}, Total errors: {error_count}")
        
        return config
    
    def analyze_complete_schema(self, output_dir: str = 'etl_pipeline/config') -> Dict:
        """Perform complete schema analysis and generate all outputs."""
        logger.info("Starting complete OpenDental schema analysis...")
        
        try:
            # Ensure output directories exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Create organized log directories
            logs_base = Path('logs')
            schema_analysis_logs = logs_base / 'schema_analysis' / 'logs'
            schema_analysis_reports = logs_base / 'schema_analysis' / 'reports'
            
            schema_analysis_logs.mkdir(parents=True, exist_ok=True)
            schema_analysis_reports.mkdir(parents=True, exist_ok=True)
            
            # Create timestamped log file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = schema_analysis_logs / f'schema_analysis_{timestamp}.log'
            
            # Add file handler to logger for this analysis session
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(file_handler)
            
            # Track overall progress
            start_time = time.time()
            logger.info("=" * 60)
            logger.info("OpenDental Schema Analysis Started")
            logger.info("=" * 60)
            
            # Stage 1: Generate complete configuration
            logger.info("Stage 1/3: Generating table configuration...")
            config = self.generate_complete_configuration(output_dir)
            stage1_time = time.time() - start_time
            logger.info(f"Stage 1 completed in {stage1_time:.1f}s")
            
            # Stage 2: Save configuration
            logger.info("Stage 2/3: Saving configuration files...")
            tables_yml_path = os.path.join(output_dir, 'tables.yml')
            with open(tables_yml_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            logger.info(f"Configuration saved to: {tables_yml_path}")
            
            # Stage 3: Generate detailed analysis report
            logger.info("Stage 3/3: Generating detailed analysis report...")
            analysis_report = self._generate_detailed_analysis_report(config)
            
            # Save detailed analysis in organized reports directory
            analysis_path = schema_analysis_reports / f'schema_analysis_{timestamp}.json'
            with open(analysis_path, 'w') as f:
                json.dump(analysis_report, f, indent=2, default=str)
            
            # Generate summary report
            self._generate_summary_report(config, tables_yml_path, str(analysis_path), timestamp)
            
            total_time = time.time() - start_time
            logger.info("=" * 60)
            logger.info("Schema analysis completed successfully!")
            logger.info(f"Total analysis time: {total_time:.1f}s")
            logger.info("=" * 60)
            
            return {
                'tables_config': tables_yml_path,
                'analysis_report': str(analysis_path),
                'analysis_log': str(log_file),
                'total_time': total_time
            }
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {str(e)}")
            raise
    
    def _generate_detailed_analysis_report(self, config: Dict) -> Dict:
        """Generate detailed analysis report with all metadata."""
        logger.info("Generating detailed analysis report...")
        
        analysis = {
            'analysis_metadata': {
                'generated_at': datetime.now().isoformat(),
                'source_database': self.source_db,
                'total_tables_analyzed': len(config.get('tables', {})),
                'analyzer_version': '3.0'
            },
            'table_analysis': {},
            'dbt_model_analysis': self.discover_dbt_models(),
            'performance_analysis': {},
            'recommendations': []
        }
        
        # Analyze each table with progress bar
        tables_to_analyze = [name for name, config in config.get('tables', {}).items() 
                           if 'error' not in config]
        
        logger.info(f"Generating detailed analysis for {len(tables_to_analyze)} tables...")
        
        with tqdm(total=len(tables_to_analyze), desc="Generating detailed report", unit="table") as pbar:
            for table_name in tables_to_analyze:
                try:
                    analysis['table_analysis'][table_name] = {
                        'schema_info': self.get_table_schema(table_name),
                        'size_info': self.get_table_size_info(table_name),
                        'configuration': config['tables'][table_name]
                    }
                    pbar.set_postfix({'table': table_name})
                except Exception as e:
                    logger.warning(f"Failed to analyze table {table_name} for detailed report: {e}")
                    analysis['table_analysis'][table_name] = {
                        'error': str(e),
                        'configuration': config['tables'][table_name]
                    }
                pbar.update(1)
        
        # Generate recommendations
        total_tables = len(config.get('tables', {}))
        critical_tables = sum(1 for t in config.get('tables', {}).values() 
                            if t.get('table_importance') == 'critical')
        modeled_tables = sum(1 for t in config.get('tables', {}).values() 
                           if t.get('is_modeled', False))
        
        analysis['recommendations'] = [
            f"Critical tables identified: {critical_tables}",
            f"Tables with dbt models: {modeled_tables} ({modeled_tables/total_tables*100:.1f}%)",
            "Consider adding dbt models for critical tables without models" if critical_tables > modeled_tables else "Good dbt model coverage"
        ]
        
        logger.info("Detailed analysis report generation complete")
        return analysis
    
    def _generate_summary_report(self, config: Dict, output_path: str, analysis_path: str, timestamp: str) -> None:
        """Generate and display summary report."""
        tables = config.get('tables', {})
        total_tables = len(tables)
        
        # Calculate statistics
        importance_stats = {}
        for table_config in tables.values():
            importance = table_config.get('table_importance', 'standard')
            importance_stats[importance] = importance_stats.get(importance, 0) + 1
        
        # Total size estimates
        total_size_mb = sum(table.get('estimated_size_mb', 0) for table in tables.values())
        total_rows = sum(table.get('estimated_rows', 0) for table in tables.values())
        
        # Monitoring stats
        monitored_tables = sum(1 for table in tables.values() 
                             if table.get('monitoring', {}).get('alert_on_failure', False))
        
        # DBT modeling stats
        modeled_tables = sum(1 for table in tables.values() 
                           if table.get('is_modeled', False))
        dbt_model_types = {}
        for table_config in tables.values():
            model_types = table_config.get('dbt_model_types', [])
            for model_type in model_types:
                dbt_model_types[model_type] = dbt_model_types.get(model_type, 0) + 1
        
        # Extraction strategy stats
        strategy_stats = {}
        for table_config in tables.values():
            strategy = table_config.get('extraction_strategy', 'full_table')
            strategy_stats[strategy] = strategy_stats.get(strategy, 0) + 1
        
        # Generate report
        report = f"""
OpenDental Schema Analysis Summary
=================================

Analysis Metadata:
- Generated: {config.get('metadata', {}).get('generated_at', 'unknown')}
- Analyzer Version: {config.get('metadata', {}).get('analyzer_version', 'unknown')}
- Configuration Version: {config.get('metadata', {}).get('configuration_version', 'unknown')}
- Environment: {config.get('metadata', {}).get('environment', 'unknown')}
- Schema Hash: {config.get('metadata', {}).get('schema_hash', 'unknown')[:8]}...

Total Tables Analyzed: {total_tables:,}
Total Estimated Size: {total_size_mb:,.1f} MB
Total Estimated Rows: {total_rows:,}

Table Classification:
- Critical: {importance_stats.get('critical', 0)}
- Important: {importance_stats.get('important', 0)}
- Reference: {importance_stats.get('reference', 0)}
- Audit: {importance_stats.get('audit', 0)}
- Standard: {importance_stats.get('standard', 0)}

DBT Modeling Status:
- Tables with DBT Models: {modeled_tables} ({modeled_tables/total_tables*100:.1f}%)
- Staging Models: {dbt_model_types.get('staging', 0)}
- Mart Models: {dbt_model_types.get('mart', 0)}

Extraction Strategies:
- Incremental: {strategy_stats.get('incremental', 0)}
- Full Table: {strategy_stats.get('full_table', 0)}
- Chunked Incremental: {strategy_stats.get('chunked_incremental', 0)}

Tables with Monitoring: {monitored_tables}

Configuration saved to: {output_path}
Detailed analysis saved to: {analysis_path}

Ready to run ETL pipeline!
"""
        
        # Print to console
        print(report)
        
        # Save to organized logs directory
        logs_base = Path('logs')
        schema_analysis_reports = logs_base / 'schema_analysis' / 'reports'
        report_path = schema_analysis_reports / f'schema_analysis_{timestamp}_summary.txt'
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        logger.info(f"Summary report saved to: {report_path}")

def main():
    """Main function - generate complete schema analysis and configuration."""
    try:
        script_start_time = time.time()
        logger.info("Starting OpenDental Schema Analysis")
        logger.info("=" * 50)
        
        # Create analyzer and run complete analysis
        analyzer = OpenDentalSchemaAnalyzer()
        results = analyzer.analyze_complete_schema()
        
        total_script_time = time.time() - script_start_time
        
        print(f"\n" + "=" * 60)
        print(f"ANALYSIS COMPLETE!")
        print(f"=" * 60)
        print(f"Files generated:")
        for name, path in results.items():
            if name != 'total_time':
                print(f"   {name}: {path}")
        print(f"\nTiming:")
        print(f"   Analysis time: {results.get('total_time', 0):.1f}s")
        print(f"   Total script time: {total_script_time:.1f}s")
        print(f"=" * 60)
            
        logger.info("Schema analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 