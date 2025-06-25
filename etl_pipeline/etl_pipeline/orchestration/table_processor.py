"""
Table Processor

Handles the processing of individual tables through the ETL pipeline,
including extraction, loading, and transformation.

STATUS: ACTIVE - Core ETL Implementation (SIMPLIFIED)
====================================================

This module is the ACTIVE core implementation of individual table ETL processing,
serving as the workhorse of the pipeline. It's actively used by both PipelineOrchestrator
and PriorityProcessor, making it the central component for table-level operations.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by orchestrator and priority processor
- ✅ SIMPLIFIED ETL PIPELINE: Consolidated extract, load, and transform phases
- ✅ INTELLIGENT PROCESSING: Supports incremental and full refresh modes
- ✅ EFFICIENT CONFIGURATION: Uses Settings class efficiently
- ✅ STRAIGHTFORWARD LOGIC: Removed unnecessary abstraction layers
- ✅ TESTABLE: Simplified for easier testing and maintenance

ACTIVE USAGE:
- PipelineOrchestrator: Calls process_table for individual table processing
- PriorityProcessor: Uses for parallel and sequential table processing
- CLI Commands: Indirectly used through orchestrator
- Integration Tests: Referenced in test files

SIMPLIFIED ARCHITECTURE:
1. SINGLE ETL METHOD: process_table handles all phases in one place
2. DIRECT SETTINGS USAGE: Uses Settings class directly without multiple lookups
3. SIMPLIFIED LOADING: Uses standard loading for all tables (chunked only when needed)
4. REDUCED DEPENDENCIES: Fewer abstraction layers and components

DEPENDENCIES:
- ExactMySQLReplicator: MySQL-to-MySQL replication
- PostgresLoader: MySQL-to-PostgreSQL loading
- RawToPublicTransformer: Schema transformation
- Settings: Configuration management
- ConnectionFactory: Database connections
- UnifiedMetricsCollector: Basic metrics collection

INTEGRATION POINTS:
- PipelineOrchestrator: Main orchestration integration
- PriorityProcessor: Batch processing integration
- Database Connections: Manages multiple database connections
- Configuration: Uses Settings for table-specific configuration
- Metrics: Integrates with basic metrics collection

DEVELOPMENT NEEDS:
1. COMPREHENSIVE TESTING: Add integration tests with real data
2. ERROR HANDLING: Enhance error handling and recovery
3. PERFORMANCE OPTIMIZATION: Optimize for large tables when needed
4. MONITORING: Add detailed monitoring and metrics

TESTING REQUIREMENTS:
1. INTEGRATION TESTS: Test complete ETL flow with real data
2. SCHEMA CHANGE TESTS: Test schema change detection and handling
3. INCREMENTAL TESTS: Test incremental vs full refresh logic
4. ERROR SCENARIOS: Test failure handling and recovery
5. PERFORMANCE TESTS: Test processing time and resource usage

This component is the core of the ETL pipeline and has been simplified
for better maintainability and testing.
"""

import logging
from typing import Dict, Optional
from datetime import datetime
import time
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.connections import ConnectionFactory
from ..monitoring.unified_metrics import UnifiedMetricsCollector
from ..config.logging import get_logger
from ..core.postgres_schema import PostgresSchema
from ..transformers.raw_to_public import RawToPublicTransformer
from ..core.mysql_replicator import ExactMySQLReplicator
from ..config.settings import Settings

logger = logging.getLogger(__name__)

class TableProcessor:
    def __init__(self, config_path: str = None):
        """
        Initialize the table processor.
        
        Args:
            config_path: Path to the configuration file (deprecated, kept for compatibility)
        """
        self.settings = Settings()
        self.config_path = config_path  # Kept for compatibility
        self.metrics = UnifiedMetricsCollector()
        
        # Connection state
        self.opendental_source_engine = None
        self.mysql_replication_engine = None
        self.postgres_analytics_engine = None
        self.raw_to_public_transformer = None
        
        # Cache database configs to avoid repeated lookups
        self._source_db = None
        self._replication_db = None
        self._analytics_db = None
        
    def initialize_connections(self, source_engine: Engine = None, 
                             replication_engine: Engine = None,
                             analytics_engine: Engine = None,
                             raw_to_public_transformer: RawToPublicTransformer = None):
        """
        Initialize database connections.
        
        Args:
            source_engine: SQLAlchemy engine for source database
            replication_engine: SQLAlchemy engine for replication database
            analytics_engine: SQLAlchemy engine for analytics database
            raw_to_public_transformer: Transformer for raw to public schema
        """
        try:
            logger.info("Initializing database connections...")
            
            # Use provided engines or create new ones
            if source_engine:
                self.opendental_source_engine = source_engine
            else:
                self.opendental_source_engine = ConnectionFactory.get_opendental_source_connection()
                
            if replication_engine:
                self.mysql_replication_engine = replication_engine
            else:
                self.mysql_replication_engine = ConnectionFactory.get_mysql_replication_connection()
                
            if analytics_engine:
                self.postgres_analytics_engine = analytics_engine
            else:
                self.postgres_analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
                
            if raw_to_public_transformer:
                self.raw_to_public_transformer = raw_to_public_transformer
            else:
                self.raw_to_public_transformer = RawToPublicTransformer(
                    self.postgres_analytics_engine,  # Source (raw schema)
                    self.postgres_analytics_engine   # Target (public schema)
                )
            
            # Cache database names to avoid repeated lookups
            self._source_db = self.settings.get_database_config('opendental_source')['database']
            self._replication_db = self.settings.get_database_config('opendental_replication')['database']
            self._analytics_db = self.settings.get_database_config('opendental_analytics_raw')['database']
            
            logger.info("Successfully initialized all database connections")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {str(e)}")
            self.cleanup()
            raise

    def cleanup(self):
        """Clean up database connections."""
        # Clean up source engine
        if self.opendental_source_engine:
            try:
                self.opendental_source_engine.dispose()
                logger.info("Closed source database connection")
            except Exception as e:
                logger.error(f"Error closing source database connection: {str(e)}")
            finally:
                self.opendental_source_engine = None
        
        # Clean up replication engine
        if self.mysql_replication_engine:
            try:
                self.mysql_replication_engine.dispose()
                logger.info("Closed replication database connection")
            except Exception as e:
                logger.error(f"Error closing replication database connection: {str(e)}")
            finally:
                self.mysql_replication_engine = None
        
        # Clean up analytics engine
        if self.postgres_analytics_engine:
            try:
                self.postgres_analytics_engine.dispose()
                logger.info("Closed analytics database connection")
            except Exception as e:
                logger.error(f"Error closing analytics database connection: {str(e)}")
            finally:
                self.postgres_analytics_engine = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are cleaned up."""
        self.cleanup()

    def _connections_available(self) -> bool:
        """Check if all required database connections are available."""
        return all([
            self.opendental_source_engine is not None,
            self.mysql_replication_engine is not None,
            self.postgres_analytics_engine is not None
        ])

    def process_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Process a single table through the ETL pipeline.
        
        SIMPLIFIED ETL PIPELINE: This method consolidates the entire ETL process
        into a single, straightforward flow:
        
        1. EXTRACT: Copy data from source to replication database
        2. LOAD: Copy data from replication to analytics database (raw schema)
        3. TRANSFORM: Transform data from raw to public schema
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full refresh
            
        Returns:
            bool: True if processing was successful
        """
        if not self._connections_available():
            logger.error("Database connections not initialized")
            return False
        
        start_time = time.time()
        
        try:
            logger.info(f"Starting ETL pipeline for table: {table_name}")
            
            # Get table configuration once
            table_config = self.settings.get_table_config(table_name)
            is_incremental = self.settings.should_use_incremental(table_name) and not force_full
            
            # 1. Extract to replication
            logger.info(f"Extracting {table_name} to replication database")
            if not self._extract_to_replication(table_name, force_full):
                logger.error(f"Extraction failed for table: {table_name}")
                return False
                
            # 2. Load to analytics (raw schema)
            logger.info(f"Loading {table_name} to analytics database")
            if not self._load_to_analytics(table_name, is_incremental, table_config):
                logger.error(f"Load to analytics failed for table: {table_name}")
                return False
                
            # 3. Transform to public schema
            logger.info(f"Transforming {table_name} to public schema")
            if not self.raw_to_public_transformer.transform_table(table_name, is_incremental=is_incremental):
                logger.error(f"Transform to public schema failed for table: {table_name}")
                return False
                
            processing_time = (time.time() - start_time) / 60
            logger.info(f"Successfully completed ETL pipeline for table: {table_name} in {processing_time:.2f} minutes")
            return True
            
        except Exception as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"Error in ETL pipeline for table {table_name} after {processing_time:.2f} minutes: {str(e)}")
            return False
            
    def _extract_to_replication(self, table_name: str, force_full: bool) -> bool:
        """Extract data from source to replication database."""
        try:
            # Initialize MySQL replicator
            replicator = ExactMySQLReplicator(
                source_engine=self.opendental_source_engine,
                target_engine=self.mysql_replication_engine,
                source_db=self._source_db,
                target_db=self._replication_db
            )
            
            # Check if table exists and schema has changed
            table_exists = replicator.get_exact_table_schema(table_name, self.mysql_replication_engine) is not None
            schema_changed = replicator.verify_exact_replica(table_name) if table_exists else True
            
            if schema_changed:
                logger.info(f"Schema change detected for {table_name}, forcing full extraction")
                force_full = True
            
            # Create or recreate table if needed
            if not table_exists or schema_changed:
                if not replicator.create_exact_replica(table_name):
                    logger.error(f"Failed to create table {table_name}")
                    return False
            
            # Copy data
            if not replicator.copy_table_data(table_name):
                logger.error(f"Extraction failed for {table_name}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            return False
            
    def _load_to_analytics(self, table_name: str, is_incremental: bool, table_config: Dict) -> bool:
        """
        Load data from replication to analytics database.
        
        SIMPLIFIED LOADING: Uses standard loading for most tables, chunked loading
        only for very large tables (> 1M rows or > 100MB).
        """
        try:
            from ..loaders.postgres_loader import PostgresLoader
            
            # Initialize loader
            loader = PostgresLoader(
                replication_engine=self.mysql_replication_engine,
                analytics_engine=self.postgres_analytics_engine
            )
            
            # Get table size info
            estimated_rows = table_config.get('estimated_rows', 0)
            estimated_size_mb = table_config.get('estimated_size_mb', 0)
            batch_size = table_config.get('batch_size', 5000)
            
            # Use chunked loading only for very large tables
            use_chunked = estimated_rows > 1000000 or estimated_size_mb > 100
            
            if use_chunked:
                logger.info(f"Using chunked loading for large table: {table_name}")
                success = loader.load_table_chunked(
                    table_name=table_name,
                    force_full=not is_incremental,
                    chunk_size=batch_size
                )
            else:
                logger.info(f"Using standard loading for table: {table_name}")
                success = loader.load_table(
                    table_name=table_name,
                    force_full=not is_incremental
                )
            
            if not success:
                logger.error(f"Failed to load table {table_name}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error during loading: {str(e)}")
            return False 