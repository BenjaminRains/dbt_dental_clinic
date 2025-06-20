"""
Table Processor

Handles the processing of individual tables through the ETL pipeline,
including extraction, loading, and transformation.

STATUS: ACTIVE - Core ETL Implementation (COMPLEX)
=================================================

This module is the ACTIVE core implementation of individual table ETL processing,
serving as the workhorse of the pipeline. It's actively used by both PipelineOrchestrator
and PriorityProcessor, making it the central component for table-level operations.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by orchestrator and priority processor
- ✅ COMPLETE ETL PIPELINE: Handles extract, load, and transform phases
- ✅ INTELLIGENT PROCESSING: Supports incremental and full refresh modes
- ✅ SCHEMA MANAGEMENT: Handles schema changes and table creation
- ✅ CHUNKED LOADING: Optimizes large table processing
- ❌ COMPLEX: Over-engineered with multiple abstraction layers
- ❌ HEAVY DEPENDENCIES: Relies on many components and configurations
- ❌ UNTESTED: Limited integration testing with real data
- ❌ CONFIGURATION HEAVY: Requires extensive configuration for each table

ACTIVE USAGE:
- PipelineOrchestrator: Calls process_table for individual table processing
- PriorityProcessor: Uses for parallel and sequential table processing
- CLI Commands: Indirectly used through orchestrator
- Integration Tests: Referenced in test files

COMPLEXITY ANALYSIS:
1. MULTIPLE ABSTRACTION LAYERS:
   - TableProcessor (orchestration)
   - ExactMySQLReplicator (extraction)
   - PostgresLoader (loading)
   - RawToPublicTransformer (transformation)
   - PostgresSchema (schema management)

2. CONFIGURATION DEPENDENCIES:
   - Settings class for table configuration
   - Database connection configurations
   - Table-specific settings (batch_size, incremental_column, etc.)
   - Schema and transformation rules

3. PROCESSING PHASES:
   - Extract: MySQL replication with schema validation
   - Load: PostgreSQL loading with chunked processing
   - Transform: Raw-to-public schema transformation

DEPENDENCIES:
- ExactMySQLReplicator: MySQL-to-MySQL replication
- PostgresLoader: MySQL-to-PostgreSQL loading
- RawToPublicTransformer: Schema transformation
- PostgresSchema: Schema management and conversion
- Settings: Configuration management
- ConnectionFactory: Database connections
- MetricsCollector: Basic metrics collection

INTEGRATION POINTS:
- PipelineOrchestrator: Main orchestration integration
- PriorityProcessor: Batch processing integration
- Database Connections: Manages multiple database connections
- Configuration: Uses Settings for table-specific configuration
- Metrics: Integrates with basic metrics collection

CRITICAL ISSUES:
1. OVER-ENGINEERED: Too many abstraction layers and components
2. COMPLEXITY: Difficult to understand and maintain
3. DEPENDENCY HEAVY: Relies on many components that may not be fully tested
4. CONFIGURATION COMPLEXITY: Requires extensive configuration for each table
5. UNTESTED: Limited testing with real data and edge cases
6. ERROR HANDLING: Basic error handling may not cover all scenarios

DEVELOPMENT NEEDS:
1. SIMPLIFICATION: Reduce complexity and abstraction layers
2. COMPREHENSIVE TESTING: Add integration tests with real data
3. ERROR HANDLING: Enhance error handling and recovery
4. PERFORMANCE OPTIMIZATION: Optimize chunked loading and processing
5. CONFIGURATION SIMPLIFICATION: Reduce configuration complexity
6. MONITORING: Add detailed monitoring and metrics

TESTING REQUIREMENTS:
1. INTEGRATION TESTS: Test complete ETL flow with real data
2. SCHEMA CHANGE TESTS: Test schema change detection and handling
3. INCREMENTAL TESTS: Test incremental vs full refresh logic
4. CHUNKED LOADING TESTS: Test large table processing
5. ERROR SCENARIOS: Test failure handling and recovery
6. PERFORMANCE TESTS: Test processing time and resource usage

This component is the core of the ETL pipeline but suffers from over-engineering
and complexity. It needs simplification and comprehensive testing before production.
"""

import logging
from typing import Dict, Optional
from datetime import datetime
import time
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.metrics import MetricsCollector
from ..core.postgres_schema import PostgresSchema
from ..transformers.raw_to_public import RawToPublicTransformer
from ..mysql_replicator import ExactMySQLReplicator
from ..config.settings import Settings
from ..core.connections import ConnectionFactory

logger = logging.getLogger(__name__)

class TableProcessor:
    def __init__(self, config_path: str = None):
        """
        Initialize the table processor.
        
        Args:
            config_path: Path to the configuration file
        """
        self.settings = Settings()
        self.config_path = config_path
        self.metrics = MetricsCollector()
        
        # Connection state
        self.opendental_source_engine = None
        self.mysql_replication_engine = None
        self.postgres_analytics_engine = None
        self.raw_to_public_transformer = None
        
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
        
        COMPLEXITY WARNING: This method orchestrates a complex ETL pipeline with
        multiple abstraction layers and dependencies. It demonstrates over-engineering
        with separate components for each phase:
        
        1. EXTRACT PHASE: Uses ExactMySQLReplicator for MySQL-to-MySQL replication
        2. LOAD PHASE: Uses PostgresLoader for MySQL-to-PostgreSQL loading
        3. TRANSFORM PHASE: Uses RawToPublicTransformer for schema transformation
        
        Each phase has its own configuration, error handling, and optimization logic,
        making this method difficult to understand, test, and maintain.
        
        SIMPLIFICATION NEEDED: Consider consolidating phases or reducing abstraction
        layers to improve maintainability and reduce complexity.
        
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
        table_config = self.settings.get_table_config(table_name)
        
        try:
            logger.info(f"Starting ETL pipeline for table: {table_name}")
            
            # 1. Extract to replication
            logger.info(f"Starting extraction phase for table: {table_name}")
            extract_success = self._extract_to_replication(table_name, force_full)
            if not extract_success:
                logger.error(f"Extraction failed for table: {table_name}")
                return False
                
            # 2. Load to raw schema
            logger.info(f"Starting load phase for table: {table_name}")
            is_incremental = self.settings.should_use_incremental(table_name) and not force_full
            load_success = self._load_to_analytics(table_name, is_incremental)
            if not load_success:
                logger.error(f"Load to raw schema failed for table: {table_name}")
                return False
                
            # 3. Transform to public schema
            logger.info(f"Starting raw-to-public transformation for table: {table_name}")
            transform_success = self.raw_to_public_transformer.transform_table(
                table_name,
                is_incremental=is_incremental
            )
            if not transform_success:
                logger.error(f"Raw-to-public transformation failed for table: {table_name}")
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
            # Get database names from settings
            source_db = self.settings.get_database_config('source')['database']
            target_db = self.settings.get_database_config('replication')['database']
            
            # Initialize MySQL replicator with database names
            replicator = ExactMySQLReplicator(
                source_engine=self.opendental_source_engine,
                target_engine=self.mysql_replication_engine,
                source_db=source_db,
                target_db=target_db
            )
            
            # Check if table exists in target
            table_exists = replicator.get_exact_table_schema(table_name, self.mysql_replication_engine) is not None
            
            # Check if schema has changed
            schema_changed = replicator.verify_exact_replica(table_name) if table_exists else True
            
            if schema_changed:
                logger.info(f"Schema change detected for {table_name}, forcing full extraction")
                force_full = True
            
            # Determine extraction strategy
            use_incremental = (
                table_exists and 
                not schema_changed and 
                self.settings.should_use_incremental(table_name) and 
                not force_full
            )
            
            # Create or recreate table if needed
            if not table_exists or schema_changed:
                success = replicator.create_exact_replica(table_name)
                if not success:
                    logger.error(f"Failed to create table {table_name}")
                    return False
            
            # Copy data
            success = replicator.copy_table_data(table_name)
            
            if not success:
                logger.error(f"Extraction failed for {table_name}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            return False
            
    def _load_to_analytics(self, table_name: str, is_incremental: bool) -> bool:
        """
        Load data from replication to analytics database.
        
        COMPLEXITY WARNING: This method demonstrates over-engineering with:
        1. Multiple configuration lookups (table_config, batch_size, estimated_size_mb)
        2. Conditional logic for chunked vs standard loading
        3. Schema management through PostgresSchema
        4. Multiple abstraction layers (PostgresLoader, schema_adapter)
        
        The chunked loading logic adds complexity without clear benefits for most tables.
        Consider simplifying to use standard loading for all tables unless proven
        performance issues exist with large tables.
        """
        try:
            # Get database names from settings
            mysql_db = self.settings.get_database_config('replication')['database']
            postgres_db = self.settings.get_database_config('analytics')['database']
            
            # Get schema information
            schema_adapter = PostgresSchema(
                mysql_engine=self.mysql_replication_engine,
                postgres_engine=self.postgres_analytics_engine,
                mysql_db=mysql_db,
                postgres_db=postgres_db
            )
            
            # Get MySQL schema
            mysql_schema = {
                'columns': schema_adapter.mysql_inspector.get_columns(table_name),
                'primary_key': schema_adapter.mysql_inspector.get_pk_constraint(table_name),
                'foreign_keys': schema_adapter.mysql_inspector.get_foreign_keys(table_name),
                'indexes': schema_adapter.mysql_inspector.get_indexes(table_name)
            }
            
            # Initialize loader
            from ..loaders.postgres_loader import PostgresLoader
            loader = PostgresLoader(
                replication_engine=self.mysql_replication_engine,
                analytics_engine=self.postgres_analytics_engine
            )
            
            # Get table configuration
            table_config = self.settings.get_table_config(table_name)
            batch_size = table_config.get('batch_size', 5000)
            
            # Determine if we should use chunked loading
            estimated_size_mb = table_config.get('estimated_size_mb', 0)
            estimated_rows = table_config.get('estimated_rows', 0)
            use_chunked = estimated_size_mb > 100 or estimated_rows > 1000000
            
            if use_chunked:
                logger.info(f"Using chunked loading for {table_name}")
                success = loader.load_table_chunked(
                    table_name=table_name,
                    mysql_schema=mysql_schema,
                    force_full=not is_incremental,
                    chunk_size=batch_size
                )
            else:
                logger.info(f"Using standard loading for {table_name}")
                success = loader.load_table(
                    table_name=table_name,
                    mysql_schema=mysql_schema,
                    force_full=not is_incremental
                )
            
            if not success:
                logger.error(f"Failed to load table {table_name}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error during loading: {str(e)}")
            return False 