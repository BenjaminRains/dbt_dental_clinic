"""
Pipeline Orchestrator

Handles the core ETL pipeline orchestration, coordinating between different components
and managing the overall flow of data through the pipeline.

STATUS: ACTIVE - Core Orchestration Implementation
=================================================

This is the current active implementation of the ETL pipeline orchestration,
serving as the main coordinator for the entire data pipeline. It's actively used
by CLI commands and represents the refactored architecture.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by CLI commands and main pipeline
- ✅ WELL-STRUCTURED: Clear separation of concerns with dedicated components
- ✅ CONNECTION MANAGEMENT: Proper database connection lifecycle management
- ✅ CONTEXT MANAGER: Implements __enter__/__exit__ for resource cleanup
- ✅ ERROR HANDLING: Comprehensive exception handling and cleanup
- ❌ UNTESTED: No comprehensive integration tests
- ❌ DEPENDENCIES: Relies on multiple components that need validation
- ❌ CONFIGURATION: Config path parameter not fully utilized
- ❌ METRICS: Uses basic metrics collector, not enhanced version

ACTIVE DEPENDENCIES:
- TableProcessor: Handles individual table processing
- PriorityProcessor: Manages table processing by priority
- RawToPublicTransformer: Transforms data between schemas
- ExactMySQLReplicator: Replicates data from source to replication
- ConnectionFactory: Manages database connections
- Settings: Configuration management
- MetricsCollector: Unified metrics collection with persistence

ARCHITECTURE OVERVIEW:
1. Connection Initialization: Sets up all database connections
2. Component Coordination: Orchestrates TableProcessor and PriorityProcessor
3. Pipeline Execution: Runs complete ETL for single tables or by priority
4. Resource Management: Ensures proper cleanup of connections

INTEGRATION POINTS:
- CLI Commands: Used by cli/commands.py for pipeline execution
- Database Connections: Manages source, replication, and analytics connections
- Table Processing: Delegates to TableProcessor for individual table ETL
- Priority Processing: Uses PriorityProcessor for batch processing
- Metrics Collection: Integrates with basic metrics collector

TESTING NEEDS:
1. INTEGRATION TESTS: Test complete pipeline flow with real data
2. CONNECTION TESTS: Validate database connection management
3. ERROR HANDLING TESTS: Test failure scenarios and cleanup
4. PERFORMANCE TESTS: Test parallel processing capabilities
5. CONFIGURATION TESTS: Test different configuration scenarios

REVIEW AREAS:
1. CONFIGURATION: Config path parameter usage and validation
2. ERROR RECOVERY: Robustness of error handling and recovery
3. RESOURCE MANAGEMENT: Memory and connection pool management
4. METRICS INTEGRATION: Upgrade to enhanced metrics collector
5. LOGGING: Comprehensive logging for debugging and monitoring
6. VALIDATION: Input validation and configuration validation
7. PERFORMANCE: Optimization opportunities in parallel processing

DEVELOPMENT PRIORITIES:
1. COMPREHENSIVE TESTING: Add integration and unit tests
2. METRICS ENHANCEMENT: Integrate with advanced metrics collector
3. CONFIGURATION VALIDATION: Add proper config validation
4. ERROR RECOVERY: Enhance error handling and recovery mechanisms
5. PERFORMANCE OPTIMIZATION: Optimize parallel processing
6. MONITORING: Add comprehensive monitoring and alerting

This orchestrator represents the core of the ETL pipeline and needs
thorough testing and validation before production deployment.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.connections import ConnectionFactory
from ..transformers.raw_to_public import RawToPublicTransformer
from ..core.mysql_replicator import ExactMySQLReplicator
from ..monitoring.unified_metrics import UnifiedMetricsCollector
from ..config.logging import get_logger
from ..core.postgres_schema import PostgresSchema
from .table_processor import TableProcessor
from .priority_processor import PriorityProcessor
from ..config.settings import Settings

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, config_path: str = None):
        """
        Initialize the pipeline orchestrator.
        
        CONFIGURATION ISSUE: The config_path parameter is accepted but not fully utilized.
        The Settings() instance is created without using the config_path, which means
        custom configuration files are not being loaded. This should be fixed to:
        1. Load custom configuration from config_path if provided
        2. Fall back to default configuration if not provided
        3. Validate configuration before proceeding
        
        Args:
            config_path: Path to the configuration file (NOT FULLY UTILIZED)
        """
        self.config_path = config_path
        self.settings = Settings()  # TODO: Use config_path parameter
        self.table_processor = TableProcessor(config_path)
        self.priority_processor = PriorityProcessor()
        
        # Initialize connection state
        self.opendental_source_engine = None
        self.mysql_replication_engine = None
        self.postgres_analytics_engine = None
        self.raw_to_public_transformer = None
        
        # Initialize metrics (will be updated with analytics engine after connections)
        self.metrics = UnifiedMetricsCollector()
        
        # Track initialization state
        self.connections_initialized = False
        
    def initialize_connections(self) -> bool:
        """
        Initialize all database connections and components.
        
        TESTING NEEDED: This method needs comprehensive testing including:
        1. Success scenarios with valid database configurations
        2. Failure scenarios with invalid database configurations
        3. Connection timeout scenarios
        4. Resource cleanup on initialization failure
        5. Validation of all component initializations
        6. Performance testing with different connection pool sizes
        
        CRITICAL PATH: This method is called before any pipeline execution,
        so failures here will prevent the entire pipeline from running.
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Initialize database connections
            self.opendental_source_engine = ConnectionFactory.get_opendental_source_connection()
            self.mysql_replication_engine = ConnectionFactory.get_mysql_replication_connection()
            self.postgres_analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
            
            # Get database names from settings
            source_db = self.settings.get_database_config('source')['database']
            target_db = self.settings.get_database_config('replication')['database']
            
            # Initialize MySQL replicator with database names
            self.mysql_replicator = ExactMySQLReplicator(
                source_engine=self.opendental_source_engine,
                target_engine=self.mysql_replication_engine,
                source_db=source_db,
                target_db=target_db
            )
            
            # Initialize raw-to-public transformer
            self.raw_to_public_transformer = RawToPublicTransformer(
                self.postgres_analytics_engine,  # Source (raw schema)
                self.postgres_analytics_engine   # Target (public schema)
            )
            
            # Initialize table processor with connections
            self.table_processor.initialize_connections(
                self.opendental_source_engine,
                self.mysql_replication_engine,
                self.postgres_analytics_engine,
                self.raw_to_public_transformer
            )
            
            self.connections_initialized = True
            logger.info("Successfully initialized all connections and components")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {str(e)}")
            self.cleanup()
            raise
            
    def cleanup(self):
        """Clean up all resources."""
        try:
            if self.opendental_source_engine:
                self.opendental_source_engine.dispose()
                self.opendental_source_engine = None
            
            if self.mysql_replication_engine:
                self.mysql_replication_engine.dispose()
                self.mysql_replication_engine = None
            
            if self.postgres_analytics_engine:
                self.postgres_analytics_engine.dispose()
                self.postgres_analytics_engine = None
            
            self.raw_to_public_transformer = None
            self.connections_initialized = False
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            
    def run_pipeline_for_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ETL pipeline for a single table.
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full refresh
            
        Returns:
            bool: True if pipeline completed successfully
        """
        if not self.connections_initialized:
            raise RuntimeError("Connections not initialized. Call initialize_connections() first.")
            
        return self.table_processor.process_table(table_name, force_full)
        
    def process_tables_by_priority(self, importance_levels: List[str] = None, 
                                 max_workers: int = 5, force_full: bool = False) -> Dict[str, List[str]]:
        """
        Process tables by priority with intelligent parallelization.
        
        CORE FUNCTIONALITY: This is the main entry point for batch processing
        and represents the heart of the orchestration logic. It coordinates:
        1. Priority-based table selection
        2. Parallel vs sequential processing decisions
        3. Error handling and failure propagation
        4. Resource management across multiple workers
        
        TESTING CRITICAL: This method needs extensive testing including:
        1. Different priority level combinations
        2. Parallel processing with various worker counts
        3. Failure scenarios and error propagation
        4. Resource exhaustion scenarios
        5. Performance testing with large table sets
        6. Integration testing with real database data
        
        Args:
            importance_levels: List of importance levels to process
            max_workers: Maximum number of parallel workers
            force_full: Whether to force full extraction
            
        Returns:
            Dict with success/failure lists for each importance level
        """
        if not self.connections_initialized:
            raise RuntimeError("Connections not initialized. Call initialize_connections() first.")
            
        return self.priority_processor.process_by_priority(
            self.table_processor,
            importance_levels,
            max_workers,
            force_full
        )
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        self.cleanup() 