"""
Pipeline Orchestrator

Handles the core ETL pipeline orchestration, coordinating between different components
and managing the overall flow of data through the pipeline.

STATUS: ACTIVE - Core Orchestration Implementation (SIMPLIFIED)
==============================================================

This is the current active implementation of the ETL pipeline orchestration,
serving as the main coordinator for the entire data pipeline. It's actively used
by CLI commands and represents the simplified architecture.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by CLI commands and main pipeline
- ✅ SIMPLIFIED ARCHITECTURE: Removed unnecessary complexity and components
- ✅ EFFICIENT CONNECTION MANAGEMENT: Streamlined connection lifecycle
- ✅ CONTEXT MANAGER: Implements __enter__/__exit__ for resource cleanup
- ✅ ERROR HANDLING: Comprehensive exception handling and cleanup
- ✅ CORE FUNCTIONALITY: Focused on essential orchestration tasks
- ❌ UNTESTED: No comprehensive integration tests

SIMPLIFIED ARCHITECTURE:
1. Connection Management: Streamlined database connection setup
2. Component Coordination: Direct delegation to TableProcessor and PriorityProcessor
3. Pipeline Execution: Simple interface for single table or batch processing
4. Resource Management: Automatic cleanup through context manager

DEPENDENCIES:
- TableProcessor: Handles individual table processing
- PriorityProcessor: Manages table processing by priority
- ConnectionFactory: Manages database connections
- Settings: Configuration management
- UnifiedMetricsCollector: Basic metrics collection

INTEGRATION POINTS:
- CLI Commands: Used by cli/commands.py for pipeline execution
- Database Connections: Manages source, replication, and analytics connections
- Table Processing: Delegates to TableProcessor for individual table ETL
- Priority Processing: Uses PriorityProcessor for batch processing
- Metrics Collection: Integrates with unified metrics collector

DEVELOPMENT NEEDS:
1. COMPREHENSIVE TESTING: Add integration and unit tests
2. ERROR RECOVERY: Enhance error handling and recovery mechanisms
3. PERFORMANCE OPTIMIZATION: Optimize parallel processing
4. MONITORING: Add comprehensive monitoring and alerting

This orchestrator represents the core of the ETL pipeline and has been simplified
for better maintainability and testing.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import time

from ..core.connections import ConnectionFactory

from ..monitoring.unified_metrics import UnifiedMetricsCollector
from .table_processor import TableProcessor
from .priority_processor import PriorityProcessor
from ..config import Settings, DatabaseType
from ..core.schema_discovery import SchemaDiscovery

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, config_path: str = None, environment: str = 'production'):
        """
        Initialize the pipeline orchestrator.
        
        SIMPLIFIED: Removed unused config_path parameter complexity.
        The Settings class handles configuration loading automatically.
        
        Args:
            config_path: Path to the configuration file (deprecated, kept for compatibility)
            environment: Environment name ('production', 'test') for database connections
        """
        self.settings = Settings(environment=environment)
        
        # Defer SchemaDiscovery creation until connections are established
        self.schema_discovery = None
        
        # Initialize components without SchemaDiscovery (will be set during connection initialization)
        self.table_processor = None
        self.priority_processor = None
        
        # Initialize metrics
        self.metrics = UnifiedMetricsCollector()
        
        # Track initialization state
        self._initialized = False
        
    def initialize_connections(self) -> bool:
        """
        Initialize all database connections and components.
        
        SIMPLIFIED: Streamlined connection initialization with automatic
        component setup through TableProcessor.
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info(f"Initializing pipeline connections for environment: {self.settings.environment}")
            
            # Create SchemaDiscovery with appropriate source database connection
            if self.settings.environment == 'test':
                source_engine = ConnectionFactory.get_source_connection(self.settings)
                source_db = self.settings.get_database_config(DatabaseType.SOURCE)['database']
            else:
                source_engine = ConnectionFactory.get_source_connection(self.settings)
                source_db = self.settings.get_database_config(DatabaseType.SOURCE)['database']
            
            self.schema_discovery = SchemaDiscovery(source_engine, source_db)
            
            # Initialize components with SchemaDiscovery
            self.table_processor = TableProcessor(schema_discovery=self.schema_discovery)
            self.priority_processor = PriorityProcessor(schema_discovery=self.schema_discovery)
            
            # Initialize table processor connections (handles all database connections)
            success = self.table_processor.initialize_connections()
            if not success:
                logger.error("Failed to initialize table processor connections")
                return False
            
            self._initialized = True
            logger.info("Successfully initialized all pipeline connections")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {str(e)}")
            self.cleanup()
            raise
            
    def cleanup(self):
        """Clean up all resources."""
        try:
            if self.table_processor:
                self.table_processor.cleanup()
            logger.info("Pipeline cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        finally:
            # Always reset initialization state, even if cleanup fails
            self._initialized = False
            
    def run_pipeline_for_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ETL pipeline for a single table.
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full refresh
            
        Returns:
            bool: True if pipeline completed successfully
        """
        if not self._initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_connections() first.")
            
        logger.info(f"Running pipeline for table: {table_name}")
        return self.table_processor.process_table(table_name, force_full)
        
    def process_tables_by_priority(self, importance_levels: List[str] = None, 
                                 max_workers: int = 5, force_full: bool = False) -> Dict[str, Dict]:
        """
        Process tables by priority with intelligent parallelization.
        
        CORE FUNCTIONALITY: This is the main entry point for batch processing.
        It coordinates priority-based table selection and parallel processing.
        
        Args:
            importance_levels: List of importance levels to process
            max_workers: Maximum number of parallel workers
            force_full: Whether to force full extraction
            
        Returns:
            Dict with success/failure lists for each importance level
        """
        if not self._initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_connections() first.")
            
        logger.info(f"Processing tables by priority with {max_workers} workers")
        return self.priority_processor.process_by_priority(
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