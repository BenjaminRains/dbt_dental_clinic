"""
Pipeline Orchestrator

Handles the core ETL pipeline orchestration, coordinating between different components
and managing the overall flow of data through the pipeline.

STATUS: ACTIVE - Core Orchestration Implementation (REFACTORED)
==============================================================

This is the current active implementation of the ETL pipeline orchestration,
serving as the main coordinator for the entire data pipeline. It's actively used
by CLI commands and represents the refactored architecture using integrated approach.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by CLI commands and main pipeline
- ✅ REFACTORED ARCHITECTURE: Uses integrated approach with SimpleMySQLReplicator and PostgresLoader
- ✅ MODERN CONNECTION ARCHITECTURE: Uses Settings injection, no direct connection management
- ✅ CONTEXT MANAGER: Implements __enter__/__exit__ for resource cleanup
- ✅ ERROR HANDLING: Comprehensive exception handling and cleanup
- ✅ CORE FUNCTIONALITY: Focused on essential orchestration tasks
- ✅ STATIC CONFIGURATION: Uses ConfigReader for table configuration
- ❌ UNTESTED: No comprehensive integration tests

REFACTORED ARCHITECTURE:
1. Connection Management: Components handle their own connections using Settings injection
2. Component Coordination: Direct delegation to TableProcessor and PriorityProcessor
3. Pipeline Execution: Simple interface for single table or batch processing
4. Resource Management: Automatic cleanup through context manager
5. Integrated Approach: Uses SimpleMySQLReplicator and PostgresLoader via components

DEPENDENCIES:
- TableProcessor: Handles individual table processing using integrated approach
- PriorityProcessor: Manages table processing by priority using integrated approach
- Settings: Configuration management with provider pattern
- ConfigReader: Static configuration management
- UnifiedMetricsCollector: Basic metrics collection

INTEGRATION POINTS:
- CLI Commands: Used by cli/commands.py for pipeline execution
- Database Connections: Components manage their own connections using Settings injection
- Table Processing: Delegates to TableProcessor for individual table ETL
- Priority Processing: Uses PriorityProcessor for batch processing
- Metrics Collection: Integrates with unified metrics collector

DEVELOPMENT NEEDS:
1. COMPREHENSIVE TESTING: Add integration and unit tests
2. ERROR RECOVERY: Enhance error handling and recovery mechanisms
3. PERFORMANCE OPTIMIZATION: Optimize parallel processing
4. MONITORING: Add comprehensive monitoring and alerting

This orchestrator represents the core of the ETL pipeline and has been refactored
to use the integrated approach with static configuration for better performance.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import time

from ..monitoring.unified_metrics import UnifiedMetricsCollector
from .table_processor import TableProcessor
from .priority_processor import PriorityProcessor
from ..config import Settings, DatabaseType, ConfigReader

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from ..exceptions.data import DataExtractionError, DataLoadingError
from ..exceptions.configuration import ConfigurationError, EnvironmentError

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, config_path: Optional[str] = None, environment: str = 'production', 
                 settings: Optional[Settings] = None, config_reader: Optional[ConfigReader] = None):
        """
        Initialize the pipeline orchestrator.
        
        MODERN ARCHITECTURE: Uses Settings injection and unified interface.
        Components handle their own connections - no direct connection management.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - Follows unified interface with Settings injection
        - Validates environment configuration before processing
        - Uses provider pattern for configuration management
        - No direct connection management - components handle their own connections
        
        Args:
            config_path: Path to the configuration file (used for ConfigReader)
            environment: Environment name ('production', 'test') for database connections
            settings: Optional Settings instance to use (for testing)
            config_reader: Optional ConfigReader instance to use (for testing)
        """
        try:
            if settings is not None:
                self.settings = settings
                self.config_path = None
            else:
                self.settings = Settings(environment=environment)
                self.config_path = config_path or "etl_pipeline/config/tables.yml"
            
            # Use injected config_reader or create default one
            if config_reader is not None:
                self.config_reader = config_reader
            else:
                # Use default ConfigReader which will auto-detect the path
                self.config_reader = ConfigReader()
            
            # ✅ MODERN ARCHITECTURE: Initialize components with ConfigReader
            # Components handle their own connections using Settings injection
            self.table_processor = TableProcessor(config_reader=self.config_reader)
            self.priority_processor = PriorityProcessor(config_reader=self.config_reader)
            
            # Initialize metrics
            self.metrics = UnifiedMetricsCollector(settings=self.settings)
            
            # Track initialization state
            self._initialized = False
            
        except ConfigurationError as e:
            logger.error(f"Configuration error in PipelineOrchestrator initialization: {e}")
            raise
        except EnvironmentError as e:
            logger.error(f"Environment error in PipelineOrchestrator initialization: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in PipelineOrchestrator initialization: {str(e)}")
            raise
    
    # Test configuration creation moved to fixtures
    # This method is deprecated and will be removed
    def _create_test_config_reader(self):
        """DEPRECATED: Use dependency injection instead."""
        raise NotImplementedError("Test configuration should be injected via fixtures")
        
    def initialize_connections(self) -> bool:
        """
        Initialize all database connections and components.
        
        MODERN ARCHITECTURE: No explicit connection initialization needed.
        Components handle their own connections using Settings injection.
        This provides faster initialization and eliminates dynamic schema discovery overhead.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - Components handle their own connections using Settings injection
        - Validates environment configuration before processing
        - Uses provider pattern for configuration management
        - No direct connection management - components handle their own connections
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info(f"Initializing pipeline components for environment: {self.settings.environment}")
            
            # ✅ MODERN ARCHITECTURE: Validate environment configuration
            # Components will handle their own connections using Settings injection
            if not self.settings.validate_configs():
                logger.error("Configuration validation failed")
                return False
            
            # ✅ MODERN ARCHITECTURE: No explicit connection initialization needed
            # TableProcessor and PriorityProcessor handle their own connections
            # using Settings injection for environment-agnostic operation
            
            self._initialized = True
            logger.info("Successfully initialized all pipeline components using modern architecture")
            return True
            
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during pipeline initialization: {e}")
            self.cleanup()
            raise
        except ConfigurationError as e:
            logger.error(f"Configuration error during pipeline initialization: {e}")
            self.cleanup()
            raise
        except EnvironmentError as e:
            logger.error(f"Environment error during pipeline initialization: {e}")
            self.cleanup()
            raise
        except Exception as e:
            logger.error(f"Unexpected error during pipeline initialization: {str(e)}")
            self.cleanup()
            raise
            
    def cleanup(self):
        """
        Clean up all resources.
        
        MODERN ARCHITECTURE: Components handle their own cleanup.
        No explicit cleanup needed for connections - they're managed by components.
        """
        try:
            # ✅ MODERN ARCHITECTURE: Components handle their own cleanup
            # TableProcessor and PriorityProcessor manage their own connections
            # and cleanup automatically when they go out of scope
            
            logger.info("Pipeline cleanup completed using modern architecture")
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error during cleanup: {e}")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        finally:
            # Always reset initialization state, even if cleanup fails
            self._initialized = False
            
    def run_pipeline_for_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Run the complete ETL pipeline for a single table.
        
        MODERN ARCHITECTURE: Uses Settings injection and unified interface.
        Components handle their own connections using Settings injection.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - TableProcessor uses unified ConnectionFactory interface
        - Validates environment configuration before processing
        - Uses provider pattern for configuration management
        - No direct connection management - components handle their own connections
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full refresh
            
        Returns:
            bool: True if pipeline completed successfully
        """
        if not self._initialized:
            raise RuntimeError("Pipeline not initialized. Call initialize_connections() first.")
            
        if self.table_processor is None:
            raise RuntimeError("TableProcessor not initialized")
            
        try:
            logger.info(f"Running pipeline for table: {table_name} using modern architecture")
            return self.table_processor.process_table(table_name, force_full)
            
        except DataExtractionError as e:
            logger.error(f"Data extraction failed for table {table_name}: {e}")
            return False
        except DataLoadingError as e:
            logger.error(f"Data loading failed for table {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed for table {table_name}: {e}")
            return False
        except DatabaseTransactionError as e:
            logger.error(f"Database transaction failed for table {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in pipeline for table {table_name}: {str(e)}")
            return False
        
    def process_tables_by_priority(self, importance_levels: Optional[List[str]] = None, 
                                 max_workers: int = 5, force_full: bool = False) -> Dict[str, Dict[str, List[str]]]:
        """
        Process tables by priority with intelligent parallelization.
        
        MODERN ARCHITECTURE: Uses Settings injection and unified interface.
        Components handle their own connections using Settings injection.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - PriorityProcessor uses unified ConnectionFactory interface
        - Validates environment configuration before processing
        - Uses provider pattern for configuration management
        - No direct connection management - components handle their own connections
        
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
            
        if self.priority_processor is None:
            raise RuntimeError("PriorityProcessor not initialized")
            
        try:
            logger.info(f"Processing tables by priority with {max_workers} workers using modern architecture")
            return self.priority_processor.process_by_priority(
                importance_levels,
                max_workers,
                force_full
            )
            
        except DataExtractionError as e:
            logger.error(f"Data extraction failed during priority processing: {e}")
            return {}
        except DataLoadingError as e:
            logger.error(f"Data loading failed during priority processing: {e}")
            return {}
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during priority processing: {e}")
            return {}
        except ConfigurationError as e:
            logger.error(f"Configuration error during priority processing: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error during priority processing: {str(e)}")
            return {}
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        self.cleanup() 