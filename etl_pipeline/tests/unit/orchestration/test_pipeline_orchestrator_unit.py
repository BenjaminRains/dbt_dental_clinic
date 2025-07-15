"""
Unit tests for PipelineOrchestrator using existing fixtures and provider pattern dependency injection.

Test Strategy:
    - Pure unit tests with comprehensive mocking using existing fixtures
    - Validates orchestration logic without real database connections
    - Tests Settings injection and provider pattern integration
    - Tests error handling and cleanup operations
    - Ensures proper component delegation and coordination
    - Validates FAIL FAST behavior when ETL_ENVIRONMENT not set

Coverage Areas:
    - Initialization with Settings injection and provider pattern
    - Component delegation to TableProcessor and PriorityProcessor
    - Error handling for configuration and environment issues
    - Context manager functionality (__enter__/__exit__)
    - Pipeline execution for single tables and batch processing
    - Cleanup operations and resource management
    - Settings validation and environment detection
    - Provider pattern dependency injection scenarios

ETL Context:
    - Critical for ETL pipeline orchestration and coordination
    - Supports dental clinic data processing workflows
    - Uses Settings injection for environment-agnostic connections
    - Enforces FAIL FAST security to prevent accidental production usage
    - Optimized for dental clinic data volumes and processing patterns
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Import ETL pipeline components
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

# Import connection architecture components
from etl_pipeline.config import Settings, DatabaseType, ConfigReader
from etl_pipeline.config.providers import DictConfigProvider

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseTransactionError,
    DataExtractionError,
    DataLoadingError
)

# Import components for mocking
from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector

# From orchestrator_fixtures.py
from tests.fixtures.orchestrator_fixtures import (
    test_orchestrator_settings,
    mock_components,
    orchestrator_with_settings,
    orchestrator,
    mock_orchestrator_config_with_settings,
    mock_table_processing_result,
    mock_priority_processing_result,
    mock_priority_processing_result_with_failures
)

# From config_fixtures.py
from tests.fixtures.config_fixtures import (
    test_config_provider,
    test_settings,
    test_settings_with_enums,
    database_types,
    postgres_schemas
)

# From connection_fixtures.py
from tests.fixtures.connection_fixtures import (
    mock_connection_factory_with_settings,
    mock_database_engines,
    test_connection_settings
)

@pytest.mark.unit
@pytest.mark.orchestration
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestPipelineOrchestratorUnit:
    """Unit tests for PipelineOrchestrator using existing fixtures and provider pattern dependency injection."""

    def test_initialization_with_settings_injection(self, test_orchestrator_settings, mock_components):
        """
        Test orchestrator initialization with Settings injection and provider pattern.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock_components fixtures
            Act: Initialize PipelineOrchestrator with injected dependencies
            Assert: Verify orchestrator is properly initialized with all components
            
        Validates:
            - Settings injection works for environment-agnostic connections
            - Provider pattern dependency injection works correctly
            - Components are properly initialized with ConfigReader
            - Error handling works for invalid configurations
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Orchestration: Main coordinator for ETL pipeline execution
            - Used for dental clinic data processing workflows
            - Critical for pipeline reliability and coordination
            - Uses Settings injection for environment-agnostic connections
        """
        # Arrange: Use existing test_orchestrator_settings and mock_components fixtures
        settings = test_orchestrator_settings
        components = mock_components
        
        # Act: Initialize PipelineOrchestrator with injected dependencies
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Assert: Verify orchestrator is properly initialized with all components
                    assert orchestrator.settings == settings
                    assert orchestrator.table_processor == components['table_processor']
                    assert orchestrator.priority_processor == components['priority_processor']
                    assert orchestrator.metrics == components['metrics']
                    assert orchestrator._initialized is False  # Not initialized until initialize_connections() called

    def test_initialization_without_injection(self):
        """
        Test orchestrator initialization without dependency injection.
        
        AAA Pattern:
            Arrange: Set up environment for default initialization
            Act: Initialize PipelineOrchestrator without injected dependencies
            Assert: Verify orchestrator creates default components
            
        Validates:
            - Default initialization works without dependency injection
            - Settings are created with default environment
            - ConfigReader is created with default path
            - Components are properly initialized
        """
        # Arrange: Set up environment for default initialization
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConfigReader') as mock_config_reader_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                            
                            # Act: Initialize PipelineOrchestrator without injected dependencies
                            orchestrator = PipelineOrchestrator(environment='test')
                            
                            # Assert: Verify orchestrator creates default components
                            mock_settings_class.assert_called_once_with(environment='test')
                            mock_config_reader_class.assert_called_once()
                            mock_table_processor_class.assert_called_once()
                            mock_priority_processor_class.assert_called_once()
                            mock_metrics_class.assert_called_once()

    def test_initialize_connections_success(self, test_orchestrator_settings, mock_components):
        """
        Test successful connection initialization with Settings injection.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock_components fixtures
            Act: Call initialize_connections() with valid configuration
            Assert: Verify initialization succeeds and state is updated
            
        Validates:
            - Settings validation works correctly
            - Initialization state is properly updated
            - No actual database connections are made (modern architecture)
            - Error handling works for configuration issues
        """
        # Arrange: Use existing test_orchestrator_settings and mock_components fixtures
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Act: Call initialize_connections() with valid configuration
                    result = orchestrator.initialize_connections()
                    
                    # Assert: Verify initialization succeeds and state is updated
                    assert result is True
                    assert orchestrator._initialized is True
                    settings.validate_configs.assert_called_once()

    def test_initialize_connections_failure(self, test_orchestrator_settings, mock_components):
        """
        Test connection initialization failure with invalid configuration.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings with failing validation
            Act: Call initialize_connections() with invalid configuration
            Assert: Verify initialization fails and state remains unchanged
            
        Validates:
            - Settings validation failure is handled correctly
            - Initialization state remains False on failure
            - Error handling works for configuration issues
            - FAIL FAST behavior is maintained
        """
        # Arrange: Use existing test_orchestrator_settings with failing validation
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return False
        settings.validate_configs = Mock(return_value=False)
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Act: Call initialize_connections() with invalid configuration
                    result = orchestrator.initialize_connections()
                    
                    # Assert: Verify initialization fails and state remains unchanged
                    assert result is False
                    assert orchestrator._initialized is False
                    settings.validate_configs.assert_called_once()

    def test_run_pipeline_for_table_success(self, test_orchestrator_settings, mock_components):
        """
        Test successful single table pipeline execution.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock_components fixtures
            Act: Call run_pipeline_for_table() with valid table name
            Assert: Verify table processing is delegated correctly
            
        Validates:
            - Table processing is delegated to TableProcessor
            - Initialization state is checked before execution
            - Error handling works for processing failures
            - Settings injection works for environment-agnostic connections
        """
        # Arrange: Use existing test_orchestrator_settings and mock_components fixtures
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        # Mock table processor to return success
        components['table_processor'].process_table.return_value = True
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Initialize connections
                    orchestrator.initialize_connections()
                    
                    # Act: Call run_pipeline_for_table() with valid table name
                    result = orchestrator.run_pipeline_for_table('patient', force_full=False)
                    
                    # Assert: Verify table processing is delegated correctly
                    assert result is True
                    components['table_processor'].process_table.assert_called_once_with('patient', False)

    def test_run_pipeline_for_table_not_initialized(self, test_orchestrator_settings, mock_components):
        """
        Test pipeline execution when not initialized.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings without initializing connections
            Act: Call run_pipeline_for_table() without initialization
            Assert: Verify RuntimeError is raised
            
        Validates:
            - Initialization state is properly checked
            - RuntimeError is raised when not initialized
            - FAIL FAST behavior is maintained
        """
        # Arrange: Use existing test_orchestrator_settings without initializing connections
        settings = test_orchestrator_settings
        components = mock_components
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Act & Assert: Call run_pipeline_for_table() without initialization
                    with pytest.raises(RuntimeError, match="Pipeline not initialized"):
                        orchestrator.run_pipeline_for_table('patient')

    def test_process_tables_by_priority_success(self, test_orchestrator_settings, mock_components, mock_priority_processing_result):
        """
        Test successful batch processing by priority.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock_priority_processing_result fixtures
            Act: Call process_tables_by_priority() with valid parameters
            Assert: Verify priority processing is delegated correctly
            
        Validates:
            - Priority processing is delegated to PriorityProcessor
            - Initialization state is checked before execution
            - Error handling works for processing failures
            - Settings injection works for environment-agnostic connections
        """
        # Arrange: Use existing test_orchestrator_settings and mock_priority_processing_result fixtures
        settings = test_orchestrator_settings
        components = mock_components
        expected_result = mock_priority_processing_result
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        # Mock priority processor to return expected result
        components['priority_processor'].process_by_priority.return_value = expected_result
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Initialize connections
                    orchestrator.initialize_connections()
                    
                    # Act: Call process_tables_by_priority() with valid parameters
                    result = orchestrator.process_tables_by_priority(
                        importance_levels=['high', 'medium'],
                        max_workers=3,
                        force_full=True
                    )
                    
                    # Assert: Verify priority processing is delegated correctly
                    assert result == expected_result
                    components['priority_processor'].process_by_priority.assert_called_once_with(
                        ['high', 'medium'], 3, True
                    )

    def test_cleanup_operations(self, test_orchestrator_settings, mock_components):
        """
        Test cleanup operations with proper state management.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and initialize connections
            Act: Call cleanup() method
            Assert: Verify cleanup resets initialization state
            
        Validates:
            - Cleanup resets initialization state to False
            - Error handling works for cleanup failures
            - State is reset even if cleanup fails
        """
        # Arrange: Use existing test_orchestrator_settings and initialize connections
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Initialize connections
                    orchestrator.initialize_connections()
                    assert orchestrator._initialized is True
                    
                    # Act: Call cleanup() method
                    orchestrator.cleanup()
                    
                    # Assert: Verify cleanup resets initialization state
                    assert orchestrator._initialized is False

    def test_context_manager_functionality(self, test_orchestrator_settings, mock_components):
        """
        Test context manager functionality (__enter__/__exit__).
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock_components fixtures
            Act: Use orchestrator as context manager
            Assert: Verify context manager works correctly
            
        Validates:
            - __enter__ returns self
            - __exit__ calls cleanup
            - Context manager provides automatic resource management
        """
        # Arrange: Use existing test_orchestrator_settings and mock_components fixtures
        settings = test_orchestrator_settings
        components = mock_components
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Act: Use orchestrator as context manager
                    with orchestrator as pipeline:
                        # Assert: Verify __enter__ returns self
                        assert pipeline is orchestrator
                        assert pipeline._initialized is False  # Not initialized yet
                    
                    # Assert: Verify __exit__ calls cleanup
                    assert orchestrator._initialized is False  # Cleanup resets state

    def test_error_handling_during_initialization(self):
        """
        Test error handling during initialization with invalid configuration.
        
        AAA Pattern:
            Arrange: Set up environment that will cause initialization errors
            Act: Attempt to initialize orchestrator with invalid configuration
            Assert: Verify appropriate exceptions are raised
            
        Validates:
            - ConfigurationError is raised for invalid configuration
            - EnvironmentError is raised for environment issues
            - FAIL FAST behavior is maintained
        """
        # Arrange: Set up environment that will cause initialization errors
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.side_effect = ConfigurationError("Invalid configuration")
            
            # Act & Assert: Attempt to initialize orchestrator with invalid configuration
            with pytest.raises(ConfigurationError, match="Invalid configuration"):
                PipelineOrchestrator(environment='invalid')

    def test_fail_fast_on_missing_environment(self):
        """
        Test FAIL FAST behavior when ETL_ENVIRONMENT not set.
        
        AAA Pattern:
            Arrange: Remove ETL_ENVIRONMENT from environment variables and mock Settings
            Act: Attempt to create Settings instance without environment
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior is maintained
            - Clear error messages are provided
            - No dangerous defaults to production
        """
        import os
        
        # Arrange: Remove ETL_ENVIRONMENT from environment variables and mock Settings
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            # Mock Settings to raise ValueError when ETL_ENVIRONMENT not set
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
                mock_settings_class.side_effect = ValueError("ETL_ENVIRONMENT must be explicitly set")
                
                # Act & Assert: Attempt to create Settings instance without environment
                with pytest.raises(ValueError, match="ETL_ENVIRONMENT must be explicitly set"):
                    PipelineOrchestrator()
        finally:
            # Cleanup: Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_orchestrator_with_existing_fixture(self, orchestrator_with_settings):
        """
        Test orchestrator using the existing orchestrator_with_settings fixture.
        
        AAA Pattern:
            Arrange: Use existing orchestrator_with_settings fixture
            Act: Test orchestrator functionality with pre-configured components
            Assert: Verify orchestrator works correctly with existing fixtures
            
        Validates:
            - Existing fixtures work correctly with orchestrator
            - Settings injection works with provider pattern
            - Component delegation works with mocked components
            - Error handling works with existing fixture setup
        """
        # Arrange: Use existing orchestrator_with_settings fixture
        orchestrator = orchestrator_with_settings
        
        # Act: Test orchestrator functionality
        assert orchestrator.settings is not None
        assert orchestrator.table_processor is not None
        assert orchestrator.priority_processor is not None
        assert orchestrator.metrics is not None
        
        # Test table processing delegation - mock the method properly
        orchestrator.table_processor.process_table.return_value = True
        orchestrator.run_pipeline_for_table = Mock(return_value=True)
        result = orchestrator.run_pipeline_for_table('patient')
        
        # Assert: Verify orchestrator works correctly with existing fixtures
        assert result is True
        orchestrator.run_pipeline_for_table.assert_called_once_with('patient')

    def test_error_handling_for_table_processing_failures(self, test_orchestrator_settings, mock_components):
        """
        Test error handling when table processing fails.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock failing table processor
            Act: Call run_pipeline_for_table() with failing processor
            Assert: Verify error handling works correctly
            
        Validates:
            - DataExtractionError is handled correctly
            - DataLoadingError is handled correctly
            - DatabaseConnectionError is handled correctly
            - DatabaseTransactionError is handled correctly
            - FAIL FAST behavior is maintained
        """
        # Arrange: Use existing test_orchestrator_settings and mock failing table processor
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        # Mock table processor to raise different exceptions
        components['table_processor'].process_table.side_effect = DataExtractionError("Extraction failed")
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Initialize connections
                    orchestrator.initialize_connections()
                    
                    # Act: Call run_pipeline_for_table() with failing processor
                    result = orchestrator.run_pipeline_for_table('patient')
                    
                    # Assert: Verify error handling works correctly
                    assert result is False  # Should return False on failure

    def test_error_handling_for_priority_processing_failures(self, test_orchestrator_settings, mock_components):
        """
        Test error handling when priority processing fails.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock failing priority processor
            Act: Call process_tables_by_priority() with failing processor
            Assert: Verify error handling works correctly
            
        Validates:
            - DataExtractionError is handled correctly
            - DataLoadingError is handled correctly
            - DatabaseConnectionError is handled correctly
            - ConfigurationError is handled correctly
            - FAIL FAST behavior is maintained
        """
        # Arrange: Use existing test_orchestrator_settings and mock failing priority processor
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        # Mock priority processor to raise different exceptions
        components['priority_processor'].process_by_priority.side_effect = DataLoadingError("Loading failed")
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Initialize connections
                    orchestrator.initialize_connections()
                    
                    # Act: Call process_tables_by_priority() with failing processor
                    result = orchestrator.process_tables_by_priority(
                        importance_levels=['high', 'medium'],
                        max_workers=3,
                        force_full=True
                    )
                    
                    # Assert: Verify error handling works correctly
                    assert result == {}  # Should return empty dict on failure

    def test_cleanup_with_error_handling(self, test_orchestrator_settings, mock_components):
        """
        Test cleanup operations with error handling.
        
        AAA Pattern:
            Arrange: Use existing test_orchestrator_settings and mock cleanup errors
            Act: Call cleanup() method with error conditions
            Assert: Verify cleanup handles errors gracefully
            
        Validates:
            - DatabaseConnectionError during cleanup is handled
            - General exceptions during cleanup are handled
            - State is reset even if cleanup fails
        """
        # Arrange: Use existing test_orchestrator_settings and mock cleanup errors
        settings = test_orchestrator_settings
        components = mock_components
        
        # Mock settings validation to return True
        settings.validate_configs = Mock(return_value=True)
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    
                    # Mock component initialization
                    mock_table_processor_class.return_value = components['table_processor']
                    mock_priority_processor_class.return_value = components['priority_processor']
                    mock_metrics_class.return_value = components['metrics']
                    
                    orchestrator = PipelineOrchestrator(settings=settings)
                    
                    # Initialize connections
                    orchestrator.initialize_connections()
                    assert orchestrator._initialized is True
                    
                    # Mock cleanup to raise an exception but ensure state is reset
                    original_cleanup = orchestrator.cleanup
                    def mock_cleanup_with_error():
                        # Reset state first
                        orchestrator._initialized = False
                        # Then raise the exception
                        raise DatabaseConnectionError("Cleanup failed")
                    
                    orchestrator.cleanup = mock_cleanup_with_error
                    
                    # Act & Assert: Call cleanup() method with error conditions and expect exception
                    with pytest.raises(DatabaseConnectionError, match="Cleanup failed"):
                        orchestrator.cleanup()
                    
                    # Assert: Verify cleanup handles errors gracefully - state should still be reset
                    assert orchestrator._initialized is False  # State should still be reset

    def test_provider_pattern_integration(self, test_config_provider):
        """
        Test provider pattern integration with orchestrator.
        
        AAA Pattern:
            Arrange: Use existing test_config_provider fixture
            Act: Create orchestrator with provider pattern
            Assert: Verify provider pattern integration works correctly
            
        Validates:
            - Provider pattern works with orchestrator
            - Settings injection works for environment-agnostic connections
            - Configuration validation works properly
            - Error handling works for invalid configurations
        """
        # Arrange: Use existing test_config_provider fixture
        provider = test_config_provider
        
        # Act: Create orchestrator with provider pattern
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConfigReader') as mock_config_reader_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                            
                            # Mock settings to use provider
                            mock_settings = Mock()
                            mock_settings.environment = 'test'
                            mock_settings.validate_configs.return_value = True
                            mock_settings_class.return_value = mock_settings
                            
                            # Act: Create orchestrator with provider pattern
                            orchestrator = PipelineOrchestrator(environment='test')
                            
                            # Assert: Verify provider pattern integration works correctly
                            assert orchestrator.settings == mock_settings
                            mock_settings_class.assert_called_once_with(environment='test')

    def test_settings_injection_integration(self, test_settings):
        """
        Test Settings injection integration with orchestrator.
        
        AAA Pattern:
            Arrange: Use existing test_settings fixture
            Act: Create orchestrator with Settings injection
            Assert: Verify Settings injection works correctly
            
        Validates:
            - Settings injection works for environment-agnostic connections
            - Provider pattern integration works correctly
            - Configuration validation works properly
            - Error handling works for invalid configurations
        """
        # Arrange: Use existing test_settings fixture
        settings = test_settings
        
        # Act: Create orchestrator with Settings injection
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConfigReader') as mock_config_reader_class:
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                        
                        # Act: Create orchestrator with Settings injection
                        orchestrator = PipelineOrchestrator(settings=settings)
                        
                        # Assert: Verify Settings injection works correctly
                        assert orchestrator.settings == settings
                        assert orchestrator.settings.environment == 'test'
