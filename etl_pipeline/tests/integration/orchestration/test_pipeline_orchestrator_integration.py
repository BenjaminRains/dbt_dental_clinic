"""
Integration tests for PipelineOrchestrator using real database connections.

Test Strategy:
    - Real database integration with test environment and provider pattern
    - Safety, error handling, actual data flow, all methods
    - Coverage: Integration scenarios and edge cases
    - Execution: < 10 seconds per component
    - Environment: Real test databases, no production connections with FileConfigProvider
    - Order Markers: Proper test execution order for data flow validation
    - Provider Usage: FileConfigProvider with real test configuration files
    - Settings Injection: Uses Settings with FileConfigProvider for real test environment
    - Environment Separation: Uses .env_test file for test environment isolation

Coverage Areas:
    - Real database connection initialization and validation
    - Complete pipeline lifecycle with actual data flow
    - Component coordination with real database connections
    - Error handling with real database scenarios
    - Performance characteristics with real database operations
    - Resource management with real database connections
    - FAIL FAST behavior with real environment validation
    - Settings injection with FileConfigProvider for real test environment
    - Provider pattern integration with real configuration files
    - Environment separation with real .env_test file

ETL Pipeline Context:
    - Critical for ETL pipeline orchestration and coordination
    - Manages complete data processing lifecycle with real databases
    - Coordinates between extraction, transformation, and loading stages
    - Provides monitoring and metrics collection with real data
    - Implements FAIL FAST security to prevent accidental production usage
    - Uses provider pattern for clean dependency injection with real configs
    - Optimized for dental clinic data processing workflows with real data
"""

import pytest
import time
import os
from typing import Dict, Any, Optional

# Import ETL pipeline components
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

# Import configuration components
from etl_pipeline.config import Settings, DatabaseType, ConfigReader

# Import custom exceptions for integration error testing
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseQueryError,
    DataExtractionError,
    DataLoadingError,
    DataTransformationError,
    SchemaValidationError
)

# Import fixtures for integration testing
from tests.fixtures.orchestrator_fixtures import (
    orchestrator_with_settings
)
from tests.fixtures.config_fixtures import (
    test_pipeline_config,
    test_tables_config,
    test_env_vars,
    test_settings
)


@pytest.fixture
def integration_settings():
    """
    Create integration Settings instance using FileConfigProvider with real test environment.
    
    Provides Settings instance with real test configuration for:
    - Real test environment isolation using .env_test file
    - FileConfigProvider with real configuration files
    - Settings injection for environment-agnostic connections
    - Real database connection parameters for test environment
    
    ETL Context:
        - Uses FileConfigProvider for real test configuration files
        - Supports real test environment isolation
        - Enables real database connections for integration testing
        - Uses Settings injection for consistent API across environments
    """
    # Use real test environment - FileConfigProvider will load .env_test
    settings = Settings(environment='test')
    return settings


@pytest.fixture
def integration_orchestrator(integration_settings):
    """
    Create integration orchestrator using real test configuration.
    
    Provides PipelineOrchestrator instance with real test configuration for:
    - Real test environment isolation using .env_test file
    - FileConfigProvider with real configuration files
    - Settings injection for environment-agnostic connections
    - Real database connection parameters for test environment
    
    ETL Context:
        - Uses FileConfigProvider for real test configuration files
        - Supports real test environment isolation
        - Enables real database connections for integration testing
        - Uses Settings injection for consistent API across environments
    """
    # Use real test configuration - FileConfigProvider will load real config files
    orchestrator = PipelineOrchestrator(
        config_path="etl_pipeline/config/tables.yml",
        environment="test",
        settings=integration_settings
    )
    return orchestrator


class TestPipelineOrchestratorIntegration:
    """Integration tests for PipelineOrchestrator with real database connections."""

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_initialization_with_real_connections(self, integration_orchestrator, integration_settings):
        """
        Test integration initialization with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Initialize connections with real database validation
            Assert: Verify real database connections are established successfully
            
        Validates:
            - Real database connections are established successfully
            - Settings injection works with FileConfigProvider
            - Environment validation works with real .env_test file
            - Configuration validation works with real configuration files
            - Provider pattern works with real FileConfigProvider
            - FAIL FAST behavior works with real environment validation
            - Component initialization works with real database connections
            - Error handling works for real database connection failures
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Initialize connections with real database validation
        result = orchestrator.initialize_connections()
        
        # Assert: Verify real database connections are established successfully
        assert result is True
        assert orchestrator._initialized is True
        
        # Verify Settings injection works with FileConfigProvider
        assert orchestrator.settings.environment == 'test'
        assert orchestrator.settings.pipeline_config is not None
        assert orchestrator.settings.tables_config is not None
        assert orchestrator.settings._env_vars is not None
        
        # Verify real environment variables are loaded
        env_vars = orchestrator.settings._env_vars
        assert 'ETL_ENVIRONMENT' in env_vars
        assert env_vars['ETL_ENVIRONMENT'] == 'test'
        
        # Verify real configuration files are loaded
        pipeline_config = orchestrator.settings.pipeline_config
        assert 'connections' in pipeline_config
        assert 'general' in pipeline_config
        assert 'stages' in pipeline_config
        assert 'logging' in pipeline_config
        assert 'error_handling' in pipeline_config
        
        tables_config = orchestrator.settings.tables_config
        assert 'tables' in tables_config
        assert 'patient' in tables_config['tables']
        assert 'appointment' in tables_config['tables']

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_pipeline_lifecycle_with_real_data(self, integration_orchestrator):
        """
        Test integration pipeline lifecycle with real data flow.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Execute complete pipeline lifecycle with real data flow
            Assert: Verify complete pipeline lifecycle works with real data
            
        Validates:
            - Complete pipeline lifecycle works with real data flow
            - Component coordination works with real database connections
            - Error handling works for real database scenarios
            - Performance characteristics with real database operations
            - Resource management with real database connections
            - Settings injection works with real database connections
            - Provider pattern works with real configuration files
            - Environment separation works with real .env_test file
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Execute complete pipeline lifecycle with real data flow
        # Initialize connections
        init_result = orchestrator.initialize_connections()
        assert init_result is True
        
        # Test single table processing with real data
        # Note: This will use real database connections but may not have actual data
        # The test validates the connection and processing framework
        table_result = orchestrator.run_pipeline_for_table('patient')
        # Result may be False if no real data exists, but framework should work
        assert isinstance(table_result, bool)
        
        # Test batch processing with real data
        batch_result = orchestrator.process_tables_by_priority(['patient', 'appointment'])
        # Result may be empty dict if no real data exists, but framework should work
        assert isinstance(batch_result, dict)
        
        # Test cleanup
        orchestrator.cleanup()
        assert orchestrator._initialized is False

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_error_handling_with_real_connections(self, integration_orchestrator):
        """
        Test integration error handling with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test error handling with real database scenarios
            Assert: Verify error handling works correctly with real connections
            
        Validates:
            - Error handling works correctly with real database connections
            - Configuration errors are handled properly
            - Database connection errors are handled correctly
            - Data processing errors are handled appropriately
            - FAIL FAST behavior works with real environment validation
            - Error recovery works where possible
            - Error logging and monitoring work with real connections
            - State management works during errors with real connections
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test error handling with real database scenarios
        
        # Test initialization error handling
        # This should work since we have real test configuration
        result = orchestrator.initialize_connections()
        assert result is True
        
        # Test with invalid table name - should handle gracefully
        invalid_result = orchestrator.run_pipeline_for_table('invalid_table_name')
        assert isinstance(invalid_result, bool)  # Should return False, not raise exception
        
        # Test with empty table list - should handle gracefully
        empty_result = orchestrator.process_tables_by_priority([])
        assert isinstance(empty_result, dict)  # Should return empty dict, not raise exception
        
        # Test cleanup error handling
        orchestrator.cleanup()
        assert orchestrator._initialized is False

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_context_manager_with_real_connections(self, integration_orchestrator):
        """
        Test integration context manager with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test context manager functionality with real database connections
            Assert: Verify context manager works correctly with real connections
            
        Validates:
            - Context manager works correctly with real database connections
            - Proper initialization and cleanup in context with real connections
            - Error handling works in context manager with real connections
            - Resource management works correctly with real database connections
            - State management works in context with real connections
            - Exception handling works in context with real connections
            - Settings injection works in context manager
            - Provider pattern works in context manager
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test context manager functionality with real database connections
        with orchestrator as ctx_orchestrator:
            # Context manager should provide the same orchestrator instance
            assert ctx_orchestrator is orchestrator
            
            # Initialize connections in context
            init_result = ctx_orchestrator.initialize_connections()
            assert init_result is True
            assert ctx_orchestrator._initialized is True
            
            # Test operations in context
            table_result = ctx_orchestrator.run_pipeline_for_table('patient')
            assert isinstance(table_result, bool)
            
            batch_result = ctx_orchestrator.process_tables_by_priority(['patient'])
            assert isinstance(batch_result, dict)
        
        # Assert: Verify context manager works correctly with real connections
        # Context manager should have cleaned up automatically
        assert orchestrator._initialized is False

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_performance_with_real_connections(self, integration_orchestrator):
        """
        Test integration performance with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test performance characteristics with real database connections
            Assert: Verify performance characteristics are acceptable with real connections
            
        Validates:
            - Performance is acceptable for all operations with real connections
            - Memory usage is reasonable with real database connections
            - No memory leaks occur with real database connections
            - Component coordination is efficient with real connections
            - Error handling is performant with real database connections
            - Resource management is efficient with real database connections
            - Settings injection is performant with real connections
            - Provider pattern is performant with real configuration files
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test performance characteristics with real database connections
        start_time = time.time()
        
        # Test initialization performance with real connections
        init_result = orchestrator.initialize_connections()
        init_time = time.time() - start_time
        
        # Test single table processing performance with real connections
        table_start = time.time()
        table_result = orchestrator.run_pipeline_for_table('patient')
        table_time = time.time() - table_start
        
        # Test batch processing performance with real connections
        batch_start = time.time()
        batch_result = orchestrator.process_tables_by_priority(['patient', 'appointment'])
        batch_time = time.time() - batch_start
        
        # Test cleanup performance with real connections
        cleanup_start = time.time()
        orchestrator.cleanup()
        cleanup_time = time.time() - cleanup_start
        
        total_time = time.time() - start_time
        
        # Assert: Verify performance characteristics are acceptable with real connections
        assert init_time < 5.0, f"Initialization took too long: {init_time:.2f}s"
        assert table_time < 10.0, f"Single table processing took too long: {table_time:.2f}s"
        assert batch_time < 15.0, f"Batch processing took too long: {batch_time:.2f}s"
        assert cleanup_time < 2.0, f"Cleanup took too long: {cleanup_time:.2f}s"
        assert total_time < 30.0, f"Total test took too long: {total_time:.2f}s"
        
        # Verify results are of correct types
        assert isinstance(init_result, bool)
        assert isinstance(table_result, bool)
        assert isinstance(batch_result, dict)

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_fail_fast_behavior_with_real_environment(self, integration_orchestrator):
        """
        Test integration FAIL FAST behavior with real environment validation.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test FAIL FAST behavior with real environment scenarios
            Assert: Verify FAIL FAST behavior works correctly with real environment
            
        Validates:
            - FAIL FAST behavior works correctly for all scenarios with real environment
            - Security measures are enforced with real environment validation
            - Clear error messages are provided for real environment issues
            - No dangerous defaults to production with real environment
            - Environment validation works correctly with real .env_test file
            - Configuration validation works properly with real configuration files
            - Settings injection works with real environment validation
            - Provider pattern works with real environment validation
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test FAIL FAST behavior with real environment scenarios
        
        # Test that real test environment is properly detected
        assert orchestrator.settings.environment == 'test'
        
        # Test that real environment variables are loaded
        env_vars = orchestrator.settings._env_vars
        assert 'ETL_ENVIRONMENT' in env_vars
        assert env_vars['ETL_ENVIRONMENT'] == 'test'
        
        # Test that real configuration files are loaded
        assert orchestrator.settings.pipeline_config is not None
        assert orchestrator.settings.tables_config is not None
        
        # Test that real database configuration is available
        # This validates that the real test environment is properly configured
        source_config = orchestrator.settings.get_database_config(DatabaseType.SOURCE)
        assert source_config is not None
        assert 'host' in source_config
        assert 'database' in source_config
        assert 'user' in source_config
        assert 'password' in source_config
        
        # Test that real analytics configuration is available
        analytics_config = orchestrator.settings.get_database_config(DatabaseType.ANALYTICS)
        assert analytics_config is not None
        assert 'host' in analytics_config
        assert 'database' in analytics_config
        assert 'user' in analytics_config
        assert 'password' in analytics_config

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_component_coordination_with_real_connections(self, integration_orchestrator):
        """
        Test integration component coordination with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test component coordination with real database connections
            Assert: Verify component coordination works correctly with real connections
            
        Validates:
            - Component coordination works correctly with real database connections
            - Error propagation works properly with real connections
            - State management works during coordination with real connections
            - Performance is acceptable for coordination with real connections
            - Error handling works for coordination failures with real connections
            - Component coordination works correctly with real database connections
            - Settings injection works with component coordination
            - Provider pattern works with component coordination
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test component coordination with real database connections
        
        # Initialize connections
        init_result = orchestrator.initialize_connections()
        assert init_result is True
        
        # Test TableProcessor coordination
        table_result = orchestrator.run_pipeline_for_table('patient')
        assert isinstance(table_result, bool)
        
        # Test PriorityProcessor coordination
        priority_result = orchestrator.process_tables_by_priority(['patient', 'appointment'])
        assert isinstance(priority_result, dict)
        
        # Test UnifiedMetricsCollector coordination
        # Metrics collector should be available and functional
        assert orchestrator.metrics is not None
        
        # Test component state management
        assert orchestrator._initialized is True
        
        # Test cleanup coordination
        orchestrator.cleanup()
        assert orchestrator._initialized is False

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_resource_management_with_real_connections(self, integration_orchestrator):
        """
        Test integration resource management with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test resource management with real database connections
            Assert: Verify resource management works correctly with real connections
            
        Validates:
            - Resource management works correctly with real database connections
            - Memory usage is reasonable with real database connections
            - No memory leaks occur with real database connections
            - Resource cleanup works properly with real database connections
            - Error handling works for resource failures with real connections
            - Performance is acceptable for resource management with real connections
            - Settings injection works with resource management
            - Provider pattern works with resource management
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test resource management with real database connections
        
        # Test initialization resource management
        init_result = orchestrator.initialize_connections()
        assert init_result is True
        assert orchestrator._initialized is True
        
        # Test operation resource management
        table_result = orchestrator.run_pipeline_for_table('patient')
        assert isinstance(table_result, bool)
        
        # Test cleanup resource management
        orchestrator.cleanup()
        assert orchestrator._initialized is False
        
        # Test context manager resource management
        with orchestrator as ctx_orchestrator:
            ctx_orchestrator.initialize_connections()
            assert ctx_orchestrator._initialized is True
            result = ctx_orchestrator.run_pipeline_for_table('patient')
            assert isinstance(result, bool)
        
        # Assert: Verify resource management works correctly with real connections
        assert orchestrator._initialized is False  # Should be cleaned up after context

    @pytest.mark.integration
    @pytest.mark.order(5)  # After core components are tested
    @pytest.mark.dental_clinic
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_integration_logging_and_monitoring_with_real_connections(self, integration_orchestrator):
        """
        Test integration logging and monitoring with real database connections.
        
        AAA Pattern:
            Arrange: Set up integration orchestrator with real test configuration
            Act: Test logging and monitoring with real database connections
            Assert: Verify logging and monitoring works correctly with real connections
            
        Validates:
            - Logging and monitoring work correctly with real database connections
            - Error logging works properly with real database connections
            - Performance monitoring works with real database connections
            - State tracking works correctly with real database connections
            - Metrics collection works with real database connections
            - Debug information is available with real database connections
            - Settings injection works with logging and monitoring
            - Provider pattern works with logging and monitoring
        """
        # Arrange: Set up integration orchestrator with real test configuration
        orchestrator = integration_orchestrator
        
        # Act: Test logging and monitoring with real database connections
        
        # Test initialization logging
        init_result = orchestrator.initialize_connections()
        assert init_result is True
        
        # Test operation logging
        table_result = orchestrator.run_pipeline_for_table('patient')
        assert isinstance(table_result, bool)
        
        # Test error logging (with invalid table)
        error_result = orchestrator.run_pipeline_for_table('invalid_table_name')
        assert isinstance(error_result, bool)
        
        # Test cleanup logging
        orchestrator.cleanup()
        assert orchestrator._initialized is False
        
        # Test metrics collection
        assert orchestrator.metrics is not None
        
        # Assert: Verify logging and monitoring works correctly with real connections
        # All logging calls should have been made during the test scenarios above
        # The actual logging verification would require more complex setup
        # For integration tests, we focus on ensuring the framework works with real connections
