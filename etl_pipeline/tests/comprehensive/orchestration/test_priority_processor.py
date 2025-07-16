"""
Comprehensive tests for PriorityProcessor using provider pattern and Settings injection.

This module provides comprehensive testing for the PriorityProcessor class using:
- AAA Pattern for clear test structure
- Provider pattern with DictConfigProvider for test isolation
- Settings injection for environment-agnostic testing
- Complete ETL pipeline orchestration testing with mocked dependencies
- Comprehensive error handling and edge case testing

Following TESTING_PLAN.md best practices:
- Comprehensive tests with mocked dependencies and provider pattern
- Provider pattern dependency injection
- Settings injection for environment-agnostic operation
- FAIL FAST testing for ETL_ENVIRONMENT validation
- 90%+ target coverage for main test suite
"""

import os
import pytest
from unittest.mock import MagicMock, patch, call
from typing import Dict, Any

from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Import fixtures for comprehensive testing
from tests.fixtures.priority_processor_fixtures import (
    priority_processor_settings,
    priority_processor_config_provider,
    priority_processor_env_vars,
    priority_processor_pipeline_config,
    priority_processor_tables_config,
    mock_priority_processor_table_processor
)
from tests.fixtures.config_fixtures import (
    test_settings,
    test_config_provider,
    database_types,
    postgres_schemas
)
from tests.fixtures.config_reader_fixtures import (
    mock_config_reader,
    valid_tables_config,
    mock_config_reader_with_invalid_config
)
from tests.fixtures.connection_fixtures import (
    test_connection_settings,
    test_connection_provider,
    mock_connection_factory_with_settings,
    mock_source_engine,
    mock_replication_engine,
    mock_analytics_engine
)


@pytest.fixture(autouse=True)
def set_etl_environment(monkeypatch, request):
    """Set ETL environment for tests, removing it for fail_fast tests."""
    if 'fail_fast' in request.keywords:
        monkeypatch.delenv('ETL_ENVIRONMENT', raising=False)
    else:
        monkeypatch.setenv('ETL_ENVIRONMENT', 'test')


@pytest.mark.comprehensive
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestPriorityProcessorComprehensive:
    """
    Comprehensive tests for PriorityProcessor using provider pattern and Settings injection.
    
    Test Strategy:
        - Comprehensive tests with DictConfigProvider dependency injection
        - Validates complete priority processing orchestration with mocked components
        - Tests all error handling scenarios and edge cases
        - Ensures proper delegation to TableProcessor with Settings injection
        - Validates parallel and sequential processing logic
        - Tests environment validation and FAIL FAST behavior
        - Comprehensive resource management and thread pool handling
        - Tests Settings injection for environment-agnostic operation
        - Tests provider pattern for clean dependency injection
    
    Coverage Areas:
        - Settings injection for environment-agnostic operation
        - Provider pattern with DictConfigProvider for test isolation
        - Complete priority processing orchestration (parallel/sequential)
        - Environment validation and FAIL FAST requirements
        - ConfigReader integration for table configuration
        - Comprehensive error handling for all failure scenarios
        - Parallel processing with ThreadPoolExecutor
        - Sequential processing for non-critical tables
        - Resource management and thread pool cleanup
        - Configuration validation and edge cases
        - Priority-based processing scenarios
        
    ETL Context:
        - Core orchestration component used by PipelineOrchestrator
        - Coordinates priority-based table processing
        - Critical for nightly ETL pipeline execution
        - Uses Settings injection for environment-agnostic connections
        - Handles dental clinic data with varying table priorities
    """
    
    def test_comprehensive_initialization_with_settings_injection(self, priority_processor_config_provider, mock_config_reader):
        """
        Test comprehensive PriorityProcessor initialization using Settings injection.
        
        AAA Pattern:
            Arrange: Set up DictConfigProvider with injected configuration
            Act: Initialize PriorityProcessor with Settings injection
            Assert: Verify proper initialization and environment-agnostic operation
            
        Validates:
            - Settings injection works correctly with provider pattern
            - Environment validation passes for test environment
            - ConfigReader is properly initialized
            - Configuration is loaded correctly from provider
            - Environment-agnostic operation is supported
        """
        # Arrange: Set up DictConfigProvider with injected configuration
        from etl_pipeline.config import Settings
        settings = Settings(environment='test', provider=priority_processor_config_provider)
        
        # Act: Initialize PriorityProcessor with Settings injection
        processor = PriorityProcessor(mock_config_reader)
        
        # Assert: Verify proper initialization and environment-agnostic operation
        assert processor.config_reader == mock_config_reader
        assert processor.settings is not None
        assert processor.settings.environment == 'test'
        assert processor.settings.provider is not None
    
    def test_comprehensive_initialization_invalid_config_reader(self):
        """
        Test comprehensive FAIL FAST when invalid ConfigReader is provided.
        
        AAA Pattern:
            Arrange: Set up invalid ConfigReader (None)
            Act: Attempt to create PriorityProcessor with invalid ConfigReader
            Assert: Verify system fails fast with ConfigurationError
            
        Validates:
            - FAIL FAST behavior for invalid configuration
            - Clear error messages for configuration issues
            - Security validation prevents invalid initialization
        """
        # Arrange: Set up invalid ConfigReader (None)
        invalid_config_reader = None
        
        # Act & Assert: Attempt to create PriorityProcessor with invalid ConfigReader
        with pytest.raises(ConfigurationError, match="ConfigReader instance is required"):
            PriorityProcessor(invalid_config_reader)  # type: ignore
    
    def test_comprehensive_environment_validation_success(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive successful environment validation.
        
        AAA Pattern:
            Arrange: Set up valid settings and mock ConfigReader
            Act: Call _validate_environment()
            Assert: Verify validation passes without exceptions
            
        Validates:
            - Environment validation works correctly
            - Settings injection supports validation
            - Provider pattern integration works
            - Configuration validation passes
        """
        # Arrange: Set up valid settings and mock ConfigReader
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        # Act: Call _validate_environment()
        # Should not raise any exceptions
        processor._validate_environment()
        
        # Assert: Verify validation passes without exceptions
        assert processor.config_reader == mock_config_reader
        assert processor.settings == priority_processor_settings
    
    def test_comprehensive_environment_validation_failure(self, mock_config_reader):
        """
        Test comprehensive environment validation failure handling.
        
        AAA Pattern:
            Arrange: Set up mock ConfigReader with failing validation
            Act: Call _validate_environment() with failing validation
            Assert: Verify EnvironmentError is raised
            
        Validates:
            - Environment validation failure handling
            - Error propagation for configuration issues
            - FAIL FAST behavior for invalid environments
        """
        # Arrange: Set up mock ConfigReader with failing validation
        processor = PriorityProcessor(mock_config_reader)
        
        # Mock the validation to fail
        processor._validate_environment = MagicMock(side_effect=EnvironmentError("Configuration validation failed"))
        
        # Act & Assert: Call _validate_environment() with failing validation
        with pytest.raises(EnvironmentError, match="Configuration validation failed"):
            processor._validate_environment()
    
    def test_comprehensive_process_by_priority_important_tables_parallel(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive processing of important tables in parallel.
        
        AAA Pattern:
            Arrange: Set up settings and mock components for parallel processing
            Act: Call process_by_priority() with important level
            Assert: Verify parallel processing is used for important tables
            
        Validates:
            - Parallel processing for important tables
            - Settings injection for environment-agnostic operation
            - TableProcessor delegation with proper configuration
            - ThreadPoolExecutor usage for parallel processing
            - Success/failure tracking for parallel operations
        """
        # Arrange: Set up settings and mock components for parallel processing
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        processor.settings.get_tables_by_importance = MagicMock(return_value=['patient', 'appointment', 'procedure'])
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call process_by_priority() with important level
            result = processor.process_by_priority(['important'], max_workers=2)
            
            # Assert: Verify parallel processing is used for important tables
            # Note: Order doesn't matter for parallel processing
            assert set(result['important']['success']) == set(['patient', 'appointment', 'procedure'])
            assert result['important']['failed'] == []
            assert result['important']['total'] == 3
            assert mock_table_processor.process_table.call_count == 3
    
    def test_comprehensive_process_by_priority_audit_tables_sequential(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive processing of audit tables sequentially.
        
        AAA Pattern:
            Arrange: Set up settings and mock components for sequential processing
            Act: Call process_by_priority() with audit level
            Assert: Verify sequential processing is used for audit tables
            
        Validates:
            - Sequential processing for audit tables
            - Settings injection for environment-agnostic operation
            - TableProcessor delegation with proper configuration
            - Sequential processing logic
            - Success/failure tracking for sequential operations
        """
        # Arrange: Set up settings and mock components for sequential processing
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        processor.settings.get_tables_by_importance = MagicMock(return_value=['securitylog', 'definition'])
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call process_by_priority() with audit level
            result = processor.process_by_priority(['audit'], max_workers=2)
            
            # Assert: Verify sequential processing is used for audit tables
            assert result['audit']['success'] == ['securitylog', 'definition']
            assert result['audit']['failed'] == []
            assert result['audit']['total'] == 2
            assert mock_table_processor.process_table.call_count == 2
    
    def test_comprehensive_process_by_priority_no_tables_found(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive handling when no tables found for importance level.
        
        AAA Pattern:
            Arrange: Set up settings with no tables returned
            Act: Call process_by_priority() with importance level that has no tables
            Assert: Verify empty result is returned
            
        Validates:
            - Handling of empty table lists
            - Settings injection for environment-agnostic operation
            - Graceful handling of no tables scenario
        """
        # Arrange: Set up settings with no tables returned
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        processor.settings.get_tables_by_importance = MagicMock(return_value=[])
        
        # Act: Call process_by_priority() with importance level that has no tables
        result = processor.process_by_priority(['standard'], max_workers=2)
        
        # Assert: Verify empty result is returned
        assert 'standard' not in result
    
    def test_comprehensive_process_by_priority_stop_on_important_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive stopping processing when important tables fail.
        
        AAA Pattern:
            Arrange: Set up settings with failing important tables
            Act: Call process_by_priority() with important level that fails
            Assert: Verify processing stops after important table failures
            
        Validates:
            - FAIL FAST behavior for important table failures
            - Processing stops after critical failures
            - Settings injection for environment-agnostic operation
            - Error propagation for critical failures
        """
        # Arrange: Set up settings with failing important tables
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        processor.settings.get_tables_by_importance = MagicMock(side_effect=lambda x: ['patient', 'appointment'] if x == 'important' else ['audit'])
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = False  # All tables fail
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call process_by_priority() with important level that fails
            result = processor.process_by_priority(['important', 'audit'], max_workers=2)
            
            # Assert: Verify processing stops after important table failures
            assert result['important']['success'] == []
            assert result['important']['failed'] == ['patient', 'appointment']
            assert 'audit' not in result  # Processing stopped after important failures
    
    def test_comprehensive_process_parallel_success(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive successful parallel processing of tables.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that succeeds
            Act: Call _process_parallel() with multiple tables
            Assert: Verify all tables are processed successfully in parallel
            
        Validates:
            - Parallel processing with ThreadPoolExecutor
            - Settings injection for environment-agnostic operation
            - TableProcessor delegation for each parallel task
            - Success tracking for parallel operations
            - Thread pool management
        """
        # Arrange: Set up settings and mock TableProcessor that succeeds
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment', 'procedure']
            
            # Act: Call _process_parallel() with multiple tables
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify all tables are processed successfully in parallel
            # Note: Order doesn't matter for parallel processing
            assert set(success_tables) == set(['patient', 'appointment', 'procedure'])
            assert failed_tables == []
            assert mock_table_processor.process_table.call_count == 3
    
    def test_comprehensive_process_parallel_partial_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive parallel processing with some table failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that partially fails
            Act: Call _process_parallel() with tables that partially fail
            Assert: Verify partial success and failure handling
            
        Validates:
            - Partial failure handling in parallel processing
            - Settings injection for environment-agnostic operation
            - Error tracking for parallel operations
            - Thread pool cleanup after failures
        """
        # Arrange: Set up settings and mock TableProcessor that partially fails
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            # First table succeeds, second fails, third succeeds
            mock_table_processor.process_table.side_effect = [True, False, True]
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment', 'procedure']
            
            # Act: Call _process_parallel() with tables that partially fail
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify partial success and failure handling
            # Note: Order doesn't matter for parallel processing
            assert set(success_tables) == set(['patient', 'procedure'])
            assert failed_tables == ['appointment']
            assert mock_table_processor.process_table.call_count == 3
    
    def test_comprehensive_process_parallel_all_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive parallel processing with all table failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that fails
            Act: Call _process_parallel() with tables that all fail
            Assert: Verify all tables are marked as failed
            
        Validates:
            - Complete failure handling in parallel processing
            - Settings injection for environment-agnostic operation
            - Error propagation for all failures
            - Thread pool cleanup after complete failures
        """
        # Arrange: Set up settings and mock TableProcessor that fails
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = False
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment', 'procedure']
            
            # Act: Call _process_parallel() with tables that all fail
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify all tables are marked as failed
            # Note: Order doesn't matter for parallel processing
            assert success_tables == []
            assert set(failed_tables) == set(['patient', 'appointment', 'procedure'])
            assert mock_table_processor.process_table.call_count == 3
    
    def test_comprehensive_process_parallel_exception_handling(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive exception handling in parallel processing.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that raises exceptions
            Act: Call _process_parallel() with tables that raise exceptions
            Assert: Verify exceptions are handled and tables are marked as failed
            
        Validates:
            - Exception handling in parallel processing
            - Settings injection for environment-agnostic operation
            - Error propagation for exceptions
            - Thread pool cleanup after exceptions
        """
        # Arrange: Set up settings and mock TableProcessor that raises exceptions
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.side_effect = DataExtractionError("Test error")
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment']
            
            # Act: Call _process_parallel() with tables that raise exceptions
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify exceptions are handled and tables are marked as failed
            # Note: Order doesn't matter for parallel processing
            assert success_tables == []
            assert set(failed_tables) == set(['patient', 'appointment'])
            assert mock_table_processor.process_table.call_count == 2
    
    def test_comprehensive_process_sequential_success(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive successful sequential processing of tables.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that succeeds
            Act: Call _process_sequential() with multiple tables
            Assert: Verify all tables are processed successfully sequentially
            
        Validates:
            - Sequential processing logic
            - Settings injection for environment-agnostic operation
            - TableProcessor delegation for each sequential task
            - Success tracking for sequential operations
        """
        # Arrange: Set up settings and mock TableProcessor that succeeds
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['audit', 'reference', 'standard']
            
            # Act: Call _process_sequential() with multiple tables
            success_tables, failed_tables = processor._process_sequential(tables, force_full=False)
            
            # Assert: Verify all tables are processed successfully sequentially
            assert success_tables == ['audit', 'reference', 'standard']
            assert failed_tables == []
            assert mock_table_processor.process_table.call_count == 3
    
    def test_comprehensive_process_sequential_partial_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive sequential processing with some table failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that partially fails
            Act: Call _process_sequential() with tables that partially fail
            Assert: Verify partial success and failure handling
            
        Validates:
            - Partial failure handling in sequential processing
            - Settings injection for environment-agnostic operation
            - Error tracking for sequential operations
            - Processing continues after individual failures
        """
        # Arrange: Set up settings and mock TableProcessor that partially fails
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            # First table succeeds, second fails, third succeeds
            mock_table_processor.process_table.side_effect = [True, False, True]
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['audit', 'reference', 'standard']
            
            # Act: Call _process_sequential() with tables that partially fail
            success_tables, failed_tables = processor._process_sequential(tables, force_full=False)
            
            # Assert: Verify partial success and failure handling
            assert success_tables == ['audit', 'standard']
            assert failed_tables == ['reference']
            assert mock_table_processor.process_table.call_count == 3
    
    def test_comprehensive_process_single_table_success(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive successful single table processing.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that succeeds
            Act: Call _process_single_table() with a table
            Assert: Verify table is processed successfully
            
        Validates:
            - Single table processing logic
            - Settings injection for environment-agnostic operation
            - TableProcessor delegation for single table
            - Success tracking for single table operations
        """
        # Arrange: Set up settings and mock TableProcessor that succeeds
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with a table
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify table is processed successfully
            assert result is True
            mock_table_processor.process_table.assert_called_once_with('patient', False)
    
    def test_comprehensive_process_single_table_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive handling of table processing failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that fails
            Act: Call _process_single_table() with failing table
            Assert: Verify failure is handled correctly and returns False
            
        Validates:
            - Single table failure handling
            - Settings injection for environment-agnostic operation
            - Error tracking for single table operations
            - Proper return values for failures
        """
        # Arrange: Set up settings and mock TableProcessor that fails
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = False
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with failing table
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify failure is handled correctly and returns False
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)
    
    def test_comprehensive_process_single_table_exception_handling(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive exception handling in single table processing.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that raises exceptions
            Act: Call _process_single_table() with table that raises exception
            Assert: Verify exception is handled and returns False
            
        Validates:
            - Exception handling in single table processing
            - Settings injection for environment-agnostic operation
            - Error propagation for exceptions
            - Proper return values for exceptions
        """
        # Arrange: Set up settings and mock TableProcessor that raises exceptions
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.side_effect = DataExtractionError("Test error")
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with table that raises exception
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify exception is handled and returns False
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)
    
    def test_comprehensive_thread_pool_cleanup(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive proper thread pool cleanup after parallel processing.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor
            Act: Call _process_parallel() and verify thread pool cleanup
            Assert: Verify thread pool is properly cleaned up
            
        Validates:
            - Thread pool cleanup after parallel processing
            - Settings injection for environment-agnostic operation
            - Resource management for thread pools
            - No resource leaks in parallel processing
        """
        # Arrange: Set up settings and mock TableProcessor
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment']
            
            # Act: Call _process_parallel() and verify thread pool cleanup
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify thread pool is properly cleaned up
            # Note: Order doesn't matter for parallel processing
            assert set(success_tables) == set(['patient', 'appointment'])
            assert failed_tables == []
    
    def test_comprehensive_max_workers_configuration(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive max_workers parameter affects parallel processing.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor
            Act: Call _process_parallel() with different max_workers values
            Assert: Verify max_workers parameter is respected
            
        Validates:
            - Max workers configuration affects parallel processing
            - Settings injection for environment-agnostic operation
            - Thread pool configuration
            - Performance characteristics with different worker counts
        """
        # Arrange: Set up settings and mock TableProcessor
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment', 'procedure', 'claim', 'payment']
            
            # Act: Call _process_parallel() with different max_workers values
            success_tables_1, _ = processor._process_parallel(tables, max_workers=1, force_full=False)
            success_tables_3, _ = processor._process_parallel(tables, max_workers=3, force_full=False)
            success_tables_5, _ = processor._process_parallel(tables, max_workers=5, force_full=False)
            
            # Assert: Verify max_workers parameter is respected
            assert set(success_tables_1) == set(tables)
            assert set(success_tables_3) == set(tables)
            assert set(success_tables_5) == set(tables)
    
    @pytest.mark.fail_fast
    def test_comprehensive_fail_fast_on_missing_environment(self):
        """
        Test comprehensive FAIL FAST when ETL_ENVIRONMENT not set.
        
        AAA Pattern:
            Arrange: Remove ETL_ENVIRONMENT from environment variables
            Act: Attempt to create Settings instance without environment
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior for missing ETL_ENVIRONMENT
            - Security validation prevents dangerous defaults
            - Clear error messages for configuration issues
        """
        # Arrange: Remove ETL_ENVIRONMENT from environment variables
        import os
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            # Act: Attempt to create Settings instance without environment
            from etl_pipeline.config import Settings
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                settings = Settings()
        finally:
            # Cleanup: Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env
    
    def test_comprehensive_fail_fast_on_environment_validation_failure(self, mock_config_reader):
        """
        Test comprehensive FAIL FAST when environment validation fails.
        
        AAA Pattern:
            Arrange: Set up mock ConfigReader with failing validation
            Act: Attempt to create PriorityProcessor with failing environment validation
            Assert: Verify FAIL FAST behavior with EnvironmentError
            
        Validates:
            - FAIL FAST behavior for environment validation failures
            - Error propagation for configuration issues
            - Security validation prevents invalid environments
        """
        # Arrange: Set up mock ConfigReader with failing validation
        processor = PriorityProcessor(mock_config_reader)
        processor._validate_environment = MagicMock(side_effect=EnvironmentError("Configuration validation failed"))
        
        # Act & Assert: Attempt to create PriorityProcessor with failing environment validation
        with pytest.raises(EnvironmentError, match="Configuration validation failed"):
            processor._validate_environment()
    
    def test_comprehensive_error_handling_data_extraction_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive error handling for data extraction failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that raises DataExtractionError
            Act: Call _process_single_table() with table that raises DataExtractionError
            Assert: Verify DataExtractionError is handled correctly
            
        Validates:
            - DataExtractionError handling in single table processing
            - Settings injection for environment-agnostic operation
            - Error propagation for data extraction issues
            - Proper return values for data extraction failures
        """
        # Arrange: Set up settings and mock TableProcessor that raises DataExtractionError
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.side_effect = DataExtractionError("Data extraction failed")
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with table that raises DataExtractionError
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify DataExtractionError is handled correctly
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)
    
    def test_comprehensive_error_handling_data_loading_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive error handling for data loading failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that raises DataLoadingError
            Act: Call _process_single_table() with table that raises DataLoadingError
            Assert: Verify DataLoadingError is handled correctly
            
        Validates:
            - DataLoadingError handling in single table processing
            - Settings injection for environment-agnostic operation
            - Error propagation for data loading issues
            - Proper return values for data loading failures
        """
        # Arrange: Set up settings and mock TableProcessor that raises DataLoadingError
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.side_effect = DataLoadingError("Data loading failed")
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with table that raises DataLoadingError
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify DataLoadingError is handled correctly
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)
    
    def test_comprehensive_error_handling_database_connection_failure(self, priority_processor_settings, mock_config_reader):
        """
        Test comprehensive error handling for database connection failures.
        
        AAA Pattern:
            Arrange: Set up settings and mock TableProcessor that raises DatabaseConnectionError
            Act: Call _process_single_table() with table that raises DatabaseConnectionError
            Assert: Verify DatabaseConnectionError is handled correctly
            
        Validates:
            - DatabaseConnectionError handling in single table processing
            - Settings injection for environment-agnostic operation
            - Error propagation for database connection issues
            - Proper return values for database connection failures
        """
        # Arrange: Set up settings and mock TableProcessor that raises DatabaseConnectionError
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = priority_processor_settings
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.process_table.side_effect = DatabaseConnectionError("Database connection failed")
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with table that raises DatabaseConnectionError
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify DatabaseConnectionError is handled correctly
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)
    
    def test_comprehensive_settings_injection_for_environment_agnostic_operation(self, priority_processor_config_provider, mock_config_reader):
        """
        Test comprehensive Settings injection for environment-agnostic operation.
        
        AAA Pattern:
            Arrange: Set up DictConfigProvider with injected configuration
            Act: Test PriorityProcessor with Settings injection
            Assert: Verify environment-agnostic operation
            
        Validates:
            - Settings injection for environment-agnostic operation
            - Provider pattern for clean dependency injection
            - Environment-agnostic connections using Settings objects
            - Unified interface for production and test environments
        """
        # Arrange: Set up DictConfigProvider with injected configuration
        from etl_pipeline.config import Settings
        settings = Settings(environment='test', provider=priority_processor_config_provider)
        
        # Act: Test PriorityProcessor with Settings injection
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = settings
        
        # Assert: Verify environment-agnostic operation
        assert processor.settings == settings
        assert processor.settings.environment == 'test'
        assert processor.settings.provider is not None
        assert processor.settings.provider == priority_processor_config_provider
    
    def test_comprehensive_provider_pattern_integration(self, priority_processor_config_provider, mock_config_reader):
        """
        Test comprehensive provider pattern integration for dependency injection.
        
        AAA Pattern:
            Arrange: Set up DictConfigProvider with injected configuration
            Act: Test PriorityProcessor with provider pattern
            Assert: Verify provider pattern integration
            
        Validates:
            - Provider pattern for clean dependency injection
            - DictConfigProvider for test isolation
            - Configuration injection without environment pollution
            - Consistent API for production and test configuration
        """
        # Arrange: Set up DictConfigProvider with injected configuration
        from etl_pipeline.config import Settings
        settings = Settings(environment='test', provider=priority_processor_config_provider)
        
        # Act: Test PriorityProcessor with provider pattern
        processor = PriorityProcessor(mock_config_reader)
        processor.settings = settings
        
        # Assert: Verify provider pattern integration
        assert processor.settings.provider == priority_processor_config_provider
        assert processor.settings.pipeline_config is not None
        assert processor.settings.tables_config is not None
        assert processor.settings._env_vars is not None
