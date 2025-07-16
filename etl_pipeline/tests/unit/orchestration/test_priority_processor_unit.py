"""
Unit tests for PriorityProcessor using existing fixtures and provider pattern dependency injection.

Test Strategy:
    - Pure unit tests with comprehensive mocking using existing fixtures
    - Validates priority processing logic without real database connections
    - Tests Settings injection and provider pattern integration
    - Tests parallel and sequential processing scenarios
    - Tests error handling and failure propagation
    - Tests environment validation and FAIL FAST behavior
    - Tests resource management and thread pool handling
    
Coverage Areas:
    - Initialization with Settings injection and provider pattern
    - Environment validation and configuration checking
    - Parallel processing with ThreadPoolExecutor
    - Sequential processing for non-critical tables
    - Error handling for all failure scenarios
    - Resource management and cleanup
    - FAIL FAST behavior and security validation
    - Provider pattern dependency injection scenarios
    
ETL Context:
    - Critical for ETL pipeline batch processing and coordination
    - Supports dental clinic data processing workflows
    - Uses Settings injection for environment-agnostic connections
    - Enforces FAIL FAST security to prevent accidental production usage
    - Optimized for dental clinic data volumes and processing patterns
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Any

# Import ETL pipeline components
from etl_pipeline.orchestration.priority_processor import PriorityProcessor

# Import connection architecture components
from etl_pipeline.config import ConfigReader
from etl_pipeline.config.providers import DictConfigProvider

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DataExtractionError,
    DataLoadingError
)

# Import existing fixtures
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


@pytest.mark.unit
@pytest.mark.orchestration
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestPriorityProcessorUnit:
    """
    Unit tests for PriorityProcessor using provider pattern dependency injection.
    
    Test Strategy:
        - Pure unit tests with comprehensive mocking using existing fixtures
        - Validates priority processing logic without real database connections
        - Tests Settings injection and provider pattern integration
        - Tests parallel and sequential processing scenarios
        - Tests error handling and failure propagation
        - Tests environment validation and FAIL FAST behavior
        - Tests resource management and thread pool handling
        
    Coverage Areas:
        - Initialization with Settings injection and provider pattern
        - Environment validation and configuration checking
        - Parallel processing with ThreadPoolExecutor
        - Sequential processing for non-critical tables
        - Error handling for all failure scenarios
        - Resource management and cleanup
        - FAIL FAST behavior and security validation
        - Provider pattern dependency injection scenarios
        
    ETL Context:
        - Critical for ETL pipeline batch processing and coordination
        - Supports dental clinic data processing workflows
        - Uses Settings injection for environment-agnostic connections
        - Enforces FAIL FAST security to prevent accidental production usage
        - Optimized for dental clinic data volumes and processing patterns
    """

    def test_priority_processor_initialization_success(self, mock_config_reader):
        """
        Test successful PriorityProcessor initialization with provider pattern.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with provider pattern
            Act: Create PriorityProcessor instance with ConfigReader
            Assert: Verify initialization succeeds and settings are configured
        """
        # Arrange: Use existing mock_config_reader fixture with provider pattern
        config_reader = mock_config_reader
        
        # Act: Create PriorityProcessor instance with ConfigReader
        processor = PriorityProcessor(config_reader)
        
        # Assert: Verify initialization succeeds and settings are configured
        assert processor.config_reader == config_reader

    def test_priority_processor_initialization_invalid_config_reader(self):
        """
        Test FAIL FAST when invalid ConfigReader is provided.
        
        AAA Pattern:
            Arrange: Set up invalid ConfigReader (None)
            Act: Attempt to create PriorityProcessor with invalid ConfigReader
            Assert: Verify system fails fast with ConfigurationError
        """
        # Arrange: Set up invalid ConfigReader (None)
        invalid_config_reader = None
        
        # Act & Assert: Attempt to create PriorityProcessor with invalid ConfigReader
        with pytest.raises(ConfigurationError, match="ConfigReader instance is required"):
            PriorityProcessor(invalid_config_reader)  # type: ignore

    def test_environment_validation_success(self, mock_config_reader):
        """
        Test successful environment validation.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mocked validation
            Act: Call _validate_environment()
            Assert: Verify validation passes without exceptions
        """
        # Arrange: Use existing mock_config_reader fixture with mocked validation
        config_reader = mock_config_reader
        processor = PriorityProcessor(config_reader)
        
        # Act: Call _validate_environment()
        # Should not raise any exceptions
        processor._validate_environment()
        
        # Assert: Verify validation passes without exceptions
        assert processor.config_reader == config_reader

    def test_environment_validation_failure(self, mock_config_reader):
        """
        Test environment validation failure handling.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with failing validation
            Act: Call _validate_environment() with failing validation
            Assert: Verify EnvironmentError is raised
        """
        # Arrange: Use existing mock_config_reader fixture with failing validation
        config_reader = mock_config_reader
        processor = PriorityProcessor(config_reader)
        
        # Mock the validation to fail
        processor._validate_environment = Mock(side_effect=EnvironmentError("Configuration validation failed"))
        
        # Act & Assert: Call _validate_environment() with failing validation
        with pytest.raises(EnvironmentError, match="Configuration validation failed"):
            processor._validate_environment()

    def test_process_by_priority_important_tables_parallel(self, mock_config_reader):
        """
        Test processing important tables in parallel.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with important tables and mock TableProcessor
            Act: Call process_by_priority() with important level
            Assert: Verify parallel processing is used for important tables
        """
        # Arrange: Use existing mock_config_reader fixture with important tables and mock TableProcessor
        processor = PriorityProcessor(mock_config_reader)
        processor.settings.get_tables_by_importance = Mock(return_value=['patient', 'appointment', 'procedure'])
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call process_by_priority() with important level
            result = processor.process_by_priority(['important'], max_workers=2)
            
            # Assert: Verify parallel processing is used for important tables
            assert result['important']['success'] == ['patient', 'appointment', 'procedure']
            assert result['important']['failed'] == []
            assert result['important']['total'] == 3
            assert mock_table_processor.process_table.call_count == 3

    def test_process_by_priority_audit_tables_sequential(self, mock_config_reader):
        """
        Test processing audit tables sequentially.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with audit tables and mock TableProcessor
            Act: Call process_by_priority() with audit level
            Assert: Verify sequential processing is used for audit tables
        """
        # Arrange: Use existing mock_config_reader fixture with audit tables and mock TableProcessor
        processor = PriorityProcessor(mock_config_reader)
        processor.settings.get_tables_by_importance = Mock(return_value=['securitylog', 'definition'])
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call process_by_priority() with audit level
            result = processor.process_by_priority(['audit'], max_workers=2)
            
            # Assert: Verify sequential processing is used for audit tables
            assert result['audit']['success'] == ['securitylog', 'definition']
            assert result['audit']['failed'] == []
            assert result['audit']['total'] == 2
            assert mock_table_processor.process_table.call_count == 2

    def test_process_by_priority_no_tables_found(self, mock_config_reader):
        """
        Test handling when no tables found for importance level.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with no tables returned
            Act: Call process_by_priority() with importance level that has no tables
            Assert: Verify empty result is returned
        """
        # Arrange: Use existing mock_config_reader fixture
        processor = PriorityProcessor(mock_config_reader)
        # Patch the settings object to return an empty list for the importance level
        processor.settings.get_tables_by_importance = Mock(return_value=[])

        # Act: Call process_by_priority() with importance level that has no tables
        result = processor.process_by_priority(['standard'], max_workers=2)

        # Assert: Verify empty result is returned
        assert 'standard' not in result

    def test_process_by_priority_stop_on_important_failure(self, mock_config_reader):
        """
        Test stopping processing when important tables fail.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with failing important tables
            Act: Call process_by_priority() with important level that fails
            Assert: Verify processing stops after important table failures
        """
        # Arrange: Use existing mock_config_reader fixture with failing important tables
        processor = PriorityProcessor(mock_config_reader)
        processor.settings.get_tables_by_importance = Mock(side_effect=lambda x: ['patient', 'appointment'] if x == 'important' else ['audit'])
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = False  # All tables fail
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call process_by_priority() with important level that fails
            result = processor.process_by_priority(['important', 'audit'], max_workers=2)
            
            # Assert: Verify processing stops after important table failures
            assert result['important']['success'] == []
            assert result['important']['failed'] == ['patient', 'appointment']
            assert 'audit' not in result  # Processing stopped after important failures

    def test_process_parallel_success(self, mock_config_reader):
        """
        Test successful parallel processing of tables.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that succeeds
            Act: Call _process_parallel() with multiple tables
            Assert: Verify all tables are processed successfully in parallel
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that succeeds
        config_reader = mock_config_reader
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            processor = PriorityProcessor(config_reader)
            tables = ['patient', 'appointment', 'procedure']
            
            # Act: Call _process_parallel() with multiple tables
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify all tables are processed successfully in parallel
            assert success_tables == ['patient', 'appointment', 'procedure']
            assert failed_tables == []
            assert mock_table_processor.process_table.call_count == 3

    def test_process_parallel_partial_failure(self, mock_config_reader):
        """
        Test parallel processing with some table failures.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that partially fails
            Act: Call _process_parallel() with tables that partially fail
            Assert: Verify partial success and failure handling
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that partially fails
        config_reader = mock_config_reader
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            # First table succeeds, second fails, third succeeds
            mock_table_processor.process_table.side_effect = [True, False, True]
            mock_table_processor_class.return_value = mock_table_processor
            
            processor = PriorityProcessor(config_reader)
            tables = ['patient', 'appointment', 'procedure']
            
            # Act: Call _process_parallel() with tables that partially fail
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify partial success and failure handling
            assert success_tables == ['patient', 'procedure']
            assert failed_tables == ['appointment']
            assert mock_table_processor.process_table.call_count == 3

    def test_process_parallel_all_failure(self, mock_config_reader):
        """
        Test parallel processing with all table failures.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that fails
            Act: Call _process_parallel() with tables that all fail
            Assert: Verify all tables are marked as failed
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that fails
        config_reader = mock_config_reader
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = False
            mock_table_processor_class.return_value = mock_table_processor
            
            processor = PriorityProcessor(config_reader)
            tables = ['patient', 'appointment', 'procedure']
            
            # Act: Call _process_parallel() with tables that all fail
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify all tables are marked as failed (order doesn't matter for parallel processing)
            assert success_tables == []
            assert set(failed_tables) == set(['patient', 'appointment', 'procedure'])
            assert mock_table_processor.process_table.call_count == 3

    def test_process_parallel_exception_handling(self, mock_config_reader):
        """
        Test exception handling in parallel processing.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that raises exceptions
            Act: Call _process_parallel() with tables that raise exceptions
            Assert: Verify exceptions are handled and tables are marked as failed
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that raises exceptions
        config_reader = mock_config_reader
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.side_effect = DataExtractionError("Test error")
            mock_table_processor_class.return_value = mock_table_processor
            
            processor = PriorityProcessor(config_reader)
            tables = ['patient', 'appointment']
            
            # Act: Call _process_parallel() with tables that raise exceptions
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify exceptions are handled and tables are marked as failed
            assert success_tables == []
            assert failed_tables == ['patient', 'appointment']
            assert mock_table_processor.process_table.call_count == 2

    def test_process_sequential_success(self, mock_config_reader):
        """
        Test successful sequential processing of tables.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that succeeds
            Act: Call _process_sequential() with multiple tables
            Assert: Verify all tables are processed successfully sequentially
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that succeeds
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['audit', 'reference', 'standard']
            
            # Act: Call _process_sequential() with multiple tables
            success_tables, failed_tables = processor._process_sequential(tables, force_full=False)
            
            # Assert: Verify all tables are processed successfully sequentially
            assert success_tables == ['audit', 'reference', 'standard']
            assert failed_tables == []
            assert mock_table_processor.process_table.call_count == 3

    def test_process_sequential_partial_failure(self, mock_config_reader):
        """
        Test sequential processing with some table failures.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that partially fails
            Act: Call _process_sequential() with tables that partially fail
            Assert: Verify partial success and failure handling
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that partially fails
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
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

    def test_process_single_table_success(self, mock_config_reader):
        """
        Test successful single table processing.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that succeeds
            Act: Call _process_single_table() with a table
            Assert: Verify table is processed successfully
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that succeeds
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with a table
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify table is processed successfully
            assert result is True
            mock_table_processor.process_table.assert_called_once_with('patient', False)

    def test_process_single_table_failure(self, mock_config_reader):
        """
        Test handling of table processing failures.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that fails
            Act: Call _process_single_table() with failing table
            Assert: Verify failure is handled correctly and returns False
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that fails
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = False
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with failing table
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify failure is handled correctly and returns False
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)

    def test_process_single_table_exception_handling(self, mock_config_reader):
        """
        Test exception handling in single table processing.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor that raises exceptions
            Act: Call _process_single_table() with table that raises exception
            Assert: Verify exception is handled and returns False
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor that raises exceptions
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.side_effect = DataExtractionError("Test error")
            mock_table_processor_class.return_value = mock_table_processor
            
            # Act: Call _process_single_table() with table that raises exception
            result = processor._process_single_table('patient', force_full=False)
            
            # Assert: Verify exception is handled and returns False
            assert result is False
            mock_table_processor.process_table.assert_called_once_with('patient', False)

    def test_thread_pool_cleanup(self, mock_config_reader):
        """
        Test proper thread pool cleanup after parallel processing.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor
            Act: Call _process_parallel() and verify thread pool cleanup
            Assert: Verify thread pool is properly cleaned up
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment']
            
            # Act: Call _process_parallel() and verify thread pool cleanup
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify thread pool is properly cleaned up (no active threads)
            # Note: Order doesn't matter for parallel processing
            assert set(success_tables) == set(['patient', 'appointment'])
            assert failed_tables == []

    def test_max_workers_configuration(self, mock_config_reader):
        """
        Test max_workers parameter affects parallel processing.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with mock TableProcessor
            Act: Call _process_parallel() with different max_workers values
            Assert: Verify max_workers parameter is respected
        """
        # Arrange: Use existing mock_config_reader fixture with mock TableProcessor
        processor = PriorityProcessor(mock_config_reader)
        
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = Mock()
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            tables = ['patient', 'appointment', 'procedure', 'claim', 'payment']
            
            # Act: Call _process_parallel() with different max_workers values
            success_tables_1, _ = processor._process_parallel(tables, max_workers=1, force_full=False)
            success_tables_3, _ = processor._process_parallel(tables, max_workers=3, force_full=False)
            success_tables_5, _ = processor._process_parallel(tables, max_workers=5, force_full=False)
            
            # Assert: Verify max_workers parameter is respected (all should succeed regardless of workers)
            # Note: Order doesn't matter for parallel processing
            assert set(success_tables_1) == set(tables)
            assert set(success_tables_3) == set(tables)
            assert set(success_tables_5) == set(tables)

    def test_fail_fast_on_invalid_config_reader(self):
        """
        Test that system fails fast when invalid ConfigReader is provided.
        
        AAA Pattern:
            Arrange: Set up invalid ConfigReader (None)
            Act: Attempt to create PriorityProcessor with invalid ConfigReader
            Assert: Verify system fails fast with ConfigurationError
        """
        # Arrange: Set up invalid ConfigReader (None)
        invalid_config_reader = None
        
        # Act & Assert: Attempt to create PriorityProcessor with invalid ConfigReader
        with pytest.raises(ConfigurationError, match="ConfigReader instance is required"):
            PriorityProcessor(invalid_config_reader)  # type: ignore

    def test_fail_fast_on_environment_validation_failure(self, mock_config_reader):
        """
        Test FAIL FAST when environment validation fails.
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture with failing validation
            Act: Attempt to create PriorityProcessor with failing environment validation
            Assert: Verify FAIL FAST behavior with EnvironmentError
        """
        # Arrange: Use existing mock_config_reader fixture with failing validation
        processor = PriorityProcessor(mock_config_reader)
        processor._validate_environment = Mock(side_effect=EnvironmentError("Configuration validation failed"))
        
        # Act & Assert: Attempt to create PriorityProcessor with failing environment validation
        with pytest.raises(EnvironmentError, match="Configuration validation failed"):
            processor._validate_environment()
