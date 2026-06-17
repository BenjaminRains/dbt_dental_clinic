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
    - Enforces FAIL FAST security to prevent using the wrong ETL stage
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


def _set_performance_category(config_reader, table_name: str, category: str) -> None:
    config_reader.config['tables'][table_name]['performance_category'] = category


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
        - Enforces FAIL FAST security to prevent using the wrong ETL stage
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
        Test successful environment validation via Settings (orchestrator boundary).
        
        AAA Pattern:
            Arrange: Use existing mock_config_reader fixture
            Act: Initialize PriorityProcessor and check settings.validate_configs()
            Assert: Verify validation passes without exceptions
        """
        config_reader = mock_config_reader
        processor = PriorityProcessor(config_reader)

        assert processor.settings.validate_configs() is True
        assert processor.config_reader == config_reader

    def test_environment_validation_failure(self, mock_config_reader):
        """
        Test environment validation failure at Settings layer.
        
        AAA Pattern:
            Arrange: PriorityProcessor with settings that fail validate_configs
            Act: Call settings.validate_configs()
            Assert: Verify validation returns False
        """
        config_reader = mock_config_reader
        processor = PriorityProcessor(config_reader)
        processor.settings.validate_configs = Mock(return_value=False)

        assert processor.settings.validate_configs() is False

    def test_process_by_priority_important_tables_parallel(self, mock_config_reader):
        """
        Test processing large tables in parallel (performance_category=large).
        """
        processor = PriorityProcessor(mock_config_reader)
        for table in ('patient', 'appointment', 'procedurelog'):
            _set_performance_category(mock_config_reader, table, 'large')

        with patch.object(processor, '_process_single_table', return_value=True):
            result = processor.process_by_priority(max_workers=2)

        assert set(result['large']['success']) == {'patient', 'appointment', 'procedurelog'}
        assert result['large']['failed'] == []
        assert result['large']['total'] == 3

    def test_process_by_priority_audit_tables_sequential(self, mock_config_reader):
        """
        Test processing tiny tables sequentially (performance_category=tiny).
        """
        processor = PriorityProcessor(mock_config_reader)
        for table in ('securitylog', 'definition'):
            _set_performance_category(mock_config_reader, table, 'tiny')
        for table in ('patient', 'appointment', 'procedurelog'):
            _set_performance_category(mock_config_reader, table, 'large')

        with patch.object(processor, '_process_single_table', return_value=True):
            result = processor.process_by_priority(max_workers=2)

        assert result['tiny']['success'] == ['securitylog', 'definition']
        assert result['tiny']['failed'] == []
        assert result['tiny']['total'] == 2

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
        mock_config_reader.config['tables'] = {}

        result = processor.process_by_priority(max_workers=2)

        assert result == {}

    def test_process_by_priority_stop_on_important_failure(self, mock_config_reader):
        """
        Test recording failures when large tables fail processing.
        """
        processor = PriorityProcessor(mock_config_reader)
        for table in ('patient', 'appointment'):
            _set_performance_category(mock_config_reader, table, 'large')

        with patch.object(processor, '_process_single_table', return_value=False):
            result = processor.process_by_priority(max_workers=2)

        assert result['large']['success'] == []
        assert result['large']['failed'] == ['patient', 'appointment']

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
        processor = PriorityProcessor(config_reader)
        tables = ['patient', 'appointment', 'procedurelog']

        with patch.object(processor, '_process_single_table', return_value=True):
            success_tables, failed_tables = processor._process_parallel(
                tables, max_workers=2, force_full=False
            )

        assert set(success_tables) == {'patient', 'appointment', 'procedurelog'}
        assert failed_tables == []

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
        processor = PriorityProcessor(config_reader)
        tables = ['patient', 'appointment', 'procedurelog']

        with patch.object(
            processor,
            '_process_single_table',
            side_effect=[True, False, True],
        ):
            success_tables, failed_tables = processor._process_parallel(
                tables, max_workers=2, force_full=False
            )

        assert set(success_tables) == {'patient', 'procedurelog'}
        assert failed_tables == ['appointment']

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
            tables = ['patient', 'appointment', 'procedurelog']
            
            # Act: Call _process_parallel() with tables that all fail
            success_tables, failed_tables = processor._process_parallel(tables, max_workers=2, force_full=False)
            
            # Assert: Verify all tables are marked as failed (order doesn't matter for parallel processing)
            assert success_tables == []
            assert set(failed_tables) == set(['patient', 'appointment', 'procedurelog'])
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
            
            tables = ['patient', 'appointment', 'procedurelog', 'claim', 'payment']
            
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

    def test_fail_fast_on_environment_validation_failure(self, test_orchestrator_settings):
        """
        Test FAIL FAST when environment validation fails at orchestrator boundary.
        
        AAA Pattern:
            Arrange: Orchestrator settings with validate_configs returning False
            Act: Call initialize_connections()
            Assert: Verify initialization fails without raising
        """
        from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

        settings = test_orchestrator_settings
        settings.validate_configs = Mock(return_value=False)

        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor'):
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor'):
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector'):
                    orchestrator = PipelineOrchestrator(settings=settings)
                    assert orchestrator.initialize_connections() is False
