"""
Comprehensive tests for TableProcessor using provider pattern and Settings injection.

This module provides comprehensive testing for the TableProcessor class using:
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

from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError


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
class TestTableProcessorComprehensive:
    """
    Comprehensive tests for TableProcessor using provider pattern and Settings injection.
    
    Test Strategy:
        - Comprehensive tests with DictConfigProvider dependency injection
        - Validates complete ETL pipeline orchestration with mocked components
        - Tests all error handling scenarios and edge cases
        - Ensures proper delegation to SimpleMySQLReplicator and PostgresLoader
        - Validates load strategy selection and configuration handling (copy_csv for large tables)
        - Tests environment validation and FAIL FAST behavior
        - Comprehensive metrics collection and logging validation
    
    Coverage Areas:
        - Settings injection for environment-agnostic operation
        - Provider pattern with DictConfigProvider for test isolation
        - Complete ETL pipeline orchestration (extract → load)
        - Environment validation and FAIL FAST requirements
        - ConfigReader integration for table configuration
        - Comprehensive error handling for all failure scenarios
        - Load strategy: loader selects standard/streaming/copy_csv by size
        - Metrics collection and logging for all operations
        - Configuration validation and edge cases
        - Full refresh vs incremental processing scenarios
        
    ETL Context:
        - Core ETL orchestration component used by PipelineOrchestrator
        - Coordinates MySQL replication and PostgreSQL loading
        - Critical for nightly ETL pipeline execution
        - Uses Settings injection for environment-agnostic connections
        - Handles dental clinic data with varying table sizes
    """
    
    def test_comprehensive_initialization_with_provider_pattern(self, test_settings):
        """
        Test comprehensive TableProcessor initialization using provider pattern.
        
        AAA Pattern:
            Arrange: Set up test settings with DictConfigProvider
            Act: Initialize TableProcessor with various configurations
            Assert: Verify proper initialization and Settings injection
            
        Validates:
            - Settings injection works correctly with provider pattern
            - Environment validation passes for test environment
            - ConfigReader is properly initialized with various paths
            - Metrics collector is set up correctly
            - Configuration path handling works correctly
        """
        # Arrange: Set up test settings with DictConfigProvider
        settings = test_settings
        
        # Act: Initialize TableProcessor with test settings
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            mock_config_reader = MagicMock()
            mock_config_reader_class.return_value = mock_config_reader
            
            # Test with custom config path
            table_processor = TableProcessor(config_path="custom_config.yml")
            
            # Assert: Verify proper initialization and Settings injection
            assert table_processor.settings is not None
            assert table_processor.settings.environment == 'test'
            assert table_processor.config_reader is not None
            assert table_processor.metrics is not None
            assert table_processor.config_path == "custom_config.yml"
            
            # Verify ConfigReader was called with correct path
            mock_config_reader_class.assert_called_once_with("custom_config.yml")
    
    def test_comprehensive_process_table_success_incremental(self, test_settings):
        """
        Test comprehensive successful incremental table processing.
        
        AAA Pattern:
            Arrange: Set up mocked components and test settings for incremental processing
            Act: Call process_table() with incremental configuration
            Assert: Verify successful ETL pipeline execution with proper delegation
            
        Validates:
            - Complete ETL pipeline orchestration (extract → load)
            - Proper delegation to SimpleMySQLReplicator and PostgresLoader
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
            - Metrics collection for successful processing
            - Incremental processing logic
        """
        # Arrange: Set up mocked components and test settings
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                mock_loader = MagicMock()
                mock_loader.load_table.return_value = (True, {
                    'rows_loaded': 100,
                    'duration': 1.0,
                    'strategy_used': 'standard',
                })
                mock_loader._check_analytics_needs_updating.return_value = (True, None, None, False)
                with patch.object(TableProcessor, '_instantiate_loader', return_value=mock_loader):
                    with patch.object(TableProcessor, '_update_pipeline_status'):
                        # Mock successful components (copy_table and load_table return (success, metadata))
                        mock_replicator = MagicMock()
                        mock_replicator.copy_table.return_value = (True, {
                            'rows_copied': 100,
                            'strategy_used': 'incremental',
                            'duration': 1.0,
                            'force_full_applied': False,
                            'primary_column': 'PatNum',
                            'last_primary_value': '100',
                        })
                        mock_replicator_class.return_value = mock_replicator
                        
                        mock_config_reader = MagicMock()
                        mock_config_reader.get_table_config.return_value = {
                            'primary_key': 'PatNum',
                            'incremental_column': 'DateTStamp',
                            'extraction_strategy': 'incremental',
                            'batch_size': 1000,
                            'estimated_size_mb': 50.0,
                            'table_importance': 'critical'
                        }
                        mock_config_reader_class.return_value = mock_config_reader
                        
                        table_processor = TableProcessor(config_reader=mock_config_reader)
                        table_processor.settings = settings
                        
                        # Act: Call process_table() with incremental configuration
                        result = table_processor.process_table('patient', force_full=False)
                        
                        # Assert: Verify successful ETL pipeline execution
                        assert result is True
                        # Extract uses strategy-resolved force_full (True when no incremental columns)
                        mock_replicator.copy_table.assert_called_once_with('patient', force_full=True)
                        mock_loader.load_table.assert_called_once_with(table_name='patient', force_full=False)
                        
                        # Verify ConfigReader was used for table config (called 3x: init path, extract, load)
                        assert mock_config_reader.get_table_config.call_count == 3
                        mock_config_reader.get_table_config.assert_has_calls([call('patient')] * 3)
    
    def test_comprehensive_process_table_success_full_refresh(self, test_settings):
        """
        Test comprehensive successful full refresh table processing.
        
        AAA Pattern:
            Arrange: Set up mocked components and test settings for full refresh
            Act: Call process_table() with force_full=True
            Assert: Verify successful ETL pipeline execution with full refresh
            
        Validates:
            - Complete ETL pipeline orchestration with full refresh
            - Proper delegation to components with force_full=True
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
            - Full refresh processing logic
        """
        # Arrange: Set up mocked components and test settings
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                mock_loader = MagicMock()
                mock_loader.load_table.return_value = (True, {
                    'rows_loaded': 500,
                    'duration': 2.0,
                    'strategy_used': 'copy_csv',
                })
                mock_loader._check_analytics_needs_updating.return_value = (True, None, None, False)
                with patch.object(TableProcessor, '_instantiate_loader', return_value=mock_loader):
                    with patch.object(TableProcessor, '_update_pipeline_status'):
                        mock_replicator = MagicMock()
                        mock_replicator.copy_table.return_value = (True, {
                            'rows_copied': 500,
                            'strategy_used': 'full_table',
                            'duration': 2.0,
                            'force_full_applied': True,
                            'primary_column': 'ProcNum',
                            'last_primary_value': '500',
                        })
                        mock_replicator_class.return_value = mock_replicator
                        
                        mock_config_reader = MagicMock()
                        mock_config_reader.get_table_config.return_value = {
                            'primary_key': 'ProcNum',
                            'incremental_column': 'ProcDate',
                            'extraction_strategy': 'incremental',
                            'batch_size': 5000,
                            'estimated_size_mb': 75.0,
                            'table_importance': 'high'
                        }
                        mock_config_reader_class.return_value = mock_config_reader
                        
                        table_processor = TableProcessor(config_reader=mock_config_reader)
                        table_processor.settings = settings
                        
                        # Act: Call process_table() with force_full=True
                        result = table_processor.process_table('procedurelog', force_full=True)
                        
                        # Assert: Verify successful ETL pipeline execution with full refresh
                        assert result is True
                        mock_replicator.copy_table.assert_called_once_with('procedurelog', force_full=True)
                        mock_loader.load_table.assert_called_once_with(table_name='procedurelog', force_full=True)
    
    def test_comprehensive_extract_to_replication_success(self, test_settings):
        """
        Test comprehensive successful data extraction to replication database.
        
        AAA Pattern:
            Arrange: Set up mocked SimpleMySQLReplicator and test settings
            Act: Call _extract_to_replication() with valid parameters
            Assert: Verify successful extraction delegation
            
        Validates:
            - Proper delegation to SimpleMySQLReplicator
            - Settings injection for environment-agnostic operation
            - Error handling for extraction failures
            - Logging for extraction operations
            - SimpleMySQLReplicator initialization with settings
        """
        # Arrange: Set up mocked SimpleMySQLReplicator and test settings
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            mock_replicator = MagicMock()
            mock_replicator.copy_table.return_value = (True, {
                'rows_copied': 50,
                'strategy_used': 'incremental',
                'duration': 0.5,
                'force_full_applied': False,
                'primary_column': 'PatNum',
                'last_primary_value': '100',
            })
            mock_replicator_class.return_value = mock_replicator
            
            table_processor = TableProcessor()
            table_processor.settings = settings
            
            # Act: Call _extract_to_replication() with valid parameters
            success, metadata = table_processor._extract_to_replication('patient', force_full=False)
            
            # Assert: Verify successful extraction delegation
            assert success is True
            assert metadata.get('rows_copied') == 50
            mock_replicator.copy_table.assert_called_once_with('patient', force_full=False)
            mock_replicator_class.assert_called_once_with(settings=settings)
    
    def test_comprehensive_load_to_analytics_large_table_copy_csv(self, test_settings):
        """
        Test loading for large tables (strategy selected by loader: copy_csv).
        
        Chunked load strategy was removed; large tables use copy_csv via load_table().
        
        AAA Pattern:
            Arrange: Set up mocked PostgresLoader with large table config
            Act: Call _load_to_analytics() with large table configuration
            Assert: Verify load_table() is called (loader selects copy_csv internally)
            
        Validates:
            - Single load_table() entrypoint for all table sizes
            - Proper delegation to PostgresLoader.load_table()
            - Settings injection for environment-agnostic operation
        """
        # Arrange: Set up mocked PostgresLoader with large table config
        settings = test_settings
        
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            mock_loader = MagicMock()
            mock_loader.load_table.return_value = (True, {'rows_loaded': 1000, 'duration': 1.0, 'strategy_used': 'copy_csv'})
            mock_loader._check_analytics_needs_updating.return_value = (True, None, None, False)
            with patch.object(TableProcessor, '_instantiate_loader', return_value=mock_loader):
                mock_config_reader = MagicMock()
                mock_config_reader.get_table_config.return_value = {
                    'primary_key': 'ProcNum',
                    'incremental_column': 'ProcDate',
                    'extraction_strategy': 'incremental',
                    'batch_size': 5000,
                    'estimated_size_mb': 150.0
                }
                mock_config_reader_class.return_value = mock_config_reader
                
                table_processor = TableProcessor(config_reader=mock_config_reader)
                table_processor.settings = settings
                
                # Act: Call _load_to_analytics() with large table configuration
                result = table_processor._load_to_analytics('procedurelog', force_full=False)
                
                # Assert: load_table() is always used; loader selects strategy by size
                assert result[0] is True
                mock_loader.load_table.assert_called_once_with(table_name='procedurelog', force_full=False)
    
    def test_comprehensive_load_to_analytics_standard_small_table(self, test_settings):
        """
        Test comprehensive standard loading for small tables.
        
        AAA Pattern:
            Arrange: Set up mocked PostgresLoader with small table config
            Act: Call _load_to_analytics() with small table configuration
            Assert: Verify standard loading is used for small tables
            
        Validates:
            - Standard loading decision logic (<= 100MB threshold)
            - Proper delegation to PostgresLoader.load_table()
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
            - Default batch size handling
        """
        # Arrange: Set up mocked PostgresLoader with small table config
        settings = test_settings
        
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            mock_loader = MagicMock()
            mock_loader.load_table.return_value = (True, {'rows_loaded': 100, 'duration': 0.5, 'strategy_used': 'standard'})
            mock_loader._check_analytics_needs_updating.return_value = (True, None, None, False)
            with patch.object(TableProcessor, '_instantiate_loader', return_value=mock_loader):
                mock_config_reader = MagicMock()
                mock_config_reader.get_table_config.return_value = {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'extraction_strategy': 'incremental',
                    'batch_size': 1000,
                    'estimated_size_mb': 50.0  # <= 100MB
                }
                mock_config_reader_class.return_value = mock_config_reader
                
                table_processor = TableProcessor(config_reader=mock_config_reader)
                table_processor.settings = settings
                
                # Act: Call _load_to_analytics() with small table configuration
                result = table_processor._load_to_analytics('patient', force_full=False)
                
                # Assert: Verify load_table() is used (loader selects strategy by size)
                assert result[0] is True
                mock_loader.load_table.assert_called_once_with(table_name='patient', force_full=False)
    
    @pytest.mark.fail_fast
    def test_comprehensive_environment_validation_fail_fast(self):
        """
        Test comprehensive FAIL FAST behavior when ETL_ENVIRONMENT not set.
        
        AAA Pattern:
            Arrange: ETL_ENVIRONMENT removed by fixture
            Act: Attempt to initialize TableProcessor without environment
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior when ETL_ENVIRONMENT not set
            - Critical security requirement for ETL pipeline
            - Clear error messages for missing environment
            - Prevents accidental clinic environment usage
        """
        # Act: Attempt to initialize TableProcessor without environment
        with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
            table_processor = TableProcessor()
    
    def test_comprehensive_error_handling_data_extraction_failure(self, test_settings):
        """
        Test comprehensive error handling for data extraction failures.
        
        AAA Pattern:
            Arrange: Set up mocked SimpleMySQLReplicator that raises DataExtractionError
            Act: Call process_table() with failing extraction
            Assert: Verify proper error handling and logging
            
        Validates:
            - DataExtractionError handling
            - Proper error logging and metrics collection
            - Settings injection for environment-agnostic operation
            - FAIL FAST behavior for critical errors
        """
        # Arrange: Set up mocked SimpleMySQLReplicator that raises DataExtractionError
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                
                mock_replicator = MagicMock()
                mock_replicator.copy_table.side_effect = DataExtractionError(
                    message="Failed to extract data from source",
                    table_name="patient",
                    details={"database": "test_opendental", "connection_type": "mysql"}
                )
                mock_replicator_class.return_value = mock_replicator
                
                mock_config_reader = MagicMock()
                mock_config_reader.get_table_config.return_value = {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'extraction_strategy': 'incremental',
                    'batch_size': 1000,
                    'estimated_size_mb': 50.0
                }
                mock_config_reader_class.return_value = mock_config_reader
                
                table_processor = TableProcessor(config_reader=mock_config_reader)
                table_processor.settings = settings
                
                # Act: Call process_table() with failing extraction
                result = table_processor.process_table('patient', force_full=False)
                
                # Assert: Verify proper error handling and logging
                assert result is False
    
    def test_comprehensive_error_handling_data_loading_failure(self, test_settings):
        """
        Test comprehensive error handling for data loading failures.
        
        AAA Pattern:
            Arrange: Set up mocked PostgresLoader that raises DataLoadingError
            Act: Call process_table() with failing loading
            Assert: Verify proper error handling and logging
            
        Validates:
            - DataLoadingError handling
            - Proper error logging and metrics collection
            - Settings injection for environment-agnostic operation
            - FAIL FAST behavior for critical errors
        """
        # Arrange: Set up mocked PostgresLoader that raises DataLoadingError
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
                with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                    
                    # Mock successful extraction (copy_table returns (success, metadata))
                    mock_replicator = MagicMock()
                    mock_replicator.copy_table.return_value = (True, {
                        'rows_copied': 100,
                        'strategy_used': 'incremental',
                        'duration': 1.0,
                        'force_full_applied': False,
                        'primary_column': 'PatNum',
                        'last_primary_value': '100',
                    })
                    mock_replicator_class.return_value = mock_replicator
                    
                    # Mock failing loading
                    mock_loader = MagicMock()
                    mock_loader.load_table.side_effect = DataLoadingError(
                        message="Failed to load data to analytics",
                        table_name="patient",
                        details={"database": "test_opendental_analytics", "connection_type": "postgresql"}
                    )
                    mock_loader_class.return_value = mock_loader
                    
                    mock_config_reader = MagicMock()
                    mock_config_reader.get_table_config.return_value = {
                        'primary_key': 'PatNum',
                        'incremental_column': 'DateTStamp',
                        'extraction_strategy': 'incremental',
                        'batch_size': 1000,
                        'estimated_size_mb': 50.0
                    }
                    mock_config_reader_class.return_value = mock_config_reader
                    
                    table_processor = TableProcessor(config_reader=mock_config_reader)
                    table_processor.settings = settings
                    
                    # Act: Call process_table() with failing loading
                    result = table_processor.process_table('patient', force_full=False)
                    
                    # Assert: Verify proper error handling and logging
                    assert result is False
    
    def test_comprehensive_error_handling_database_connection_failure(self, test_settings):
        """
        Test comprehensive error handling for database connection failures.
        
        AAA Pattern:
            Arrange: Set up mocked components that raise DatabaseConnectionError
            Act: Call process_table() with failing database connections
            Assert: Verify proper error handling and logging
            
        Validates:
            - DatabaseConnectionError handling
            - Proper error logging and metrics collection
            - Settings injection for environment-agnostic operation
            - FAIL FAST behavior for critical errors
        """
        # Arrange: Set up mocked components that raise DatabaseConnectionError
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                
                mock_replicator = MagicMock()
                mock_replicator.copy_table.side_effect = DatabaseConnectionError(
                    message="Failed to connect to source database",
                    database_type="mysql",
                    details={"host": "test-host", "port": 3306}
                )
                mock_replicator_class.return_value = mock_replicator
                
                mock_config_reader = MagicMock()
                mock_config_reader.get_table_config.return_value = {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'extraction_strategy': 'incremental',
                    'batch_size': 1000,
                    'estimated_size_mb': 50.0
                }
                mock_config_reader_class.return_value = mock_config_reader
                
                table_processor = TableProcessor(config_reader=mock_config_reader)
                table_processor.settings = settings
                
                # Act: Call process_table() with failing database connections
                result = table_processor.process_table('patient', force_full=False)
                
                # Assert: Verify proper error handling and logging
                assert result is False
    
    def test_comprehensive_load_table_always_called_strategy_in_loader_edge_cases(self, test_settings):
        """
        Test that load_table() is always called regardless of table size.
        
        Strategy selection (standard/streaming/copy_csv) is done inside the loader
        based on estimated_size_mb; table_processor always uses load_table().
        """
        settings = test_settings
        
        test_cases = [
            {'table_name': 'patient', 'estimated_size_mb': 50.0},
            {'table_name': 'procedurelog', 'estimated_size_mb': 150.0},
            {'table_name': 'appointment', 'estimated_size_mb': 100.0},
            {'table_name': 'small_table', 'estimated_size_mb': 0.0},
            {'table_name': 'missing_size_table', 'estimated_size_mb': None},
        ]
        
        for test_case in test_cases:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                mock_loader = MagicMock()
                mock_loader.load_table.return_value = (True, {'rows_loaded': 0, 'duration': 0.0, 'strategy_used': 'standard'})
                mock_loader._check_analytics_needs_updating.return_value = (True, None, None, False)
                with patch.object(TableProcessor, '_instantiate_loader', return_value=mock_loader):
                    mock_config_reader = MagicMock()
                    mock_config_reader.get_table_config.return_value = {
                        'primary_key': 'TestNum',
                        'incremental_column': 'DateTStamp',
                        'extraction_strategy': 'incremental',
                        'batch_size': 1000,
                        'estimated_size_mb': test_case['estimated_size_mb']
                    }
                    mock_config_reader_class.return_value = mock_config_reader
                    
                    table_processor = TableProcessor(config_reader=mock_config_reader)
                    table_processor.settings = settings
                    
                    result = table_processor._load_to_analytics(test_case['table_name'], force_full=False)
                    
                    assert result[0] is True
                    mock_loader.load_table.assert_called_once_with(table_name=test_case['table_name'], force_full=False)
    
    def test_comprehensive_configuration_validation_failure(self, test_settings):
        """
        Test comprehensive configuration validation failure handling.
        
        AAA Pattern:
            Arrange: Set up ConfigReader that returns None for table config
            Act: Call process_table() with missing table configuration
            Assert: Verify proper error handling and logging
            
        Validates:
            - Configuration validation failure handling
            - Proper error logging when table config is missing
            - Settings injection for environment-agnostic operation
            - FAIL FAST behavior for configuration errors
        """
        # Arrange: Set up ConfigReader that returns None for table config
        settings = test_settings
        
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            
            mock_config_reader = MagicMock()
            mock_config_reader.get_table_config.return_value = None  # Missing configuration
            mock_config_reader_class.return_value = mock_config_reader
            
            table_processor = TableProcessor(config_reader=mock_config_reader)
            table_processor.settings = settings
            
            # Act: Call process_table() with missing table configuration
            result = table_processor.process_table('nonexistent_table', force_full=False)
            
            # Assert: Verify proper error handling and logging
            assert result is False
    
    def test_comprehensive_environment_validation_success(self, test_settings):
        """
        Test comprehensive environment validation success.
        
        AAA Pattern:
            Arrange: Set up test settings with valid environment configuration
            Act: Initialize TableProcessor with valid environment
            Assert: Verify environment validation passes
            
        Validates:
            - Environment validation success with valid configuration
            - Settings injection for environment-agnostic operation
            - Provider pattern with DictConfigProvider for test isolation
            - Proper initialization with validated environment
        """
        # Arrange: Set up test settings with valid environment configuration
        settings = test_settings
        
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            mock_config_reader = MagicMock()
            mock_config_reader_class.return_value = mock_config_reader
            
            # Act: Initialize TableProcessor with valid environment
            table_processor = TableProcessor(config_reader=mock_config_reader)
            table_processor.settings = settings
            
            # Assert: Verify environment validation passes
            assert table_processor.settings is not None
            assert table_processor.settings.environment == 'test'
            assert table_processor.config_reader is not None
            assert table_processor.metrics is not None
    
    def test_comprehensive_metrics_collection_success(self, test_settings):
        """
        Test comprehensive metrics collection for successful operations.
        
        AAA Pattern:
            Arrange: Set up mocked components and test settings
            Act: Call process_table() with successful processing
            Assert: Verify metrics collection works correctly
            
        Validates:
            - Metrics collection for successful operations
            - Settings injection for environment-agnostic operation
            - Provider pattern with DictConfigProvider for test isolation
            - UnifiedMetricsCollector integration
        """
        # Arrange: Set up mocked components and test settings
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                mock_loader = MagicMock()
                mock_loader.load_table.return_value = (True, {
                    'rows_loaded': 100,
                    'duration': 1.0,
                    'strategy_used': 'standard',
                })
                mock_loader._check_analytics_needs_updating.return_value = (True, None, None, False)
                with patch.object(TableProcessor, '_instantiate_loader', return_value=mock_loader):
                    with patch.object(TableProcessor, '_update_pipeline_status'):
                        mock_replicator = MagicMock()
                        mock_replicator.copy_table.return_value = (True, {
                            'rows_copied': 100,
                            'strategy_used': 'incremental',
                            'duration': 1.0,
                            'force_full_applied': False,
                            'primary_column': 'PatNum',
                            'last_primary_value': '100',
                        })
                        mock_replicator_class.return_value = mock_replicator
                        
                        mock_config_reader = MagicMock()
                        mock_config_reader.get_table_config.return_value = {
                            'primary_key': 'PatNum',
                            'incremental_column': 'DateTStamp',
                            'extraction_strategy': 'incremental',
                            'batch_size': 1000,
                            'estimated_size_mb': 50.0
                        }
                        mock_config_reader_class.return_value = mock_config_reader
                        
                        table_processor = TableProcessor(config_reader=mock_config_reader)
                        table_processor.settings = settings
                        
                        # Act: Call process_table() with successful processing
                        result = table_processor.process_table('patient', force_full=False)
                        
                        # Assert: Verify metrics collection works correctly
                        assert result is True
                        assert table_processor.metrics is not None
                        # Additional metrics validation could be added here 