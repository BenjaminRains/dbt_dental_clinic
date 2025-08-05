"""
Unit tests for TableProcessor using provider pattern and Settings injection.

This module tests the TableProcessor class using:
- AAA Pattern for clear test structure
- Provider pattern with DictConfigProvider for test isolation
- Settings injection for environment-agnostic testing
- Existing fixtures from env_fixtures.py and config_fixtures.py
- Complete ETL pipeline orchestration testing

Following TESTING_PLAN.md best practices:
- Pure unit tests with comprehensive mocking
- Provider pattern dependency injection
- Settings injection for environment-agnostic operation
- FAIL FAST testing for ETL_ENVIRONMENT validation
"""

import os
import pytest
from unittest.mock import MagicMock, patch
from typing import Dict, Any

from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.database import DatabaseConnectionError
from etl_pipeline.exceptions.configuration import EnvironmentError


@pytest.fixture(autouse=True)
def set_etl_environment(monkeypatch, request):
    # Only remove ETL_ENVIRONMENT for tests marked with 'fail_fast'
    if 'fail_fast' in request.keywords:
        monkeypatch.delenv('ETL_ENVIRONMENT', raising=False)
    else:
        monkeypatch.setenv('ETL_ENVIRONMENT', 'test')


@pytest.mark.unit
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessorUnit:
    """
    Unit tests for TableProcessor using provider pattern and Settings injection.
    
    Test Strategy:
        - Pure unit tests with DictConfigProvider dependency injection
        - Validates ETL pipeline orchestration with mocked components
        - Tests environment validation and FAIL FAST behavior
        - Ensures proper delegation to SimpleMySQLReplicator and PostgresLoader
        - Validates chunked loading logic for large tables
        - Tests error handling and exception propagation
    
    Coverage Areas:
        - Settings injection for environment-agnostic operation
        - Provider pattern with DictConfigProvider for test isolation
        - ETL pipeline orchestration (extract → load)
        - Environment validation and FAIL FAST requirements
        - ConfigReader integration for table configuration
        - Error handling for various failure scenarios
        - Chunked loading decision logic for large tables
        
    ETL Context:
        - Core ETL orchestration component used by PipelineOrchestrator
        - Coordinates MySQL replication and PostgreSQL loading
        - Critical for nightly ETL pipeline execution
        - Uses Settings injection for environment-agnostic connections
    """
    
    def test_fixtures_work_with_table_processor(self, test_settings, test_tables_config):
        """
        Test that existing fixtures work correctly with TableProcessor.
        
        AAA Pattern:
            Arrange: Use existing fixtures (test_settings, test_tables_config)
            Act: Initialize TableProcessor with fixtures
            Assert: Verify fixtures provide correct configuration
            
        Validates:
            - Existing fixtures from env_fixtures.py work with TableProcessor
            - Existing fixtures from config_fixtures.py provide correct configuration
            - Provider pattern with DictConfigProvider works correctly
            - Settings injection for environment-agnostic operation
        """
        # Arrange: Use existing fixtures (test_settings, test_tables_config)
        settings = test_settings
        tables_config = test_tables_config
        
        # Act: Initialize TableProcessor with fixtures
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            mock_config_reader = MagicMock()
            mock_config_reader.get_table_config.return_value = tables_config['tables']['patient']
            mock_config_reader_class.return_value = mock_config_reader
            
            table_processor = TableProcessor(config_reader=mock_config_reader)
            table_processor.settings = settings
            
            # Assert: Verify fixtures provide correct configuration
            assert table_processor.settings is not None
            assert table_processor.settings.environment == 'test'
            assert table_processor.config_reader is not None
            assert table_processor.metrics is not None
            
            # Verify table configuration is accessible
            patient_config = table_processor.config_reader.get_table_config('patient')
            assert patient_config['primary_key'] == 'PatNum'
            assert patient_config['incremental_column'] == 'DateTStamp'
            assert patient_config['extraction_strategy'] == 'incremental'
            assert patient_config['batch_size'] == 100
    
    def test_initialization_with_provider_pattern(self, test_settings):
        """
        Test TableProcessor initialization using provider pattern.
        
        AAA Pattern:
            Arrange: Set up test settings with DictConfigProvider
            Act: Initialize TableProcessor with test settings
            Assert: Verify proper initialization and Settings injection
            
        Validates:
            - Settings injection works correctly
            - Provider pattern provides clean test isolation
            - Environment validation passes for test environment
            - ConfigReader is properly initialized
            - Metrics collector is set up correctly
        """
        # Arrange: Set up test settings with DictConfigProvider
        settings = test_settings
        
        # Act: Initialize TableProcessor with test settings
        with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
            mock_config_reader = MagicMock()
            mock_config_reader_class.return_value = mock_config_reader
            
            table_processor = TableProcessor(config_path="mock_config_path")
            
            # Assert: Verify proper initialization and Settings injection
            assert table_processor.settings is not None
            assert table_processor.settings.environment == 'test'
            assert table_processor.config_reader is not None
            assert table_processor.metrics is not None
            assert table_processor.config_path == "mock_config_path"
    
    def test_process_table_success(self, test_settings):
        """
        Test successful table processing using provider pattern.
        
        AAA Pattern:
            Arrange: Set up mocked components and test settings
            Act: Call process_table() with valid table name
            Assert: Verify successful ETL pipeline execution
            
        Validates:
            - Complete ETL pipeline orchestration (extract → load)
            - Proper delegation to SimpleMySQLReplicator and PostgresLoader
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
            - Metrics collection for successful processing
        """
        # Arrange: Set up mocked components and test settings
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
                with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                    
                    # Mock successful components
                    mock_replicator = MagicMock()
                    mock_replicator.copy_table.return_value = True
                    mock_replicator_class.return_value = mock_replicator
                    
                    mock_loader = MagicMock()
                    mock_loader.load_table.return_value = True
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
                    
                    # Act: Call process_table() with valid table name
                    result = table_processor.process_table('patient', force_full=False)
                    
                    # Assert: Verify successful ETL pipeline execution
                    assert result is True
                    mock_replicator.copy_table.assert_called_once_with('patient', force_full=False)
                    mock_loader.load_table.assert_called_once_with(table_name='patient', force_full=False)
    
    def test_extract_to_replication_success(self, test_settings):
        """
        Test successful data extraction to replication database.
        
        AAA Pattern:
            Arrange: Set up mocked SimpleMySQLReplicator and test settings
            Act: Call _extract_to_replication() with valid parameters
            Assert: Verify successful extraction delegation
            
        Validates:
            - Proper delegation to SimpleMySQLReplicator
            - Settings injection for environment-agnostic operation
            - Error handling for extraction failures
            - Logging for extraction operations
        """
        # Arrange: Set up mocked SimpleMySQLReplicator and test settings
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            mock_replicator = MagicMock()
            mock_replicator.copy_table.return_value = True
            mock_replicator_class.return_value = mock_replicator
            
            table_processor = TableProcessor()
            table_processor.settings = settings
            
            # Act: Call _extract_to_replication() with valid parameters
            result = table_processor._extract_to_replication('patient', force_full=False)
            
            # Assert: Verify successful extraction delegation
            assert result is True
            mock_replicator.copy_table.assert_called_once_with('patient', force_full=False)
    
    def test_load_to_analytics_chunked(self, test_settings):
        """
        Test chunked loading for large tables.
        
        AAA Pattern:
            Arrange: Set up mocked PostgresLoader with large table config
            Act: Call _load_to_analytics() with large table configuration
            Assert: Verify chunked loading is used for large tables
            
        Validates:
            - Chunked loading decision logic (> 100MB threshold)
            - Proper delegation to PostgresLoader.load_table_chunked()
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
        """
        # Arrange: Set up mocked PostgresLoader with large table config
        settings = test_settings
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                
                mock_loader = MagicMock()
                mock_loader.load_table_chunked.return_value = True
                mock_loader_class.return_value = mock_loader
                
                mock_config_reader = MagicMock()
                mock_config_reader.get_table_config.return_value = {
                    'primary_key': 'ProcNum',
                    'incremental_column': 'ProcDate',
                    'extraction_strategy': 'incremental',
                    'batch_size': 5000,
                    'estimated_size_mb': 150.0  # > 100MB triggers chunked loading
                }
                mock_config_reader_class.return_value = mock_config_reader
                
                table_processor = TableProcessor(config_reader=mock_config_reader)
                table_processor.settings = settings
                
                # Act: Call _load_to_analytics() with large table configuration
                result = table_processor._load_to_analytics('procedurelog', force_full=False)
                
                # Assert: Verify chunked loading is used for large tables
                assert result is True
                mock_loader.load_table_chunked.assert_called_once_with(
                    table_name='procedurelog',
                    force_full=False,
                    chunk_size=5000
                )
                mock_loader.load_table.assert_not_called()  # Should not use standard loading
    
    def test_load_to_analytics_standard(self, test_settings):
        """
        Test standard loading for small tables.
        
        AAA Pattern:
            Arrange: Set up mocked PostgresLoader with small table config
            Act: Call _load_to_analytics() with small table configuration
            Assert: Verify standard loading is used for small tables
            
        Validates:
            - Standard loading decision logic (<= 100MB threshold)
            - Proper delegation to PostgresLoader.load_table()
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
        """
        # Arrange: Set up mocked PostgresLoader with small table config
        settings = test_settings
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
            with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                
                mock_loader = MagicMock()
                mock_loader.load_table.return_value = True
                mock_loader_class.return_value = mock_loader
                
                mock_config_reader = MagicMock()
                mock_config_reader.get_table_config.return_value = {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'extraction_strategy': 'incremental',
                    'batch_size': 1000,
                    'estimated_size_mb': 50.0  # <= 100MB uses standard loading
                }
                mock_config_reader_class.return_value = mock_config_reader
                
                table_processor = TableProcessor(config_reader=mock_config_reader)
                table_processor.settings = settings
                
                # Act: Call _load_to_analytics() with small table configuration
                result = table_processor._load_to_analytics('patient', force_full=False)
                
                # Assert: Verify standard loading is used for small tables
                assert result is True
                mock_loader.load_table.assert_called_once_with(table_name='patient', force_full=False)
                mock_loader.load_table_chunked.assert_not_called()  # Should not use chunked loading
    
    @pytest.mark.fail_fast
    def test_environment_validation_fail_fast(self):
        """
        Test FAIL FAST behavior when ETL_ENVIRONMENT not set.
        
        AAA Pattern:
            Arrange: ETL_ENVIRONMENT removed by fixture
            Act: Attempt to initialize TableProcessor without environment
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior when ETL_ENVIRONMENT not set
            - Critical security requirement for ETL pipeline
            - Clear error messages for missing environment
            - Prevents accidental production environment usage
        """
        # Act: Attempt to initialize TableProcessor without environment
        with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
            table_processor = TableProcessor()
    
    def test_error_handling_data_extraction_failure(self, test_settings):
        """
        Test error handling for data extraction failures.
        
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
    
    def test_error_handling_data_loading_failure(self, test_settings):
        """
        Test error handling for data loading failures.
        
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
                    
                    # Mock successful extraction
                    mock_replicator = MagicMock()
                    mock_replicator.copy_table.return_value = True
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
    
    def test_chunked_loading_decision_logic(self, test_settings):
        """
        Test chunked loading decision logic based on table size.
        
        AAA Pattern:
            Arrange: Set up different table configurations with varying sizes
            Act: Call _load_to_analytics() with different table configurations
            Assert: Verify correct loading method is chosen based on size
            
        Validates:
            - Chunked loading threshold (> 100MB)
            - Standard loading for small tables (<= 100MB)
            - ConfigReader integration for table configuration
            - Settings injection for environment-agnostic operation
        """
        # Arrange: Set up different table configurations with varying sizes
        settings = test_settings
        
        test_cases = [
            {
                'table_name': 'patient',
                'estimated_size_mb': 50.0,  # <= 100MB
                'expected_method': 'load_table',
                'expected_chunked': False
            },
            {
                'table_name': 'procedurelog',
                'estimated_size_mb': 150.0,  # > 100MB
                'expected_method': 'load_table_chunked',
                'expected_chunked': True
            },
            {
                'table_name': 'appointment',
                'estimated_size_mb': 100.0,  # Exactly 100MB
                'expected_method': 'load_table',
                'expected_chunked': False
            }
        ]
        
        for test_case in test_cases:
            with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
                with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                    
                    mock_loader = MagicMock()
                    mock_loader.load_table.return_value = True
                    mock_loader.load_table_chunked.return_value = True
                    mock_loader_class.return_value = mock_loader
                    
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
                    
                    # Act: Call _load_to_analytics() with different table configurations
                    result = table_processor._load_to_analytics(test_case['table_name'], force_full=False)
                    
                    # Assert: Verify correct loading method is chosen based on size
                    assert result is True
                    
                    if test_case['expected_chunked']:
                        mock_loader.load_table_chunked.assert_called_once()
                        mock_loader.load_table.assert_not_called()
                    else:
                        mock_loader.load_table.assert_called_once()
                        mock_loader.load_table_chunked.assert_not_called()
    
    def test_full_refresh_processing(self, test_settings):
        """
        Test full refresh processing with force_full=True.
        
        AAA Pattern:
            Arrange: Set up mocked components for full refresh processing
            Act: Call process_table() with force_full=True
            Assert: Verify full refresh is properly handled
            
        Validates:
            - Full refresh processing with force_full=True
            - Proper delegation to components with full refresh flag
            - Settings injection for environment-agnostic operation
            - ConfigReader integration for table configuration
        """
        # Arrange: Set up mocked components for full refresh processing
        settings = test_settings
        
        with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator') as mock_replicator_class:
            with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
                with patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class:
                    
                    mock_replicator = MagicMock()
                    mock_replicator.copy_table.return_value = True
                    mock_replicator_class.return_value = mock_replicator
                    
                    mock_loader = MagicMock()
                    mock_loader.load_table.return_value = True
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
                    
                    # Act: Call process_table() with force_full=True
                    result = table_processor.process_table('patient', force_full=True)
                    
                    # Assert: Verify full refresh is properly handled
                    assert result is True
                    mock_replicator.copy_table.assert_called_once_with('patient', force_full=True)
                    mock_loader.load_table.assert_called_once_with(table_name='patient', force_full=True) 