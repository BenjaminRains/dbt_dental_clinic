"""
Unit tests for OpenDentalSchemaAnalyzer batch processing using provider pattern.

This module tests the OpenDentalSchemaAnalyzer batch processing with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests batch schema info processing
    - Tests batch size info processing
    - Tests batch processing with ConnectionManager
    - Tests timeout handling in batch operations

Coverage Areas:
    - Batch schema info processing
    - Batch size info processing
    - Batch processing with ConnectionManager
    - Timeout handling in batch operations
    - Performance optimization in batch operations

ETL Context:
    - Critical for ETL pipeline configuration generation
    - Tests with mocked dental clinic database schemas
    - Uses Settings injection with DictConfigProvider for unit testing
    - Generates test tables.yml for ETL pipeline configuration
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


class TestOpenDentalSchemaAnalyzerBatchProcessing:
    """Unit tests for OpenDentalSchemaAnalyzer batch processing using provider pattern."""
    
    def test_batch_schema_info_processing(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test batch schema info processing.
        
        AAA Pattern:
            Arrange: Set up mock schema data for batch processing
            Act: Call get_batch_schema_info() method
            Assert: Verify batch schema processing works correctly
            
        Validates:
            - Batch schema info processing
            - Performance optimization in batch operations
            - Integration with schema analysis
        """
        # Arrange: Set up mock schema data for batch processing
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector with batch processing support
            mock_inspector = Mock()
            
            def mock_get_columns(table_name):
                # Return list of column dictionaries as expected by the real code
                return [
                    {'name': 'id', 'type': 'INT', 'nullable': False, 'default': None},
                    {'name': 'name', 'type': 'VARCHAR(255)', 'nullable': True, 'default': None}
                ]
            
            def mock_get_pk_constraint(table_name):
                # Return primary key constraint as expected by the real code
                return {'constrained_columns': ['id']}
            
            def mock_get_foreign_keys(table_name):
                # Return foreign keys as expected by the real code
                return []
            
            def mock_get_indexes(table_name):
                # Return indexes as expected by the real code
                return []
            
            # Configure the mock inspector methods
            mock_inspector.get_columns = mock_get_columns
            mock_inspector.get_pk_constraint = mock_get_pk_constraint
            mock_inspector.get_foreign_keys = mock_get_foreign_keys
            mock_inspector.get_indexes = mock_get_indexes
            
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_batch_schema_info() method
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            
            # Assert: Verify batch schema processing works correctly
            assert isinstance(batch_schema_info, dict)
            assert len(batch_schema_info) == 3
            for table_name in test_tables:
                assert table_name in batch_schema_info

    def test_batch_size_info_processing(self, mock_settings_with_dict_provider, mock_size_data, mock_environment_variables):
        """
        Test batch size info processing.
        
        AAA Pattern:
            Arrange: Set up mock size data for batch processing
            Act: Call get_batch_size_info() method
            Assert: Verify batch size processing works correctly
            
        Validates:
            - Batch size info processing
            - Performance optimization in batch operations
            - Integration with size analysis
        """
        # Arrange: Set up mock size data for batch processing
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock execute_with_retry to return our test data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'TABLE_ROWS' in query:
                    # Return estimated row count
                    mock_result.scalar.return_value = 50000
                elif 'information_schema.tables' in query:
                    # Return size in MB
                    mock_result.scalar.return_value = 25.5
                else:
                    # Return 0 for other queries
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_batch_size_info() method
            batch_size_info = analyzer.get_batch_size_info(test_tables)
            
            # Assert: Verify batch size processing works correctly
            assert isinstance(batch_size_info, dict)
            assert len(batch_size_info) == 3
            for table_name in test_tables:
                assert table_name in batch_size_info

    def test_batch_processing_with_connection_manager(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test batch processing with ConnectionManager.
        
        AAA Pattern:
            Arrange: Set up mock data and ConnectionManager for batch processing
            Act: Call batch processing methods
            Assert: Verify batch processing with ConnectionManager works correctly
            
        Validates:
            - Batch processing with ConnectionManager
            - Performance optimization in batch operations
            - Integration with ConnectionManager
        """
        # Arrange: Set up mock data and ConnectionManager for batch processing
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock execute_with_retry to return our test data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'TABLE_ROWS' in query:
                    mock_result.scalar.return_value = 50000
                elif 'information_schema.tables' in query:
                    mock_result.scalar.return_value = 25.5
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call batch processing methods
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            batch_size_info = analyzer.get_batch_size_info(test_tables)
            
            # Assert: Verify batch processing with ConnectionManager works correctly
            assert isinstance(batch_schema_info, dict)
            assert isinstance(batch_size_info, dict)
            assert len(batch_schema_info) == 3
            assert len(batch_size_info) == 3

    def test_timeout_handling_in_batch_operations(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test timeout handling in batch operations.
        
        AAA Pattern:
            Arrange: Set up mock operations that timeout
            Act: Call batch processing methods
            Assert: Verify timeout handling works correctly
            
        Validates:
            - Timeout handling in batch operations
            - Graceful timeout handling and recovery
            - Error propagation and logging
        """
        # Arrange: Set up mock operations that timeout
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector that raises timeout exceptions
            mock_inspector = Mock()
            mock_inspector.get_columns.side_effect = Exception("Database timeout")
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call batch processing methods
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            
            # Assert: Verify timeout handling works correctly by checking error dictionaries
            assert isinstance(batch_schema_info, dict)
            assert len(batch_schema_info) == 3
            
            # Verify each table has an error entry
            for table_name in test_tables:
                assert table_name in batch_schema_info
                table_info = batch_schema_info[table_name]
                assert 'error' in table_info
                assert 'Database timeout' in table_info['error'] 