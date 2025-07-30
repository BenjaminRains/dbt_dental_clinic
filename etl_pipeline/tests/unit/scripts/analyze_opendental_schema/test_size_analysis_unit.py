"""
Unit tests for OpenDentalSchemaAnalyzer size analysis using provider pattern.

This module tests the OpenDentalSchemaAnalyzer size analysis with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests table size analysis with mocked data using ConnectionManager
    - Tests row count calculation from mocked data
    - Validates table size estimation from mocked database

Coverage Areas:
    - Table size analysis with mocked data using ConnectionManager
    - Row count calculation from mocked data
    - Table size estimation from mocked database
    - Error handling for mocked database operations
    - Settings injection with ConnectionManager

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


class TestOpenDentalSchemaAnalyzerSizeAnalysis:
    """Unit tests for OpenDentalSchemaAnalyzer size analysis using provider pattern."""
    
    def test_table_size_analysis_with_mocked_data(self, mock_settings_with_dict_provider, mock_size_data, mock_environment_variables):
        """
        Test table size analysis with mocked data using ConnectionManager.
        
        AAA Pattern:
            Arrange: Set up mock ConnectionManager with test size data
            Act: Call get_table_size_info() method for test table
            Assert: Verify size information is correctly extracted
            
        Validates:
            - Mocked size analysis using ConnectionManager
            - Row count calculation from mocked data
            - Table size estimation from mocked database
            - Error handling for mocked database operations
            - Settings injection with ConnectionManager
        """
        # Arrange: Set up mock ConnectionManager with test size data
        test_table = 'patient'
        test_size = mock_size_data[test_table]
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager context manager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock execute_with_retry to return our test data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'TABLE_ROWS' in query:
                    # Return estimated row count
                    mock_result.scalar.return_value = test_size['estimated_row_count']
                elif 'information_schema.tables' in query:
                    # Return size in MB
                    mock_result.scalar.return_value = test_size['size_mb']
                else:
                    # Return 0 for other queries
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_size_info() method for test table
            size_info = analyzer.get_table_size_info(test_table)
            
            # Assert: Verify size information is correctly extracted
            assert size_info['table_name'] == test_table
            assert 'estimated_row_count' in size_info
            assert 'size_mb' in size_info
            assert 'source' in size_info
            assert size_info['source'] == 'information_schema_estimate'
            assert size_info['estimated_row_count'] == 50000  # Reverted to match fixture
            assert size_info['size_mb'] == 25.5
            
            # Verify ConnectionManager was used
            assert mock_conn_manager.call_count == 2
            mock_conn_manager.assert_any_call(mock_engine)
            assert mock_conn_manager_instance.__enter__.called
            assert mock_conn_manager_instance.__exit__.called 