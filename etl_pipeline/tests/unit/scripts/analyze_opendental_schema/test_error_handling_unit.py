"""
Unit tests for OpenDentalSchemaAnalyzer error handling using provider pattern.

This module tests the OpenDentalSchemaAnalyzer error handling with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests error handling in table schema analysis
    - Tests error handling in table size analysis
    - Tests schema hash generation error handling
    - Validates graceful error handling and recovery

Coverage Areas:
    - Error handling in table schema analysis
    - Error handling in table size analysis
    - Schema hash generation error handling
    - Graceful error handling and recovery
    - Error propagation and logging

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


class TestOpenDentalSchemaAnalyzerErrorHandling:
    """Unit tests for OpenDentalSchemaAnalyzer error handling using provider pattern."""
    
    def test_error_handling_in_table_schema_analysis(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test error handling in table schema analysis.
        
        AAA Pattern:
            Arrange: Set up mock inspector that raises exceptions
            Act: Call get_table_schema() method
            Assert: Verify error is handled gracefully
            
        Validates:
            - Error handling in table schema analysis
            - Graceful error handling and recovery
            - Error propagation and logging
        """
        # Arrange: Set up mock inspector that raises exceptions
        test_table = 'patient'
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector that raises exceptions
            mock_inspector = Mock()
            mock_inspector.get_columns.side_effect = Exception("Database connection error")
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_schema() method
            schema_info = analyzer.get_table_schema(test_table)
            
            # Assert: Verify error handling works correctly by checking error dictionary
            assert isinstance(schema_info, dict)
            assert 'error' in schema_info
            assert 'Database connection error' in schema_info['error']

    def test_error_handling_in_table_size_analysis(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test error handling in table size analysis.
        
        AAA Pattern:
            Arrange: Set up mock ConnectionManager that raises exceptions
            Act: Call get_table_size_info() method
            Assert: Verify error is handled gracefully
            
        Validates:
            - Error handling in table size analysis
            - Graceful error handling and recovery
            - Error propagation and logging
        """
        # Arrange: Set up mock ConnectionManager that raises exceptions
        test_table = 'patient'
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager that raises exceptions
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager_instance.execute_with_retry.side_effect = Exception("Query execution error")
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_size_info() method
            size_info = analyzer.get_table_size_info(test_table)
            
            # Assert: Verify error handling works correctly by checking error dictionary
            assert isinstance(size_info, dict)
            assert 'error' in size_info
            assert 'Query execution error' in size_info['error']

    def test_schema_hash_generation_error_handling(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test schema hash generation error handling.
        
        AAA Pattern:
            Arrange: Set up mock schema data that causes hash generation errors
            Act: Call generate_schema_hash() method
            Assert: Verify error is handled gracefully
            
        Validates:
            - Schema hash generation error handling
            - Graceful error handling and recovery
            - Error propagation and logging
        """
        # Arrange: Set up mock schema data that causes hash generation errors
        test_table = 'patient'
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector that raises exceptions
            mock_inspector = Mock()
            mock_inspector.get_columns.side_effect = Exception("Schema inspection error")
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock the get_table_schema method to raise the exception
            def mock_get_table_schema(table_name):
                raise Exception("Schema inspection error")
            
            analyzer.get_table_schema = mock_get_table_schema
            
            # Act: Call generate_schema_hash() method
            schema_hash = analyzer.generate_schema_hash(test_table)
            
            # Assert: Verify error handling works correctly by checking for "unknown" hash
            assert isinstance(schema_hash, str)
            assert schema_hash == "unknown", "Should return 'unknown' when schema inspection fails" 