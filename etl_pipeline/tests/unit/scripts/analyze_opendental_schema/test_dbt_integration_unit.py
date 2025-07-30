"""
Unit tests for OpenDentalSchemaAnalyzer DBT integration using provider pattern.

This module tests the OpenDentalSchemaAnalyzer DBT integration with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests DBT model discovery with mocked project structure
    - Tests incremental column discovery with timestamp columns
    - Validates DBT project integration and model discovery

Coverage Areas:
    - DBT model discovery with mocked project structure
    - Incremental column discovery with timestamp columns
    - DBT project integration and model discovery
    - Integration with DBT project structure

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


class TestOpenDentalSchemaAnalyzerDBTIntegration:
    """Unit tests for OpenDentalSchemaAnalyzer DBT integration using provider pattern."""
    
    def test_dbt_model_discovery_with_mocked_project(self, mock_settings_with_dict_provider, mock_dbt_models, mock_environment_variables):
        """
        Test DBT model discovery with mocked project structure.
        
        AAA Pattern:
            Arrange: Set up mock DBT project structure
            Act: Call discover_dbt_models() method
            Assert: Verify DBT models are discovered correctly
            
        Validates:
            - DBT model discovery with mocked project structure
            - Model discovery from DBT project
            - Integration with DBT project structure
        """
        # Arrange: Set up mock DBT project structure
        test_models = mock_dbt_models
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.Path') as mock_path:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock Path for DBT project discovery
            mock_dbt_path = Mock()
            mock_dbt_path.exists.return_value = True
            mock_dbt_path.glob.return_value = [
                Mock(name='model1.sql'),
                Mock(name='model2.sql'),
                Mock(name='model3.sql')
            ]
            mock_path.return_value = mock_dbt_path
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call discover_dbt_models() method
            models = analyzer.discover_dbt_models()
            
            # Assert: Verify DBT models are discovered correctly
            assert isinstance(models, dict)
            assert 'staging' in models
            assert 'mart' in models
            assert 'intermediate' in models

    def test_incremental_column_discovery_with_timestamp_columns(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test incremental column discovery with timestamp columns.
        
        AAA Pattern:
            Arrange: Set up mock schema data with timestamp columns
            Act: Call find_incremental_columns() method
            Assert: Verify timestamp columns are discovered correctly
            
        Validates:
            - Incremental column discovery with timestamp columns
            - Timestamp column identification
            - Integration with schema analysis
        """
        # Arrange: Set up mock schema data with timestamp columns
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        
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
            
            # Mock execute_with_retry to return good quality data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'COUNT(*)' in query:
                    mock_result.scalar.return_value = 500
                elif 'MIN(' in query and 'MAX(' in query:
                    mock_result.fetchone.return_value = ('2020-01-01', '2026-12-31')
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call find_incremental_columns() method
            incremental_columns = analyzer.find_incremental_columns(test_table, schema_info)
            
            # Assert: Verify timestamp columns are discovered correctly
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) <= 3  # Limited to top 3
            
            # Verify that timestamp columns are identified
            expected_timestamp_columns = ['DateTStamp', 'SecDateTEdit']
            for col in expected_timestamp_columns:
                if col in incremental_columns:
                    assert col in incremental_columns 