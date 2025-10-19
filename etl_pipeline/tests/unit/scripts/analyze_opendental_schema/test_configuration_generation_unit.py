"""
Unit tests for OpenDentalSchemaAnalyzer configuration generation using provider pattern.

This module tests the OpenDentalSchemaAnalyzer configuration generation with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests complete configuration generation
    - Tests schema hash generation
    - Tests configuration file writing and validation

Coverage Areas:
    - Complete configuration generation
    - Schema hash generation
    - Configuration file writing and validation
    - Integration with all analysis components

ETL Context:
    - Critical for ETL pipeline configuration generation
    - Tests with mocked dental clinic database schemas
    - Uses Settings injection with DictConfigProvider for unit testing
    - Generates test tables.yml for ETL pipeline configuration
"""
import pytest
import tempfile
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


class TestOpenDentalSchemaAnalyzerConfigurationGeneration:
    """Unit tests for OpenDentalSchemaAnalyzer configuration generation using provider pattern."""
    
    def test_complete_configuration_generation_with_mocked_data(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test complete configuration generation with mocked data.
        
        AAA Pattern:
            Arrange: Set up mock data for complete configuration generation
            Act: Call generate_complete_configuration() method
            Assert: Verify complete configuration generation works correctly
            
        Validates:
            - Complete configuration generation
            - Integration with all analysis components
            - Configuration structure and content
        """
        # Arrange: Set up mock data for complete configuration generation
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
            
            # Mock execute_with_retry to return good quality data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'COUNT(*)' in query:
                    mock_result.scalar.return_value = 500
                elif 'information_schema.tables' in query:
                    mock_result.scalar.return_value = 5.5
                elif 'MIN(' in query and 'MAX(' in query:
                    mock_result.fetchone.return_value = ('2020-01-01', '2023-12-31')
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: mock_dbt_models
            
            # Mock batch schema and size methods to return valid data
            def mock_get_batch_schema_info(table_names):
                batch_results = {}
                for table_name in table_names:
                    batch_results[table_name] = mock_schema_data.get(table_name, {
                        'table_name': table_name,
                        'columns': {'id': {'type': 'int'}, 'name': {'type': 'varchar'}},
                        'primary_keys': ['id'],
                        'foreign_keys': [],
                        'indexes': []
                    })
                return batch_results
            
            def mock_get_batch_size_info(table_names):
                batch_results = {}
                for table_name in table_names:
                    batch_results[table_name] = mock_size_data.get(table_name, {
                        'table_name': table_name,
                        'estimated_row_count': 1000,
                        'size_mb': 5.0,
                        'source': 'information_schema_estimate'
                    })
                return batch_results
            
            analyzer.get_batch_schema_info = mock_get_batch_schema_info
            analyzer.get_batch_size_info = mock_get_batch_size_info
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify complete configuration generation works correctly
                assert 'tables' in config
                assert len(config['tables']) == 3
                
                # Verify each table has required configuration
                for table_name, table_config in config['tables'].items():
                    assert 'table_name' in table_config
                    assert 'table_importance' in table_config
                    assert 'extraction_strategy' in table_config
                    assert 'estimated_rows' in table_config
                    assert 'estimated_size_mb' in table_config

    def test_schema_hash_generation_with_mocked_data(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test schema hash generation with mocked data.
        
        AAA Pattern:
            Arrange: Set up mock schema data for hash generation
            Act: Call generate_schema_hash() method
            Assert: Verify schema hash generation works correctly
            
        Validates:
            - Schema hash generation
            - Hash consistency and uniqueness
            - Integration with schema analysis
        """
        # Arrange: Set up mock schema data for hash generation
        test_table = 'patient'
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock schema data
            def mock_get_columns(table_name):
                return [
                    {'name': 'id', 'type': 'INT', 'nullable': False, 'default': None},
                    {'name': 'name', 'type': 'VARCHAR(255)', 'nullable': True, 'default': None}
                ]
            
            def mock_get_pk_constraint(table_name):
                return {'constrained_columns': ['id']}
            
            def mock_get_foreign_keys(table_name):
                return []
            
            def mock_get_indexes(table_name):
                return []
            
            # Configure the mock inspector methods
            mock_inspector.get_columns = mock_get_columns
            mock_inspector.get_pk_constraint = mock_get_pk_constraint
            mock_inspector.get_foreign_keys = mock_get_foreign_keys
            mock_inspector.get_indexes = mock_get_indexes
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call _generate_schema_hash() method (private, takes list of tables)
            schema_hash = analyzer._generate_schema_hash([test_table])
            
            # Assert: Verify schema hash generation works correctly
            assert isinstance(schema_hash, str)
            assert len(schema_hash) > 0

    def test_schema_hash_generation_error_handling(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test schema hash generation error handling.
        
        AAA Pattern:
            Arrange: Set up mock data that causes hash generation errors
            Act: Call generate_schema_hash() method
            Assert: Verify error handling works correctly
            
        Validates:
            - Schema hash generation error handling
            - Graceful error handling and recovery
            - Error propagation and logging
        """
        # Arrange: Set up mock data that causes hash generation errors
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
            
            # Mock the get_table_schema method to raise the exception
            def mock_get_table_schema(table_name):
                raise Exception("Schema inspection error")
            
            analyzer = OpenDentalSchemaAnalyzer()
            analyzer.get_table_schema = mock_get_table_schema
            
            # Act: Call _generate_schema_hash() method (private, takes list of tables)
            schema_hash = analyzer._generate_schema_hash([test_table])
            
            # Assert: Verify error handling works correctly by checking for "unknown" hash
            assert isinstance(schema_hash, str)
            assert schema_hash == "unknown", "Should return 'unknown' when schema inspection fails"

    def test_fail_fast_on_missing_environment(self):
        """
        Test fail fast on missing environment.
        
        AAA Pattern:
            Arrange: Set up missing environment variables
            Act: Try to initialize analyzer
            Assert: Verify fail fast behavior works correctly
            
        Validates:
            - Fail fast on missing environment
            - Environment validation
            - Error handling for missing configuration
        """
        # Arrange: Set up missing environment variables and mock environment files to not exist
        with patch.dict('os.environ', {}, clear=True), \
             patch('pathlib.Path.exists', return_value=False):
            # Act & Assert: Try to initialize analyzer and verify fail fast behavior
            with pytest.raises(ValueError, match="ETL_ENVIRONMENT"):
                OpenDentalSchemaAnalyzer() 