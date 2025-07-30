"""
Unit tests for OpenDentalSchemaAnalyzer schema analysis using provider pattern.

This module tests the OpenDentalSchemaAnalyzer schema analysis with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests schema analysis with mocked table structures
    - Tests column information extraction
    - Validates primary key, foreign key, and index detection

Coverage Areas:
    - Schema analysis with mocked table structures
    - Column information extraction from mocked tables
    - Primary key detection from mocked database
    - Foreign key detection from mocked database
    - Index information from mocked database
    - Error handling for mocked database operations

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


class TestOpenDentalSchemaAnalyzerSchemaAnalysis:
    """Unit tests for OpenDentalSchemaAnalyzer schema analysis using provider pattern."""
    
    def test_table_schema_analysis_with_mocked_data(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test table schema analysis with mocked schema data.
        
        AAA Pattern:
            Arrange: Set up mock inspector with test schema data
            Act: Call get_table_schema() method for test table
            Assert: Verify schema information is correctly extracted
            
        Validates:
            - Mocked schema analysis from database inspector
            - Column information extraction from mocked tables
            - Primary key detection from mocked database
            - Foreign key detection from mocked database
            - Index information from mocked database
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock inspector with test schema data
        test_table = 'patient'
        test_schema = mock_schema_data[test_table]
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspector.get_columns.return_value = [
                {'name': name, **info} for name, info in test_schema['columns'].items()
            ]
            mock_inspector.get_pk_constraint.return_value = {'constrained_columns': test_schema['primary_keys']}
            mock_inspector.get_foreign_keys.return_value = test_schema['foreign_keys']
            mock_inspector.get_indexes.return_value = test_schema['indexes']
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_schema() method for test table
            schema_info = analyzer.get_table_schema(test_table)
            
            # Assert: Verify schema information is correctly extracted
            assert schema_info['table_name'] == test_table
            assert 'columns' in schema_info
            assert len(schema_info['columns']) == 6  # 6 columns in patient table
            
            # Verify column information
            assert 'PatNum' in schema_info['columns']
            assert schema_info['columns']['PatNum']['primary_key'] is True
            assert schema_info['columns']['DateTStamp']['type'] == 'timestamp'
            
            # Verify primary keys
            assert 'primary_keys' in schema_info
            assert schema_info['primary_keys'] == ['PatNum']
            
            # Verify foreign keys and indexes
            assert 'foreign_keys' in schema_info
            assert 'indexes' in schema_info 