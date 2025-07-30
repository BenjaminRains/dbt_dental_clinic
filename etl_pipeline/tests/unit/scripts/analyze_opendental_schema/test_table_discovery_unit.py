"""
Unit tests for OpenDentalSchemaAnalyzer table discovery using provider pattern.

This module tests the OpenDentalSchemaAnalyzer table discovery with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests table discovery with mocked database inspector
    - Tests filtering of excluded patterns
    - Validates database inspector integration

Coverage Areas:
    - Table discovery with mocked database inspector
    - Proper filtering of excluded patterns
    - Error handling for mocked database operations
    - Settings injection with mocked database connections
    - Inspector integration and method mocking

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


class TestOpenDentalSchemaAnalyzerTableDiscovery:
    """Unit tests for OpenDentalSchemaAnalyzer table discovery using provider pattern."""
    
    def test_table_discovery_with_mocked_inspector(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test table discovery with mocked database inspector.
        
        AAA Pattern:
            Arrange: Set up mock inspector with test table names
            Act: Call discover_all_tables() method
            Assert: Verify tables are discovered correctly
            
        Validates:
            - Mocked table discovery from database inspector
            - Proper filtering of excluded patterns
            - Error handling for mocked database operations
            - Settings injection with mocked database connections
        """
        # Arrange: Set up mock inspector with test table names
        test_tables = ['patient', 'appointment', 'procedurelog', 'insplan', 'definition', 'securitylog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector with proper method configuration
            mock_inspector = Mock()
            
            # Configure mock inspector methods to return proper data structures
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
            mock_inspector.get_table_names.return_value = test_tables
            
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call discover_all_tables() method
            tables = analyzer.discover_all_tables()
            
            # Assert: Verify tables are discovered correctly
            assert isinstance(tables, list)
            assert len(tables) == 6
            assert 'patient' in tables
            assert 'appointment' in tables
            assert 'procedurelog' in tables 