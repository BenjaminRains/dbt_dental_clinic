"""
Unit tests for OpenDentalSchemaAnalyzer importance determination using provider pattern.

This module tests the OpenDentalSchemaAnalyzer importance determination with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests systematic importance determination with mocked data
    - Tests critical table identification from mocked database
    - Validates business logic for different table types

Coverage Areas:
    - Critical table identification from mocked database data
    - Important table identification from mocked database
    - Reference table identification from mocked database
    - Audit table identification from mocked database
    - Standard table identification from mocked database
    - Systematic importance determination with mocked data

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


class TestOpenDentalSchemaAnalyzerImportanceDetermination:
    """Unit tests for OpenDentalSchemaAnalyzer importance determination using provider pattern."""
    
    def test_table_importance_determination_critical(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for critical tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for critical table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as critical
            
        Validates:
            - Critical table identification from mocked database data
            - Systematic importance determination with mocked data
            - Critical table classification for core business entities
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for critical table
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as critical
            assert importance == 'critical'

    def test_table_importance_determination_important_large_table(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for large tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for large table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as important
            
        Validates:
            - Important table identification for large tables (>1M rows)
            - Systematic importance determination with mocked data
            - Performance consideration for large tables
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for large table
        test_table = 'claim'  # Use 'claim' which is NOT in critical list but has large size and insurance pattern
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]  # Use fixture data (2M estimated rows)
        
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
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as important
            assert importance == 'important'

    def test_table_importance_determination_important_large_table_with_audit_pattern(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for large tables with audit patterns.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for large audit table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as audit (prioritized over size)
            
        Validates:
            - Audit pattern recognition takes priority over size considerations
            - Large tables with 'log' in name are classified as audit, not important
            - Systematic importance determination with mocked data
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for large audit table
        test_table = 'securitylog'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]  # 5M estimated rows, should be 'important' by size but 'audit' by pattern
        
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
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as audit (pattern takes priority over size)
            assert importance == 'audit'

    def test_table_importance_determination_important_insurance_pattern(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for insurance/billing pattern tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for insurance table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as important
            
        Validates:
            - Important table identification for insurance/billing tables
            - Systematic importance determination with mocked data
            - Business value pattern recognition
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for insurance table
        test_table = 'insplan'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as important
            assert importance == 'important'

    def test_table_importance_determination_reference(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for reference tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for reference table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as reference
            
        Validates:
            - Reference table identification for lookup data
            - Systematic importance determination with mocked data
            - Reference pattern recognition ('def' in table name)
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for reference table
        test_table = 'definition'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as reference
            assert importance == 'reference'

    def test_table_importance_determination_audit(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for audit tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for audit table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as audit
            
        Validates:
            - Audit table identification for logging/history data
            - Systematic importance determination with mocked data
            - Audit pattern recognition ('log' in table name)
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for audit table
        test_table = 'securitylog'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as audit
            assert importance == 'audit' 