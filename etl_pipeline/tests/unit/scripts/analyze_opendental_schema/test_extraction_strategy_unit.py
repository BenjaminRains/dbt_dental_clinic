"""
Unit tests for OpenDentalSchemaAnalyzer extraction strategy determination using provider pattern.

This module tests the OpenDentalSchemaAnalyzer extraction strategy determination with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests extraction strategy determination with mocked data
    - Tests small table vs incremental extraction logic
    - Validates extraction strategy selection based on table characteristics

Coverage Areas:
    - Extraction strategy determination for small tables
    - Extraction strategy determination for incremental tables
    - Strategy selection based on table size and characteristics
    - Integration with size analysis and schema analysis

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


class TestOpenDentalSchemaAnalyzerExtractionStrategy:
    """Unit tests for OpenDentalSchemaAnalyzer extraction strategy determination using provider pattern."""
    
    def test_extraction_strategy_determination_small_table(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test extraction strategy determination for small tables.
        
        AAA Pattern:
            Arrange: Set up mock schema data for small table
            Act: Call determine_extraction_strategy() method
            Assert: Verify small table strategy is correctly determined
            
        Validates:
            - Small table extraction strategy determination
            - Strategy selection based on table size
            - Integration with schema analysis
        """
        # Arrange: Set up mock schema data for small table
        test_table = 'definition'
        schema_info = mock_schema_data[test_table]
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock find_incremental_columns to return empty list for small table
            analyzer.find_incremental_columns = lambda table_name, schema_info: []
            
            # Act: Call determine_extraction_strategy() method
            strategy = analyzer.determine_extraction_strategy(test_table, schema_info, {'estimated_row_count': 100})
            
            # Assert: Verify full table strategy is correctly determined when no incremental columns
            assert strategy == 'full_table'

    def test_extraction_strategy_determination_incremental(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test extraction strategy determination for incremental tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for large table
            Act: Call determine_extraction_strategy() method
            Assert: Verify incremental strategy is correctly determined
            
        Validates:
            - Incremental extraction strategy determination
            - Strategy selection based on table size and characteristics
            - Integration with size analysis
        """
        # Arrange: Set up mock schema and size data for large table
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock find_incremental_columns to return timestamp columns for incremental table
            analyzer.find_incremental_columns = lambda table_name, schema_info: ['DateTStamp', 'SecDateTEdit']
            
            # Act: Call determine_extraction_strategy() method
            strategy = analyzer.determine_extraction_strategy(test_table, schema_info, size_info)
            
            # Assert: Verify incremental strategy is correctly determined
            assert strategy == 'incremental' 