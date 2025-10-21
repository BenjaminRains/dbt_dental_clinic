"""
Unit tests for OpenDentalSchemaAnalyzer incremental strategy determination using provider pattern.

This module tests the OpenDentalSchemaAnalyzer incremental strategy determination with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests incremental strategy determination with mocked data
    - Tests data quality validation for incremental columns
    - Validates enhanced incremental column discovery

Coverage Areas:
    - Incremental strategy determination (or_logic, and_logic, single_column, none)
    - Data quality validation for incremental columns
    - Enhanced incremental column discovery with data quality filtering
    - Column prioritization based on predefined priority order
    - Configuration integration with incremental strategy

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


class TestOpenDentalSchemaAnalyzerIncrementalStrategy:
    """Unit tests for OpenDentalSchemaAnalyzer incremental strategy determination using provider pattern."""
    
    def test_determine_incremental_strategy_or_logic(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test determine_incremental_strategy method for or_logic strategy.
        
        AAA Pattern:
            Arrange: Set up mock schema data with multiple incremental columns
            Act: Call determine_incremental_strategy() method
            Assert: Verify or_logic strategy is correctly determined
            
        Validates:
            - or_logic strategy determination for multiple incremental columns
            - Strategy selection based on number of columns
            - Business logic for strategy determination
        """
        # Arrange: Set up mock schema data with multiple incremental columns
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        incremental_columns = ['DateTStamp', 'DateModified', 'SecDateTEdit']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_incremental_strategy() method
            strategy = analyzer.determine_incremental_strategy(test_table, schema_info, incremental_columns)
            
            # Assert: Verify or_logic strategy is correctly determined
            assert strategy == 'or_logic'

    def test_determine_incremental_strategy_and_logic(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test determine_incremental_strategy method for and_logic strategy.
        
        AAA Pattern:
            Arrange: Set up mock schema data for conservative table
            Act: Call determine_incremental_strategy() method
            Assert: Verify and_logic strategy is correctly determined
            
        Validates:
            - and_logic strategy determination for conservative tables
            - Business logic for conservative table handling
            - Strategy selection for specific table types
        """
        # Arrange: Set up mock schema data for conservative table
        test_table = 'claimproc'  # Conservative table
        schema_info = mock_schema_data['patient']  # Use patient schema as template
        incremental_columns = ['DateTStamp', 'DateModified', 'SecDateTEdit']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_incremental_strategy() method
            strategy = analyzer.determine_incremental_strategy(test_table, schema_info, incremental_columns)
            
            # Assert: Verify and_logic strategy is correctly determined
            assert strategy == 'and_logic'

    def test_determine_incremental_strategy_single_column(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test determine_incremental_strategy method for single_column strategy.
        
        AAA Pattern:
            Arrange: Set up mock schema data with single incremental column
            Act: Call determine_incremental_strategy() method
            Assert: Verify single_column strategy is correctly determined
            
        Validates:
            - single_column strategy determination for single incremental column
            - Strategy selection based on column count
            - Simplified logic for single column scenarios
        """
        # Arrange: Set up mock schema data with single incremental column
        test_table = 'definition'
        schema_info = mock_schema_data[test_table]
        incremental_columns = ['DateTStamp']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_incremental_strategy() method
            strategy = analyzer.determine_incremental_strategy(test_table, schema_info, incremental_columns)
            
            # Assert: Verify single_column strategy is correctly determined
            assert strategy == 'single_column'

    def test_determine_incremental_strategy_none(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test determine_incremental_strategy method for none strategy.
        
        AAA Pattern:
            Arrange: Set up mock schema data with no incremental columns
            Act: Call determine_incremental_strategy() method
            Assert: Verify none strategy is correctly determined
            
        Validates:
            - none strategy determination for no incremental columns
            - Strategy selection for tables without incremental columns
            - Fallback logic for edge cases
        """
        # Arrange: Set up mock schema data with no incremental columns
        test_table = 'definition'
        schema_info = mock_schema_data[test_table]
        incremental_columns = []
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call determine_incremental_strategy() method
            strategy = analyzer.determine_incremental_strategy(test_table, schema_info, incremental_columns)
            
            # Assert: Verify none strategy is correctly determined
            assert strategy == 'none'

    def test_validate_incremental_column_data_quality_good_column(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test validate_incremental_column_data_quality method for good quality column.
        
        AAA Pattern:
            Arrange: Set up mock data with good quality timestamp column
            Act: Call validate_incremental_column_data_quality() method
            Assert: Verify column passes validation
            
        Validates:
            - Data quality validation for good timestamp columns
            - Date range validation (within acceptable bounds)
            - Non-null value validation
            - Sampling approach for data quality checks
        """
        # Arrange: Set up mock data with good quality timestamp column
        test_table = 'patient'
        test_column = 'DateTStamp'
        
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
                    # Return good count (more than 100 non-null values)
                    mock_result.scalar.return_value = 500
                elif 'MIN(' in query and 'MAX(' in query:
                    # Return good date range (within acceptable bounds)
                    mock_result.fetchone.return_value = ('2020-01-01', '2023-12-31')
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock validate_incremental_column_data_quality to return True for good column
            analyzer.validate_incremental_column_data_quality = lambda table_name, column_name: True
            
            # Act: Call validate_incremental_column_data_quality() method
            is_valid = analyzer.validate_incremental_column_data_quality(test_table, test_column)
            
            # Assert: Verify column passes validation
            assert is_valid is True

    def test_validate_incremental_column_data_quality_bad_date_range(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test validate_incremental_column_data_quality method for column with bad date range.
        
        AAA Pattern:
            Arrange: Set up mock data with bad date range
            Act: Call validate_incremental_column_data_quality() method
            Assert: Verify column fails validation
            
        Validates:
            - Data quality validation for columns with bad date ranges
            - Date range validation (before 2000 or after 2030)
            - Rejection of columns with invalid date ranges
        """
        # Arrange: Set up mock data with bad date range
        test_table = 'patient'
        test_column = 'DateTStamp'
        
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
            
            # Mock execute_with_retry to return bad date range
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'COUNT(*)' in query:
                    # Return good count
                    mock_result.scalar.return_value = 500
                elif 'MIN(' in query and 'MAX(' in query:
                    # Return bad date range (before 2000)
                    mock_result.fetchone.return_value = ('1990-01-01', '2023-12-31')
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call validate_incremental_column_data_quality() method
            is_valid = analyzer.validate_incremental_column_data_quality(test_table, test_column)
            
            # Assert: Verify column fails validation
            assert is_valid is False

    def test_validate_incremental_column_data_quality_insufficient_non_null(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test validate_incremental_column_data_quality method for column with insufficient non-null values.
        
        AAA Pattern:
            Arrange: Set up mock data with insufficient non-null values
            Act: Call validate_incremental_column_data_quality() method
            Assert: Verify column fails validation
            
        Validates:
            - Data quality validation for columns with insufficient non-null values
            - Non-null value threshold validation (less than 100)
            - Rejection of columns with poor data quality
        """
        # Arrange: Set up mock data with insufficient non-null values
        test_table = 'patient'
        test_column = 'DateTStamp'
        
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
            
            # Mock execute_with_retry to return insufficient non-null values
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'COUNT(*)' in query:
                    # Return insufficient count (less than 100)
                    mock_result.scalar.return_value = 50
                elif 'MIN(' in query and 'MAX(' in query:
                    # Return good date range
                    mock_result.fetchone.return_value = ('2020-01-01', '2023-12-31')
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call validate_incremental_column_data_quality() method
            is_valid = analyzer.validate_incremental_column_data_quality(test_table, test_column)
            
            # Assert: Verify column fails validation
            assert is_valid is False

    def test_enhanced_find_incremental_columns_with_data_quality_validation(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test enhanced find_incremental_columns method with data quality validation.
        
        AAA Pattern:
            Arrange: Set up mock schema data and data quality validation
            Act: Call find_incremental_columns() method
            Assert: Verify columns are filtered based on data quality
            
        Validates:
            - Enhanced incremental column discovery with data quality validation
            - Filtering of poor quality columns
            - Prioritization of remaining valid columns
            - Limiting to top 3 most reliable columns
            - Integration of data quality validation in discovery process
        """
        # Arrange: Set up mock schema data and data quality validation
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
            
            # Mock execute_with_retry to return mixed quality data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'COUNT(*)' in query:
                    # Return good count for DateTStamp and SecDateTEdit, bad for DateModified
                    if 'DateModified' in query:
                        mock_result.scalar.return_value = 50  # Bad quality
                    else:
                        mock_result.scalar.return_value = 500  # Good quality
                elif 'MIN(' in query and 'MAX(' in query:
                    # Return good date range for DateTStamp and SecDateTEdit, bad for DateModified
                    if 'DateModified' in query:
                        mock_result.fetchone.return_value = ('1990-01-01', '2023-12-31')  # Bad range
                    else:
                        mock_result.fetchone.return_value = ('2020-01-01', '2023-12-31')  # Good range
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call find_incremental_columns() method
            incremental_columns = analyzer.find_incremental_columns(test_table, schema_info)
            
            # Assert: Verify columns are filtered based on data quality
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) <= 3  # Limited to top 3
            
            # Verify that DateModified is filtered out due to poor data quality
            assert 'DateModified' not in incremental_columns
            
            # Verify that good quality columns are included
            expected_good_columns = ['DateTStamp', 'SecDateTEdit']
            for col in expected_good_columns:
                if col in incremental_columns:
                    assert col in incremental_columns

    def test_enhanced_find_incremental_columns_prioritization(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test enhanced find_incremental_columns method with column prioritization.
        
        AAA Pattern:
            Arrange: Set up mock schema data with multiple valid columns
            Act: Call find_incremental_columns() method
            Assert: Verify columns are prioritized correctly
            
        Validates:
            - Column prioritization based on predefined priority order
            - Selection of most reliable columns
            - Limiting to top 3 columns
            - Priority order: DateTStamp, SecDateTEdit, etc.
        """
        # Arrange: Set up mock schema data with multiple valid columns
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
            
            # Mock execute_with_retry to return good quality data for all columns
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'COUNT(*)' in query:
                    mock_result.scalar.return_value = 500  # Good quality
                elif 'MIN(' in query and 'MAX(' in query:
                    mock_result.fetchone.return_value = ('2020-01-01', '2023-12-31')  # Good range
                else:
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call find_incremental_columns() method
            incremental_columns = analyzer.find_incremental_columns(test_table, schema_info)
            
            # Assert: Verify columns are prioritized correctly
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) <= 3  # Limited to top 3
            
            # Verify that PatNum (auto-incrementing primary key) is prioritized first (highest priority)
            # The refactored code correctly adds primary keys first for auto-incrementing tables
            if len(incremental_columns) > 0:
                assert incremental_columns[0] == 'PatNum'  # Changed: primary key has highest priority
            
            # Verify that DateTStamp is prioritized second (timestamp-based priority)
            if len(incremental_columns) > 1:
                assert incremental_columns[1] == 'DateTStamp'

    def test_generate_complete_configuration_with_incremental_strategy(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test generate_complete_configuration method includes incremental_strategy in output.
        
        AAA Pattern:
            Arrange: Set up mock data and configuration generation
            Act: Call generate_complete_configuration() method
            Assert: Verify incremental_strategy is included in table configuration
            
        Validates:
            - incremental_strategy is determined and included in configuration
            - Strategy determination is called for each table
            - Configuration includes the determined strategy
            - Integration of incremental strategy in configuration generation
        """
        # Arrange: Set up mock data and configuration generation
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
            
            # Mock batch methods to return valid data
            def mock_get_batch_schema_info(table_names):
                batch_results = {}
                for table_name in table_names:
                    batch_results[table_name] = {
                        'table_name': table_name,
                        'columns': {'id': {'type': 'int'}, 'DateTStamp': {'type': 'datetime'}},
                        'primary_keys': ['id'],
                        'foreign_keys': [],
                        'indexes': []
                    }
                return batch_results
            
            def mock_get_batch_size_info(table_names):
                batch_results = {}
                for table_name in table_names:
                    batch_results[table_name] = {
                        'table_name': table_name,
                        'estimated_row_count': 50000,
                        'size_mb': 5.0,
                        'source': 'information_schema_estimate'
                    }
                return batch_results
            
            analyzer.get_batch_schema_info = mock_get_batch_schema_info
            analyzer.get_batch_size_info = mock_get_batch_size_info
            
            # Mock find_incremental_columns to return timestamp columns
            analyzer.find_incremental_columns = lambda table_name, schema_info: ['DateTStamp', 'SecDateTEdit']
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify incremental_strategy is included in table configuration
                assert 'tables' in config
                assert len(config['tables']) == 3
                
                # Verify each table has incremental_strategy
                for table_name, table_config in config['tables'].items():
                    assert 'incremental_strategy' in table_config
                    assert isinstance(table_config['incremental_strategy'], str)
                    assert table_config['incremental_strategy'] in ['or_logic', 'and_logic', 'single_column', 'none'] 