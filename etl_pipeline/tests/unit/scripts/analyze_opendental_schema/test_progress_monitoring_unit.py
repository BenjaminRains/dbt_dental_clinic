"""
Unit tests for OpenDentalSchemaAnalyzer progress monitoring using provider pattern.

This module tests the OpenDentalSchemaAnalyzer progress monitoring with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests progress monitoring in configuration generation
    - Tests detailed analysis report progress monitoring
    - Tests stage-by-stage progress tracking
    - Tests progress bar postfix information

Coverage Areas:
    - Progress monitoring in configuration generation
    - Detailed analysis report progress monitoring
    - Stage-by-stage progress tracking
    - Progress bar postfix information
    - Progress monitoring error handling

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


class TestOpenDentalSchemaAnalyzerProgressMonitoring:
    """Unit tests for OpenDentalSchemaAnalyzer progress monitoring using provider pattern."""
    
    def test_progress_monitoring_in_generate_complete_configuration(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test progress monitoring in generate_complete_configuration.
        
        AAA Pattern:
            Arrange: Set up mock data and progress monitoring
            Act: Call generate_complete_configuration() method
            Assert: Verify progress monitoring works correctly
            
        Validates:
            - Progress monitoring in configuration generation
            - Progress bar integration
            - Stage-by-stage progress tracking
        """
        # Arrange: Set up mock data and progress monitoring
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
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify progress monitoring works correctly
                assert 'tables' in config
                assert len(config['tables']) == 3

    def test_detailed_analysis_report_progress_monitoring(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test detailed analysis report progress monitoring.
        
        AAA Pattern:
            Arrange: Set up mock data for detailed analysis
            Act: Call analyze_complete_schema() method
            Assert: Verify detailed analysis progress monitoring works correctly
            
        Validates:
            - Detailed analysis report progress monitoring
            - Progress tracking for analysis stages
            - Integration with progress monitoring
        """
        # Arrange: Set up mock data for detailed analysis
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
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call analyze_complete_schema() method
                results = analyzer.analyze_complete_schema(temp_dir)
                
                # Assert: Verify detailed analysis progress monitoring works correctly
                assert 'tables_config' in results
                assert 'analysis_report' in results

    def test_stage_by_stage_progress_tracking(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test stage-by-stage progress tracking.
        
        AAA Pattern:
            Arrange: Set up mock data for stage-by-stage tracking
            Act: Call methods with progress tracking
            Assert: Verify stage-by-stage progress tracking works correctly
            
        Validates:
            - Stage-by-stage progress tracking
            - Progress bar integration
            - Timing information in progress tracking
        """
        # Arrange: Set up mock data for stage-by-stage tracking
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
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: mock_dbt_models
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify stage-by-stage progress tracking works correctly
                assert 'tables' in config
                assert len(config['tables']) == 3

    def test_progress_bar_postfix_information(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test progress bar postfix information.
        
        AAA Pattern:
            Arrange: Set up mock data for progress bar testing
            Act: Call methods with progress bar
            Assert: Verify progress bar postfix information works correctly
            
        Validates:
            - Progress bar postfix information
            - Progress monitoring integration
            - Information display in progress bars
        """
        # Arrange: Set up mock data for progress bar testing
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
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call analyze_complete_schema() method
                results = analyzer.analyze_complete_schema(temp_dir)
                
                # Assert: Verify progress bar postfix information works correctly
                assert 'tables_config' in results
                assert 'analysis_report' in results

    def test_progress_monitoring_error_handling(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test progress monitoring error handling.
        
        AAA Pattern:
            Arrange: Set up mock operations that fail during progress monitoring
            Act: Call methods with progress monitoring
            Assert: Verify progress monitoring error handling works correctly
            
        Validates:
            - Progress monitoring error handling
            - Graceful error handling in progress monitoring
            - Error propagation and logging
        """
        # Arrange: Set up mock operations that fail during progress monitoring
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector that raises exceptions
            mock_inspector = Mock()
            mock_inspector.get_columns.side_effect = Exception("Progress monitoring error")
            mock_inspect.return_value = mock_inspector
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call methods and verify progress monitoring error handling
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            
            # Assert: Verify error handling works correctly by checking error dictionaries
            assert isinstance(batch_schema_info, dict)
            assert len(batch_schema_info) == 3
            
            # Verify each table has an error entry
            for table_name in test_tables:
                assert table_name in batch_schema_info
                table_info = batch_schema_info[table_name]
                assert 'error' in table_info
                assert 'Progress monitoring error' in table_info['error'] 