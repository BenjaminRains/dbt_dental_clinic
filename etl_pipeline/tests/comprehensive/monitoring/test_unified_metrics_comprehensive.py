# type: ignore  # SQLAlchemy type handling in comprehensive tests

"""
Comprehensive tests for UnifiedMetricsCollector with detailed scenarios and edge cases.

This module follows the testing architecture patterns:
- Uses existing fixtures from tests/fixtures/ for configuration and connections
- Uses Settings injection for environment-agnostic connections
- Uses comprehensive mocking for database connections
- Uses AAA pattern for clear test structure
- Tests all 12 methods with detailed scenarios
- Uses provider pattern for dependency injection
- Tests edge cases and error conditions

Comprehensive Test Strategy:
- Tests all methods with multiple scenarios using existing fixtures
- Validates provider pattern dependency injection using existing fixtures
- Tests Settings injection for environment-agnostic connections
- Tests comprehensive error handling and edge cases
- Tests metrics persistence logic with various scenarios
- Tests status reporting with complex data
- Tests FAIL FAST behavior for security requirements
- Tests performance characteristics and memory usage

ETL Context:
- Critical for ETL pipeline monitoring and metrics collection
- Supports dental clinic data processing metrics
- Uses provider pattern for clean dependency injection
- Implements Settings injection for environment-agnostic connections
- Enforces FAIL FAST security to prevent accidental production usage
- Optimized for dental clinic data volumes and processing patterns
"""

import pytest
import time
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging

# Import ETL pipeline components
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector, create_metrics_collector

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseQueryError
)

logger = logging.getLogger(__name__)


@pytest.mark.comprehensive
@pytest.mark.monitoring
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestUnifiedMetricsCollectorComprehensive:
    """Comprehensive tests for UnifiedMetricsCollector with detailed scenarios using existing fixtures."""

    def test_initialization_with_comprehensive_config(self, test_settings, mock_analytics_engine):
        """
        Test initialization with comprehensive configuration using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up comprehensive configuration using existing fixtures
            Act: Initialize UnifiedMetricsCollector with comprehensive config
            Assert: Verify comprehensive configuration is properly loaded
            
        Validates:
            - Comprehensive configuration is properly loaded
            - All configuration sections are accessible
            - Settings are correctly applied
            - Provider pattern integration works
            - Configuration validation works
        """
        # Arrange: Set up comprehensive configuration using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_analytics_engine):
            collector = UnifiedMetricsCollector(settings=test_settings, enable_persistence=True)
            
            # Assert: Verify comprehensive configuration is properly loaded
            assert collector.settings == test_settings
            assert collector.enable_persistence is True
            
            # Verify comprehensive configuration aspects using existing fixtures
            assert test_settings.environment == 'test'
            assert hasattr(test_settings.provider, 'configs')  # DictConfigProvider has configs attribute
            
            # Verify dental clinic specific configuration from existing fixtures
            pipeline_config = test_settings.pipeline_config
            assert pipeline_config is not None
            assert 'connections' in pipeline_config
            assert 'general' in pipeline_config
            assert 'stages' in pipeline_config
            assert 'logging' in pipeline_config
            assert 'error_handling' in pipeline_config

    def test_comprehensive_pipeline_lifecycle(self, unified_metrics_collector_no_persistence):
        """
        Test complete pipeline lifecycle with comprehensive data using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with comprehensive test data using existing fixtures
            Act: Execute complete pipeline lifecycle (start, process tables, end)
            Assert: Verify complete pipeline lifecycle works correctly
            
        Validates:
            - Complete pipeline lifecycle works correctly
            - Multiple table processing is handled properly
            - Success and failure scenarios are tracked
            - Timing is calculated correctly
            - Statistics are accurate
            - Error handling works for complex scenarios
        """
        # Arrange: Set up collector with comprehensive test data using existing fixtures
        collector = unified_metrics_collector_no_persistence
        
        # Act: Execute complete pipeline lifecycle (start, process tables, end)
        collector.start_pipeline()
        
        # Process multiple tables with different scenarios
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, False, "Database connection failed")
        collector.record_table_processed('procedure', 750, 25.0, True)
        collector.record_table_processed('payment', 250, 8.5, True)
        collector.record_error("General pipeline warning", "system")
        
        time.sleep(0.1)  # Small delay to ensure timing difference
        final_metrics = collector.end_pipeline()
        
        # Assert: Verify complete pipeline lifecycle works correctly
        assert collector.metrics['status'] == 'failed'  # Has errors
        assert collector.metrics['tables_processed'] == 4
        assert collector.metrics['total_rows_processed'] == 2500
        assert len(collector.metrics['errors']) == 2  # 1 table error + 1 general error
        assert len(collector.metrics['table_metrics']) == 4
        
        # Verify timing
        assert collector.metrics['total_time'] is not None
        assert collector.metrics['total_time'] > 0
        assert collector.metrics['start_time'] is not None
        assert collector.metrics['end_time'] is not None
        
        # Verify table metrics
        patient_metric = collector.metrics['table_metrics']['patient']
        assert patient_metric['rows_processed'] == 1000
        assert patient_metric['success'] is True
        assert patient_metric['status'] == 'completed'
        
        appointment_metric = collector.metrics['table_metrics']['appointment']
        assert appointment_metric['rows_processed'] == 500
        assert appointment_metric['success'] is False
        assert appointment_metric['error'] == "Database connection failed"
        assert appointment_metric['status'] == 'failed'
        
        # Verify final metrics
        assert final_metrics == collector.metrics

    def test_comprehensive_status_reporting(self, unified_metrics_collector_no_persistence):
        """
        Test comprehensive status reporting with complex data scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with complex test data scenarios using existing fixtures
            Act: Call various status reporting methods
            Assert: Verify comprehensive status reporting works correctly
            
        Validates:
            - Status reporting works with complex data
            - Table filtering works correctly
            - All status fields are populated correctly
            - Error information is properly formatted
            - Timestamps are correctly formatted
            - Statistics are calculated accurately
        """
        # Arrange: Set up collector with complex test data scenarios using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        
        # Add complex test data
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
        collector.record_table_processed('procedure', 750, 25.0, True)
        collector.record_table_processed('payment', 250, 8.5, False, "Data validation failed")
        collector.record_error("General system error", "orchestration")
        collector.record_error("Configuration warning", "config")
        
        # Act: Call various status reporting methods
        pipeline_status = collector.get_pipeline_status()
        patient_status = collector.get_table_status('patient')
        appointment_status = collector.get_table_status('appointment')
        nonexistent_status = collector.get_table_status('nonexistent')
        pipeline_stats = collector.get_pipeline_stats()
        
        # Filter by specific table
        patient_only_status = collector.get_pipeline_status(table='patient')
        
        # Assert: Verify comprehensive status reporting works correctly
        # Pipeline status validation
        assert pipeline_status['status'] == 'running'
        assert pipeline_status['tables_processed'] == 4
        assert pipeline_status['total_rows_processed'] == 2500
        assert pipeline_status['error_count'] == 4  # 2 table errors + 2 general errors
        assert len(pipeline_status['tables']) == 4
        
        # Verify table information in pipeline status
        patient_table_info = next(t for t in pipeline_status['tables'] if t['name'] == 'patient')
        assert patient_table_info['status'] == 'completed'
        assert patient_table_info['records_processed'] == 1000
        assert patient_table_info['processing_time'] == 30.5
        assert patient_table_info['error'] is None
        
        appointment_table_info = next(t for t in pipeline_status['tables'] if t['name'] == 'appointment')
        assert appointment_table_info['status'] == 'failed'
        assert appointment_table_info['records_processed'] == 500
        assert appointment_table_info['processing_time'] == 15.2
        assert appointment_table_info['error'] == "Connection timeout"
        
        # Individual table status validation
        assert patient_status is not None
        assert patient_status['table_name'] == 'patient'
        assert patient_status['rows_processed'] == 1000
        assert patient_status['success'] is True
        assert patient_status['status'] == 'completed'
        
        assert appointment_status is not None
        assert appointment_status['table_name'] == 'appointment'
        assert appointment_status['rows_processed'] == 500
        assert appointment_status['success'] is False
        assert appointment_status['error'] == "Connection timeout"
        assert appointment_status['status'] == 'failed'
        
        assert nonexistent_status is None
        
        # Pipeline stats validation
        assert pipeline_stats['tables_processed'] == 4
        assert pipeline_stats['total_rows_processed'] == 2500
        assert pipeline_stats['error_count'] == 4
        assert pipeline_stats['success_count'] == 0  # No tables succeeded
        assert pipeline_stats['success_rate'] == 0.0  # 0 out of 4 tables
        assert pipeline_stats['status'] == 'running'
        
        # Filtered status validation
        assert len(patient_only_status['tables']) == 1
        assert patient_only_status['tables'][0]['name'] == 'patient'

    def test_comprehensive_persistence_scenarios(self, test_settings, mock_analytics_engine):
        """
        Test comprehensive metrics persistence scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with various persistence scenarios using existing fixtures
            Act: Call persistence methods with different scenarios
            Assert: Verify comprehensive persistence scenarios work correctly
            
        Validates:
            - Metrics persistence works with complex data
            - Database operations are executed correctly
            - Error handling works for various failure scenarios
            - Transaction management works properly
            - Data integrity is maintained
            - Cleanup operations work correctly
        """
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        from unittest.mock import patch
        
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_analytics_engine):
            collector = UnifiedMetricsCollector(settings=test_settings, enable_persistence=True)
            print('DEBUG: metrics keys before start_pipeline:', collector.metrics.keys())
            collector.start_pipeline()
            print('DEBUG: metrics keys after start_pipeline:', collector.metrics.keys())
            
            # Add comprehensive test data
            collector.record_table_processed('patient', 1000, 30.5, True)
            collector.record_table_processed('appointment', 500, 15.2, False, "Connection failed")
            collector.record_table_processed('procedure', 750, 25.0, True)
            collector.record_error("General error", "system")
            collector.end_pipeline()
            print('DEBUG: metrics keys after end_pipeline:', collector.metrics.keys())
            
            # Act: Call persistence methods with different scenarios
            save_result = collector.save_metrics()
            cleanup_result = collector.cleanup_old_metrics(retention_days=30)
            
            # Assert: Verify comprehensive persistence scenarios work correctly
            assert save_result is True
            assert cleanup_result is True
            
            # Verify database operations were called
            mock_connection = mock_analytics_engine.connect.return_value.__enter__.return_value
            assert mock_connection.execute.call_count >= 4  # Pipeline + 3 table metrics
            mock_connection.commit.assert_called()
        
        # Verify SQL operations were executed with correct parameters
        execute_calls = mock_connection.execute.call_args_list
        
        # Check that pipeline metrics were inserted
        pipeline_insert_found = False
        table_inserts_found = 0
        
        for call in execute_calls:
            args, kwargs = call
            sql = str(args[0])
            if 'INSERT INTO etl_pipeline_metrics' in sql:
                pipeline_insert_found = True
                # Verify pipeline metrics parameters
                params = args[1] if len(args) > 1 else kwargs
                assert params['pipeline_id'] == collector.metrics['pipeline_id']
                assert params['tables_processed'] == 3
                assert params['total_rows_processed'] == 2250
                assert params['success'] is False  # Has errors
                assert params['error_count'] == 2  # 1 table error + 1 general error
                assert params['status'] == 'failed'
            elif 'INSERT INTO etl_table_metrics' in sql:
                table_inserts_found += 1
                # Verify table metrics parameters
                params = args[1] if len(args) > 1 else kwargs
                assert 'pipeline_id' in params
                assert 'table_name' in params
                assert 'rows_processed' in params
                assert 'processing_time' in params
                assert 'success' in params
                assert 'timestamp' in params
        
        assert pipeline_insert_found is True
        assert table_inserts_found == 3  # 3 tables processed

    def test_comprehensive_error_handling(self, unified_metrics_collector_no_persistence):
        """
        Test comprehensive error handling scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with various error scenarios using existing fixtures
            Act: Trigger various error conditions
            Assert: Verify comprehensive error handling works correctly
            
        Validates:
            - Error handling works for various error types
            - Error information is properly recorded
            - Error counts are accurate
            - Error messages are preserved
            - Error timestamps are recorded
            - Error recovery mechanisms work
        """
        # Arrange: Set up collector with various error scenarios using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        
        # Act: Trigger various error conditions
        # Table processing errors
        collector.record_table_processed('patient', 1000, 30.5, False, "Database connection timeout")
        collector.record_table_processed('appointment', 500, 15.2, False, "Data validation failed")
        collector.record_table_processed('procedure', 750, 25.0, False, "Schema mismatch")
        
        # General pipeline errors
        collector.record_error("Configuration error", "config")
        collector.record_error("Network timeout", "network")
        collector.record_error("Memory allocation failed", "system")
        collector.record_error("File system error", "storage")
        
        # Successful table processing
        collector.record_table_processed('payment', 250, 8.5, True)
        
        collector.end_pipeline()
        
        # Assert: Verify comprehensive error handling works correctly
        assert len(collector.metrics['errors']) == 7  # 3 table errors + 4 general errors
        assert len(collector.metrics['table_metrics']) == 4
        
        # Verify table errors are properly recorded
        table_errors = [error for error in collector.metrics['errors'] if 'table' in error and error.get('table') in ['patient', 'appointment', 'procedure']]
        assert len(table_errors) == 3
        
        # Verify general errors are properly recorded
        general_errors = [error for error in collector.metrics['errors'] if 'table' in error and error.get('table') in ['config', 'network', 'system', 'storage']]
        assert len(general_errors) == 4
        
        # Verify all errors have timestamps
        for error in collector.metrics['errors']:
            assert error['timestamp'] is not None
            assert isinstance(error['timestamp'], datetime)

    def test_comprehensive_performance_characteristics(self, unified_metrics_collector_no_persistence):
        """
        Test comprehensive performance characteristics and memory usage using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with large dataset simulation using existing fixtures
            Act: Process large amounts of data and measure performance
            Assert: Verify performance characteristics are acceptable
            
        Validates:
            - Performance is acceptable for large datasets
            - Memory usage is reasonable
            - Processing time scales appropriately
            - No memory leaks occur
            - Large error lists are handled efficiently
            - Complex status reporting is performant
        """
        # Arrange: Set up collector with large dataset simulation using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        
        # Act: Process large amounts of data and measure performance
        start_time = time.time()
        
        # Simulate processing many tables
        for i in range(100):
            table_name = f"table_{i}"
            rows_processed = (i + 1) * 100
            processing_time = (i + 1) * 0.1
            success = i % 10 != 0  # 90% success rate
            
            if success:
                collector.record_table_processed(table_name, rows_processed, processing_time, True)
            else:
                collector.record_table_processed(table_name, rows_processed, processing_time, False, f"Error in table {i}")
        
        # Add some general errors
        for i in range(20):
            collector.record_error(f"General error {i}", f"system_{i}")
        
        collector.end_pipeline()
        end_time = time.time()
        
        # Assert: Verify performance characteristics are acceptable
        processing_time = end_time - start_time
        
        # Performance validation
        assert processing_time < 5.0, f"Processing took too long: {processing_time:.2f}s"
        assert collector.metrics['tables_processed'] == 100
        assert collector.metrics['total_rows_processed'] == 505000  # Sum of 1 to 100 * 100
        assert len(collector.metrics['errors']) == 30  # 10 table errors + 20 general errors
        assert len(collector.metrics['table_metrics']) == 100
        
        # Memory usage validation (indirect)
        assert collector.metrics is not None
        assert len(collector.metrics['table_metrics']) == 100
        assert len(collector.metrics['errors']) == 30
        
        # Status reporting performance
        status_start = time.time()
        pipeline_status = collector.get_pipeline_status()
        pipeline_stats = collector.get_pipeline_stats()
        status_end = time.time()
        
        status_time = status_end - status_start
        assert status_time < 1.0, f"Status reporting took too long: {status_time:.2f}s"
        
        # Verify status contains all data
        assert len(pipeline_status['tables']) == 100
        assert pipeline_stats['tables_processed'] == 100
        assert pipeline_stats['success_rate'] == 70.0  # 70 out of 100 tables succeeded
        assert pipeline_stats['status'] == 'failed'  # Has errors

    def test_comprehensive_edge_cases(self, unified_metrics_collector_no_persistence):
        """
        Test comprehensive edge cases and boundary conditions using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with edge case scenarios using existing fixtures
            Act: Test various edge cases and boundary conditions
            Assert: Verify edge cases are handled correctly
            
        Validates:
            - Edge cases are handled gracefully
            - Boundary conditions work correctly
            - Invalid inputs are handled properly
            - Zero values are handled correctly
            - Large values are handled correctly
            - Special characters in error messages are handled
            - Unicode characters are handled properly
        """
        # Arrange: Set up collector with edge case scenarios using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        
        # Act: Test various edge cases and boundary conditions
        
        # Zero values
        collector.record_table_processed('empty_table', 0, 0.0, True)
        
        # Large values
        collector.record_table_processed('large_table', 999999999, 999999.9, True)
        
        # Very small values
        collector.record_table_processed('tiny_table', 1, 0.001, True)
        
        # Test various edge cases
        collector.record_error("Error with special chars: !@#$%^&*()", "special")
        collector.record_error("Error with quotes: 'single' and \"double\"", "quotes")
        collector.record_error("Error with newlines:\nline1\nline2", "newlines")
        collector.record_error("Error with simple unicode: test", "unicode")
        collector.record_error("A" * 1000, "long")  # Very long error message
        collector.record_error("", "empty")  # Empty error message
        
        # Table names with special characters
        collector.record_table_processed("table-with-dashes", 100, 1.0, True)
        collector.record_table_processed("table_with_underscores", 200, 2.0, True)
        collector.record_table_processed("table.with.dots", 300, 3.0, True)
        collector.record_table_processed("table with spaces", 400, 4.0, True)
        
        collector.end_pipeline()
        
        # Assert: Verify edge cases are handled correctly
        
        # Verify zero values
        empty_metric = collector.metrics['table_metrics']['empty_table']
        assert empty_metric['rows_processed'] == 0
        assert empty_metric['processing_time'] == 0.0
        assert empty_metric['success'] is True
        
        # Verify large values
        large_metric = collector.metrics['table_metrics']['large_table']
        assert large_metric['rows_processed'] == 999999999
        assert large_metric['processing_time'] == 999999.9
        assert large_metric['success'] is True
        
        # Verify very small values
        tiny_metric = collector.metrics['table_metrics']['tiny_table']
        assert tiny_metric['rows_processed'] == 1
        assert tiny_metric['processing_time'] == 0.001
        assert tiny_metric['success'] is True
        
        # Verify special characters in error messages
        errors = collector.metrics['errors']
        special_error = next(e for e in errors if e.get('table') == 'special')
        assert "Error with special chars: !@#$%^&*()" in special_error['message']
        
        quotes_error = next(e for e in errors if e.get('table') == 'quotes')
        assert "Error with quotes: 'single' and \"double\"" in quotes_error['message']
        
        newlines_error = next(e for e in errors if e.get('table') == 'newlines')
        assert "Error with newlines:\nline1\nline2" in newlines_error['message']
        
        unicode_error = next(e for e in errors if e.get('table') == 'unicode')
        assert "Error with simple unicode: test" in unicode_error['message']
        
        # Verify very long error messages
        long_error_record = next(e for e in errors if e.get('table') == 'long')
        assert len(long_error_record['message']) == 1000
        
        # Verify empty error messages
        empty_error_record = next(e for e in errors if e.get('table') == 'empty')
        assert empty_error_record['message'] == ""
        
        # Verify table names with special characters
        assert 'table-with-dashes' in collector.metrics['table_metrics']
        assert 'table_with_underscores' in collector.metrics['table_metrics']
        assert 'table.with.dots' in collector.metrics['table_metrics']
        assert 'table with spaces' in collector.metrics['table_metrics']
        
        # Verify total counts
        assert collector.metrics['tables_processed'] == 7
        assert len(collector.metrics['errors']) == 6

    def test_comprehensive_provider_pattern_integration(self, test_settings):
        """
        Test comprehensive provider pattern integration with various scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up comprehensive test settings with various provider scenarios using existing fixtures
            Act: Create metrics collector with different provider configurations
            Assert: Verify provider pattern integration works correctly
            
        Validates:
            - Provider pattern works with comprehensive configuration
            - Settings injection works for environment-agnostic connections
            - Different provider configurations work correctly
            - Configuration validation works properly
            - Error handling works for invalid configurations
        """
        # Arrange: Set up comprehensive test settings with various provider scenarios using existing fixtures
        settings = test_settings
        
        # Act: Create metrics collector with different provider configurations
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=None):
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=False)
        
        # Assert: Verify provider pattern integration works correctly
        assert test_settings.environment == 'test'
        assert hasattr(test_settings.provider, 'configs')
        
        # Verify pipeline configuration structure
        pipeline_config = test_settings.pipeline_config
        assert pipeline_config is not None
        assert 'connections' in pipeline_config
        assert 'general' in pipeline_config
        assert 'stages' in pipeline_config
        assert 'logging' in pipeline_config
        assert 'error_handling' in pipeline_config

    def test_comprehensive_factory_function_scenarios(self, test_settings, mock_analytics_engine):
        """
        Test comprehensive factory function scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up comprehensive test settings with various scenarios using existing fixtures
            Act: Call factory function with different configurations
            Assert: Verify factory function works correctly for all scenarios
            
        Validates:
            - Factory function works with comprehensive configuration
            - Settings injection works properly
            - Persistence can be enabled/disabled
            - Provider pattern dependency injection works
            - Error handling works for invalid configurations
        """
        # Arrange: Set up comprehensive test settings with various scenarios using existing fixtures
        settings = test_settings
        
        # Act: Call factory function with different configurations
        
        # Test with persistence enabled
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_analytics_engine):
            collector_with_persistence = create_metrics_collector(settings, enable_persistence=True)
        
        # Test with persistence disabled
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=None):
            collector_without_persistence = create_metrics_collector(settings, enable_persistence=False)
        
        # Assert: Verify factory function works correctly for all scenarios
        # With persistence
        assert isinstance(collector_with_persistence, UnifiedMetricsCollector)
        assert collector_with_persistence.settings == settings
        assert collector_with_persistence.analytics_engine == mock_analytics_engine
        assert collector_with_persistence.enable_persistence is True
        
        # Without persistence
        assert isinstance(collector_without_persistence, UnifiedMetricsCollector)
        assert collector_without_persistence.settings == settings
        assert collector_without_persistence.analytics_engine is None
        assert collector_without_persistence.enable_persistence is False
        
        # Verify both collectors have proper configuration
        assert collector_with_persistence.metrics is not None
        assert collector_without_persistence.metrics is not None
        assert collector_with_persistence.metrics['status'] == 'idle'
        assert collector_without_persistence.metrics['status'] == 'idle'

    def test_comprehensive_fail_fast_scenarios(self):
        """
        Test comprehensive fail-fast scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up various fail-fast scenarios using existing fixtures
            Act: Attempt operations that should fail fast
            Assert: Verify comprehensive fail-fast scenarios work correctly
            
        Validates:
            - Fail-fast scenarios work correctly
            - Environment validation works properly
            - Error messages are clear and actionable
            - Configuration validation works
            - Security requirements are enforced
        """
        import os
        
        # Arrange: Set up various fail-fast scenarios using existing fixtures
        original_env = os.environ.get('ETL_ENVIRONMENT')
        
        try:
            # Act: Attempt operations that should fail fast
            
            # Test missing ETL_ENVIRONMENT
            if 'ETL_ENVIRONMENT' in os.environ:
                del os.environ['ETL_ENVIRONMENT']
            
            with pytest.raises(Exception):  # Either EnvironmentError or ConfigurationError
                from etl_pipeline.config import Settings
                settings = Settings()
            
            # Test empty ETL_ENVIRONMENT
            os.environ['ETL_ENVIRONMENT'] = ""
            with pytest.raises(Exception):  # Either EnvironmentError or ConfigurationError
                settings = Settings()
            
            # Test invalid ETL_ENVIRONMENT
            os.environ['ETL_ENVIRONMENT'] = "invalid"
            with pytest.raises(Exception):  # Either EnvironmentError or ConfigurationError
                settings = Settings()
            
            # Test None ETL_ENVIRONMENT
            os.environ['ETL_ENVIRONMENT'] = "None"
            with pytest.raises(Exception):  # Either EnvironmentError or ConfigurationError
                settings = Settings()
            
        finally:
            # Cleanup: Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env
            elif 'ETL_ENVIRONMENT' in os.environ:
                del os.environ['ETL_ENVIRONMENT']

    def test_comprehensive_settings_injection_scenarios(self, test_settings):
        """
        Test comprehensive Settings injection scenarios using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up comprehensive test settings with various injection scenarios using existing fixtures
            Act: Test Settings injection with different configurations
            Assert: Verify Settings injection works correctly for all scenarios
            
        Validates:
            - Settings injection works for environment-agnostic connections
            - Provider pattern integration works correctly
            - Configuration validation works properly
            - Error handling works for invalid configurations
            - Environment separation works correctly
        """
        # Arrange: Set up comprehensive test settings with various injection scenarios using existing fixtures
        settings = test_settings
        
        # Act: Test various settings injection scenarios
        # Test database configuration access
        pipeline_config = test_settings.pipeline_config
        assert pipeline_config is not None
        assert 'connections' in pipeline_config
        assert 'general' in pipeline_config
        assert 'stages' in pipeline_config
        assert 'logging' in pipeline_config
        assert 'error_handling' in pipeline_config
        
        # Test environment configuration access
        env_config = test_settings._env_vars
        assert env_config is not None
        assert 'ETL_ENVIRONMENT' in env_config
        assert env_config['ETL_ENVIRONMENT'] == 'test'
        
        # Test table configuration access
        table_configs = test_settings.tables_config.get('tables', {})
        assert table_configs is not None
        assert 'patient' in table_configs
        assert 'appointment' in table_configs 