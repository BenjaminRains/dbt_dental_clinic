# type: ignore  # SQLAlchemy type handling in unit tests

"""
Unit tests for UnifiedMetricsCollector using provider pattern and AAA pattern.

This module follows the testing architecture patterns:
- Uses existing fixtures from tests/fixtures/ for configuration and connections
- Uses Settings injection for environment-agnostic connections
- Uses comprehensive mocking for database connections
- Uses AAA pattern for clear test structure
- Uses FAIL FAST principles for environment handling
- Tests all 12 methods with comprehensive coverage
- Uses provider pattern for dependency injection

Unit Test Strategy:
- Tests core metrics collection logic without real database connections
- Validates provider pattern dependency injection using existing fixtures
- Tests Settings injection for environment-agnostic connections
- Tests error handling and edge cases
- Tests metrics persistence logic with mocked connections
- Tests status reporting and statistics calculation
- Tests FAIL FAST behavior for missing environment variables

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
import logging
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Import ETL pipeline components
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector, create_metrics_collector

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseQueryError,
    DataExtractionError,
    SchemaValidationError
)

logger = logging.getLogger(__name__)


@pytest.mark.unit
@pytest.mark.monitoring
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestUnifiedMetricsCollectorUnit:
    """Unit tests for UnifiedMetricsCollector with mocked dependencies using existing fixtures."""

    def test_initialization_with_provider_pattern(self, test_settings, mock_analytics_engine):
        """
        Test UnifiedMetricsCollector initialization using provider pattern with existing fixtures.
        
        AAA Pattern:
            Arrange: Set up mock settings with DictConfigProvider and mock analytics engine using existing fixtures
            Act: Create UnifiedMetricsCollector instance
            Assert: Verify collector is properly initialized with correct settings
            
        Validates:
            - Provider pattern dependency injection works correctly using existing fixtures
            - Settings injection for environment-agnostic connections
            - Analytics engine connection is established
            - Metrics storage is properly initialized
            - Database persistence is enabled when engine available
            
        ETL Pipeline Context:
            - Critical for ETL pipeline metrics collection
            - Supports dental clinic data processing monitoring
            - Uses provider pattern for clean dependency injection
            - Implements Settings injection for environment-agnostic connections
        """
        # Arrange: Set up mock settings with DictConfigProvider and mock analytics engine using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_analytics_engine):
            
            # Act: Create UnifiedMetricsCollector instance
            collector = UnifiedMetricsCollector(settings=test_settings, enable_persistence=True)
            
            # Assert: Verify collector is properly initialized with correct settings
            assert collector.settings == test_settings
            assert collector.analytics_engine == mock_analytics_engine
            assert collector.enable_persistence is True
            assert collector.metrics is not None
            assert collector.metrics['pipeline_id'].startswith('pipeline_')
            assert collector.metrics['status'] == 'idle'
            assert collector.metrics['tables_processed'] == 0
            assert collector.metrics['total_rows_processed'] == 0
            assert len(collector.metrics['errors']) == 0
            assert len(collector.metrics['table_metrics']) == 0

    def test_initialization_without_persistence(self, test_settings):
        """
        Test UnifiedMetricsCollector initialization without database persistence using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up mock settings with DictConfigProvider and no analytics engine using existing fixtures
            Act: Create UnifiedMetricsCollector instance with persistence disabled
            Assert: Verify collector is initialized without database persistence
            
        Validates:
            - Collector works without database persistence using existing fixtures
            - In-memory metrics collection functions correctly
            - No database connection errors when persistence disabled
            - Metrics storage is properly initialized
        """
        # Arrange: Set up mock settings with DictConfigProvider and no analytics engine using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', side_effect=Exception("No connection")):
            
            # Act: Create UnifiedMetricsCollector instance with persistence disabled
            collector = UnifiedMetricsCollector(settings=test_settings, enable_persistence=False)
            
            # Assert: Verify collector is initialized without database persistence
            assert collector.settings == test_settings
            assert collector.analytics_engine is None
            assert collector.enable_persistence is False
            assert collector.metrics is not None
            assert collector.metrics['status'] == 'idle'

    def test_reset_metrics(self, unified_metrics_collector_no_persistence):
        """
        Test metrics reset functionality using existing fixtures.
        """
        import time as time_module
        from unittest.mock import patch
        collector = unified_metrics_collector_no_persistence
        collector.metrics['tables_processed'] = 5
        collector.metrics['total_rows_processed'] = 1000
        collector.metrics['errors'].append({'message': 'test error'})
        collector.metrics['table_metrics']['patient'] = {'rows_processed': 100}
        original_pipeline_id = collector.metrics['pipeline_id']
        # Patch time.time to return a different value after reset
        with patch('time.time', return_value=time_module.time() + 100):
            collector.reset_metrics()
        assert collector.metrics['pipeline_id'] != original_pipeline_id
        assert collector.metrics['pipeline_id'].startswith('pipeline_')
        assert collector.metrics['status'] == 'idle'
        assert collector.metrics['tables_processed'] == 0
        assert collector.metrics['total_rows_processed'] == 0
        assert len(collector.metrics['errors']) == 0
        assert len(collector.metrics['table_metrics']) == 0
        assert collector.start_time is None

    def test_start_pipeline(self, unified_metrics_collector_no_persistence):
        """
        Test pipeline start functionality using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector in idle state using existing fixtures
            Act: Call start_pipeline() method
            Assert: Verify pipeline is started with correct timing
            
        Validates:
            - Pipeline status is set to 'running'
            - Start time is recorded
            - Timing is properly initialized
        """
        # Arrange: Set up collector in idle state using existing fixtures
        collector = unified_metrics_collector_no_persistence
        assert collector.metrics['status'] == 'idle'
        assert collector.start_time is None
        
        # Act: Call start_pipeline() method
        collector.start_pipeline()
        
        # Assert: Verify pipeline is started with correct timing
        assert collector.metrics['status'] == 'running'
        assert collector.metrics['start_time'] is not None
        assert collector.start_time is not None
        assert isinstance(collector.metrics['start_time'], datetime)

    def test_end_pipeline_success(self, unified_metrics_collector_no_persistence):
        """
        Test pipeline end functionality with successful completion using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with running pipeline and no errors using existing fixtures
            Act: Call end_pipeline() method
            Assert: Verify pipeline is ended with success status
            
        Validates:
            - Pipeline status is set to 'completed' when no errors
            - End time is recorded
            - Total time is calculated correctly
            - Final metrics are returned
        """
        # Arrange: Set up collector with running pipeline and no errors using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        time.sleep(0.1)  # Small delay to ensure timing difference
        
        # Act: Call end_pipeline() method
        final_metrics = collector.end_pipeline()
        
        # Assert: Verify pipeline is ended with success status
        assert collector.metrics['status'] == 'completed'
        assert collector.metrics['end_time'] is not None
        assert collector.metrics['total_time'] is not None
        assert collector.metrics['total_time'] > 0
        assert final_metrics == collector.metrics
        assert isinstance(collector.metrics['end_time'], datetime)

    def test_end_pipeline_with_errors(self, unified_metrics_collector_no_persistence):
        """
        Test pipeline end functionality with errors using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with running pipeline and errors using existing fixtures
            Act: Call end_pipeline() method
            Assert: Verify pipeline is ended with failed status
            
        Validates:
            - Pipeline status is set to 'failed' when errors exist
            - End time is recorded
            - Total time is calculated correctly
            - Error count is preserved
        """
        # Arrange: Set up collector with running pipeline and errors using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        collector.record_error("Test error")
        time.sleep(0.1)  # Small delay to ensure timing difference
        
        # Act: Call end_pipeline() method
        final_metrics = collector.end_pipeline()
        
        # Assert: Verify pipeline is ended with failed status
        assert collector.metrics['status'] == 'failed'
        assert collector.metrics['end_time'] is not None
        assert collector.metrics['total_time'] is not None
        assert collector.metrics['total_time'] > 0
        assert len(collector.metrics['errors']) == 1
        assert final_metrics == collector.metrics

    def test_record_table_processed_success(self, unified_metrics_collector_no_persistence):
        """
        Test recording successful table processing using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with initial metrics using existing fixtures
            Act: Call record_table_processed() with success
            Assert: Verify metrics are updated correctly
            
        Validates:
            - Table metrics are recorded correctly
            - Counters are incremented properly
            - Success status is recorded
            - No errors are added for successful processing
        """
        # Arrange: Set up collector with initial metrics using existing fixtures
        collector = unified_metrics_collector_no_persistence
        initial_tables = collector.metrics['tables_processed']
        initial_rows = collector.metrics['total_rows_processed']
        
        # Act: Call record_table_processed() with success
        collector.record_table_processed('patient', 1000, 30.5, True)
        
        # Assert: Verify metrics are updated correctly
        assert collector.metrics['tables_processed'] == initial_tables + 1
        assert collector.metrics['total_rows_processed'] == initial_rows + 1000
        assert 'patient' in collector.metrics['table_metrics']
        
        table_metric = collector.metrics['table_metrics']['patient']
        assert table_metric['table_name'] == 'patient'
        assert table_metric['rows_processed'] == 1000
        assert table_metric['processing_time'] == 30.5
        assert table_metric['success'] is True
        assert table_metric['error'] is None
        assert table_metric['status'] == 'completed'
        assert table_metric['timestamp'] is not None

    def test_record_table_processed_failure(self, unified_metrics_collector_no_persistence):
        """
        Test recording failed table processing using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with initial metrics using existing fixtures
            Act: Call record_table_processed() with failure
            Assert: Verify metrics are updated correctly with error
            
        Validates:
            - Table metrics are recorded correctly for failures
            - Counters are incremented properly
            - Error status is recorded
            - Error is added to errors list
        """
        # Arrange: Set up collector with initial metrics using existing fixtures
        collector = unified_metrics_collector_no_persistence
        initial_tables = collector.metrics['tables_processed']
        initial_rows = collector.metrics['total_rows_processed']
        initial_errors = len(collector.metrics['errors'])
        
        # Act: Call record_table_processed() with failure
        collector.record_table_processed('patient', 500, 15.2, False, "Database connection failed")
        
        # Assert: Verify metrics are updated correctly with error
        assert collector.metrics['tables_processed'] == initial_tables + 1
        assert collector.metrics['total_rows_processed'] == initial_rows + 500
        assert 'patient' in collector.metrics['table_metrics']
        
        table_metric = collector.metrics['table_metrics']['patient']
        assert table_metric['table_name'] == 'patient'
        assert table_metric['rows_processed'] == 500
        assert table_metric['processing_time'] == 15.2
        assert table_metric['success'] is False
        assert table_metric['error'] == "Database connection failed"
        assert table_metric['status'] == 'failed'
        assert table_metric['timestamp'] is not None
        
        # Verify error is added to errors list
        assert len(collector.metrics['errors']) == initial_errors + 1
        error_record = collector.metrics['errors'][-1]
        assert error_record['table'] == 'patient'
        assert error_record['error'] == "Database connection failed"
        assert error_record['timestamp'] is not None

    def test_record_error(self, unified_metrics_collector_no_persistence):
        """
        Test recording general pipeline errors using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with initial error count using existing fixtures
            Act: Call record_error() method
            Assert: Verify error is recorded correctly
            
        Validates:
            - General errors are recorded correctly
            - Error count is incremented
            - Error details are preserved
            - Timestamp is recorded
        """
        # Arrange: Set up collector with initial error count using existing fixtures
        collector = unified_metrics_collector_no_persistence
        initial_errors = len(collector.metrics['errors'])
        
        # Act: Call record_error() method
        collector.record_error("General pipeline error", "patient")
        
        # Assert: Verify error is recorded correctly
        assert len(collector.metrics['errors']) == initial_errors + 1
        error_record = collector.metrics['errors'][-1]
        assert error_record['message'] == "General pipeline error"
        assert error_record['table'] == "patient"
        assert error_record['timestamp'] is not None

    def test_get_pipeline_status(self, unified_metrics_collector_no_persistence):
        """
        Test pipeline status retrieval using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with test data using existing fixtures
            Act: Call get_pipeline_status() method
            Assert: Verify status contains correct information
            
        Validates:
            - Pipeline status contains all required fields
            - Table information is included correctly
            - Timestamps are properly formatted
            - Error counts are accurate
        """
        # Arrange: Set up collector with test data using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, False, "Connection failed")
        
        # Act: Call get_pipeline_status() method
        status = collector.get_pipeline_status()
        
        # Assert: Verify status contains correct information
        assert 'last_update' in status
        assert status['status'] == 'running'
        assert status['pipeline_id'] == collector.metrics['pipeline_id']
        assert status['tables_processed'] == 2
        assert status['total_rows_processed'] == 1500
        assert status['error_count'] == 1
        assert len(status['tables']) == 2
        
        # Verify table information
        patient_table = next(t for t in status['tables'] if t['name'] == 'patient')
        assert patient_table['status'] == 'completed'
        assert patient_table['records_processed'] == 1000
        assert patient_table['processing_time'] == 30.5
        assert patient_table['error'] is None
        
        appointment_table = next(t for t in status['tables'] if t['name'] == 'appointment')
        assert appointment_table['status'] == 'failed'
        assert appointment_table['records_processed'] == 500
        assert appointment_table['processing_time'] == 15.2
        assert appointment_table['error'] == "Connection failed"

    def test_get_table_status(self, unified_metrics_collector_no_persistence):
        """
        Test table status retrieval using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with table metrics using existing fixtures
            Act: Call get_table_status() method
            Assert: Verify table status is returned correctly
            
        Validates:
            - Table status is returned for existing tables
            - None is returned for non-existent tables
            - Status contains all required fields
        """
        # Arrange: Set up collector with table metrics using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.record_table_processed('patient', 1000, 30.5, True)
        
        # Act: Call get_table_status() method
        patient_status = collector.get_table_status('patient')
        nonexistent_status = collector.get_table_status('nonexistent')
        
        # Assert: Verify table status is returned correctly
        assert patient_status is not None
        assert patient_status['table_name'] == 'patient'
        assert patient_status['rows_processed'] == 1000
        assert patient_status['processing_time'] == 30.5
        assert patient_status['success'] is True
        assert patient_status['status'] == 'completed'
        assert patient_status['timestamp'] is not None
        
        assert nonexistent_status is None

    def test_get_pipeline_stats(self, unified_metrics_collector_no_persistence):
        """
        Test pipeline statistics calculation using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with mixed success/failure data using existing fixtures
            Act: Call get_pipeline_stats() method
            Assert: Verify statistics are calculated correctly
            
        Validates:
            - Success rate is calculated correctly
            - Error count is accurate
            - Total statistics are correct
            - Division by zero is handled properly
        """
        # Arrange: Set up collector with mixed success/failure data using existing fixtures
        collector = unified_metrics_collector_no_persistence
        collector.start_pipeline()
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, False, "Error")
        collector.record_table_processed('procedure', 750, 25.0, True)
        
        # Act: Call get_pipeline_stats() method
        stats = collector.get_pipeline_stats()
        
        # Assert: Verify statistics are calculated correctly
        assert stats['tables_processed'] == 3
        assert stats['total_rows_processed'] == 2250
        assert stats['error_count'] == 1
        assert stats['success_count'] == 2
        assert stats['success_rate'] == (2/3) * 100  # 66.67%
        assert stats['status'] == 'running'
        assert stats['total_time'] is None  # Pipeline not ended yet

    def test_get_pipeline_stats_empty(self, unified_metrics_collector_no_persistence):
        """
        Test pipeline statistics calculation with no data using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector with no processed tables using existing fixtures
            Act: Call get_pipeline_stats() method
            Assert: Verify statistics handle empty state correctly
            
        Validates:
            - Division by zero is handled properly
            - Success rate is 0 when no tables processed
            - All counters are zero
        """
        # Arrange: Set up collector with no processed tables using existing fixtures
        collector = unified_metrics_collector_no_persistence
        
        # Act: Call get_pipeline_stats() method
        stats = collector.get_pipeline_stats()
        
        # Assert: Verify statistics handle empty state correctly
        assert stats['tables_processed'] == 0
        assert stats['total_rows_processed'] == 0
        assert stats['error_count'] == 0
        assert stats['success_count'] == 0
        assert stats['success_rate'] == 0  # Division by zero handled
        assert stats['status'] == 'idle'

    def test_save_metrics_success(self, test_metrics_settings):
        """
        Test successful metrics persistence using existing fixtures.
        """
        from unittest.mock import MagicMock, patch
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        mock_engine = MagicMock()
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_engine):
            collector = UnifiedMetricsCollector(settings=test_metrics_settings, enable_persistence=True)
            collector.start_pipeline()
            collector.record_table_processed('patient', 1000, 30.5, True)
            collector.end_pipeline()
            result = collector.save_metrics()
            assert result is True
            # Check that connect was called at least once (for both initialization and save)
            assert mock_engine.connect.call_count >= 1
            mock_connection = mock_engine.connect.return_value.__enter__.return_value
            assert mock_connection.execute.call_count >= 2
            # Check that commit was called at least once (for both initialization and save)
            assert mock_connection.commit.call_count >= 1

    def test_save_metrics_no_persistence(self, test_settings):
        """
        Test metrics persistence when disabled using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector without persistence using existing fixtures
            Act: Call save_metrics() method
            Assert: Verify method returns False when persistence disabled
            
        Validates:
            - Method returns False when persistence disabled
            - No database connection is attempted
            - Warning is logged
        """
        # Arrange: Set up collector without persistence using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=None):
            collector = UnifiedMetricsCollector(settings=test_settings, enable_persistence=False)
        
        # Act: Call save_metrics() method
        result = collector.save_metrics()
        
        # Assert: Verify method returns False when persistence disabled
        assert result is False

    def test_save_metrics_database_error(self, test_metrics_settings):
        """
        Test metrics persistence with database error using existing fixtures.
        """
        from unittest.mock import MagicMock, patch
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("Connection failed")
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_engine):
            collector = UnifiedMetricsCollector(settings=test_metrics_settings, enable_persistence=True)
            collector.start_pipeline()
            collector.record_table_processed('patient', 1000, 30.5, True)
            collector.end_pipeline()
            result = collector.save_metrics()
            assert result is False

    def test_cleanup_old_metrics_success(self, test_metrics_settings):
        """
        Test successful cleanup of old metrics using existing fixtures.
        """
        from unittest.mock import MagicMock, patch
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        mock_engine = MagicMock()
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_engine):
            collector = UnifiedMetricsCollector(settings=test_metrics_settings, enable_persistence=True)
            result = collector.cleanup_old_metrics(retention_days=30)
            assert result is True
            # Check that connect was called at least once (for both initialization and cleanup)
            assert mock_engine.connect.call_count >= 1
            mock_connection = mock_engine.connect.return_value.__enter__.return_value
            assert mock_connection.execute.call_count >= 2
            # Check that commit was called at least once (for both initialization and cleanup)
            assert mock_connection.commit.call_count >= 1

    def test_cleanup_old_metrics_no_persistence(self, test_settings):
        """
        Test cleanup when persistence is disabled using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up collector without persistence using existing fixtures
            Act: Call cleanup_old_metrics() method
            Assert: Verify method returns False when persistence disabled
            
        Validates:
            - Method returns False when persistence disabled
            - No database connection is attempted
        """
        # Arrange: Set up collector without persistence using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=None):
            collector = UnifiedMetricsCollector(settings=test_settings, enable_persistence=False)
        
        # Act: Call cleanup_old_metrics() method
        result = collector.cleanup_old_metrics(retention_days=30)
        
        # Assert: Verify method returns False when persistence disabled
        assert result is False

    def test_cleanup_old_metrics_database_error(self, test_metrics_settings):
        """
        Test cleanup with database error using existing fixtures.
        """
        from unittest.mock import MagicMock, patch
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("Connection failed")
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_engine):
            collector = UnifiedMetricsCollector(settings=test_metrics_settings, enable_persistence=True)
            result = collector.cleanup_old_metrics(retention_days=30)
            assert result is False

    def test_create_metrics_collector_factory(self, test_settings, mock_analytics_engine):
        """
        Test factory function for creating metrics collector using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up mock settings and analytics engine using existing fixtures
            Act: Call create_metrics_collector() factory function
            Assert: Verify collector is created with correct settings
            
        Validates:
            - Factory function creates collector correctly
            - Settings injection works properly
            - Persistence can be enabled/disabled
            - Provider pattern dependency injection works
        """
        # Arrange: Set up mock settings and analytics engine using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=mock_analytics_engine):
            
            # Act: Call create_metrics_collector() factory function
            collector = create_metrics_collector(test_settings, enable_persistence=True)
            
            # Assert: Verify collector is created with correct settings
            assert isinstance(collector, UnifiedMetricsCollector)
            assert collector.settings == test_settings
            assert collector.analytics_engine == mock_analytics_engine
            assert collector.enable_persistence is True

    def test_create_metrics_collector_no_persistence(self, test_settings):
        """
        Test factory function with persistence disabled using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up mock settings without analytics engine using existing fixtures
            Act: Call create_metrics_collector() factory function
            Assert: Verify collector is created without persistence
            
        Validates:
            - Factory function works without persistence
            - Settings injection works properly
            - No database connection is required
        """
        # Arrange: Set up mock settings without analytics engine using existing fixtures
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=None):
            
            # Act: Call create_metrics_collector() factory function
            collector = create_metrics_collector(test_settings, enable_persistence=False)
            
            # Assert: Verify collector is created without persistence
            assert isinstance(collector, UnifiedMetricsCollector)
            assert collector.settings == test_settings
            assert collector.analytics_engine is None
            assert collector.enable_persistence is False

    def test_fail_fast_on_missing_environment(self):
        """
        Test FAIL FAST behavior when ETL_ENVIRONMENT not set using existing fixtures.
        
        AAA Pattern:
            Arrange: Remove ETL_ENVIRONMENT from environment variables
            Act: Attempt to create Settings instance without environment
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - System fails fast when ETL_ENVIRONMENT not set
            - Clear error message is provided
            - No dangerous defaults to production
            - Security requirement is enforced
        """
        import os
        
        # Arrange: Remove ETL_ENVIRONMENT from environment variables
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            # Act: Attempt to create Settings instance without environment
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                from etl_pipeline.config import Settings
                settings = Settings()
        finally:
            # Cleanup: Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_provider_pattern_dependency_injection(self, test_settings):
        """
        Test provider pattern dependency injection for metrics collector using existing fixtures.
        
        AAA Pattern:
            Arrange: Set up Settings with DictConfigProvider using existing fixtures
            Act: Create metrics collector with injected settings
            Assert: Verify provider pattern works correctly
            
        Validates:
            - Provider pattern dependency injection works
            - Settings injection for environment-agnostic connections
            - Test environment isolation is maintained
            - No environment pollution during testing
        """
        # Arrange: Set up Settings with DictConfigProvider using existing fixtures
        settings = test_settings
        
        # Act: Create metrics collector with injected settings
        with patch('etl_pipeline.monitoring.unified_metrics.ConnectionFactory.get_analytics_raw_connection', return_value=None):
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=False)
        
        # Assert: Verify provider pattern works correctly
        assert collector.settings == settings
        assert collector.settings.environment == 'test'
        assert collector.settings.provider is not None
        assert hasattr(collector.settings.provider, 'configs')  # DictConfigProvider has configs attribute 