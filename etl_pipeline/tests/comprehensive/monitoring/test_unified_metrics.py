"""Comprehensive tests for unified metrics functionality."""

import pytest
import json
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector


@pytest.fixture
def mock_analytics_engine():
    """Mock analytics engine for testing."""
    engine = MagicMock(spec=Engine)
    engine.url = Mock()
    engine.url.database = 'opendental_analytics'
    return engine


@pytest.fixture
def mock_connection():
    """Mock database connection with context manager support."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=None)
    return mock_conn


@pytest.fixture
def metrics_collector_with_persistence(mock_analytics_engine):
    """Metrics collector with database persistence enabled."""
    with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
        collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
        yield collector


class TestUnifiedMetricsCollectorInitialization:
    """Test UnifiedMetricsCollector initialization and configuration."""

    def test_initialization_with_analytics_engine(self, mock_analytics_engine):
        """Test initialization with analytics engine."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine)
            
            assert collector.analytics_engine == mock_analytics_engine
            assert collector.enable_persistence is True
            assert collector.metrics['status'] == 'idle'
            assert collector.metrics['tables_processed'] == 0
            assert collector.metrics['total_rows_processed'] == 0
            assert 'pipeline_' in collector.metrics['pipeline_id']

    def test_initialization_without_analytics_engine(self):
        """Test initialization without analytics engine."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector()
            
            assert collector.analytics_engine is None
            assert collector.enable_persistence is False
            assert collector.metrics['status'] == 'idle'

    def test_initialization_disable_persistence(self, mock_analytics_engine):
        """Test initialization with persistence disabled."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=False)
            
            assert collector.analytics_engine == mock_analytics_engine
            assert collector.enable_persistence is False

    def test_initialization_with_persistence_enabled(self, mock_analytics_engine):
        """Test initialization with persistence enabled."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            with patch.object(UnifiedMetricsCollector, '_initialize_metrics_table') as mock_init:
                collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
                
                assert collector.enable_persistence is True
                mock_init.assert_called_once()

    def test_initialization_without_engine_disables_persistence(self):
        """Test that persistence is disabled when no engine provided."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(enable_persistence=True)
            
            assert collector.enable_persistence is False


class TestUnifiedMetricsCollectorCoreFunctionality:
    """Test core metrics collection functionality."""

    def test_reset_metrics(self, unified_metrics_collector_no_persistence):
        """Test metrics reset functionality."""
        collector = unified_metrics_collector_no_persistence
        
        # Add some data
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_error("Test error")
        
        # Reset metrics
        collector.reset_metrics()
        
        assert collector.metrics['tables_processed'] == 0
        assert collector.metrics['total_rows_processed'] == 0
        assert len(collector.metrics['errors']) == 0
        assert collector.metrics['status'] == 'idle'
        assert collector.start_time is None
        assert 'pipeline_' in collector.metrics['pipeline_id']

    def test_start_pipeline(self, unified_metrics_collector_no_persistence):
        """Test pipeline start functionality."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.return_value = 1234567890.0
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                collector.start_pipeline()
                
                assert collector.start_time == 1234567890.0
                assert collector.metrics['start_time'] == mock_now
                assert collector.metrics['status'] == 'running'

    def test_end_pipeline_success(self, unified_metrics_collector_no_persistence):
        """Test pipeline end with success."""
        collector = unified_metrics_collector_no_persistence
        
        # Start pipeline
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567920.0]  # 30 seconds later
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                collector.start_pipeline()
                result = collector.end_pipeline()
                
                assert result['status'] == 'completed'
                assert result['end_time'] == mock_now
                assert result['total_time'] == 30.0

    def test_end_pipeline_with_errors(self, unified_metrics_collector_no_persistence):
        """Test pipeline end with errors."""
        collector = unified_metrics_collector_no_persistence
        
        # Start pipeline and add errors
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567920.0]
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                collector.start_pipeline()
                collector.record_error("Test error")
                result = collector.end_pipeline()
                
                assert result['status'] == 'failed'
                assert result['total_time'] == 30.0

    def test_end_pipeline_without_start(self, unified_metrics_collector_no_persistence):
        """Test pipeline end without starting."""
        collector = unified_metrics_collector_no_persistence
        
        result = collector.end_pipeline()
        
        assert result['status'] == 'idle'
        assert result['total_time'] is None


class TestTableProcessingMetrics:
    """Test table processing metrics recording."""

    def test_record_table_processed_success(self, unified_metrics_collector_no_persistence):
        """Test recording successful table processing."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            collector.record_table_processed('patient', 1000, 30.5, True)
            
            assert collector.metrics['tables_processed'] == 1
            assert collector.metrics['total_rows_processed'] == 1000
            assert 'patient' in collector.metrics['table_metrics']
            
            table_metric = collector.metrics['table_metrics']['patient']
            assert table_metric['table_name'] == 'patient'
            assert table_metric['rows_processed'] == 1000
            assert table_metric['processing_time'] == 30.5
            assert table_metric['success'] is True
            assert table_metric['error'] is None
            assert table_metric['status'] == 'completed'
            assert table_metric['timestamp'] == mock_now

    def test_record_table_processed_failure(self, unified_metrics_collector_no_persistence):
        """Test recording failed table processing."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
            
            assert collector.metrics['tables_processed'] == 1
            assert collector.metrics['total_rows_processed'] == 500
            assert len(collector.metrics['errors']) == 1
            
            table_metric = collector.metrics['table_metrics']['appointment']
            assert table_metric['success'] is False
            assert table_metric['error'] == "Connection timeout"
            assert table_metric['status'] == 'failed'
            
            error_record = collector.metrics['errors'][0]
            assert error_record['table'] == 'appointment'
            assert error_record['error'] == "Connection timeout"
            assert error_record['timestamp'] == mock_now

    def test_record_table_processed_failure_no_error(self, unified_metrics_collector_no_persistence):
        """Test recording failed table processing without error message."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_table_processed('procedure', 750, 25.0, False)
        
        assert len(collector.metrics['errors']) == 0  # No error message provided
        assert collector.metrics['table_metrics']['procedure']['success'] is False

    def test_record_table_processed_multiple_tables(self, unified_metrics_collector_no_persistence):
        """Test recording multiple table processing events."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, True)
        collector.record_table_processed('procedure', 750, 25.0, False, "Error")
        
        assert collector.metrics['tables_processed'] == 3
        assert collector.metrics['total_rows_processed'] == 2250
        assert len(collector.metrics['errors']) == 1
        assert len(collector.metrics['table_metrics']) == 3


class TestErrorRecording:
    """Test error recording functionality."""

    def test_record_error_general(self, unified_metrics_collector_no_persistence):
        """Test recording general pipeline error."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            collector.record_error("Database connection failed")
            
            assert len(collector.metrics['errors']) == 1
            error_record = collector.metrics['errors'][0]
            assert error_record['message'] == "Database connection failed"
            assert error_record['table'] is None
            assert error_record['timestamp'] == mock_now

    def test_record_error_with_table(self, unified_metrics_collector_no_persistence):
        """Test recording error with table association."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            collector.record_error("Table not found", "nonexistent_table")
            
            assert len(collector.metrics['errors']) == 1
            error_record = collector.metrics['errors'][0]
            assert error_record['message'] == "Table not found"
            assert error_record['table'] == "nonexistent_table"
            assert error_record['timestamp'] == mock_now

    def test_record_error_multiple_errors(self, unified_metrics_collector_no_persistence):
        """Test recording multiple errors."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_error("First error")
        collector.record_error("Second error", "table1")
        collector.record_error("Third error", "table2")
        
        assert len(collector.metrics['errors']) == 3
        assert collector.metrics['errors'][0]['message'] == "First error"
        assert collector.metrics['errors'][1]['table'] == "table1"
        assert collector.metrics['errors'][2]['table'] == "table2"


class TestPipelineStatusReporting:
    """Test pipeline status reporting functionality."""

    def test_get_pipeline_status_initial(self, unified_metrics_collector_no_persistence):
        """Test initial pipeline status."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            status = collector.get_pipeline_status()
            
            assert status['status'] == 'idle'
            assert status['pipeline_id'] == collector.metrics['pipeline_id']
            assert status['tables_processed'] == 0
            assert status['total_rows_processed'] == 0
            assert status['error_count'] == 0
            assert status['tables'] == []
            assert status['last_update'] == mock_now.isoformat()

    def test_get_pipeline_status_with_data(self, unified_metrics_collector_no_persistence):
        """Test pipeline status with processed data."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            # Add some data
            collector.record_table_processed('patient', 1000, 30.5, True)
            collector.record_table_processed('appointment', 500, 15.2, False, "Error")
            
            status = collector.get_pipeline_status()
            
            assert status['tables_processed'] == 2
            assert status['total_rows_processed'] == 1500
            assert status['error_count'] == 1
            assert len(status['tables']) == 2
            
            # Check table details
            patient_table = next(t for t in status['tables'] if t['name'] == 'patient')
            assert patient_table['status'] == 'completed'
            assert patient_table['records_processed'] == 1000
            assert patient_table['processing_time'] == 30.5
            assert patient_table['error'] is None
            
            appointment_table = next(t for t in status['tables'] if t['name'] == 'appointment')
            assert appointment_table['status'] == 'failed'
            assert appointment_table['error'] == "Error"

    def test_get_pipeline_status_filtered_by_table(self, unified_metrics_collector_no_persistence):
        """Test pipeline status filtered by specific table."""
        collector = unified_metrics_collector_no_persistence
        
        # Add data for multiple tables
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, True)
        collector.record_table_processed('procedure', 750, 25.0, True)
        
        # Filter by specific table
        status = collector.get_pipeline_status(table='patient')
        
        assert len(status['tables']) == 1
        assert status['tables'][0]['name'] == 'patient'
        assert status['tables_processed'] == 3  # Total still shows all
        assert status['total_rows_processed'] == 2250  # Total still shows all

    def test_get_pipeline_status_nonexistent_table(self, unified_metrics_collector_no_persistence):
        """Test pipeline status for non-existent table."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_table_processed('patient', 1000, 30.5, True)
        
        status = collector.get_pipeline_status(table='nonexistent')
        
        assert status['tables'] == []
        assert status['tables_processed'] == 1  # Total still shows all


class TestTableStatusReporting:
    """Test table-specific status reporting."""

    def test_get_table_status_existing(self, unified_metrics_collector_no_persistence):
        """Test getting status for existing table."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            collector.record_table_processed('patient', 1000, 30.5, True)
            
            table_status = collector.get_table_status('patient')
            
            assert table_status is not None
            assert table_status['table_name'] == 'patient'
            assert table_status['rows_processed'] == 1000
            assert table_status['processing_time'] == 30.5
            assert table_status['success'] is True
            assert table_status['error'] is None
            assert table_status['status'] == 'completed'
            assert table_status['timestamp'] == mock_now

    def test_get_table_status_nonexistent(self, unified_metrics_collector_no_persistence):
        """Test getting status for non-existent table."""
        collector = unified_metrics_collector_no_persistence
        
        table_status = collector.get_table_status('nonexistent')
        
        assert table_status is None

    def test_get_table_status_failed_table(self, unified_metrics_collector_no_persistence):
        """Test getting status for failed table."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
        
        table_status = collector.get_table_status('appointment')
        
        assert table_status['success'] is False
        assert table_status['error'] == "Connection timeout"
        assert table_status['status'] == 'failed'


class TestPipelineStatistics:
    """Test pipeline statistics calculation."""

    def test_get_pipeline_stats_empty(self, unified_metrics_collector_no_persistence):
        """Test pipeline stats with no data."""
        collector = unified_metrics_collector_no_persistence
        
        stats = collector.get_pipeline_stats()
        
        assert stats['tables_processed'] == 0
        assert stats['total_rows_processed'] == 0
        assert stats['error_count'] == 0
        assert stats['success_count'] == 0
        assert stats['success_rate'] == 0.0
        assert stats['total_time'] is None
        assert stats['status'] == 'idle'

    def test_get_pipeline_stats_mixed_success_failure(self, unified_metrics_collector_no_persistence):
        """Test pipeline stats with mixed success and failure."""
        collector = unified_metrics_collector_no_persistence
        
        # Add mixed data
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, False, "Error")
        collector.record_table_processed('procedure', 750, 25.0, True)
        
        stats = collector.get_pipeline_stats()
        
        assert stats['tables_processed'] == 3
        assert stats['total_rows_processed'] == 2250
        assert stats['error_count'] == 1
        assert stats['success_count'] == 2
        assert stats['success_rate'] == (2/3) * 100
        assert stats['status'] == 'idle'

    def test_get_pipeline_stats_all_success(self, unified_metrics_collector_no_persistence):
        """Test pipeline stats with all successful processing."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, True)
        
        stats = collector.get_pipeline_stats()
        
        assert stats['success_rate'] == 100.0
        assert stats['error_count'] == 0
        assert stats['success_count'] == 2

    def test_get_pipeline_stats_all_failure(self, unified_metrics_collector_no_persistence):
        """Test pipeline stats with all failed processing."""
        collector = unified_metrics_collector_no_persistence
        
        collector.record_table_processed('patient', 1000, 30.5, False, "Error 1")
        collector.record_table_processed('appointment', 500, 15.2, False, "Error 2")
        
        stats = collector.get_pipeline_stats()
        
        assert stats['success_rate'] == 0.0
        assert stats['error_count'] == 2
        assert stats['success_count'] == 0

    def test_get_pipeline_stats_with_pipeline_time(self, unified_metrics_collector_no_persistence):
        """Test pipeline stats with pipeline timing."""
        collector = unified_metrics_collector_no_persistence
        
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567920.0]
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                collector.start_pipeline()
                collector.record_table_processed('patient', 1000, 30.5, True)
                collector.end_pipeline()
                
                stats = collector.get_pipeline_stats()
                
                assert stats['total_time'] == 30.0
                assert stats['status'] == 'completed'


class TestDatabasePersistence:
    """Test database persistence functionality."""

    def test_save_metrics_success(self, metrics_collector_with_persistence, mock_unified_metrics_connection):
        """Test successful metrics saving."""
        collector = metrics_collector_with_persistence
        
        # Mock the engine connect method
        collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
        
        # Add some data
        collector.record_table_processed('patient', 1000, 30.5, True)
        collector.record_table_processed('appointment', 500, 15.2, False, "Error")
        
        # Mock the execute method to return a mock result
        mock_result = MagicMock()
        mock_unified_metrics_connection.execute.return_value = mock_result
        
        result = collector.save_metrics()
        
        assert result is True
        assert mock_unified_metrics_connection.execute.call_count == 3  # 1 pipeline + 2 table metrics
        mock_unified_metrics_connection.commit.assert_called_once()

    def test_save_metrics_no_persistence(self, unified_metrics_collector_no_persistence):
        """Test metrics saving when persistence is disabled."""
        collector = unified_metrics_collector_no_persistence
        
        result = collector.save_metrics()
        
        assert result is False

    def test_save_metrics_database_error(self, metrics_collector_with_persistence, mock_unified_metrics_connection):
        """Test metrics saving with database error."""
        collector = metrics_collector_with_persistence
        
        # Mock the engine connect method
        collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
        
        # Mock execute to raise an exception
        mock_unified_metrics_connection.execute.side_effect = SQLAlchemyError("Database error")
        
        result = collector.save_metrics()
        
        assert result is False

    def test_cleanup_old_metrics_success(self, metrics_collector_with_persistence, mock_unified_metrics_connection):
        """Test successful cleanup of old metrics."""
        collector = metrics_collector_with_persistence
        
        # Mock the engine connect method
        collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
        
        # Mock the execute method
        mock_result = MagicMock()
        mock_unified_metrics_connection.execute.return_value = mock_result
        
        result = collector.cleanup_old_metrics(retention_days=30)
        
        assert result is True
        assert mock_unified_metrics_connection.execute.call_count == 2  # 2 DELETE statements
        mock_unified_metrics_connection.commit.assert_called_once()

    def test_cleanup_old_metrics_no_persistence(self, unified_metrics_collector_no_persistence):
        """Test cleanup when persistence is disabled."""
        collector = unified_metrics_collector_no_persistence
        
        result = collector.cleanup_old_metrics(retention_days=30)
        
        assert result is False

    def test_cleanup_old_metrics_database_error(self, metrics_collector_with_persistence, mock_unified_metrics_connection):
        """Test cleanup with database error."""
        collector = metrics_collector_with_persistence
        
        # Mock the engine connect method
        collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
        
        # Mock execute to raise an exception
        mock_unified_metrics_connection.execute.side_effect = SQLAlchemyError("Database error")
        
        result = collector.cleanup_old_metrics(retention_days=30)
        
        assert result is False

    def test_initialize_metrics_table_success(self, mock_analytics_engine, mock_unified_metrics_connection):
        """Test successful metrics table initialization."""
        # Mock the engine connect method
        mock_analytics_engine.connect.return_value = mock_unified_metrics_connection
        
        # Mock the execute method
        mock_result = MagicMock()
        mock_unified_metrics_connection.execute.return_value = mock_result
        
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
            
            # Verify table creation calls
            assert mock_unified_metrics_connection.execute.call_count == 6  # 2 CREATE TABLE + 4 CREATE INDEX
            mock_unified_metrics_connection.commit.assert_called_once()
            assert collector.enable_persistence is True

    def test_initialize_metrics_table_failure(self, mock_analytics_engine, mock_unified_metrics_connection):
        """Test metrics table initialization failure."""
        # Mock the engine connect method
        mock_analytics_engine.connect.return_value = mock_unified_metrics_connection
        
        # Mock execute to raise an exception
        mock_unified_metrics_connection.execute.side_effect = SQLAlchemyError("Table creation failed")
        
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
            
            # Verify persistence is disabled on failure
            assert collector.enable_persistence is False


class TestBackwardCompatibility:
    """Test backward compatibility aliases."""

    def test_metrics_collector_alias(self):
        """Test MetricsCollector alias."""
        from etl_pipeline.monitoring.unified_metrics import MetricsCollector
        
        collector = MetricsCollector()
        assert isinstance(collector, UnifiedMetricsCollector)

    def test_pipeline_metrics_alias(self):
        """Test PipelineMetrics alias."""
        from etl_pipeline.monitoring.unified_metrics import PipelineMetrics
        
        collector = PipelineMetrics()
        assert isinstance(collector, UnifiedMetricsCollector)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_zero_division_in_success_rate(self, unified_metrics_collector_no_persistence):
        """Test success rate calculation with zero tables."""
        collector = unified_metrics_collector_no_persistence
        
        stats = collector.get_pipeline_stats()
        
        # Should not raise ZeroDivisionError
        assert stats['success_rate'] == 0.0

    def test_pipeline_id_uniqueness(self, unified_metrics_collector_no_persistence):
        """Test that pipeline IDs are unique."""
        # Mock time.time to return different values for each collector
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890, 1234567891]  # Different timestamps
            
            collector1 = UnifiedMetricsCollector()
            collector2 = UnifiedMetricsCollector()
            
            # Verify the pipeline IDs are different
            id1 = collector1.metrics['pipeline_id']
            id2 = collector2.metrics['pipeline_id']
            
            assert id1 != id2, f"Pipeline IDs should be unique: {id1} vs {id2}"
            
            # Also verify they follow the expected format
            assert id1.startswith('pipeline_')
            assert id2.startswith('pipeline_')
            
            # Extract timestamps and verify they're different
            timestamp1 = int(id1.split('_')[1])
            timestamp2 = int(id2.split('_')[1])
            assert timestamp1 != timestamp2, f"Timestamps should be different: {timestamp1} vs {timestamp2}"
            
            # Verify the mocked values were used
            assert timestamp1 == 1234567890
            assert timestamp2 == 1234567891

    def test_metrics_json_serialization(self, metrics_collector_with_persistence, mock_unified_metrics_connection):
        """Test that metrics can be serialized to JSON."""
        collector = metrics_collector_with_persistence
        
        # Add data that includes datetime objects
        collector.record_table_processed('patient', 1000, 30.5, True)
        
        # This should not raise an exception
        json_str = json.dumps(collector.metrics, default=str)
        assert isinstance(json_str, str)
        assert 'patient' in json_str

    def test_large_number_of_tables(self, unified_metrics_collector_no_persistence):
        """Test handling large number of tables."""
        collector = unified_metrics_collector_no_persistence
        
        # Process many tables
        for i in range(100):
            collector.record_table_processed(f'table_{i}', 100, 1.0, True)
        
        assert collector.metrics['tables_processed'] == 100
        assert collector.metrics['total_rows_processed'] == 10000
        assert len(collector.metrics['table_metrics']) == 100

    def test_large_number_of_errors(self, unified_metrics_collector_no_persistence):
        """Test handling large number of errors."""
        collector = unified_metrics_collector_no_persistence
        
        # Record many errors
        for i in range(50):
            collector.record_error(f"Error {i}")
        
        assert len(collector.metrics['errors']) == 50
        
        stats = collector.get_pipeline_stats()
        assert stats['error_count'] == 50 