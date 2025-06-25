"""Unit tests for unified metrics functionality."""

import pytest
import json
import time
from datetime import datetime
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector


@pytest.mark.unit
class TestUnifiedMetricsCollectorUnit:
    """Fast unit tests for UnifiedMetricsCollector with comprehensive mocking."""

    @pytest.fixture
    def metrics_collector(self):
        """Basic metrics collector without persistence for unit tests."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(enable_persistence=False)
            yield collector

    def test_initialization_basic(self, metrics_collector):
        """Test basic initialization without persistence."""
        assert metrics_collector.analytics_engine is None
        assert metrics_collector.enable_persistence is False
        assert metrics_collector.metrics['status'] == 'idle'
        assert metrics_collector.metrics['tables_processed'] == 0
        assert 'pipeline_' in metrics_collector.metrics['pipeline_id']

    def test_initialization_with_engine(self, mock_analytics_engine):
        """Test initialization with analytics engine."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            with patch.object(UnifiedMetricsCollector, '_initialize_metrics_table') as mock_init:
                collector = UnifiedMetricsCollector(mock_analytics_engine)
                
                assert collector.analytics_engine == mock_analytics_engine
                assert collector.enable_persistence is True
                mock_init.assert_called_once()

    def test_reset_metrics(self, metrics_collector):
        """Test metrics reset functionality."""
        # Add some data
        metrics_collector.record_table_processed('patient', 1000, 30.5, True)
        metrics_collector.record_error("Test error")
        
        # Reset metrics
        metrics_collector.reset_metrics()
        
        assert metrics_collector.metrics['tables_processed'] == 0
        assert metrics_collector.metrics['total_rows_processed'] == 0
        assert len(metrics_collector.metrics['errors']) == 0
        assert metrics_collector.metrics['status'] == 'idle'
        assert metrics_collector.start_time is None

    def test_start_pipeline(self, metrics_collector):
        """Test pipeline start functionality."""
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.return_value = 1234567890.0
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                metrics_collector.start_pipeline()
                
                assert metrics_collector.start_time == 1234567890.0
                assert metrics_collector.metrics['start_time'] == mock_now
                assert metrics_collector.metrics['status'] == 'running'

    def test_end_pipeline_success(self, metrics_collector):
        """Test pipeline end with success."""
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567920.0]  # 30 seconds later
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                metrics_collector.start_pipeline()
                result = metrics_collector.end_pipeline()
                
                assert result['status'] == 'completed'
                assert result['end_time'] == mock_now
                assert result['total_time'] == 30.0

    def test_end_pipeline_with_errors(self, metrics_collector):
        """Test pipeline end with errors."""
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567920.0]
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                metrics_collector.start_pipeline()
                metrics_collector.record_error("Test error")
                result = metrics_collector.end_pipeline()
                
                assert result['status'] == 'failed'
                assert result['total_time'] == 30.0

    def test_record_table_processed_success(self, metrics_collector):
        """Test recording successful table processing."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            metrics_collector.record_table_processed('patient', 1000, 30.5, True)
            
            assert metrics_collector.metrics['tables_processed'] == 1
            assert metrics_collector.metrics['total_rows_processed'] == 1000
            
            table_metric = metrics_collector.metrics['table_metrics']['patient']
            assert table_metric['table_name'] == 'patient'
            assert table_metric['rows_processed'] == 1000
            assert table_metric['processing_time'] == 30.5
            assert table_metric['success'] is True
            assert table_metric['error'] is None
            assert table_metric['status'] == 'completed'
            assert table_metric['timestamp'] == mock_now

    def test_record_table_processed_failure(self, metrics_collector):
        """Test recording failed table processing."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            metrics_collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
            
            assert metrics_collector.metrics['tables_processed'] == 1
            assert len(metrics_collector.metrics['errors']) == 1
            
            table_metric = metrics_collector.metrics['table_metrics']['appointment']
            assert table_metric['success'] is False
            assert table_metric['error'] == "Connection timeout"
            assert table_metric['status'] == 'failed'
            
            error_record = metrics_collector.metrics['errors'][0]
            assert error_record['table'] == 'appointment'
            assert error_record['error'] == "Connection timeout"
            assert error_record['timestamp'] == mock_now

    def test_record_error_general(self, metrics_collector):
        """Test recording general pipeline error."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            metrics_collector.record_error("Database connection failed")
            
            assert len(metrics_collector.metrics['errors']) == 1
            error_record = metrics_collector.metrics['errors'][0]
            assert error_record['message'] == "Database connection failed"
            assert error_record['table'] is None
            assert error_record['timestamp'] == mock_now

    def test_record_error_with_table(self, metrics_collector):
        """Test recording error with table association."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            metrics_collector.record_error("Table not found", "nonexistent_table")
            
            assert len(metrics_collector.metrics['errors']) == 1
            error_record = metrics_collector.metrics['errors'][0]
            assert error_record['message'] == "Table not found"
            assert error_record['table'] == "nonexistent_table"
            assert error_record['timestamp'] == mock_now

    def test_get_pipeline_status_initial(self, metrics_collector):
        """Test initial pipeline status."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            status = metrics_collector.get_pipeline_status()
            
            assert status['status'] == 'idle'
            assert status['pipeline_id'] == metrics_collector.metrics['pipeline_id']
            assert status['tables_processed'] == 0
            assert status['total_rows_processed'] == 0
            assert status['error_count'] == 0
            assert status['tables'] == []
            assert status['last_update'] == mock_now.isoformat()

    def test_get_pipeline_status_with_data(self, metrics_collector):
        """Test pipeline status with processed data."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            metrics_collector.record_table_processed('patient', 1000, 30.5, True)
            metrics_collector.record_table_processed('appointment', 500, 15.2, False, "Error")
            
            status = metrics_collector.get_pipeline_status()
            
            assert status['tables_processed'] == 2
            assert status['total_rows_processed'] == 1500
            assert status['error_count'] == 1
            assert len(status['tables']) == 2

    def test_get_pipeline_status_filtered(self, metrics_collector):
        """Test pipeline status filtered by table."""
        metrics_collector.record_table_processed('patient', 1000, 30.5, True)
        metrics_collector.record_table_processed('appointment', 500, 15.2, True)
        
        status = metrics_collector.get_pipeline_status(table='patient')
        
        assert len(status['tables']) == 1
        assert status['tables'][0]['name'] == 'patient'

    def test_get_table_status_existing(self, metrics_collector):
        """Test getting status for existing table."""
        with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
            mock_now = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            metrics_collector.record_table_processed('patient', 1000, 30.5, True)
            
            table_status = metrics_collector.get_table_status('patient')
            
            assert table_status is not None
            assert table_status['table_name'] == 'patient'
            assert table_status['rows_processed'] == 1000
            assert table_status['processing_time'] == 30.5
            assert table_status['success'] is True
            assert table_status['timestamp'] == mock_now

    def test_get_table_status_nonexistent(self, metrics_collector):
        """Test getting status for non-existent table."""
        table_status = metrics_collector.get_table_status('nonexistent')
        assert table_status is None

    def test_get_pipeline_stats_empty(self, metrics_collector):
        """Test pipeline stats with no data."""
        stats = metrics_collector.get_pipeline_stats()
        
        assert stats['tables_processed'] == 0
        assert stats['total_rows_processed'] == 0
        assert stats['error_count'] == 0
        assert stats['success_count'] == 0
        assert stats['success_rate'] == 0.0
        assert stats['total_time'] is None
        assert stats['status'] == 'idle'

    def test_get_pipeline_stats_mixed(self, metrics_collector):
        """Test pipeline stats with mixed success and failure."""
        metrics_collector.record_table_processed('patient', 1000, 30.5, True)
        metrics_collector.record_table_processed('appointment', 500, 15.2, False, "Error")
        metrics_collector.record_table_processed('procedure', 750, 25.0, True)
        
        stats = metrics_collector.get_pipeline_stats()
        
        assert stats['tables_processed'] == 3
        assert stats['total_rows_processed'] == 2250
        assert stats['error_count'] == 1
        assert stats['success_count'] == 2
        assert stats['success_rate'] == (2/3) * 100

    def test_get_pipeline_stats_all_success(self, metrics_collector):
        """Test pipeline stats with all successful processing."""
        metrics_collector.record_table_processed('patient', 1000, 30.5, True)
        metrics_collector.record_table_processed('appointment', 500, 15.2, True)
        
        stats = metrics_collector.get_pipeline_stats()
        
        assert stats['success_rate'] == 100.0
        assert stats['error_count'] == 0
        assert stats['success_count'] == 2

    def test_get_pipeline_stats_all_failure(self, metrics_collector):
        """Test pipeline stats with all failed processing."""
        metrics_collector.record_table_processed('patient', 1000, 30.5, False, "Error 1")
        metrics_collector.record_table_processed('appointment', 500, 15.2, False, "Error 2")
        
        stats = metrics_collector.get_pipeline_stats()
        
        assert stats['success_rate'] == 0.0
        assert stats['error_count'] == 2
        assert stats['success_count'] == 0

    def test_save_metrics_no_persistence(self, metrics_collector):
        """Test metrics saving when persistence is disabled."""
        result = metrics_collector.save_metrics()
        assert result is False

    def test_cleanup_old_metrics_no_persistence(self, metrics_collector):
        """Test cleanup when persistence is disabled."""
        result = metrics_collector.cleanup_old_metrics(retention_days=30)
        assert result is False

    def test_save_metrics_success(self, mock_analytics_engine, mock_unified_metrics_connection):
        """Test successful metrics saving."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
            
            # Mock the engine connect method
            collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
            
            # Add some data
            collector.record_table_processed('patient', 1000, 30.5, True)
            
            # Mock the execute method
            mock_result = MagicMock()
            mock_unified_metrics_connection.execute.return_value = mock_result
            
            result = collector.save_metrics()
            
            assert result is True
            assert mock_unified_metrics_connection.execute.call_count == 2  # 1 pipeline + 1 table metric
            mock_unified_metrics_connection.commit.assert_called_once()

    def test_save_metrics_database_error(self, mock_analytics_engine, mock_unified_metrics_connection):
        """Test metrics saving with database error."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
            
            # Mock the engine connect method
            collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
            
            # Mock execute to raise an exception
            mock_unified_metrics_connection.execute.side_effect = SQLAlchemyError("Database error")
            
            result = collector.save_metrics()
            
            assert result is False

    def test_cleanup_old_metrics_success(self, mock_analytics_engine, mock_unified_metrics_connection):
        """Test successful cleanup of old metrics."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
            
            # Mock the engine connect method
            collector.analytics_engine.connect.return_value = mock_unified_metrics_connection
            
            # Mock the execute method
            mock_result = MagicMock()
            mock_unified_metrics_connection.execute.return_value = mock_result
            
            result = collector.cleanup_old_metrics(retention_days=30)
            
            assert result is True
            assert mock_unified_metrics_connection.execute.call_count == 2  # 2 DELETE statements
            mock_unified_metrics_connection.commit.assert_called_once()

    def test_cleanup_old_metrics_database_error(self, mock_analytics_engine, mock_unified_metrics_connection):
        """Test cleanup with database error."""
        with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
            collector = UnifiedMetricsCollector(mock_analytics_engine, enable_persistence=True)
            
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

    def test_backward_compatibility_aliases(self):
        """Test backward compatibility aliases."""
        from etl_pipeline.monitoring.unified_metrics import MetricsCollector, PipelineMetrics
        
        collector1 = MetricsCollector()
        collector2 = PipelineMetrics()
        
        assert isinstance(collector1, UnifiedMetricsCollector)
        assert isinstance(collector2, UnifiedMetricsCollector)

    def test_pipeline_id_uniqueness(self):
        """Test that pipeline IDs are unique."""
        # Add a small delay to ensure different timestamps
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567891.0]  # Different timestamps
            
            collector1 = UnifiedMetricsCollector()
            collector2 = UnifiedMetricsCollector()
            
            assert collector1.metrics['pipeline_id'] != collector2.metrics['pipeline_id']

    def test_metrics_json_serialization(self, metrics_collector):
        """Test that metrics can be serialized to JSON."""
        metrics_collector.record_table_processed('patient', 1000, 30.5, True)
        
        # This should not raise an exception
        json_str = json.dumps(metrics_collector.metrics, default=str)
        assert isinstance(json_str, str)
        assert 'patient' in json_str

    def test_zero_division_in_success_rate(self, metrics_collector):
        """Test success rate calculation with zero tables."""
        stats = metrics_collector.get_pipeline_stats()
        
        # Should not raise ZeroDivisionError
        assert stats['success_rate'] == 0.0

    def test_end_pipeline_without_start(self, metrics_collector):
        """Test pipeline end without starting."""
        result = metrics_collector.end_pipeline()
        
        assert result['status'] == 'idle'
        assert result['total_time'] is None

    def test_record_table_processed_failure_no_error(self, metrics_collector):
        """Test recording failed table processing without error message."""
        metrics_collector.record_table_processed('procedure', 750, 25.0, False)
        
        assert len(metrics_collector.metrics['errors']) == 0  # No error message provided
        assert metrics_collector.metrics['table_metrics']['procedure']['success'] is False

    def test_get_pipeline_status_nonexistent_table(self, metrics_collector):
        """Test pipeline status for non-existent table."""
        metrics_collector.record_table_processed('patient', 1000, 30.5, True)
        
        status = metrics_collector.get_pipeline_status(table='nonexistent')
        
        assert status['tables'] == []
        assert status['tables_processed'] == 1  # Total still shows all

    def test_get_table_status_failed_table(self, metrics_collector):
        """Test getting status for failed table."""
        metrics_collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
        
        table_status = metrics_collector.get_table_status('appointment')
        
        assert table_status['success'] is False
        assert table_status['error'] == "Connection timeout"
        assert table_status['status'] == 'failed'

    def test_get_pipeline_stats_with_pipeline_time(self, metrics_collector):
        """Test pipeline stats with pipeline timing."""
        with patch('etl_pipeline.monitoring.unified_metrics.time') as mock_time:
            mock_time.time.side_effect = [1234567890.0, 1234567920.0]
            with patch('etl_pipeline.monitoring.unified_metrics.datetime') as mock_datetime:
                mock_now = datetime(2023, 1, 1, 12, 0, 0)
                mock_datetime.now.return_value = mock_now
                
                metrics_collector.start_pipeline()
                metrics_collector.record_table_processed('patient', 1000, 30.5, True)
                metrics_collector.end_pipeline()
                
                stats = metrics_collector.get_pipeline_stats()
                
                assert stats['total_time'] == 30.0
                assert stats['status'] == 'completed' 