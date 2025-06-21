"""Tests for unified metrics functionality."""

import pytest
from datetime import datetime
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector

def test_unified_metrics_initialization():
    """Test UnifiedMetricsCollector initialization."""
    collector = UnifiedMetricsCollector()
    assert collector is not None
    assert collector.metrics['status'] == 'idle'
    assert collector.metrics['tables_processed'] == 0
    assert collector.metrics['total_rows_processed'] == 0

def test_pipeline_start_end():
    """Test pipeline start and end functionality."""
    collector = UnifiedMetricsCollector()
    
    # Start pipeline
    collector.start_pipeline()
    assert collector.metrics['status'] == 'running'
    assert collector.metrics['start_time'] is not None
    
    # End pipeline
    result = collector.end_pipeline()
    assert result['status'] == 'completed'
    assert result['end_time'] is not None
    assert result['total_time'] is not None

def test_table_processing_recording():
    """Test recording table processing metrics."""
    collector = UnifiedMetricsCollector()
    
    # Record table processing
    collector.record_table_processed('patient', 1000, 30.5, True)
    assert collector.metrics['tables_processed'] == 1
    assert collector.metrics['total_rows_processed'] == 1000
    assert 'patient' in collector.metrics['table_metrics']
    
    # Record failed table
    collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
    assert collector.metrics['tables_processed'] == 2
    assert collector.metrics['total_rows_processed'] == 1500
    assert len(collector.metrics['errors']) == 1

def test_error_recording():
    """Test error recording functionality."""
    collector = UnifiedMetricsCollector()
    
    # Record general error
    collector.record_error("Database connection failed")
    assert len(collector.metrics['errors']) == 1
    assert collector.metrics['errors'][0]['message'] == "Database connection failed"
    
    # Record table-specific error
    collector.record_error("Table not found", "nonexistent_table")
    assert len(collector.metrics['errors']) == 2
    assert collector.metrics['errors'][1]['table'] == "nonexistent_table"

def test_pipeline_status():
    """Test pipeline status reporting."""
    collector = UnifiedMetricsCollector()
    
    # Get initial status
    status = collector.get_pipeline_status()
    assert status['status'] == 'idle'
    assert status['tables_processed'] == 0
    assert status['total_rows_processed'] == 0
    
    # Process some tables and get status
    collector.record_table_processed('patient', 1000, 30.5, True)
    collector.record_table_processed('appointment', 500, 15.2, True)
    
    status = collector.get_pipeline_status()
    assert status['tables_processed'] == 2
    assert status['total_rows_processed'] == 1500
    assert len(status['tables']) == 2

def test_table_status():
    """Test table-specific status reporting."""
    collector = UnifiedMetricsCollector()
    
    # Record table processing
    collector.record_table_processed('patient', 1000, 30.5, True)
    
    # Get table status
    table_status = collector.get_table_status('patient')
    assert table_status is not None
    assert table_status['table_name'] == 'patient'
    assert table_status['rows_processed'] == 1000
    assert table_status['processing_time'] == 30.5
    assert table_status['success'] is True
    
    # Get non-existent table status
    table_status = collector.get_table_status('nonexistent')
    assert table_status is None

def test_pipeline_stats():
    """Test pipeline statistics calculation."""
    collector = UnifiedMetricsCollector()
    
    # Process tables with mixed success/failure
    collector.record_table_processed('patient', 1000, 30.5, True)
    collector.record_table_processed('appointment', 500, 15.2, False, "Error")
    collector.record_table_processed('procedure', 750, 25.0, True)
    
    stats = collector.get_pipeline_stats()
    assert stats['tables_processed'] == 3
    assert stats['total_rows_processed'] == 2250
    assert stats['error_count'] == 1
    assert stats['success_count'] == 2
    assert stats['success_rate'] == (2/3) * 100

def test_metrics_reset():
    """Test metrics reset functionality."""
    collector = UnifiedMetricsCollector()
    
    # Add some data
    collector.record_table_processed('patient', 1000, 30.5, True)
    collector.record_error("Test error")
    
    # Reset metrics
    collector.reset_metrics()
    assert collector.metrics['tables_processed'] == 0
    assert collector.metrics['total_rows_processed'] == 0
    assert len(collector.metrics['errors']) == 0
    assert collector.metrics['status'] == 'idle' 