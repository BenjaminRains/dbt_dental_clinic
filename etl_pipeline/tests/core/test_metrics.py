"""
Tests for the metrics module.
"""
import pytest
from datetime import datetime
import time

from etl_pipeline.core.metrics import MetricsCollector

def test_metrics_collector_initialization():
    """Test MetricsCollector initialization."""
    collector = MetricsCollector()
    
    assert collector.metrics['start_time'] is None
    assert collector.metrics['end_time'] is None
    assert collector.metrics['tables_processed'] == 0
    assert collector.metrics['rows_processed'] == 0
    assert collector.metrics['errors'] == []

def test_pipeline_timing():
    """Test pipeline timing recording."""
    collector = MetricsCollector()
    
    # Test start time
    collector.start_pipeline()
    assert isinstance(collector.metrics['start_time'], datetime)
    
    # Add a small delay to ensure different timestamps
    time.sleep(0.1)
    
    # Test end time
    collector.end_pipeline()
    assert isinstance(collector.metrics['end_time'], datetime)
    assert collector.metrics['start_time'] < collector.metrics['end_time']

def test_table_processing():
    """Test table processing metrics."""
    collector = MetricsCollector()
    
    # Test single table
    collector.record_table_processed('table1', 1000)
    assert collector.metrics['tables_processed'] == 1
    assert collector.metrics['rows_processed'] == 1000
    
    # Test multiple tables
    collector.record_table_processed('table2', 2000)
    assert collector.metrics['tables_processed'] == 2
    assert collector.metrics['rows_processed'] == 3000

def test_error_recording():
    """Test error recording."""
    collector = MetricsCollector()
    
    # Record an error
    error_message = "Test error"
    collector.record_error(error_message)
    
    assert len(collector.metrics['errors']) == 1
    error = collector.metrics['errors'][0]
    assert error['message'] == error_message
    assert isinstance(error['timestamp'], datetime)

def test_get_metrics():
    """Test metrics retrieval."""
    collector = MetricsCollector()
    
    # Record some metrics
    collector.start_pipeline()
    collector.record_table_processed('table1', 1000)
    collector.record_error("Test error")
    collector.end_pipeline()
    
    # Get metrics
    metrics = collector.get_metrics()
    
    assert isinstance(metrics, dict)
    assert metrics['tables_processed'] == 1
    assert metrics['rows_processed'] == 1000
    assert len(metrics['errors']) == 1
    assert isinstance(metrics['start_time'], datetime)
    assert isinstance(metrics['end_time'], datetime) 