"""
Metrics fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Monitoring and metrics collection
- Performance tracking
- Unified metrics
- Metrics utilities
"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def mock_unified_metrics_connection():
    """Mock unified metrics connection for testing."""
    connection = MagicMock()
    connection.execute.return_value = MagicMock()
    connection.close.return_value = None
    return connection


@pytest.fixture
def unified_metrics_collector_no_persistence():
    """Unified metrics collector without persistence for testing."""
    try:
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        collector = UnifiedMetricsCollector(enable_persistence=False)
        return collector
    except ImportError:
        # Fallback mock collector
        collector = MagicMock()
        collector.enable_persistence = False
        collector.metrics = {
            'status': 'idle',
            'tables_processed': 0,
            'total_rows_processed': 0,
            'errors': [],
            'table_metrics': {},
            'pipeline_id': 'test_pipeline_001'
        }
        return collector


@pytest.fixture
def mock_metrics_data():
    """Mock metrics data for testing."""
    return {
        'pipeline_metrics': {
            'total_tables_processed': 15,
            'total_rows_processed': 50000,
            'total_processing_time': 1800.5,
            'average_processing_time_per_table': 120.0,
            'success_rate': 0.93,
            'error_count': 1
        },
        'performance_metrics': {
            'cpu_usage_avg': 45.2,
            'memory_usage_avg': 67.8,
            'disk_io_read_mb': 1250.5,
            'disk_io_write_mb': 890.3,
            'network_io_mb': 450.7
        },
        'database_metrics': {
            'source_connection_count': 5,
            'target_connection_count': 3,
            'query_execution_time_avg': 2.5,
            'slow_queries_count': 3,
            'connection_errors': 0
        }
    }


@pytest.fixture
def mock_performance_metrics():
    """Mock performance metrics for testing."""
    return {
        'timestamp': datetime.now(),
        'cpu_usage': 45.2,
        'memory_usage': 67.8,
        'disk_usage': 23.1,
        'network_io': 125.5,
        'active_connections': 5,
        'queue_length': 2,
        'response_time_avg': 150.3
    }


@pytest.fixture
def mock_pipeline_metrics():
    """Mock pipeline metrics for testing."""
    return {
        'pipeline_id': 'test_pipeline_001',
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'status': 'completed',
        'tables_processed': 15,
        'rows_processed': 50000,
        'processing_time': 1800.5,
        'success_rate': 0.93,
        'errors': [
            {
                'table': 'claim',
                'error': 'Connection timeout',
                'timestamp': datetime.now() - timedelta(minutes=30)
            }
        ]
    }


@pytest.fixture
def mock_database_metrics():
    """Mock database metrics for testing."""
    return {
        'source_db': {
            'connection_count': 5,
            'active_queries': 2,
            'query_execution_time_avg': 2.5,
            'slow_queries': 1,
            'connection_errors': 0
        },
        'target_db': {
            'connection_count': 3,
            'active_queries': 1,
            'query_execution_time_avg': 1.8,
            'slow_queries': 0,
            'connection_errors': 0
        }
    }


@pytest.fixture
def mock_metrics_collector():
    """Mock metrics collector for testing."""
    collector = MagicMock()
    collector.collect_system_metrics.return_value = {
        'cpu_usage': 45.2,
        'memory_usage': 67.8,
        'disk_usage': 23.1
    }
    collector.collect_pipeline_metrics.return_value = {
        'tables_processed': 15,
        'rows_processed': 50000,
        'processing_time': 1800.5
    }
    collector.collect_database_metrics.return_value = {
        'connection_count': 8,
        'active_queries': 3,
        'query_execution_time_avg': 2.1
    }
    return collector


@pytest.fixture
def mock_metrics_storage():
    """Mock metrics storage for testing."""
    storage = MagicMock()
    storage.store_metrics.return_value = True
    storage.retrieve_metrics.return_value = {}
    storage.delete_metrics.return_value = True
    return storage


@pytest.fixture
def mock_metrics_aggregator():
    """Mock metrics aggregator for testing."""
    aggregator = MagicMock()
    aggregator.aggregate_metrics.return_value = {
        'hourly_avg': {
            'cpu_usage': 42.5,
            'memory_usage': 65.2,
            'processing_time': 1750.3
        },
        'daily_avg': {
            'cpu_usage': 38.7,
            'memory_usage': 62.1,
            'processing_time': 1680.8
        }
    }
    return aggregator


@pytest.fixture
def mock_metrics_alert():
    """Mock metrics alert for testing."""
    alert = MagicMock()
    alert.check_thresholds.return_value = [
        {
            'metric': 'cpu_usage',
            'value': 85.2,
            'threshold': 80.0,
            'severity': 'warning'
        }
    ]
    alert.send_alert.return_value = True
    return alert


@pytest.fixture
def mock_metrics_dashboard():
    """Mock metrics dashboard for testing."""
    dashboard = MagicMock()
    dashboard.get_summary_metrics.return_value = {
        'total_pipelines': 25,
        'successful_pipelines': 23,
        'failed_pipelines': 2,
        'average_processing_time': 1650.5,
        'total_rows_processed': 1250000
    }
    dashboard.get_detailed_metrics.return_value = {
        'pipeline_001': {
            'status': 'completed',
            'processing_time': 1800.5,
            'rows_processed': 50000
        }
    }
    return dashboard


@pytest.fixture
def sample_metrics_query():
    """Sample metrics query for testing."""
    return """
    SELECT 
        pipeline_id,
        start_time,
        end_time,
        status,
        tables_processed,
        rows_processed,
        processing_time
    FROM pipeline_metrics 
    WHERE start_time >= '2024-01-01' 
    ORDER BY start_time DESC;
    """


@pytest.fixture
def mock_metrics_error():
    """Mock metrics error for testing error handling."""
    class MockMetricsError(Exception):
        def __init__(self, message="Metrics collection failed", metric_type=None):
            self.message = message
            self.metric_type = metric_type
            super().__init__(self.message)
    
    return MockMetricsError


@pytest.fixture
def metrics_collector_with_settings():
    """Metrics collector with settings dependency injection support."""
    try:
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        from tests.fixtures.config_fixtures import test_pipeline_config
        from tests.fixtures.env_fixtures import test_env_vars
        
        # Create a mock settings object that would be injected
        mock_settings = MagicMock()
        mock_settings.get_database_config.return_value = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_analytics',
            'user': 'test_user',
            'password': 'test_pass'
        }
        mock_settings.pipeline_config = test_pipeline_config
        
        # Create metrics collector (currently without settings injection, but ready for future)
        collector = UnifiedMetricsCollector(enable_persistence=False)
        
        # Add settings as an attribute for testing purposes
        collector.settings = mock_settings
        
        return collector
    except ImportError:
        # Fallback mock collector
        collector = MagicMock()
        collector.enable_persistence = False
        collector.metrics = {
            'status': 'idle',
            'tables_processed': 0,
            'total_rows_processed': 0,
            'errors': [],
            'table_metrics': {},
            'pipeline_id': 'test_pipeline_001'
        }
        collector.settings = MagicMock()
        return collector


@pytest.fixture
def mock_analytics_engine_for_metrics():
    """Mock analytics engine specifically for metrics testing."""
    engine = MagicMock()
    engine.name = 'postgresql'
    
    # Create a mock URL object
    mock_url = MagicMock()
    mock_url.database = 'test_analytics'
    mock_url.host = 'localhost'
    mock_url.port = 5432
    engine.url = mock_url
    
    # Mock connection behavior
    mock_connection = MagicMock()
    mock_connection.execute.return_value = MagicMock()
    mock_connection.commit.return_value = None
    mock_connection.close.return_value = None
    
    engine.connect.return_value = mock_connection
    
    return engine 