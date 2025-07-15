"""
Metrics fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Monitoring and metrics collection
- Performance tracking
- Unified metrics
- Metrics utilities

Follows the connection architecture patterns:
- Uses provider pattern for dependency injection
- Uses Settings injection for environment-agnostic metrics
- Uses environment separation for test vs production metrics
- Uses unified interface with ConnectionFactory
"""

import pytest
from unittest.mock import MagicMock, Mock, patch
from typing import Dict, List, Any
from datetime import datetime, timedelta

from etl_pipeline.config import create_test_settings
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.core import ConnectionFactory


@pytest.fixture
def test_metrics_settings():
    """Test metrics settings using provider pattern for dependency injection."""
    # Create test provider with injected metrics configuration
    test_provider = DictConfigProvider(
        pipeline={
            'general': {
                'pipeline_name': 'dental_clinic_etl',
                'environment': 'test',
                'timezone': 'UTC',
                'max_retries': 3,
                'retry_delay_seconds': 300,
                'batch_size': 25000,
                'parallel_jobs': 6
            },
            'connections': {
                'source': {
                    'pool_size': 5,
                    'connect_timeout': 10
                },
                'replication': {
                    'pool_size': 10,
                    'max_overflow': 20
                },
                'analytics': {
                    'application_name': 'etl_pipeline_test'
                }
            },
            'stages': {
                'extract': {
                    'enabled': True,
                    'timeout_minutes': 30,
                    'error_threshold': 0.01
                },
                'load': {
                    'enabled': True,
                    'timeout_minutes': 45,
                    'error_threshold': 0.01
                },
                'transform': {
                    'enabled': True,
                    'timeout_minutes': 60,
                    'error_threshold': 0.01
                }
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': {
                    'enabled': True,
                    'path': 'logs/pipeline.log',
                    'max_size_mb': 100,
                    'backup_count': 10
                },
                'console': {
                    'enabled': True,
                    'level': 'INFO'
                }
            },
            'error_handling': {
                'max_consecutive_failures': 3,
                'failure_notification_threshold': 2,
                'auto_retry': {
                    'enabled': True,
                    'max_attempts': 3,
                    'delay_minutes': 5
                }
            },
            'metrics': {
                'enable_persistence': True,
                'retention_days': 30,
                'cleanup_enabled': True,
                'alert_thresholds': {
                    'cpu_usage': 80.0,
                    'memory_usage': 85.0,
                    'processing_time': 3600.0
                }
            }
        },
        env={
            # Test environment variables for metrics
            'ETL_ENVIRONMENT': 'test',
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }
    )
    
    # Create test settings with provider injection
    return create_test_settings(
        pipeline_config=test_provider.configs['pipeline'],
        env_vars=test_provider.configs['env']
    )


@pytest.fixture
def mock_unified_metrics_connection(test_metrics_settings):
    """Mock unified metrics connection using Settings injection."""
    connection = MagicMock()
    connection.execute.return_value = MagicMock()
    connection.close.return_value = None
    return connection


@pytest.fixture
def unified_metrics_collector_no_persistence(test_metrics_settings):
    """Unified metrics collector without persistence using Settings injection."""
    try:
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        
        # Create collector with Settings injection
        collector = UnifiedMetricsCollector(
            settings=test_metrics_settings,
            enable_persistence=False
        )
        return collector
    except ImportError:
        # Fallback mock collector
        collector = MagicMock()
        collector.enable_persistence = False
        collector.settings = test_metrics_settings
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
def unified_metrics_collector_with_persistence(test_metrics_settings):
    """Unified metrics collector with persistence using Settings injection."""
    try:
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        
        # Mock the analytics engine to prevent real connections
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_analytics_raw_connection') as mock_factory:
            mock_engine = MagicMock()
            mock_factory.return_value = mock_engine
            
            # Create collector with Settings injection
            collector = UnifiedMetricsCollector(
                settings=test_metrics_settings,
                enable_persistence=True
            )
            return collector
    except ImportError:
        # Fallback mock collector
        collector = MagicMock()
        collector.enable_persistence = True
        collector.settings = test_metrics_settings
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
def mock_metrics_collector(test_metrics_settings):
    """Mock metrics collector using Settings injection."""
    collector = MagicMock()
    collector.settings = test_metrics_settings
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
def mock_metrics_storage(test_metrics_settings):
    """Mock metrics storage using Settings injection."""
    storage = MagicMock()
    storage.settings = test_metrics_settings
    storage.store_metrics.return_value = True
    storage.retrieve_metrics.return_value = {}
    storage.delete_metrics.return_value = True
    return storage


@pytest.fixture
def mock_metrics_aggregator(test_metrics_settings):
    """Mock metrics aggregator using Settings injection."""
    aggregator = MagicMock()
    aggregator.settings = test_metrics_settings
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
def mock_metrics_alert(test_metrics_settings):
    """Mock metrics alert using Settings injection."""
    alert = MagicMock()
    alert.settings = test_metrics_settings
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
def mock_metrics_dashboard(test_metrics_settings):
    """Mock metrics dashboard using Settings injection."""
    dashboard = MagicMock()
    dashboard.settings = test_metrics_settings
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
def metrics_collector_with_settings(test_metrics_settings):
    """Metrics collector with Settings dependency injection."""
    try:
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        
        # Mock the analytics engine to prevent real connections
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_analytics_raw_connection') as mock_factory:
            mock_engine = MagicMock()
            mock_factory.return_value = mock_engine
            
            # Create metrics collector with Settings injection
            collector = UnifiedMetricsCollector(
                settings=test_metrics_settings,
                enable_persistence=True
            )
            
            return collector
    except ImportError:
        # Fallback mock collector
        collector = MagicMock()
        collector.enable_persistence = True
        collector.settings = test_metrics_settings
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
def mock_analytics_engine_for_metrics(test_metrics_settings):
    """Mock analytics engine specifically for metrics testing using Settings injection."""
    engine = MagicMock()
    engine.name = 'postgresql'
    
    # Create a mock URL object
    mock_url = MagicMock()
    mock_url.database = 'test_opendental_analytics'
    mock_url.host = 'test-analytics-host'
    mock_url.port = 5432
    engine.url = mock_url
    
    # Mock connection behavior
    mock_connection = MagicMock()
    mock_connection.execute.return_value = MagicMock()
    mock_connection.commit.return_value = None
    mock_connection.close.return_value = None
    
    engine.connect.return_value = mock_connection
    
    return engine


@pytest.fixture
def metrics_configs_with_settings(test_metrics_settings):
    """Test metrics configurations using Settings injection."""
    # Test metrics configuration from settings
    metrics_config = test_metrics_settings.pipeline_config.get('metrics', {})
    
    return {
        'enable_persistence': metrics_config.get('enable_persistence', True),
        'retention_days': metrics_config.get('retention_days', 30),
        'cleanup_enabled': metrics_config.get('cleanup_enabled', True),
        'alert_thresholds': metrics_config.get('alert_thresholds', {
            'cpu_usage': 80.0,
            'memory_usage': 85.0,
            'processing_time': 3600.0
        })
    }


@pytest.fixture
def connection_factory_with_metrics_settings(test_metrics_settings):
    """ConnectionFactory with Settings injection for metrics testing."""
    # Mock the ConnectionFactory methods to return mock engines
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        mock_factory.get_analytics_raw_connection.return_value = MagicMock()
        mock_factory.get_analytics_staging_connection.return_value = MagicMock()
        mock_factory.get_analytics_intermediate_connection.return_value = MagicMock()
        mock_factory.get_analytics_marts_connection.return_value = MagicMock()
        
        yield mock_factory


@pytest.fixture
def mock_metrics_provider():
    """Mock metrics provider for testing provider pattern integration."""
    with patch('etl_pipeline.monitoring.unified_metrics.DictConfigProvider') as mock_provider:
        mock_provider_instance = MagicMock()
        mock_provider.return_value = mock_provider_instance
        
        # Configure mock provider with test metrics config
        mock_provider_instance.get_config.return_value = {
            'metrics': {
                'enable_persistence': True,
                'retention_days': 30,
                'cleanup_enabled': True
            }
        }
        
        yield mock_provider_instance 