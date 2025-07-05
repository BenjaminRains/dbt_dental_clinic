"""
Priority processor fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Priority processing components
- Table prioritization
- Processing queues
- Priority utilities
"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def mock_priority_processor_settings():
    """Mock priority processor settings for testing."""
    return {
        'enabled': True,
        'max_priority_levels': 5,
        'default_priority': 3,
        'priority_weights': {
            'critical': 5,
            'high': 4,
            'important': 3,
            'normal': 2,
            'low': 1
        },
        'batch_size': 1000,
        'timeout': 300
    }


@pytest.fixture
def mock_priority_processor_table_processor():
    """Mock table processor for priority processing testing."""
    processor = MagicMock()
    processor.process_table.return_value = True
    processor.get_processing_stats.return_value = {
        'tables_processed': 5,
        'rows_processed': 15000,
        'processing_time': 1800.5
    }
    return processor


@pytest.fixture
def priority_processor(mock_priority_processor_settings):
    """Priority processor instance with mocked settings."""
    try:
        from etl_pipeline.core.priority_processor import PriorityProcessor
        processor = PriorityProcessor(settings=mock_priority_processor_settings)
        return processor
    except ImportError:
        # Fallback mock processor
        processor = MagicMock()
        processor.settings = mock_priority_processor_settings
        processor.process_priority_queue.return_value = True
        return processor


@pytest.fixture
def sample_priority_queue():
    """Sample priority queue for testing."""
    return [
        {
            'table_name': 'patient',
            'priority': 5,
            'priority_level': 'critical',
            'batch_size': 1000,
            'added_time': datetime.now() - timedelta(minutes=10)
        },
        {
            'table_name': 'appointment',
            'priority': 4,
            'priority_level': 'high',
            'batch_size': 500,
            'added_time': datetime.now() - timedelta(minutes=5)
        },
        {
            'table_name': 'procedurelog',
            'priority': 3,
            'priority_level': 'important',
            'batch_size': 2000,
            'added_time': datetime.now() - timedelta(minutes=15)
        },
        {
            'table_name': 'claim',
            'priority': 2,
            'priority_level': 'normal',
            'batch_size': 1500,
            'added_time': datetime.now() - timedelta(minutes=20)
        },
        {
            'table_name': 'payment',
            'priority': 1,
            'priority_level': 'low',
            'batch_size': 800,
            'added_time': datetime.now() - timedelta(minutes=25)
        }
    ]


@pytest.fixture
def mock_priority_table_config():
    """Mock priority table configuration for testing."""
    return {
        'tables': {
            'patient': {
                'priority': 5,
                'priority_level': 'critical',
                'batch_size': 1000,
                'processing_order': 1
            },
            'appointment': {
                'priority': 4,
                'priority_level': 'high',
                'batch_size': 500,
                'processing_order': 2
            },
            'procedurelog': {
                'priority': 3,
                'priority_level': 'important',
                'batch_size': 2000,
                'processing_order': 3
            },
            'claim': {
                'priority': 2,
                'priority_level': 'normal',
                'batch_size': 1500,
                'processing_order': 4
            },
            'payment': {
                'priority': 1,
                'priority_level': 'low',
                'batch_size': 800,
                'processing_order': 5
            }
        }
    }


@pytest.fixture
def mock_priority_processor_stats():
    """Mock priority processor statistics for testing."""
    return {
        'total_tables_queued': 15,
        'total_tables_processed': 12,
        'total_processing_time': 3600.5,
        'average_processing_time': 300.0,
        'priority_level_stats': {
            'critical': {'processed': 3, 'avg_time': 180.0},
            'high': {'processed': 4, 'avg_time': 240.0},
            'important': {'processed': 3, 'avg_time': 300.0},
            'normal': {'processed': 2, 'avg_time': 360.0},
            'low': {'processed': 0, 'avg_time': 0.0}
        },
        'queue_stats': {
            'current_queue_size': 3,
            'max_queue_size': 20,
            'average_wait_time': 45.5
        }
    }


@pytest.fixture
def mock_priority_scheduler():
    """Mock priority scheduler for testing."""
    scheduler = MagicMock()
    scheduler.schedule_table.return_value = True
    scheduler.get_next_table.return_value = {
        'table_name': 'patient',
        'priority': 5,
        'priority_level': 'critical'
    }
    scheduler.get_queue_status.return_value = {
        'queue_size': 5,
        'highest_priority': 5,
        'lowest_priority': 1
    }
    return scheduler


@pytest.fixture
def mock_priority_validator():
    """Mock priority validator for testing."""
    validator = MagicMock()
    validator.validate_priority.return_value = True
    validator.validate_table_config.return_value = True
    validator.get_validation_errors.return_value = []
    return validator


@pytest.fixture
def sample_priority_rules():
    """Sample priority rules for testing."""
    return {
        'critical': {
            'description': 'Critical tables that must be processed first',
            'tables': ['patient', 'appointment'],
            'timeout': 180,
            'retry_count': 3
        },
        'high': {
            'description': 'High priority tables',
            'tables': ['procedurelog', 'claim'],
            'timeout': 300,
            'retry_count': 2
        },
        'important': {
            'description': 'Important tables',
            'tables': ['payment', 'insurance'],
            'timeout': 600,
            'retry_count': 1
        },
        'normal': {
            'description': 'Normal priority tables',
            'tables': ['document', 'task'],
            'timeout': 900,
            'retry_count': 1
        },
        'low': {
            'description': 'Low priority tables',
            'tables': ['log', 'temp'],
            'timeout': 1800,
            'retry_count': 0
        }
    }


@pytest.fixture
def mock_priority_monitor():
    """Mock priority monitor for testing."""
    monitor = MagicMock()
    monitor.start_monitoring.return_value = True
    monitor.stop_monitoring.return_value = True
    monitor.get_priority_metrics.return_value = {
        'queue_depth': 5,
        'processing_rate': 2.5,
        'average_wait_time': 45.5,
        'priority_distribution': {
            'critical': 1,
            'high': 2,
            'important': 1,
            'normal': 1,
            'low': 0
        }
    }
    return monitor


@pytest.fixture
def mock_priority_error_handler():
    """Mock priority error handler for testing."""
    error_handler = MagicMock()
    error_handler.handle_priority_error.return_value = True
    error_handler.retry_table.return_value = True
    error_handler.escalate_priority.return_value = True
    return error_handler


@pytest.fixture
def sample_priority_workflow():
    """Sample priority workflow for testing."""
    return {
        'workflow_id': 'priority_workflow_001',
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'status': 'completed',
        'steps': [
            {
                'step': 'queue_analysis',
                'status': 'completed',
                'duration': 30,
                'tables_analyzed': 15
            },
            {
                'step': 'priority_assignment',
                'status': 'completed',
                'duration': 60,
                'priorities_assigned': 15
            },
            {
                'step': 'queue_processing',
                'status': 'completed',
                'duration': 3300,
                'tables_processed': 12
            }
        ],
        'priority_summary': {
            'critical_processed': 3,
            'high_processed': 4,
            'important_processed': 3,
            'normal_processed': 2,
            'low_processed': 0
        }
    }


@pytest.fixture
def mock_priority_optimizer():
    """Mock priority optimizer for testing."""
    optimizer = MagicMock()
    optimizer.optimize_queue.return_value = True
    optimizer.rebalance_priorities.return_value = True
    optimizer.get_optimization_suggestions.return_value = [
        {
            'table': 'patient',
            'suggestion': 'Increase batch size',
            'expected_improvement': 0.15
        }
    ]
    return optimizer 