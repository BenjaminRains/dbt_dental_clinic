"""
Orchestrator fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Pipeline orchestration
- Component coordination
- Workflow management
- Orchestration utilities
"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def mock_components():
    """Mock pipeline components for orchestrator testing."""
    return {
        'replicator': MagicMock(),
        'transformer': MagicMock(),
        'loader': MagicMock(),
        'validator': MagicMock(),
        'monitor': MagicMock()
    }


@pytest.fixture
def orchestrator(mock_components):
    """Pipeline orchestrator instance with mocked components."""
    try:
        from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
        orchestrator = PipelineOrchestrator(
            replicator=mock_components['replicator'],
            transformer=mock_components['transformer'],
            loader=mock_components['loader'],
            validator=mock_components['validator'],
            monitor=mock_components['monitor']
        )
        return orchestrator
    except ImportError:
        # Fallback mock orchestrator
        orchestrator = MagicMock()
        orchestrator.replicator = mock_components['replicator']
        orchestrator.transformer = mock_components['transformer']
        orchestrator.loader = mock_components['loader']
        orchestrator.validator = mock_components['validator']
        orchestrator.monitor = mock_components['monitor']
        return orchestrator


@pytest.fixture
def mock_orchestrator_config():
    """Mock orchestrator configuration for testing."""
    return {
        'enabled': True,
        'parallel_execution': True,
        'max_workers': 4,
        'retry_failed': True,
        'max_retries': 3,
        'retry_delay': 5,
        'timeout': 3600,
        'monitoring_enabled': True,
        'logging_level': 'INFO'
    }


@pytest.fixture
def mock_workflow_steps():
    """Mock workflow steps for orchestrator testing."""
    return [
        {
            'name': 'replicate_data',
            'component': 'replicator',
            'method': 'replicate_all_tables',
            'dependencies': [],
            'timeout': 1800,
            'retry_on_failure': True
        },
        {
            'name': 'transform_data',
            'component': 'transformer',
            'method': 'transform_all_tables',
            'dependencies': ['replicate_data'],
            'timeout': 1200,
            'retry_on_failure': True
        },
        {
            'name': 'load_data',
            'component': 'loader',
            'method': 'load_all_tables',
            'dependencies': ['transform_data'],
            'timeout': 900,
            'retry_on_failure': True
        },
        {
            'name': 'validate_data',
            'component': 'validator',
            'method': 'validate_all_tables',
            'dependencies': ['load_data'],
            'timeout': 600,
            'retry_on_failure': False
        }
    ]


@pytest.fixture
def mock_workflow_execution():
    """Mock workflow execution for testing."""
    return {
        'workflow_id': 'test_workflow_001',
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'status': 'completed',
        'steps': [
            {
                'name': 'replicate_data',
                'status': 'completed',
                'start_time': datetime.now() - timedelta(hours=1),
                'end_time': datetime.now() - timedelta(minutes=45),
                'duration': 900,
                'error': None
            },
            {
                'name': 'transform_data',
                'status': 'completed',
                'start_time': datetime.now() - timedelta(minutes=45),
                'end_time': datetime.now() - timedelta(minutes=30),
                'duration': 900,
                'error': None
            },
            {
                'name': 'load_data',
                'status': 'completed',
                'start_time': datetime.now() - timedelta(minutes=30),
                'end_time': datetime.now() - timedelta(minutes=15),
                'duration': 900,
                'error': None
            },
            {
                'name': 'validate_data',
                'status': 'completed',
                'start_time': datetime.now() - timedelta(minutes=15),
                'end_time': datetime.now(),
                'duration': 900,
                'error': None
            }
        ]
    }


@pytest.fixture
def mock_workflow_execution_with_errors():
    """Mock workflow execution with errors for testing."""
    return {
        'workflow_id': 'test_workflow_002',
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'status': 'failed',
        'steps': [
            {
                'name': 'replicate_data',
                'status': 'completed',
                'start_time': datetime.now() - timedelta(hours=1),
                'end_time': datetime.now() - timedelta(minutes=45),
                'duration': 900,
                'error': None
            },
            {
                'name': 'transform_data',
                'status': 'failed',
                'start_time': datetime.now() - timedelta(minutes=45),
                'end_time': datetime.now() - timedelta(minutes=40),
                'duration': 300,
                'error': 'Transformation failed: Invalid data format'
            },
            {
                'name': 'load_data',
                'status': 'skipped',
                'start_time': None,
                'end_time': None,
                'duration': 0,
                'error': None
            },
            {
                'name': 'validate_data',
                'status': 'skipped',
                'start_time': None,
                'end_time': None,
                'duration': 0,
                'error': None
            }
        ]
    }


@pytest.fixture
def mock_orchestrator_stats():
    """Mock orchestrator statistics for testing."""
    return {
        'total_workflows': 10,
        'successful_workflows': 8,
        'failed_workflows': 2,
        'total_execution_time': timedelta(hours=5),
        'average_execution_time': timedelta(minutes=30),
        'last_execution': datetime.now() - timedelta(hours=1),
        'active_workflows': 0
    }


@pytest.fixture
def mock_dependency_graph():
    """Mock dependency graph for workflow testing."""
    return {
        'replicate_data': [],
        'transform_data': ['replicate_data'],
        'load_data': ['transform_data'],
        'validate_data': ['load_data'],
        'generate_reports': ['validate_data'],
        'cleanup_temp': ['generate_reports']
    }


@pytest.fixture
def mock_orchestrator_error():
    """Mock orchestrator error for testing error handling."""
    class MockOrchestratorError(Exception):
        def __init__(self, message="Orchestration failed", step=None, workflow_id=None):
            self.message = message
            self.step = step
            self.workflow_id = workflow_id
            super().__init__(self.message)
    
    return MockOrchestratorError


@pytest.fixture
def mock_workflow_scheduler():
    """Mock workflow scheduler for testing."""
    scheduler = MagicMock()
    scheduler.schedule_workflow.return_value = 'test_workflow_001'
    scheduler.cancel_workflow.return_value = True
    scheduler.get_workflow_status.return_value = 'scheduled'
    return scheduler


@pytest.fixture
def mock_workflow_monitor():
    """Mock workflow monitor for testing."""
    monitor = MagicMock()
    monitor.start_monitoring.return_value = True
    monitor.stop_monitoring.return_value = True
    monitor.get_metrics.return_value = {
        'cpu_usage': 45.2,
        'memory_usage': 67.8,
        'disk_usage': 23.1,
        'active_connections': 5
    }
    return monitor 