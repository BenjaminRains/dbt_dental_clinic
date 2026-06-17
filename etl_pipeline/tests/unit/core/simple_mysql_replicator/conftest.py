"""Shared fixtures and helpers for SimpleMySQLReplicator unit tests."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator


STANDARD_TABLE_CONFIGS = {
    'patient': {
        'incremental_columns': ['DateTStamp'],
        'batch_size': 1000,
        'estimated_size_mb': 50,
        'extraction_strategy': 'incremental',
        'performance_category': 'medium',
        'processing_priority': 5,
        'estimated_processing_time_minutes': 0.1,
        'memory_requirements_mb': 10,
    },
    'appointment': {
        'incremental_columns': ['AptDateTime'],
        'batch_size': 500,
        'estimated_size_mb': 25,
        'extraction_strategy': 'incremental',
        'performance_category': 'small',
        'processing_priority': 3,
        'estimated_processing_time_minutes': 0.05,
        'memory_requirements_mb': 5,
    },
    'procedurelog': {
        'incremental_columns': ['ProcDate'],
        'batch_size': 2000,
        'estimated_size_mb': 100,
        'extraction_strategy': 'full_table',
        'performance_category': 'large',
        'processing_priority': 7,
        'estimated_processing_time_minutes': 0.5,
        'memory_requirements_mb': 50,
    },
}


@contextmanager
def copy_table_test_patches(replicator, execute_result=(True, 100), force_full_refresh=False):
    """Patch copy_table dependencies for isolated unit tests."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=None)
    mock_result = MagicMock()
    mock_result.scalar.return_value = execute_result[1]
    mock_conn.execute.return_value = mock_result
    replicator.target_engine.connect.return_value = mock_conn
    replicator.target_engine.url.database = 'test_replication'

    mock_process = MagicMock()
    mock_process.memory_info.return_value.rss = 100 * 1024 * 1024

    with patch.object(replicator.performance_optimizer, 'should_use_full_refresh', return_value=force_full_refresh), \
         patch.object(replicator, '_get_last_copy_time', return_value=None), \
         patch.object(replicator, '_execute_table_copy', return_value=execute_result) as mock_execute, \
         patch.object(replicator, '_get_max_primary_value_from_copied_data', return_value=None), \
         patch.object(replicator, '_update_copy_status', return_value=True), \
         patch.object(replicator.performance_optimizer, '_track_performance_optimized'), \
         patch('psutil.Process', return_value=mock_process), \
         patch('etl_pipeline.core.simple_mysql_replicator.time.time', return_value=1000.0):
        yield mock_execute


@pytest.fixture
def replicator_with_mock_engines(test_settings):
    """Create replicator with mocked engines and standard table configs."""
    mock_source_engine = MagicMock()
    mock_target_engine = MagicMock()
    mock_target_engine.url = MagicMock()
    mock_target_engine.url.database = 'test_replication'

    with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
         patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
         patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True), \
         patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=dict(STANDARD_TABLE_CONFIGS)):
        replicator = SimpleMySQLReplicator(settings=test_settings)
        replicator.source_engine = mock_source_engine
        replicator.target_engine = mock_target_engine
        return replicator
