"""
Unit tests for SimpleMySQLReplicator error handling on the unified copy path.
"""
from unittest.mock import MagicMock, patch

import pytest

from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseQueryError
from etl_pipeline.exceptions.data import DataExtractionError
from etl_pipeline.exceptions.configuration import ConfigurationError

from tests.unit.core.simple_mysql_replicator.conftest import (
    copy_table_test_patches,
    replicator_with_mock_engines,
)


@pytest.fixture
def replicator_with_error_config(test_settings):
    mock_source_engine = MagicMock()
    mock_target_engine = MagicMock()
    mock_target_engine.url = MagicMock()
    mock_target_engine.url.database = 'test_replication'

    mock_config = {
        'patient': {
            'incremental_columns': ['DateTStamp'],
            'batch_size': 1000,
            'estimated_size_mb': 50,
            'extraction_strategy': 'incremental',
            'performance_category': 'medium',
            'processing_priority': 5,
        }
    }

    with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
         patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
         patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config), \
         patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
        from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
        replicator = SimpleMySQLReplicator(settings=test_settings)
        replicator.source_engine = mock_source_engine
        replicator.target_engine = mock_target_engine
        return replicator


class TestSimpleMySQLReplicatorErrorHandling:
    def test_copy_table_exception_handling(self, replicator_with_error_config):
        replicator = replicator_with_error_config
        with patch.object(replicator, '_execute_table_copy', side_effect=DataExtractionError("Test exception")), \
             patch('psutil.Process') as mock_psutil, \
             patch.object(replicator, '_get_last_copy_time', return_value=None), \
             patch.object(replicator.performance_optimizer, 'should_use_full_refresh', return_value=False):
            mock_psutil.return_value.memory_info.return_value.rss = 100 * 1024 * 1024
            success, metadata = replicator.copy_table('patient')
        assert success is False
        assert 'error' in metadata

    def test_copy_incremental_unified_exception_handling(self, replicator_with_error_config):
        replicator = replicator_with_error_config
        with patch.object(replicator, '_get_incremental_metadata', side_effect=DatabaseConnectionError("Test exception")):
            success, rows = replicator._copy_incremental_unified(
                'patient', replicator.table_configs['patient'], 1000
            )
        assert success is False
        assert rows == 0

    def test_get_last_copy_primary_value_exception_handling(self, replicator_with_error_config):
        replicator = replicator_with_error_config
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.side_effect = DatabaseConnectionError("Database error")
        replicator.target_engine.connect.return_value = mock_conn
        assert replicator._get_last_copy_primary_value('patient') is None

    def test_get_max_primary_value_exception_handling(self, replicator_with_error_config):
        replicator = replicator_with_error_config
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.side_effect = RuntimeError("Query failed")
        replicator.target_engine.connect.return_value = mock_conn
        assert replicator._get_max_primary_value_from_copied_data('patient', 'DateTStamp') is None

    def test_bulk_operations_exception_handling(self, replicator_with_error_config):
        replicator = replicator_with_error_config
        with patch.object(replicator, 'copy_table', side_effect=DataExtractionError("Bulk operation error")):
            results = replicator.copy_all_tables()
        assert isinstance(results, dict)
        assert results['patient'] is False

    def test_specific_exception_handling_validation(self, replicator_with_error_config):
        replicator = replicator_with_error_config

        with patch.object(replicator, '_execute_table_copy', side_effect=DatabaseConnectionError("Connection timeout")):
            success, _ = replicator.copy_table('patient')
            assert success is False

        with patch.object(replicator, '_execute_table_copy', side_effect=DataExtractionError("Extraction failed")):
            success, _ = replicator.copy_table('patient')
            assert success is False

        with patch.object(replicator, 'get_extraction_strategy', side_effect=ConfigurationError("Invalid configuration")):
            success, _ = replicator.copy_table('patient')
            assert success is False

    def test_execute_table_copy_exception_returns_false(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['patient']
        with patch.object(replicator, '_copy_incremental_unified', side_effect=RuntimeError("unexpected")):
            success, rows = replicator._execute_table_copy('patient', config, 'incremental')
        assert success is False
        assert rows == 0

    def test_copy_table_returns_metadata_on_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        with copy_table_test_patches(replicator, execute_result=(True, 42)):
            success, metadata = replicator.copy_table('patient')
        assert success is True
        assert metadata['rows_copied'] == 42
