"""
Unit tests for SimpleMySQLReplicator data processing via unified bulk operations.
"""
import pytest
from unittest.mock import MagicMock, patch

from etl_pipeline.exceptions.database import DatabaseConnectionError
from etl_pipeline.exceptions.data import DataExtractionError

from tests.unit.core.simple_mysql_replicator.conftest import replicator_with_mock_engines


class TestDataProcessingMethods:
    """Unit tests for _execute_bulk_operation and unified copy helpers."""

    @staticmethod
    def _bulk_rows():
        return [
            (1, 'John', '2024-01-01'),
            (2, 'Jane', '2024-01-02'),
            (3, 'Bob', '2024-01-03'),
        ]

    def test_execute_bulk_operation_insert_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['tiny_table'] = {
            'performance_category': 'tiny',
            'primary_key': 'id',
        }
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.connection.connection.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn

        with patch.object(replicator, '_clean_row_data', side_effect=lambda row, cols, table: row):
            rows_inserted = replicator.performance_optimizer._execute_bulk_operation(
                'tiny_table', ['id', 'name', 'date'], self._bulk_rows(), operation_type='insert'
            )
        assert rows_inserted == 3
        mock_cursor.executemany.assert_called_once()

    def test_execute_bulk_operation_empty_rows(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['tiny_table'] = {'primary_key': 'id'}
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_cursor.fetchone.return_value = (0,)
        mock_conn.connection.connection.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn

        rows_inserted = replicator.performance_optimizer._execute_bulk_operation(
            'tiny_table', ['id', 'name', 'date'], [], operation_type='insert'
        )
        assert rows_inserted == 0

    def test_execute_bulk_operation_failure(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['tiny_table'] = {'primary_key': 'id'}
        mock_conn = MagicMock()
        mock_conn.connection.connection.cursor.side_effect = DatabaseConnectionError("Connection failed")
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn

        with patch.object(replicator, '_clean_row_data', side_effect=lambda row, cols, table: row):
            with pytest.raises(DatabaseConnectionError):
                replicator.performance_optimizer._execute_bulk_operation(
                    'tiny_table', ['id', 'name'], [(1, 'John')], operation_type='insert'
                )

    def test_execute_bulk_operation_upsert(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['small_table'] = {'primary_key': 'id'}
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.connection.connection.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn

        with patch.object(replicator, '_clean_row_data', side_effect=lambda row, cols, table: row):
            rows_inserted = replicator.performance_optimizer._execute_bulk_operation(
                'small_table', ['id', 'name', 'date'], self._bulk_rows(), operation_type='upsert'
            )
        assert rows_inserted == 3

    def test_copy_full_table_unified_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['procedurelog']
        with patch.object(replicator, '_get_table_total_count', return_value=3), \
             patch.object(replicator, '_recreate_table_structure', return_value=True), \
             patch.object(replicator.performance_optimizer, '_execute_bulk_operation', return_value=3):
            mock_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.fetchall.side_effect = [self._bulk_rows(), []]
            mock_result.keys.return_value = ['id', 'name', 'date']
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=None)
            mock_conn.execute.return_value = mock_result
            replicator.source_engine.connect.return_value = mock_conn
            success, rows = replicator._copy_full_table_unified('procedurelog', 1000, config)
        assert success is True
        assert rows == 3

    def test_copy_full_table_unified_recreate_failure(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['procedurelog']
        with patch.object(replicator, '_get_table_total_count', return_value=0), \
             patch.object(replicator, '_recreate_table_structure', return_value=False):
            success, rows = replicator._copy_full_table_unified('procedurelog', 1000, config)
        assert success is False
        assert rows == 0

    def test_execute_table_copy_large_table(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['procedurelog']
        with patch.object(replicator, '_copy_full_table_unified', return_value=(True, 1_000_000)) as mock_full:
            success, rows = replicator._execute_table_copy('procedurelog', config, 'full_table')
        assert success is True
        assert rows == 1_000_000
        mock_full.assert_called_once()

    def test_execute_table_copy_failure(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['procedurelog']
        with patch.object(replicator, '_copy_full_table_unified', side_effect=DataExtractionError("Copy failed")):
            success, rows = replicator._execute_table_copy('procedurelog', config, 'full_table')
        assert success is False
        assert rows == 0

    def test_recreate_table_structure_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        mock_source_result = MagicMock()
        mock_source_result.fetchone.return_value = (
            'test_table',
            'CREATE TABLE `test_table` (`id` int NOT NULL, PRIMARY KEY (`id`))',
        )
        mock_source_conn.execute.return_value = mock_source_result
        mock_source_conn.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_conn.__exit__ = MagicMock(return_value=None)
        mock_target_conn.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_conn.__exit__ = MagicMock(return_value=None)
        replicator.source_engine.connect.return_value = mock_source_conn
        replicator.target_engine.connect.return_value = mock_target_conn

        with patch.object(replicator.settings, 'get_replication_connection_config', return_value={'database': 'test_db'}):
            result = replicator._recreate_table_structure('test_table')
        assert result is True

    def test_calculate_adaptive_batch_size_with_config(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = {'batch_size': 75000, 'performance_category': 'large', 'estimated_rows': 1000000}
        batch_size = replicator.performance_optimizer.calculate_adaptive_batch_size('large_table', config)
        assert batch_size == 75000

    def test_get_connection_manager_config_large_table(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = {'estimated_size_mb': 200, 'performance_category': 'large'}
        manager_config = replicator._get_connection_manager_config('large_table', config)
        assert manager_config['max_retries'] == 5
        assert manager_config['retry_delay'] == 2.0
