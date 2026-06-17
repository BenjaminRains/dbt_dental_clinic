"""
Unit tests for SimpleMySQLReplicator table copy logic using provider pattern.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from etl_pipeline.exceptions.database import DatabaseConnectionError

from tests.unit.core.simple_mysql_replicator.conftest import (
    copy_table_test_patches,
    replicator_with_mock_engines,
)


class TestSimpleMySQLReplicatorTableCopyLogic:
    """Unit tests for copy_table routing and metadata."""

    def test_copy_table_incremental_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        with copy_table_test_patches(replicator, execute_result=(True, 100)) as mock_execute:
            success, metadata = replicator.copy_table('patient')
        assert success is True
        assert metadata['rows_copied'] == 100
        assert metadata['strategy_used'] == 'incremental'
        mock_execute.assert_called_once()
        assert mock_execute.call_args[0][0] == 'patient'

    def test_copy_table_incremental_failure(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        with copy_table_test_patches(replicator, execute_result=(False, 0)):
            success, metadata = replicator.copy_table('patient')
        assert success is False
        assert metadata['rows_copied'] == 0

    def test_copy_table_force_full(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        with copy_table_test_patches(replicator, execute_result=(True, 500)) as mock_execute:
            success, metadata = replicator.copy_table('patient', force_full=True)
        assert success is True
        assert metadata['force_full_applied'] is True
        assert mock_execute.call_args[0][2] == 'full_table'

    def test_copy_table_no_configuration(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        success, metadata = replicator.copy_table('unknown_table')
        assert success is False
        assert metadata['error'] == 'No configuration found'

    def test_copy_table_full_table_strategy(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        with copy_table_test_patches(replicator, execute_result=(True, 1000)) as mock_execute:
            success, metadata = replicator.copy_table('procedurelog')
        assert success is True
        assert metadata['strategy_used'] == 'full_table'
        mock_execute.assert_called_once()

    def test_copy_table_incremental_chunked_strategy(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['large_chunked_table'] = {
            'extraction_strategy': 'incremental_chunked',
            'incremental_columns': ['DateTStamp'],
            'batch_size': 1000,
            'estimated_size_mb': 200,
            'performance_category': 'large',
            'processing_priority': 8,
            'estimated_processing_time_minutes': 1.0,
            'memory_requirements_mb': 100,
        }
        with copy_table_test_patches(replicator, execute_result=(True, 500)) as mock_execute:
            success, metadata = replicator.copy_table('large_chunked_table')
        assert success is True
        assert metadata['strategy_used'] == 'incremental_chunked'
        mock_execute.assert_called_once()

    def test_copy_table_unknown_strategy_falls_back_to_full_table(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['patient']['extraction_strategy'] = 'unknown_strategy'
        with copy_table_test_patches(replicator, execute_result=(True, 50)):
            success, metadata = replicator.copy_table('patient')
        assert success is True
        assert metadata['strategy_used'] == 'full_table'

    def test_copy_table_incremental_no_columns(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['patient']['incremental_columns'] = []
        with copy_table_test_patches(replicator, execute_result=(False, 0)):
            success, metadata = replicator.copy_table('patient')
        assert success is False

    def test_copy_table_incremental_multiple_columns(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['patient']['incremental_columns'] = [
            'DateTStamp', 'DateModified', 'DateCreated'
        ]
        with copy_table_test_patches(replicator, execute_result=(True, 200)):
            success, metadata = replicator.copy_table('patient')
        assert success is True
        assert metadata['rows_copied'] == 200


class TestIncrementalTrackingMethods:
    """Unit tests for incremental tracking helpers on the unified copy path."""

    def test_get_last_copy_primary_value_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value.fetchone.return_value = ('2024-01-15', 'DateTStamp')
        replicator.target_engine.connect.return_value = mock_conn

        result = replicator._get_last_copy_primary_value('patient')
        assert result == '2024-01-15'

    def test_get_last_copy_primary_value_no_data(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value.fetchone.return_value = None
        replicator.target_engine.connect.return_value = mock_conn

        result = replicator._get_last_copy_primary_value('patient')
        assert result is None

    def test_get_last_copy_primary_value_exception_handling(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.side_effect = DatabaseConnectionError("Connection failed")
        replicator.target_engine.connect.return_value = mock_conn

        result = replicator._get_last_copy_primary_value('patient')
        assert result is None

    def test_get_max_primary_value_from_copied_data_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value.scalar.return_value = datetime(2024, 1, 16, 14, 45, 0)
        replicator.target_engine.connect.return_value = mock_conn

        result = replicator._get_max_primary_value_from_copied_data('patient', 'DateTStamp')
        assert result == str(datetime(2024, 1, 16, 14, 45, 0))

    def test_get_max_primary_value_from_copied_data_no_data(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.return_value.scalar.return_value = None
        replicator.target_engine.connect.return_value = mock_conn

        result = replicator._get_max_primary_value_from_copied_data('patient', 'DateTStamp')
        assert result is None

    def test_get_max_primary_value_from_copied_data_exception_handling(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_conn.execute.side_effect = DatabaseConnectionError("Connection failed")
        replicator.target_engine.connect.return_value = mock_conn

        result = replicator._get_max_primary_value_from_copied_data('patient', 'DateTStamp')
        assert result is None

    def test_copy_incremental_unified_no_incremental_columns(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = dict(replicator.table_configs['patient'])
        config['incremental_columns'] = []
        success, rows = replicator._copy_incremental_unified('patient', config, 1000)
        assert success is False
        assert rows == 0

    def test_execute_table_copy_routes_full_table(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['procedurelog']
        with patch.object(replicator, '_copy_full_table_unified', return_value=(True, 100)) as mock_full:
            success, rows = replicator._execute_table_copy('procedurelog', config, 'full_table')
        assert success is True
        assert rows == 100
        mock_full.assert_called_once()

    def test_execute_table_copy_routes_incremental_chunked(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['patient']
        with patch.object(replicator.performance_optimizer, 'should_use_full_refresh', return_value=False), \
             patch.object(replicator, '_copy_incremental_chunked', return_value=(True, 25)) as mock_chunked:
            success, rows = replicator._execute_table_copy('patient', config, 'incremental_chunked')
        assert success is True
        assert rows == 25
        mock_chunked.assert_called_once()
