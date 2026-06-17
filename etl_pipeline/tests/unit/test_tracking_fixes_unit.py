"""
Unit tests for tracking table fixes.

Covers:
1. Primary value retrieval during MySQL replication
2. Copy/load tracking updates on success and failure
3. Tracking validation during replicator initialization
"""

import pytest
from unittest.mock import MagicMock, patch

from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.loaders.postgres_loader import (
    PostgresLoader,
    LoadPreparation,
    LoadResult,
    LoadStrategyType,
)
from etl_pipeline.config import DatabaseType

from tests.unit.core.simple_mysql_replicator.conftest import copy_table_test_patches


TEST_TABLE_CONFIG = {
    'test_table': {
        'primary_incremental_column': 'id',
        'incremental_columns': ['id', 'updated_at'],
        'batch_size': 1000,
        'extraction_strategy': 'incremental',
        'performance_category': 'medium',
        'processing_priority': 5,
        'estimated_size_mb': 10,
        'estimated_processing_time_minutes': 0.1,
        'memory_requirements_mb': 5,
    }
}


def _make_load_prep(table_name: str = 'test_table') -> LoadPreparation:
    return LoadPreparation(
        table_name=table_name,
        table_config=TEST_TABLE_CONFIG[table_name],
        mysql_schema={'columns': []},
        query='SELECT * FROM test_table',
        primary_column='id',
        incremental_columns=['id', 'updated_at'],
        incremental_strategy='or_logic',
        should_truncate=False,
        force_full=False,
        batch_size=1000,
        estimated_rows=100,
        estimated_size_mb=10.0,
    )


@pytest.fixture
def mock_replicator(test_settings):
    """SimpleMySQLReplicator with mocked engines and tracking validation."""
    mock_source_engine = MagicMock()
    mock_target_engine = MagicMock()
    mock_target_engine.url = MagicMock()
    mock_target_engine.url.database = 'test_replication'

    with patch(
        'etl_pipeline.core.connections.ConnectionFactory.get_source_connection',
        return_value=mock_source_engine,
    ), patch(
        'etl_pipeline.core.connections.ConnectionFactory.get_replication_connection',
        return_value=mock_target_engine,
    ), patch.object(
        SimpleMySQLReplicator,
        '_validate_tracking_tables_exist',
        return_value=True,
    ), patch.object(
        SimpleMySQLReplicator,
        '_load_configuration',
        return_value=dict(TEST_TABLE_CONFIG),
    ):
        replicator = SimpleMySQLReplicator(settings=test_settings)
        replicator.target_engine = mock_target_engine
        return replicator


@pytest.fixture
def mock_loader(test_settings):
    """PostgresLoader with injected mock engines (matches table_processor wiring)."""
    def mock_get_database_config(db_type, *args):
        if db_type == DatabaseType.ANALYTICS:
            return {'database': 'test_db', 'schema': 'raw'}
        if db_type == DatabaseType.REPLICATION:
            return {'database': 'test_replication'}
        return {'database': 'test_db'}

    test_settings.get_database_config = MagicMock(side_effect=mock_get_database_config)

    replication_engine = MagicMock()
    analytics_engine = MagicMock()
    schema_adapter = MagicMock()
    schema_adapter.get_table_schema_from_mysql.return_value = {'columns': []}
    schema_adapter.ensure_table_exists.return_value = True

    return PostgresLoader(
        replication_engine=replication_engine,
        analytics_engine=analytics_engine,
        settings=test_settings,
        schema_adapter=schema_adapter,
        table_configs=TEST_TABLE_CONFIG,
    )


class TestTrackingFixes:
    """Tracking behavior for replication and load phases."""

    def test_get_max_primary_value_from_copied_data(self, mock_replicator):
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.scalar.return_value = '1000'
        mock_conn.execute.return_value = mock_result
        mock_replicator.target_engine.connect.return_value = mock_conn

        result = mock_replicator._get_max_primary_value_from_copied_data('test_table', 'id')

        assert result == '1000'
        mock_conn.execute.assert_called_once()

    def test_copy_table_updates_tracking_with_primary_value(self, mock_replicator):
        with copy_table_test_patches(mock_replicator, execute_result=(True, 100)):
            with patch.object(
                mock_replicator,
                '_get_max_primary_value_from_copied_data',
                return_value='1000',
            ):
                success, metadata = mock_replicator.copy_table('test_table')

            assert success is True
            assert metadata['last_primary_value'] == '1000'
            mock_replicator._update_copy_status.assert_called_once_with(
                'test_table', 100, 'success', '1000', 'id'
            )

    def test_load_table_updates_tracking_on_failure(self, mock_loader):
        load_prep = _make_load_prep()
        failed_result = LoadResult(
            success=False,
            rows_loaded=0,
            strategy_used='streaming',
            duration=0.1,
            error='Test error',
        )

        with patch.object(mock_loader, '_prepare_table_load', return_value=load_prep), \
             patch.object(mock_loader, '_select_strategy', return_value=LoadStrategyType.STREAMING), \
             patch.object(mock_loader.strategies[LoadStrategyType.STREAMING], 'execute', return_value=failed_result), \
             patch.object(mock_loader, '_ensure_tracking_record_exists'), \
             patch.object(mock_loader, '_update_load_status_hybrid') as mock_update:
            success, _metadata = mock_loader.load_table('test_table')

        assert success is False
        mock_update.assert_called_once()
        assert mock_update.call_args.kwargs['load_status'] == 'failed'
        assert mock_update.call_args.kwargs['rows_loaded'] == 0

    def test_mysql_tracking_validation_on_init(self, test_settings):
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()

        with patch(
            'etl_pipeline.core.connections.ConnectionFactory.get_source_connection',
            return_value=mock_source_engine,
        ), patch(
            'etl_pipeline.core.connections.ConnectionFactory.get_replication_connection',
            return_value=mock_target_engine,
        ), patch.object(
            SimpleMySQLReplicator,
            '_validate_tracking_tables_exist',
            return_value=True,
        ) as mock_validate, patch.object(
            SimpleMySQLReplicator,
            '_load_configuration',
            return_value=dict(TEST_TABLE_CONFIG),
        ):
            SimpleMySQLReplicator(settings=test_settings)

        mock_validate.assert_called_once()

    def test_failed_load_strategies_update_tracking(self, mock_loader):
        load_prep = _make_load_prep()
        strategy_types = [
            LoadStrategyType.STREAMING,
            LoadStrategyType.STANDARD,
            LoadStrategyType.COPY_CSV,
            LoadStrategyType.PARALLEL,
        ]

        for strategy_type in strategy_types:
            failed_result = LoadResult(
                success=False,
                rows_loaded=0,
                strategy_used=strategy_type.value,
                duration=0.1,
                error='Test error',
            )

            with patch.object(mock_loader, '_prepare_table_load', return_value=load_prep), \
                 patch.object(mock_loader, '_select_strategy', return_value=strategy_type), \
                 patch.object(mock_loader.strategies[strategy_type], 'execute', return_value=failed_result), \
                 patch.object(mock_loader, '_ensure_tracking_record_exists'), \
                 patch.object(mock_loader, '_update_load_status_hybrid') as mock_update:
                mock_loader.load_table('test_table')

            mock_update.assert_called_once()
            assert mock_update.call_args.kwargs['load_status'] == 'failed'
            mock_update.reset_mock()
