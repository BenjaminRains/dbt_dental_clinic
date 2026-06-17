"""Unit tests for postgres_loader parallel strategy, config gate, and post-load validation."""

from unittest.mock import MagicMock

import pytest

from etl_pipeline.loaders.postgres_loader import (
    LoadPreparation,
    LoadResult,
    LoadStrategyType,
    ParallelLoadStrategy,
    PostgresLoader,
)
from etl_pipeline.config import create_test_settings


@pytest.fixture
def postgres_loader():
    settings = create_test_settings(
        pipeline={
            "stages": {
                "load": {
                    "enable_parallel_loading": False,
                    "parallel_workers": 4,
                    "parallel_min_rows": 1_000_000,
                }
            }
        }
    )

    def mock_get_database_config(db_type, *args, **kwargs):
        return {"database": "test_db", "schema": "raw"}

    settings.get_database_config = MagicMock(side_effect=mock_get_database_config)

    replication_engine = MagicMock()
    replication_engine.url.database = "test_repl"
    analytics_engine = MagicMock()
    schema_adapter = MagicMock()

    return PostgresLoader(
        replication_engine=replication_engine,
        analytics_engine=analytics_engine,
        settings=settings,
        schema_adapter=schema_adapter,
    )


def _load_prep(**overrides):
    base = {
        "table_name": "patient",
        "table_config": {},
        "mysql_schema": {},
        "query": "SELECT * FROM test_repl.patient",
        "primary_column": "PatNum",
        "incremental_columns": [],
        "incremental_strategy": "or_logic",
        "should_truncate": True,
        "force_full": True,
        "batch_size": 10000,
        "estimated_rows": 2_000_000,
        "estimated_size_mb": 300.0,
    }
    base.update(overrides)
    return LoadPreparation(**base)


@pytest.mark.unit
class TestPostgresLoaderStrategySelection:
    def test_parallel_disabled_uses_copy_csv_for_large_table(self, postgres_loader):
        prep = _load_prep()
        assert postgres_loader._select_strategy(prep) == LoadStrategyType.COPY_CSV

    def test_parallel_enabled_for_massive_table(self, postgres_loader):
        postgres_loader.settings.get_pipeline_setting = MagicMock(
            side_effect=lambda key, default=None: {
                "stages.load.enable_parallel_loading": True,
                "stages.load.parallel_workers": 4,
                "stages.load.parallel_min_rows": 1_000_000,
            }.get(key, default)
        )
        prep = _load_prep(estimated_rows=2_000_000)
        assert postgres_loader._select_strategy(prep) == LoadStrategyType.PARALLEL

    def test_parallel_not_selected_below_row_threshold(self, postgres_loader):
        postgres_loader.settings.get_pipeline_setting = MagicMock(
            side_effect=lambda key, default=None: {
                "stages.load.enable_parallel_loading": True,
                "stages.load.parallel_min_rows": 5_000_000,
            }.get(key, default)
        )
        prep = _load_prep(estimated_rows=2_000_000, estimated_size_mb=300.0)
        assert postgres_loader._select_strategy(prep) == LoadStrategyType.COPY_CSV


@pytest.mark.unit
class TestParallelLoadStrategy:
    def test_falls_back_without_primary_column(self):
        fallback = MagicMock()
        fallback.execute.return_value = LoadResult(
            success=True, rows_loaded=10, strategy_used="copy_csv", duration=0.1
        )
        strategy = ParallelLoadStrategy(
            MagicMock(), MagicMock(), "raw", MagicMock(), fallback_strategy=fallback
        )
        prep = _load_prep(primary_column=None, should_truncate=True)
        result = strategy.execute(prep)
        assert result.success is True
        assert result.strategy_used == "copy_csv"
        fallback.execute.assert_called_once_with(prep)

    def test_split_pk_ranges_even_distribution(self):
        ranges = ParallelLoadStrategy._split_pk_ranges(1, 100, 4)
        assert ranges[0][0] == 1
        assert ranges[-1][1] == 100
        assert len(ranges) == 4


@pytest.mark.unit
class TestPostLoadValidation:
    def test_full_load_row_count_mismatch_warns(self, postgres_loader):
        postgres_loader._get_analytics_row_count = MagicMock(return_value=90)
        postgres_loader._get_replication_row_count = MagicMock(return_value=100)
        postgres_loader._count_null_primary_keys = MagicMock(return_value=0)

        prep = _load_prep(should_truncate=True)
        load_result = LoadResult(success=True, rows_loaded=90, strategy_used="standard", duration=1.0)
        validation = postgres_loader._validate_post_load("patient", prep, load_result)

        assert validation["row_count_match"] is False
        assert len(validation["warnings"]) == 1
        assert "mismatch" in validation["warnings"][0]

    def test_primary_key_null_warning(self, postgres_loader):
        postgres_loader._get_analytics_row_count = MagicMock(return_value=100)
        postgres_loader._get_replication_row_count = MagicMock(return_value=100)
        postgres_loader._count_null_primary_keys = MagicMock(return_value=3)

        prep = _load_prep(should_truncate=False)
        load_result = LoadResult(success=True, rows_loaded=5, strategy_used="streaming", duration=1.0)
        validation = postgres_loader._validate_post_load("patient", prep, load_result)

        assert validation["primary_key_nulls"] == 3
        assert any("NULL" in w for w in validation["warnings"])
