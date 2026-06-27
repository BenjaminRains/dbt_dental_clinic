"""Unit tests for Phase 2 replica sync config helpers."""

import pytest

from etl_pipeline.monitoring.replica_sync_config import (
    build_mysql_loader_where_clause,
    build_mysql_lookback_predicate,
    get_loader_incremental_columns,
    get_replicator_watermark_column,
    lookback_resync_enabled,
    resolve_watermark_cursor,
    should_use_streaming_upsert,
    wrap_mysql_incremental_with_lookback_config,
)

PROCEDURELOG_CONFIG = {
    "sync_profile": "in_place_updates",
    "primary_incremental_column": "DateTStamp",
    "replicator_watermark_column": "DateTStamp",
    "incremental_columns": ["ProcNum", "DateTStamp"],
    "incremental_strategy": "or_logic",
    "primary_key": "ProcNum",
    "lookback_resync": {
        "enabled": True,
        "window_days": 30,
        "predicate_columns": ["DateComplete", "ProcDate"],
    },
}

APPEND_CONFIG = {
    "sync_profile": "append_only",
    "primary_incremental_column": "PatNum",
    "replicator_watermark_column": "PatNum",
    "incremental_columns": ["PatNum", "DateTStamp"],
    "incremental_strategy": "or_logic",
}


class TestReplicaSyncConfig:
    def test_get_replicator_watermark_column_prefers_explicit_field(self):
        assert get_replicator_watermark_column(PROCEDURELOG_CONFIG) == "DateTStamp"

    def test_loader_columns_mutation_table_uses_timestamp_only(self):
        assert get_loader_incremental_columns(PROCEDURELOG_CONFIG) == ["DateTStamp"]

    def test_loader_columns_append_only_keeps_all(self):
        assert get_loader_incremental_columns(APPEND_CONFIG) == ["PatNum", "DateTStamp"]

    def test_build_mysql_lookback_predicate(self):
        predicate = build_mysql_lookback_predicate(PROCEDURELOG_CONFIG)
        assert "DateComplete" in predicate
        assert "ProcDate" in predicate
        assert "INTERVAL 30 DAY" in predicate

    def test_wrap_with_lookback(self):
        wrapped = wrap_mysql_incremental_with_lookback_config(
            "`DateTStamp` > '2026-01-01'", PROCEDURELOG_CONFIG
        )
        assert wrapped.startswith("(`DateTStamp` > '2026-01-01') OR (")
        assert "DateComplete" in wrapped

    def test_should_use_streaming_upsert(self):
        assert should_use_streaming_upsert(PROCEDURELOG_CONFIG, should_truncate=False)
        assert not should_use_streaming_upsert(PROCEDURELOG_CONFIG, should_truncate=True)
        assert not should_use_streaming_upsert(APPEND_CONFIG, should_truncate=False)

    def test_resolve_watermark_cursor_resets_on_column_change(self):
        assert resolve_watermark_cursor(
            "procedurelog", "12345", "ProcNum", "DateTStamp"
        ) is None
        assert resolve_watermark_cursor(
            "procedurelog", "2026-06-26", "DateTStamp", "DateTStamp"
        ) == "2026-06-26"

    def test_build_loader_where_in_place_uses_timestamp_not_pk(self):
        where = build_mysql_loader_where_clause(
            PROCEDURELOG_CONFIG,
            last_analytics_load="2026-06-27 01:51:51",
            last_primary_value="2026-06-26 22:51:51",
            is_integer_column=lambda col: col == "ProcNum",
        )
        assert "DateTStamp" in where
        assert "ProcNum" not in where
        assert "2026-06-26 22:51:51" in where

    def test_build_loader_where_append_only_uses_pk_and_timestamp(self):
        where = build_mysql_loader_where_clause(
            APPEND_CONFIG,
            last_analytics_load="2026-06-27 01:51:51",
            last_primary_value="35000",
            is_integer_column=lambda col: col == "PatNum",
        )
        assert "PatNum" in where
        assert "DateTStamp" in where
        assert " OR " in where

    def test_replicator_and_loader_reference_same_watermark(self):
        watermark = get_replicator_watermark_column(PROCEDURELOG_CONFIG)
        loader_cols = get_loader_incremental_columns(PROCEDURELOG_CONFIG)
        assert watermark in loader_cols
        assert "ProcNum" not in loader_cols
