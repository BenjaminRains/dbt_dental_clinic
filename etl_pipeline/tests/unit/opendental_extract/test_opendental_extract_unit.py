"""Unit tests for opendental_extract — no Settings / loader fixtures."""

from datetime import timedelta

import pytest

from etl_pipeline.opendental_extract import (
    SourceMySQLConfig,
    clean_row_data,
    resolve_watermark_cursor,
)
from etl_pipeline.opendental_extract.query_builder import (
    build_incremental_where_clause,
    build_mysql_lookback_predicate,
    uses_in_place_timestamp_watermark,
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

SHEETFIELD_MISCLASSIFIED = {
    "sync_profile": "in_place_updates",
    "primary_incremental_column": "SheetFieldNum",
    "replicator_watermark_column": "SheetFieldNum",
    "incremental_columns": ["SheetFieldNum"],
    "incremental_strategy": "single_column",
    "primary_key": "SheetFieldNum",
}

PATIENT_APPEND = {
    "sync_profile": "append_only",
    "primary_incremental_column": "PatNum",
    "replicator_watermark_column": "PatNum",
    "incremental_columns": ["PatNum"],
    "incremental_strategy": "or_logic",
}


class TestSourceMySQLConfig:
    def test_from_mapping_required_keys(self):
        cfg = SourceMySQLConfig.from_mapping(
            {
                "host": "localhost",
                "port": 3306,
                "database": "opendental",
                "user": "readonly",
                "password": "secret",
            }
        )
        assert cfg.host == "localhost"
        assert cfg.port == 3306
        assert cfg.database == "opendental"

    def test_from_mapping_missing_raises(self):
        with pytest.raises(ValueError, match="missing required"):
            SourceMySQLConfig.from_mapping({"host": "localhost"})


class TestCursor:
    def test_resolve_keeps_valid_cursor(self):
        assert (
            resolve_watermark_cursor(
                "procedurelog", "2026-01-01", "DateTStamp", "DateTStamp"
            )
            == "2026-01-01"
        )

    def test_resolve_ignores_stale_column(self):
        assert (
            resolve_watermark_cursor("procedurelog", "12345", "ProcNum", "DateTStamp")
            is None
        )

    def test_resolve_none_last_value(self):
        assert resolve_watermark_cursor("patient", None, "PatNum", "PatNum") is None


class TestQueryBuilder:
    def test_procedurelog_uses_timestamp_watermark(self):
        assert uses_in_place_timestamp_watermark(PROCEDURELOG_CONFIG) is True

    def test_sheetfield_pk_misclassification_guard(self):
        assert uses_in_place_timestamp_watermark(SHEETFIELD_MISCLASSIFIED) is False

    def test_lookback_predicate(self):
        pred = build_mysql_lookback_predicate(PROCEDURELOG_CONFIG)
        assert pred is not None
        assert "DateComplete" in pred
        assert "DATE_SUB" in pred

    def test_wrap_lookback(self):
        wrapped = wrap_mysql_incremental_with_lookback_config(
            "`DateTStamp` > '2026-01-01'", PROCEDURELOG_CONFIG
        )
        assert wrapped.startswith("(")
        assert " OR " in wrapped

    def test_incremental_where_timestamp_path(self):
        where = build_incremental_where_clause(
            PROCEDURELOG_CONFIG,
            last_primary_value="2026-06-01 12:00:00",
        )
        assert "DateTStamp" in where
        assert "2026-06-01" in where

    def test_incremental_where_pk_append(self):
        where = build_incremental_where_clause(
            PATIENT_APPEND,
            last_primary_value="1000",
        )
        assert "`PatNum` > 1000" in where


class TestRowCleaner:
    def test_preserves_primitives(self):
        row = [1, "hello", 3.5, True, None]
        cleaned = clean_row_data(row, ["a", "b", "c", "d", "e"], "patient")
        assert cleaned == [1, "hello", 3.5, True, None]

    def test_strips_control_chars(self):
        row = ["ok\x00bad"]
        cleaned = clean_row_data(row, ["note"], "patient")
        assert cleaned == ["okbad"]

    def test_timedelta_as_time_literal(self):
        row = [timedelta(hours=-1)]
        cleaned = clean_row_data(row, ["TimeVal"], "schedule")
        assert cleaned == ["-01:00:00"]
