"""Unit tests for the read-side extract cursor."""

from etl_pipeline.core.cursor import (
    TRACKING_PRIMARY_VALUE,
    TRACKING_TIMESTAMP_ONLY,
    ExtractCursor,
)
from etl_pipeline.monitoring.replica_sync_config import resolve_watermark_cursor


def test_stale_column_resets_cursor():
    cursor = ExtractCursor(
        table_name="procedurelog",
        watermark_column="ProcNum",
        last_value="12345",
    )
    assert cursor.column_changed("DateTStamp")
    assert cursor.resolved_value("DateTStamp") is None
    assert cursor.tracking_kind() == TRACKING_PRIMARY_VALUE


def test_same_timestamp_column_keeps_value():
    cursor = ExtractCursor(
        table_name="procedurelog",
        watermark_column="DateTStamp",
        last_value="2026-06-26",
    )
    assert not cursor.column_changed("DateTStamp")
    assert cursor.resolved_value("DateTStamp") == "2026-06-26"
    assert cursor.tracking_kind() == TRACKING_TIMESTAMP_ONLY


def test_missing_value_stays_missing():
    cursor = ExtractCursor(table_name="patient", watermark_column="DateTStamp")
    assert cursor.resolved_value("DateTStamp") is None
    assert cursor.tracking_kind() == TRACKING_TIMESTAMP_ONLY


def test_missing_column_has_no_tracking_kind():
    cursor = ExtractCursor(table_name="patient", last_value="10")
    assert cursor.tracking_kind() is None
    assert cursor.resolved_value("PatNum") == "10"


def test_advance_returns_a_new_cursor():
    current = ExtractCursor(
        table_name="payment",
        watermark_column="PayNum",
        last_value="10",
        rows_copied=1,
    )
    nxt = current.advance(
        last_value="40",
        watermark_column="DateTStamp",
        rows_copied=12,
    )
    assert current.last_value == "10"
    assert nxt.table_name == "payment"
    assert nxt.last_value == "40"
    assert nxt.watermark_column == "DateTStamp"
    assert nxt.rows_copied == 12
    assert nxt.copy_status == "success"
    assert nxt.tracking_kind() == TRACKING_TIMESTAMP_ONLY


def test_resolve_watermark_cursor_uses_extract_cursor():
    assert resolve_watermark_cursor(
        "procedurelog", "12345", "ProcNum", "DateTStamp"
    ) is None
    assert resolve_watermark_cursor(
        "procedurelog", "2026-06-26", "DateTStamp", "DateTStamp"
    ) == "2026-06-26"
