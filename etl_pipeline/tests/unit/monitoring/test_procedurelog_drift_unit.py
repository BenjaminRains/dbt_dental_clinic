"""Unit tests for ETL-FND-001 procedurelog drift helpers."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from etl_pipeline.monitoring.procedurelog_drift import (
    PROCEDURELOG_LOOKBACK_DAYS,
    check_procedurelog_drift,
    fetch_mysql_complete_totals,
    fetch_postgres_raw_complete_totals,
    mysql_lookback_resync_predicate,
    wrap_mysql_incremental_with_lookback,
)


@pytest.mark.unit
class TestProcedurelogLookbackPredicates:
    def test_mysql_lookback_resync_predicate_default(self):
        predicate = mysql_lookback_resync_predicate()
        assert "DATE_SUB(CURDATE(), INTERVAL 30 DAY)" in predicate
        assert "`DateComplete` >=" in predicate
        assert "`ProcDate` >=" in predicate

    def test_wrap_mysql_incremental_with_lookback(self):
        wrapped = wrap_mysql_incremental_with_lookback("`DateTStamp` > '2026-01-01'")
        assert wrapped.startswith("(`DateTStamp` > '2026-01-01') OR (")
        assert "`DateComplete` >=" in wrapped


@pytest.mark.unit
class TestProcedurelogDriftCheck:
    def _mock_engine(self, complete_rows: int, complete_fees: str):
        engine = MagicMock()
        conn = MagicMock()
        row = MagicMock()
        row.complete_rows = complete_rows
        row.complete_fees = complete_fees
        conn.execute.return_value.one.return_value = row
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        ctx.__exit__.return_value = None
        engine.connect.return_value = ctx
        return engine

    def test_check_procedurelog_drift_in_sync(self):
        source = self._mock_engine(140, "15239.00")
        raw = self._mock_engine(140, "15239.00")

        result = check_procedurelog_drift(
            source,
            raw,
            source_database="opendental",
            lookback_days=PROCEDURELOG_LOOKBACK_DAYS,
        )

        assert result.drift_detected is False
        assert result.row_delta == 0
        assert result.fee_delta == Decimal("0")

    def test_check_procedurelog_drift_detects_row_gap(self):
        source = self._mock_engine(140, "15239.00")
        raw = self._mock_engine(48, "3719.00")

        result = check_procedurelog_drift(
            source,
            raw,
            source_database="opendental",
            lookback_days=PROCEDURELOG_LOOKBACK_DAYS,
        )

        assert result.drift_detected is True
        assert result.row_delta == 92
        assert "drift detected" in result.message

    def test_fetch_queries_use_lookback_parameter(self):
        source = self._mock_engine(1, "10.00")
        fetch_mysql_complete_totals(
            source,
            database="opendental",
            lookback_days=30,
        )
        sql = str(source.connect.return_value.__enter__.return_value.execute.call_args[0][0])
        assert "lookback_days" in sql

        raw = self._mock_engine(1, "10.00")
        fetch_postgres_raw_complete_totals(raw, lookback_days=30)
        sql = str(raw.connect.return_value.__enter__.return_value.execute.call_args[0][0])
        assert "make_interval" in sql
