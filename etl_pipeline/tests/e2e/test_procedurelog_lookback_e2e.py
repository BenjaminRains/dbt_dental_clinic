"""
ETL-FND-001: procedurelog lookback_resync e2e (TP → Complete without DateTStamp bump).

Requires test databases (`.env_test`) with procedurelog.DateComplete available —
run `python scripts/setup_test_databases.py` after pulling, or let this module
ALTER existing tables on the fly.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.orchestration import PipelineOrchestrator
from tests.fixtures.test_data_definitions import (
    PROC_STATUS_COMPLETE,
    PROC_STATUS_TP,
    get_tp_complete_lookback_scenario,
)

logger = logging.getLogger(__name__)

PROC_NUM = 1


def _mysql_has_column(conn, table: str, column: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = :table
              AND column_name = :column
            """
        ),
        {"table": table, "column": column},
    )
    return int(result.scalar() or 0) > 0


def _pg_has_column(conn, schema: str, table: str, column: str) -> bool:
    result = conn.execute(
        text(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
              AND column_name = :column
            """
        ),
        {"schema": schema, "table": table, "column": column},
    )
    return int(result.scalar() or 0) > 0


def _ensure_mysql_procedurelog_lookback_schema(engine: Engine) -> None:
    with engine.connect() as conn:
        if not _mysql_has_column(conn, "procedurelog", "DateComplete"):
            conn.execute(
                text(
                    "ALTER TABLE procedurelog "
                    "ADD COLUMN DateComplete DATE NOT NULL DEFAULT '0001-01-01'"
                )
            )
            logger.info("Added DateComplete to MySQL procedurelog on %s", engine.url.database)
        conn.commit()


def _ensure_postgres_procedurelog_lookback_schema(engine: Engine) -> None:
    """
    Recreate analytics procedurelog for lookback e2e.

    Test SchemaAdapter maps MySQL TINYINT → boolean, so ProcStatus stays boolean.
    OD statuses 1 and 2 both load as true; assert heal via fee/DateComplete/ProcNote.
    Replication MySQL still carries real ProcStatus=2.
    """
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw.procedurelog CASCADE"))
        conn.execute(
            text(
                """
                CREATE TABLE raw.procedurelog (
                    "ProcNum" bigint PRIMARY KEY,
                    "PatNum" bigint NOT NULL,
                    "AptNum" bigint NOT NULL,
                    "ProcStatus" boolean DEFAULT false,
                    "ProcFee" decimal(10,2) NOT NULL DEFAULT 0.00,
                    "ProcFeeCur" decimal(10,2) NOT NULL DEFAULT 0.00,
                    "ProcDate" date NOT NULL,
                    "CodeNum" bigint NOT NULL DEFAULT 0,
                    "ProcNote" text,
                    "DateTStamp" timestamp without time zone NOT NULL,
                    "DateComplete" date NOT NULL DEFAULT '0001-01-01',
                    "SecDateEntry" date NOT NULL DEFAULT '0001-01-01'
                )
                """
            )
        )
        conn.commit()
        logger.info("Recreated raw.procedurelog (boolean ProcStatus + DateComplete)")


def _upsert_mysql_procedure(engine: Engine, row: Dict[str, Any]) -> None:
    columns = list(row.keys())
    col_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    updates = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in columns if c != "ProcNum")
    sql = (
        f"INSERT INTO procedurelog ({col_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    with engine.connect() as conn:
        conn.execute(text(sql), row)
        conn.commit()


def _fetch_mysql_proc_row(engine: Engine) -> Optional[Dict[str, Any]]:
    sql = text(
        "SELECT ProcStatus, ProcFee, DateComplete, ProcNote, DateTStamp "
        "FROM procedurelog WHERE ProcNum = :proc_num"
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"proc_num": PROC_NUM}).fetchone()
    if row is None:
        return None
    return {
        "ProcStatus": int(row[0]),
        "ProcFee": float(row[1]),
        "DateComplete": row[2],
        "ProcNote": row[3],
        "DateTStamp": row[4],
    }


def _fetch_pg_proc_row(engine: Engine) -> Optional[Dict[str, Any]]:
    sql = text(
        'SELECT "ProcFee", "DateComplete", "ProcNote" '
        'FROM raw.procedurelog WHERE "ProcNum" = :proc_num'
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"proc_num": PROC_NUM}).fetchone()
    if row is None:
        return None
    return {
        "ProcFee": float(row[0]),
        "DateComplete": row[1],
        "ProcNote": row[2],
    }


def _delete_proc_everywhere(settings: Settings) -> None:
    source = ConnectionFactory.get_source_connection(settings)
    repl = ConnectionFactory.get_replication_connection(settings)
    analytics = ConnectionFactory.get_analytics_raw_connection(settings)
    try:
        for engine, is_pg in ((source, False), (repl, False), (analytics, True)):
            with engine.connect() as conn:
                if is_pg:
                    conn.execute(
                        text('DELETE FROM raw.procedurelog WHERE "ProcNum" = :n'),
                        {"n": PROC_NUM},
                    )
                else:
                    conn.execute(
                        text("DELETE FROM procedurelog WHERE ProcNum = :n"),
                        {"n": PROC_NUM},
                    )
                conn.commit()
    finally:
        source.dispose()
        repl.dispose()
        analytics.dispose()


class TestProcedurelogLookbackE2E:
    """TP → Complete heals via lookback_resync even when DateTStamp does not advance."""

    @pytest.fixture(scope="class")
    def test_settings(self):
        return Settings(environment="test")

    @pytest.fixture(scope="function")
    def lookback_schema(self, test_settings):
        source = ConnectionFactory.get_source_connection(test_settings)
        repl = ConnectionFactory.get_replication_connection(test_settings)
        analytics = ConnectionFactory.get_analytics_raw_connection(test_settings)
        try:
            _ensure_mysql_procedurelog_lookback_schema(source)
            _ensure_mysql_procedurelog_lookback_schema(repl)
            _ensure_postgres_procedurelog_lookback_schema(analytics)
            yield
        finally:
            source.dispose()
            repl.dispose()
            analytics.dispose()

    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.lookback
    def test_tp_to_complete_healed_by_lookback_without_datetstamp_bump(
        self,
        test_settings,
        lookback_schema,
    ):
        """
        Arrange: seed TP row with recent ProcDate / DateTStamp; full ETL.
        Act: Complete in source without changing DateTStamp; incremental extract.
        Assert: replication ProcStatus=2 / fee / DateComplete heal via lookback.
        """
        tp_row, complete_row = get_tp_complete_lookback_scenario(proc_num=PROC_NUM)
        assert tp_row["DateTStamp"] == complete_row["DateTStamp"]
        assert tp_row["ProcStatus"] == PROC_STATUS_TP
        assert complete_row["ProcStatus"] == PROC_STATUS_COMPLETE

        _delete_proc_everywhere(test_settings)

        source = ConnectionFactory.get_source_connection(test_settings)
        try:
            _upsert_mysql_procedure(source, tp_row)
        finally:
            source.dispose()

        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()

        # 1) Seed TP through full pipeline
        assert orchestrator.run_pipeline_for_table("procedurelog", force_full=True)

        analytics = ConnectionFactory.get_analytics_raw_connection(test_settings)
        try:
            after_full = _fetch_pg_proc_row(analytics)
            assert after_full is not None
            assert after_full["ProcFee"] == pytest.approx(150.00)
            assert str(after_full["DateComplete"]) in ("0001-01-01", "1-01-01")
        finally:
            analytics.dispose()

        # 2) TP → Complete without advancing DateTStamp
        source = ConnectionFactory.get_source_connection(test_settings)
        try:
            _upsert_mysql_procedure(source, complete_row)
            frozen = _fetch_mysql_proc_row(source)
            assert frozen is not None
            assert frozen["ProcStatus"] == PROC_STATUS_COMPLETE
            assert str(frozen["DateTStamp"]).startswith(
                str(complete_row["DateTStamp"].date())
            ), f"DateTStamp must stay frozen; got {frozen['DateTStamp']}"
        finally:
            source.dispose()

        time.sleep(0.2)

        # 3) Incremental extract — lookback must heal MySQL replication even though
        #    DateTStamp did not advance. (Analytics TINYINT→boolean adapter cannot
        #    represent OD ProcStatus=2 in this mini test DB; clinic raw uses integer.)
        replicator = SimpleMySQLReplicator(settings=test_settings)
        ok, _meta = replicator.copy_table("procedurelog", force_full=False)
        assert ok, "Incremental+lookback extract to replication failed"

        repl = ConnectionFactory.get_replication_connection(test_settings)
        try:
            repl_row = _fetch_mysql_proc_row(repl)
            assert repl_row is not None
            assert repl_row["ProcStatus"] == PROC_STATUS_COMPLETE, (
                f"Replication should pick up Complete via lookback; got {repl_row}"
            )
            assert repl_row["ProcFee"] == pytest.approx(160.00)
            assert str(repl_row["DateComplete"]) == str(complete_row["DateComplete"])
            assert "Completed via lookback" in (repl_row["ProcNote"] or "")
        finally:
            repl.dispose()

        logger.info(
            "ETL-FND-001 lookback e2e passed: ProcNum=%s TP→Complete with frozen DateTStamp",
            PROC_NUM,
        )
