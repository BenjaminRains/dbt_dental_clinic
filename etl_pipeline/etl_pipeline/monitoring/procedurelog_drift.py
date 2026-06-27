"""
ETL-FND-001: procedurelog replica row drift detection and lookback re-sync helpers.

Compares complete-production totals (ProcStatus=2 by DateComplete) between OpenDental
MySQL and analytics raw.procedurelog over a rolling business-date window.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

PROCEDURELOG_TABLE = "procedurelog"
PROCEDURELOG_LOOKBACK_DAYS = 30
DEFAULT_FEE_TOLERANCE = Decimal("0.01")


@dataclass(frozen=True)
class CompleteProductionTotals:
    complete_rows: int
    complete_fees: Decimal


@dataclass(frozen=True)
class ProcedurelogDriftResult:
    lookback_days: int
    source: CompleteProductionTotals
    raw: CompleteProductionTotals
    row_delta: int
    fee_delta: Decimal
    drift_detected: bool
    message: str


def mysql_lookback_resync_predicate(
    lookback_days: int = PROCEDURELOG_LOOKBACK_DAYS,
) -> str:
    """Rows to re-copy on each incremental run (MySQL replication DB syntax)."""
    return (
        f"(DateComplete >= DATE_SUB(CURDATE(), INTERVAL {lookback_days} DAY) "
        f"OR ProcDate >= DATE_SUB(CURDATE(), INTERVAL {lookback_days} DAY))"
    )


def wrap_mysql_incremental_with_lookback(
    where_clause: str,
    lookback_days: int = PROCEDURELOG_LOOKBACK_DAYS,
) -> str:
    """Union watermark incremental filter with business-date lookback (ETL-FND-001)."""
    lookback = mysql_lookback_resync_predicate(lookback_days)
    return f"({where_clause}) OR ({lookback})"


def is_procedurelog_table(table_name: str) -> bool:
    return table_name.lower() == PROCEDURELOG_TABLE


def _to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def fetch_mysql_complete_totals(
    engine: Engine,
    *,
    database: str,
    table: str = PROCEDURELOG_TABLE,
    lookback_days: int = PROCEDURELOG_LOOKBACK_DAYS,
) -> CompleteProductionTotals:
    query = text(
        f"""
        SELECT COUNT(*) AS complete_rows,
               COALESCE(ROUND(SUM(ProcFee), 2), 0) AS complete_fees
        FROM `{database}`.`{table}`
        WHERE ProcStatus = 2
          AND DateComplete >= DATE_SUB(CURDATE(), INTERVAL :lookback_days DAY)
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"lookback_days": lookback_days}).one()
    return CompleteProductionTotals(
        complete_rows=int(row.complete_rows),
        complete_fees=_to_decimal(row.complete_fees),
    )


def fetch_postgres_raw_complete_totals(
    engine: Engine,
    *,
    table: str = PROCEDURELOG_TABLE,
    lookback_days: int = PROCEDURELOG_LOOKBACK_DAYS,
) -> CompleteProductionTotals:
    query = text(
        f"""
        SELECT COUNT(*) AS complete_rows,
               COALESCE(ROUND(SUM("ProcFee")::numeric, 2), 0) AS complete_fees
        FROM raw.{table}
        WHERE "ProcStatus" = 2
          AND "DateComplete"::date >= CURRENT_DATE - make_interval(days => :lookback_days)
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"lookback_days": lookback_days}).one()
    return CompleteProductionTotals(
        complete_rows=int(row.complete_rows),
        complete_fees=_to_decimal(row.complete_fees),
    )


def check_procedurelog_drift(
    source_engine: Engine,
    analytics_engine: Engine,
    *,
    source_database: str,
    lookback_days: int = PROCEDURELOG_LOOKBACK_DAYS,
    fee_tolerance: Decimal = DEFAULT_FEE_TOLERANCE,
) -> ProcedurelogDriftResult:
    """Compare MySQL source vs raw.procedurelog complete-production totals."""
    source = fetch_mysql_complete_totals(
        source_engine,
        database=source_database,
        lookback_days=lookback_days,
    )
    raw = fetch_postgres_raw_complete_totals(
        analytics_engine,
        lookback_days=lookback_days,
    )

    row_delta = source.complete_rows - raw.complete_rows
    fee_delta = source.complete_fees - raw.complete_fees
    drift_detected = row_delta != 0 or abs(fee_delta) > fee_tolerance

    if drift_detected:
        message = (
            f"procedurelog drift detected ({lookback_days}-day window): "
            f"source {source.complete_rows} rows / ${source.complete_fees} vs "
            f"raw {raw.complete_rows} rows / ${raw.complete_fees} "
            f"(delta {row_delta} rows, ${fee_delta} fees)"
        )
    else:
        message = (
            f"procedurelog in sync ({lookback_days}-day window): "
            f"{source.complete_rows} complete rows, ${source.complete_fees}"
        )

    return ProcedurelogDriftResult(
        lookback_days=lookback_days,
        source=source,
        raw=raw,
        row_delta=row_delta,
        fee_delta=fee_delta,
        drift_detected=drift_detected,
        message=message,
    )
