"""
Layer 0 replica aggregate drift checks (Phase 3).

Compares business-meaningful MySQL source totals vs raw.* over a rolling window.
Config: etl_pipeline/config/replica_drift_checks.yml
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "replica_drift_checks.yml"
DEFAULT_AMOUNT_TOLERANCE = Decimal("0.01")


@dataclass(frozen=True)
class CheckDefinition:
    check_id: str
    tier: str
    table: str
    description: str
    lookback_days: int
    amount_tolerance: Decimal
    enabled: bool = True
    delegate: Optional[str] = None
    source_where: Optional[str] = None
    raw_where: Optional[str] = None
    source_amounts: Dict[str, str] = field(default_factory=dict)
    raw_amounts: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AggregateSnapshot:
    row_count: int
    amounts: Dict[str, Decimal]


@dataclass(frozen=True)
class ReplicaDriftCheckResult:
    check_id: str
    tier: str
    table: str
    description: str
    lookback_days: int
    source: AggregateSnapshot
    raw: AggregateSnapshot
    row_delta: int
    amount_deltas: Dict[str, Decimal]
    drift_detected: bool
    message: str


@dataclass(frozen=True)
class ReplicaDriftRunSummary:
    results: List[ReplicaDriftCheckResult]
    checks_run: int
    checks_passed: int
    checks_failed: int
    any_drift_detected: bool

    @property
    def all_passed(self) -> bool:
        return not self.any_drift_detected


def _to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def default_config_path() -> Path:
    return DEFAULT_CONFIG_PATH


def load_check_definitions(
    config_path: Optional[Path] = None,
    *,
    tier: Optional[str] = None,
    check_ids: Optional[Sequence[str]] = None,
    enabled_only: bool = True,
) -> List[CheckDefinition]:
    path = config_path or default_config_path()
    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    definitions: List[CheckDefinition] = []
    for entry in payload.get("checks", []):
        if enabled_only and not entry.get("enabled", True):
            continue
        if tier is not None and str(entry.get("tier", "")).upper() != tier.upper():
            continue
        check_id = entry["id"]
        if check_ids is not None and check_id not in check_ids:
            continue

        source = entry.get("source") or {}
        raw = entry.get("raw") or {}
        definitions.append(
            CheckDefinition(
                check_id=check_id,
                tier=str(entry.get("tier", "A")),
                table=entry["table"],
                description=entry.get("description", ""),
                lookback_days=int(entry.get("lookback_days", 30)),
                amount_tolerance=Decimal(str(entry.get("amount_tolerance", DEFAULT_AMOUNT_TOLERANCE))),
                enabled=bool(entry.get("enabled", True)),
                delegate=entry.get("delegate"),
                source_where=source.get("where"),
                raw_where=raw.get("where"),
                source_amounts=dict(source.get("amounts") or {}),
                raw_amounts=dict(raw.get("amounts") or {}),
            )
        )
    return definitions


def _build_aggregate_query(
    *,
    table: str,
    where_clause: str,
    amount_exprs: Dict[str, str],
    source_database: Optional[str] = None,
) -> str:
    amount_sql = ", ".join(f"{expr} AS {name}" for name, expr in amount_exprs.items())
    if source_database:
        from_clause = f"`{source_database}`.`{table}`"
    else:
        from_clause = f"raw.{table}"
    return f"""
        SELECT COUNT(*) AS row_count
             , {amount_sql}
        FROM {from_clause}
        WHERE {where_clause}
    """


def _fetch_aggregate_snapshot(
    engine: Engine,
    *,
    table: str,
    where_clause: str,
    amount_exprs: Dict[str, str],
    lookback_days: int,
    source_database: Optional[str] = None,
) -> AggregateSnapshot:
    query = text(
        _build_aggregate_query(
            table=table,
            where_clause=where_clause,
            amount_exprs=amount_exprs,
            source_database=source_database,
        )
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"lookback_days": lookback_days}).one()

    amounts = {
        name: _to_decimal(getattr(row, name))
        for name in amount_exprs
    }
    return AggregateSnapshot(row_count=int(row.row_count), amounts=amounts)


def _run_procedurelog_delegate(
    definition: CheckDefinition,
    source_engine: Engine,
    analytics_engine: Engine,
    *,
    source_database: str,
    lookback_days: Optional[int] = None,
    fee_tolerance: Optional[Decimal] = None,
) -> ReplicaDriftCheckResult:
    from etl_pipeline.monitoring.procedurelog_drift import check_procedurelog_drift

    window = lookback_days if lookback_days is not None else definition.lookback_days
    tolerance = fee_tolerance if fee_tolerance is not None else definition.amount_tolerance
    proc_result = check_procedurelog_drift(
        source_engine,
        analytics_engine,
        source_database=source_database,
        lookback_days=window,
        fee_tolerance=tolerance,
    )
    source = AggregateSnapshot(
        row_count=proc_result.source.complete_rows,
        amounts={"complete_fees": proc_result.source.complete_fees},
    )
    raw = AggregateSnapshot(
        row_count=proc_result.raw.complete_rows,
        amounts={"complete_fees": proc_result.raw.complete_fees},
    )
    amount_deltas = {
        "complete_fees": proc_result.fee_delta,
    }
    return ReplicaDriftCheckResult(
        check_id=definition.check_id,
        tier=definition.tier,
        table=definition.table,
        description=definition.description,
        lookback_days=window,
        source=source,
        raw=raw,
        row_delta=proc_result.row_delta,
        amount_deltas=amount_deltas,
        drift_detected=proc_result.drift_detected,
        message=proc_result.message,
    )


def run_replica_drift_check(
    definition: CheckDefinition,
    source_engine: Engine,
    analytics_engine: Engine,
    *,
    source_database: str,
    lookback_days: Optional[int] = None,
) -> ReplicaDriftCheckResult:
    window = lookback_days if lookback_days is not None else definition.lookback_days

    if definition.delegate == "procedurelog":
        return _run_procedurelog_delegate(
            definition,
            source_engine,
            analytics_engine,
            source_database=source_database,
            lookback_days=window,
        )

    if not definition.source_where or not definition.raw_where:
        raise ValueError(f"{definition.check_id} missing source/raw query config")
    if definition.source_amounts.keys() != definition.raw_amounts.keys():
        raise ValueError(f"{definition.check_id} source/raw amount keys must match")

    source = _fetch_aggregate_snapshot(
        source_engine,
        table=definition.table,
        where_clause=definition.source_where,
        amount_exprs=definition.source_amounts,
        lookback_days=window,
        source_database=source_database,
    )
    raw = _fetch_aggregate_snapshot(
        analytics_engine,
        table=definition.table,
        where_clause=definition.raw_where,
        amount_exprs=definition.raw_amounts,
        lookback_days=window,
    )

    row_delta = source.row_count - raw.row_count
    amount_deltas = {
        name: source.amounts[name] - raw.amounts[name]
        for name in definition.source_amounts
    }
    drift_detected = row_delta != 0 or any(
        abs(delta) > definition.amount_tolerance for delta in amount_deltas.values()
    )

    amount_summary = ", ".join(
        f"{name} source=${source.amounts[name]} raw=${raw.amounts[name]}"
        for name in definition.source_amounts
    )
    if drift_detected:
        message = (
            f"{definition.check_id} drift detected ({window}-day window): "
            f"source {source.row_count} rows vs raw {raw.row_count} rows "
            f"(delta {row_delta}); {amount_summary}"
        )
    else:
        message = (
            f"{definition.check_id} in sync ({window}-day window): "
            f"{source.row_count} rows; {amount_summary}"
        )

    return ReplicaDriftCheckResult(
        check_id=definition.check_id,
        tier=definition.tier,
        table=definition.table,
        description=definition.description,
        lookback_days=window,
        source=source,
        raw=raw,
        row_delta=row_delta,
        amount_deltas=amount_deltas,
        drift_detected=drift_detected,
        message=message,
    )


def run_replica_drift_checks(
    source_engine: Engine,
    analytics_engine: Engine,
    *,
    source_database: str,
    config_path: Optional[Path] = None,
    tier: Optional[str] = None,
    check_ids: Optional[Sequence[str]] = None,
    lookback_days: Optional[int] = None,
) -> ReplicaDriftRunSummary:
    definitions = load_check_definitions(
        config_path,
        tier=tier,
        check_ids=check_ids,
    )
    results: List[ReplicaDriftCheckResult] = []
    for definition in definitions:
        results.append(
            run_replica_drift_check(
                definition,
                source_engine,
                analytics_engine,
                source_database=source_database,
                lookback_days=lookback_days,
            )
        )

    failed = sum(1 for result in results if result.drift_detected)
    return ReplicaDriftRunSummary(
        results=results,
        checks_run=len(results),
        checks_passed=len(results) - failed,
        checks_failed=failed,
        any_drift_detected=failed > 0,
    )


def summary_to_airflow_payload(summary: ReplicaDriftRunSummary) -> Dict[str, Any]:
    return {
        "replica_drift_detected": summary.any_drift_detected,
        "checks_run": summary.checks_run,
        "checks_passed": summary.checks_passed,
        "checks_failed": summary.checks_failed,
        "checks": [
            {
                "check_id": result.check_id,
                "table": result.table,
                "drift_detected": result.drift_detected,
                "row_delta": result.row_delta,
                "amount_deltas": {
                    name: float(delta) for name, delta in result.amount_deltas.items()
                },
            }
            for result in summary.results
        ],
    }
