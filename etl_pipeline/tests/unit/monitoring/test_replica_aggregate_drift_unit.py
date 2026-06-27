"""Unit tests for Layer 0 replica aggregate drift checks."""

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from etl_pipeline.monitoring.replica_aggregate_drift import (
    load_check_definitions,
    run_replica_drift_check,
    run_replica_drift_checks,
)


@pytest.fixture
def sample_config(tmp_path: Path) -> Path:
    payload = {
        "checks": [
            {
                "id": "L0-PAY-001",
                "tier": "A",
                "table": "payment",
                "enabled": True,
                "description": "payments",
                "lookback_days": 30,
                "amount_tolerance": 0.01,
                "source": {
                    "where": "PayDate >= DATE_SUB(CURDATE(), INTERVAL :lookback_days DAY)",
                    "amounts": {"pay_amount": "COALESCE(ROUND(SUM(PayAmt), 2), 0)"},
                },
                "raw": {
                    "where": '"PayDate"::date >= CURRENT_DATE - make_interval(days => :lookback_days)',
                    "amounts": {
                        "pay_amount": 'COALESCE(ROUND(SUM("PayAmt")::numeric, 2), 0)'
                    },
                },
            },
            {
                "id": "L0-PROC-001",
                "tier": "A",
                "table": "procedurelog",
                "enabled": True,
                "delegate": "procedurelog",
                "description": "production",
                "lookback_days": 30,
                "amount_tolerance": 0.01,
            },
            {
                "id": "L0-DISABLED",
                "tier": "A",
                "table": "patient",
                "enabled": False,
                "description": "disabled",
                "lookback_days": 30,
                "amount_tolerance": 0.01,
            },
        ]
    }
    path = tmp_path / "replica_drift_checks.yml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def _mock_engine(row_count: int, amounts: dict[str, str]):
    engine = MagicMock()
    conn = MagicMock()
    row = MagicMock()
    row.row_count = row_count
    for name, value in amounts.items():
        setattr(row, name, value)
    conn.execute.return_value.one.return_value = row
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = None
    engine.connect.return_value = ctx
    return engine


@pytest.mark.unit
class TestLoadCheckDefinitions:
    def test_load_tier_a_excludes_disabled(self, sample_config: Path):
        checks = load_check_definitions(sample_config, tier="A")
        ids = {check.check_id for check in checks}
        assert ids == {"L0-PAY-001", "L0-PROC-001"}

    def test_load_specific_check(self, sample_config: Path):
        checks = load_check_definitions(sample_config, check_ids=["L0-PAY-001"])
        assert len(checks) == 1
        assert checks[0].table == "payment"


@pytest.mark.unit
class TestReplicaAggregateDrift:
    def test_payment_check_in_sync(self, sample_config: Path):
        source = _mock_engine(100, {"pay_amount": "5000.00"})
        raw = _mock_engine(100, {"pay_amount": "5000.00"})
        definition = load_check_definitions(sample_config, check_ids=["L0-PAY-001"])[0]

        result = run_replica_drift_check(
            definition,
            source,
            raw,
            source_database="opendental",
        )

        assert result.drift_detected is False
        assert result.row_delta == 0
        assert result.amount_deltas["pay_amount"] == Decimal("0")

    def test_payment_check_detects_drift(self, sample_config: Path):
        source = _mock_engine(100, {"pay_amount": "5000.00"})
        raw = _mock_engine(95, {"pay_amount": "4800.00"})
        definition = load_check_definitions(sample_config, check_ids=["L0-PAY-001"])[0]

        result = run_replica_drift_check(
            definition,
            source,
            raw,
            source_database="opendental",
        )

        assert result.drift_detected is True
        assert result.row_delta == 5
        assert "drift detected" in result.message

    def test_procedurelog_delegate(self, sample_config: Path, monkeypatch):
        from etl_pipeline.monitoring import procedurelog_drift as proc_mod

        delegate_result = proc_mod.ProcedurelogDriftResult(
            lookback_days=30,
            source=proc_mod.CompleteProductionTotals(140, Decimal("15239.00")),
            raw=proc_mod.CompleteProductionTotals(140, Decimal("15239.00")),
            row_delta=0,
            fee_delta=Decimal("0"),
            drift_detected=False,
            message="procedurelog in sync",
        )
        monkeypatch.setattr(
            proc_mod,
            "check_procedurelog_drift",
            lambda *args, **kwargs: delegate_result,
        )

        definition = load_check_definitions(sample_config, check_ids=["L0-PROC-001"])[0]
        result = run_replica_drift_check(
            definition,
            MagicMock(),
            MagicMock(),
            source_database="opendental",
        )

        assert result.check_id == "L0-PROC-001"
        assert result.drift_detected is False

    def test_run_summary_counts_failures(self, sample_config: Path):
        source = _mock_engine(10, {"pay_amount": "100.00"})
        raw = _mock_engine(8, {"pay_amount": "80.00"})

        summary = run_replica_drift_checks(
            source,
            raw,
            source_database="opendental",
            config_path=sample_config,
            check_ids=["L0-PAY-001"],
        )

        assert summary.checks_run == 1
        assert summary.checks_failed == 1
        assert summary.any_drift_detected is True

    def test_bundled_config_has_tier_a_checks(self):
        checks = load_check_definitions(tier="A")
        ids = {check.check_id for check in checks}
        assert ids == {"L0-PROC-001", "L0-PAY-001", "L0-PAY-002", "L0-CLM-001", "L0-CLAIM-001", "L0-ADJ-001"}
