"""Tests for analytics freshness probes."""

from datetime import datetime, timezone
from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.freshness import (
    FreshnessRow,
    _classify_probe,
    _business_date_age_hours,
    _probe_age_hours,
    collect_freshness_report,
    format_freshness_age,
    freshness_stages_for_status,
    probes_for_stage,
)
from mdc_cli.main import app

runner = CliRunner()


def test_freshness_stages_default_local_only():
    assert freshness_stages_for_status(None) == ["local"]
    assert freshness_stages_for_status("local") == ["local"]
    assert freshness_stages_for_status("clinic") == ["clinic"]
    assert freshness_stages_for_status("demo") == []


def test_classify_etl_warn_and_stale():
    assert _classify_probe("etl:payment", 30)[0] == "ok"
    assert _classify_probe("etl:payment", 40)[0] == "warn"
    assert _classify_probe("etl:payment", 80)[0] == "stale"


def test_classify_future_business_date():
    status, note = _classify_probe("business:latest_production_date", -240, is_future=True)
    assert status == "warn"
    assert "future" in note


def test_business_date_age_uses_calendar_days():
    now = datetime(2026, 6, 25, 15, 0, 0, tzinfo=timezone.utc)
    latest = datetime(2026, 6, 24, 0, 0, 0, tzinfo=timezone.utc)
    age, is_future = _business_date_age_hours(latest, now=now)
    assert is_future is False
    assert age == 24.0


def test_format_freshness_age_future():
    assert "ahead" in format_freshness_age(-48, is_future=True)


def test_clinic_probes_skip_etl():
    names = {p.name for p in probes_for_stage("clinic")}
    assert "etl:payment" not in names
    assert "mart:mart_daily_payments" in names


@patch("mdc_cli.freshness._run_all_probes")
@patch("mdc_cli.freshness.load_freshness_env_dict")
def test_collect_freshness_report_clinic_skips_etl_rows(mock_load_env, mock_run_probes):
    mock_load_env.return_value = (
        {
            "POSTGRES_ANALYTICS_HOST": "127.0.0.1",
            "POSTGRES_ANALYTICS_PORT": "5433",
            "POSTGRES_ANALYTICS_DB": "opendental_analytics",
            "POSTGRES_ANALYTICS_USER": "analytics_user",
            "POSTGRES_ANALYTICS_PASSWORD": "secret",
        },
        "secrets_manager:test",
        None,
    )
    mock_run_probes.return_value = (
        [("mart:mart_provider_performance", "2026-06-25 22:20:00", None)],
        [],
    )
    report = collect_freshness_report("clinic", tunnel_db=True, now=datetime(2026, 6, 25, 23, 0, 0, tzinfo=timezone.utc))
    etl_rows = [row for row in report.rows if row.probe.startswith("etl:")]
    assert len(etl_rows) == 3
    assert all(row.status == "n/a" for row in etl_rows)


@patch("mdc_cli.freshness._run_all_probes")
@patch("mdc_cli.freshness.load_freshness_env_dict")
def test_collect_freshness_report_parses_rows(mock_load_env, mock_run_probes):
    mock_load_env.return_value = (
        {
            "POSTGRES_ANALYTICS_HOST": "localhost",
            "POSTGRES_ANALYTICS_PORT": "5432",
            "POSTGRES_ANALYTICS_DB": "opendental_analytics",
            "POSTGRES_ANALYTICS_USER": "analytics_user",
            "POSTGRES_ANALYTICS_PASSWORD": "secret",
        },
        None,
        None,
    )
    mock_run_probes.return_value = (
        [
            ("etl:payment", "2026-06-24 10:00:00", None),
            ("mart:mart_provider_performance", "2026-06-20 08:00:00", None),
        ],
        [],
    )
    fixed_now = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)
    report = collect_freshness_report("local", now=fixed_now)
    assert report.error is None
    assert len(report.rows) == 2
    assert report.rows[0].probe == "etl:payment"
    assert report.rows[0].status == "ok"
    assert report.rows[1].status == "warn"


@patch("mdc_cli.commands.status.collect_freshness_report")
def test_status_includes_freshness_by_default(mock_collect):
    mock_collect.return_value = type(
        "R",
        (),
        {
            "stage": "local",
            "database_label": "opendental_analytics @ localhost:5432",
            "rows": [
                FreshnessRow(
                    probe="etl:payment",
                    latest_at=datetime(2026, 6, 24, tzinfo=timezone.utc),
                    age_hours=12,
                    status="ok",
                )
            ],
            "error": None,
            "hint": None,
            "password_source": None,
            "password_warning": None,
        },
    )()
    result = runner.invoke(app, ["status", "--env", "local"])
    assert result.exit_code == 0
    assert "Data freshness" in result.stdout
    assert "etl:payment" in result.stdout
    mock_collect.assert_called_once()


@patch("mdc_cli.commands.status.collect_freshness_report")
def test_status_no_freshness_flag(mock_collect):
    result = runner.invoke(app, ["status", "--env", "local", "--no-freshness"])
    assert result.exit_code == 0
    assert "Data freshness" not in result.stdout
    mock_collect.assert_not_called()
