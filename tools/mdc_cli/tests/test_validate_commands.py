"""Tests for Phase 4.2 validation commands."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


@patch("mdc_cli.commands.api.validate_api_stage", return_value=(True, None))
def test_api_test_config_success(mock_validate):
    result = runner.invoke(app, ["api", "test-config", "--env", "local"])
    assert result.exit_code == 0
    assert "API" in result.stdout
    assert "local" in result.stdout
    assert "ok" in result.stdout
    mock_validate.assert_called_once_with("local")


@patch("mdc_cli.commands.api.validate_api_stage", return_value=(False, "missing POSTGRES_ANALYTICS_HOST"))
def test_api_test_config_failure(mock_validate):
    result = runner.invoke(app, ["api", "test-config", "--env", "clinic"])
    assert result.exit_code == 1
    combined = result.stdout + result.stderr
    assert "fail" in combined
    assert "POSTGRES_ANALYTICS_HOST" in combined


@patch("mdc_cli.commands.api.validate_api_stage", return_value=(True, None))
def test_api_health_config_only(mock_validate):
    result = runner.invoke(app, ["api", "health", "--env", "local"])
    assert result.exit_code == 0
    assert "healthy" in result.stdout
    mock_validate.assert_called_once_with("local")


@patch("mdc_cli.commands.etl.validate_etl_stage", return_value=(True, None))
def test_etl_validate_load_profile(mock_validate):
    result = runner.invoke(app, ["etl", "validate", "--env", "local"])
    assert result.exit_code == 0
    assert "profile=load" in result.stdout
    mock_validate.assert_called_once_with(
        "local", profile="load", tunnel_db=False, tunnel_port=None
    )


@patch("mdc_cli.commands.etl.validate_etl_stage", return_value=(True, None))
def test_etl_validate_full_profile(mock_validate):
    result = runner.invoke(app, ["etl", "validate", "--env", "clinic", "--profile", "full"])
    assert result.exit_code == 0
    mock_validate.assert_called_once_with(
        "clinic", profile="full", tunnel_db=False, tunnel_port=None
    )


@patch("mdc_cli.commands.dbt.validate_dbt_stage", return_value=(True, None))
@patch("mdc_cli.commands.dbt.dbt_config_path")
def test_dbt_validate_success(mock_config_path, mock_validate):
    mock_config_path.return_value = Path("dbt_dental_models/.env_local")
    result = runner.invoke(app, ["dbt", "validate", "--env", "local"])
    assert result.exit_code == 0
    assert "DBT" in result.stdout
    assert "ok" in result.stdout
    mock_validate.assert_called_once_with("local")


def test_api_test_config_invalid_stage():
    result = runner.invoke(app, ["api", "test-config", "--env", "demo-only"])
    assert result.exit_code != 0
