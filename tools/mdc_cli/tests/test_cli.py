"""CLI smoke tests."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli import __version__
from mdc_cli.main import app

runner = CliRunner()


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "status" in result.stdout
    assert "api" in result.stdout


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert f"mdc {__version__}" in result.stdout


def test_status_runs():
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "mdc status" in result.stdout
    assert "api" in result.stdout
    assert "etl" in result.stdout


def test_status_env_filter():
    result = runner.invoke(app, ["status", "--env", "local"])
    assert result.exit_code == 0
    assert "local" in result.stdout
    assert "clinic" not in result.stdout.split("Config")[0]


def test_status_invalid_env():
    result = runner.invoke(app, ["status", "--env", "not-a-stage"])
    assert result.exit_code != 0
    combined = result.stdout + result.stderr
    assert "Unsupported stage" in combined


@patch("mdc_cli.commands.api.run_isolated", return_value=0)
@patch("mdc_cli.commands.api.require_component_python")
@patch("mdc_cli.commands.api.load_env_dict_isolated")
@patch("mdc_cli.commands.api.validate_api_stage", return_value=(True, None))
def test_api_run_command_registered(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    mock_load.return_value = {"CLINIC_API_KEY": "test-key"}
    mock_python.return_value = Path("api/venv/Scripts/python.exe")
    result = runner.invoke(app, ["api", "run", "--env", "local"])
    assert result.exit_code == 0
    mock_run.assert_called_once()
