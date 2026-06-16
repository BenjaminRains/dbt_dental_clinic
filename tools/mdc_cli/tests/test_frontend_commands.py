"""Tests for frontend commands (Phase 5.3)."""

from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


def test_frontend_dev_help():
    result = runner.invoke(app, ["frontend", "dev", "--help"])
    assert result.exit_code == 0
    assert "Vite" in result.output


@patch("mdc_cli.commands.frontend._run_forever", return_value=0)
@patch("mdc_cli.commands.frontend.read_local_api_key_from_pem", return_value="key123")
def test_frontend_dev_runs_npm(mock_key, mock_run):
    result = runner.invoke(app, ["frontend", "dev"])
    assert result.exit_code == 0
    mock_run.assert_called_once()
    assert mock_run.call_args.args[0] == ["npm", "run", "dev"]


@patch("mdc_cli.commands.deploy.deploy_frontend_target", return_value=0)
def test_deploy_frontend_clinic_target(mock_deploy):
    result = runner.invoke(app, ["deploy", "frontend", "--target", "clinic"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("clinic")


@patch("mdc_cli.commands.deploy.deploy_frontend_target", return_value=0)
def test_deploy_frontend_default_demo(mock_deploy):
    result = runner.invoke(app, ["deploy", "frontend"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("demo")


def test_deploy_frontend_invalid_target():
    result = runner.invoke(app, ["deploy", "frontend", "--target", "local"])
    assert result.exit_code == 2
