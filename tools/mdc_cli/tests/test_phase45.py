"""Tests for Phase 4.5 tunnel/deploy wrappers."""

from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


@patch("mdc_cli.commands.tunnel.tunnel_clinic_db", return_value=0)
def test_tunnel_clinic_db(mock_tunnel):
    result = runner.invoke(app, ["tunnel", "clinic-db"])
    assert result.exit_code == 0
    mock_tunnel.assert_called_once()


@patch("mdc_cli.commands.tunnel.tunnel_rds_demo", return_value=0)
def test_tunnel_close_is_informational(mock_rds):
    result = runner.invoke(app, ["tunnel", "close"])
    assert result.exit_code == 0
    mock_rds.assert_not_called()


@patch("mdc_cli.commands.deploy.deploy_frontend_target", return_value=0)
def test_deploy_frontend(mock_deploy):
    result = runner.invoke(app, ["deploy", "frontend"])
    assert result.exit_code == 0
    mock_deploy.assert_called_once_with("demo")


@patch("mdc_cli.commands.etl.run_isolated", return_value=0)
@patch("mdc_cli.commands.etl.require_component_python")
@patch("mdc_cli.commands.etl.load_env_dict_isolated")
@patch("mdc_cli.commands.etl.validate_etl_stage", return_value=(True, None))
def test_etl_status_command(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    from pathlib import Path

    mock_load.return_value = {}
    mock_python.return_value = Path("etl/venv/Scripts/python.exe")
    result = runner.invoke(app, ["etl", "status", "--env", "clinic"])
    assert result.exit_code == 0
    cmd = mock_run.call_args.kwargs["cmd"]
    assert "status" in cmd
    assert "--config" in cmd
    assert "etl_pipeline/config/pipeline.yml" in cmd
