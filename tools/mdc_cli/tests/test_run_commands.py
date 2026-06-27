"""Tests for Phase 4.3 runtime commands."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


@patch("mdc_cli.commands.api.run_isolated", return_value=0)
@patch("mdc_cli.commands.api.require_component_python")
@patch("mdc_cli.commands.api.load_env_dict_isolated")
@patch("mdc_cli.commands.api.validate_api_stage", return_value=(True, None))
def test_api_run_local_defaults_reload(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    mock_load.return_value = {"API_HOST": "127.0.0.1", "API_PORT": "9000"}
    mock_python.return_value = Path("api/venv/Scripts/python.exe")

    result = runner.invoke(app, ["api", "run", "--env", "local"])

    assert result.exit_code == 0
    cmd = mock_run.call_args.kwargs["cmd"]
    assert "--reload" in cmd
    assert "--port" in cmd
    assert "9000" in cmd


@patch("mdc_cli.commands.api.run_isolated", return_value=0)
@patch("mdc_cli.commands.api.require_component_python")
@patch("mdc_cli.commands.api.load_env_dict_isolated")
@patch("mdc_cli.commands.api.validate_api_stage", return_value=(True, None))
def test_api_run_clinic_no_reload_by_default(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    mock_load.return_value = {}
    mock_python.return_value = Path("api/venv/Scripts/python.exe")

    result = runner.invoke(app, ["api", "run", "--env", "clinic"])

    assert result.exit_code == 0
    cmd = mock_run.call_args.kwargs["cmd"]
    assert "--reload" not in cmd


@patch("mdc_cli.commands.api.run_isolated", return_value=0)
@patch("mdc_cli.commands.api.require_component_python")
@patch("mdc_cli.commands.api.load_env_dict_isolated")
@patch("mdc_cli.commands.api.validate_api_stage", return_value=(True, None))
def test_api_run_clinic_tunnel_db_overrides_postgres(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    mock_load.return_value = {
        "POSTGRES_ANALYTICS_HOST": "dental-clinic-analytics.example.rds.amazonaws.com",
        "POSTGRES_ANALYTICS_PORT": "5432",
    }
    mock_python.return_value = Path("api/venv/Scripts/python.exe")

    result = runner.invoke(
        app,
        ["api", "run", "--env", "clinic", "--tunnel-db", "--port", "8001"],
    )

    assert result.exit_code == 0
    settings = mock_run.call_args.kwargs["settings"]
    assert settings["POSTGRES_ANALYTICS_HOST"] == "127.0.0.1"
    assert settings["POSTGRES_ANALYTICS_PORT"] == "5433"


@patch("mdc_cli.commands.etl.run_isolated", return_value=0)
@patch("mdc_cli.commands.etl.require_component_python")
@patch("mdc_cli.commands.etl.load_env_dict_isolated")
@patch("mdc_cli.commands.etl.validate_etl_stage", return_value=(True, None))
def test_etl_run_passes_passthrough_args(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    mock_load.return_value = {}
    mock_python.return_value = Path("etl/venv/Scripts/python.exe")

    result = runner.invoke(
        app,
        ["etl", "run", "--env", "clinic", "--profile", "full", "--", "--tables", "patient"],
    )

    assert result.exit_code == 0
    cmd = mock_run.call_args.kwargs["cmd"]
    assert "run" in cmd
    assert "--tables" in cmd
    assert "patient" in cmd


@patch("mdc_cli.commands.dbt.run_dbt_command", return_value=0)
@patch("mdc_cli.commands.dbt.validate_dbt_stage", return_value=(True, None))
def test_dbt_run_delegates(mock_validate, mock_run):
    result = runner.invoke(app, ["dbt", "run", "--env", "local"])
    assert result.exit_code == 0
    mock_run.assert_called_once_with("local", ["run"], tunnel_db=False, tunnel_port=None)


@patch("mdc_cli.commands.dbt.run_dbt_command", return_value=0)
@patch("mdc_cli.commands.dbt.validate_dbt_stage", return_value=(True, None))
def test_dbt_run_clinic_tunnel_db(mock_validate, mock_run):
    result = runner.invoke(
        app,
        ["dbt", "run", "--env", "clinic", "--tunnel-db", "--tunnel-port", "5433", "--", "--select", "mart_daily_payments"],
    )
    assert result.exit_code == 0
    mock_run.assert_called_once_with(
        "clinic",
        ["run", "--select", "mart_daily_payments"],
        tunnel_db=True,
        tunnel_port=5433,
    )


@patch("mdc_cli.commands.dbt.run_dbt_command", return_value=0)
@patch("mdc_cli.commands.dbt.validate_dbt_stage", return_value=(True, None))
def test_dbt_invoke_passthrough(mock_validate, mock_run):
    result = runner.invoke(
        app,
        ["dbt", "invoke", "--env", "clinic", "--", "deps"],
    )
    assert result.exit_code == 0
    mock_run.assert_called_once_with("clinic", ["deps"], tunnel_db=False, tunnel_port=None)


def test_resolve_component_python_script_cmd():
    from pathlib import Path

    from mdc_cli.run_helper import resolve_component_python_script_cmd

    python = Path("etl/venv/Scripts/python.exe")
    assert resolve_component_python_script_cmd(
        python,
        ["python", "scripts/check_procedurelog_drift.py", "--warn-only"],
    ) == [
        str(python),
        "scripts/check_procedurelog_drift.py",
        "--warn-only",
    ]
    assert resolve_component_python_script_cmd(python, ["check-procedurelog-drift"]) is None


@patch("mdc_cli.commands.etl.run_isolated", return_value=0)
@patch("mdc_cli.commands.etl.require_component_python")
@patch("mdc_cli.commands.etl.load_env_dict_isolated")
@patch("mdc_cli.commands.etl.validate_etl_stage", return_value=(True, None))
def test_etl_invoke_cli_subcommand(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    from pathlib import Path

    mock_load.return_value = {}
    mock_python.return_value = Path("etl/venv/Scripts/python.exe")
    result = runner.invoke(
        app,
        ["etl", "invoke", "--env", "local", "--", "check-procedurelog-drift", "--warn-only"],
    )
    assert result.exit_code == 0
    cmd = mock_run.call_args.kwargs["cmd"]
    assert cmd[-2:] == ["check-procedurelog-drift", "--warn-only"]
    assert "-m" in cmd
    assert "etl_pipeline.cli.main" in cmd


@patch("mdc_cli.commands.etl.run_isolated", return_value=0)
@patch("mdc_cli.commands.etl.require_component_python")
@patch("mdc_cli.commands.etl.load_env_dict_isolated")
@patch("mdc_cli.commands.etl.validate_etl_stage", return_value=(True, None))
def test_etl_invoke_python_script(
    mock_validate,
    mock_load,
    mock_python,
    mock_run,
):
    from pathlib import Path

    mock_load.return_value = {}
    venv_python = Path("etl/venv/Scripts/python.exe")
    mock_python.return_value = venv_python
    result = runner.invoke(
        app,
        [
            "etl",
            "invoke",
            "--env",
            "local",
            "--",
            "python",
            "scripts/check_procedurelog_drift.py",
            "--warn-only",
        ],
    )
    assert result.exit_code == 0
    cmd = mock_run.call_args.kwargs["cmd"]
    assert cmd == [
        str(venv_python),
        "scripts/check_procedurelog_drift.py",
        "--warn-only",
    ]
    assert "etl_pipeline.cli.main" not in cmd
