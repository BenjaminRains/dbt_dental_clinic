"""Tests for mdc airflow commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


def test_airflow_help():
    result = runner.invoke(app, ["airflow", "--help"])
    assert result.exit_code == 0
    assert "init" in result.stdout
    assert "start" in result.stdout
    assert "logs" in result.stdout


@patch("mdc_cli.commands.airflow.run_airflow_init", return_value=0)
def test_airflow_init(mock_init):
    result = runner.invoke(app, ["airflow", "init"])
    assert result.exit_code == 0
    mock_init.assert_called_once()


@patch("mdc_cli.commands.airflow.run_airflow_docker_init", return_value=0)
def test_airflow_init_docker(mock_init):
    result = runner.invoke(app, ["airflow", "init", "--docker"])
    assert result.exit_code == 0
    mock_init.assert_called_once()


@patch(
    "mdc_cli.commands.airflow.run_airflow_init",
    side_effect=RuntimeError("Windows-only"),
)
def test_airflow_init_non_windows_message(mock_init):
    result = runner.invoke(app, ["airflow", "init"])
    assert result.exit_code == 2
    assert "Windows-only" in result.stderr
    mock_init.assert_called_once()


@patch("mdc_cli.commands.airflow.run_airflow_start", return_value=0)
def test_airflow_start_scheduler(mock_start):
    result = runner.invoke(app, ["airflow", "start", "--scheduler"])
    assert result.exit_code == 0
    mock_start.assert_called_once_with(
        scheduler=True, dag_processor=False, api_server=False
    )


@patch("mdc_cli.commands.airflow.run_airflow_start", return_value=0)
def test_airflow_start_dag_processor(mock_start):
    result = runner.invoke(app, ["airflow", "start", "--dag-processor"])
    assert result.exit_code == 0
    mock_start.assert_called_once_with(
        scheduler=False, dag_processor=True, api_server=False
    )


@patch("mdc_cli.commands.airflow.run_airflow_start", return_value=0)
def test_airflow_start_api_server(mock_start):
    result = runner.invoke(app, ["airflow", "start", "--api-server"])
    assert result.exit_code == 0
    mock_start.assert_called_once_with(
        scheduler=False, dag_processor=False, api_server=True
    )


@patch("mdc_cli.commands.airflow.run_airflow_start", return_value=0)
def test_airflow_start_webserver_alias(mock_start):
    result = runner.invoke(app, ["airflow", "start", "--webserver"])
    assert result.exit_code == 0
    mock_start.assert_called_once_with(
        scheduler=False, dag_processor=False, api_server=True
    )


def test_airflow_start_both_flags_rejected():
    result = runner.invoke(app, ["airflow", "start", "--scheduler", "--api-server"])
    assert result.exit_code == 2
    assert "separate terminals" in result.stderr


@patch("mdc_cli.commands.airflow.run_airflow_logs", return_value=0)
def test_airflow_logs_defaults(mock_logs):
    result = runner.invoke(app, ["airflow", "logs"])
    assert result.exit_code == 0
    mock_logs.assert_called_once_with(
        dag_id="etl_pipeline",
        run_id="",
        task="",
        limit=8,
        tail=0,
    )


@patch("mdc_cli.commands.airflow.run_airflow_logs", return_value=0)
def test_airflow_logs_with_options(mock_logs):
    result = runner.invoke(
        app,
        [
            "airflow",
            "logs",
            "--dag-id",
            "schema_analysis",
            "--run-id",
            "manual__2026-06-20",
            "--task",
            "refresh_schema_configuration",
            "--tail",
            "30",
        ],
    )
    assert result.exit_code == 0
    mock_logs.assert_called_once_with(
        dag_id="schema_analysis",
        run_id="manual__2026-06-20",
        task="refresh_schema_configuration",
        limit=8,
        tail=30,
    )
