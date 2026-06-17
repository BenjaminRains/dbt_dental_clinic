"""Tests for mdc publish commands."""

from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app

runner = CliRunner()


@patch("mdc_cli.commands.publish.invoke_ps_script_file", return_value=0)
@patch("mdc_cli.commands.publish.is_local_tcp_port_open", return_value=True)
def test_publish_analytics_tunnel_ok(mock_port_open, mock_invoke):
    result = runner.invoke(app, ["publish", "analytics", "--env", "clinic"])
    assert result.exit_code == 0
    mock_port_open.assert_called_once_with("127.0.0.1", 5433)
    mock_invoke.assert_called_once()


@patch("mdc_cli.commands.publish.invoke_ps_script_file", return_value=0)
@patch("mdc_cli.commands.publish.is_local_tcp_port_open", return_value=False)
def test_publish_analytics_tunnel_missing_exits_early(mock_port_open, mock_invoke):
    result = runner.invoke(app, ["publish", "analytics", "--env", "clinic"])
    assert result.exit_code == 1
    assert "mdc tunnel clinic-db" in result.output
    assert "CLINIC_ANALYTICS_WORKFLOW.md" in result.output
    mock_invoke.assert_not_called()


@patch("mdc_cli.commands.publish.invoke_ps_script_file", return_value=0)
@patch("mdc_cli.commands.publish.is_local_tcp_port_open", return_value=False)
def test_publish_analytics_skip_tunnel_check(mock_port_open, mock_invoke):
    result = runner.invoke(
        app,
        ["publish", "analytics", "--env", "clinic", "--skip-tunnel-check"],
    )
    assert result.exit_code == 0
    mock_port_open.assert_not_called()
    mock_invoke.assert_called_once()


@patch("mdc_cli.commands.publish.invoke_ps_script_file", return_value=0)
def test_publish_analytics_direct_rds_skips_tunnel_check(mock_invoke):
    result = runner.invoke(
        app,
        ["publish", "analytics", "--env", "clinic", "--use-direct-rds"],
    )
    assert result.exit_code == 0
    mock_invoke.assert_called_once()
    args = mock_invoke.call_args[0][1]
    assert "-UseDirectRds" in args
