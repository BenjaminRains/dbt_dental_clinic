"""Tests for background clinic DB SSM tunnel helpers."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from mdc_cli.ssm import (
    ClinicDbTunnelError,
    _stop_port_forward_process,
    _windows_descendant_pids,
    managed_clinic_db_tunnel,
    wait_for_local_port,
)


def test_wait_for_local_port_eventually_true():
    with patch("mdc_cli.ssm.is_local_tcp_port_open", side_effect=[False, False, True]):
        assert wait_for_local_port("127.0.0.1", 5433, timeout_seconds=5, poll_interval=0.01) is True


def test_wait_for_local_port_timeout():
    with patch("mdc_cli.ssm.is_local_tcp_port_open", return_value=False):
        assert wait_for_local_port("127.0.0.1", 5433, timeout_seconds=0.05, poll_interval=0.01) is False


@patch("mdc_cli.ssm.is_local_tcp_port_open", return_value=True)
def test_managed_tunnel_reuses_existing(mock_open):
    with managed_clinic_db_tunnel(local_port="5433") as source:
        assert source == "existing"
    mock_open.assert_called_once()


@patch("mdc_cli.ssm._stop_port_forward_process")
@patch("mdc_cli.ssm._start_port_forward_background")
@patch("mdc_cli.ssm.wait_for_local_port", return_value=True)
@patch("mdc_cli.ssm.is_local_tcp_port_open", return_value=False)
@patch("mdc_cli.ssm.load_ssm_context")
def test_managed_tunnel_starts_background(
    mock_ctx,
    mock_open,
    mock_wait,
    mock_start,
    mock_stop,
):
    ctx = MagicMock()
    ctx.clinic_api_instance_id = "i-abc"
    ctx.rds_endpoint = "rds.example.com"
    mock_ctx.return_value = ctx
    proc = MagicMock()
    proc.poll.return_value = None
    mock_start.return_value = proc

    with managed_clinic_db_tunnel(local_port="5433", wait_timeout_seconds=10) as source:
        assert source == "started"

    mock_start.assert_called_once()
    mock_stop.assert_called_once_with(proc)


@patch("mdc_cli.ssm.load_ssm_context")
@patch("mdc_cli.ssm.is_local_tcp_port_open", return_value=False)
def test_managed_tunnel_missing_instance_raises(mock_open, mock_ctx):
    ctx = MagicMock()
    ctx.clinic_api_instance_id = None
    ctx.rds_endpoint = "rds.example.com"
    mock_ctx.return_value = ctx

    with pytest.raises(ClinicDbTunnelError, match="instance ID missing"):
        with managed_clinic_db_tunnel(local_port="5433"):
            pass


def _taskkill_cmds(mock_run) -> list[list[str]]:
    return [list(call.args[0]) for call in mock_run.call_args_list]


@patch("mdc_cli.ssm.sys.platform", "win32")
@patch("mdc_cli.ssm._windows_descendant_pids", return_value=[111])
@patch("mdc_cli.ssm.subprocess.run")
def test_stop_kills_windows_plugin_tree(mock_run, mock_descendants):
    proc = MagicMock()
    proc.pid = 10
    proc._mdc_descendant_pids = [222]
    proc.wait.return_value = 0

    _stop_port_forward_process(proc)

    assert _taskkill_cmds(mock_run) == [
        ["taskkill", "/F", "/T", "/PID", "10"],
        ["taskkill", "/F", "/T", "/PID", "222"],
        ["taskkill", "/F", "/T", "/PID", "111"],
    ]
    proc.terminate.assert_not_called()
    mock_descendants.assert_called_once_with(10)


@patch("mdc_cli.ssm.sys.platform", "win32")
@patch("mdc_cli.ssm._windows_descendant_pids", return_value=[])
@patch("mdc_cli.ssm.subprocess.run")
def test_stop_kills_saved_plugin_after_parent_exits(mock_run, mock_descendants):
    proc = MagicMock()
    proc.pid = 10
    proc.poll.return_value = 0
    proc._mdc_descendant_pids = [333]
    proc.wait.return_value = 0

    _stop_port_forward_process(proc)

    assert ["taskkill", "/F", "/T", "/PID", "333"] in _taskkill_cmds(mock_run)
    proc.terminate.assert_not_called()
    mock_descendants.assert_called_once_with(10)


@patch("mdc_cli.ssm.sys.platform", "linux")
def test_stop_on_posix_terminates_parent_only():
    proc = MagicMock()
    proc.poll.return_value = None
    proc.wait.return_value = 0

    _stop_port_forward_process(proc)

    proc.terminate.assert_called_once_with()
    proc.wait.assert_called_once_with(timeout=15)


@patch("mdc_cli.ssm.sys.platform", "linux")
def test_stop_on_posix_skips_when_parent_already_exited():
    proc = MagicMock()
    proc.poll.return_value = 0

    _stop_port_forward_process(proc)

    proc.terminate.assert_not_called()


def test_windows_descendant_snapshot_returns_pids():
    if sys.platform != "win32":
        pytest.skip("Windows process snapshot")
    pids = _windows_descendant_pids(os.getpid())
    assert isinstance(pids, list)
    assert all(isinstance(pid, int) and pid > 0 for pid in pids)
