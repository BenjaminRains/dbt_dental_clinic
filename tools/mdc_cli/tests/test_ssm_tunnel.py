"""Tests for background clinic DB SSM tunnel helpers."""

from unittest.mock import MagicMock, patch

import pytest

from mdc_cli.ssm import (
    ClinicDbTunnelError,
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
