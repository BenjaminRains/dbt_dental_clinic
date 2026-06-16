"""Tests for SSM port-forward helpers (Phase 5.1)."""

import json
from unittest.mock import patch

from typer.testing import CliRunner

from mdc_cli.main import app
from mdc_cli.ssm import load_ssm_context, port_forward_parameters_json, tunnel_clinic_db

runner = CliRunner()


def test_port_forward_parameters_json_matches_powershell_escaping():
    raw = port_forward_parameters_json("db.example.com", "5432", "5433")
    assert raw == (
        '{\\"host\\":[\\"db.example.com\\"],'
        '\\"portNumber\\":[\\"5432\\"],'
        '\\"localPortNumber\\":[\\"5433\\"]}'
    )
    # AWS CLI receives this string; inner JSON after unescape
    inner = raw.replace("\\", "")
    payload = json.loads(inner)
    assert payload["host"] == ["db.example.com"]
    assert payload["localPortNumber"] == ["5433"]


def test_load_ssm_context_from_fixture(tmp_path, monkeypatch):
    creds = {
        "backend_api": {
            "ec2": {"instance_id": "i-demo"},
            "clinic_api": {"ec2": {"instance_id": "i-clinic"}},
            "clinic_database_reference": {
                "rds": {"endpoint": "rds.example.com"},
            },
        },
        "demo_database": {
            "ec2": {"instance_id": "i-demodb"},
            "database_connection": {"host": "demo.internal", "port": "5432"},
        },
    }
    cred_path = tmp_path / "deployment_credentials.json"
    cred_path.write_text(json.dumps(creds), encoding="utf-8")
    monkeypatch.setattr("mdc_cli.credentials.DEPLOYMENT_CREDENTIALS", cred_path)

    ctx = load_ssm_context()
    assert ctx.api_instance_id == "i-demo"
    assert ctx.clinic_api_instance_id == "i-clinic"
    assert ctx.demo_db_instance_id == "i-demodb"
    assert ctx.rds_endpoint == "rds.example.com"
    assert ctx.demo_db_host == "demo.internal"


@patch("mdc_cli.ssm.run_subprocess", return_value=0)
@patch("mdc_cli.ssm.load_ssm_context")
def test_tunnel_clinic_db_invokes_aws(mock_ctx, mock_run):
    from mdc_cli.ssm import SsmContext

    mock_ctx.return_value = SsmContext(
        api_instance_id="i-demo",
        clinic_api_instance_id="i-clinic",
        demo_db_instance_id="i-demodb",
        rds_endpoint="rds.example.com",
        demo_db_host="demo.internal",
        demo_db_port="5432",
    )
    code = tunnel_clinic_db()
    assert code == 0
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert "aws" in cmd[0].lower() or cmd[0].endswith("aws")
    assert "start-session" in cmd
    assert "i-clinic" in cmd


@patch("mdc_cli.commands.tunnel.tunnel_clinic_db", return_value=0)
def test_tunnel_cli_clinic_db(mock_tunnel):
    result = runner.invoke(app, ["tunnel", "clinic-db"])
    assert result.exit_code == 0
    mock_tunnel.assert_called_once()


@patch("mdc_cli.commands.ssm.connect_clinic_api", return_value=0)
def test_ssm_connect_clinic_api(mock_connect):
    result = runner.invoke(app, ["ssm", "connect", "clinic-api"])
    assert result.exit_code == 0
    mock_connect.assert_called_once()
