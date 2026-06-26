"""Tests for Secrets Manager credential resolution."""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from mdc_cli.main import app
from mdc_cli.secrets_manager import (
    ClinicAnalyticsSecret,
    _parse_clinic_analytics_secret_payload,
    check_clinic_credential_sync,
    normalize_clinic_password_value,
    clinic_password_value_is_json_blob,
    plain_clinic_password,
    resolve_clinic_rds_password,
    sync_clinic_env_from_secrets,
    update_dotenv_key,
)

runner = CliRunner()

RDS_MASTER_ARN = "arn:aws:secretsmanager:us-east-1:123:secret:rds!db-abc"
RDS_MASTER_NAME = "rds!db-83a24c7f-7e85-4168-ba14-ad6e63905c49"


@pytest.fixture(autouse=True)
def suppress_deprecated_etl_key_warnings():
    with patch("mdc_cli.postgres_env.deprecated_etl_analytics_keys", return_value=[]):
        yield


@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
def test_resolve_prefers_secrets_manager(mock_fetch):
    mock_fetch.return_value = ClinicAnalyticsSecret(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        username="analytics_user",
        password="live-password",
        host="rds.example.com",
        port=5432,
        database="opendental_analytics",
    )
    resolution = resolve_clinic_rds_password("stale-file-password", prefer_secrets_manager=True)
    assert resolution.password == "live-password"
    assert resolution.source.startswith("secrets_manager:")


@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
def test_resolve_falls_back_to_env_file(mock_fetch):
    mock_fetch.side_effect = RuntimeError("aws CLI not found")
    resolution = resolve_clinic_rds_password("file-password", prefer_secrets_manager=True)
    assert resolution.password == "file-password"
    assert resolution.source == "env_file"
    assert resolution.warning


def test_update_dotenv_key_replaces_existing(tmp_path):
    env_file = tmp_path / ".env_api_clinic"
    env_file.write_text("POSTGRES_ANALYTICS_PASSWORD=old\nPOSTGRES_ANALYTICS_HOST=localhost\n", encoding="utf-8")
    changed = update_dotenv_key(env_file, "POSTGRES_ANALYTICS_PASSWORD", "new")
    assert changed is True
    text = env_file.read_text(encoding="utf-8")
    assert "POSTGRES_ANALYTICS_PASSWORD=new" in text
    assert "old" not in text


@patch("mdc_cli.secrets_manager.fetch_rds_master_user_secret_arn", return_value=RDS_MASTER_ARN)
@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
def test_check_clinic_credential_sync_stale_file(mock_fetch, _mock_master, tmp_path, monkeypatch):
    from mdc_cli import secrets_manager as sm

    api_env = tmp_path / ".env_api_clinic"
    api_env.write_text("POSTGRES_ANALYTICS_PASSWORD=stale\n", encoding="utf-8")
    monkeypatch.setattr(sm, "API_DIR", tmp_path)
    monkeypatch.setattr(sm, "DEPLOYMENT_CREDENTIALS", tmp_path / "missing.json")

    mock_fetch.return_value = ClinicAnalyticsSecret(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        username="analytics_user",
        password="current",
        host=None,
        port=None,
        database=None,
    )

    report = check_clinic_credential_sync()
    assert report.secrets_manager_ok is True
    assert len(report.rows) == 1
    assert report.rows[0].status == "stale"
    assert "differs" in report.rows[0].note


@patch("mdc_cli.secrets_manager.fetch_rds_master_user_secret_arn", return_value=RDS_MASTER_ARN)
@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
def test_check_clinic_credential_sync_ok_when_sm_password_field_is_json_blob(
    mock_fetch, _mock_master, tmp_path, monkeypatch
):
    from mdc_cli import secrets_manager as sm

    api_env = tmp_path / ".env_api_clinic"
    api_env.write_text("POSTGRES_ANALYTICS_PASSWORD=live\n", encoding="utf-8")
    monkeypatch.setattr(sm, "API_DIR", tmp_path)
    creds_path = tmp_path / "deployment_credentials.json"
    creds_path.write_text(
        '{"clinic_database":{"postgresql":{"password":"live"}}}',
        encoding="utf-8",
    )
    monkeypatch.setattr(sm, "DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr("mdc_cli.paths.DEPLOYMENT_CREDENTIALS", creds_path)
    monkeypatch.setattr("mdc_cli.credentials.DEPLOYMENT_CREDENTIALS", creds_path)

    raw_secret = '{"username":"analytics_user","password":"live","host":"rds.example.com"}'
    mock_fetch.return_value = ClinicAnalyticsSecret(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        username="analytics_user",
        password=raw_secret,
        host="rds.example.com",
        port=5432,
        database="opendental_analytics",
        raw_secret_string=raw_secret,
    )

    report = check_clinic_credential_sync()
    assert report.secrets_manager_ok is True
    assert all(row.status == "ok" for row in report.rows)


@patch("mdc_cli.secrets_manager.fetch_rds_master_user_secret_arn", return_value=RDS_MASTER_ARN)
@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
def test_check_clinic_credential_sync_in_sync(mock_fetch, _mock_master, tmp_path, monkeypatch):
    from mdc_cli import secrets_manager as sm

    api_env = tmp_path / ".env_api_clinic"
    api_env.write_text("POSTGRES_ANALYTICS_PASSWORD=current\n", encoding="utf-8")
    monkeypatch.setattr(sm, "API_DIR", tmp_path)
    monkeypatch.setattr(sm, "DEPLOYMENT_CREDENTIALS", tmp_path / "missing.json")

    mock_fetch.return_value = ClinicAnalyticsSecret(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        username="analytics_user",
        password="current",
        host=None,
        port=None,
        database=None,
    )

    report = check_clinic_credential_sync()
    assert report.rows[0].status == "ok"


@patch("mdc_cli.commands.status.check_clinic_credential_sync")
def test_status_includes_credential_sync(mock_check):
    from mdc_cli.secrets_manager import ClinicCredentialSyncReport, CredentialSyncRow

    mock_check.return_value = ClinicCredentialSyncReport(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        secrets_manager_ok=True,
        rows=[
            CredentialSyncRow(
                target="api/.env_api_clinic",
                status="stale",
                note="differs from Secrets Manager (rotation) — run: mdc secrets pull clinic",
            )
        ],
    )
    result = CliRunner().invoke(app, ["status", "--no-freshness"])
    assert result.exit_code == 0
    assert "Clinic RDS credential sync" in result.stdout
    assert "api/.env_api_clinic" in result.stdout
    assert "dental-clinic/database" not in result.stdout
    mock_check.assert_called_once()


@patch("mdc_cli.commands.secrets.sync_clinic_env_from_secrets")
@patch("mdc_cli.commands.secrets.clinic_rds_master_secret_id", return_value=RDS_MASTER_ARN)
def test_secrets_pull_clinic(mock_master_id, mock_sync):
    from mdc_cli.secrets_manager import ClinicAnalyticsSecret, ClinicEnvSyncResult

    mock_sync.return_value = ClinicEnvSyncResult(
        secret=ClinicAnalyticsSecret(
            secret_id=RDS_MASTER_ARN,
            region="us-east-1",
            username="analytics_user",
            password="x",
            host=None,
            port=None,
            database=None,
        ),
        api_env_changed=True,
        deployment_credentials_changed=True,
        repaired_json_password=True,
        rds_master_secret_id=RDS_MASTER_ARN,
    )
    result = runner.invoke(app, ["secrets", "pull", "clinic"])
    assert result.exit_code == 0
    assert "Updated POSTGRES_ANALYTICS" in result.stdout
    assert "Repaired POSTGRES_ANALYTICS_PASSWORD" in result.stdout
    assert "dental-clinic/database" not in result.stdout
    mock_sync.assert_called_once()


def test_plain_clinic_password_extracts_from_json_password_field():
    blob = (
        '{\\"username\\":\\"analytics_user\\",'
        '\\"password\\":\\"secret-value\\",'
        '\\"host\\":\\"rds.example.com\\"}'
    )
    assert plain_clinic_password(blob, raw_secret=blob) == "secret-value"


@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
@patch("mdc_cli.secrets_manager.fetch_rds_master_user_secret_arn", return_value=RDS_MASTER_ARN)
def test_sync_clinic_env_repairs_json_blob_file(mock_master, mock_fetch, tmp_path):
    api_env = tmp_path / ".env_api_clinic"
    api_env.write_text(
        'POSTGRES_ANALYTICS_PASSWORD={\\"username\\":\\"analytics_user\\",'
        '\\"password\\":\\"old\\",\\"host\\":\\"rds.example.com\\"}\n'
        "POSTGRES_ANALYTICS_HOST=localhost\n",
        encoding="utf-8",
    )
    mock_fetch.return_value = ClinicAnalyticsSecret(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        username="analytics_user",
        password='{\\"username\\":\\"analytics_user\\",\\"password\\":\\"live\\"}',
        host="rds.example.com",
        port=5432,
        database="opendental_analytics",
        raw_secret_string=(
            '{"username":"analytics_user","password":"live","host":"rds.example.com"}'
        ),
    )

    result = sync_clinic_env_from_secrets(
        api_env_file=api_env,
        update_deployment_credentials=False,
    )

    assert result.repaired_json_password is True
    assert result.rds_master_secret_id == RDS_MASTER_ARN
    text = api_env.read_text(encoding="utf-8")
    assert "POSTGRES_ANALYTICS_PASSWORD=live" in text
    assert "username" not in text.split("POSTGRES_ANALYTICS_PASSWORD=", 1)[1].splitlines()[0]


def test_update_dotenv_key_password_never_quotes_json_like_value(tmp_path):
    env_file = tmp_path / ".env_api_clinic"
    env_file.write_text('POSTGRES_ANALYTICS_PASSWORD=old\n', encoding="utf-8")
    blob_password = "abc{not-json}def"
    changed = update_dotenv_key(env_file, "POSTGRES_ANALYTICS_PASSWORD", blob_password)
    assert changed is True
    text = env_file.read_text(encoding="utf-8")
    assert f"POSTGRES_ANALYTICS_PASSWORD={blob_password}" in text
    assert 'POSTGRES_ANALYTICS_PASSWORD="' not in text


def test_normalize_clinic_password_quoted_escaped_json_blob():
    blob = (
        '{\\"username\\":\\"analytics_user\\",'
        '\\"password\\":\\"secret-value\\",'
        '\\"host\\":\\"rds.example.com\\"}'
    )
    quoted = f'"{blob}"'
    assert normalize_clinic_password_value(quoted) == "secret-value"


def test_update_dotenv_key_replaces_escaped_json_blob(tmp_path):
    env_file = tmp_path / ".env_api_clinic"
    env_file.write_text(
        'POSTGRES_ANALYTICS_PASSWORD={\\"username\\":\\"analytics_user\\",'
        '\\"password\\":\\"old-secret\\"}\n',
        encoding="utf-8",
    )
    changed = update_dotenv_key(env_file, "POSTGRES_ANALYTICS_PASSWORD", "newplain")
    assert changed is True
    text = env_file.read_text(encoding="utf-8")
    assert "POSTGRES_ANALYTICS_PASSWORD=newplain" in text
    assert "username" not in text


def test_parse_plain_text_secret_payload():
    secret = _parse_clinic_analytics_secret_payload(
        "plain-password-only",
        secret_id=RDS_MASTER_NAME,
        region="us-east-1",
    )
    assert secret.password == "plain-password-only"


def test_normalize_clinic_password_extracts_json_blob():
    blob = (
        '{\\"username\\":\\"analytics_user\\",'
        '\\"password\\":\\"secret-value\\",'
        '\\"host\\":\\"rds.example.com\\"}'
    )
    assert normalize_clinic_password_value(blob) == "secret-value"
    assert clinic_password_value_is_json_blob(blob) is True


def test_normalize_clinic_password_plain_value():
    assert normalize_clinic_password_value("plain-secret") == "plain-secret"
    assert clinic_password_value_is_json_blob("plain-secret") is False


@patch("mdc_cli.secrets_manager.fetch_rds_master_user_secret_arn", return_value=RDS_MASTER_ARN)
@patch("mdc_cli.secrets_manager.fetch_live_clinic_rds_secret")
def test_check_clinic_credential_sync_warns_on_json_blob(mock_fetch, _mock_master, tmp_path, monkeypatch):
    from mdc_cli import secrets_manager as sm

    api_env = tmp_path / ".env_api_clinic"
    api_env.write_text(
        'POSTGRES_ANALYTICS_PASSWORD={"username":"analytics_user","password":"current"}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(sm, "API_DIR", tmp_path)
    monkeypatch.setattr(sm, "DEPLOYMENT_CREDENTIALS", tmp_path / "missing.json")

    mock_fetch.return_value = ClinicAnalyticsSecret(
        secret_id=RDS_MASTER_ARN,
        region="us-east-1",
        username="analytics_user",
        password="current",
        host=None,
        port=None,
        database=None,
    )

    report = check_clinic_credential_sync()
    assert report.api_password_malformed is True
    assert report.rows[0].status == "warn"
    assert "JSON secret" in report.rows[0].note
