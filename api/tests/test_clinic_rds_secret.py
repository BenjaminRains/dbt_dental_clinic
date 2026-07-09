"""Unit tests for clinic RDS live password resolution (rotation-safe)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

import clinic_rds_secret as crs


@pytest.fixture(autouse=True)
def _clear_cache_and_env(monkeypatch):
    crs.invalidate_clinic_rds_password_cache()
    monkeypatch.delenv("API_CLINIC_RDS_LIVE_PASSWORD", raising=False)
    monkeypatch.delenv("CLINIC_RDS_MASTER_SECRET_ARN", raising=False)
    monkeypatch.delenv("CLINIC_RDS_INSTANCE_ID", raising=False)
    monkeypatch.delenv("API_CLINIC_RDS_PASSWORD_TTL_SECONDS", raising=False)
    yield
    crs.invalidate_clinic_rds_password_cache()


def test_parse_secret_string_plain():
    assert crs._parse_secret_string("s3cret!") == "s3cret!"


def test_parse_secret_string_json():
    raw = json.dumps({"username": "analytics_user", "password": "p@ss?word"})
    assert crs._parse_secret_string(raw) == "p@ss?word"


def test_parse_secret_string_nested_json_blob():
    inner = json.dumps({"username": "analytics_user", "password": "real"})
    raw = json.dumps({"username": "analytics_user", "password": inner})
    assert crs._parse_secret_string(raw) == "real"


def test_live_password_disabled_uses_file(monkeypatch):
    monkeypatch.setenv("API_CLINIC_RDS_LIVE_PASSWORD", "0")
    password, source = crs.resolve_clinic_analytics_password("file-pass")
    assert password == "file-pass"
    assert source == "env_file"


def test_resolve_falls_back_when_sm_fails(monkeypatch):
    monkeypatch.setenv("API_CLINIC_RDS_LIVE_PASSWORD", "1")

    def _boom():
        raise RuntimeError("no aws")

    monkeypatch.setattr(crs, "fetch_live_clinic_rds_password", _boom)
    password, source = crs.resolve_clinic_analytics_password("file-pass")
    assert password == "file-pass"
    assert source == "env_file_fallback"


def test_fetch_uses_cache(monkeypatch):
    monkeypatch.setenv("API_CLINIC_RDS_LIVE_PASSWORD", "1")
    monkeypatch.setenv("CLINIC_RDS_MASTER_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:1:secret:rds!db-test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    sm_client = MagicMock()
    sm_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"username": "analytics_user", "password": "live-1"})
    }
    rds_client = MagicMock()

    def _client(service, region_name=None):
        if service == "secretsmanager":
            return sm_client
        if service == "rds":
            return rds_client
        raise AssertionError(service)

    with patch.dict("sys.modules", {"boto3": MagicMock(client=_client)}):
        # Force import path inside _fetch_password_from_aws
        import sys

        fake_boto3 = MagicMock()
        fake_boto3.client.side_effect = _client
        monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

        first = crs.fetch_live_clinic_rds_password()
        second = crs.fetch_live_clinic_rds_password()

    assert first.password == "live-1"
    assert second.password == "live-1"
    assert sm_client.get_secret_value.call_count == 1


def test_force_refresh_bypasses_cache(monkeypatch):
    monkeypatch.setenv("CLINIC_RDS_MASTER_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:1:secret:rds!db-test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")

    sm_client = MagicMock()
    sm_client.get_secret_value.side_effect = [
        {"SecretString": json.dumps({"password": "live-1"})},
        {"SecretString": json.dumps({"password": "live-2"})},
    ]

    def _client(service, region_name=None):
        if service == "secretsmanager":
            return sm_client
        return MagicMock()

    import sys

    fake_boto3 = MagicMock()
    fake_boto3.client.side_effect = _client
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    first = crs.fetch_live_clinic_rds_password()
    second = crs.fetch_live_clinic_rds_password(force_refresh=True)
    assert first.password == "live-1"
    assert second.password == "live-2"
    assert sm_client.get_secret_value.call_count == 2


def test_config_clinic_uses_live_password(monkeypatch):
    monkeypatch.setenv("API_ENVIRONMENT", "clinic")
    monkeypatch.setenv("POSTGRES_ANALYTICS_HOST", "db.example.rds.amazonaws.com")
    monkeypatch.setenv("POSTGRES_ANALYTICS_PORT", "5432")
    monkeypatch.setenv("POSTGRES_ANALYTICS_DB", "opendental_analytics")
    monkeypatch.setenv("POSTGRES_ANALYTICS_USER", "analytics_user")
    monkeypatch.setenv("POSTGRES_ANALYTICS_PASSWORD", "stale-file")
    monkeypatch.setenv("API_CLINIC_RDS_LIVE_PASSWORD", "1")

    from config import reset_config, APIConfig

    reset_config()

    def _fake_resolve(file_password: str):
        assert file_password == "stale-file"
        return "live-pass", "secrets_manager:test"

    monkeypatch.setattr(
        "clinic_rds_secret.resolve_clinic_analytics_password",
        _fake_resolve,
    )
    cfg = APIConfig(environment="clinic")
    db = cfg.get_database_config()
    assert db["password"] == "live-pass"
