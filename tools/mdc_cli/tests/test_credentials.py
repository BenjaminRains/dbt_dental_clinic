"""Tests for credentials.py (Phase 5.2)."""

import json
from pathlib import Path

import pytest

from mdc_cli.credentials import (
    read_env_file_value,
    resolve_clinic_frontend_config,
    resolve_demo_frontend_config,
)


def test_read_env_file_value(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("CLINIC_API_KEY=secret-key\n# comment\nFOO=bar\n", encoding="utf-8")
    assert read_env_file_value(env_file, "CLINIC_API_KEY") == "secret-key"
    assert read_env_file_value(env_file, "MISSING") is None


def test_resolve_demo_frontend_config_from_json(tmp_path: Path, monkeypatch):
    creds = {
        "frontend": {
            "domain": "dbtdentalclinic.com",
            "s3_buckets": {"frontend": {"bucket_name": "demo-bucket"}},
            "cloudfront": {"distribution_id": "EDEMO123"},
        },
    }
    cred_path = tmp_path / "deployment_credentials.json"
    cred_path.write_text(json.dumps(creds), encoding="utf-8")
    api_dir = tmp_path / "api"
    api_dir.mkdir()
    (api_dir / ".env_api_demo").write_text("DEMO_API_KEY=demo-key\n", encoding="utf-8")

    monkeypatch.setattr("mdc_cli.credentials.DEPLOYMENT_CREDENTIALS", cred_path)
    monkeypatch.setattr("mdc_cli.credentials.API_DIR", api_dir)
    monkeypatch.delenv("FRONTEND_BUCKET_NAME", raising=False)
    monkeypatch.delenv("FRONTEND_DIST_ID", raising=False)
    monkeypatch.delenv("DEMO_API_KEY", raising=False)

    config = resolve_demo_frontend_config()
    assert config.bucket_name == "demo-bucket"
    assert config.distribution_id == "EDEMO123"
    assert config.api_key == "demo-key"
    assert config.vite_is_demo is True


def test_resolve_clinic_frontend_config_requires_api_key(tmp_path: Path, monkeypatch):
    creds = {
        "clinic_frontend": {
            "domain": "clinic.dbtdentalclinic.com",
            "s3_buckets": {"clinic_frontend": {"bucket_name": "clinic-bucket"}},
            "cloudfront": {"distribution_id": "ECLINIC123"},
        },
    }
    cred_path = tmp_path / "deployment_credentials.json"
    cred_path.write_text(json.dumps(creds), encoding="utf-8")
    api_dir = tmp_path / "api"
    api_dir.mkdir()

    monkeypatch.setattr("mdc_cli.credentials.DEPLOYMENT_CREDENTIALS", cred_path)
    monkeypatch.setattr("mdc_cli.credentials.API_DIR", api_dir)
    monkeypatch.delenv("CLINIC_API_KEY", raising=False)

    with pytest.raises(ValueError, match="CLINIC_API_KEY"):
        resolve_clinic_frontend_config()
