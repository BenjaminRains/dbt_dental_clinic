"""Tests for credentials.py (Phase 5.2)."""

import json
from pathlib import Path

import pytest

from mdc_cli.credentials import (
    _norm,
    read_env_file_value,
    resolve_clinic_frontend_config,
    resolve_demo_frontend_config,
    resolve_demo_hosting_config,
)


def test_norm_coerces_int_port():
    assert _norm(5432) == "5432"


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
    monkeypatch.setattr("mdc_cli.credentials.FRONTEND_DEPLOY_JSON", tmp_path / "missing.json")
    monkeypatch.setattr("mdc_cli.credentials.API_DIR", api_dir)
    monkeypatch.delenv("FRONTEND_BUCKET_NAME", raising=False)
    monkeypatch.delenv("FRONTEND_DIST_ID", raising=False)
    monkeypatch.delenv("DEMO_API_KEY", raising=False)

    config = resolve_demo_frontend_config()
    assert config.bucket_name == "demo-bucket"
    assert config.distribution_id == "EDEMO123"
    assert config.api_key == "demo-key"
    assert config.vite_is_demo is True


def test_resolve_demo_hosting_from_demo_frontend_section(tmp_path: Path, monkeypatch):
    creds = {
        "demo_frontend": {
            "domain": "dbtdentalclinic.com",
            "s3_buckets": {"frontend": {"bucket_name": "live-bucket"}},
            "cloudfront": {"distribution_id": "ELIVE123"},
        },
    }
    cred_path = tmp_path / "deployment_credentials.json"
    cred_path.write_text(json.dumps(creds), encoding="utf-8")

    monkeypatch.setattr("mdc_cli.credentials.DEPLOYMENT_CREDENTIALS", cred_path)
    monkeypatch.setattr("mdc_cli.credentials.FRONTEND_DEPLOY_JSON", tmp_path / "missing.json")
    monkeypatch.delenv("FRONTEND_BUCKET_NAME", raising=False)
    monkeypatch.delenv("FRONTEND_DIST_ID", raising=False)

    hosting = resolve_demo_hosting_config()
    assert hosting.bucket_name == "live-bucket"
    assert hosting.distribution_id == "ELIVE123"
    assert hosting.domain == "https://dbtdentalclinic.com"


def test_resolve_demo_hosting_from_frontend_deploy_json(tmp_path: Path, monkeypatch):
    deploy_json = tmp_path / ".frontend-deploy.json"
    deploy_json.write_text(
        json.dumps(
            {
                "BucketName": "json-bucket",
                "DistributionId": "EJSON123",
                "Domain": "https://dbtdentalclinic.com",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "mdc_cli.credentials.DEPLOYMENT_CREDENTIALS",
        tmp_path / "missing-credentials.json",
    )
    monkeypatch.setattr("mdc_cli.credentials.FRONTEND_DEPLOY_JSON", deploy_json)
    monkeypatch.delenv("FRONTEND_BUCKET_NAME", raising=False)
    monkeypatch.delenv("FRONTEND_DIST_ID", raising=False)

    hosting = resolve_demo_hosting_config()
    assert hosting.bucket_name == "json-bucket"
    assert hosting.distribution_id == "EJSON123"


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
