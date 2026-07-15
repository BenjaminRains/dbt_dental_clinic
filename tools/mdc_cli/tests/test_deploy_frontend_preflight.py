"""Unit tests for frontend deploy preflight helpers (split Phase 4)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import typer

from mdc_cli.credentials import FrontendDeployConfig
from mdc_cli import deploy_frontend as df


def _write_workspace(frontend: Path, app: str, name: str) -> Path:
    app_dir = frontend / "apps" / app
    app_dir.mkdir(parents=True)
    (app_dir / "package.json").write_text(
        json.dumps({"name": name}),
        encoding="utf-8",
    )
    return app_dir


def test_validate_workspace_app_ok(tmp_path: Path):
    frontend = tmp_path / "frontend"
    _write_workspace(frontend, "portfolio", "@mdc/portfolio")
    app_dir = df.validate_workspace_app(frontend, "demo")
    assert app_dir.name == "portfolio"


def test_validate_workspace_app_name_mismatch(tmp_path: Path):
    frontend = tmp_path / "frontend"
    _write_workspace(frontend, "portfolio", "@mdc/clinic")
    with pytest.raises(typer.Exit):
        df.validate_workspace_app(frontend, "demo")


def test_validate_api_url_rejects_clinic_host_for_demo():
    config = FrontendDeployConfig(
        target="demo",
        bucket_name="b",
        distribution_id="d",
        domain="https://dbtdentalclinic.com",
        api_url="https://api-clinic.dbtdentalclinic.com",
        api_key="k",
        vite_is_demo=True,
    )
    with pytest.raises(typer.Exit):
        df.validate_api_url_for_target(config)


def test_validate_api_url_rejects_demo_host_for_clinic():
    config = FrontendDeployConfig(
        target="clinic",
        bucket_name="b",
        distribution_id="d",
        domain="https://clinic.dbtdentalclinic.com",
        api_url="https://api.dbtdentalclinic.com",
        api_key="k",
        vite_is_demo=False,
    )
    with pytest.raises(typer.Exit):
        df.validate_api_url_for_target(config)


def test_write_and_read_last_deploy_record(tmp_path: Path, monkeypatch):
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    monkeypatch.setattr(df, "REPO_ROOT", tmp_path)
    config = FrontendDeployConfig(
        target="demo",
        bucket_name="demo-bucket",
        distribution_id="EDIST",
        domain="https://dbtdentalclinic.com",
        api_url="https://api.dbtdentalclinic.com",
        api_key="k",
        vite_is_demo=True,
    )
    with patch.object(df.typer, "echo"):
        df.write_last_deploy_record(frontend, config, "@mdc/portfolio")
    record = df.read_last_deploy_record(frontend, "demo")
    assert record is not None
    assert record["workspace"] == "@mdc/portfolio"
    assert record["bucket"] == "demo-bucket"
    assert "deployed_at" in record
