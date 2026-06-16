"""Tests for pydantic env loaders used by mdc (formerly export_env_for_shell.py)."""

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def clean_export_env(monkeypatch):
    prefixes = (
        "API_",
        "ETL_",
        "POSTGRES_",
        "DEMO_",
        "TEST_",
        "CLINIC_",
        "OPENDENTAL_",
        "MYSQL_",
        "GLIC_",
        "PGSSL",
    )
    for key in list(os.environ.keys()):
        if any(key.startswith(p) for p in prefixes):
            monkeypatch.delenv(key, raising=False)
    yield


class TestEnvLoaderExports:
    @pytest.mark.unit
    def test_export_etl_test_from_os_env(self, clean_export_env, monkeypatch):
        env_test = REPO_ROOT / "etl_pipeline" / ".env_test"
        if not env_test.exists():
            pytest.skip(".env_test not present")

        etl_root = REPO_ROOT / "etl_pipeline"
        sys.path.insert(0, str(etl_root))
        from etl_pipeline.config.settings_v2 import load_etl_env_dict

        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        data = load_etl_env_dict(environment="test", config_dir=etl_root)
        assert data["ETL_ENVIRONMENT"] == "test"
        assert "TEST_POSTGRES_ANALYTICS_HOST" in data

    @pytest.mark.unit
    def test_export_api_validates_stage(self, clean_export_env, monkeypatch):
        env_local = REPO_ROOT / "api" / ".env_api_local"
        if not env_local.exists():
            pytest.skip(".env_api_local not present")

        api_dir = REPO_ROOT / "api"
        sys.path.insert(0, str(api_dir))
        from settings import export_api_env_dict

        monkeypatch.setenv("API_ENVIRONMENT", "local")
        data = export_api_env_dict(environment="local")
        assert data["API_ENVIRONMENT"] == "local"

    @pytest.mark.unit
    def test_export_api_blank_port_falls_back_to_file(self, clean_export_env, monkeypatch):
        env_local = REPO_ROOT / "api" / ".env_api_local"
        if not env_local.exists():
            pytest.skip(".env_api_local not present")

        api_dir = REPO_ROOT / "api"
        sys.path.insert(0, str(api_dir))
        from settings import export_api_env_dict

        monkeypatch.setenv("API_ENVIRONMENT", "local")
        monkeypatch.setenv("POSTGRES_ANALYTICS_PORT", "")
        data = export_api_env_dict(environment="local")
        assert data["API_ENVIRONMENT"] == "local"
        assert data.get("POSTGRES_ANALYTICS_PORT") == "5432"

    @pytest.mark.unit
    def test_export_api_env_dict_direct(self, clean_export_env, monkeypatch):
        env_local = REPO_ROOT / "api" / ".env_api_local"
        if not env_local.exists():
            pytest.skip(".env_api_local not present")

        api_dir = REPO_ROOT / "api"
        sys.path.insert(0, str(api_dir))
        from settings import export_api_env_dict

        monkeypatch.setenv("API_ENVIRONMENT", "local")
        data = export_api_env_dict(environment="local")
        assert data["API_ENVIRONMENT"] == "local"
        assert data.get("POSTGRES_ANALYTICS_HOST")

    @pytest.mark.unit
    def test_export_etl_local_load_profile(self, clean_export_env, monkeypatch, tmp_path):
        etl_dir = tmp_path
        lines = [
            "MYSQL_REPLICATION_HOST=localhost",
            "MYSQL_REPLICATION_PORT=3305",
            "MYSQL_REPLICATION_DB=repl_db",
            "MYSQL_REPLICATION_USER=repl_user",
            "MYSQL_REPLICATION_PASSWORD=secret-repl",
            "POSTGRES_ANALYTICS_HOST=localhost",
            "POSTGRES_ANALYTICS_PORT=5432",
            "POSTGRES_ANALYTICS_DB=analytics_db",
            "POSTGRES_ANALYTICS_SCHEMA=raw",
            "POSTGRES_ANALYTICS_USER=analytics_user",
            "POSTGRES_ANALYTICS_PASSWORD=secret-pg",
        ]
        (etl_dir / ".env_local").write_text("\n".join(lines) + "\n", encoding="utf-8")

        etl_root = REPO_ROOT / "etl_pipeline"
        sys.path.insert(0, str(etl_root))
        from etl_pipeline.config.settings_v2 import load_etl_env_dict

        monkeypatch.setenv("ETL_ENVIRONMENT", "local")
        data = load_etl_env_dict(environment="local", config_dir=etl_dir, profile="load")
        assert data["ETL_PROFILE"] == "load"
        assert "OPENDENTAL_SOURCE_HOST" not in data
        assert data["MYSQL_REPLICATION_DB"] == "repl_db"
