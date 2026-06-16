"""
Unit tests for etl_pipeline.config.settings_v2 (Phase 2 pydantic-settings loader).
"""

import os
from pathlib import Path

import pytest

from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.config.settings_v2 import (
    STAGE_PREFIXES,
    load_etl_env_dict,
)

ETL_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ETL_ROOT


def _test_env_vars(stage: str) -> dict[str, str]:
    """Minimal valid env var set for a stage."""
    p = STAGE_PREFIXES[stage]
    return {
        "ETL_ENVIRONMENT": stage,
        f"{p['source']}HOST": "source.example.com",
        f"{p['source']}PORT": "3306",
        f"{p['source']}DB": "opendental",
        f"{p['source']}USER": "readonly",
        f"{p['source']}PASSWORD": "secret-src",
        f"{p['replication']}HOST": "localhost",
        f"{p['replication']}PORT": "3305",
        f"{p['replication']}DB": "repl_db",
        f"{p['replication']}USER": "repl_user",
        f"{p['replication']}PASSWORD": "secret-repl",
        f"{p['analytics']}HOST": "localhost",
        f"{p['analytics']}PORT": "5432",
        f"{p['analytics']}DB": "analytics_db",
        f"{p['analytics']}SCHEMA": "raw",
        f"{p['analytics']}USER": "analytics_user",
        f"{p['analytics']}PASSWORD": "secret-pg",
    }


@pytest.fixture
def clean_etl_env(monkeypatch):
    """Remove ETL-prefixed vars between tests."""
    prefixes = (
        "ETL_",
        "OPENDENTAL_SOURCE_",
        "GLIC_OPENDENTAL_SOURCE_",
        "MYSQL_REPLICATION_",
        "POSTGRES_ANALYTICS_",
        "TEST_OPENDENTAL_SOURCE_",
        "TEST_MYSQL_REPLICATION_",
        "TEST_POSTGRES_ANALYTICS_",
    )
    for key in list(os.environ.keys()):
        if any(key.startswith(p) for p in prefixes):
            monkeypatch.delenv(key, raising=False)
    yield


class TestSettingsV2:
    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_load_etl_env_dict_from_os_env(self, clean_etl_env, monkeypatch, tmp_path):
        """When host is in OS env, load from process env without requiring env file."""
        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        for key, value in _test_env_vars("test").items():
            monkeypatch.setenv(key, value)

        env_dict = load_etl_env_dict(environment="test", config_dir=tmp_path)

        assert env_dict["TEST_POSTGRES_ANALYTICS_HOST"] == "localhost"
        assert env_dict["TEST_OPENDENTAL_SOURCE_DB"] == "opendental"
        assert env_dict["ETL_ENVIRONMENT"] == "test"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_env_precedence_os_host_wins(self, clean_etl_env, monkeypatch, tmp_path):
        """Phase 0: OS analytics host wins over on-disk .env file."""
        env_file = tmp_path / ".env_test"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("test").items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\nTEST_POSTGRES_ANALYTICS_HOST=file-host-should-lose\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        for key, value in _test_env_vars("test").items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("TEST_POSTGRES_ANALYTICS_HOST", "os-host-wins")

        env_dict = load_etl_env_dict(environment="test", config_dir=tmp_path)
        assert env_dict["TEST_POSTGRES_ANALYTICS_HOST"] == "os-host-wins"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_file_config_provider_delegates_to_settings_v2(
        self, clean_etl_env, monkeypatch, tmp_path
    ):
        """FileConfigProvider.get_config('env') uses pydantic loader."""
        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        for key, value in _test_env_vars("test").items():
            monkeypatch.setenv(key, value)

        provider = FileConfigProvider(tmp_path, environment="test")
        env_config = provider.get_config("env")

        assert env_config["TEST_MYSQL_REPLICATION_DB"] == "repl_db"
        assert env_config["ETL_ENVIRONMENT"] == "test"

    @pytest.mark.unit
    def test_missing_env_file_and_no_os_host_raises(self, clean_etl_env, tmp_path):
        with pytest.raises(ValueError, match="not found"):
            load_etl_env_dict(environment="test", config_dir=tmp_path)

    @pytest.mark.unit
    def test_invalid_stage_rejected(self, clean_etl_env):
        with pytest.raises(ValueError, match="Invalid environment 'demo'"):
            load_etl_env_dict(environment="demo", config_dir=CONFIG_DIR)

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_supplemental_glic_from_file_on_local(self, clean_etl_env, monkeypatch, tmp_path):
        """Local stage picks up GLIC_* vars from file when not in typed models."""
        env_file = tmp_path / ".env_local"
        lines = [f"{k}={v}" for k, v in _test_env_vars("local").items() if k != "ETL_ENVIRONMENT"]
        lines.append("GLIC_OPENDENTAL_SOURCE_HOST=glic.example.com")
        lines.append("GLIC_OPENDENTAL_SOURCE_DB=glic_db")
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        monkeypatch.setenv("ETL_ENVIRONMENT", "local")
        for key, value in _test_env_vars("local").items():
            monkeypatch.setenv(key, value)

        env_dict = load_etl_env_dict(environment="local", config_dir=tmp_path)
        assert env_dict["GLIC_OPENDENTAL_SOURCE_HOST"] == "glic.example.com"
        assert env_dict["GLIC_OPENDENTAL_SOURCE_DB"] == "glic_db"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_settings_uses_typed_connections_from_provider(
        self, clean_etl_env, monkeypatch, tmp_path
    ):
        """Settings delegates _get_base_config to pydantic-validated connections."""
        from etl_pipeline.config.settings import DatabaseType, Settings

        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        for key, value in _test_env_vars("test").items():
            monkeypatch.setenv(key, value)

        provider = FileConfigProvider(tmp_path, environment="test")
        settings = Settings(environment="test", provider=provider)

        assert settings.validate_configs() is True
        analytics = settings.get_database_config(DatabaseType.ANALYTICS)
        assert analytics["host"] == "localhost"
        assert analytics["database"] == "analytics_db"
        assert analytics["schema"] == "raw"
        assert provider._connection_settings is not None
