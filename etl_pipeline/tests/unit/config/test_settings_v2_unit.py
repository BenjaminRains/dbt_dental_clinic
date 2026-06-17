"""
Unit tests for etl_pipeline.config.settings_v2 (Phase 2 pydantic-settings loader).
"""

import os
from pathlib import Path

import pytest

from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.config.settings_v2 import (
    STAGE_PREFIXES,
    ETLProfile,
    connection_config_dict,
    load_etl_connection_settings,
    load_etl_connection_settings_from_env,
    load_etl_env_dict,
    resolve_etl_profile,
    resolve_etl_stage,
)

ETL_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ETL_ROOT


def _test_env_vars(stage: str, *, include_source: bool = True) -> dict[str, str]:
    """Minimal valid env var set for a stage."""
    p = STAGE_PREFIXES[stage]
    env = {
        "ETL_ENVIRONMENT": stage,
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
    if include_source:
        env.update(
            {
                f"{p['source']}HOST": "source.example.com",
                f"{p['source']}PORT": "3306",
                f"{p['source']}DB": "opendental",
                f"{p['source']}USER": "readonly",
                f"{p['source']}PASSWORD": "secret-src",
            }
        )
    return env


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


class TestResolveEtlStage:
    @pytest.mark.unit
    def test_resolve_explicit_stage(self):
        assert resolve_etl_stage("test").value == "test"

    @pytest.mark.unit
    def test_resolve_from_os_env(self, clean_etl_env, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "clinic")
        assert resolve_etl_stage().value == "clinic"

    @pytest.mark.unit
    def test_missing_etl_environment_raises(self, clean_etl_env):
        with pytest.raises(ValueError, match="ETL_ENVIRONMENT environment variable is not set"):
            resolve_etl_stage()

    @pytest.mark.unit
    def test_production_stage_rejected(self, clean_etl_env):
        with pytest.raises(ValueError, match="production.*removed"):
            resolve_etl_stage("production")

    @pytest.mark.unit
    def test_demo_stage_rejected(self, clean_etl_env):
        with pytest.raises(ValueError, match="Invalid environment 'demo'"):
            resolve_etl_stage("demo")

    @pytest.mark.unit
    def test_settings_maps_missing_env_to_environment_error(self, clean_etl_env):
        from etl_pipeline.config.settings import Settings
        from etl_pipeline.exceptions.configuration import EnvironmentError

        with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
            Settings._detect_environment()

    @pytest.mark.unit
    def test_settings_maps_invalid_env_to_configuration_error(self, clean_etl_env, monkeypatch):
        from etl_pipeline.config.settings import Settings
        from etl_pipeline.exceptions.configuration import ConfigurationError

        monkeypatch.setenv("ETL_ENVIRONMENT", "invalid")
        with pytest.raises(ConfigurationError, match="Invalid environment"):
            Settings._detect_environment()

    @pytest.mark.unit
    def test_file_config_provider_rejects_demo(self, clean_etl_env, monkeypatch, tmp_path):
        monkeypatch.setenv("ETL_ENVIRONMENT", "demo")
        with pytest.raises(ValueError, match="Invalid environment 'demo'"):
            FileConfigProvider(tmp_path)

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

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_dict_provider_uses_typed_connections_from_env(self):
        """DictConfigProvider validates injected env via settings_v2."""
        from etl_pipeline.config.providers import DictConfigProvider
        from etl_pipeline.config.settings import DatabaseType, Settings

        env = _test_env_vars("test")
        provider = DictConfigProvider(env=env, environment="test")
        settings = Settings(environment="test", provider=provider)

        assert settings.validate_configs() is True
        assert provider._connection_settings is not None
        assert load_etl_connection_settings_from_env(env, environment="test").stage.value == "test"
        analytics = settings.get_database_config(DatabaseType.ANALYTICS)
        assert analytics["host"] == "localhost"
        assert analytics["database"] == "analytics_db"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_blank_os_port_uses_default_from_file(self, clean_etl_env, monkeypatch, tmp_path):
        """Blank POSTGRES_ANALYTICS_PORT in OS env must not break validation (Phase 3 shell export)."""
        env_file = tmp_path / ".env_clinic"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("clinic").items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("ETL_ENVIRONMENT", "clinic")
        monkeypatch.setenv("POSTGRES_ANALYTICS_PORT", "")

        env_dict = load_etl_env_dict(environment="clinic", config_dir=tmp_path)
        assert env_dict["POSTGRES_ANALYTICS_PORT"] == "5432"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_blank_os_postgres_vars_fall_back_to_file(self, clean_etl_env, monkeypatch, tmp_path):
        """Stale blank POSTGRES_* shell vars must not block .env_<stage> values."""
        env_file = tmp_path / ".env_clinic"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("clinic").items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\nETL_BATCH_SIZE=1000\nMETRICS_PATH=/metrics\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("ETL_ENVIRONMENT", "clinic")
        monkeypatch.setenv("ETL_BATCH_SIZE", "1000")
        monkeypatch.setenv("METRICS_PATH", "/metrics")
        for key in _test_env_vars("clinic"):
            if key.startswith("POSTGRES_ANALYTICS_"):
                monkeypatch.setenv(key, "")

        env_dict = load_etl_env_dict(environment="clinic", config_dir=tmp_path)
        assert env_dict["POSTGRES_ANALYTICS_HOST"] == "localhost"
        assert env_dict["POSTGRES_ANALYTICS_DB"] == "analytics_db"
        assert env_dict["POSTGRES_ANALYTICS_USER"] == "analytics_user"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_blank_opendental_vars_fall_back_to_file(self, clean_etl_env, monkeypatch, tmp_path):
        """Stale blank OPENDENTAL_* vars after api-init must not block etl local load."""
        env_file = tmp_path / ".env_local"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("local").items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\nCLINIC_API_KEY=fake-key-from-failed-api-init\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("ETL_ENVIRONMENT", "local")
        monkeypatch.setenv("CLINIC_API_KEY", "fake-key-from-failed-api-init")
        for key in _test_env_vars("local"):
            if key.startswith("OPENDENTAL_SOURCE_"):
                monkeypatch.setenv(key, "")

        env_dict = load_etl_env_dict(environment="local", config_dir=tmp_path)
        assert env_dict["OPENDENTAL_SOURCE_HOST"] == "source.example.com"

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_local_load_profile_without_source(self, clean_etl_env, monkeypatch, tmp_path):
        """Phase 3.5: local load profile validates replication + analytics only."""
        env_file = tmp_path / ".env_local"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("local", include_source=False).items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\n",
            encoding="utf-8",
        )

        monkeypatch.setenv("ETL_ENVIRONMENT", "local")
        for key, value in _test_env_vars("local", include_source=False).items():
            monkeypatch.setenv(key, value)

        settings = load_etl_connection_settings(
            environment="local",
            config_dir=tmp_path,
            profile="load",
        )
        assert settings.profile == ETLProfile.LOAD
        assert settings.source is None
        assert settings.replication.host == "localhost"

        env_dict = load_etl_env_dict(
            environment="local",
            config_dir=tmp_path,
            profile="load",
        )
        assert env_dict["ETL_PROFILE"] == "load"
        assert "OPENDENTAL_SOURCE_HOST" not in env_dict
        assert env_dict["MYSQL_REPLICATION_DB"] == "repl_db"
        assert env_dict["POSTGRES_ANALYTICS_DB"] == "analytics_db"

    @pytest.mark.unit
    def test_resolve_etl_profile_defaults(self):
        from etl_pipeline.config.settings_v2 import ETLStage

        assert resolve_etl_profile(ETLStage.LOCAL) == ETLProfile.LOAD
        assert resolve_etl_profile(ETLStage.CLINIC) == ETLProfile.FULL
        assert resolve_etl_profile(ETLStage.TEST) == ETLProfile.FULL

    @pytest.mark.unit
    def test_connection_config_dict_source_missing_on_load_profile(
        self, clean_etl_env, monkeypatch, tmp_path
    ):
        env_file = tmp_path / ".env_local"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("local", include_source=False).items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("ETL_ENVIRONMENT", "local")
        settings = load_etl_connection_settings(
            environment="local",
            config_dir=tmp_path,
            profile="load",
        )

        with pytest.raises(ValueError, match="source connection is not configured"):
            connection_config_dict(settings, "source")

    @pytest.mark.unit
    @pytest.mark.provider_pattern
    def test_file_config_provider_requires_full_profile(
        self, clean_etl_env, monkeypatch, tmp_path
    ):
        """Pipeline runtime (FileConfigProvider) always uses profile=full."""
        env_file = tmp_path / ".env_local"
        env_file.write_text(
            "\n".join(
                f"{k}={v}"
                for k, v in _test_env_vars("local", include_source=False).items()
                if k != "ETL_ENVIRONMENT"
            )
            + "\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("ETL_ENVIRONMENT", "local")
        for key, value in _test_env_vars("local", include_source=False).items():
            monkeypatch.setenv(key, value)

        with pytest.raises(ValueError, match="Invalid ETL configuration"):
            FileConfigProvider(tmp_path, environment="local")


@pytest.mark.unit
def test_create_settings_default_config_dir_is_etl_pipeline_root():
    """create_settings() must load .env_<stage> from etl_pipeline/, not etl_pipeline/etl_pipeline/."""
    from etl_pipeline.config.settings import Settings

    root = Settings.etl_pipeline_root()
    assert root.is_dir()
    assert (root / "etl_pipeline" / "config" / "pipeline.yml").exists()
    assert (root / ".env_clinic.template").exists() or (root / ".env_test.template").exists()
