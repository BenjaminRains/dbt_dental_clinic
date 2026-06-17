"""Unit tests for scripts.script_env (settings_v2 authority, no load_dotenv)."""

import os

import pytest

from etl_pipeline.config.script_env import (
    VALID_ETL_STAGES,
    add_stage_argument,
    apply_supplemental_env,
    load_script_settings,
    resolve_script_stage,
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings, reset_settings
from tests.fixtures.env_fixtures import COMPLETE_LOCAL_ENV


@pytest.mark.unit
class TestResolveScriptStage:
    def test_explicit_stage(self, monkeypatch):
        monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
        assert resolve_script_stage("clinic") == "clinic"

    def test_from_etl_environment(self, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        assert resolve_script_stage() == "test"

    def test_default_stage(self, monkeypatch):
        monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
        assert resolve_script_stage(default="local") == "local"

    def test_fail_fast_when_unset(self, monkeypatch):
        monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
        with pytest.raises(ValueError, match="ETL_ENVIRONMENT is not set"):
            resolve_script_stage()

    def test_rejects_production(self, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "production")
        with pytest.raises(ValueError, match="production"):
            resolve_script_stage()

    def test_rejects_invalid_stage(self, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "demo")
        with pytest.raises(ValueError, match="Invalid ETL stage"):
            resolve_script_stage()


@pytest.mark.unit
class TestApplySupplementalEnv:
    def test_fills_missing_os_env_without_overriding(self, monkeypatch):
        monkeypatch.delenv("MYSQL_ROOT_HOST", raising=False)
        monkeypatch.setenv("OPENDENTAL_SOURCE_HOST", "from-os")
        provider = DictConfigProvider(
            env={
                **COMPLETE_LOCAL_ENV,
                "MYSQL_ROOT_HOST": "from-file",
                "OPENDENTAL_SOURCE_HOST": "from-file",
            },
            environment="local",
            profile="load",
        )
        settings = Settings(environment="local", provider=provider)
        apply_supplemental_env(settings)
        assert os.environ["MYSQL_ROOT_HOST"] == "from-file"
        assert os.environ["OPENDENTAL_SOURCE_HOST"] == "from-os"


@pytest.mark.unit
class TestLoadScriptSettings:
    def test_loads_test_stage(self, monkeypatch):
        monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
        reset_settings()
        settings = load_script_settings("test")
        try:
            assert settings.environment == "test"
            assert os.environ["ETL_ENVIRONMENT"] == "test"
        finally:
            reset_settings()


@pytest.mark.unit
class TestAddStageArgument:
    def test_adds_stage_choices(self):
        import argparse

        parser = argparse.ArgumentParser()
        add_stage_argument(parser)
        args = parser.parse_args(["--stage", "clinic"])
        assert args.stage == "clinic"
        assert set(VALID_ETL_STAGES) == {"local", "clinic", "test"}
