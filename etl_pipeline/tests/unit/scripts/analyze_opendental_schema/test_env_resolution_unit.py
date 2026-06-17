"""Unit tests for analyze_opendental_schema env resolution (no load_dotenv)."""

import os

import pytest

from scripts.analyze_opendental_schema import resolve_analysis_stage


@pytest.mark.unit
class TestResolveAnalysisStage:
    def test_explicit_stage(self, monkeypatch):
        monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
        assert resolve_analysis_stage("clinic") == "clinic"

    def test_from_etl_environment(self, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "test")
        assert resolve_analysis_stage() == "test"

    def test_fail_fast_when_unset(self, monkeypatch):
        monkeypatch.delenv("ETL_ENVIRONMENT", raising=False)
        with pytest.raises(ValueError, match="ETL_ENVIRONMENT is not set"):
            resolve_analysis_stage()

    def test_rejects_production(self, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "production")
        with pytest.raises(ValueError, match="production"):
            resolve_analysis_stage()

    def test_rejects_invalid_stage(self, monkeypatch):
        monkeypatch.setenv("ETL_ENVIRONMENT", "demo")
        with pytest.raises(ValueError, match="Invalid ETL stage"):
            resolve_analysis_stage()
