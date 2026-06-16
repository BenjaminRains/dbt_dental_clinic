"""Tests for stage validation helpers."""

import pytest
import typer

from mdc_cli.stages import require_api_stage, require_etl_profile, require_etl_stage


def test_require_api_stage_accepts_valid():
    assert require_api_stage("local") == "local"


def test_require_api_stage_rejects_invalid():
    with pytest.raises(typer.BadParameter, match="Unsupported API stage"):
        require_api_stage("not-a-stage")


def test_require_etl_stage_accepts_valid():
    assert require_etl_stage("clinic") == "clinic"


def test_require_etl_profile_rejects_invalid():
    with pytest.raises(typer.BadParameter, match="Unsupported ETL profile"):
        require_etl_profile("pipeline")
