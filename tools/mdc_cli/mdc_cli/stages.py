"""Stage name validation for mdc commands."""

from __future__ import annotations

import typer

from mdc_cli.paths import API_STAGES, ETL_STAGES

ETL_PROFILES = ("load", "full")


def require_api_stage(stage: str) -> str:
    if stage not in API_STAGES:
        raise typer.BadParameter(
            f"Unsupported API stage '{stage}'. Expected one of: {list(API_STAGES)}"
        )
    return stage


def require_etl_stage(stage: str) -> str:
    if stage not in ETL_STAGES:
        raise typer.BadParameter(
            f"Unsupported ETL stage '{stage}'. Expected one of: {list(ETL_STAGES)}"
        )
    return stage


def require_etl_profile(profile: str) -> str:
    if profile not in ETL_PROFILES:
        raise typer.BadParameter(
            f"Unsupported ETL profile '{profile}'. Expected one of: {list(ETL_PROFILES)}"
        )
    return profile
