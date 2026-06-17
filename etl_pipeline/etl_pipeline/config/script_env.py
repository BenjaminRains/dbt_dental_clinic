"""Shared ETL stage resolution for standalone scripts (settings_v2 authority)."""

from __future__ import annotations

import os
from typing import Optional

from .settings import Settings, create_settings, reset_settings, set_settings

VALID_ETL_STAGES = ("local", "clinic", "test")


def resolve_script_stage(
    explicit: Optional[str] = None,
    *,
    default: Optional[str] = None,
) -> str:
    """Resolve ETL stage from explicit value, ETL_ENVIRONMENT, or default."""
    stage = (explicit or os.getenv("ETL_ENVIRONMENT") or default or "").strip()
    if not stage:
        raise ValueError(
            "ETL_ENVIRONMENT is not set. Set ETL_ENVIRONMENT=local|clinic|test "
            "or pass an explicit stage."
        )
    if stage == "production":
        raise ValueError(
            "Invalid ETL_ENVIRONMENT 'production'. Use 'local' or 'clinic'."
        )
    if stage not in VALID_ETL_STAGES:
        raise ValueError(
            f"Invalid ETL stage '{stage}'. Must be one of: {', '.join(VALID_ETL_STAGES)}"
        )
    return stage


def apply_supplemental_env(settings: Settings) -> None:
    """Expose provider env dict to os.environ where OS has no value (Phase 0)."""
    env_dict = settings.provider.get_config("env")
    for key, value in env_dict.items():
        if value and not (os.getenv(key) or "").strip():
            os.environ[key] = str(value)


def load_script_settings(
    stage: Optional[str] = None,
    *,
    default_stage: Optional[str] = None,
) -> Settings:
    """Load Settings via create_settings and register as global instance."""
    resolved = resolve_script_stage(stage, default=default_stage)
    os.environ["ETL_ENVIRONMENT"] = resolved
    reset_settings()
    settings = create_settings(environment=resolved)
    apply_supplemental_env(settings)
    set_settings(settings)
    return settings
