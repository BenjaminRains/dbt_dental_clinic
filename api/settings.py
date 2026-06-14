"""
Typed API configuration via pydantic-settings (Phase 2).

Single loader for API_ENVIRONMENT, stage .env files, and analytics DB credentials.
OS environment variables win over on-disk .env_api_* files (Phase 0 precedence).
"""

from __future__ import annotations

import logging
import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

API_DIR = Path(__file__).resolve().parent

STAGE_PREFIX: dict[str, str] = {
    "test": "TEST_POSTGRES_ANALYTICS_",
    "demo": "DEMO_POSTGRES_",
    "clinic": "POSTGRES_ANALYTICS_",
    "local": "POSTGRES_ANALYTICS_",
}

HOST_ENV_KEYS: dict[str, str] = {
    "test": "TEST_POSTGRES_ANALYTICS_HOST",
    "demo": "DEMO_POSTGRES_HOST",
    "clinic": "POSTGRES_ANALYTICS_HOST",
    "local": "POSTGRES_ANALYTICS_HOST",
}


class Stage(str, Enum):
    TEST = "test"
    DEMO = "demo"
    CLINIC = "clinic"
    LOCAL = "local"


class AnalyticsDBSettings(BaseSettings):
    """Analytics Postgres connection; env prefix is set per stage at load time."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    host: str
    port: int = 5432
    db: str
    user: str
    password: SecretStr


class APIRuntimeSettings(BaseSettings):
    """Optional API server settings (API_* env vars)."""

    model_config = SettingsConfigDict(
        env_prefix="API_",
        extra="ignore",
        case_sensitive=False,
    )

    cors_origins: str = Field(
        default="http://localhost:3000",
        validation_alias="CORS_ORIGINS",
    )
    debug: bool = False
    log_level: str = "info"
    port: int = 8000
    host: str = "0.0.0.0"


class APISettings:
    """Resolved API configuration for one stage."""

    __slots__ = ("stage", "analytics", "runtime", "sslmode_env")

    def __init__(
        self,
        stage: Stage,
        analytics: AnalyticsDBSettings,
        runtime: APIRuntimeSettings,
        sslmode_env: str = "",
    ):
        self.stage = stage
        self.analytics = analytics
        self.runtime = runtime
        self.sslmode_env = sslmode_env.strip()


def _detect_stage(explicit: Optional[str] = None) -> Stage:
    raw = explicit if explicit is not None else os.getenv("API_ENVIRONMENT")
    if not raw:
        raise ValueError(
            "API_ENVIRONMENT environment variable is not set. "
            "Must be one of: test, demo, clinic, local"
        )
    if raw == "production":
        raise ValueError(
            f"Invalid environment '{raw}'. "
            "'production' has been removed. Use 'demo' for portfolio/demo deployment. "
            "Valid environments: test, demo, clinic, local"
        )
    try:
        return Stage(raw)
    except ValueError as exc:
        raise ValueError(
            f"Invalid environment '{raw}'. Must be one of: "
            f"{[s.value for s in Stage]}"
        ) from exc


def _resolve_sslmode(env_file: Optional[Path]) -> str:
    """OS env wins; if only the stage file has SSLMODE, read it without load_dotenv."""
    explicit = os.getenv("POSTGRES_ANALYTICS_SSLMODE", "").strip()
    if explicit or env_file is None:
        return explicit
    try:
        from dotenv import dotenv_values

        return (dotenv_values(env_file).get("POSTGRES_ANALYTICS_SSLMODE") or "").strip()
    except Exception:
        return ""


def _env_file_for_stage(stage: Stage) -> Optional[Path]:
    """
    Return the stage .env file to load, or None when OS env is authoritative (Phase 0).

    When the analytics host var is already in the process environment (systemd, Docker,
    environment_manager.ps1), do not read a second on-disk .env_api_* file.
    """
    host_key = HOST_ENV_KEYS[stage.value]
    if os.getenv(host_key):
        logger.info(
            "Analytics DB config already present in OS environment (%s set); "
            "skipping on-disk .env_api_%s so it cannot shadow it.",
            host_key,
            stage.value,
        )
        return None

    env_file = API_DIR / f".env_api_{stage.value}"
    if env_file.exists():
        logger.info(
            "Loading API environment variables from: %s (does not override existing env)",
            env_file,
        )
        return env_file

    logger.warning("API environment file not found: %s", env_file)
    logger.info("Using environment variables from system/os.environ")
    return None


def load_api_settings(*, environment: Optional[str] = None) -> APISettings:
    """Load and validate settings (not cached — use get_settings() in production)."""
    stage = _detect_stage(environment)
    prefix = STAGE_PREFIX[stage.value]
    env_file = _env_file_for_stage(stage)

    logger.info("Validating API environment: %s", stage.value)

    try:
        analytics = AnalyticsDBSettings(
            _env_file=env_file,
            _env_prefix=prefix,
        )
        runtime = APIRuntimeSettings(_env_file=env_file)
    except ValidationError as exc:
        raise ValueError(f"Invalid API configuration for {stage.value} environment: {exc}") from exc
    sslmode_env = _resolve_sslmode(env_file)

    logger.info("Environment validation passed")
    return APISettings(
        stage=stage,
        analytics=analytics,
        runtime=runtime,
        sslmode_env=sslmode_env,
    )


@lru_cache
def get_settings() -> APISettings:
    """Cached settings for the process (FastAPI startup, get_config())."""
    return load_api_settings()


def reset_settings() -> None:
    """Clear cached settings (tests)."""
    get_settings.cache_clear()
