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
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from typing import Any, Tuple, Type


class IgnoreBlankEnvSettingsSource(PydanticBaseSettingsSource):
    """Drop blank env / dotenv values so on-disk defaults are not overridden by stale shell blanks."""

    def __init__(self, settings_cls: Type[BaseSettings], source: PydanticBaseSettingsSource):
        super().__init__(settings_cls)
        self.source = source

    def get_field_value(self, field, field_name):
        return self.source.get_field_value(field, field_name)

    def prepare_field_value(self, field_name, field, value, value_is_complex):
        return self.source.prepare_field_value(field_name, field, value, value_is_complex)

    def __call__(self) -> dict[str, Any]:
        data = self.source()
        return {
            key: value
            for key, value in data.items()
            if not (isinstance(value, str) and not value.strip())
        }

    @property
    def field_mapping(self):
        return self.source.field_mapping


class BlankEnvAwareSettings(BaseSettings):
    """Base settings that ignore empty-string env values (common in .env files and shells)."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            IgnoreBlankEnvSettingsSource(settings_cls, env_settings),
            IgnoreBlankEnvSettingsSource(settings_cls, dotenv_settings),
            file_secret_settings,
        )

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


class AnalyticsDBSettings(BlankEnvAwareSettings):
    """Analytics Postgres connection; env prefix is set per stage at load time."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    host: str
    port: int = 5432
    db: str
    user: str
    password: SecretStr


class APIRuntimeSettings(BlankEnvAwareSettings):
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


def _analytics_env_complete_in_os(host_key: str, prefix: str) -> bool:
    """Phase 0: OS env is authoritative only when analytics creds are fully present (non-blank)."""
    host = (os.getenv(host_key) or "").strip()
    if not host:
        return False
    for suffix in ("DB", "USER", "PASSWORD"):
        if not (os.getenv(f"{prefix}{suffix}") or "").strip():
            return False
    return True


def _env_file_for_stage(stage: Stage) -> Optional[Path]:
    """
    Return the stage .env file to load, or None when OS env is authoritative (Phase 0).

    When the analytics host var is already in the process environment (systemd, Docker,
    environment_manager.ps1), do not read a second on-disk .env_api_* file.
    """
    host_key = HOST_ENV_KEYS[stage.value]
    prefix = STAGE_PREFIX[stage.value]
    if _analytics_env_complete_in_os(host_key, prefix):
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


def _fill_missing_clinic_env_from_stage_file(stage: Stage) -> None:
    """
    Fill missing CLINIC_* values from stage file when analytics OS env skips dotenv.

    This is fill-only and does not override any existing non-blank process values.
    """
    env_file = API_DIR / f".env_api_{stage.value}"
    if not env_file.exists():
        return
    try:
        from dotenv import dotenv_values

        values = dotenv_values(env_file) or {}
    except Exception:
        return

    for key, value in values.items():
        if not key.startswith("CLINIC_"):
            continue
        if value is None or not str(value).strip():
            continue
        if (os.getenv(key) or "").strip():
            continue
        os.environ[key] = str(value).strip()


def load_api_settings(*, environment: Optional[str] = None) -> APISettings:
    """Load and validate settings (not cached — use get_settings() in production)."""
    stage = _detect_stage(environment)
    prefix = STAGE_PREFIX[stage.value]
    env_file = _env_file_for_stage(stage)
    if env_file is None:
        _fill_missing_clinic_env_from_stage_file(stage)

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


def export_api_env_dict(*, environment: Optional[str] = None) -> dict[str, str]:
    """
    Validate API config and return a flat env dict for PowerShell api-init (Phase 3).

    Runs pydantic validation first, then merges stage file values with OS env (OS wins).
    """
    stage = _detect_stage(environment)
    os.environ["API_ENVIRONMENT"] = stage.value
    load_api_settings(environment=stage.value)

    env_file = API_DIR / f".env_api_{stage.value}"
    result: dict[str, str] = {}

    if env_file.exists():
        from dotenv import dotenv_values

        for key, value in (dotenv_values(env_file) or {}).items():
            if value is not None and str(value).strip():
                result[key] = str(value).strip()

    for key in list(result.keys()):
        os_val = os.getenv(key)
        if os_val is not None and str(os_val).strip():
            result[key] = os_val.strip()

    for key, val in os.environ.items():
        if not val:
            continue
        if key in ("API_ENVIRONMENT",) or key.startswith(
            ("API_", "POSTGRES_", "DEMO_", "TEST_", "CLINIC_", "PGSSL")
        ):
            result[key] = val

    result["API_ENVIRONMENT"] = stage.value
    return result
