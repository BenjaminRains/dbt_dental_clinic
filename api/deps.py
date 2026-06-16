"""FastAPI dependencies for typed API configuration (Phase 2)."""

from typing import Optional

from settings import APISettings, get_settings


def get_api_settings() -> APISettings:
    """Return cached typed API settings for Depends() injection."""
    return get_settings()


def get_api_settings_optional() -> Optional[APISettings]:
    """Return typed settings when configured; None if API_ENVIRONMENT is unset."""
    try:
        return get_settings()
    except ValueError:
        return None
