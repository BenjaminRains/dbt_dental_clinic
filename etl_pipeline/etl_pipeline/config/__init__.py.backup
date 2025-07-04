# etl_pipeline/config/__init__.py
"""
Configuration management for the ETL pipeline.

This module provides centralized configuration management through the Settings class,
which handles database connections, pipeline settings, and table configurations.
Configuration can be loaded from environment variables and YAML files.

Exports:
    Settings: Main configuration class that manages all pipeline settings
    settings: Global Settings instance for use throughout the pipeline
"""
from typing import TYPE_CHECKING

from .settings import Settings, settings

if TYPE_CHECKING:
    pass

__all__ = [
    'Settings',
    'settings'
]