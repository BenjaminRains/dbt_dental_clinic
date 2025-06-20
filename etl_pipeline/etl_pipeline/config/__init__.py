# etl_pipeline/config/__init__.py
"""
Configuration management for the ETL pipeline.

This module provides centralized configuration management through the Settings class,
which handles database connections, pipeline settings, and table configurations.
Configuration can be loaded from environment variables and YAML files.

Exports:
    Settings: Main configuration class that manages all pipeline settings
    settings: Global Settings instance for use throughout the pipeline
    ConfigLoader: Legacy configuration loader for backward compatibility
    ETLConfig: Legacy configuration container for backward compatibility
    load_config: Legacy configuration loading function for backward compatibility
"""
from typing import TYPE_CHECKING

from .settings import Settings, settings
from .loader import ConfigLoader, ETLConfig, load_config

if TYPE_CHECKING:
    from .loader import ConfigLoader, ETLConfig

__all__ = [
    'Settings',
    'settings',
    'ConfigLoader',
    'ETLConfig',
    'load_config'
]