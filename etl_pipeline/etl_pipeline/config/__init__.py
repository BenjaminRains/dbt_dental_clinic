"""
Configuration Management for ETL Pipeline
=========================================

This package provides configuration management capabilities for the ETL pipeline,
including static configuration reading and settings management.

The configuration package is separate from connection handling, which is managed
by the core package following the clean separation of concerns architecture.
"""

from .settings import (
    Settings,
    DatabaseType,
    PostgresSchema,
    get_settings,
    reset_settings,
    set_settings,
    create_settings,
    create_test_settings
)

from .config_reader import ConfigReader

# Export clean interface for configuration management only
__all__ = [
    'Settings',
    'DatabaseType',
    'PostgresSchema',
    'get_settings',
    'reset_settings', 
    'set_settings',
    'create_settings',
    'create_test_settings',
    'ConfigReader'
]

# Note: No default settings instance exported to avoid implicit dependencies
# ETL scripts should explicitly import and use settings as needed