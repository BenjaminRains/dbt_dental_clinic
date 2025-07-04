"""
Step 1.3: Replace etl_pipeline/config/__init__.py

BACKUP YOUR EXISTING FILE FIRST!
cp etl_pipeline/config/__init__.py etl_pipeline/config/__init__.py.backup

Then replace with this clean implementation.
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

# Export clean interface
__all__ = [
    'Settings',
    'DatabaseType',
    'PostgresSchema',
    'get_settings',
    'reset_settings', 
    'set_settings',
    'create_settings',
    'create_test_settings',
    'settings'  # For backward compatibility
]

# Default settings instance (lazy-loaded)
settings = get_settings()