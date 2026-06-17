"""
ETL Pipeline for Dental Clinic Data
"""

__version__ = "0.1.0"

# Avoid importing core/cli here — config-only imports (e.g. mdc status → settings_v2)
# must not require SQLAlchemy or other runtime DB dependencies.

__all__ = ["__version__"] 