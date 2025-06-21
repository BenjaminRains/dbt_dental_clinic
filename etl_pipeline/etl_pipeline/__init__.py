"""
ETL Pipeline for Dental Clinic Data
"""

__version__ = "0.1.0"

# Make cli module available at package level
from .cli import get_cli

# Make core module available
from . import core

__all__ = ['get_cli', 'core'] 