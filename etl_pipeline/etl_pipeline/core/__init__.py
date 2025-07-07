"""
ETL Pipeline Core Components
===========================

This package provides the core components for the ETL pipeline,
including connection management and database operations.
"""

from .connections import (
    ConnectionFactory,
    ConnectionManager,
    create_connection_manager
)

# Export clean interface for ETL scripts
__all__ = [
    'ConnectionFactory',
    'ConnectionManager', 
    'create_connection_manager'
]
