"""
Logger utilities for ETL Pipeline - UTILITIES ONLY

REDUNDANT WITH config/logging.py - NEEDS CONSOLIDATION
=====================================================
This logger module is redundant with the comprehensive logging configuration
in config/logging.py and creates confusion about which logging system to use.

Current Status:
- logger.py: Simple utility functions and ETLLogger class
- config/logging.py: Comprehensive logging configuration with multiple formats
- Both are current code but serve different purposes
- Creates confusion about which logging approach to use

Redundancy Issues:
- Two different logging systems in the same codebase
- logger.py: Basic wrapper around standard logging
- config/logging.py: Full configuration with file/console handlers
- No clear guidance on which system to use
- ETL-specific methods in logger.py could be integrated into main system

Recommendation:
- Consolidate ETL-specific methods into config/logging.py
- Remove basic wrapper functions (get_logger)
- Provide single, comprehensive logging system
- Establish clear logging patterns and guidelines

TODO: Consolidate logging systems to eliminate redundancy
TODO: Integrate ETL-specific methods into config/logging.py
TODO: Establish single logging approach for the pipeline
"""
import logging
from typing import Optional

def get_logger(name: str = "etl_pipeline") -> logging.Logger:
    """Get a logger instance with the specified name."""
    return logging.getLogger(name)

class ETLLogger:
    """ETL Pipeline Logger class for consistent logging across the application."""

    def __init__(self, name: str = "etl_pipeline", log_level: str = "INFO"):
        self.logger = get_logger(name)
        self.log_level = log_level
        # Set the log level
        level = getattr(logging, log_level.upper())
        self.logger.setLevel(level)

    def info(self, message: str, **kwargs) -> None:
        self.logger.info(message, **kwargs)

    def debug(self, message: str, **kwargs) -> None:
        self.logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        self.logger.critical(message, **kwargs)

    def log_sql_query(self, query: str, params: Optional[dict] = None) -> None:
        if params:
            self.debug(f"SQL Query: {query} | Parameters: {params}")
        else:
            self.debug(f"SQL Query: {query}")

    def log_etl_start(self, table_name: str, operation: str) -> None:
        self.info(f"[START] Starting {operation} for table: {table_name}")

    def log_etl_complete(self, table_name: str, operation: str, records_count: int = 0) -> None:
        self.info(f"[PASS] Completed {operation} for table: {table_name} | Records: {records_count}")

    def log_etl_error(self, table_name: str, operation: str, error: Exception) -> None:
        self.error(f"[FAIL] Error during {operation} for table: {table_name} | Error: {error}")

    def log_validation_result(self, table_name: str, passed: bool, issues_count: int = 0) -> None:
        if passed:
            self.info(f"[PASS] Validation passed for table: {table_name}")
        else:
            self.warning(f"[WARN] Validation failed for table: {table_name} | Issues: {issues_count}") 