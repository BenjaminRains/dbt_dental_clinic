"""
Logger utilities for ETL Pipeline - UTILITIES ONLY
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