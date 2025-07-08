# etl_pipeline/config/logging.py
"""
Unified logging configuration for ETL Pipeline

CONSOLIDATED LOGGING SYSTEM
===========================
This module provides a unified logging system that consolidates:
- Comprehensive logging configuration (file/console handlers, formats)
- ETL-specific logging methods (operations, validation, SQL queries)
- Environment variable support
- Auto-initialization

Usage:
    from etl_pipeline.config.logging import get_logger, ETLLogger
    
    # Simple usage
    logger = get_logger(__name__)
    logger.info("Processing table...")
    
    # ETL-specific usage
    etl_logger = ETLLogger(__name__)
    etl_logger.log_etl_start("patients", "extraction")
    etl_logger.log_etl_complete("patients", "extraction", 1000)
"""
import logging
import logging.config
import os
from pathlib import Path
from typing import Optional
import sys


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: Optional[str] = None,
    format_type: str = "detailed"
) -> None:
    """
    Set up logging configuration for the ETL pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file name
        log_dir: Optional log directory path
        format_type: Format type ('simple', 'detailed', 'json')
    """
    # Ensure log directory exists
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Define format strings
    formats = {
        "simple": "%(levelname)s - %(message)s",
        "detailed": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "json": "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    }
    
    formatter = logging.Formatter(
        formats.get(format_type, formats["detailed"]),
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        if log_dir:
            log_path = Path(log_dir) / log_file
        else:
            log_path = Path(log_file)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        root_logger.addHandler(file_handler)
    
    # ETL specific logger
    etl_logger = logging.getLogger("etl_pipeline")
    etl_logger.setLevel(getattr(logging, log_level.upper()))


def configure_sql_logging(enabled: bool = False, level: str = "WARNING") -> None:
    """
    Configure SQL query logging.
    
    Args:
        enabled: Whether to enable SQL logging
        level: SQL logging level
    """
    sql_loggers = [
        "sqlalchemy.engine",
        "sqlalchemy.dialects",
        "sqlalchemy.pool",
        "sqlalchemy.orm"
    ]
    
    for logger_name in sql_loggers:
        logger = logging.getLogger(logger_name)
        if enabled:
            logger.setLevel(getattr(logging, level.upper()))
        else:
            logger.setLevel(logging.WARNING)


def get_logger(name: str = "etl_pipeline") -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


class ETLLogger:
    """
    Enhanced ETL Pipeline Logger class for consistent logging across the application.
    
    This class provides both standard logging methods and ETL-specific logging
    methods for common ETL operations.
    """

    def __init__(self, name: str = "etl_pipeline", log_level: Optional[str] = None):
        """
        Initialize ETL Logger.
        
        Args:
            name: Logger name (typically __name__)
            log_level: Optional log level override
        """
        self.logger = get_logger(name)
        if log_level:
            level = getattr(logging, log_level.upper())
            self.logger.setLevel(level)

    # Standard logging methods
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, **kwargs)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self.logger.critical(message, **kwargs)

    # ETL-specific logging methods
    def log_sql_query(self, query: str, params: Optional[dict] = None) -> None:
        """
        Log SQL query for debugging purposes.
        
        Args:
            query: SQL query string
            params: Optional query parameters
        """
        if params:
            self.debug(f"SQL Query: {query} | Parameters: {params}")
        else:
            self.debug(f"SQL Query: {query}")

    def log_etl_start(self, table_name: str, operation: str) -> None:
        """
        Log the start of an ETL operation.
        
        Args:
            table_name: Name of the table being processed
            operation: Type of operation (extraction, transformation, loading)
        """
        self.info(f"[START] Starting {operation} for table: {table_name}")

    def log_etl_complete(self, table_name: str, operation: str, records_count: int = 0) -> None:
        """
        Log the successful completion of an ETL operation.
        
        Args:
            table_name: Name of the table that was processed
            operation: Type of operation that was completed
            records_count: Number of records processed
        """
        self.info(f"[PASS] Completed {operation} for table: {table_name} | Records: {records_count}")

    def log_etl_error(self, table_name: str, operation: str, error: Optional[Exception]) -> None:
        """
        Log an error during an ETL operation.
        
        Args:
            table_name: Name of the table being processed
            operation: Type of operation that failed
            error: Exception that occurred (can be None)
        """
        self.error(f"[FAIL] Error during {operation} for table: {table_name} | Error: {error}")

    def log_validation_result(self, table_name: str, passed: bool, issues_count: int = 0) -> None:
        """
        Log the result of data validation.
        
        Args:
            table_name: Name of the table that was validated
            passed: Whether validation passed
            issues_count: Number of validation issues found
        """
        if passed:
            self.info(f"[PASS] Validation passed for table: {table_name}")
        else:
            self.warning(f"[WARN] Validation failed for table: {table_name} | Issues: {issues_count}")

    def log_performance(self, operation: str, duration: float, records_count: int = 0) -> None:
        """
        Log performance metrics for an operation.
        
        Args:
            operation: Name of the operation
            duration: Duration in seconds
            records_count: Number of records processed
        """
        if records_count > 0:
            rate = records_count / duration if duration > 0 else 0
            self.info(f"[PERF] {operation} completed in {duration:.2f}s | {records_count} records | {rate:.0f} records/sec")
        else:
            self.info(f"[PERF] {operation} completed in {duration:.2f}s")


# Default configuration dictionary
DEFAULT_LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "simple": {
            "format": "%(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "detailed",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "etl_pipeline.log",
            "mode": "a"
        }
    },
    "loggers": {
        "etl_pipeline": {
            "level": "INFO",
            "handlers": ["console", "file"],
            "propagate": False
        },
        "sqlalchemy.engine": {
            "level": "WARNING",
            "handlers": ["console"],
            "propagate": False
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"]
    }
}


def init_default_logger():
    """Initialize default logger configuration."""
    try:
        # Try to get log directory from environment or use default
        log_dir = os.getenv("ETL_LOG_PATH", "logs")
        log_level = os.getenv("ETL_LOG_LEVEL", "INFO")
        
        setup_logging(
            log_level=log_level,
            log_file="etl_pipeline.log",
            log_dir=log_dir,
            format_type="detailed"
        )
    except Exception as e:
        # Fallback to basic console logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        print(f"Warning: Could not set up advanced logging: {e}")


# Auto-initialize when module is imported
init_default_logger()