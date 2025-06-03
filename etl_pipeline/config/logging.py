# etl_pipeline/config/logging.py
"""
Logging configuration for ETL Pipeline - CONFIGURATION ONLY
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