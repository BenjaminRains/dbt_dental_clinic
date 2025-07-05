"""
Logging fixtures for ETL pipeline tests.

This module provides fixtures and utilities for testing logging functionality
in the ETL pipeline, including mock loggers, logging configuration, and
logging-related test data.
"""

import pytest
import logging
from unittest.mock import MagicMock, patch
from typing import Dict, Any, Optional


@pytest.fixture
def mock_logger():
    """Mock logger for testing logging functionality."""
    with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        yield mock_logger


@pytest.fixture
def mock_logging_config():
    """Mock logging configuration for testing."""
    return {
        'log_level': 'INFO',
        'log_file': 'test.log',
        'log_dir': 'logs',
        'format_type': 'detailed'
    }


@pytest.fixture
def mock_sql_logging_config():
    """Mock SQL logging configuration for testing."""
    return {
        'enabled': True,
        'level': 'DEBUG'
    }


@pytest.fixture
def mock_logging_handlers():
    """Mock logging handlers for testing."""
    with patch('logging.StreamHandler') as mock_stream_handler, \
         patch('logging.FileHandler') as mock_file_handler:
        
        mock_stream = MagicMock()
        mock_file = MagicMock()
        mock_stream_handler.return_value = mock_stream
        mock_file_handler.return_value = mock_file
        
        yield {
            'stream_handler': mock_stream,
            'file_handler': mock_file,
            'stream_handler_class': mock_stream_handler,
            'file_handler_class': mock_file_handler
        }


@pytest.fixture
def mock_logging_formatters():
    """Mock logging formatters for testing."""
    with patch('logging.Formatter') as mock_formatter:
        mock_formatter_instance = MagicMock()
        mock_formatter.return_value = mock_formatter_instance
        yield mock_formatter_instance


@pytest.fixture
def mock_os_operations():
    """Mock OS operations for logging tests."""
    with patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as mock_exists:
        
        mock_makedirs.return_value = None
        mock_exists.return_value = True
        
        yield {
            'makedirs': mock_makedirs,
            'exists': mock_exists
        }


@pytest.fixture
def sample_log_messages():
    """Sample log messages for testing."""
    return {
        'info': 'Test info message',
        'debug': 'Test debug message',
        'warning': 'Test warning message',
        'error': 'Test error message',
        'critical': 'Test critical message',
        'sql_query': 'SELECT * FROM test_table WHERE id = %s',
        'sql_params': {'id': 123},
        'etl_start': 'Starting extraction for table: test_table',
        'etl_complete': 'Completed extraction for table: test_table | Records: 100',
        'etl_error': 'Error during extraction for table: test_table | Error: test error',
        'validation_passed': 'Validation passed for table: test_table',
        'validation_failed': 'Validation failed for table: test_table | Issues: 5',
        'performance': 'test_operation completed in 2.50s | 1000 records | 400 records/sec'
    }


@pytest.fixture
def mock_logging_environment():
    """Mock environment variables for logging tests."""
    return {
        'ETL_LOG_LEVEL': 'INFO',
        'ETL_LOG_PATH': 'logs',
        'ETL_LOG_FORMAT': 'detailed'
    }


@pytest.fixture
def mock_logging_get_logger():
    """Mock get_logger function for testing."""
    with patch('logging.getLogger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        yield mock_get_logger


@pytest.fixture
def mock_logging_basic_config():
    """Mock basic logging configuration for testing."""
    with patch('logging.basicConfig') as mock_basic_config:
        yield mock_basic_config


@pytest.fixture
def mock_logging_setup():
    """Comprehensive mock setup for logging tests."""
    with patch('logging.getLogger') as mock_get_logger, \
         patch('logging.StreamHandler') as mock_stream_handler, \
         patch('logging.FileHandler') as mock_file_handler, \
         patch('logging.Formatter') as mock_formatter, \
         patch('os.makedirs') as mock_makedirs, \
         patch('os.path.exists') as mock_exists:
        
        mock_logger = MagicMock()
        mock_stream = MagicMock()
        mock_file = MagicMock()
        mock_formatter_instance = MagicMock()
        
        mock_get_logger.return_value = mock_logger
        mock_stream_handler.return_value = mock_stream
        mock_file_handler.return_value = mock_file
        mock_formatter.return_value = mock_formatter_instance
        mock_makedirs.return_value = None
        mock_exists.return_value = True
        
        yield {
            'logger': mock_logger,
            'stream_handler': mock_stream,
            'file_handler': mock_file,
            'formatter': mock_formatter_instance,
            'get_logger': mock_get_logger,
            'stream_handler_class': mock_stream_handler,
            'file_handler_class': mock_file_handler,
            'formatter_class': mock_formatter,
            'makedirs': mock_makedirs,
            'exists': mock_exists
        }


@pytest.fixture
def mock_sql_loggers():
    """Mock SQL loggers for testing SQL logging functionality."""
    sql_logger_names = [
        'sqlalchemy.engine',
        'sqlalchemy.pool',
        'sqlalchemy.dialects',
        'sqlalchemy.orm'
    ]
    
    mock_loggers = {}
    
    with patch('logging.getLogger') as mock_get_logger:
        # Create individual mock loggers for each SQL logger
        for logger_name in sql_logger_names:
            mock_logger = MagicMock()
            mock_loggers[logger_name] = mock_logger
        
        # Configure the mock to return different loggers based on the name
        def get_logger_side_effect(name=None):
            if name in sql_logger_names:
                return mock_loggers[name]
            return MagicMock()
        
        mock_get_logger.side_effect = get_logger_side_effect
        
        yield mock_loggers


@pytest.fixture
def mock_etl_logger():
    """Mock ETL logger instance for testing."""
    with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Mock the ETLLogger class methods
        from etl_pipeline.config.logging import ETLLogger
        
        # Create a mock instance
        etl_logger = ETLLogger.__new__(ETLLogger)
        etl_logger.logger = mock_logger
        
        yield etl_logger


@pytest.fixture
def sample_log_records():
    """Sample log records for testing log processing."""
    return [
        {
            'level': 'INFO',
            'message': 'Test info message',
            'timestamp': '2024-01-01 12:00:00',
            'module': 'test_module',
            'function': 'test_function'
        },
        {
            'level': 'ERROR',
            'message': 'Test error message',
            'timestamp': '2024-01-01 12:01:00',
            'module': 'test_module',
            'function': 'test_function',
            'exception': 'TestException'
        },
        {
            'level': 'DEBUG',
            'message': 'SQL Query: SELECT * FROM test_table',
            'timestamp': '2024-01-01 12:02:00',
            'module': 'database',
            'function': 'execute_query',
            'sql_params': {'id': 123}
        }
    ]


@pytest.fixture
def mock_logging_context():
    """Context manager for comprehensive logging mocking."""
    class LoggingContext:
        def __init__(self):
            self.patches = []
            self.mocks = {}
        
        def __enter__(self):
            # Set up all the patches
            patches_config = [
                ('logging.getLogger', 'get_logger'),
                ('logging.StreamHandler', 'stream_handler'),
                ('logging.FileHandler', 'file_handler'),
                ('logging.Formatter', 'formatter'),
                ('os.makedirs', 'makedirs'),
                ('os.path.exists', 'exists'),
                ('logging.basicConfig', 'basic_config')
            ]
            
            for target, name in patches_config:
                patch_obj = patch(target)
                mock_obj = patch_obj.start()
                self.patches.append(patch_obj)
                self.mocks[name] = mock_obj
            
            # Set up return values
            mock_logger = MagicMock()
            self.mocks['get_logger'].return_value = mock_logger
            self.mocks['logger'] = mock_logger
            
            return self.mocks
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            # Clean up all patches
            for patch_obj in self.patches:
                patch_obj.stop()
    
    return LoggingContext() 