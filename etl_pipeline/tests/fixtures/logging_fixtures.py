"""
Logging fixtures for ETL pipeline tests.

This module provides fixtures and utilities for testing logging functionality
in the ETL pipeline, including mock loggers, logging configuration, and
logging-related test data.

Follows the connection architecture patterns:
- Uses provider pattern for dependency injection
- Uses Settings integration for environment-specific logging
- Uses environment separation for test vs production logging
- Uses unified interface with get_logger and ETLLogger
"""

import pytest
import logging
from unittest.mock import MagicMock, patch
from typing import Dict, Any, Optional

from etl_pipeline.config import create_test_settings
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.logging import get_logger, ETLLogger


@pytest.fixture
def test_logging_settings():
    """Test logging settings using provider pattern for dependency injection."""
    # Create test provider with injected logging configuration
    test_provider = DictConfigProvider(
        pipeline={
            'logging': {
                'log_level': 'DEBUG',
                'log_file': 'test_etl.log',
                'log_dir': 'test_logs',
                'format_type': 'detailed',
                'sql_logging': {
                    'enabled': True,
                    'level': 'DEBUG'
                }
            }
        },
        env={
            # Test environment variables for logging
            'TEST_ETL_LOG_LEVEL': 'DEBUG',
            'TEST_ETL_LOG_PATH': 'test_logs',
            'TEST_ETL_LOG_FORMAT': 'detailed',
            'TEST_ETL_SQL_LOGGING': 'true'
        }
    )
    
    # Create test settings with provider injection
    return create_test_settings(
        pipeline_config=test_provider.configs['pipeline'],
        env_vars=test_provider.configs['env']
    )


@pytest.fixture
def mock_logger(test_logging_settings):
    """Mock logger using Settings injection and provider pattern."""
    with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        yield mock_logger


@pytest.fixture
def mock_logging_config(test_logging_settings):
    """Mock logging configuration using Settings injection."""
    return {
        'log_level': 'DEBUG',
        'log_file': 'test_etl.log',
        'log_dir': 'test_logs',
        'format_type': 'detailed',
        'sql_logging': {
            'enabled': True,
            'level': 'DEBUG'
        }
    }


@pytest.fixture
def mock_sql_logging_config(test_logging_settings):
    """Mock SQL logging configuration using Settings injection."""
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
def mock_logging_environment(test_logging_settings):
    """Mock environment variables for logging tests using Settings injection."""
    return {
        'TEST_ETL_LOG_LEVEL': 'DEBUG',
        'TEST_ETL_LOG_PATH': 'test_logs',
        'TEST_ETL_LOG_FORMAT': 'detailed',
        'TEST_ETL_SQL_LOGGING': 'true'
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
def mock_logging_setup(test_logging_settings):
    """Comprehensive mock setup for logging tests using Settings injection."""
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
def mock_etl_logger(test_logging_settings):
    """Mock ETL logger instance using Settings injection."""
    with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Create ETLLogger instance with test settings
        etl_logger = ETLLogger("test_etl_logger", log_level="DEBUG")
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
def mock_logging_context(test_logging_settings):
    """Context manager for comprehensive logging mocking using Settings injection."""
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


@pytest.fixture
def logging_configs_with_settings(test_logging_settings):
    """Test logging configurations using Settings injection."""
    # Test logging configuration from settings
    logging_config = test_logging_settings.pipeline_config.get('logging', {})
    
    return {
        'log_level': logging_config.get('log_level', 'DEBUG'),
        'log_file': logging_config.get('log_file', 'test_etl.log'),
        'log_dir': logging_config.get('log_dir', 'test_logs'),
        'format_type': logging_config.get('format_type', 'detailed'),
        'sql_logging': logging_config.get('sql_logging', {'enabled': True, 'level': 'DEBUG'})
    }


@pytest.fixture
def etl_logger_with_settings(test_logging_settings):
    """ETLLogger instance with Settings injection for testing."""
    with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Create ETLLogger with test settings
        etl_logger = ETLLogger("test_etl_logger", log_level="DEBUG")
        etl_logger.logger = mock_logger
        
        yield etl_logger


@pytest.fixture
def mock_logging_provider():
    """Mock logging provider for testing provider pattern integration."""
    with patch('etl_pipeline.config.logging.DictConfigProvider') as mock_provider:
        mock_provider_instance = MagicMock()
        mock_provider.return_value = mock_provider_instance
        
        # Configure mock provider with test logging config
        mock_provider_instance.get_config.return_value = {
            'logging': {
                'log_level': 'DEBUG',
                'log_file': 'test_etl.log',
                'log_dir': 'test_logs',
                'format_type': 'detailed'
            }
        }
        
        yield mock_provider_instance 