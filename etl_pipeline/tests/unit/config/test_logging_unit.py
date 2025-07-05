"""
Unit tests for the unified logging system.
Fast, isolated tests with comprehensive mocking.
"""
import pytest
from unittest.mock import patch, MagicMock
import logging
import os
from pathlib import Path

from etl_pipeline.config.logging import (
    setup_logging, 
    configure_sql_logging, 
    init_default_logger,
    get_logger,
    ETLLogger
)

# Import fixtures from the new modular structure
from tests.fixtures.logging_fixtures import (
    mock_logger,
    mock_logging_config,
    mock_sql_logging_config,
    mock_logging_handlers,
    mock_logging_formatters,
    mock_os_operations,
    sample_log_messages,
    mock_logging_environment,
    mock_logging_get_logger,
    mock_logging_basic_config,
    mock_logging_setup,
    mock_sql_loggers,
    mock_etl_logger,
    sample_log_records,
    mock_logging_context
)


@pytest.mark.unit
class TestSetupLoggingUnit:
    """Unit tests for logging setup functionality."""

    def test_setup_logging_basic_configuration(self, mock_logging_setup):
        """Test basic logging configuration with mocks."""
        mocks = mock_logging_setup
        
        setup_logging(log_level="INFO", log_file="test.log", log_dir="logs", format_type="detailed")
        
        # Verify logger configuration
        mocks['logger'].setLevel.assert_called_with(logging.INFO)
        mocks['logger'].addHandler.assert_called()
        mocks['makedirs'].assert_called_once_with("logs", exist_ok=True)

    def test_setup_logging_without_file_handler(self, mock_logging_setup):
        """Test setup_logging when no log file is specified."""
        mocks = mock_logging_setup
        
        setup_logging(log_level="DEBUG", format_type="simple")
        
        # Should only add console handler, not file handler
        mocks['logger'].setLevel.assert_called_with(logging.DEBUG)
        mocks['file_handler_class'].assert_not_called()

    def test_setup_logging_format_types(self, mock_logging_setup):
        """Test different format types for logging."""
        mocks = mock_logging_setup
        
        # Test simple format
        setup_logging(format_type="simple")
        mocks['logger'].addHandler.assert_called()
        
        # Test json format
        mocks['logger'].reset_mock()
        setup_logging(format_type="json")
        mocks['logger'].addHandler.assert_called()
        
        # Test invalid format (should default to detailed)
        mocks['logger'].reset_mock()
        setup_logging(format_type="invalid")
        mocks['logger'].addHandler.assert_called()

    def test_setup_logging_log_levels(self, mock_logging_setup):
        """Test different log levels."""
        mocks = mock_logging_setup
        
        # Test different log levels
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            mocks['logger'].reset_mock()
            setup_logging(log_level=level)
            mocks['logger'].setLevel.assert_called_with(getattr(logging, level))


@pytest.mark.unit
class TestSQLLoggingUnit:
    """Unit tests for SQL logging configuration."""

    def test_configure_sql_logging_enabled(self, mock_sql_loggers):
        """Test SQL logging configuration when enabled."""
        configure_sql_logging(enabled=True, level="DEBUG")
        
        # Should be called 4 times (one for each SQL logger)
        for logger_name, mock_logger in mock_sql_loggers.items():
            mock_logger.setLevel.assert_called_with(logging.DEBUG)

    def test_configure_sql_logging_disabled(self, mock_sql_loggers):
        """Test SQL logging configuration when disabled."""
        configure_sql_logging(enabled=False)
        
        # Should be called 4 times with WARNING level
        for logger_name, mock_logger in mock_sql_loggers.items():
            mock_logger.setLevel.assert_called_with(logging.WARNING)

    def test_configure_sql_logging_different_levels(self, mock_sql_loggers):
        """Test SQL logging with different levels."""
        # Test different levels
        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            # Reset all loggers
            for mock_logger in mock_sql_loggers.values():
                mock_logger.reset_mock()
            
            configure_sql_logging(enabled=True, level=level)
            
            for logger_name, mock_logger in mock_sql_loggers.items():
                mock_logger.setLevel.assert_called_with(getattr(logging, level))


@pytest.mark.unit
class TestGetLoggerUnit:
    """Unit tests for get_logger function."""

    def test_get_logger_default_name(self):
        """Test get_logger with default name."""
        # Don't mock this test - it should test the actual functionality
        logger = get_logger()
        assert isinstance(logger, logging.Logger)
        assert logger.name == "etl_pipeline"

    def test_get_logger_custom_name(self):
        """Test get_logger with custom name."""
        # Don't mock this test - it should test the actual functionality
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_multiple_calls(self):
        """Test that multiple calls return the same logger instance."""
        # Don't mock this test - it should test the actual functionality
        logger1 = get_logger("test_module")
        logger2 = get_logger("test_module")
        assert logger1 is logger2  # Same instance


@pytest.mark.unit
class TestETLLoggerUnit:
    """Unit tests for ETLLogger class."""

    def test_etl_logger_initialization_default(self, mock_logger):
        """Test ETLLogger initialization with default parameters."""
        etl_logger = ETLLogger()
        assert etl_logger.logger == mock_logger
        # Should not set level when not specified
        mock_logger.setLevel.assert_not_called()

    def test_etl_logger_initialization_with_level(self, mock_logger):
        """Test ETLLogger initialization with custom log level."""
        etl_logger = ETLLogger("test_module", "DEBUG")
        assert etl_logger.logger == mock_logger
        mock_logger.setLevel.assert_called_with(logging.DEBUG)

    def test_etl_logger_initialization_with_invalid_level(self, mock_logger):
        """Test ETLLogger initialization with invalid log level."""
        with pytest.raises(AttributeError):
            ETLLogger("test_module", "INVALID_LEVEL")

    def test_basic_logging_methods(self, mock_logger):
        """Test basic logging methods with kwargs."""
        etl_logger = ETLLogger()
        
        # Test all basic logging methods
        etl_logger.info("test info", extra={"key": "value"})
        etl_logger.debug("test debug", extra={"key": "value"})
        etl_logger.warning("test warning", extra={"key": "value"})
        etl_logger.error("test error", extra={"key": "value"})
        etl_logger.critical("test critical", extra={"key": "value"})
        
        # Verify calls
        mock_logger.info.assert_called_with("test info", extra={"key": "value"})
        mock_logger.debug.assert_called_with("test debug", extra={"key": "value"})
        mock_logger.warning.assert_called_with("test warning", extra={"key": "value"})
        mock_logger.error.assert_called_with("test error", extra={"key": "value"})
        mock_logger.critical.assert_called_with("test critical", extra={"key": "value"})

    def test_sql_query_logging_with_params(self, mock_logger):
        """Test SQL query logging with parameters."""
        etl_logger = ETLLogger()
        etl_logger.log_sql_query("SELECT * FROM table", {"param": "value"})
        
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "SELECT * FROM table" in call_args[0]
        assert "param" in call_args[0]
        assert "value" in call_args[0]

    def test_sql_query_logging_without_params(self, mock_logger):
        """Test SQL query logging without parameters."""
        etl_logger = ETLLogger()
        etl_logger.log_sql_query("SELECT * FROM table")
        
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "SELECT * FROM table" in call_args[0]
        assert "Parameters" not in call_args[0]

    def test_etl_operation_logging_start(self, mock_logger):
        """Test ETL start operation logging."""
        etl_logger = ETLLogger()
        etl_logger.log_etl_start("test_table", "extraction")
        
        mock_logger.info.assert_called_with("[START] Starting extraction for table: test_table")

    def test_etl_operation_logging_complete(self, mock_logger):
        """Test ETL complete operation logging."""
        etl_logger = ETLLogger()
        etl_logger.log_etl_complete("test_table", "extraction", 100)
        
        mock_logger.info.assert_called_with("[PASS] Completed extraction for table: test_table | Records: 100")

    def test_etl_operation_logging_error(self, mock_logger):
        """Test ETL error operation logging."""
        etl_logger = ETLLogger()
        test_error = Exception("test error")
        etl_logger.log_etl_error("test_table", "extraction", test_error)
        
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: test_table | Error: test error")

    def test_validation_logging_passed(self, mock_logger):
        """Test validation result logging when passed."""
        etl_logger = ETLLogger()
        etl_logger.log_validation_result("test_table", True)
        
        mock_logger.info.assert_called_with("[PASS] Validation passed for table: test_table")

    def test_validation_logging_failed(self, mock_logger):
        """Test validation result logging when failed."""
        etl_logger = ETLLogger()
        etl_logger.log_validation_result("test_table", False, 5)
        
        mock_logger.warning.assert_called_with("[WARN] Validation failed for table: test_table | Issues: 5")

    def test_performance_logging_with_records(self, mock_logger):
        """Test performance logging with record count."""
        etl_logger = ETLLogger()
        etl_logger.log_performance("test_operation", 2.5, 1000)
        
        mock_logger.info.assert_called_with("[PERF] test_operation completed in 2.50s | 1000 records | 400 records/sec")

    def test_performance_logging_without_records(self, mock_logger):
        """Test performance logging without record count."""
        etl_logger = ETLLogger()
        etl_logger.log_performance("test_operation", 1.5)
        
        mock_logger.info.assert_called_with("[PERF] test_operation completed in 1.50s")

    def test_performance_logging_zero_duration(self, mock_logger):
        """Test performance logging with zero duration."""
        etl_logger = ETLLogger()
        etl_logger.log_performance("test_operation", 0.0, 100)
        
        mock_logger.info.assert_called_with("[PERF] test_operation completed in 0.00s | 100 records | 0 records/sec")

    def test_performance_logging_edge_cases(self, mock_logger):
        """Test performance logging edge cases."""
        etl_logger = ETLLogger()
        
        # Test very small duration
        etl_logger.log_performance("fast_operation", 0.001, 1000)
        mock_logger.info.assert_called_with("[PERF] fast_operation completed in 0.00s | 1000 records | 1000000 records/sec")
        
        # Test very large duration
        mock_logger.reset_mock()
        etl_logger.log_performance("slow_operation", 3600.0, 1000000)
        mock_logger.info.assert_called_with("[PERF] slow_operation completed in 3600.00s | 1000000 records | 278 records/sec")

    def test_etl_logger_with_sample_messages(self, mock_logger, sample_log_messages):
        """Test ETL logger with sample log messages."""
        etl_logger = ETLLogger()
        
        # Test with sample messages
        etl_logger.info(sample_log_messages['info'])
        etl_logger.debug(sample_log_messages['debug'])
        etl_logger.warning(sample_log_messages['warning'])
        etl_logger.error(sample_log_messages['error'])
        etl_logger.critical(sample_log_messages['critical'])
        
        # Verify calls
        mock_logger.info.assert_called_with(sample_log_messages['info'])
        mock_logger.debug.assert_called_with(sample_log_messages['debug'])
        mock_logger.warning.assert_called_with(sample_log_messages['warning'])
        mock_logger.error.assert_called_with(sample_log_messages['error'])
        mock_logger.critical.assert_called_with(sample_log_messages['critical'])

    def test_etl_logger_sql_query_with_sample_data(self, mock_logger, sample_log_messages):
        """Test ETL logger SQL query logging with sample data."""
        etl_logger = ETLLogger()
        etl_logger.log_sql_query(sample_log_messages['sql_query'], sample_log_messages['sql_params'])
        
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert sample_log_messages['sql_query'] in call_args[0]
        assert 'id' in call_args[0]
        assert '123' in call_args[0]


@pytest.mark.unit
class TestInitDefaultLoggerUnit:
    """Unit tests for default logger initialization."""

    def test_init_default_logger_success(self, mock_logging_environment):
        """Test successful default logger initialization."""
        with patch('os.getenv', side_effect=lambda key, default: mock_logging_environment.get(key, default)), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="INFO", 
                log_file="etl_pipeline.log", 
                log_dir="logs", 
                format_type="detailed"
            )

    def test_init_default_logger_with_context_manager(self, mock_logging_context):
        """Test default logger initialization using context manager fixture."""
        with mock_logging_context as mocks:
            # Mock the setup_logging function
            with patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
                init_default_logger()
                
                # Verify that setup_logging was called
                mock_setup_logging.assert_called_once()
                
                # Verify that the logger was created
                assert mocks['logger'] is not None

    def test_init_default_logger_with_custom_env_vars(self):
        """Test default logger initialization with custom environment variables."""
        custom_env = {
            'ETL_LOG_LEVEL': 'DEBUG',
            'ETL_LOG_PATH': 'custom_logs',
            'ETL_LOG_FORMAT': 'detailed'
        }
        with patch('os.getenv', side_effect=lambda key, default: custom_env.get(key, default)), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="DEBUG", 
                log_file="etl_pipeline.log", 
                log_dir="custom_logs", 
                format_type="detailed"
            )

    def test_init_default_logger_exception_handling(self, mock_logging_environment, mock_logging_basic_config):
        """Test default logger initialization when setup fails."""
        with patch('os.getenv', side_effect=lambda key, default: mock_logging_environment.get(key, default)), \
             patch('etl_pipeline.config.logging.setup_logging', side_effect=Exception("Test error")), \
             patch('builtins.print') as mock_print:
            
            init_default_logger()
            
            # Should fall back to basic config
            mock_logging_basic_config.assert_called_once()
            mock_print.assert_called_once()
            assert "Warning: Could not set up advanced logging" in mock_print.call_args[0][0]

    def test_init_default_logger_environment_variable_handling(self):
        """Test environment variable handling in init_default_logger."""
        # Test with None values (should use defaults)
        # os.getenv returns the default value when key doesn't exist
        with patch('os.getenv', side_effect=lambda key, default: default), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="INFO", 
                log_file="etl_pipeline.log", 
                log_dir="logs", 
                format_type="detailed"
            ) 