"""
Comprehensive tests for the unified logging system.
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


class TestSetupLogging:
    """Test logging setup functionality."""

    def test_setup_logging(self):
        """Test that setup_logging configures the root logger correctly."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler, \
             patch('logging.FileHandler') as mock_file_handler, \
             patch('os.makedirs') as mock_makedirs:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            mock_file_handler.return_value = MagicMock()
            setup_logging(log_level="INFO", log_file="test.log", log_dir="logs", format_type="detailed")
            mock_logger.setLevel.assert_called_with(logging.INFO)
            mock_logger.addHandler.assert_called()

    def test_setup_logging_without_log_dir(self):
        """Test setup_logging when log_dir is not provided."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler, \
             patch('logging.FileHandler') as mock_file_handler, \
             patch('os.makedirs') as mock_makedirs:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            mock_file_handler.return_value = MagicMock()
            setup_logging(log_level="INFO", log_file="test.log", log_dir=None, format_type="detailed")
            mock_makedirs.assert_not_called()
            mock_logger.addHandler.assert_called()

    def test_setup_logging_different_formats(self):
        """Test setup_logging with different format types."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            
            # Test simple format
            setup_logging(format_type="simple")
            mock_logger.addHandler.assert_called()
            
            # Test json format
            mock_logger.reset_mock()
            setup_logging(format_type="json")
            mock_logger.addHandler.assert_called()


class TestSQLLogging:
    """Test SQL logging configuration."""

    def test_configure_sql_logging_enabled(self):
        """Test that configure_sql_logging sets the correct log level for SQL loggers."""
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            configure_sql_logging(enabled=True, level="DEBUG")
            mock_logger.setLevel.assert_called_with(logging.DEBUG)

    def test_configure_sql_logging_disabled(self):
        """Test configure_sql_logging when SQL logging is disabled."""
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            configure_sql_logging(enabled=False)
            mock_logger.setLevel.assert_called_with(logging.WARNING)


class TestGetLogger:
    """Test get_logger function."""

    def test_get_logger_default(self):
        """Test get_logger with default name."""
        logger = get_logger()
        assert isinstance(logger, logging.Logger)
        assert logger.name == "etl_pipeline"

    def test_get_logger_custom_name(self):
        """Test get_logger with custom name."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"


class TestETLLogger:
    """Test ETLLogger class."""

    @pytest.fixture
    def mock_logger(self):
        """Mock logger for testing."""
        with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            yield mock_logger

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

    def test_basic_logging_methods(self, mock_logger):
        """Test basic logging methods with kwargs."""
        etl_logger = ETLLogger()
        etl_logger.info("test info", extra={"key": "value"})
        etl_logger.debug("test debug", extra={"key": "value"})
        etl_logger.warning("test warning", extra={"key": "value"})
        etl_logger.error("test error", extra={"key": "value"})
        etl_logger.critical("test critical", extra={"key": "value"})
        
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

    def test_sql_query_logging_without_params(self, mock_logger):
        """Test SQL query logging without parameters."""
        etl_logger = ETLLogger()
        etl_logger.log_sql_query("SELECT * FROM table")
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "SELECT * FROM table" in call_args[0]
        assert "Parameters" not in call_args[0]

    def test_etl_operation_logging(self, mock_logger):
        """Test ETL operation logging methods."""
        etl_logger = ETLLogger()
        
        # Test start logging
        etl_logger.log_etl_start("test_table", "extraction")
        mock_logger.info.assert_called_with("[START] Starting extraction for table: test_table")
        
        # Test complete logging
        mock_logger.reset_mock()
        etl_logger.log_etl_complete("test_table", "extraction", 100)
        mock_logger.info.assert_called_with("[PASS] Completed extraction for table: test_table | Records: 100")
        
        # Test error logging
        mock_logger.reset_mock()
        test_error = Exception("test error")
        etl_logger.log_etl_error("test_table", "extraction", test_error)
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: test_table | Error: test error")

    def test_validation_logging(self, mock_logger):
        """Test validation result logging."""
        etl_logger = ETLLogger()
        
        # Test passed validation
        etl_logger.log_validation_result("test_table", True)
        mock_logger.info.assert_called_with("[PASS] Validation passed for table: test_table")
        
        # Test failed validation
        mock_logger.reset_mock()
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


class TestInitDefaultLogger:
    """Test default logger initialization."""

    def test_init_default_logger_success(self):
        """Test that init_default_logger initializes the default logger configuration."""
        with patch('os.getenv', side_effect=lambda key, default: "logs" if key == "ETL_LOG_PATH" else "INFO"), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            init_default_logger()
            mock_setup_logging.assert_called_with(
                log_level="INFO", 
                log_file="etl_pipeline.log", 
                log_dir="logs", 
                format_type="detailed"
            )

    def test_init_default_logger_exception(self):
        """Test init_default_logger when an exception occurs during setup."""
        with patch('os.getenv', side_effect=lambda key, default: "logs" if key == "ETL_LOG_PATH" else "INFO"), \
             patch('etl_pipeline.config.logging.setup_logging', side_effect=Exception("Test error")), \
             patch('logging.basicConfig') as mock_basic_config, \
             patch('builtins.print') as mock_print:
            init_default_logger()
            mock_basic_config.assert_called_once()
            mock_print.assert_called_once()
            assert "Warning: Could not set up advanced logging" in mock_print.call_args[0][0] 