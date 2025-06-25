"""
Comprehensive tests for the unified logging system.
Full functionality testing with mocked dependencies.
"""
import pytest
from unittest.mock import patch, MagicMock, call
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
    """Test logging setup functionality with comprehensive coverage."""

    def test_setup_logging_complete_workflow(self):
        """Test complete logging setup workflow with all components."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler, \
             patch('logging.FileHandler') as mock_file_handler, \
             patch('logging.Formatter') as mock_formatter, \
             patch('os.makedirs') as mock_makedirs:
            
            # Setup mocks
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            mock_file_handler.return_value = MagicMock()
            mock_formatter.return_value = MagicMock()
            
            # Execute setup
            setup_logging(
                log_level="DEBUG", 
                log_file="test.log", 
                log_dir="logs", 
                format_type="detailed"
            )
            
            # Verify complete workflow
            mock_makedirs.assert_called_once_with("logs", exist_ok=True)
            mock_formatter.assert_called_once()
            mock_logger.setLevel.assert_called_with(logging.DEBUG)
            assert mock_logger.addHandler.call_count >= 1  # At least console handler

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
            
            # Should not create directory
            mock_makedirs.assert_not_called()
            # Should still add handlers
            mock_logger.addHandler.assert_called()

    def test_setup_logging_different_formats(self):
        """Test setup_logging with different format types."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler, \
             patch('logging.Formatter') as mock_formatter:
            
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            mock_formatter.return_value = MagicMock()
            
            # Test simple format
            setup_logging(format_type="simple")
            mock_formatter.assert_called()
            mock_logger.addHandler.assert_called()
            
            # Test json format
            mock_logger.reset_mock()
            mock_formatter.reset_mock()
            setup_logging(format_type="json")
            mock_formatter.assert_called()
            mock_logger.addHandler.assert_called()

    def test_setup_logging_handler_removal(self):
        """Test that existing handlers are removed before adding new ones."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler:
            
            # Create mock logger with existing handlers
            mock_logger = MagicMock()
            mock_handler = MagicMock()
            mock_logger.handlers = [mock_handler]  # Simulate existing handlers
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            
            setup_logging()
            
            # Should remove existing handlers first
            mock_logger.removeHandler.assert_called_with(mock_handler)
            # Then add new handlers
            mock_logger.addHandler.assert_called()

    def test_setup_logging_etl_logger_configuration(self):
        """Test that ETL-specific logger is configured."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler:
            
            mock_root_logger = MagicMock()
            mock_etl_logger = MagicMock()
            mock_get_logger.side_effect = [mock_root_logger, mock_etl_logger]
            mock_stream_handler.return_value = MagicMock()
            
            setup_logging(log_level="WARNING")
            
            # Root logger should be configured
            mock_root_logger.setLevel.assert_called_with(logging.WARNING)
            # ETL logger should also be configured
            mock_etl_logger.setLevel.assert_called_with(logging.WARNING)


class TestSQLLogging:
    """Test SQL logging configuration with comprehensive coverage."""

    def test_configure_sql_logging_enabled_comprehensive(self):
        """Test comprehensive SQL logging configuration when enabled."""
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            configure_sql_logging(enabled=True, level="DEBUG")
            
            # Should configure all SQL loggers
            expected_calls = [
                call("sqlalchemy.engine"),
                call("sqlalchemy.dialects"),
                call("sqlalchemy.pool"),
                call("sqlalchemy.orm")
            ]
            mock_get_logger.assert_has_calls(expected_calls, any_order=True)
            assert mock_logger.setLevel.call_count == 4
            mock_logger.setLevel.assert_called_with(logging.DEBUG)

    def test_configure_sql_logging_disabled_comprehensive(self):
        """Test comprehensive SQL logging configuration when disabled."""
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            configure_sql_logging(enabled=False)
            
            # Should configure all SQL loggers with WARNING level
            assert mock_logger.setLevel.call_count == 4
            mock_logger.setLevel.assert_called_with(logging.WARNING)

    def test_configure_sql_logging_all_levels(self):
        """Test SQL logging configuration with all possible levels."""
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            # Test all valid log levels
            for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                mock_logger.reset_mock()
                configure_sql_logging(enabled=True, level=level)
                mock_logger.setLevel.assert_called_with(getattr(logging, level))


class TestGetLogger:
    """Test get_logger function with comprehensive coverage."""

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

    def test_get_logger_singleton_behavior(self):
        """Test that get_logger returns singleton instances."""
        logger1 = get_logger("singleton_test")
        logger2 = get_logger("singleton_test")
        assert logger1 is logger2  # Same instance

    def test_get_logger_different_names(self):
        """Test that different names return different loggers."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        assert logger1 is not logger2  # Different instances


class TestETLLogger:
    """Test ETLLogger class with comprehensive coverage."""

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

    def test_etl_logger_initialization_with_invalid_level(self, mock_logger):
        """Test ETLLogger initialization with invalid log level."""
        with pytest.raises(AttributeError):
            ETLLogger("test_module", "INVALID_LEVEL")

    def test_basic_logging_methods_comprehensive(self, mock_logger):
        """Test all basic logging methods with various kwargs."""
        etl_logger = ETLLogger()
        
        # Test all basic logging methods with different kwargs
        test_cases = [
            ("info", {"extra": {"key": "value"}}),
            ("debug", {"extra": {"user": "test"}}),
            ("warning", {"extra": {"level": "high"}}),
            ("error", {"extra": {"code": 500}}),
            ("critical", {"extra": {"fatal": True}})
        ]
        
        for method_name, kwargs in test_cases:
            method = getattr(etl_logger, method_name)
            method(f"test {method_name}", **kwargs)
            
            mock_method = getattr(mock_logger, method_name)
            mock_method.assert_called_with(f"test {method_name}", **kwargs)

    def test_sql_query_logging_comprehensive(self, mock_logger):
        """Test comprehensive SQL query logging scenarios."""
        etl_logger = ETLLogger()
        
        # Test with parameters
        etl_logger.log_sql_query("SELECT * FROM users WHERE id = ?", {"id": 123})
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "SELECT * FROM users WHERE id = ?" in call_args[0]
        assert "id" in call_args[0]
        assert "123" in call_args[0]
        
        # Test without parameters
        mock_logger.reset_mock()
        etl_logger.log_sql_query("SELECT COUNT(*) FROM users")
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "SELECT COUNT(*) FROM users" in call_args[0]
        assert "Parameters" not in call_args[0]
        
        # Test with empty parameters (should be treated as no parameters)
        mock_logger.reset_mock()
        etl_logger.log_sql_query("SELECT * FROM users", {})
        mock_logger.debug.assert_called_once()
        call_args = mock_logger.debug.call_args[0]
        assert "SELECT * FROM users" in call_args[0]
        # Empty dict is falsy, so it goes to the else branch
        assert "Parameters" not in call_args[0]

    def test_etl_operation_logging_comprehensive(self, mock_logger):
        """Test comprehensive ETL operation logging."""
        etl_logger = ETLLogger()
        
        # Test start logging
        etl_logger.log_etl_start("patients", "extraction")
        mock_logger.info.assert_called_with("[START] Starting extraction for table: patients")
        
        # Test complete logging with records
        mock_logger.reset_mock()
        etl_logger.log_etl_complete("patients", "extraction", 1000)
        mock_logger.info.assert_called_with("[PASS] Completed extraction for table: patients | Records: 1000")
        
        # Test complete logging without records
        mock_logger.reset_mock()
        etl_logger.log_etl_complete("patients", "extraction")
        mock_logger.info.assert_called_with("[PASS] Completed extraction for table: patients | Records: 0")
        
        # Test error logging
        mock_logger.reset_mock()
        test_error = Exception("Database connection failed")
        etl_logger.log_etl_error("patients", "extraction", test_error)
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: patients | Error: Database connection failed")

    def test_validation_logging_comprehensive(self, mock_logger):
        """Test comprehensive validation result logging."""
        etl_logger = ETLLogger()
        
        # Test passed validation
        etl_logger.log_validation_result("patients", True)
        mock_logger.info.assert_called_with("[PASS] Validation passed for table: patients")
        
        # Test failed validation with issues
        mock_logger.reset_mock()
        etl_logger.log_validation_result("patients", False, 5)
        mock_logger.warning.assert_called_with("[WARN] Validation failed for table: patients | Issues: 5")
        
        # Test failed validation without issues count
        mock_logger.reset_mock()
        etl_logger.log_validation_result("patients", False)
        mock_logger.warning.assert_called_with("[WARN] Validation failed for table: patients | Issues: 0")

    def test_performance_logging_comprehensive(self, mock_logger):
        """Test comprehensive performance logging scenarios."""
        etl_logger = ETLLogger()
        
        # Test with records and duration
        etl_logger.log_performance("extraction", 2.5, 1000)
        mock_logger.info.assert_called_with("[PERF] extraction completed in 2.50s | 1000 records | 400 records/sec")
        
        # Test without records
        mock_logger.reset_mock()
        etl_logger.log_performance("validation", 1.5)
        mock_logger.info.assert_called_with("[PERF] validation completed in 1.50s")
        
        # Test zero duration
        mock_logger.reset_mock()
        etl_logger.log_performance("fast_operation", 0.0, 100)
        mock_logger.info.assert_called_with("[PERF] fast_operation completed in 0.00s | 100 records | 0 records/sec")
        
        # Test very small duration
        mock_logger.reset_mock()
        etl_logger.log_performance("micro_operation", 0.001, 1000)
        mock_logger.info.assert_called_with("[PERF] micro_operation completed in 0.00s | 1000 records | 1000000 records/sec")
        
        # Test very large duration
        mock_logger.reset_mock()
        etl_logger.log_performance("batch_processing", 3600.0, 1000000)
        mock_logger.info.assert_called_with("[PERF] batch_processing completed in 3600.00s | 1000000 records | 278 records/sec")

    def test_etl_logger_error_handling(self, mock_logger):
        """Test ETLLogger error handling scenarios."""
        etl_logger = ETLLogger()
        
        # Test with None error
        etl_logger.log_etl_error("test_table", "extraction", None)
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: test_table | Error: None")
        
        # Test with complex error object
        class CustomError(Exception):
            def __str__(self):
                return "Custom error message"
        
        custom_error = CustomError()
        mock_logger.reset_mock()
        etl_logger.log_etl_error("test_table", "extraction", custom_error)
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: test_table | Error: Custom error message")

    def test_log_sql_query_with_empty_params(self, mock_logger):
        """Test SQL query logging with empty parameters."""
        etl_logger = ETLLogger()
        etl_logger.log_sql_query("SELECT * FROM users", {})
        
        # Empty dict is falsy, so should go to else branch
        mock_logger.debug.assert_called_once_with("SQL Query: SELECT * FROM users")


class TestInitDefaultLogger:
    """Test default logger initialization with comprehensive coverage."""

    def test_init_default_logger_success(self):
        """Test successful default logger initialization."""
        with patch('os.getenv', side_effect=lambda key, default: "logs" if key == "ETL_LOG_PATH" else "INFO"), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="INFO", 
                log_file="etl_pipeline.log", 
                log_dir="logs", 
                format_type="detailed"
            )

    def test_init_default_logger_with_custom_env_vars(self):
        """Test default logger initialization with custom environment variables."""
        with patch('os.getenv', side_effect=lambda key, default: "custom_logs" if key == "ETL_LOG_PATH" else "DEBUG"), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="DEBUG", 
                log_file="etl_pipeline.log", 
                log_dir="custom_logs", 
                format_type="detailed"
            )

    def test_init_default_logger_exception_handling(self):
        """Test default logger initialization when setup fails."""
        with patch('os.getenv', side_effect=lambda key, default: "logs" if key == "ETL_LOG_PATH" else "INFO"), \
             patch('etl_pipeline.config.logging.setup_logging', side_effect=Exception("Test error")), \
             patch('logging.basicConfig') as mock_basic_config, \
             patch('builtins.print') as mock_print:
            
            init_default_logger()
            
            # Should fall back to basic config
            mock_basic_config.assert_called_once()
            mock_print.assert_called_once()
            assert "Warning: Could not set up advanced logging" in mock_print.call_args[0][0]

    def test_init_default_logger_environment_variable_handling(self):
        """Test environment variable handling in init_default_logger."""
        # Test with None values (should use defaults)
        # os.getenv returns None when key doesn't exist, then uses default
        with patch('os.getenv', side_effect=lambda key, default: default), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="INFO", 
                log_file="etl_pipeline.log", 
                log_dir="logs", 
                format_type="detailed"
            )

    def test_init_default_logger_mixed_env_vars(self):
        """Test init_default_logger with mixed environment variable scenarios."""
        # Test with only ETL_LOG_PATH set
        with patch('os.getenv', side_effect=lambda key, default: "custom_path" if key == "ETL_LOG_PATH" else default), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            init_default_logger()
            
            mock_setup_logging.assert_called_with(
                log_level="INFO", 
                log_file="etl_pipeline.log", 
                log_dir="custom_path", 
                format_type="detailed"
            )


class TestLoggingIntegration:
    """Test logging integration scenarios with mocked dependencies."""

    def test_complete_logging_workflow(self):
        """Test complete logging workflow from setup to usage."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler, \
             patch('logging.FileHandler') as mock_file_handler, \
             patch('os.makedirs') as mock_makedirs:
            
            # Setup mocks
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            mock_file_handler.return_value = MagicMock()
            
            # Setup logging
            setup_logging(log_level="DEBUG", log_file="workflow.log", log_dir="logs")
            
            # Create ETLLogger and use it
            etl_logger = ETLLogger("workflow_test")
            etl_logger.log_etl_start("test_table", "extraction")
            etl_logger.log_etl_complete("test_table", "extraction", 500)
            etl_logger.log_performance("extraction", 1.5, 500)
            
            # Verify all interactions
            mock_makedirs.assert_called_once_with("logs", exist_ok=True)
            assert mock_logger.info.call_count == 3  # start, complete, performance
            assert mock_logger.debug.call_count == 0  # no debug calls in this workflow

    def test_error_recovery_workflow(self):
        """Test logging workflow with error recovery."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler') as mock_stream_handler:
            
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_stream_handler.return_value = MagicMock()
            
            # Setup logging
            setup_logging(log_level="INFO")
            
            # Create ETLLogger and simulate error workflow
            etl_logger = ETLLogger("error_test")
            etl_logger.log_etl_start("error_table", "extraction")
            
            # Simulate error
            test_error = Exception("Connection timeout")
            etl_logger.log_etl_error("error_table", "extraction", test_error)
            
            # Verify error logging
            mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: error_table | Error: Connection timeout") 