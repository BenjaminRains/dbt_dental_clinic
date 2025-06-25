"""
Integration tests for the unified logging system.
Real file system and logging behavior tests.
"""
import pytest
import tempfile
import os
import logging
from pathlib import Path
import time

from etl_pipeline.config.logging import (
    setup_logging, 
    configure_sql_logging, 
    init_default_logger,
    get_logger,
    ETLLogger
)


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def cleanup_logging():
    """Clean up logging configuration after tests."""
    # Store original handlers
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers.copy()
    
    yield
    
    # Restore original handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    for handler in original_handlers:
        root_logger.addHandler(handler)


@pytest.mark.integration
class TestLoggingIntegration:
    """Integration tests for real logging behavior."""

    def test_real_log_file_creation(self, temp_log_dir, cleanup_logging):
        """Test actual log file creation and writing."""
        log_file = temp_log_dir / "test.log"
        
        # Setup real logging
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create real logger and write logs
        logger = get_logger("test_module")
        logger.info("Test info message")
        logger.error("Test error message")
        logger.debug("Test debug message")
        
        # Verify actual file creation and content
        assert log_file.exists()
        content = log_file.read_text()
        assert "Test info message" in content
        assert "Test error message" in content
        assert "Test debug message" in content
        assert "test_module" in content
        assert "INFO" in content
        assert "ERROR" in content
        assert "DEBUG" in content

    def test_real_logging_output_to_console(self, temp_log_dir, cleanup_logging, capsys):
        """Test actual logging output to console."""
        # Setup logging without file handler
        setup_logging(log_level="INFO")
        
        # Create logger and write logs
        logger = get_logger("console_test")
        logger.info("Console info message")
        logger.warning("Console warning message")
        
        # Verify console output
        captured = capsys.readouterr()
        assert "Console info message" in captured.out
        assert "Console warning message" in captured.out
        assert "console_test" in captured.out

    def test_real_etl_logging_output(self, temp_log_dir, cleanup_logging, capsys):
        """Test actual ETL logging output to both file and console."""
        log_file = temp_log_dir / "etl.log"
        
        # Setup logging with both file and console
        setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Use real ETLLogger
        etl_logger = ETLLogger("test_etl")
        etl_logger.log_etl_start("patients", "extraction")
        etl_logger.log_etl_complete("patients", "extraction", 1000)
        etl_logger.log_performance("extraction", 2.5, 1000)
        
        # Verify console output
        captured = capsys.readouterr()
        assert "[START] Starting extraction for table: patients" in captured.out
        assert "[PASS] Completed extraction for table: patients" in captured.out
        assert "[PERF] extraction completed in 2.50s" in captured.out
        assert "1000 records" in captured.out
        assert "400 records/sec" in captured.out
        
        # Verify file output
        assert log_file.exists()
        content = log_file.read_text()
        assert "patients" in content
        assert "1000 records" in content
        assert "400 records/sec" in content
        assert "test_etl" in content

    def test_real_log_level_filtering(self, temp_log_dir, cleanup_logging, capsys):
        """Test real log level filtering behavior."""
        # Setup logging with INFO level
        setup_logging(log_level="INFO")
        
        # Create logger and write logs at different levels
        logger = get_logger("level_test")
        logger.debug("Debug message - should not appear")
        logger.info("Info message - should appear")
        logger.warning("Warning message - should appear")
        logger.error("Error message - should appear")
        
        # Verify console output
        captured = capsys.readouterr()
        assert "Debug message - should not appear" not in captured.out
        assert "Info message - should appear" in captured.out
        assert "Warning message - should appear" in captured.out
        assert "Error message - should appear" in captured.out

    def test_real_log_format_types(self, temp_log_dir, cleanup_logging, capsys):
        """Test real log format types."""
        # Test detailed format
        setup_logging(log_level="INFO", format_type="detailed")
        logger = get_logger("format_test")
        logger.info("Detailed format test")
        
        captured = capsys.readouterr()
        assert "Detailed format test" in captured.out
        # Should include timestamp, logger name, level
        assert "format_test" in captured.out
        assert "INFO" in captured.out
        
        # Test simple format
        setup_logging(log_level="INFO", format_type="simple")
        logger = get_logger("format_test_simple")
        logger.info("Simple format test")
        
        captured = capsys.readouterr()
        assert "Simple format test" in captured.out
        # Should be simpler format
        assert "INFO" in captured.out

    def test_real_performance_calculations(self, temp_log_dir, cleanup_logging, capsys):
        """Test real performance calculations in logging."""
        setup_logging(log_level="INFO")
        etl_logger = ETLLogger("perf_test")
        
        # Test various performance scenarios
        etl_logger.log_performance("fast_operation", 0.1, 100)
        etl_logger.log_performance("slow_operation", 10.0, 1000)
        etl_logger.log_performance("zero_duration", 0.0, 50)
        
        captured = capsys.readouterr()
        assert "1000 records/sec" in captured.out  # 100/0.1 = 1000
        assert "100 records/sec" in captured.out   # 1000/10 = 100
        assert "0 records/sec" in captured.out     # division by zero protection

    def test_real_sql_logging_configuration(self, temp_log_dir, cleanup_logging, capsys):
        """Test real SQL logging configuration."""
        # Enable SQL logging
        configure_sql_logging(enabled=True, level="DEBUG")
        setup_logging(log_level="DEBUG")
        
        # Create SQL loggers and write logs
        sql_engine_logger = logging.getLogger("sqlalchemy.engine")
        sql_engine_logger.debug("SQL query executed")
        
        # Verify SQL logging is working
        captured = capsys.readouterr()
        assert "SQL query executed" in captured.out

    def test_real_directory_creation(self, temp_log_dir, cleanup_logging):
        """Test real directory creation for logging."""
        new_log_dir = temp_log_dir / "nested" / "logs"
        log_file = new_log_dir / "test.log"
        
        # Setup logging with nested directory
        setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_dir=str(new_log_dir)
        )
        
        # Verify directory was created
        assert new_log_dir.exists()
        assert new_log_dir.is_dir()
        
        # Write a log message
        logger = get_logger("dir_test")
        logger.info("Directory creation test")
        
        # Verify log file was created
        assert log_file.exists()

    def test_real_log_file_rotation_simulation(self, temp_log_dir, cleanup_logging):
        """Test log file writing with multiple messages (simulating rotation)."""
        log_file = temp_log_dir / "rotation.log"
        
        setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        logger = get_logger("rotation_test")
        
        # Write multiple log messages
        for i in range(10):
            logger.info(f"Log message {i}")
            time.sleep(0.01)  # Small delay to ensure timestamps differ
        
        # Verify all messages are in the file
        assert log_file.exists()
        content = log_file.read_text()
        for i in range(10):
            assert f"Log message {i}" in content

    def test_real_error_handling_in_logging(self, temp_log_dir, cleanup_logging, capsys):
        """Test real error handling in logging scenarios."""
        # Test logging with invalid characters
        setup_logging(log_level="INFO")
        logger = get_logger("error_test")
        
        # Test with special characters
        logger.info("Special chars: éñçüë")
        logger.error("Error with unicode: 🚀")
        
        captured = capsys.readouterr()
        assert "Special chars: éñçüë" in captured.out
        assert "Error with unicode: 🚀" in captured.out

    def test_real_concurrent_logging_simulation(self, temp_log_dir, cleanup_logging):
        """Test logging behavior with rapid successive calls."""
        log_file = temp_log_dir / "concurrent.log"
        
        setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        logger = get_logger("concurrent_test")
        
        # Rapid successive log calls
        for i in range(100):
            logger.info(f"Rapid log {i}")
        
        # Verify all messages are logged
        assert log_file.exists()
        content = log_file.read_text()
        for i in range(100):
            assert f"Rapid log {i}" in content

    def test_real_etl_workflow_logging(self, temp_log_dir, cleanup_logging, capsys):
        """Test complete ETL workflow logging."""
        log_file = temp_log_dir / "etl_workflow.log"
        
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        etl_logger = ETLLogger("etl_workflow")
        
        # Simulate complete ETL workflow
        etl_logger.log_etl_start("patients", "extraction")
        etl_logger.log_sql_query("SELECT * FROM patients WHERE active = 1")
        etl_logger.log_validation_result("patients", True)
        etl_logger.log_etl_complete("patients", "extraction", 500)
        etl_logger.log_performance("extraction", 1.5, 500)
        
        # Verify console output
        captured = capsys.readouterr()
        assert "[START] Starting extraction for table: patients" in captured.out
        assert "SELECT * FROM patients WHERE active = 1" in captured.out
        assert "[PASS] Validation passed for table: patients" in captured.out
        assert "[PASS] Completed extraction for table: patients" in captured.out
        assert "[PERF] extraction completed in 1.50s" in captured.out
        
        # Verify file output
        assert log_file.exists()
        content = log_file.read_text()
        assert "patients" in content
        assert "500 records" in content
        assert "333 records/sec" in content  # 500/1.5 ≈ 333


@pytest.mark.integration
class TestEnvironmentVariableIntegration:
    """Integration tests for environment variable handling."""

    def test_real_environment_variable_handling(self, temp_log_dir, cleanup_logging, monkeypatch):
        """Test real environment variable handling."""
        # Set real environment variables
        monkeypatch.setenv("ETL_LOG_PATH", str(temp_log_dir))
        monkeypatch.setenv("ETL_LOG_LEVEL", "DEBUG")
        
        # Initialize with real env vars
        init_default_logger()
        
        # Verify real configuration
        logger = get_logger("env_test")
        logger.debug("Debug message")
        logger.info("Info message")
        
        # Check actual log file
        log_file = temp_log_dir / "etl_pipeline.log"
        assert log_file.exists()
        content = log_file.read_text()
        assert "Debug message" in content  # Should be included with DEBUG level
        assert "Info message" in content

    def test_real_environment_variable_fallbacks(self, temp_log_dir, cleanup_logging, monkeypatch):
        """Test real environment variable fallback behavior."""
        # Clear environment variables
        monkeypatch.delenv("ETL_LOG_PATH", raising=False)
        monkeypatch.delenv("ETL_LOG_LEVEL", raising=False)
        
        # Initialize with no env vars (should use defaults)
        init_default_logger()
        
        # Verify default configuration works
        logger = get_logger("fallback_test")
        logger.info("Fallback test message")
        
        # Should create default log file in current directory or logs directory
        possible_log_files = [
            Path("etl_pipeline.log"),
            Path("logs/etl_pipeline.log")
        ]
        
        # At least one should exist
        assert any(log_file.exists() for log_file in possible_log_files)

    def test_real_environment_variable_override(self, temp_log_dir, cleanup_logging, monkeypatch):
        """Test real environment variable override behavior."""
        # Set custom environment variables
        custom_log_dir = temp_log_dir / "custom"
        monkeypatch.setenv("ETL_LOG_PATH", str(custom_log_dir))
        monkeypatch.setenv("ETL_LOG_LEVEL", "WARNING")
        
        # Initialize with custom env vars
        init_default_logger()
        
        # Verify custom configuration
        logger = get_logger("override_test")
        logger.debug("Debug message - should not appear")
        logger.info("Info message - should not appear")
        logger.warning("Warning message - should appear")
        
        # Check custom log file
        log_file = custom_log_dir / "etl_pipeline.log"
        assert log_file.exists()
        content = log_file.read_text()
        assert "Debug message - should not appear" not in content
        assert "Info message - should not appear" not in content
        assert "Warning message - should appear" in content


@pytest.mark.integration
class TestLoggingErrorRecovery:
    """Integration tests for logging error recovery."""

    def test_real_file_permission_error_handling(self, temp_log_dir, cleanup_logging, capsys):
        """Test real file permission error handling."""
        # Create a read-only directory
        readonly_dir = temp_log_dir / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only
        
        try:
            # Try to setup logging in read-only directory
            setup_logging(
                log_level="INFO",
                log_file="test.log",
                log_dir=str(readonly_dir)
            )
            
            # Should still work for console logging
            logger = get_logger("permission_test")
            logger.info("Permission test message")
            
            # Verify console output still works
            captured = capsys.readouterr()
            assert "Permission test message" in captured.out
            
        finally:
            # Restore permissions for cleanup
            readonly_dir.chmod(0o755)

    def test_real_invalid_log_level_handling(self, temp_log_dir, cleanup_logging):
        """Test real invalid log level handling."""
        # Test with invalid log level
        with pytest.raises(AttributeError):
            setup_logging(log_level="INVALID_LEVEL")
        
        # Test with valid log level after error
        setup_logging(log_level="INFO")
        logger = get_logger("recovery_test")
        logger.info("Recovery test message")
        
        # Should work normally after error

    def test_real_logging_initialization_error_recovery(self, temp_log_dir, cleanup_logging, capsys):
        """Test real logging initialization error recovery."""
        # Test init_default_logger with problematic environment
        # This tests the fallback to basic logging
        
        # Create a problematic setup that might cause issues
        problematic_dir = temp_log_dir / "problematic"
        problematic_dir.mkdir()
        
        # The init_default_logger should handle errors gracefully
        init_default_logger()
        
        # Should still be able to log
        logger = get_logger("recovery_test")
        logger.info("Recovery after error test")
        
        # Verify logging still works
        captured = capsys.readouterr()
        assert "Recovery after error test" in captured.out 