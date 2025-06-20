"""
Tests for the logger module.
"""
import pytest
from unittest.mock import patch, MagicMock
import logging

from etl_pipeline.core.logger import get_logger, ETLLogger

@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch('etl_pipeline.core.logger.logging.getLogger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        yield mock_logger

def test_get_logger():
    """Test that get_logger returns a standard logger instance."""
    logger = get_logger()
    assert isinstance(logger, logging.Logger)

def test_etl_logger_initialization(mock_logger):
    """Test that ETLLogger initializes correctly and sets log level."""
    logger = ETLLogger()
    assert logger.logger == mock_logger
    mock_logger.setLevel.assert_called_once()

def test_basic_logging_methods(mock_logger):
    """Test basic logging methods with kwargs."""
    logger = ETLLogger()
    logger.info("test info", extra={"key": "value"})
    logger.debug("test debug", extra={"key": "value"})
    logger.warning("test warning", extra={"key": "value"})
    logger.error("test error", extra={"key": "value"})
    logger.critical("test critical", extra={"key": "value"})
    mock_logger.info.assert_called_with("test info", extra={"key": "value"})
    mock_logger.debug.assert_called_with("test debug", extra={"key": "value"})
    mock_logger.warning.assert_called_with("test warning", extra={"key": "value"})
    mock_logger.error.assert_called_with("test error", extra={"key": "value"})
    mock_logger.critical.assert_called_with("test critical", extra={"key": "value"})

def test_sql_query_logging(mock_logger):
    """Test SQL query logging (should use debug)."""
    logger = ETLLogger()
    logger.log_sql_query("SELECT * FROM table", {"param": "value"})
    mock_logger.debug.assert_called_once()
    call_args = mock_logger.debug.call_args[0]
    assert "SELECT * FROM table" in call_args[0]
    assert "param" in call_args[0]

def test_etl_operation_logging(mock_logger):
    """Test ETL operation logging methods."""
    logger = ETLLogger()
    logger.log_etl_start("test_table", "extraction")
    mock_logger.info.assert_called_with("[START] Starting extraction for table: test_table")
    mock_logger.reset_mock()
    logger.log_etl_complete("test_table", "extraction", 100)
    mock_logger.info.assert_called_with("[PASS] Completed extraction for table: test_table | Records: 100")
    mock_logger.reset_mock()
    logger.log_etl_error("test_table", "extraction", Exception("fail"))
    mock_logger.error.assert_called()

def test_validation_logging(mock_logger):
    """Test validation result logging."""
    logger = ETLLogger()
    logger.log_validation_result("test_table", True)
    mock_logger.info.assert_called_with("[PASS] Validation passed for table: test_table")
    mock_logger.reset_mock()
    logger.log_validation_result("test_table", False, 5)
    mock_logger.warning.assert_called_with("[WARN] Validation failed for table: test_table | Issues: 5") 