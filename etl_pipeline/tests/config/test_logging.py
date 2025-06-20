import pytest
from unittest.mock import patch, MagicMock
import logging
from etl_pipeline.config.logging import setup_logging, configure_sql_logging, init_default_logger

def test_setup_logging():
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

def test_setup_logging_without_log_dir():
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

def test_configure_sql_logging():
    """Test that configure_sql_logging sets the correct log level for SQL loggers."""
    with patch('logging.getLogger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        configure_sql_logging(enabled=True, level="DEBUG")
        mock_logger.setLevel.assert_called_with(logging.DEBUG)

def test_configure_sql_logging_disabled():
    """Test configure_sql_logging when SQL logging is disabled."""
    with patch('logging.getLogger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        configure_sql_logging(enabled=False)
        mock_logger.setLevel.assert_called_with(logging.WARNING)

def test_init_default_logger():
    """Test that init_default_logger initializes the default logger configuration."""
    with patch('os.getenv', side_effect=lambda key, default: "logs" if key == "ETL_LOG_PATH" else "INFO"), \
         patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
        init_default_logger()
        mock_setup_logging.assert_called_with(log_level="INFO", log_file="etl_pipeline.log", log_dir="logs", format_type="detailed")

def test_init_default_logger_exception():
    """Test init_default_logger when an exception occurs during setup."""
    with patch('os.getenv', side_effect=lambda key, default: "logs" if key == "ETL_LOG_PATH" else "INFO"), \
         patch('etl_pipeline.config.logging.setup_logging', side_effect=Exception("Test error")), \
         patch('logging.basicConfig') as mock_basic_config:
        init_default_logger()
        mock_basic_config.assert_called_once() 