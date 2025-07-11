"""
Comprehensive tests for the unified logging system using provider pattern and Settings injection.
Follows the three-tier ETL testing strategy and connection architecture best practices.
"""

import pytest
from unittest.mock import patch, MagicMock, call
from etl_pipeline.config.logging import setup_logging, configure_sql_logging, get_logger, ETLLogger
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings

@pytest.fixture
def test_settings():
    """Settings with DictConfigProvider for test environment isolation."""
    provider = DictConfigProvider(
        pipeline={'logging': {'log_level': 'DEBUG', 'log_file': 'test.log', 'log_dir': 'logs', 'format_type': 'detailed'}},
        env={'TEST_ETL_LOG_LEVEL': 'DEBUG', 'TEST_ETL_LOG_PATH': 'logs'}
    )
    return Settings(environment='test', provider=provider)

@pytest.fixture
def mock_logger():
    with patch('etl_pipeline.config.logging.get_logger') as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        yield mock_logger

class TestComprehensiveLogging:
    """
    Comprehensive logging tests using provider pattern and Settings injection.
    Validates: handler setup, ETLLogger methods, SQL logging, error handling, and environment separation.
    ETL Context: Ensures logging is robust, environment-agnostic, and follows provider pattern for all configuration.
    """

    def test_setup_logging_with_settings(self, test_settings):
        """Test setup_logging using Settings injection and provider pattern."""
        with patch('logging.getLogger') as mock_get_logger, \
             patch('logging.StreamHandler'), \
             patch('logging.FileHandler'), \
             patch('logging.Formatter'), \
             patch('os.makedirs') as mock_makedirs:
            
            # Setup mock for the root logger that setup_logging actually uses
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            config = test_settings.pipeline_config['logging']
            setup_logging(
                log_level=config['log_level'],
                log_file=config['log_file'],
                log_dir=config['log_dir'],
                format_type=config['format_type']
            )
            mock_makedirs.assert_called_once_with('logs', exist_ok=True)
            mock_logger.setLevel.assert_called_with(10)  # DEBUG

    def test_setup_logging_handler_removal(self, mock_logger):
        """Test that existing handlers are removed before adding new ones (provider pattern)."""
        with patch('logging.getLogger') as mock_get_logger, patch('logging.StreamHandler'):
            mock_logger_instance = MagicMock()
            mock_handler = MagicMock()
            mock_logger_instance.handlers = [mock_handler]
            mock_get_logger.return_value = mock_logger_instance
            setup_logging()
            mock_logger_instance.removeHandler.assert_called_with(mock_handler)
            mock_logger_instance.addHandler.assert_called()

    def test_etl_logger_methods(self, mock_logger):
        """Test all ETLLogger methods with Settings injection."""
        etl_logger = ETLLogger("etl_pipeline")
        etl_logger.info("info")
        etl_logger.debug("debug")
        etl_logger.warning("warning")
        etl_logger.error("error")
        etl_logger.critical("critical")
        etl_logger.log_sql_query("SELECT 1", {"id": 1})
        etl_logger.log_etl_start("patients", "extraction")
        etl_logger.log_etl_complete("patients", "extraction", 100)
        etl_logger.log_etl_error("patients", "extraction", Exception("fail"))
        etl_logger.log_validation_result("patients", True)
        etl_logger.log_performance("extraction", 2.0, 100)
        assert mock_logger.info.called
        assert mock_logger.debug.called
        assert mock_logger.error.called

    def test_sql_logging_configuration(self):
        """Test SQL logging configuration with provider pattern."""
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            configure_sql_logging(enabled=True, level="DEBUG")
            assert mock_logger.setLevel.call_count == 4
            configure_sql_logging(enabled=False)
            assert mock_logger.setLevel.call_count == 8  # 4 more calls

    def test_get_logger_provider_pattern(self):
        """Test get_logger returns correct logger using provider pattern."""
        logger = get_logger()
        assert hasattr(logger, 'info')
        assert logger.name == "etl_pipeline"
        custom_logger = get_logger("custom")
        assert custom_logger.name == "custom"

    def test_etl_logger_error_handling(self, mock_logger):
        """Test ETLLogger error handling scenarios (provider pattern)."""
        etl_logger = ETLLogger()
        etl_logger.log_etl_error("test_table", "extraction", None)
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: test_table | Error: None")
        class CustomError(Exception):
            def __str__(self):
                return "Custom error message"
        custom_error = CustomError()
        mock_logger.reset_mock()
        etl_logger.log_etl_error("test_table", "extraction", custom_error)
        mock_logger.error.assert_called_with("[FAIL] Error during extraction for table: test_table | Error: Custom error message")

    def test_log_sql_query_with_empty_params(self, mock_logger):
        """Test SQL query logging with empty parameters (provider pattern)."""
        etl_logger = ETLLogger()
        etl_logger.log_sql_query("SELECT * FROM users", {})
        mock_logger.debug.assert_called_once_with("SQL Query: SELECT * FROM users")

    def test_validation_logging(self, mock_logger):
        """Test validation result logging (provider pattern)."""
        etl_logger = ETLLogger()
        etl_logger.log_validation_result("patients", True)
        mock_logger.info.assert_called_with("[PASS] Validation passed for table: patients")
        mock_logger.reset_mock()
        etl_logger.log_validation_result("patients", False, 5)
        mock_logger.warning.assert_called_with("[WARN] Validation failed for table: patients | Issues: 5")
        mock_logger.reset_mock()
        etl_logger.log_validation_result("patients", False)
        mock_logger.warning.assert_called_with("[WARN] Validation failed for table: patients | Issues: 0")

    def test_performance_logging(self, mock_logger):
        """Test performance logging scenarios (provider pattern)."""
        etl_logger = ETLLogger()
        etl_logger.log_performance("extraction", 2.5, 1000)
        mock_logger.info.assert_called_with("[PERF] extraction completed in 2.50s | 1000 records | 400 records/sec")
        mock_logger.reset_mock()
        etl_logger.log_performance("validation", 1.5)
        mock_logger.info.assert_called_with("[PERF] validation completed in 1.50s")
        mock_logger.reset_mock()
        etl_logger.log_performance("fast_operation", 0.0, 100)
        mock_logger.info.assert_called_with("[PERF] fast_operation completed in 0.00s | 100 records | 0 records/sec")
        mock_logger.reset_mock()
        etl_logger.log_performance("micro_operation", 0.001, 1000)
        mock_logger.info.assert_called_with("[PERF] micro_operation completed in 0.00s | 1000 records | 1000000 records/sec")
        mock_logger.reset_mock()
        etl_logger.log_performance("batch_processing", 3600.0, 1000000)
        mock_logger.info.assert_called_with("[PERF] batch_processing completed in 3600.00s | 1000000 records | 278 records/sec")

    def test_logging_environment_separation(self, test_settings, mock_logger):
        """Test that logging respects environment separation (test vs production)."""
        config = test_settings.pipeline_config['logging']
        assert config['log_level'] == 'DEBUG'
        assert config['log_file'] == 'test.log'
        assert config['log_dir'] == 'logs'
        # No production variables should be present
        for key in test_settings._env_vars:
            assert key.startswith('TEST_')

    def test_logging_error_recovery(self, mock_logger):
        """Test logging error recovery (provider pattern)."""
        with patch('etl_pipeline.config.logging.setup_logging', side_effect=Exception("Test error")), \
             patch('logging.basicConfig') as mock_basic_config, \
             patch('builtins.print') as mock_print:
            from etl_pipeline.config.logging import init_default_logger
            init_default_logger()
            mock_basic_config.assert_called_once()
            mock_print.assert_called_once()
            assert "Warning: Could not set up advanced logging" in mock_print.call_args[0][0] 