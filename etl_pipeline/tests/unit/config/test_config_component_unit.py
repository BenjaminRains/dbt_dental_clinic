"""
Unit tests for configuration component integration.

This module tests how all configuration components work together using mocked dependencies:
- Environment variable integration
- Database connection string generation
- Table configuration integration
- Multi-environment support
- Logging integration
- Error handling across components

These are UNIT tests that use mocks to test integration logic without requiring
real external dependencies like databases or files.
"""

import pytest
import os
from unittest.mock import patch, mock_open
from etl_pipeline.config.settings import Settings
from etl_pipeline.config import logging


class TestConfigIntegration:
    """Test integration between different configuration components."""

    @pytest.mark.unit
    def test_settings_with_complete_config(self, complete_config_environment, 
                                          valid_pipeline_config, complete_tables_config):
        """Test Settings class with complete configuration."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value=complete_tables_config):
            
            settings = Settings()
            
            # Test environment variables are loaded
            assert settings.get_database_config('source')['host'] == 'source_host'
            assert settings.get_database_config('analytics')['host'] == 'analytics_host'
            
            # Test pipeline config is loaded
            assert settings.pipeline_config == valid_pipeline_config
            assert settings.get_pipeline_setting('general.batch_size') == 25000
            
            # Test tables config is loaded
            assert settings.tables_config == complete_tables_config
            assert settings.get_table_config('patient')['primary_key'] == 'PatNum'

    @pytest.mark.unit
    def test_database_config_with_pipeline_overrides(self, complete_config_environment,
                                                    valid_pipeline_config):
        """Test database configuration with pipeline overrides."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value={'tables': {}}):
            
            settings = Settings()
            
            # Test that pipeline config overrides are applied
            source_config = settings.get_database_config('source')
            assert source_config['pool_size'] == 6  # From pipeline config
            assert source_config['connect_timeout'] == 60  # From pipeline config
            assert source_config['host'] == 'source_host'  # From environment

    @pytest.mark.unit
    def test_connection_string_generation(self, complete_config_environment,
                                        valid_pipeline_config):
        """Test connection string generation with complete configuration."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value={'tables': {}}):
            
            settings = Settings()
            
            # Test MySQL connection string
            mysql_conn_str = settings.get_connection_string('source')
            assert 'mysql+pymysql://' in mysql_conn_str
            assert 'source_host:3306' in mysql_conn_str
            assert 'source_db' in mysql_conn_str
            assert 'connect_timeout=60' in mysql_conn_str
            
            # Test PostgreSQL connection string
            postgres_conn_str = settings.get_connection_string('analytics')
            assert 'postgresql+psycopg2://' in postgres_conn_str
            assert 'analytics_host:5432' in postgres_conn_str
            assert 'analytics_db' in postgres_conn_str
            assert 'application_name=dental_clinic_etl' in postgres_conn_str

    @pytest.mark.unit
    def test_table_config_integration(self, complete_config_environment,
                                     complete_tables_config):
        """Test table configuration integration."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value={}), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value=complete_tables_config):
            
            settings = Settings()
            
            # Test table configurations
            patient_config = settings.get_table_config('patient')
            assert patient_config['primary_key'] == 'PatNum'
            assert patient_config['incremental'] is True
            assert patient_config['table_importance'] == 'critical'
            
            # Test table importance filtering
            critical_tables = settings.get_tables_by_importance('critical')
            assert 'patient' in critical_tables
            
            important_tables = settings.get_tables_by_importance('important')
            assert 'appointment' in important_tables
            assert 'procedurelog' in important_tables

    @pytest.mark.unit
    def test_config_validation_integration(self, complete_config_environment,
                                          valid_pipeline_config, complete_tables_config):
        """Test configuration validation across all components."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value=complete_tables_config):
            
            settings = Settings()
            
            # Test that all configurations are valid
            assert settings.validate_configs() is True
            
            # Test that all required components are present
            assert 'general' in settings.pipeline_config
            assert 'connections' in settings.pipeline_config
            assert 'stages' in settings.pipeline_config
            assert 'tables' in settings.tables_config


class TestConfigEnvironmentIntegration:
    """Test environment-specific configuration integration."""

    @pytest.mark.unit
    def test_test_environment_config(self, complete_config_environment):
        """Test configuration with test environment variables."""
        # Add test environment variables
        test_env_vars = complete_config_environment.copy()
        test_env_vars.update({
            'TEST_OPENDENTAL_SOURCE_HOST': 'test_source_host',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_source_db',
            'TEST_POSTGRES_ANALYTICS_HOST': 'test_analytics_host',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db'
        })
        
        with patch.dict(os.environ, test_env_vars), \
             patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value={}), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            # Create settings with test environment
            settings = Settings(environment='test')
            
            # Test that test environment variables are used
            source_config = settings.get_database_config('source')
            assert source_config['host'] == 'test_source_host'
            assert source_config['database'] == 'test_source_db'
            
            analytics_config = settings.get_database_config('analytics')
            assert analytics_config['host'] == 'test_analytics_host'
            assert analytics_config['database'] == 'test_analytics_db'

    @pytest.mark.unit
    def test_environment_detection(self, complete_config_environment):
        """Test automatic environment detection."""
        # Set environment variable
        env_vars = complete_config_environment.copy()
        env_vars['ETL_ENVIRONMENT'] = 'test'
        with patch.dict(os.environ, env_vars, clear=True), \
             patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value={}), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            # Use get_global_settings() which has environment detection
            from etl_pipeline.config.settings import get_global_settings
            settings = get_global_settings()
            assert settings.environment == 'test'

    @pytest.mark.unit
    def test_environment_fallback(self, complete_config_environment):
        """Test environment variable fallback behavior."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value={}), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            settings = Settings()
            # Should default to 'production' when no environment is set
            assert settings.environment == 'production'


class TestConfigLoggingIntegration:
    """Test configuration integration with logging system."""

    @pytest.mark.unit
    def test_logging_config_from_pipeline(self, valid_pipeline_config):
        """Test logging configuration from pipeline config."""
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            settings = Settings()
            
            # Test logging configuration extraction
            logging_config = settings.get_pipeline_setting('logging')
            assert logging_config['level'] == 'INFO'
            assert logging_config['file']['enabled'] is True
            assert logging_config['console']['enabled'] is True

    @pytest.mark.unit
    def test_logging_setup_with_config(self, valid_pipeline_config):
        """Test logging setup using pipeline configuration."""
        with patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
            
            settings = Settings()
            logging_config = settings.get_pipeline_setting('logging')
            
            # Simulate logging setup using config
            logging.setup_logging(
                log_level=logging_config['level'],
                log_file=logging_config['file']['path'] if logging_config['file']['enabled'] else None,
                log_dir='logs',
                format_type='detailed'
            )
            
            mock_setup_logging.assert_called_once()


class TestConfigErrorHandling:
    """Test error handling in configuration integration."""

    @pytest.mark.unit
    def test_missing_environment_variables(self, valid_pipeline_config):
        """Test handling of missing environment variables."""
        # Use empty environment variables
        with patch.dict(os.environ, {}, clear=True), \
             patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value=valid_pipeline_config), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            settings = Settings()
            
            # Should fail validation
            assert settings.validate_configs() is False

    @pytest.mark.unit
    def test_invalid_pipeline_config_integration(self, complete_config_environment):
        """Test handling of invalid pipeline configuration."""
        invalid_config = {
            'general': {
                'pipeline_name': 'test',
                'batch_size': 'invalid'  # Should be int
            }
        }
        
        with patch.dict(os.environ, complete_config_environment), \
             patch('pathlib.Path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data='test: value')), \
             patch('yaml.safe_load', return_value=invalid_config), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            settings = Settings()
            
            # Should handle invalid config gracefully
            batch_size = settings.get_pipeline_setting('general.batch_size')
            assert batch_size == 'invalid'  # Returns as-is

    @pytest.mark.unit
    def test_missing_config_files(self, complete_config_environment):
        """Test handling when config files are missing."""
        with patch.dict(os.environ, complete_config_environment), \
             patch('pathlib.Path.exists', return_value=False), \
             patch('etl_pipeline.config.settings.Settings.load_environment'):
            
            settings = Settings()
            
            # Should use empty configs
            assert settings.pipeline_config == {}
            assert settings.tables_config == {'tables': {}}
            
            # Should still work with environment variables
            assert settings.get_database_config('source')['host'] == 'source_host'


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 