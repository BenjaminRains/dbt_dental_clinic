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
from unittest.mock import patch, mock_open, MagicMock
from etl_pipeline.config import Settings, DatabaseType, PostgresSchema
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config import logging


class TestConfigIntegration:
    """Test integration between different configuration components."""

    @pytest.mark.unit
    def test_settings_with_complete_config(self, complete_config_environment, 
                                          valid_pipeline_config, complete_tables_config):
        """Test Settings class with complete configuration."""
        # Create test provider with injected configurations
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables=complete_tables_config,
            env=complete_config_environment
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Test environment variables are loaded
        assert settings.get_database_config(DatabaseType.SOURCE)['host'] == 'source_host'
        assert settings.get_database_config(DatabaseType.ANALYTICS)['host'] == 'analytics_host'
        
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
        # Create test provider with pipeline overrides
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env=complete_config_environment
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Test that pipeline config overrides are applied
        source_config = settings.get_database_config(DatabaseType.SOURCE)
        assert source_config['pool_size'] == 6  # From pipeline config
        assert source_config['connect_timeout'] == 60  # From pipeline config
        assert source_config['host'] == 'source_host'  # From environment

    @pytest.mark.unit
    def test_connection_string_generation(self, complete_config_environment,
                                        valid_pipeline_config):
        """Test connection string generation with complete configuration."""
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env=complete_config_environment
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Test MySQL connection string
        mysql_conn_str = settings.get_connection_string(DatabaseType.SOURCE)
        assert 'mysql+pymysql://' in mysql_conn_str
        assert 'source_host:3306' in mysql_conn_str
        assert 'source_db' in mysql_conn_str
        assert 'connect_timeout=60' in mysql_conn_str
        
        # Test PostgreSQL connection string
        postgres_conn_str = settings.get_connection_string(DatabaseType.ANALYTICS)
        assert 'postgresql+psycopg2://' in postgres_conn_str
        assert 'analytics_host:5432' in postgres_conn_str
        assert 'analytics_db' in postgres_conn_str
        assert 'application_name=dental_clinic_etl' in postgres_conn_str  # From pipeline config override

    @pytest.mark.unit
    def test_table_config_integration(self, complete_config_environment,
                                     complete_tables_config):
        """Test table configuration integration."""
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline={},
            tables=complete_tables_config,
            env=complete_config_environment
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
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
        # Create test provider with complete configuration
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables=complete_tables_config,
            env=complete_config_environment
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
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
        
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env=test_env_vars
        )
        
        # Create settings with test environment
        settings = Settings(environment='test', provider=test_provider)
        
        # Test that test environment variables are used
        source_config = settings.get_database_config(DatabaseType.SOURCE)
        assert source_config['host'] == 'test_source_host'
        assert source_config['database'] == 'test_source_db'
        
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS)
        assert analytics_config['host'] == 'test_analytics_host'
        assert analytics_config['database'] == 'test_analytics_db'

    @pytest.mark.unit
    def test_environment_detection(self, complete_config_environment):
        """Test automatic environment detection."""
        # Set environment variable
        env_vars = complete_config_environment.copy()
        env_vars['ETL_ENVIRONMENT'] = 'test'
        
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env=env_vars
        )
        
        # Create settings after environment variables are set
        settings = Settings(environment='test', provider=test_provider)
        
        # Test environment detection
        assert settings.environment == 'test'
        assert settings.env_prefix == 'TEST_'

    @pytest.mark.unit
    def test_environment_fallback(self, complete_config_environment):
        """Test environment variable fallback behavior."""
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env=complete_config_environment
        )
        
        settings = Settings(environment='production', provider=test_provider)
        # Should default to 'production' when no environment is set
        assert settings.environment == 'production'


class TestConfigLoggingIntegration:
    """Test configuration integration with logging system."""

    @pytest.mark.unit
    def test_logging_config_from_pipeline(self, valid_pipeline_config):
        """Test logging configuration from pipeline config."""
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(provider=test_provider)
        
        # Test logging configuration extraction
        logging_config = settings.get_pipeline_setting('logging')
        assert logging_config['level'] == 'INFO'
        assert logging_config['file']['enabled'] is True
        assert logging_config['console']['enabled'] is True

    @pytest.mark.unit
    def test_logging_setup_with_config(self, valid_pipeline_config):
        """Test logging setup using pipeline configuration."""
        # Create test provider
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(provider=test_provider)
        logging_config = settings.get_pipeline_setting('logging')
        
        # Simulate logging setup using config
        with patch('etl_pipeline.config.logging.setup_logging') as mock_setup_logging:
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
        test_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}  # Empty environment variables
        )
        
        settings = Settings(provider=test_provider)
        
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
        
        # Create test provider with invalid config
        test_provider = DictConfigProvider(
            pipeline=invalid_config,
            tables={'tables': {}},
            env=complete_config_environment
        )
        
        settings = Settings(provider=test_provider)
        
        # Should handle invalid config gracefully
        batch_size = settings.get_pipeline_setting('general.batch_size')
        assert batch_size == 'invalid'  # Returns as-is

    @pytest.mark.unit
    def test_missing_config_files(self, complete_config_environment):
        """Test handling when config files are missing."""
        # Create test provider with minimal config
        test_provider = DictConfigProvider(
            pipeline={},  # Empty pipeline config
            tables={'tables': {}},  # Empty tables config
            env=complete_config_environment
        )
        
        settings = Settings(provider=test_provider)
        
        # Should use empty configs
        assert settings.pipeline_config == {}
        assert settings.tables_config == {'tables': {}}
        
        # Should still work with environment variables
        assert settings.get_database_config(DatabaseType.SOURCE)['host'] == 'source_host'


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 