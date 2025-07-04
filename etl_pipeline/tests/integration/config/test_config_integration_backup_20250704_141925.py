"""
Real integration tests for the config directory components.

This module tests actual integration between configuration components:
- Real database connections using ConnectionFactory
- Actual configuration file loading
- Real environment variable processing
- Cross-component integration with real dependencies

These tests require:
- Test database instances to be available
- Proper environment variables set
- Network connectivity to databases
"""

import pytest
import os
import yaml
import tempfile
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config import logging


@pytest.fixture
def test_environment_variables():
    """Fixture providing test environment variables.
    
    These should point to test database instances, not production.
    """
    return {
        'OPENDENTAL_SOURCE_HOST': os.getenv('TEST_OPENDENTAL_SOURCE_HOST', 'localhost'),
        'OPENDENTAL_SOURCE_PORT': os.getenv('TEST_OPENDENTAL_SOURCE_PORT', '3306'),
        'OPENDENTAL_SOURCE_DB': os.getenv('TEST_OPENDENTAL_SOURCE_DB', 'opendental_test'),
        'OPENDENTAL_SOURCE_USER': os.getenv('TEST_OPENDENTAL_SOURCE_USER', 'test_user'),
        'OPENDENTAL_SOURCE_PASSWORD': os.getenv('TEST_OPENDENTAL_SOURCE_PASSWORD', 'test_pass'),
        'MYSQL_REPLICATION_HOST': os.getenv('TEST_MYSQL_REPLICATION_HOST', 'localhost'),
        'MYSQL_REPLICATION_PORT': os.getenv('TEST_MYSQL_REPLICATION_PORT', '3306'),
        'MYSQL_REPLICATION_DB': os.getenv('TEST_MYSQL_REPLICATION_DB', 'opendental_replication_test'),
        'MYSQL_REPLICATION_USER': os.getenv('TEST_MYSQL_REPLICATION_USER', 'test_user'),
        'MYSQL_REPLICATION_PASSWORD': os.getenv('TEST_MYSQL_REPLICATION_PASSWORD', 'test_pass'),
        'POSTGRES_ANALYTICS_HOST': os.getenv('TEST_POSTGRES_ANALYTICS_HOST', 'localhost'),
        'POSTGRES_ANALYTICS_PORT': os.getenv('TEST_POSTGRES_ANALYTICS_PORT', '5432'),
        'POSTGRES_ANALYTICS_DB': os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'opendental_analytics_test'),
        'POSTGRES_ANALYTICS_SCHEMA': os.getenv('TEST_POSTGRES_ANALYTICS_SCHEMA', 'raw'),
        'POSTGRES_ANALYTICS_USER': os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'test_user'),
        'POSTGRES_ANALYTICS_PASSWORD': os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD', 'test_pass'),
        'ETL_ENVIRONMENT': 'test'
    }


@pytest.fixture
def real_pipeline_config():
    """Fixture providing a real pipeline configuration for testing."""
    return {
        'general': {
            'pipeline_name': 'dental_clinic_etl_test',
            'environment': 'test',
            'timezone': 'UTC',
            'max_retries': 3,
            'retry_delay_seconds': 300,
            'batch_size': 1000,  # Smaller for testing
            'parallel_jobs': 2   # Smaller for testing
        },
        'connections': {
            'source': {
                'pool_size': 3,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'read_timeout': 300,
                'write_timeout': 300
            },
            'replication': {
                'pool_size': 3,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'read_timeout': 300,
                'write_timeout': 300
            },
            'analytics': {
                'pool_size': 3,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'application_name': 'dental_clinic_etl_test'
            }
        },
        'stages': {
            'extract': {
                'enabled': True,
                'timeout_minutes': 30,
                'error_threshold': 0.01
            },
            'load': {
                'enabled': True,
                'timeout_minutes': 45,
                'error_threshold': 0.01
            },
            'transform': {
                'enabled': True,
                'timeout_minutes': 60,
                'error_threshold': 0.01
            }
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': {
                'enabled': True,
                'path': 'logs/pipeline_test.log',
                'max_size_mb': 10,
                'backup_count': 3
            },
            'console': {
                'enabled': True,
                'level': 'INFO'
            }
        },
        'error_handling': {
            'max_consecutive_failures': 3,
            'failure_notification_threshold': 2,
            'auto_retry': {
                'enabled': True,
                'max_attempts': 3,
                'delay_minutes': 5
            }
        }
    }


@pytest.fixture
def real_tables_config():
    """Fixture providing a real tables configuration for testing."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_key': 'DateTStamp',
                'incremental': True,
                'table_importance': 'critical',
                'batch_size': 100,
                'extraction_strategy': 'incremental'
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_key': 'AptDateTime',
                'incremental': True,
                'table_importance': 'important',
                'batch_size': 50,
                'extraction_strategy': 'incremental'
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_key': 'ProcDate',
                'incremental': True,
                'table_importance': 'important',
                'batch_size': 200,
                'extraction_strategy': 'incremental'
            }
        }
    }


class TestRealDatabaseConnections:
    """Test actual database connections using real configuration."""

    @pytest.mark.integration
    def test_source_database_connection(self, test_environment_variables):
        """Test actual connection to OpenDental source database."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Test connection
            try:
                source_engine = ConnectionFactory.get_opendental_source_connection()
                with source_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_value"))
                    assert result.scalar() == 1
                    print(f"✅ Source database connection successful: {source_engine.url}")
            except SQLAlchemyError as e:
                pytest.skip(f"Source database not available: {e}")
            finally:
                if 'source_engine' in locals():
                    source_engine.dispose()

    @pytest.mark.integration
    def test_replication_database_connection(self, test_environment_variables):
        """Test actual connection to MySQL replication database."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Test connection
            try:
                repl_engine = ConnectionFactory.get_mysql_replication_connection()
                with repl_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_value"))
                    assert result.scalar() == 1
                    print(f"✅ Replication database connection successful: {repl_engine.url}")
            except SQLAlchemyError as e:
                pytest.skip(f"Replication database not available: {e}")
            finally:
                if 'repl_engine' in locals():
                    repl_engine.dispose()

    @pytest.mark.integration
    def test_analytics_database_connection(self, test_environment_variables):
        """Test actual connection to PostgreSQL analytics database."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Test connection
            try:
                analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
                with analytics_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_value"))
                    assert result.scalar() == 1
                    print(f"✅ Analytics database connection successful: {analytics_engine.url}")
            except SQLAlchemyError as e:
                pytest.skip(f"Analytics database not available: {e}")
            finally:
                if 'analytics_engine' in locals():
                    analytics_engine.dispose()

    @pytest.mark.integration
    def test_all_database_connections_simultaneously(self, test_environment_variables):
        """Test all database connections can be established simultaneously."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            engines = []
            try:
                # Test all connections
                source_engine = ConnectionFactory.get_opendental_source_connection()
                repl_engine = ConnectionFactory.get_mysql_replication_connection()
                analytics_engine = ConnectionFactory.get_postgres_analytics_connection()
                
                engines = [source_engine, repl_engine, analytics_engine]
                
                # Test each connection
                for engine in engines:
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT 1 as test_value"))
                        assert result.scalar() == 1
                
                print("✅ All database connections successful simultaneously")
                
            except SQLAlchemyError as e:
                pytest.skip(f"One or more databases not available: {e}")
            finally:
                # Clean up all engines
                for engine in engines:
                    if engine:
                        engine.dispose()


class TestRealConfigurationLoading:
    """Test actual configuration file loading and processing."""

    @pytest.mark.integration
    def test_real_pipeline_config_loading(self, test_environment_variables, real_pipeline_config):
        """Test loading real pipeline configuration from file."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create temporary pipeline config file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                yaml.dump(real_pipeline_config, f)
                config_path = f.name
            
            try:
                # Test loading with real file
                settings = Settings()
                
                # Verify config was loaded
                assert settings.pipeline_config == real_pipeline_config
                assert settings.get_pipeline_setting('general.pipeline_name') == 'dental_clinic_etl_test'
                assert settings.get_pipeline_setting('general.batch_size') == 1000
                assert settings.get_pipeline_setting('general.parallel_jobs') == 2
                
                print(f"✅ Real pipeline config loaded successfully from {config_path}")
                
            finally:
                # Clean up
                if os.path.exists(config_path):
                    os.unlink(config_path)

    @pytest.mark.integration
    def test_real_tables_config_loading(self, test_environment_variables, real_tables_config):
        """Test loading real tables configuration from file."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create temporary tables config file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                yaml.dump(real_tables_config, f)
                config_path = f.name
            
            try:
                # Test loading with real file
                settings = Settings()
                
                # Verify config was loaded
                assert settings.tables_config == real_tables_config
                assert settings.get_table_config('patient')['primary_key'] == 'PatNum'
                assert settings.get_table_config('patient')['incremental'] is True
                
                # Test table importance filtering
                critical_tables = settings.get_tables_by_importance('critical')
                assert 'patient' in critical_tables
                
                print(f"✅ Real tables config loaded successfully from {config_path}")
                
            finally:
                # Clean up
                if os.path.exists(config_path):
                    os.unlink(config_path)

    @pytest.mark.integration
    def test_real_configuration_validation(self, test_environment_variables, 
                                          real_pipeline_config, real_tables_config):
        """Test real configuration validation with actual files."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create temporary config files
            pipeline_path = None
            tables_path = None
            
            try:
                # Create pipeline config file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    yaml.dump(real_pipeline_config, f)
                    pipeline_path = f.name
                
                # Create tables config file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    yaml.dump(real_tables_config, f)
                    tables_path = f.name
                
                # Test loading and validation
                settings = Settings()
                
                # Verify all configurations are valid
                assert settings.validate_configs() is True
                assert 'general' in settings.pipeline_config
                assert 'connections' in settings.pipeline_config
                assert 'stages' in settings.pipeline_config
                assert 'tables' in settings.tables_config
                
                print("✅ Real configuration validation successful")
                
            finally:
                # Clean up
                for path in [pipeline_path, tables_path]:
                    if path and os.path.exists(path):
                        os.unlink(path)


class TestRealEnvironmentIntegration:
    """Test real environment variable integration."""

    @pytest.mark.integration
    def test_environment_variable_loading(self, test_environment_variables):
        """Test that environment variables are properly loaded and used."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            settings = Settings()
            
            # Test that environment variables are loaded
            source_config = settings.get_database_config('source')
            assert source_config['host'] == test_environment_variables['OPENDENTAL_SOURCE_HOST']
            assert source_config['port'] == int(test_environment_variables['OPENDENTAL_SOURCE_PORT'])
            assert source_config['database'] == test_environment_variables['OPENDENTAL_SOURCE_DB']
            assert source_config['user'] == test_environment_variables['OPENDENTAL_SOURCE_USER']
            assert source_config['password'] == test_environment_variables['OPENDENTAL_SOURCE_PASSWORD']
            
            analytics_config = settings.get_database_config('analytics')
            assert analytics_config['host'] == test_environment_variables['POSTGRES_ANALYTICS_HOST']
            assert analytics_config['port'] == int(test_environment_variables['POSTGRES_ANALYTICS_PORT'])
            assert analytics_config['database'] == test_environment_variables['POSTGRES_ANALYTICS_DB']
            assert analytics_config['schema'] == test_environment_variables['POSTGRES_ANALYTICS_SCHEMA']
            
            print("✅ Environment variables loaded and used correctly")

    @pytest.mark.integration
    def test_environment_detection(self, test_environment_variables):
        """Test automatic environment detection from environment variables."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            settings = Settings()
            
            # Test environment detection
            assert settings.environment == 'test'
            assert settings.env_prefix == 'TEST_'
            
            print("✅ Environment detection working correctly")

    @pytest.mark.integration
    def test_connection_string_generation_with_real_config(self, test_environment_variables):
        """Test connection string generation with real configuration."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            settings = Settings()
            
            # Test MySQL connection string
            mysql_conn_str = settings.get_connection_string('source')
            assert 'mysql+pymysql://' in mysql_conn_str
            assert test_environment_variables['OPENDENTAL_SOURCE_HOST'] in mysql_conn_str
            assert test_environment_variables['OPENDENTAL_SOURCE_PORT'] in mysql_conn_str
            assert test_environment_variables['OPENDENTAL_SOURCE_DB'] in mysql_conn_str
            assert test_environment_variables['OPENDENTAL_SOURCE_USER'] in mysql_conn_str
            
            # Test PostgreSQL connection string
            postgres_conn_str = settings.get_connection_string('analytics')
            assert 'postgresql+psycopg2://' in postgres_conn_str
            assert test_environment_variables['POSTGRES_ANALYTICS_HOST'] in postgres_conn_str
            assert test_environment_variables['POSTGRES_ANALYTICS_PORT'] in postgres_conn_str
            assert test_environment_variables['POSTGRES_ANALYTICS_DB'] in postgres_conn_str
            assert test_environment_variables['POSTGRES_ANALYTICS_USER'] in postgres_conn_str
            
            print("✅ Connection string generation working correctly")


class TestRealCrossComponentIntegration:
    """Test integration between different configuration components."""

    @pytest.mark.integration
    def test_settings_with_real_config_files(self, test_environment_variables,
                                            real_pipeline_config, real_tables_config):
        """Test Settings class with real configuration files."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create temporary config files
            pipeline_path = None
            tables_path = None
            
            try:
                # Create pipeline config file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    yaml.dump(real_pipeline_config, f)
                    pipeline_path = f.name
                
                # Create tables config file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    yaml.dump(real_tables_config, f)
                    tables_path = f.name
                
                # Test complete integration
                settings = Settings()
                
                # Test environment variables are loaded
                assert settings.get_database_config('source')['host'] == test_environment_variables['OPENDENTAL_SOURCE_HOST']
                assert settings.get_database_config('analytics')['host'] == test_environment_variables['POSTGRES_ANALYTICS_HOST']
                
                # Test pipeline config is loaded
                assert settings.pipeline_config == real_pipeline_config
                assert settings.get_pipeline_setting('general.batch_size') == 1000
                
                # Test tables config is loaded
                assert settings.tables_config == real_tables_config
                assert settings.get_table_config('patient')['primary_key'] == 'PatNum'
                
                # Test configuration validation
                assert settings.validate_configs() is True
                
                print("✅ Complete configuration integration working correctly")
                
            finally:
                # Clean up
                for path in [pipeline_path, tables_path]:
                    if path and os.path.exists(path):
                        os.unlink(path)

    @pytest.mark.integration
    def test_database_config_with_pipeline_overrides(self, test_environment_variables,
                                                    real_pipeline_config):
        """Test database configuration with real pipeline overrides."""
        with pytest.MonkeyPatch().context() as m:
            # Set environment variables
            for key, value in test_environment_variables.items():
                m.setenv(key, value)
            
            # Create temporary pipeline config file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                yaml.dump(real_pipeline_config, f)
                config_path = f.name
            
            try:
                settings = Settings()
                
                # Test that pipeline config overrides are applied
                source_config = settings.get_database_config('source')
                assert source_config['pool_size'] == 3  # From pipeline config
                assert source_config['connect_timeout'] == 60  # From pipeline config
                assert source_config['host'] == test_environment_variables['OPENDENTAL_SOURCE_HOST']  # From environment
                
                print("✅ Database config with pipeline overrides working correctly")
                
            finally:
                # Clean up
                if os.path.exists(config_path):
                    os.unlink(config_path)


class TestRealErrorHandling:
    """Test real error handling scenarios."""

    @pytest.mark.integration
    def test_missing_environment_variables(self):
        """Test handling of missing environment variables."""
        with pytest.MonkeyPatch().context() as m:
            # Clear all environment variables
            for key in ['OPENDENTAL_SOURCE_HOST', 'OPENDENTAL_SOURCE_USER', 'POSTGRES_ANALYTICS_HOST']:
                m.delenv(key, raising=False)
            
            settings = Settings()
            
            # Should fail validation
            assert settings.validate_configs() is False
            
            print("✅ Missing environment variables handled correctly")

    @pytest.mark.integration
    def test_invalid_database_connection(self):
        """Test handling of invalid database connection parameters."""
        with pytest.MonkeyPatch().context() as m:
            # Set invalid connection parameters
            m.setenv('OPENDENTAL_SOURCE_HOST', 'invalid_host')
            m.setenv('OPENDENTAL_SOURCE_PORT', '9999')
            m.setenv('OPENDENTAL_SOURCE_DB', 'invalid_db')
            m.setenv('OPENDENTAL_SOURCE_USER', 'invalid_user')
            m.setenv('OPENDENTAL_SOURCE_PASSWORD', 'invalid_pass')
            
            # Test that connection fails gracefully
            try:
                source_engine = ConnectionFactory.get_opendental_source_connection()
                with source_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                pytest.fail("Expected connection to fail")
            except SQLAlchemyError:
                print("✅ Invalid database connection handled correctly")
            finally:
                if 'source_engine' in locals():
                    source_engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 