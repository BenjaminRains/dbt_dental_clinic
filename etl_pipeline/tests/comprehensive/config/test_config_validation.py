"""
Comprehensive configuration validation tests.

This module tests configuration validation across all config components,
including schema validation, data type checking, and business rule validation.
Uses modern Settings class with provider pattern dependency injection.
"""

import pytest
from unittest.mock import MagicMock, patch
from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema
from etl_pipeline.config.providers import DictConfigProvider


class TestPipelineConfigValidation:
    """Test pipeline configuration validation using Settings class with provider pattern."""

    @pytest.mark.validation
    def test_validate_configs_complete_configuration(self):
        """Test validation of complete configuration using Settings class."""
        # Create test provider with complete configuration
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'batch_size': 25000,
                    'parallel_jobs': 5
                },
                'connections': {
                    'source': {'pool_size': 5},
                    'replication': {'pool_size': 10},
                    'analytics': {'pool_size': 8}
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
                    }
                },
                'logging': {
                    'level': 'INFO',
                    'file': {
                        'enabled': True,
                        'path': 'logs/pipeline.log',
                        'max_size_mb': 100,
                        'backup_count': 10
                    }
                },
                'error_handling': {
                    'auto_retry': {
                        'enabled': True,
                        'max_attempts': 3,
                        'delay_minutes': 5
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True

    @pytest.mark.validation
    def test_validate_configs_missing_environment_variables(self):
        """Test validation with missing environment variables."""
        # Create test provider with missing environment variables
        test_provider = DictConfigProvider(
            pipeline={
                'general': {'pipeline_name': 'test_pipeline'},
                'connections': {},
                'stages': {},
                'logging': {'level': 'INFO'},
                'error_handling': {}
            },
            tables={'tables': {}},
            env={
                # Missing most required environment variables
                'OPENDENTAL_SOURCE_HOST': 'test-host'
                # Missing: port, database, user, password, etc.
            }
        )
        
        # Mock os.getenv to return None for all environment variables
        with patch('os.getenv', return_value=None):
            settings = Settings(environment='test', provider=test_provider)
            
            # Should fail validation due to missing environment variables
            assert settings.validate_configs() is False

    @pytest.mark.validation
    def test_validate_configs_test_environment_prefixing(self):
        """Test validation with test environment variable prefixing."""
        # Create test provider with TEST_ prefixed variables
        test_provider = DictConfigProvider(
            pipeline={
                'general': {'pipeline_name': 'test_pipeline'},
                'connections': {},
                'stages': {},
                'logging': {'level': 'INFO'},
                'error_handling': {}
            },
            tables={'tables': {}},
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation with TEST_ prefixed variables
        assert settings.validate_configs() is True

    @pytest.mark.validation
    def test_validate_configs_schema_optional(self):
        """Test that schema is optional for PostgreSQL validation."""
        # Create test provider without schema variable
        test_provider = DictConfigProvider(
            pipeline={
                'general': {'pipeline_name': 'test_pipeline'},
                'connections': {},
                'stages': {},
                'logging': {'level': 'INFO'},
                'error_handling': {}
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                # Missing: POSTGRES_ANALYTICS_SCHEMA (should be optional)
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation (schema is optional)
        assert settings.validate_configs() is True


class TestConfigurationErrorCases:
    """Test configuration validation error cases using Settings class."""

    @pytest.mark.validation
    def test_invalid_batch_size_handling(self):
        """Test handling of invalid batch size in pipeline configuration."""
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test',
                    'batch_size': 0  # Invalid: must be > 0
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid batch_size is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid batch_size is returned as-is
        batch_size = settings.get_pipeline_setting('general.batch_size')
        assert batch_size == 0

    @pytest.mark.validation
    def test_invalid_parallel_jobs_handling(self):
        """Test handling of invalid parallel jobs in pipeline configuration."""
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test',
                    'parallel_jobs': 25  # Invalid: too high
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid parallel_jobs is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid parallel_jobs is returned as-is
        parallel_jobs = settings.get_pipeline_setting('general.parallel_jobs')
        assert parallel_jobs == 25

    @pytest.mark.validation
    def test_invalid_error_threshold_handling(self):
        """Test handling of invalid error threshold in pipeline configuration."""
        test_provider = DictConfigProvider(
            pipeline={
                'stages': {
                    'extract': {
                        'enabled': True,
                        'timeout_minutes': 30,
                        'error_threshold': 1.5  # Invalid: > 1.0
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid error_threshold is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid error_threshold is returned as-is
        error_threshold = settings.get_pipeline_setting('stages.extract.error_threshold')
        assert error_threshold == 1.5

    @pytest.mark.validation
    def test_invalid_timeout_minutes_handling(self):
        """Test handling of invalid timeout minutes in pipeline configuration."""
        test_provider = DictConfigProvider(
            pipeline={
                'stages': {
                    'extract': {
                        'enabled': True,
                        'timeout_minutes': -5,  # Invalid: negative
                        'error_threshold': 0.01
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid timeout_minutes is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid timeout_minutes is returned as-is
        timeout_minutes = settings.get_pipeline_setting('stages.extract.timeout_minutes')
        assert timeout_minutes == -5

    @pytest.mark.validation
    def test_invalid_pool_size_handling(self):
        """Test handling of invalid pool size in pipeline configuration."""
        test_provider = DictConfigProvider(
            pipeline={
                'connections': {
                    'source': {
                        'pool_size': 0  # Invalid: must be > 0
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid pool_size is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid pool_size is returned as-is
        pool_size = settings.get_pipeline_setting('connections.source.pool_size')
        assert pool_size == 0

    @pytest.mark.validation
    def test_invalid_log_level_handling(self):
        """Test handling of invalid log level in pipeline configuration."""
        test_provider = DictConfigProvider(
            pipeline={
                'logging': {
                    'level': 'INVALID_LEVEL'  # Invalid log level
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid log_level is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid log_level is returned as-is
        log_level = settings.get_pipeline_setting('logging.level')
        assert log_level == 'INVALID_LEVEL'


class TestConfigurationBusinessRules:
    """Test configuration business rule validation using Settings class."""

    @pytest.mark.validation
    def test_batch_size_reasonable_range(self):
        """Test that batch size is within reasonable range."""
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'batch_size': 25000  # Reasonable range
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test batch size is in reasonable range
        batch_size = settings.get_pipeline_setting('general.batch_size')
        assert batch_size is not None
        assert isinstance(batch_size, (int, float))
        assert 1000 <= batch_size <= 100000

    @pytest.mark.validation
    def test_parallel_jobs_reasonable_range(self):
        """Test that parallel jobs is within reasonable range."""
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'parallel_jobs': 5  # Reasonable range
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test parallel jobs is in reasonable range
        parallel_jobs = settings.get_pipeline_setting('general.parallel_jobs')
        assert parallel_jobs is not None
        assert isinstance(parallel_jobs, (int, float))
        assert 1 <= parallel_jobs <= 20

    @pytest.mark.validation
    def test_timeout_minutes_reasonable_range(self):
        """Test that timeout minutes are within reasonable range."""
        test_provider = DictConfigProvider(
            pipeline={
                'stages': {
                    'extract': {
                        'enabled': True,
                        'timeout_minutes': 30,  # Reasonable range
                        'error_threshold': 0.01
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test timeout minutes is in reasonable range
        timeout_minutes = settings.get_pipeline_setting('stages.extract.timeout_minutes')
        assert timeout_minutes is not None
        assert isinstance(timeout_minutes, (int, float))
        assert 1 <= timeout_minutes <= 120

    @pytest.mark.validation
    def test_error_threshold_reasonable_range(self):
        """Test that error thresholds are within reasonable range."""
        test_provider = DictConfigProvider(
            pipeline={
                'stages': {
                    'extract': {
                        'enabled': True,
                        'timeout_minutes': 30,
                        'error_threshold': 0.01  # Reasonable range
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test error threshold is in reasonable range
        error_threshold = settings.get_pipeline_setting('stages.extract.error_threshold')
        assert error_threshold is not None
        assert isinstance(error_threshold, (int, float))
        assert 0.0 <= error_threshold <= 1.0

    @pytest.mark.validation
    def test_pool_size_reasonable_range(self):
        """Test that pool sizes are within reasonable range."""
        test_provider = DictConfigProvider(
            pipeline={
                'connections': {
                    'source': {
                        'pool_size': 5  # Reasonable range
                    }
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test pool size is in reasonable range
        pool_size = settings.get_pipeline_setting('connections.source.pool_size')
        assert pool_size is not None
        assert isinstance(pool_size, (int, float))
        assert 1 <= pool_size <= 50


class TestConfigurationConsistency:
    """Test configuration consistency across sections using Settings class."""

    @pytest.mark.validation
    def test_parallel_jobs_consistency(self):
        """Test consistency between parallel_jobs and connection pool sizes."""
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'parallel_jobs': 5
                },
                'connections': {
                    'source': {'pool_size': 5},
                    'replication': {'pool_size': 10},
                    'analytics': {'pool_size': 8}
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test consistency: parallel_jobs should not exceed pool sizes
        parallel_jobs = settings.get_pipeline_setting('general.parallel_jobs')
        source_pool_size = settings.get_pipeline_setting('connections.source.pool_size')
        replication_pool_size = settings.get_pipeline_setting('connections.replication.pool_size')
        analytics_pool_size = settings.get_pipeline_setting('connections.analytics.pool_size')
        
        assert parallel_jobs is not None and isinstance(parallel_jobs, (int, float))
        assert source_pool_size is not None and isinstance(source_pool_size, (int, float))
        assert replication_pool_size is not None and isinstance(replication_pool_size, (int, float))
        assert analytics_pool_size is not None and isinstance(analytics_pool_size, (int, float))
        
        assert parallel_jobs <= source_pool_size
        assert parallel_jobs <= replication_pool_size
        assert parallel_jobs <= analytics_pool_size

    @pytest.mark.validation
    def test_timeout_consistency(self):
        """Test consistency of timeout values across stages."""
        test_provider = DictConfigProvider(
            pipeline={
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
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test timeout consistency: each stage should have reasonable timeout
        extract_timeout = settings.get_pipeline_setting('stages.extract.timeout_minutes')
        load_timeout = settings.get_pipeline_setting('stages.load.timeout_minutes')
        transform_timeout = settings.get_pipeline_setting('stages.transform.timeout_minutes')
        
        assert extract_timeout is not None and isinstance(extract_timeout, (int, float))
        assert load_timeout is not None and isinstance(load_timeout, (int, float))
        assert transform_timeout is not None and isinstance(transform_timeout, (int, float))
        
        assert 1 <= extract_timeout <= 120
        assert 1 <= load_timeout <= 120
        assert 1 <= transform_timeout <= 120

    @pytest.mark.validation
    def test_error_threshold_consistency(self):
        """Test consistency of error thresholds across stages."""
        test_provider = DictConfigProvider(
            pipeline={
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
                }
            },
            tables={'tables': {}},
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'test_db',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_PORT': '3306',
                'MYSQL_REPLICATION_DB': 'test_repl_db',
                'MYSQL_REPLICATION_USER': 'test_repl_user',
                'MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_PORT': '5432',
                'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test error threshold consistency: all stages should have reasonable thresholds
        extract_threshold = settings.get_pipeline_setting('stages.extract.error_threshold')
        load_threshold = settings.get_pipeline_setting('stages.load.error_threshold')
        transform_threshold = settings.get_pipeline_setting('stages.transform.error_threshold')
        
        assert extract_threshold is not None and isinstance(extract_threshold, (int, float))
        assert load_threshold is not None and isinstance(load_threshold, (int, float))
        assert transform_threshold is not None and isinstance(transform_threshold, (int, float))
        
        assert 0.0 <= extract_threshold <= 1.0
        assert 0.0 <= load_threshold <= 1.0
        assert 0.0 <= transform_threshold <= 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 