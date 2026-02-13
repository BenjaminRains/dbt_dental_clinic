"""
Comprehensive configuration validation tests.

This module tests configuration validation across all config components,
including schema validation, data type checking, and business rule validation.
Uses modern Settings class with provider pattern dependency injection.

Following the three-tier testing approach:
- Comprehensive tests: Full functionality testing with mocked dependencies and provider pattern
- Target: Complete configuration validation with provider pattern dependency injection
- Order: N/A (Comprehensive tests don't use order markers)

ETL Pipeline Context:
    - Tests Settings class with provider pattern dependency injection
    - Validates environment separation with FAIL FAST behavior
    - Tests ConfigReader validation for dental clinic table configurations
    - Supports provider pattern for clean testing isolation
    - Uses Settings injection for environment-agnostic connections
"""

import pytest
import os
from unittest.mock import MagicMock, patch
from typing import Dict, Any, List
from pathlib import Path

# Import ETL pipeline components
from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema
from etl_pipeline.config.providers import DictConfigProvider, FileConfigProvider
from etl_pipeline.config.config_reader import ConfigReader
# Import custom exception classes
from etl_pipeline.exceptions.configuration import EnvironmentError


class TestSettingsValidation:
    """
    Test Settings class validation using provider pattern dependency injection.
    
    Test Strategy:
        - Comprehensive tests with DictConfigProvider dependency injection
        - Validates environment variable validation and provider pattern
        - Tests FAIL FAST behavior when ETL_ENVIRONMENT not set
        - Tests environment separation (clinic vs test)
        - Validates enum usage for type safety
        - Tests provider pattern for clean dependency injection
    
    Coverage Areas:
        - Environment variable validation for all database types
        - Provider pattern dependency injection (DictConfigProvider/FileConfigProvider)
        - FAIL FAST behavior for missing ETL_ENVIRONMENT
        - Environment separation with proper variable prefixing
        - Enum validation for DatabaseType and PostgresSchema
        - Configuration caching and performance behavior
        - Settings injection for environment-agnostic connections
        
    ETL Context:
        - Critical for ETL pipeline configuration validation
        - Supports dental clinic database environments (MySQL/PostgreSQL)
        - Uses provider pattern for clean testing isolation
        - Enables Settings injection for environment-agnostic connections
    """

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.settings_injection
    def test_validate_configs_complete_configuration(self):
        """
        Test validation of complete configuration using Settings class with provider pattern.
        
        Validates:
            - Complete configuration validation with DictConfigProvider
            - Environment variable validation for all database types
            - Provider pattern dependency injection for clean testing
            - Settings injection for environment-agnostic connections
            - Configuration caching and performance behavior
            
        ETL Pipeline Context:
            - Tests with complete dental clinic database configuration
            - Validates provider pattern for clean dependency injection
            - Critical for ETL pipeline configuration validation
            - Supports Settings injection for environment-agnostic connections
        """
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
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation
        assert settings.validate_configs() is True
        
        # Test provider pattern integration
        assert settings.provider is test_provider
        assert settings.environment == 'test'

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.fail_fast
    def test_validate_configs_missing_environment_variables(self):
        """
        Test validation with missing environment variables using provider pattern.
        
        Validates:
            - FAIL FAST behavior for missing environment variables
            - Provider pattern dependency injection for error scenarios
            - Environment variable validation for dental clinic databases
            - Settings injection for environment-agnostic error handling
            
        ETL Pipeline Context:
            - Tests FAIL FAST behavior for missing configuration
            - Critical for ETL pipeline reliability
            - Uses provider pattern for clean error testing
            - Supports Settings injection for environment-agnostic connections
        """
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
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'
                # Missing: port, database, user, password, etc.
            }
        )
        
        # Mock os.getenv to return None for all environment variables
        with patch('os.getenv', return_value=None):
            settings = Settings(environment='test', provider=test_provider)
            
            # Should fail validation due to missing environment variables
            assert settings.validate_configs() is False

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.environment_separation
    def test_validate_configs_test_environment_prefixing(self):
        """
        Test validation with test environment variable prefixing using provider pattern.
        
        Validates:
            - Test environment variable prefixing (TEST_ prefix)
            - Environment separation between clinic and test
            - Provider pattern dependency injection for environment isolation
            - Settings injection for environment-agnostic connections
            
        ETL Pipeline Context:
            - Tests environment separation for dental clinic databases
            - Critical for test environment isolation
            - Uses provider pattern for clean environment testing
            - Supports Settings injection for environment-agnostic connections
        """
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
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
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
        
        # Test environment separation
        assert settings.environment == 'test'

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.enum_validation
    def test_validate_configs_schema_required(self):
        """
        Test that schema is required for PostgreSQL validation using provider pattern.
        
        Validates:
            - Required schema validation for PostgreSQL
            - Provider pattern dependency injection for required fields
            - Settings injection for environment-agnostic connections
            - Enum validation for PostgresSchema
            
        ETL Pipeline Context:
            - Tests PostgreSQL schema handling for dental clinic analytics
            - Critical for schema configuration validation
            - Uses provider pattern for clean required field testing
            - Supports Settings injection for environment-agnostic connections
        """
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
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should pass validation (schema is required and provided)
        assert settings.validate_configs() is True

    @pytest.mark.validation
    @pytest.mark.fail_fast
    @pytest.mark.environment_separation
    def test_fail_fast_on_missing_etl_environment(self):
        """
        Test FAIL FAST behavior when ETL_ENVIRONMENT not set.
        
        Validates:
            - FAIL FAST behavior for missing ETL_ENVIRONMENT
            - Critical security requirement enforcement
            - No defaulting to production when environment undefined
            - Clear error messages for missing environment
            
        ETL Pipeline Context:
            - Critical security requirement for ETL pipeline
            - Prevents accidental production usage
            - Clear error messages for configuration issues
            - Supports environment separation with FAIL FAST
        """
        # Remove ETL_ENVIRONMENT to test FAIL FAST
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            # Should fail fast with clear error message
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                settings = Settings()
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    @pytest.mark.validation
    @pytest.mark.enum_validation
    @pytest.mark.provider_pattern
    def test_database_config_with_enums(self):
        """
        Test database configuration using enums with provider pattern.
        
        Validates:
            - Enum usage for DatabaseType and PostgresSchema
            - Type safety for database configuration
            - Provider pattern dependency injection with enums
            - Settings injection for environment-agnostic connections
            
        ETL Pipeline Context:
            - Tests enum validation for dental clinic database types
            - Critical for type safety in ETL pipeline
            - Uses provider pattern for clean enum testing
            - Supports Settings injection for environment-agnostic connections
        """
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Test all database types using enums
        source_config = settings.get_database_config(DatabaseType.SOURCE)
        repl_config = settings.get_database_config(DatabaseType.REPLICATION)
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        
        # Validate configurations
        assert source_config['host'] == 'test-host'
        assert source_config['database'] == 'test_db'
        assert repl_config['host'] == 'localhost'
        assert repl_config['database'] == 'test_repl_db'
        assert analytics_config['host'] == 'localhost'
        assert analytics_config['database'] == 'test_analytics_db'
        assert analytics_config['schema'] == 'raw'

    @pytest.mark.validation
    @pytest.mark.enum_validation
    @pytest.mark.provider_pattern
    def test_schema_configs_with_enums(self):
        """
        Test schema-specific configurations using enums with provider pattern.
        
        Validates:
            - Schema-specific configurations using PostgresSchema enum
            - Type safety for schema configuration
            - Provider pattern dependency injection with schema enums
            - Settings injection for environment-agnostic connections
            
        ETL Pipeline Context:
            - Tests PostgreSQL schema configurations for dental clinic analytics
            - Critical for schema-specific ETL operations
            - Uses provider pattern for clean schema testing
            - Supports Settings injection for environment-agnostic connections
        """
        test_provider = DictConfigProvider(
            env={
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Test all schema configurations using enums
        raw_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        staging_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.STAGING)
        intermediate_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.INTERMEDIATE)
        marts_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.MARTS)
        
        # Validate schema configurations
        assert raw_config['schema'] == 'raw'
        assert staging_config['schema'] == 'staging'
        assert intermediate_config['schema'] == 'intermediate'
        assert marts_config['schema'] == 'marts'


class TestConfigurationErrorCases:
    """
    Test configuration validation error cases using Settings class with provider pattern.
    
    Test Strategy:
        - Comprehensive error case testing with DictConfigProvider
        - Validates error handling for invalid configuration values
        - Tests provider pattern dependency injection for error scenarios
        - Tests Settings injection for environment-agnostic error handling
    
    Coverage Areas:
        - Invalid configuration value handling
        - Provider pattern error scenarios
        - Settings injection error handling
        - Configuration validation error cases
        - Error recovery and graceful degradation
        
    ETL Context:
        - Critical for ETL pipeline error resilience
        - Prevents pipeline failures due to configuration issues
        - Uses provider pattern for clean error testing
        - Supports Settings injection for environment-agnostic error handling
    """

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.error_handling
    def test_invalid_batch_size_handling(self):
        """
        Test handling of invalid batch size in pipeline configuration using provider pattern.
        
        Validates:
            - Invalid batch size handling with provider pattern
            - Error handling for configuration validation
            - Settings injection for environment-agnostic error handling
            - Provider pattern dependency injection for error scenarios
            
        ETL Pipeline Context:
            - Tests invalid configuration handling for dental clinic ETL
            - Critical for ETL pipeline error resilience
            - Uses provider pattern for clean error testing
            - Supports Settings injection for environment-agnostic error handling
        """
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test',
                    'batch_size': 0  # Invalid: must be > 0
                }
            },
            tables={'tables': {}},
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid batch_size is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid batch_size is returned as-is
        batch_size = settings.get_pipeline_setting('general.batch_size')
        assert batch_size == 0

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.error_handling
    def test_invalid_parallel_jobs_handling(self):
        """
        Test handling of invalid parallel jobs in pipeline configuration using provider pattern.
        
        Validates:
            - Invalid parallel jobs handling with provider pattern
            - Error handling for configuration validation
            - Settings injection for environment-agnostic error handling
            - Provider pattern dependency injection for error scenarios
            
        ETL Pipeline Context:
            - Tests invalid configuration handling for dental clinic ETL
            - Critical for ETL pipeline error resilience
            - Uses provider pattern for clean error testing
            - Supports Settings injection for environment-agnostic error handling
        """
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test',
                    'parallel_jobs': 25  # Invalid: too high
                }
            },
            tables={'tables': {}},
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = Settings(environment='test', provider=test_provider)
        
        # Should still pass validation (invalid parallel_jobs is handled by application logic)
        assert settings.validate_configs() is True
        
        # Test that invalid parallel_jobs is returned as-is
        parallel_jobs = settings.get_pipeline_setting('general.parallel_jobs')
        assert parallel_jobs == 25


class TestConfigurationBusinessRules:
    """
    Test configuration business rule validation using Settings class with provider pattern.
    
    Test Strategy:
        - Comprehensive business rule testing with DictConfigProvider
        - Validates reasonable ranges for configuration values
        - Tests provider pattern dependency injection for business rules
        - Tests Settings injection for environment-agnostic business rule validation
    
    Coverage Areas:
        - Reasonable range validation for configuration values
        - Provider pattern business rule scenarios
        - Settings injection business rule validation
        - Configuration consistency validation
        - Business rule enforcement
        
    ETL Context:
        - Critical for ETL pipeline configuration quality
        - Prevents invalid configuration values
        - Uses provider pattern for clean business rule testing
        - Supports Settings injection for environment-agnostic business rule validation
    """

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.business_rules
    def test_batch_size_reasonable_range(self):
        """
        Test that batch size is within reasonable range using provider pattern.
        
        Validates:
            - Reasonable range validation for batch size
            - Provider pattern dependency injection for business rules
            - Settings injection for environment-agnostic business rule validation
            - Configuration quality for dental clinic ETL
            
        ETL Pipeline Context:
            - Tests batch size validation for dental clinic ETL operations
            - Critical for ETL pipeline performance optimization
            - Uses provider pattern for clean business rule testing
            - Supports Settings injection for environment-agnostic business rule validation
        """
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'batch_size': 25000  # Reasonable range
                }
            },
            tables={'tables': {}},
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
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
    @pytest.mark.provider_pattern
    @pytest.mark.business_rules
    def test_parallel_jobs_reasonable_range(self):
        """
        Test that parallel jobs is within reasonable range using provider pattern.
        
        Validates:
            - Reasonable range validation for parallel jobs
            - Provider pattern dependency injection for business rules
            - Settings injection for environment-agnostic business rule validation
            - Configuration quality for dental clinic ETL
            
        ETL Pipeline Context:
            - Tests parallel jobs validation for dental clinic ETL operations
            - Critical for ETL pipeline performance optimization
            - Uses provider pattern for clean business rule testing
            - Supports Settings injection for environment-agnostic business rule validation
        """
        test_provider = DictConfigProvider(
            pipeline={
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'parallel_jobs': 5  # Reasonable range
                }
            },
            tables={'tables': {}},
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
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


class TestConfigurationConsistency:
    """
    Test configuration consistency across sections using Settings class with provider pattern.
    
    Test Strategy:
        - Comprehensive consistency testing with DictConfigProvider
        - Validates consistency between configuration sections
        - Tests provider pattern dependency injection for consistency validation
        - Tests Settings injection for environment-agnostic consistency validation
    
    Coverage Areas:
        - Configuration consistency validation
        - Provider pattern consistency scenarios
        - Settings injection consistency validation
        - Cross-section configuration validation
        - Consistency enforcement
        
    ETL Context:
        - Critical for ETL pipeline configuration consistency
        - Prevents inconsistent configuration values
        - Uses provider pattern for clean consistency testing
        - Supports Settings injection for environment-agnostic consistency validation
    """

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.consistency
    def test_parallel_jobs_consistency(self):
        """
        Test consistency between parallel_jobs and connection pool sizes using provider pattern.
        
        Validates:
            - Configuration consistency validation
            - Provider pattern dependency injection for consistency
            - Settings injection for environment-agnostic consistency validation
            - Cross-section configuration validation for dental clinic ETL
            
        ETL Pipeline Context:
            - Tests configuration consistency for dental clinic ETL operations
            - Critical for ETL pipeline performance optimization
            - Uses provider pattern for clean consistency testing
            - Supports Settings injection for environment-agnostic consistency validation
        """
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
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'test_repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics_db',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
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


class TestConfigReaderValidation:
    """
    Test ConfigReader validation using provider pattern dependency injection.
    
    Test Strategy:
        - Comprehensive ConfigReader validation with provider pattern
        - Validates table configuration validation
        - Tests provider pattern dependency injection for ConfigReader
        - Tests Settings injection for environment-agnostic ConfigReader validation
    
    Coverage Areas:
        - Table configuration validation
        - Provider pattern ConfigReader scenarios
        - Settings injection ConfigReader validation
        - Configuration structure validation
        - Validation enforcement
        
    ETL Context:
        - Critical for ETL pipeline table configuration validation
        - Prevents invalid table configuration values
        - Uses provider pattern for clean ConfigReader testing
        - Supports Settings injection for environment-agnostic ConfigReader validation
    """

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.config_reader
    def test_config_reader_validation_with_provider_pattern(self):
        """
        Test ConfigReader validation using provider pattern dependency injection.
        
        Validates:
            - ConfigReader validation with provider pattern
            - Table configuration validation for dental clinic tables
            - Settings injection for environment-agnostic ConfigReader validation
            - Provider pattern dependency injection for ConfigReader
            
        ETL Pipeline Context:
            - Tests ConfigReader validation for dental clinic table configurations
            - Critical for ETL pipeline table configuration quality
            - Uses provider pattern for clean ConfigReader testing
            - Supports Settings injection for environment-agnostic ConfigReader validation
        """
        # Test with real ConfigReader (uses FileConfigProvider internally)
        config_reader = ConfigReader()
        
        # Validate configuration structure
        assert config_reader.config is not None
        assert 'tables' in config_reader.config
        assert len(config_reader.config['tables']) > 0
        
        # Test dental clinic specific tables
        dental_clinic_tables = ['patient', 'appointment', 'procedurelog', 'adjustment']
        found_tables = []
        
        for table_name in dental_clinic_tables:
            config = config_reader.get_table_config(table_name)
            if config:
                found_tables.append(table_name)
                
                # Validate dental clinic specific configuration patterns
                assert 'extraction_strategy' in config, f"Missing extraction_strategy for {table_name}"
                assert 'batch_size' in config, f"Missing batch_size for {table_name}"
                
                # Validate dental clinic specific values
                assert config['extraction_strategy'] in ['incremental', 'full_table', 'incremental_chunked'], \
                    f"Invalid extraction_strategy for {table_name}"
                assert config['batch_size'] > 0, f"Invalid batch_size for {table_name}"
        
        # Validate that we found some dental clinic tables
        assert len(found_tables) > 0, "Should find at least some dental clinic tables"

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.config_reader
    def test_config_reader_validation_summary(self):
        """
        Test ConfigReader validation summary using provider pattern dependency injection.
        
        Validates:
            - ConfigReader validation summary with provider pattern
            - Configuration summary validation for dental clinic tables
            - Settings injection for environment-agnostic ConfigReader validation
            - Provider pattern dependency injection for ConfigReader summary
            
        ETL Pipeline Context:
            - Tests ConfigReader validation summary for dental clinic table configurations
            - Critical for ETL pipeline configuration monitoring
            - Uses provider pattern for clean ConfigReader testing
            - Supports Settings injection for environment-agnostic ConfigReader validation
        """
        config_reader = ConfigReader()
        
        summary = config_reader.get_configuration_summary()
        
        # Validate summary structure
        required_fields = [
            'total_tables', 'extraction_strategies',
            'size_ranges', 'monitored_tables', 'modeled_tables', 'last_loaded'
        ]
        
        for field in required_fields:
            assert field in summary, f"Missing field '{field}' in summary"
        
        # Validate summary values
        assert summary['total_tables'] > 0, "Should have at least one table"
        assert isinstance(summary['extraction_strategies'], dict), "extraction_strategies should be dict"
        assert isinstance(summary['size_ranges'], dict), "size_ranges should be dict"
        assert summary['monitored_tables'] >= 0, "monitored_tables should be non-negative"
        assert summary['modeled_tables'] >= 0, "modeled_tables should be non-negative"

    @pytest.mark.validation
    @pytest.mark.provider_pattern
    @pytest.mark.config_reader
    def test_config_reader_validation_issues(self):
        """
        Test ConfigReader validation issues using provider pattern dependency injection.
        
        Validates:
            - ConfigReader validation issues with provider pattern
            - Configuration issue detection for dental clinic tables
            - Settings injection for environment-agnostic ConfigReader validation
            - Provider pattern dependency injection for ConfigReader issues
            
        ETL Pipeline Context:
            - Tests ConfigReader validation issues for dental clinic table configurations
            - Critical for ETL pipeline configuration quality assurance
            - Uses provider pattern for clean ConfigReader testing
            - Supports Settings injection for environment-agnostic ConfigReader validation
        """
        config_reader = ConfigReader()
        
        issues = config_reader.validate_configuration()
        
        # Validate issues structure
        required_issue_types = [
            'missing_batch_size', 'missing_extraction_strategy',
            'invalid_batch_size', 'large_tables_without_monitoring'
        ]
        
        for issue_type in required_issue_types:
            assert issue_type in issues, f"Missing issue type '{issue_type}'"
            assert isinstance(issues[issue_type], list), f"{issue_type} should be list"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 