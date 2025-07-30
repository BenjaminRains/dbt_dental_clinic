"""
Configuration fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Pipeline configuration
- Table configuration  
- Settings creation and management
- Configuration validation
- Database type and schema enums
- Provider pattern implementation for dependency injection
"""

import os
import pytest
import shutil
import tempfile
from unittest.mock import patch, mock_open
from typing import Dict, Any
from pathlib import Path

# Import new configuration system components
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema,
    reset_settings,
    create_test_settings
)
from etl_pipeline.config.providers import DictConfigProvider


def _copy_tables_yml_files_to_temp_dir(real_config_dir: Path, temp_dir: Path):
    for yml_file in real_config_dir.glob("tables*.yml"):
        shutil.copy(yml_file, temp_dir / yml_file.name)

@pytest.fixture
def temp_tables_config_dir():
    """
    Creates a temp directory and copies all tables*.yml files from the real config dir into it.
    Yields the temp dir Path for use in tests.
    """
    real_config_dir = Path(__file__).parent.parent / "etl_pipeline" / "etl_pipeline" / "config"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        _copy_tables_yml_files_to_temp_dir(real_config_dir, temp_dir_path)
        yield temp_dir_path


@pytest.fixture
def database_types():
    """Database type enums for testing."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """PostgreSQL schema enums for testing."""
    return PostgresSchema


@pytest.fixture
def test_pipeline_config():
    """Standard test pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'environment': 'test',
            'batch_size': 1000,
            'parallel_jobs': 2
        },
        'connections': {
            'source': {
                'pool_size': 2,
                'connect_timeout': 30
            },
            'replication': {
                'pool_size': 2,
                'connect_timeout': 30
            },
            'analytics': {
                'pool_size': 2,
                'connect_timeout': 30
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
                'path': 'logs/pipeline.log',
                'max_size_mb': 100,
                'backup_count': 10
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
def test_tables_config():
    """Standard test tables configuration."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'batch_size': 100
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 50
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 200
            }
        }
    }


@pytest.fixture
def test_env_vars():
    """Test environment variables following connection architecture naming convention.
    
    This fixture provides test environment variables that conform to the connection architecture:
    - Uses TEST_ prefix for test environment variables
    - Follows the environment-specific variable naming convention
    - Matches the .env_test file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'test',
        
        # OpenDental Source (Test) - following architecture naming
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
        
        # MySQL Replication (Test) - following architecture naming
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3305',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
        
        # PostgreSQL Analytics (Test) - following architecture naming
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
    }


@pytest.fixture
def production_env_vars():
    """Production environment variables following connection architecture naming convention.
    
    This fixture provides production environment variables that conform to the connection architecture:
    - Uses non-prefixed variables for production environment
    - Follows the environment-specific variable naming convention
    - Matches the .env_production file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'production',
        
        # OpenDental Source (Production) - following architecture naming
        'OPENDENTAL_SOURCE_HOST': 'prod-source-host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'opendental',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass',
        
        # MySQL Replication (Production) - following architecture naming
        'MYSQL_REPLICATION_HOST': 'prod-repl-host',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'opendental_replication',
        'MYSQL_REPLICATION_USER': 'repl_user',
        'MYSQL_REPLICATION_PASSWORD': 'repl_pass',
        
        # PostgreSQL Analytics (Production) - following architecture naming
        'POSTGRES_ANALYTICS_HOST': 'prod-analytics-host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }


@pytest.fixture
def test_config_provider(test_pipeline_config, test_tables_config, test_env_vars):
    """Test configuration provider following the provider pattern for dependency injection.
    
    This fixture implements the DictConfigProvider pattern as specified in the connection architecture:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline=test_pipeline_config,
        tables=test_tables_config,
        env=test_env_vars
    )


@pytest.fixture
def production_config_provider(valid_pipeline_config, complete_tables_config, production_env_vars):
    """Production configuration provider following the provider pattern.
    
    This fixture implements the DictConfigProvider pattern for production-like testing:
    - Uses DictConfigProvider with production-like configuration
    - Provides injected configuration for integration testing
    - Supports dependency injection for configuration swapping
    """
    return DictConfigProvider(
        pipeline=valid_pipeline_config,
        tables=complete_tables_config,
        env=production_env_vars
    )


@pytest.fixture
def test_settings(test_config_provider):
    """Test settings with provider injection following connection architecture.
    
    This fixture implements the Settings injection pattern as specified in the architecture:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider for testing (as recommended)
    - Supports dependency injection for clean test isolation
    - Follows the unified interface pattern
    """
    return Settings(environment='test', provider=test_config_provider)


@pytest.fixture
def production_settings(production_config_provider):
    """Production settings with provider injection following connection architecture.
    
    This fixture implements the Settings injection pattern for production-like testing:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider with production-like configuration
    - Supports dependency injection for integration testing
    """
    return Settings(environment='production', provider=production_config_provider)


@pytest.fixture
def test_settings_with_enums(test_config_provider):
    """Create test settings using enums for database configuration testing.
    
    This fixture uses the factory function pattern as recommended in the architecture:
    - Uses create_test_settings() factory function
    - Provides enum support for type safety
    - Uses provider pattern for dependency injection
    """
    return create_test_settings(
        pipeline_config=test_config_provider.get_config('pipeline'),
        tables_config=test_config_provider.get_config('tables'),
        env_vars=test_config_provider.get_config('env')
    )


@pytest.fixture
def source_database_environment():
    """Fixture providing source database environment variables following architecture.
    
    This fixture provides environment variables specifically for source database testing,
    following the connection architecture naming convention.
    """
    return {
        'ETL_ENVIRONMENT': 'test',
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass'
    }


@pytest.fixture
def replication_database_environment():
    """Fixture providing replication database environment variables following architecture.
    
    This fixture provides environment variables specifically for replication database testing,
    following the connection architecture naming convention.
    """
    return {
        'ETL_ENVIRONMENT': 'test',
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3305',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass'
    }


@pytest.fixture
def analytics_database_environment():
    """Fixture providing analytics database environment variables following architecture.
    
    This fixture provides environment variables specifically for analytics database testing,
    following the connection architecture naming convention.
    """
    return {
        'ETL_ENVIRONMENT': 'test',
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
    }


@pytest.fixture
def valid_pipeline_config():
    """Fixture providing a valid pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'dental_clinic_etl',
            'environment': 'production',
            'timezone': 'UTC',
            'max_retries': 3,
            'retry_delay_seconds': 300,
            'batch_size': 25000,
            'parallel_jobs': 6
        },
        'connections': {
            'source': {
                'pool_size': 6,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'read_timeout': 300,
                'write_timeout': 300
            },
            'replication': {
                'pool_size': 6,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'read_timeout': 300,
                'write_timeout': 300
            },
            'analytics': {
                'pool_size': 6,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'application_name': 'dental_clinic_etl'
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
                'path': 'logs/pipeline.log',
                'max_size_mb': 100,
                'backup_count': 10
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
def minimal_pipeline_config():
    """Fixture providing a minimal pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'batch_size': 1000
        }
    }


@pytest.fixture
def invalid_pipeline_config():
    """Fixture providing an invalid pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'batch_size': 'invalid_string'  # Should be int
        },
        'connections': {
            'source': {
                'pool_size': -1  # Invalid negative value
            }
        }
    }


@pytest.fixture
def complete_tables_config():
    """Fixture providing complete tables configuration."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_key': 'DateTStamp',
                'incremental': True,
                'table_importance': 'critical',
                'batch_size': 1000,
                'extraction_strategy': 'incremental'
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_key': 'AptDateTime',
                'incremental': True,
                'table_importance': 'important',
                'batch_size': 500,
                'extraction_strategy': 'incremental'
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_key': 'ProcDate',
                'incremental': True,
                'table_importance': 'important',
                'batch_size': 2000,
                'extraction_strategy': 'incremental'
            },
            'securitylog': {
                'primary_key': 'SecurityLogNum',
                'incremental': False,
                'table_importance': 'reference',
                'batch_size': 5000,
                'extraction_strategy': 'full'
            }
        }
    }


@pytest.fixture
def mock_settings_environment():
    """Fixture providing a context manager for mocking Settings environment.
    
    This fixture provides a context manager for mocking the Settings environment
    while maintaining the provider pattern for dependency injection.
    """
    def _mock_settings_environment(env_vars=None, pipeline_config=None, tables_config=None):
        """Create a context manager for mocking Settings environment."""
        patches = []
        
        # Mock environment variables
        if env_vars is not None:
            patches.append(patch.dict(os.environ, env_vars))
        
        # Mock file existence and loading
        if pipeline_config is not None or tables_config is not None:
            patches.append(patch('pathlib.Path.exists', return_value=True))
            
            # Configure YAML loading
            def yaml_side_effect(data):
                if 'pipeline' in str(data) and pipeline_config is not None:
                    return pipeline_config
                elif 'tables' in str(data) and tables_config is not None:
                    return tables_config
                return {}
            
            patches.append(patch('yaml.safe_load', side_effect=yaml_side_effect))
            patches.append(patch('builtins.open', mock_open(read_data='test: value')))
        
        # Always mock load_environment to avoid actual environment loading
        patches.append(patch('etl_pipeline.config.settings.Settings.load_environment'))
        
        return patches
    
    return _mock_settings_environment


@pytest.fixture
def database_config_test_cases():
    """Test cases for database configuration using enums.
    
    This fixture provides test cases that use enums for type safety
    as specified in the connection architecture.
    """
    return [
        # (db_type, schema, expected_config_keys)
        (DatabaseType.SOURCE, None, ['host', 'port', 'database', 'user', 'password']),
        (DatabaseType.REPLICATION, None, ['host', 'port', 'database', 'user', 'password']),
        (DatabaseType.ANALYTICS, PostgresSchema.RAW, ['host', 'port', 'database', 'schema', 'user', 'password']),
        (DatabaseType.ANALYTICS, PostgresSchema.STAGING, ['host', 'port', 'database', 'schema', 'user', 'password']),
        (DatabaseType.ANALYTICS, PostgresSchema.INTERMEDIATE, ['host', 'port', 'database', 'schema', 'user', 'password']),
        (DatabaseType.ANALYTICS, PostgresSchema.MARTS, ['host', 'port', 'database', 'schema', 'user', 'password']),
    ]


@pytest.fixture
def schema_specific_configs():
    """Schema-specific configuration test cases.
    
    This fixture provides schema-specific configurations that use enums
    for type safety as specified in the connection architecture.
    """
    return {
        'raw': {
            'schema': 'raw',
            'application_name': 'etl_pipeline'
        },
        'staging': {
            'schema': 'staging',
            'application_name': 'etl_pipeline'
        },
        'intermediate': {
            'schema': 'intermediate',
            'application_name': 'etl_pipeline'
        },
        'marts': {
            'schema': 'marts',
            'application_name': 'etl_pipeline'
        }
    }


@pytest.fixture
def connection_config_validation_cases():
    """Test cases for connection configuration validation.
    
    This fixture provides test cases that use enums for type safety
    and follow the connection architecture validation patterns.
    """
    return [
        # Valid configurations
        {
            'name': 'valid_source',
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'config': {
                'host': 'localhost',
                'port': 3306,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass'
            },
            'should_pass': True
        },
        {
            'name': 'valid_analytics_raw',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'config': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_analytics',
                'schema': 'raw',
                'user': 'test_user',
                'password': 'test_pass'
            },
            'should_pass': True
        },
        # Invalid configurations
        {
            'name': 'missing_host',
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'config': {
                'port': 3306,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass'
            },
            'should_pass': False
        },
        {
            'name': 'invalid_port',
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'config': {
                'host': 'localhost',
                'port': 'invalid',
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass'
            },
            'should_pass': False
        }
    ] 