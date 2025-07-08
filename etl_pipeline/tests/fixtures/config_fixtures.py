"""
Configuration fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Pipeline configuration
- Table configuration  
- Settings creation and management
- Configuration validation
- Database type and schema enums
"""

import os
import pytest
from unittest.mock import patch, mock_open
from typing import Dict, Any

# Import new configuration system components
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema,
    reset_settings,
    create_test_settings
)


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
def complete_config_environment():
    """Fixture providing a complete configuration environment."""
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'source_host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'source_db',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass',
        'MYSQL_REPLICATION_HOST': 'repl_host',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'repl_db',
        'MYSQL_REPLICATION_USER': 'repl_user',
        'MYSQL_REPLICATION_PASSWORD': 'repl_pass',
        'POSTGRES_ANALYTICS_HOST': 'analytics_host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics_db',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }
    return env_vars


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
    """Fixture providing a context manager for mocking Settings environment."""
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
def test_settings_with_enums(test_env_vars, test_pipeline_config, test_tables_config):
    """Create test settings using enums for database configuration testing."""
    return create_test_settings(
        pipeline_config=test_pipeline_config,
        tables_config=test_tables_config,
        env_vars=test_env_vars
    )


@pytest.fixture
def database_config_test_cases():
    """Test cases for database configuration using enums."""
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
    """Schema-specific configuration test cases."""
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
    """Test cases for connection configuration validation."""
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