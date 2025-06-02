"""
Integration tests for the configuration system.
"""
import os
import pytest
from pathlib import Path
import yaml
from sqlalchemy import create_engine
from etl_pipeline.config import DatabaseConfig, PipelineConfig
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.loaders.postgres_loader import PostgresLoader

@pytest.fixture
def test_env():
    """Set up test environment variables."""
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'test_source_host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'test_source_db',
        'OPENDENTAL_SOURCE_USER': 'test_source_user',
        'OPENDENTAL_SOURCE_PW': 'test_source_pass',
        'STAGING_MYSQL_HOST': 'test_staging_host',
        'STAGING_MYSQL_PORT': '3306',
        'STAGING_MYSQL_DB': 'test_staging_db',
        'STAGING_MYSQL_USER': 'test_staging_user',
        'STAGING_MYSQL_PASSWORD': 'test_staging_pass',
        'TARGET_POSTGRES_HOST': 'test_target_host',
        'TARGET_POSTGRES_PORT': '5432',
        'TARGET_POSTGRES_DB': 'test_target_db',
        'TARGET_POSTGRES_USER': 'test_target_user',
        'TARGET_POSTGRES_PASSWORD': 'test_target_pass',
        'TARGET_POSTGRES_SCHEMA': 'test_schema'
    }
    
    # Save original environment
    original_env = dict(os.environ)
    
    # Set test environment
    os.environ.update(env_vars)
    
    yield env_vars
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)

@pytest.fixture
def test_config_path(tmp_path):
    """Create a temporary test configuration file."""
    config = {
        'general': {
            'environment': 'test',
            'version': '1.0.0'
        },
        'connections': {
            'source': {
                'schema': 'source_db',
                'charset': 'utf8mb4',
                'pool_size': 5,
                'max_overflow': 10
            },
            'staging': {
                'schema': 'staging',
                'charset': 'utf8mb4',
                'pool_size': 5,
                'max_overflow': 10
            },
            'target': {
                'schema': 'test_schema',
                'charset': 'utf8',
                'pool_size': 5,
                'max_overflow': 10
            }
        },
        'stages': {
            'extract': {
                'enabled': True,
                'schedule': '0 * * * *',
                'timeout_minutes': 30
            },
            'transform': {
                'enabled': True,
                'schedule': '5 * * * *',
                'timeout_minutes': 45
            },
            'load': {
                'enabled': True,
                'schedule': '10 * * * *',
                'timeout_minutes': 60
            }
        },
        'monitoring': {
            'enabled': True,
            'log_level': 'INFO',
            'alerts': {
                'email': {
                    'enabled': True,
                    'recipients': ['test@example.com']
                }
            }
        },
        'performance': {
            'batch_size': 10000,
            'max_workers': 4,
            'timeout_seconds': 3600
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': 'etl.log'
        }
    }
    
    config_path = tmp_path / "pipeline.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(config, f)
    
    return config_path

def test_config_integration(test_env, test_config_path):
    """Test integration between configuration and database connections."""
    # Load configuration
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    # Verify database configurations
    source_config = DatabaseConfig.get_source_config()
    assert source_config['host'] == test_env['OPENDENTAL_SOURCE_HOST']
    assert source_config['port'] == int(test_env['OPENDENTAL_SOURCE_PORT'])
    assert source_config['database'] == test_env['OPENDENTAL_SOURCE_DB']
    
    staging_config = DatabaseConfig.get_staging_config()
    assert staging_config['host'] == test_env['STAGING_MYSQL_HOST']
    assert staging_config['port'] == int(test_env['STAGING_MYSQL_PORT'])
    assert staging_config['database'] == test_env['STAGING_MYSQL_DB']
    
    target_config = DatabaseConfig.get_target_config()
    assert target_config['host'] == test_env['TARGET_POSTGRES_HOST']
    assert target_config['port'] == int(test_env['TARGET_POSTGRES_PORT'])
    assert target_config['database'] == test_env['TARGET_POSTGRES_DB']
    assert target_config['schema'] == test_env['TARGET_POSTGRES_SCHEMA']

def test_connection_factory_integration(test_env, test_config_path):
    """Test integration between configuration and connection factory."""
    # Load configuration
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    # Get connections
    source_conn = ConnectionFactory.get_source_connection()
    staging_conn = ConnectionFactory.get_staging_connection()
    target_conn = ConnectionFactory.get_target_connection()
    
    # Verify connection URLs
    assert source_conn.url.host == test_env['OPENDENTAL_SOURCE_HOST']
    assert source_conn.url.port == int(test_env['OPENDENTAL_SOURCE_PORT'])
    assert source_conn.url.database == test_env['OPENDENTAL_SOURCE_DB']
    
    assert staging_conn.url.host == test_env['STAGING_MYSQL_HOST']
    assert staging_conn.url.port == int(test_env['STAGING_MYSQL_PORT'])
    assert staging_conn.url.database == test_env['STAGING_MYSQL_DB']
    
    assert target_conn.url.host == test_env['TARGET_POSTGRES_HOST']
    assert target_conn.url.port == int(test_env['TARGET_POSTGRES_PORT'])
    assert target_conn.url.database == test_env['TARGET_POSTGRES_DB']

def test_postgres_loader_integration(test_env, test_config_path):
    """Test integration between configuration and postgres loader."""
    # Load configuration
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    # Get connections
    source_conn = ConnectionFactory.get_source_connection()
    target_conn = ConnectionFactory.get_target_connection()
    
    # Create loader
    loader = PostgresLoader(source_conn, target_conn)
    
    # Verify schema configurations
    assert loader.target_schema == test_env['TARGET_POSTGRES_SCHEMA']
    assert loader.staging_schema == config.get_connection_config('staging')['schema']
    
    # Verify pipeline configuration
    assert loader.config.general['environment'] == 'test'
    assert loader.config.general['version'] == '1.0.0'
    
    # Verify stage configurations
    assert loader.config.is_stage_enabled('extract') is True
    assert loader.config.get_stage_schedule('extract') == '0 * * * *'
    assert loader.config.get_stage_timeout('extract') == 30 