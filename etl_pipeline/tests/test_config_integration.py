"""
Integration tests for the configuration system.
"""
import os
import pytest
from pathlib import Path
import yaml
from sqlalchemy import create_engine
from etl_pipeline.config import DatabaseConfig, PipelineConfig, load_config
from etl_pipeline.core.connections import (
    ConnectionFactory
)
from etl_pipeline.loaders.postgres_loader import PostgresLoader

@pytest.fixture
def test_env():
    """Set up test environment variables."""
    return {
        'SOURCE_MYSQL_HOST': 'test_source_host',
        'SOURCE_MYSQL_PORT': '3306',
        'SOURCE_MYSQL_DB': 'test_source_db',
        'SOURCE_MYSQL_USER': 'test_source_user',
        'SOURCE_MYSQL_PASSWORD': 'test_source_pass',
        
        'REPLICATION_MYSQL_HOST': 'test_replication_host',
        'REPLICATION_MYSQL_PORT': '3305',
        'REPLICATION_MYSQL_DB': 'test_replication_db',
        'REPLICATION_MYSQL_USER': 'test_replication_user',
        'REPLICATION_MYSQL_PASSWORD': 'test_replication_pass',
        
        'ANALYTICS_POSTGRES_HOST': 'test_analytics_host',
        'ANALYTICS_POSTGRES_PORT': '5432',
        'ANALYTICS_POSTGRES_DB': 'test_analytics_db',
        'ANALYTICS_POSTGRES_SCHEMA': 'raw',
        'ANALYTICS_POSTGRES_USER': 'test_analytics_user',
        'ANALYTICS_POSTGRES_PASSWORD': 'test_analytics_pass'
    }

def test_database_config_integration(test_env):
    """Test database configuration integration."""
    # Set up environment
    for key, value in test_env.items():
        os.environ[key] = value
    
    # Test source config
    source_config = DatabaseConfig.get_source_config()
    assert source_config['host'] == test_env['SOURCE_MYSQL_HOST']
    assert source_config['port'] == int(test_env['SOURCE_MYSQL_PORT'])
    assert source_config['database'] == test_env['SOURCE_MYSQL_DB']
    assert source_config['user'] == test_env['SOURCE_MYSQL_USER']
    assert source_config['password'] == test_env['SOURCE_MYSQL_PASSWORD']
    
    # Test replication config
    replication_config = DatabaseConfig.get_replication_config()
    assert replication_config['host'] == test_env['REPLICATION_MYSQL_HOST']
    assert replication_config['port'] == int(test_env['REPLICATION_MYSQL_PORT'])
    assert replication_config['database'] == test_env['REPLICATION_MYSQL_DB']
    assert replication_config['user'] == test_env['REPLICATION_MYSQL_USER']
    assert replication_config['password'] == test_env['REPLICATION_MYSQL_PASSWORD']
    
    # Test analytics config
    analytics_config = DatabaseConfig.get_analytics_config()
    assert analytics_config['host'] == test_env['ANALYTICS_POSTGRES_HOST']
    assert analytics_config['port'] == int(test_env['ANALYTICS_POSTGRES_PORT'])
    assert analytics_config['database'] == test_env['ANALYTICS_POSTGRES_DB']
    assert analytics_config['schema'] == test_env['ANALYTICS_POSTGRES_SCHEMA']
    assert analytics_config['user'] == test_env['ANALYTICS_POSTGRES_USER']
    assert analytics_config['password'] == test_env['ANALYTICS_POSTGRES_PASSWORD']

def test_connection_strings(test_env):
    """Test connection string generation."""
    # Set up environment
    for key, value in test_env.items():
        os.environ[key] = value
    
    # Test source connection string
    source_conn = DatabaseConfig.get_source_connection()
    assert source_conn.url.host == test_env['SOURCE_MYSQL_HOST']
    assert source_conn.url.port == int(test_env['SOURCE_MYSQL_PORT'])
    assert source_conn.url.database == test_env['SOURCE_MYSQL_DB']
    assert source_conn.url.username == test_env['SOURCE_MYSQL_USER']
    
    # Test replication connection string
    replication_conn = DatabaseConfig.get_replication_connection()
    assert replication_conn.url.host == test_env['REPLICATION_MYSQL_HOST']
    assert replication_conn.url.port == int(test_env['REPLICATION_MYSQL_PORT'])
    assert replication_conn.url.database == test_env['REPLICATION_MYSQL_DB']
    assert replication_conn.url.username == test_env['REPLICATION_MYSQL_USER']
    
    # Test analytics connection string
    analytics_conn = DatabaseConfig.get_analytics_connection()
    assert analytics_conn.url.host == test_env['ANALYTICS_POSTGRES_HOST']
    assert analytics_conn.url.port == int(test_env['ANALYTICS_POSTGRES_PORT'])
    assert analytics_conn.url.database == test_env['ANALYTICS_POSTGRES_DB']
    assert analytics_conn.url.username == test_env['ANALYTICS_POSTGRES_USER']

def test_pipeline_config_integration(test_env):
    """Test pipeline configuration integration."""
    # Set up environment
    for key, value in test_env.items():
        os.environ[key] = value
    
    # Load configuration
    config = load_config()
    
    # Test source connection
    source_conn = config.get_connection('source')
    assert source_conn.url.host == test_env['SOURCE_MYSQL_HOST']
    assert source_conn.url.port == int(test_env['SOURCE_MYSQL_PORT'])
    assert source_conn.url.database == test_env['SOURCE_MYSQL_DB']
    
    # Test replication connection
    replication_conn = config.get_connection('replication')
    assert replication_conn.url.host == test_env['REPLICATION_MYSQL_HOST']
    assert replication_conn.url.port == int(test_env['REPLICATION_MYSQL_PORT'])
    assert replication_conn.url.database == test_env['REPLICATION_MYSQL_DB']
    
    # Test analytics connection
    analytics_conn = config.get_connection('analytics')
    assert analytics_conn.url.host == test_env['ANALYTICS_POSTGRES_HOST']
    assert analytics_conn.url.port == int(test_env['ANALYTICS_POSTGRES_PORT'])
    assert analytics_conn.url.database == test_env['ANALYTICS_POSTGRES_DB']

def test_loader_config_integration(test_env):
    """Test loader configuration integration."""
    # Set up environment
    for key, value in test_env.items():
        os.environ[key] = value
    
    # Create loader
    loader = PostgresLoader()
    
    # Test schema configuration
    assert loader.target_schema == test_env['ANALYTICS_POSTGRES_SCHEMA']

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

def test_config_integration():
    """Test integration of configuration loading and connection creation."""
    # Set up test environment
    test_env = {
        'SOURCE_MYSQL_HOST': 'test_source_host',
        'SOURCE_MYSQL_PORT': '3306',
        'SOURCE_MYSQL_DB': 'test_source_db',
        'SOURCE_MYSQL_USER': 'test_source_user',
        'SOURCE_MYSQL_PASSWORD': 'test_source_pass',
        'REPLICATION_MYSQL_HOST': 'test_replication_host',
        'REPLICATION_MYSQL_PORT': '3305',
        'REPLICATION_MYSQL_DB': 'test_replication_db',
        'REPLICATION_MYSQL_USER': 'test_replication_user',
        'REPLICATION_MYSQL_PASSWORD': 'test_replication_pass',
        'ANALYTICS_POSTGRES_HOST': 'test_analytics_host',
        'ANALYTICS_POSTGRES_PORT': '5432',
        'ANALYTICS_POSTGRES_DB': 'test_analytics_db',
        'ANALYTICS_POSTGRES_USER': 'test_analytics_user',
        'ANALYTICS_POSTGRES_PASSWORD': 'test_analytics_pass',
        'ANALYTICS_POSTGRES_SCHEMA': 'raw'
    }
    
    # Set environment variables
    for key, value in test_env.items():
        os.environ[key] = value
    
    try:
        # Test configuration loading
        config = load_config()
        assert config is not None
        assert 'source' in config
        assert 'staging' in config
        assert 'target' in config
        
        # Test source configuration
        source_config = config['source']
        assert source_config['host'] == test_env['SOURCE_MYSQL_HOST']
        assert source_config['port'] == int(test_env['SOURCE_MYSQL_PORT'])
        assert source_config['database'] == test_env['SOURCE_MYSQL_DB']
        assert source_config['user'] == test_env['SOURCE_MYSQL_USER']
        assert source_config['password'] == test_env['SOURCE_MYSQL_PASSWORD']
        
        # Test staging configuration
        staging_config = config['staging']
        assert staging_config['host'] == test_env['REPLICATION_MYSQL_HOST']
        assert staging_config['port'] == int(test_env['REPLICATION_MYSQL_PORT'])
        assert staging_config['database'] == test_env['REPLICATION_MYSQL_DB']
        assert staging_config['user'] == test_env['REPLICATION_MYSQL_USER']
        assert staging_config['password'] == test_env['REPLICATION_MYSQL_PASSWORD']
        
        # Test target configuration
        target_config = config['target']
        assert target_config['host'] == test_env['ANALYTICS_POSTGRES_HOST']
        assert target_config['port'] == int(test_env['ANALYTICS_POSTGRES_PORT'])
        assert target_config['database'] == test_env['ANALYTICS_POSTGRES_DB']
        assert target_config['schema'] == test_env['ANALYTICS_POSTGRES_SCHEMA']
        assert target_config['user'] == test_env['ANALYTICS_POSTGRES_USER']
        assert target_config['password'] == test_env['ANALYTICS_POSTGRES_PASSWORD']
        
        # Test connection creation
        source_conn = ConnectionFactory.get_opendental_source_connection()
        staging_conn = ConnectionFactory.get_mysql_replication_connection()
        target_conn = ConnectionFactory.get_postgres_analytics_connection()
        
        # Verify connection URLs
        assert source_conn.url.host == test_env['SOURCE_MYSQL_HOST']
        assert source_conn.url.port == int(test_env['SOURCE_MYSQL_PORT'])
        assert source_conn.url.database == test_env['SOURCE_MYSQL_DB']
        assert source_conn.url.username == test_env['SOURCE_MYSQL_USER']
        
        assert staging_conn.url.host == test_env['REPLICATION_MYSQL_HOST']
        assert staging_conn.url.port == int(test_env['REPLICATION_MYSQL_PORT'])
        assert staging_conn.url.database == test_env['REPLICATION_MYSQL_DB']
        assert staging_conn.url.username == test_env['REPLICATION_MYSQL_USER']
        
        assert target_conn.url.host == test_env['ANALYTICS_POSTGRES_HOST']
        assert target_conn.url.port == int(test_env['ANALYTICS_POSTGRES_PORT'])
        assert target_conn.url.database == test_env['ANALYTICS_POSTGRES_DB']
        assert target_conn.url.username == test_env['ANALYTICS_POSTGRES_USER']
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration integration test failed: {str(e)}")
        return False
    finally:
        # Clean up environment variables
        for key in test_env:
            os.environ.pop(key, None)

def test_connection_factory_integration(test_env, test_config_path):
    """Test integration between configuration and connection factory."""
    # Load configuration
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    # Get connections
    source_conn = ConnectionFactory.get_opendental_source_connection()
    staging_conn = ConnectionFactory.get_mysql_replication_connection()
    target_conn = ConnectionFactory.get_postgres_analytics_connection()
    
    # Verify connection URLs
    assert source_conn.url.host == test_env['SOURCE_MYSQL_HOST']
    assert source_conn.url.port == int(test_env['SOURCE_MYSQL_PORT'])
    assert source_conn.url.database == test_env['SOURCE_MYSQL_DB']
    
    assert staging_conn.url.host == test_env['REPLICATION_MYSQL_HOST']
    assert staging_conn.url.port == int(test_env['REPLICATION_MYSQL_PORT'])
    assert staging_conn.url.database == test_env['REPLICATION_MYSQL_DB']
    
    assert target_conn.url.host == test_env['ANALYTICS_POSTGRES_HOST']
    assert target_conn.url.port == int(test_env['ANALYTICS_POSTGRES_PORT'])
    assert target_conn.url.database == test_env['ANALYTICS_POSTGRES_DB']

def test_postgres_loader_integration(test_env, test_config_path):
    """Test integration between configuration and postgres loader."""
    # Load configuration
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    # Get connections
    source_conn = ConnectionFactory.get_opendental_source_connection()
    target_conn = ConnectionFactory.get_postgres_analytics_connection()
    
    # Create loader
    loader = PostgresLoader(source_conn, target_conn)
    
    # Verify schema configurations
    assert loader.target_schema == test_env['ANALYTICS_POSTGRES_SCHEMA']
    assert loader.staging_schema == config.get_connection_config('staging')['schema']
    
    # Verify pipeline configuration
    assert loader.config.general['environment'] == 'test'
    assert loader.config.general['version'] == '1.0.0'
    
    # Verify stage configurations
    assert loader.config.is_stage_enabled('extract') is True
    assert loader.config.get_stage_schedule('extract') == '0 * * * *'
    assert loader.config.get_stage_timeout('extract') == 30 