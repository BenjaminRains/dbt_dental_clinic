"""
Tests for the data loader module.
"""
import os
import pytest
from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.config import get_source_config, get_staging_config, get_target_config

@pytest.fixture
def setup_test_env():
    """Set up test environment variables."""
    # Source database
    os.environ['SOURCE_MYSQL_HOST'] = 'test_source_host'
    os.environ['SOURCE_MYSQL_PORT'] = '3306'
    os.environ['SOURCE_MYSQL_DB'] = 'test_source_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_source_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_source_pass'
    
    # Replication database
    os.environ['REPLICATION_MYSQL_HOST'] = 'test_replication_host'
    os.environ['REPLICATION_MYSQL_PORT'] = '3305'
    os.environ['REPLICATION_MYSQL_DB'] = 'test_replication_db'
    os.environ['REPLICATION_MYSQL_USER'] = 'test_replication_user'
    os.environ['REPLICATION_MYSQL_PASSWORD'] = 'test_replication_pass'
    
    # Analytics database
    os.environ['ANALYTICS_POSTGRES_HOST'] = 'test_analytics_host'
    os.environ['ANALYTICS_POSTGRES_PORT'] = '5432'
    os.environ['ANALYTICS_POSTGRES_DB'] = 'test_analytics_db'
    os.environ['ANALYTICS_POSTGRES_SCHEMA'] = 'raw'
    os.environ['ANALYTICS_POSTGRES_USER'] = 'test_analytics_user'
    os.environ['ANALYTICS_POSTGRES_PASSWORD'] = 'test_analytics_pass'
    
    yield
    
    # Clean up environment variables
    for key in list(os.environ.keys()):
        if any(prefix in key for prefix in ['SOURCE_MYSQL_', 'REPLICATION_MYSQL_', 'ANALYTICS_POSTGRES_']):
            del os.environ[key]

def test_get_source_config():
    """Test getting source database configuration."""
    os.environ['SOURCE_MYSQL_HOST'] = 'test_host'
    os.environ['SOURCE_MYSQL_PORT'] = '3306'
    os.environ['SOURCE_MYSQL_DB'] = 'test_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_pass'
    
    config = get_source_config()
    assert config['host'] == 'test_host'
    assert config['port'] == 3306
    assert config['database'] == 'test_db'
    assert config['user'] == 'test_user'
    assert config['password'] == 'test_pass'

def test_get_staging_config():
    """Test getting staging database configuration."""
    os.environ['REPLICATION_MYSQL_HOST'] = 'test_staging_host'
    os.environ['REPLICATION_MYSQL_PORT'] = '3305'
    os.environ['REPLICATION_MYSQL_DB'] = 'test_staging_db'
    os.environ['REPLICATION_MYSQL_USER'] = 'test_staging_user'
    os.environ['REPLICATION_MYSQL_PASSWORD'] = 'test_staging_pass'
    
    config = get_staging_config()
    assert config['host'] == 'test_staging_host'
    assert config['port'] == 3305
    assert config['database'] == 'test_staging_db'
    assert config['user'] == 'test_staging_user'
    assert config['password'] == 'test_staging_pass'

def test_get_target_config():
    """Test getting target database configuration."""
    os.environ['ANALYTICS_POSTGRES_HOST'] = 'test_target_host'
    os.environ['ANALYTICS_POSTGRES_PORT'] = '5432'
    os.environ['ANALYTICS_POSTGRES_DB'] = 'test_target_db'
    os.environ['ANALYTICS_POSTGRES_SCHEMA'] = 'raw'
    os.environ['ANALYTICS_POSTGRES_USER'] = 'test_target_user'
    os.environ['ANALYTICS_POSTGRES_PASSWORD'] = 'test_target_pass'
    
    config = get_target_config()
    assert config['host'] == 'test_target_host'
    assert config['port'] == 5432
    assert config['database'] == 'test_target_db'
    assert config['schema'] == 'raw'
    assert config['user'] == 'test_target_user'
    assert config['password'] == 'test_target_pass' 