"""
Tests for the configuration system.
"""
import os
import pytest
from pathlib import Path
import yaml
from etl_pipeline.config import DatabaseConfig, PipelineConfig, load_config

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
                'charset': 'utf8mb4'
            },
            'replication': {
                'schema': 'replication',
                'charset': 'utf8mb4'
            },
            'analytics': {
                'schema': 'raw',
                'charset': 'utf8'
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
                },
                'slack': {
                    'enabled': False,
                    'webhook_url': 'https://hooks.slack.com/services/xxx'
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

def test_pipeline_config_singleton():
    """Test that PipelineConfig is a singleton."""
    config1 = PipelineConfig()
    config2 = PipelineConfig()
    assert config1 is config2

def test_pipeline_config_load(test_config_path):
    """Test loading configuration from file."""
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    assert config.general['environment'] == 'test'
    assert config.general['version'] == '1.0.0'
    
    assert config.connections['source']['schema'] == 'source_db'
    assert config.connections['replication']['schema'] == 'replication'
    assert config.connections['analytics']['schema'] == 'raw'
    
    assert config.stages['extract']['enabled'] is True
    assert config.stages['transform']['enabled'] is True
    assert config.stages['load']['enabled'] is True

def test_pipeline_config_validation():
    """Test configuration validation."""
    config = PipelineConfig()
    
    # Test missing required sections
    with pytest.raises(ValueError):
        config._validate_config()
    
    # Test with minimal valid config
    config._config = {
        'general': {},
        'connections': {},
        'stages': {},
        'monitoring': {},
        'performance': {},
        'logging': {}
    }
    config._validate_config()  # Should not raise

def test_pipeline_config_getters(test_config_path):
    """Test configuration getter methods."""
    config = PipelineConfig()
    config.load_config(test_config_path)
    
    # Test get_stage_config
    extract_config = config.get_stage_config('extract')
    assert extract_config['enabled'] is True
    assert extract_config['schedule'] == '0 * * * *'
    
    # Test is_stage_enabled
    assert config.is_stage_enabled('extract') is True
    assert config.is_stage_enabled('nonexistent') is False
    
    # Test get_stage_schedule
    assert config.get_stage_schedule('extract') == '0 * * * *'
    assert config.get_stage_schedule('nonexistent') == ''
    
    # Test get_stage_timeout
    assert config.get_stage_timeout('extract') == 30
    assert config.get_stage_timeout('nonexistent') == 30  # Default value
    
    # Test get_connection_config
    source_config = config.get_connection_config('source')
    assert source_config['schema'] == 'source_db'
    assert source_config['charset'] == 'utf8mb4'
    
    # Test get_monitoring_config
    monitoring_config = config.get_monitoring_config()
    assert monitoring_config['enabled'] is True
    assert monitoring_config['log_level'] == 'INFO'
    
    # Test get_alert_config
    email_alert = config.get_alert_config('email')
    assert email_alert['enabled'] is True
    assert email_alert['recipients'] == ['test@example.com']
    
    # Test get_logging_config
    logging_config = config.get_logging_config()
    assert logging_config['level'] == 'INFO'
    assert logging_config['format'] == '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

def test_database_config_environment():
    """Test database configuration environment loading."""
    # Test environment variable loading
    os.environ['SOURCE_MYSQL_HOST'] = 'test_host'
    os.environ['SOURCE_MYSQL_PORT'] = '3306'
    os.environ['SOURCE_MYSQL_DB'] = 'test_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_pass'
    
    source_config = DatabaseConfig.get_source_config()
    assert source_config['host'] == 'test_host'
    assert source_config['port'] == 3306
    assert source_config['database'] == 'test_db'
    assert source_config['user'] == 'test_user'
    assert source_config['password'] == 'test_pass'

def test_database_config_validation():
    """Test database configuration validation."""
    # Test with missing variables
    os.environ.clear()
    assert DatabaseConfig.validate_configs() is False
    
    # Test with empty variables
    os.environ['SOURCE_MYSQL_HOST'] = ''
    os.environ['SOURCE_MYSQL_PORT'] = '3306'
    os.environ['SOURCE_MYSQL_DB'] = 'test_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_pass'
    assert DatabaseConfig.validate_configs() is False
    
    # Test with valid variables
    os.environ['SOURCE_MYSQL_HOST'] = 'test_host'
    assert DatabaseConfig.validate_configs() is True

def test_valid_config():
    """Test configuration with valid environment variables."""
    # Set up test environment
    os.environ['SOURCE_MYSQL_HOST'] = 'test_host'
    os.environ['SOURCE_MYSQL_PORT'] = '3306'
    os.environ['SOURCE_MYSQL_DB'] = 'test_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_pass'
    
    os.environ['REPLICATION_MYSQL_HOST'] = 'test_replication_host'
    os.environ['REPLICATION_MYSQL_PORT'] = '3305'
    os.environ['REPLICATION_MYSQL_DB'] = 'test_replication_db'
    os.environ['REPLICATION_MYSQL_USER'] = 'test_replication_user'
    os.environ['REPLICATION_MYSQL_PASSWORD'] = 'test_replication_pass'
    
    os.environ['ANALYTICS_POSTGRES_HOST'] = 'test_analytics_host'
    os.environ['ANALYTICS_POSTGRES_PORT'] = '5432'
    os.environ['ANALYTICS_POSTGRES_DB'] = 'test_analytics_db'
    os.environ['ANALYTICS_POSTGRES_SCHEMA'] = 'raw'
    os.environ['ANALYTICS_POSTGRES_USER'] = 'test_analytics_user'
    os.environ['ANALYTICS_POSTGRES_PASSWORD'] = 'test_analytics_pass'
    
    # Test configuration loading
    config = load_config()
    assert config is not None
    assert 'source' in config
    assert 'replication' in config
    assert 'analytics' in config

def test_missing_required_vars():
    """Test configuration with missing required environment variables."""
    # Clear environment
    os.environ.clear()
    
    # Set only some variables
    os.environ['SOURCE_MYSQL_HOST'] = ''
    os.environ['SOURCE_MYSQL_PORT'] = '3306'
    os.environ['SOURCE_MYSQL_DB'] = 'test_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_pass'
    
    # Test configuration loading
    with pytest.raises(ValueError):
        load_config()

def test_invalid_port():
    """Test configuration with invalid port numbers."""
    # Set up test environment with invalid port
    os.environ['SOURCE_MYSQL_HOST'] = 'test_host'
    os.environ['SOURCE_MYSQL_PORT'] = 'invalid'
    os.environ['SOURCE_MYSQL_DB'] = 'test_db'
    os.environ['SOURCE_MYSQL_USER'] = 'test_user'
    os.environ['SOURCE_MYSQL_PASSWORD'] = 'test_pass'
    
    # Test configuration loading
    with pytest.raises(ValueError):
        load_config() 