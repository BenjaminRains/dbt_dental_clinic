import pytest
from unittest.mock import patch, mock_open, MagicMock
import yaml
from etl_pipeline.config.loader import ConfigLoader, ETLConfig

@pytest.fixture
def mock_yaml_data():
    return {
        'pipeline': {'key': 'value'},
        'monitoring': {'enabled': True},
        'tables': {
            'source_tables': {'table1': {'schema': 'public'}},
            'staging_tables': {'table2': {'schema': 'staging'}},
            'target_tables': {'table3': {'schema': 'target'}}
        }
    }

@pytest.fixture
def mock_tables_yaml_data():
    return {
        'source_tables': {'table1': {'schema': 'public'}},
        'staging_tables': {'table2': {'schema': 'staging'}},
        'target_tables': {'table3': {'schema': 'target'}}
    }

@pytest.fixture
def mock_database_config():
    return {
        'source': {'host': 'localhost', 'port': 3306, 'database': 'source_db', 'username': 'user'},
        'staging': {'host': 'localhost', 'port': 5432, 'database': 'staging_db', 'username': 'user'},
        'target': {'host': 'localhost', 'port': 5432, 'database': 'target_db', 'username': 'user'}
    }

@pytest.fixture
def config_loader(mock_yaml_data, mock_tables_yaml_data, mock_database_config):
    # Mock Path.exists to return True
    with patch('pathlib.Path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data=yaml.dump(mock_yaml_data))), \
         patch('etl_pipeline.config.loader.ConfigLoader._load_tables_config', return_value=mock_tables_yaml_data), \
         patch('etl_pipeline.config.loader.ConfigLoader._load_database_config', return_value=mock_database_config):
        loader = ConfigLoader('fake_config.yaml')
        return loader

def test_config_loader_initialization(config_loader):
    """Test that ConfigLoader initializes correctly."""
    assert config_loader.config_path.name == 'fake_config.yaml'
    assert config_loader._config is not None

def test_load_config(config_loader):
    """Test that configuration is loaded correctly."""
    config = config_loader.get_config()
    assert isinstance(config, ETLConfig)
    assert config.pipeline == {'key': 'value'}
    assert config.monitoring == {'enabled': True}
    assert config.source_tables == {'table1': {'schema': 'public'}}
    assert config.staging_tables == {'table2': {'schema': 'staging'}}
    assert config.target_tables == {'table3': {'schema': 'target'}}

def test_validate_config(config_loader):
    """Test configuration validation."""
    # Assuming validation logs warnings for missing fields
    # This test ensures no exceptions are raised during validation
    config_loader._validate_config()

def test_get_database_config(config_loader):
    """Test retrieving database configuration."""
    source_config = config_loader.get_database_config('source')
    assert source_config == {'host': 'localhost', 'port': 3306, 'database': 'source_db', 'username': 'user'}

def test_get_table_config(config_loader):
    """Test retrieving table configuration."""
    table_config = config_loader.get_table_config('table1', 'source')
    assert table_config == {'schema': 'public'}

def test_get_pipeline_config(config_loader):
    """Test retrieving pipeline configuration."""
    pipeline_config = config_loader.get_pipeline_config()
    assert pipeline_config == {'key': 'value'}

def test_get_monitoring_config(config_loader):
    """Test retrieving monitoring configuration."""
    monitoring_config = config_loader.get_monitoring_config()
    assert monitoring_config == {'enabled': True} 