import pytest
from unittest.mock import patch, mock_open, MagicMock
import os
from pathlib import Path
from etl_pipeline.config.settings import Settings

@pytest.fixture
def mock_env_vars():
    """Fixture to set up mock environment variables."""
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
        'POSTGRES_ANALYTICS_SCHEMA': 'public',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars

@pytest.fixture
def mock_pipeline_config():
    """Fixture to provide mock pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'dental_clinic_elt',
            'environment': 'production',
            'batch_size': 1000,
            'max_retries': 3
        },
        'connections': {
            'source': {
                'connect_timeout': 20,
                'read_timeout': 40
            }
        }
    }

@pytest.fixture
def mock_tables_config():
    """Fixture to provide mock tables configuration."""
    return {
        'source_tables': {
            'patients': {
                'primary_key': 'patient_id',
                'incremental_key': 'last_modified'
            },
            'appointments': {
                'primary_key': 'appointment_id',
                'incremental_key': 'appointment_date'
            }
        },
        'analytics_tables': {
            'dim_patients': {
                'type': 'dimension',
                'source': 'patients'
            }
        }
    }

def test_load_environment():
    """Test loading environment variables from .env files."""
    # Create a mock for the final .env path
    mock_env_path = MagicMock()
    mock_env_path.exists.return_value = True
    
    # Create a mock for the Path class that returns our mock path
    mock_path = MagicMock()
    mock_path.parent.parent.__truediv__.return_value = mock_env_path
    
    # Patch both Path and load_dotenv
    with patch('etl_pipeline.config.settings.Path', return_value=mock_path), \
         patch('etl_pipeline.config.settings.load_dotenv') as mock_load_dotenv:
        Settings.load_environment()
        mock_load_dotenv.assert_called_once_with(mock_env_path)

def test_load_pipeline_config(mock_pipeline_config):
    """Test loading pipeline configuration from YAML file."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_pipeline_config):
        mock_exists.return_value = True
        settings = Settings()
        assert settings.pipeline_config == mock_pipeline_config

def test_load_tables_config(mock_tables_config):
    """Test loading tables configuration from YAML file."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_tables_config):
        mock_exists.return_value = True
        settings = Settings()
        assert settings.tables_config == mock_tables_config

def test_get_database_config(mock_env_vars):
    """Test getting database configuration."""
    settings = Settings()
    config = settings.get_database_config('source')
    assert config['host'] == 'source_host'
    assert config['port'] == 3306
    assert config['database'] == 'source_db'
    assert config['user'] == 'source_user'
    assert config['password'] == 'source_pass'

def test_get_connection_string(mock_env_vars):
    """Test generating connection strings."""
    settings = Settings()
    
    # Test MySQL connection string
    mysql_conn_str = settings.get_connection_string('source')
    assert 'mysql+pymysql://' in mysql_conn_str
    assert 'source_user:source_pass@source_host:3306/source_db' in mysql_conn_str
    
    # Test PostgreSQL connection string
    pg_conn_str = settings.get_connection_string('analytics')
    assert 'postgresql+psycopg2://' in pg_conn_str
    assert 'analytics_user:analytics_pass@analytics_host:5432/analytics_db' in pg_conn_str
    assert 'options=-csearch_path%3Dpublic' in pg_conn_str

def test_validate_configs(mock_env_vars):
    """Test configuration validation."""
    settings = Settings()
    assert settings.validate_configs() is True

def test_validate_configs_missing_vars():
    """Test configuration validation with missing variables."""
    with patch.dict(os.environ, {}, clear=True), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):
        settings = Settings()
        # Force revalidation by clearing the environment
        with patch.dict(os.environ, {}, clear=True):
            assert settings.validate_configs() is False

def test_get_pipeline_setting(mock_pipeline_config):
    """Test getting pipeline settings."""
    with patch('pathlib.Path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_pipeline_config), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):
        settings = Settings()
        # Force reload of pipeline config
        settings.pipeline_config = mock_pipeline_config
        # Test getting a nested setting
        assert settings.get_pipeline_setting('general.batch_size') == mock_pipeline_config['general']['batch_size']
        # Test getting a top-level setting
        assert settings.get_pipeline_setting('connections.source.connect_timeout') == mock_pipeline_config['connections']['source']['connect_timeout']
        # Test default value
        assert settings.get_pipeline_setting('nonexistent', 'default') == 'default'

def test_get_table_config(mock_tables_config):
    """Test getting table configuration."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_tables_config):
        mock_exists.return_value = True
        settings = Settings()
        table_config = settings.get_table_config('patients')
        assert table_config['primary_key'] == 'patient_id'
        assert table_config['incremental_key'] == 'last_modified'

def test_list_tables(mock_tables_config):
    """Test listing tables."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_tables_config):
        mock_exists.return_value = True
        settings = Settings()
        source_tables = settings.list_tables('source_tables')
        assert 'patients' in source_tables
        assert 'appointments' in source_tables 