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
            },
            'replication': {
                'connect_timeout': 15,
                'read_timeout': 35
            },
            'analytics': {
                'connect_timeout': 25,
                'application_name': 'test_etl'
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
                'incremental_key': 'last_modified',
                'incremental': True,
                'table_importance': 'critical'
            },
            'appointments': {
                'primary_key': 'appointment_id',
                'incremental_key': 'appointment_date',
                'incremental': True,
                'table_importance': 'important'
            },
            'procedures': {
                'primary_key': 'procedure_id',
                'incremental': False,
                'table_importance': 'audit'
            },
            'securitylog': {
                'primary_key': 'securitylog_id',
                'incremental': False,
                'table_importance': 'reference'
            }
        },
        'analytics_tables': {
            'dim_patients': {
                'type': 'dimension',
                'source': 'patients'
            },
            'fact_appointments': {
                'type': 'fact',
                'source': 'appointments'
            }
        }
    }

@pytest.fixture
def mock_settings_instance(mock_env_vars, mock_pipeline_config, mock_tables_config):
    """Fixture to provide a fully configured Settings instance."""
    with patch('pathlib.Path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load') as mock_yaml_load, \
         patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        # Configure yaml.safe_load to return different configs based on file
        def yaml_side_effect(data):
            if 'pipeline' in str(data):
                return mock_pipeline_config
            elif 'tables' in str(data):
                return mock_tables_config
            return {}
        mock_yaml_load.side_effect = yaml_side_effect
        settings = Settings()
        return settings

# ============================================================================
# ENVIRONMENT LOADING TESTS
# ============================================================================

@pytest.mark.unit
def test_load_environment():
    """Test loading environment variables from .env files."""
    # Create a mock for the final .env path
    mock_env_path = MagicMock()
    mock_env_path.exists.return_value = True
    
    # Create a mock for the Path class that returns our mock path
    # Path(__file__).parent.parent = etl_pipeline/
    mock_path = MagicMock()
    mock_path.parent.parent.__truediv__.return_value = mock_env_path
    
    # Patch both Path and load_dotenv
    with patch('etl_pipeline.config.settings.Path', return_value=mock_path), \
         patch('etl_pipeline.config.settings.load_dotenv') as mock_load_dotenv:
        Settings.load_environment()
        mock_load_dotenv.assert_called_once_with(mock_env_path)

@pytest.mark.unit
def test_load_environment_fallback_to_parent():
    """Test loading environment from parent directory when etl_pipeline/.env doesn't exist."""
    # Mock etl_pipeline/.env doesn't exist, but parent/.env does
    mock_etl_env_path = MagicMock()
    mock_etl_env_path.exists.return_value = False
    
    mock_parent_env_path = MagicMock()
    mock_parent_env_path.exists.return_value = True
    
    # Create proper mock path hierarchy matching actual code:
    # Path(__file__).parent.parent = etl_pipeline/
    # Path(__file__).parent.parent.parent = parent/
    mock_path = MagicMock()
    mock_path.parent.parent.__truediv__.return_value = mock_etl_env_path  # etl_pipeline/.env
    mock_path.parent.parent.parent.__truediv__.return_value = mock_parent_env_path  # parent/.env
    
    with patch('etl_pipeline.config.settings.Path', return_value=mock_path), \
         patch('etl_pipeline.config.settings.load_dotenv') as mock_load_dotenv:
        Settings.load_environment()
        mock_load_dotenv.assert_called_once_with(mock_parent_env_path)

@pytest.mark.unit
def test_load_environment_no_env_files():
    """Test loading environment when no .env files exist."""
    mock_env_path = MagicMock()
    mock_env_path.exists.return_value = False
    
    # Create proper mock path hierarchy matching actual code:
    # Path(__file__).parent.parent = etl_pipeline/
    # Path(__file__).parent.parent.parent = parent/
    mock_path = MagicMock()
    mock_path.parent.parent.__truediv__.return_value = mock_env_path  # etl_pipeline/.env
    mock_path.parent.parent.parent.__truediv__.return_value = mock_env_path  # parent/.env
    
    with patch('etl_pipeline.config.settings.Path', return_value=mock_path), \
         patch('etl_pipeline.config.settings.load_dotenv') as mock_load_dotenv, \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        Settings.load_environment()
        mock_load_dotenv.assert_not_called()
        mock_logger.warning.assert_called_once()

# ============================================================================
# CONFIGURATION LOADING TESTS
# ============================================================================

@pytest.mark.unit
def test_load_pipeline_config(mock_pipeline_config):
    """Test loading pipeline configuration from YAML file."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_pipeline_config), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        mock_exists.return_value = True
        settings = Settings()
        assert settings.pipeline_config == mock_pipeline_config

@pytest.mark.unit
def test_load_pipeline_config_yml_file():
    """Test loading pipeline configuration from actual .yml file."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        
        # Load the actual pipeline config
        result = settings.load_pipeline_config()
        
        # Should load the real pipeline.yml file
        assert isinstance(result, dict)
        assert len(result) > 0  # Should have some configuration
        print(f"DEBUG: Loaded pipeline config keys: {list(result.keys())}")
        print(f"DEBUG: Config content: {result}")

@pytest.mark.unit
def test_load_pipeline_config_file_not_found():
    """Test loading pipeline configuration when file doesn't exist."""
    with patch('pathlib.Path.exists', return_value=False), \
         patch('etl_pipeline.config.settings.Settings.load_environment'), \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        settings = Settings()
        assert settings.pipeline_config == {}
        # Check that the specific warning was called (not just any warning)
        warning_calls = [call for call in mock_logger.warning.call_args_list 
                        if 'Pipeline config file not found' in str(call)]
        assert len(warning_calls) == 1

@pytest.mark.unit
def test_load_pipeline_config_yaml_error():
    """Test loading pipeline configuration with YAML parsing error."""
    with patch('pathlib.Path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data='invalid yaml')), \
         patch('yaml.safe_load', side_effect=Exception("YAML Error")), \
         patch('etl_pipeline.config.settings.Settings.load_environment'), \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        settings = Settings()
        assert settings.pipeline_config == {}
        # Check that the specific error was called (not just any error)
        error_calls = [call for call in mock_logger.error.call_args_list 
                      if 'Failed to load pipeline config' in str(call)]
        assert len(error_calls) == 1

@pytest.mark.unit
def test_load_tables_config(mock_tables_config):
    """Test loading tables configuration from YAML file."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=mock_tables_config), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        mock_exists.return_value = True
        settings = Settings()
        assert settings.tables_config == mock_tables_config

@pytest.mark.unit
def test_load_tables_config_file_not_found():
    """Test loading tables configuration when file doesn't exist."""
    with patch('pathlib.Path.exists', return_value=False), \
         patch('etl_pipeline.config.settings.Settings.load_environment'), \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        settings = Settings()
        assert settings.tables_config == {}
        # Check that the specific warning was called (not just any warning)
        warning_calls = [call for call in mock_logger.warning.call_args_list 
                        if 'Tables config file not found' in str(call)]
        assert len(warning_calls) == 1

# ============================================================================
# DATABASE CONFIGURATION TESTS
# ============================================================================

@pytest.mark.unit
def test_get_database_config(mock_env_vars):
    """Test getting database configuration."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        config = settings.get_database_config('source')
        assert config['host'] == 'source_host'
        assert config['port'] == 3306
        assert config['database'] == 'source_db'
        assert config['user'] == 'source_user'
        assert config['password'] == 'source_pass'

@pytest.mark.unit
def test_get_database_config_with_pipeline_overrides(mock_settings_instance):
    """Test getting database configuration with pipeline config overrides."""
    config = mock_settings_instance.get_database_config('source')
    assert config['connect_timeout'] == 20  # From pipeline config
    assert config['read_timeout'] == 40     # From pipeline config
    assert config['host'] == 'source_host'  # From environment

@pytest.mark.unit
def test_get_database_config_caching(mock_env_vars):
    """Test that database configuration is cached."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        
        # First call should populate cache
        config1 = settings.get_database_config('source')
        
        # Second call should use cache
        config2 = settings.get_database_config('source')
        
        assert config1 is config2  # Same object reference
        assert config1 == config2  # Same content

@pytest.mark.unit
def test_get_database_config_unknown_type():
    """Test getting database configuration for unknown database type."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        with pytest.raises(ValueError, match="Unknown database type"):
            settings.get_database_config('unknown')

@pytest.mark.unit
def test_get_database_config_mysql_defaults(mock_env_vars):
    """Test MySQL default connection parameters."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        config = settings.get_database_config('source')
        
        assert config['connect_timeout'] == 10
        assert config['read_timeout'] == 30
        assert config['write_timeout'] == 30
        assert config['charset'] == 'utf8mb4'

@pytest.mark.unit
def test_get_database_config_postgres_defaults(mock_env_vars):
    """Test PostgreSQL default connection parameters."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        config = settings.get_database_config('analytics')
        
        assert config['connect_timeout'] == 10
        assert config['application_name'] == 'etl_pipeline'

@pytest.mark.unit
def test_get_database_config_invalid_port():
    """Test handling of invalid port values."""
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'source_host',
        'OPENDENTAL_SOURCE_PORT': 'invalid_port',
        'OPENDENTAL_SOURCE_DB': 'source_db',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass'
    }
    with patch.dict(os.environ, env_vars), \
         patch('etl_pipeline.config.settings.Settings.load_environment'), \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        settings = Settings()
        config = settings.get_database_config('source')
        assert config['port'] == 3306  # Default MySQL port
        mock_logger.warning.assert_called_once()

# ============================================================================
# CONNECTION STRING TESTS
# ============================================================================

@pytest.mark.unit
def test_get_connection_string_mysql(mock_env_vars):
    """Test generating MySQL connection string."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        mysql_conn_str = settings.get_connection_string('source')
        
        assert 'mysql+pymysql://' in mysql_conn_str
        assert 'source_user:source_pass@source_host:3306/source_db' in mysql_conn_str
        assert 'connect_timeout=10' in mysql_conn_str
        assert 'read_timeout=30' in mysql_conn_str
        assert 'write_timeout=30' in mysql_conn_str
        assert 'charset=utf8mb4' in mysql_conn_str

@pytest.mark.unit
def test_get_connection_string_postgres(mock_env_vars):
    """Test generating PostgreSQL connection string."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        pg_conn_str = settings.get_connection_string('analytics')
        
        assert 'postgresql+psycopg2://' in pg_conn_str
        assert 'analytics_user:analytics_pass@analytics_host:5432/analytics_db' in pg_conn_str
        assert 'connect_timeout=10' in pg_conn_str
        assert 'application_name=etl_pipeline' in pg_conn_str
        assert 'options=-csearch_path%3Dpublic' in pg_conn_str

@pytest.mark.unit
def test_get_connection_string_missing_fields():
    """Test connection string generation with missing required fields."""
    # Set up environment with missing fields
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'source_host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        # Missing database, user, password
    }
    with patch.dict(os.environ, env_vars), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):
        settings = Settings()
        with pytest.raises(ValueError, match="Missing required database config fields"):
            settings.get_connection_string('source')

@pytest.mark.unit
def test_get_connection_string_postgres_no_schema():
    """Test PostgreSQL connection string without schema."""
    env_vars = {
        'POSTGRES_ANALYTICS_HOST': 'analytics_host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics_db',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        # No schema specified
    }
    with patch.dict(os.environ, env_vars), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):
        settings = Settings()
        pg_conn_str = settings.get_connection_string('analytics')
        assert 'options=-csearch_path' not in pg_conn_str

# ============================================================================
# CONFIGURATION VALIDATION TESTS
# ============================================================================

@pytest.mark.unit
def test_validate_configs_success(mock_env_vars):
    """Test successful configuration validation."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        assert settings.validate_configs() is True

@pytest.mark.unit
def test_validate_configs_missing_vars():
    """Test configuration validation with missing variables."""
    with patch.dict(os.environ, {}, clear=True), \
         patch('etl_pipeline.config.settings.Settings.load_environment'), \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        settings = Settings()
        # Force revalidation by clearing the environment
        with patch.dict(os.environ, {}, clear=True):
            assert settings.validate_configs() is False
            mock_logger.error.assert_called()

@pytest.mark.unit
def test_validate_configs_empty_vars():
    """Test configuration validation with empty/invalid variables."""
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'none',
        'OPENDENTAL_SOURCE_PORT': 'null',
        'OPENDENTAL_SOURCE_DB': '',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass'
    }
    with patch.dict(os.environ, env_vars), \
         patch('etl_pipeline.config.settings.Settings.load_environment'), \
         patch('etl_pipeline.config.settings.logger') as mock_logger:
        settings = Settings()
        assert settings.validate_configs() is False
        mock_logger.error.assert_called()

@pytest.mark.unit
def test_validate_configs_schema_optional():
    """Test that schema is optional for PostgreSQL validation."""
    env_vars = {
        'POSTGRES_ANALYTICS_HOST': 'analytics_host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics_db',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        # No schema - should be optional
    }
    with patch.dict(os.environ, env_vars), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):
        settings = Settings()
        assert settings.validate_configs() is True

# ============================================================================
# PIPELINE SETTINGS TESTS
# ============================================================================

@pytest.mark.unit
def test_get_pipeline_setting_nested(mock_settings_instance):
    """Test getting nested pipeline settings."""
    # Test getting a nested setting
    batch_size = mock_settings_instance.get_pipeline_setting('general.batch_size')
    assert batch_size == 1000
    
    # Test getting a deeply nested setting
    timeout = mock_settings_instance.get_pipeline_setting('connections.source.connect_timeout')
    assert timeout == 20

@pytest.mark.unit
def test_get_pipeline_setting_default_value(mock_settings_instance):
    """Test getting pipeline setting with default value."""
    # Test default value for non-existent setting
    result = mock_settings_instance.get_pipeline_setting('nonexistent', 'default')
    assert result == 'default'
    
    # Test default value for non-existent nested setting
    result = mock_settings_instance.get_pipeline_setting('general.nonexistent', 'default')
    assert result == 'default'

@pytest.mark.unit
def test_get_pipeline_setting_empty_key(mock_settings_instance):
    """Test getting pipeline setting with empty key."""
    result = mock_settings_instance.get_pipeline_setting('', 'default')
    assert result == 'default'
    
    result = mock_settings_instance.get_pipeline_setting(None, 'default')
    assert result == 'default'

@pytest.mark.unit
def test_get_pipeline_setting_non_dict_value(mock_settings_instance):
    """Test getting pipeline setting when intermediate value is not a dict."""
    # This would require modifying the mock to have a non-dict value
    # For now, test the logic with a custom config
    custom_config = {'general': 'not_a_dict'}
    mock_settings_instance.pipeline_config = custom_config
    
    result = mock_settings_instance.get_pipeline_setting('general.batch_size', 'default')
    assert result == 'default'

# ============================================================================
# TABLE CONFIGURATION TESTS
# ============================================================================

@pytest.mark.unit
def test_get_table_config(mock_settings_instance):
    """Test getting table configuration."""
    table_config = mock_settings_instance.get_table_config('patients')
    assert table_config['primary_key'] == 'patient_id'
    assert table_config['incremental_key'] == 'last_modified'
    assert table_config['incremental'] is True
    assert table_config['table_importance'] == 'critical'

@pytest.mark.unit
def test_get_table_config_nonexistent_table(mock_settings_instance):
    """Test getting configuration for non-existent table."""
    table_config = mock_settings_instance.get_table_config('nonexistent_table')
    assert table_config == {}

@pytest.mark.unit
def test_get_table_config_custom_table_type(mock_settings_instance):
    """Test getting table configuration with custom table type."""
    table_config = mock_settings_instance.get_table_config('dim_patients', 'analytics_tables')
    assert table_config['type'] == 'dimension'
    assert table_config['source'] == 'patients'

@pytest.mark.unit
def test_list_tables(mock_settings_instance):
    """Test listing tables."""
    source_tables = mock_settings_instance.list_tables('source_tables')
    assert 'patients' in source_tables
    assert 'appointments' in source_tables
    assert 'procedures' in source_tables
    assert 'securitylog' in source_tables
    assert len(source_tables) == 4

@pytest.mark.unit
def test_list_tables_empty_type(mock_settings_instance):
    """Test listing tables for non-existent table type."""
    tables = mock_settings_instance.list_tables('nonexistent_type')
    assert tables == []

@pytest.mark.unit
def test_list_tables_analytics_tables(mock_settings_instance):
    """Test listing analytics tables."""
    analytics_tables = mock_settings_instance.list_tables('analytics_tables')
    assert 'dim_patients' in analytics_tables
    assert 'fact_appointments' in analytics_tables
    assert len(analytics_tables) == 2

# ============================================================================
# TABLE PRIORITY AND INCREMENTAL TESTS
# ============================================================================

@pytest.mark.unit
def test_should_use_incremental_true(mock_settings_instance):
    """Test should_use_incremental returns True for incremental tables."""
    assert mock_settings_instance.should_use_incremental('patients') is True
    assert mock_settings_instance.should_use_incremental('appointments') is True

@pytest.mark.unit
def test_should_use_incremental_false(mock_settings_instance):
    """Test should_use_incremental returns False for non-incremental tables."""
    assert mock_settings_instance.should_use_incremental('procedures') is False
    assert mock_settings_instance.should_use_incremental('securitylog') is False

@pytest.mark.unit
def test_should_use_incremental_nonexistent_table(mock_settings_instance):
    """Test should_use_incremental for non-existent table."""
    assert mock_settings_instance.should_use_incremental('nonexistent') is False

@pytest.mark.unit
def test_get_tables_by_importance_critical(mock_settings_instance):
    """Test getting critical tables by importance."""
    critical_tables = mock_settings_instance.get_tables_by_importance('critical')
    assert isinstance(critical_tables, list)

@pytest.mark.unit
def test_get_tables_by_importance_important(mock_settings_instance):
    """Test getting important tables by importance."""
    important_tables = mock_settings_instance.get_tables_by_importance('important')
    assert isinstance(important_tables, list)

@pytest.mark.unit
def test_get_tables_by_importance_audit(mock_settings_instance):
    """Test getting audit tables by importance."""
    audit_tables = mock_settings_instance.get_tables_by_importance('audit')
    assert isinstance(audit_tables, list)

@pytest.mark.unit
def test_get_tables_by_importance_reference(mock_settings_instance):
    """Test getting reference tables by importance."""
    reference_tables = mock_settings_instance.get_tables_by_importance('reference')
    assert isinstance(reference_tables, list)

@pytest.mark.unit
def test_get_tables_by_importance_nonexistent(mock_settings_instance):
    """Test getting tables for nonexistent importance level."""
    tables = mock_settings_instance.get_tables_by_importance('nonexistent')
    assert tables == []

@pytest.mark.unit
def test_get_tables_by_importance_custom_table_type(mock_settings_instance):
    """Test getting tables by importance with custom table type."""
    analytics_tables = mock_settings_instance.get_tables_by_importance('dimension', 'analytics_tables')
    assert isinstance(analytics_tables, list)

# ============================================================================
# INITIALIZATION TESTS
# ============================================================================

@pytest.mark.unit
def test_settings_initialization(mock_env_vars):
    """Test Settings class initialization."""
    with patch('pathlib.Path.exists', return_value=False), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        assert hasattr(settings, 'pipeline_config')
        assert hasattr(settings, 'tables_config')
        assert hasattr(settings, '_connection_cache')
        assert settings._connection_cache == {}

@pytest.mark.unit
def test_global_settings_instance():
    """Test that global settings instance is created."""
    from etl_pipeline.config.settings import settings
    assert isinstance(settings, Settings)
    assert hasattr(settings, 'get_database_config')
    assert hasattr(settings, 'validate_configs')

# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.unit
def test_get_base_config_unknown_db_type():
    """Test _get_base_config with unknown database type."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        with pytest.raises(ValueError, match="Unknown database type"):
            settings._get_base_config('unknown_type')

@pytest.mark.unit
def test_get_connection_string_unknown_db_type():
    """Test get_connection_string with unknown database type."""
    with patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        with pytest.raises(ValueError, match="Unknown database type"):
            settings.get_connection_string('unknown_type')

@pytest.mark.unit
def test_yaml_safe_load_returns_none():
    """Test handling when yaml.safe_load returns None."""
    with patch('pathlib.Path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data='test: value')), \
         patch('yaml.safe_load', return_value=None), \
         patch('etl_pipeline.config.settings.Settings.load_environment'):  # Prevent real .env loading
        settings = Settings()
        assert settings.pipeline_config == {}
        assert settings.tables_config == {} 