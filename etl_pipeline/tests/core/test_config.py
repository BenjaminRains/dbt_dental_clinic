"""
Tests for the config module.

LEGACY NOTE:
============
This test suite covers the legacy configuration system (etl_pipeline.core.config.PipelineConfig).
It is maintained for backward compatibility and to ensure reliability of the old config system
until full migration to the new etl_pipeline.config.settings.Settings module is complete.
New tests and features should target the new settings module where possible.
"""
import os
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open
import yaml

from etl_pipeline.core.config import PipelineConfig

@pytest.fixture
def mock_env_vars():
    """Mock environment variables."""
    with patch.dict(os.environ, {
        'OPENDENTAL_SOURCE_DB': 'test_source_db',
        'MYSQL_REPLICATION_DB': 'test_replication_db',
        'POSTGRES_ANALYTICS_DB': 'test_analytics_db',
        'POSTGRES_ANALYTICS_SCHEMA': 'test_schema'
    }):
        yield

@pytest.fixture
def mock_env_file():
    """Mock .env file existence."""
    with patch('pathlib.Path.exists') as mock_exists:
        mock_exists.return_value = True
        yield

@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'source_tables': {
            'table1': {
                'incremental_column': 'id',
                'batch_size': 1000,
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'estimated_size_mb': 100,
                'monitoring': {
                    'alert_on_failure': True,
                    'max_extraction_time_minutes': 5,
                    'data_quality_threshold': 0.99
                }
            },
            'table2': {
                'incremental_column': None,
                'batch_size': 5000,
                'extraction_strategy': 'full_table',
                'table_importance': 'reference',
                'estimated_size_mb': 50,
                'monitoring': {
                    'alert_on_failure': False,
                    'max_extraction_time_minutes': 10,
                    'data_quality_threshold': 0.95
                }
            }
        }
    }

def test_pipeline_config_initialization(mock_env_vars, mock_env_file, sample_config):
    """Test PipelineConfig initialization."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(sample_config))):
        config = PipelineConfig()
        
        assert config.source_db == 'test_source_db'
        assert config.replication_db == 'test_replication_db'
        assert config.analytics_db == 'test_analytics_db'
        assert config.analytics_schema == 'test_schema'
        assert config.config == sample_config

def test_missing_env_file():
    """Test initialization with missing .env file."""
    with patch('pathlib.Path.exists') as mock_exists:
        mock_exists.return_value = False
        with pytest.raises(FileNotFoundError):
            PipelineConfig()

def test_missing_env_vars(mock_env_file, sample_config):
    """Test initialization with missing environment variables."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(sample_config))):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                PipelineConfig()
            assert "Missing required environment variables" in str(exc_info.value)

def test_load_config_error(mock_env_vars, mock_env_file):
    """Test error handling in load_config."""
    with patch('builtins.open', mock_open(read_data='invalid: yaml: content')):
        with pytest.raises(yaml.YAMLError):
            PipelineConfig()

def test_get_table_config(mock_env_vars, mock_env_file, sample_config):
    """Test getting table configuration."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(sample_config))):
        config = PipelineConfig()
        
        # Test existing table
        table_config = config.get_table_config('table1')
        assert table_config['incremental_column'] == 'id'
        assert table_config['batch_size'] == 1000
        assert table_config['extraction_strategy'] == 'incremental'
        assert table_config['table_importance'] == 'critical'
        
        # Test non-existent table (should return defaults)
        default_config = config.get_table_config('non_existent')
        assert default_config['incremental_column'] is None
        assert default_config['batch_size'] == 5000
        assert default_config['extraction_strategy'] == 'full_table'
        assert default_config['table_importance'] == 'reference'

def test_get_tables_by_priority(mock_env_vars, mock_env_file, sample_config):
    """Test getting tables by priority."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(sample_config))):
        config = PipelineConfig()
        
        # Test critical tables
        critical_tables = config.get_tables_by_priority('critical')
        assert len(critical_tables) == 1
        assert 'table1' in critical_tables
        
        # Test reference tables
        reference_tables = config.get_tables_by_priority('reference')
        assert len(reference_tables) == 1
        assert 'table2' in reference_tables

def test_get_critical_tables(mock_env_vars, mock_env_file, sample_config):
    """Test getting critical tables."""
    with patch('builtins.open', mock_open(read_data=yaml.dump(sample_config))):
        config = PipelineConfig()
        
        critical_tables = config.get_critical_tables()
        assert len(critical_tables) == 1
        assert 'table1' in critical_tables 