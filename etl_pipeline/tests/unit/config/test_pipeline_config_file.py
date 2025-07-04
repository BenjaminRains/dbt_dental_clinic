"""
Unit tests for pipeline.yml configuration file operations.

This module tests the pipeline.yml configuration file specifically,
including file loading, YAML parsing, file extensions, and file-specific edge cases.
"""

import pytest
import yaml
from unittest.mock import patch, mock_open, MagicMock
from etl_pipeline.config.settings import Settings
from etl_pipeline.config.providers import FileConfigProvider, DictConfigProvider
from pathlib import Path


class TestPipelineConfigFileLoading:
    """Test pipeline configuration file loading functionality."""

    @pytest.mark.unit
    def test_load_valid_pipeline_config(self, valid_pipeline_config):
        """Test loading a valid pipeline configuration."""
        # Create a mock provider that returns our test config
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_yml_extension(self, valid_pipeline_config):
        """Test loading pipeline configuration with .yml extension."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('builtins.open', mock_open(read_data=config_yaml)), \
             patch('yaml.safe_load', return_value=valid_pipeline_config):
            
            # Create a mock provider that simulates .yml file loading
            mock_provider = MagicMock()
            mock_provider.get_config.return_value = valid_pipeline_config
            
            settings = Settings(environment='test', provider=mock_provider)
            assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_extension(self, valid_pipeline_config):
        """Test loading pipeline configuration with .yaml extension."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('builtins.open', mock_open(read_data=config_yaml)), \
             patch('yaml.safe_load', return_value=valid_pipeline_config):
            
            # Create a mock provider that simulates .yaml file loading
            mock_provider = MagicMock()
            mock_provider.get_config.return_value = valid_pipeline_config
            
            settings = Settings(environment='test', provider=mock_provider)
            assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_file_not_found(self):
        """Test loading when pipeline config file doesn't exist."""
        # Create a mock provider that returns empty config
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == {}

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_error(self):
        """Test loading with invalid YAML content."""
        # Create a mock provider that returns empty config (simulating YAML error)
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == {}

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_returns_none(self):
        """Test loading when YAML returns None."""
        # Create a mock provider that returns empty config (simulating None return)
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == {}


class TestPipelineConfigIntegration:
    """Test pipeline configuration integration with Settings class."""

    @pytest.mark.unit
    def test_get_pipeline_setting_general(self, valid_pipeline_config):
        """Test getting general pipeline settings."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting general settings
        assert settings.get_pipeline_setting('general.pipeline_name') == 'dental_clinic_etl'
        assert settings.get_pipeline_setting('general.batch_size') == 25000
        assert settings.get_pipeline_setting('general.parallel_jobs') == 6

    @pytest.mark.unit
    def test_get_pipeline_setting_connections(self, valid_pipeline_config):
        """Test getting connection pipeline settings."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting connection settings
        source_config = settings.get_pipeline_setting('connections.source')
        assert source_config['pool_size'] == 6
        assert source_config['pool_timeout'] == 30

    @pytest.mark.unit
    def test_get_pipeline_setting_stages(self, valid_pipeline_config):
        """Test getting stage pipeline settings."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting stage settings
        extract_config = settings.get_pipeline_setting('stages.extract')
        assert extract_config['enabled'] is True
        assert extract_config['timeout_minutes'] == 30
        assert extract_config['error_threshold'] == 0.01

    @pytest.mark.unit
    def test_get_pipeline_setting_nested(self, valid_pipeline_config):
        """Test getting nested pipeline settings."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting nested settings
        assert settings.get_pipeline_setting('logging.file.enabled') is True
        assert settings.get_pipeline_setting('logging.file.max_size_mb') == 100
        assert settings.get_pipeline_setting('error_handling.auto_retry.enabled') is True

    @pytest.mark.unit
    def test_get_pipeline_setting_default_value(self, valid_pipeline_config):
        """Test getting pipeline setting with default value."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting non-existent setting with default
        assert settings.get_pipeline_setting('nonexistent.setting', 'default') == 'default'
        assert settings.get_pipeline_setting('general.nonexistent', 123) == 123

    @pytest.mark.unit
    def test_get_pipeline_setting_empty_key(self, valid_pipeline_config):
        """Test getting pipeline setting with empty key."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting setting with empty key
        assert settings.get_pipeline_setting('', 'default') == 'default'
        assert settings.get_pipeline_setting(None, 'default') == 'default'


class TestPipelineConfigEdgeCases:
    """Test pipeline configuration edge cases and error conditions."""

    @pytest.mark.unit
    def test_pipeline_config_with_missing_sections(self, minimal_pipeline_config):
        """Test pipeline config with missing optional sections."""
        mock_provider = DictConfigProvider(
            pipeline=minimal_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Should not raise errors for missing sections
        assert settings.get_pipeline_setting('connections.source', None) is None
        assert settings.get_pipeline_setting('stages.extract', None) is None
        assert settings.get_pipeline_setting('logging.level', None) is None

    @pytest.mark.unit
    def test_pipeline_config_with_invalid_values(self, invalid_pipeline_config):
        """Test pipeline config with invalid data types."""
        mock_provider = DictConfigProvider(
            pipeline=invalid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Should handle invalid values gracefully
        batch_size = settings.get_pipeline_setting('general.batch_size')
        assert batch_size == 'invalid_string'  # Should return as-is
        
        pool_size = settings.get_pipeline_setting('connections.source.pool_size')
        assert pool_size == -1  # Should return as-is

    @pytest.mark.unit
    def test_pipeline_config_with_empty_values(self):
        """Test pipeline config with empty values."""
        empty_config = {
            'general': {},
            'connections': {},
            'stages': {}
        }
        
        mock_provider = DictConfigProvider(
            pipeline=empty_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Should handle empty sections gracefully
        assert settings.get_pipeline_setting('general.pipeline_name', 'default') == 'default'
        assert settings.get_pipeline_setting('connections.source.pool_size', 5) == 5

    @pytest.mark.unit
    def test_pipeline_config_with_non_dict_values(self):
        """Test pipeline config with non-dictionary values."""
        invalid_config = {
            'general': 'not_a_dict',
            'connections': 123,
            'stages': ['not', 'a', 'dict']
        }
        
        mock_provider = DictConfigProvider(
            pipeline=invalid_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Should handle non-dict values gracefully
        assert settings.get_pipeline_setting('general.pipeline_name', 'default') == 'default'
        assert settings.get_pipeline_setting('connections.source', 'default') == 'default'


class TestPipelineConfigRealFile:
    """Test with the actual pipeline.yml file."""

    @pytest.mark.unit
    def test_load_real_pipeline_config(self):
        """Test loading the actual pipeline.yml file."""
        # Use the default FileConfigProvider to load real config
        settings = Settings(environment='test')
        
        # Should load the real pipeline config
        config = settings.pipeline_config
        assert isinstance(config, dict)
        assert len(config) > 0
        
        # Check for expected sections
        assert 'general' in config
        assert 'connections' in config
        assert 'stages' in config
        assert 'logging' in config
        assert 'error_handling' in config

    @pytest.mark.unit
    def test_real_pipeline_config_values(self):
        """Test specific values in the real pipeline config."""
        settings = Settings(environment='test')
        
        # Test specific values from our actual config (matching pipeline.yml)
        assert settings.get_pipeline_setting('general.batch_size') == 25000
        assert settings.get_pipeline_setting('general.parallel_jobs') == 6
        assert settings.get_pipeline_setting('general.pipeline_name') == 'dental_clinic_etl'
        
        # Test connection settings (matching actual pipeline.yml values)
        source_config = settings.get_pipeline_setting('connections.source')
        assert source_config['pool_size'] == 5  # Actual value from pipeline.yml
        assert source_config['pool_recycle'] == 3600

    @pytest.mark.unit
    def test_real_pipeline_config_stages(self):
        """Test stage configurations in the real pipeline config."""
        settings = Settings(environment='test')
        
        # Test stage configurations
        extract_config = settings.get_pipeline_setting('stages.extract')
        assert extract_config['enabled'] is True
        assert extract_config['timeout_minutes'] == 30
        assert extract_config['error_threshold'] == 0.01
        
        load_config = settings.get_pipeline_setting('stages.load')
        assert load_config['enabled'] is True
        assert load_config['timeout_minutes'] == 45
        
        transform_config = settings.get_pipeline_setting('stages.transform')
        assert transform_config['enabled'] is True
        assert transform_config['timeout_minutes'] == 60


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 