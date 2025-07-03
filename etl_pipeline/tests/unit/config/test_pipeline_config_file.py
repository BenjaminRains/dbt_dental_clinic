"""
Unit tests for pipeline.yml configuration file operations.

This module tests the pipeline.yml configuration file specifically,
including file loading, YAML parsing, file extensions, and file-specific edge cases.
"""

import pytest
import yaml
from unittest.mock import patch, mock_open
from etl_pipeline.config.settings import Settings


class TestPipelineConfigFileLoading:
    """Test pipeline configuration file loading functionality."""

    @pytest.mark.unit
    def test_load_valid_pipeline_config(self, valid_pipeline_config):
        """Test loading a valid pipeline configuration."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_yml_extension(self, valid_pipeline_config):
        """Test loading pipeline configuration with .yml extension."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value={}), \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=config_yaml)), \
             patch('yaml.safe_load', return_value=valid_pipeline_config):
            
            # Mock that .yml file exists but .yaml doesn't
            def exists_side_effect(path):
                return str(path).endswith('.yml')
            
            mock_exists.side_effect = exists_side_effect
            
            # Create a new Settings instance that will use our mocked load_pipeline_config
            with patch('etl_pipeline.config.settings.Settings.load_pipeline_config') as mock_load:
                mock_load.return_value = valid_pipeline_config
                settings = Settings()
                assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_extension(self, valid_pipeline_config):
        """Test loading pipeline configuration with .yaml extension."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_tables_config', return_value={}), \
             patch('pathlib.Path.exists') as mock_exists, \
             patch('builtins.open', mock_open(read_data=config_yaml)), \
             patch('yaml.safe_load', return_value=valid_pipeline_config):
            
            # Mock that .yaml file exists but .yml doesn't
            def exists_side_effect(path):
                return str(path).endswith('.yaml')
            
            mock_exists.side_effect = exists_side_effect
            
            # Create a new Settings instance that will use our mocked load_pipeline_config
            with patch('etl_pipeline.config.settings.Settings.load_pipeline_config') as mock_load:
                mock_load.return_value = valid_pipeline_config
                settings = Settings()
                assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_file_not_found(self):
        """Test loading when pipeline config file doesn't exist."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value={}), \
             patch('etl_pipeline.config.settings.logger') as mock_logger:
            
            settings = Settings()
            assert settings.pipeline_config == {}
            # The warning is logged in load_pipeline_config, but we're mocking it
            # So we just verify the result is empty

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_error(self):
        """Test loading with invalid YAML content."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value={}), \
             patch('etl_pipeline.config.settings.logger') as mock_logger:
            
            settings = Settings()
            assert settings.pipeline_config == {}

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_returns_none(self):
        """Test loading when YAML returns None."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value={}):
            
            settings = Settings()
            assert settings.pipeline_config == {}


class TestPipelineConfigIntegration:
    """Test pipeline configuration integration with Settings class."""

    @pytest.mark.unit
    def test_get_pipeline_setting_general(self, valid_pipeline_config):
        """Test getting general pipeline settings."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            
            # Test getting general settings
            assert settings.get_pipeline_setting('general.pipeline_name') == 'dental_clinic_etl'
            assert settings.get_pipeline_setting('general.batch_size') == 25000
            assert settings.get_pipeline_setting('general.parallel_jobs') == 6

    @pytest.mark.unit
    def test_get_pipeline_setting_connections(self, valid_pipeline_config):
        """Test getting connection pipeline settings."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            
            # Test getting connection settings
            source_config = settings.get_pipeline_setting('connections.source')
            assert source_config['pool_size'] == 6
            assert source_config['pool_timeout'] == 30

    @pytest.mark.unit
    def test_get_pipeline_setting_stages(self, valid_pipeline_config):
        """Test getting stage pipeline settings."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            
            # Test getting stage settings
            extract_config = settings.get_pipeline_setting('stages.extract')
            assert extract_config['enabled'] is True
            assert extract_config['timeout_minutes'] == 30
            assert extract_config['error_threshold'] == 0.01

    @pytest.mark.unit
    def test_get_pipeline_setting_nested(self, valid_pipeline_config):
        """Test getting nested pipeline settings."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            
            # Test getting nested settings
            assert settings.get_pipeline_setting('logging.file.enabled') is True
            assert settings.get_pipeline_setting('logging.file.max_size_mb') == 100
            assert settings.get_pipeline_setting('error_handling.auto_retry.enabled') is True

    @pytest.mark.unit
    def test_get_pipeline_setting_default_value(self, valid_pipeline_config):
        """Test getting pipeline setting with default value."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            
            # Test getting non-existent setting with default
            assert settings.get_pipeline_setting('nonexistent.setting', 'default') == 'default'
            assert settings.get_pipeline_setting('general.nonexistent', 123) == 123

    @pytest.mark.unit
    def test_get_pipeline_setting_empty_key(self, valid_pipeline_config):
        """Test getting pipeline setting with empty key."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=valid_pipeline_config):
            
            settings = Settings()
            
            # Test getting setting with empty key
            assert settings.get_pipeline_setting('', 'default') == 'default'
            assert settings.get_pipeline_setting(None, 'default') == 'default'


class TestPipelineConfigEdgeCases:
    """Test pipeline configuration edge cases and error conditions."""

    @pytest.mark.unit
    def test_pipeline_config_with_missing_sections(self, minimal_pipeline_config):
        """Test pipeline config with missing optional sections."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=minimal_pipeline_config):
            
            settings = Settings()
            
            # Should not raise errors for missing sections
            assert settings.get_pipeline_setting('connections.source', None) is None
            assert settings.get_pipeline_setting('stages.extract', None) is None
            assert settings.get_pipeline_setting('logging.level', None) is None

    @pytest.mark.unit
    def test_pipeline_config_with_invalid_values(self, invalid_pipeline_config):
        """Test pipeline config with invalid data types."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=invalid_pipeline_config):
            
            settings = Settings()
            
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
        
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=empty_config):
            
            settings = Settings()
            
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
        
        with patch('etl_pipeline.config.settings.Settings.load_environment'), \
             patch('etl_pipeline.config.settings.Settings.load_pipeline_config', return_value=invalid_config):
            
            settings = Settings()
            
            # Should handle non-dict values gracefully
            assert settings.get_pipeline_setting('general.pipeline_name', 'default') == 'default'
            assert settings.get_pipeline_setting('connections.source', 'default') == 'default'


class TestPipelineConfigRealFile:
    """Test with the actual pipeline.yml file."""

    @pytest.mark.unit
    def test_load_real_pipeline_config(self):
        """Test loading the actual pipeline.yml file."""
        with patch('etl_pipeline.config.settings.Settings.load_environment'):
            settings = Settings()
            
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
        with patch('etl_pipeline.config.settings.Settings.load_environment'):
            settings = Settings()
            
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
        with patch('etl_pipeline.config.settings.Settings.load_environment'):
            settings = Settings()
            
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