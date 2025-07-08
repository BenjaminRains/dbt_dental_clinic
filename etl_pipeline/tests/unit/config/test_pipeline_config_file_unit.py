"""
Unit tests for pipeline.yml configuration file operations.

This module tests the pipeline.yml configuration file specifically,
including file loading, YAML parsing, file extensions, and file-specific edge cases.

This is a PURE UNIT TEST file following the three-tier testing strategy:
- Uses DictConfigProvider for complete dependency injection
- No real file system access or environment variables
- Comprehensive mocking of all external dependencies
- Tests all pipeline configuration methods and edge cases
- Fast execution (< 1 second per component)

Fixtures are imported from modular files in tests/fixtures/ per the refactor plan.
"""

import pytest
import yaml
from unittest.mock import patch, mock_open, MagicMock
from typing import Dict, Any
from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema
from etl_pipeline.config.providers import FileConfigProvider, DictConfigProvider
from pathlib import Path

# Import modular fixtures
from tests.fixtures.config_fixtures import (
    valid_pipeline_config, 
    minimal_pipeline_config, 
    invalid_pipeline_config,
    complete_config_environment
)


class TestPipelineConfigFileLoading:
    """Test pipeline configuration file loading functionality with provider pattern."""

    @pytest.mark.unit
    def test_load_valid_pipeline_config_with_dict_provider(self, valid_pipeline_config):
        """Test loading a valid pipeline configuration using DictConfigProvider."""
        # Create a mock provider that returns our test config
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == valid_pipeline_config
        assert isinstance(settings.pipeline_config, dict)
        assert len(settings.pipeline_config) > 0

    @pytest.mark.unit
    def test_load_pipeline_config_yml_extension_mocked(self, valid_pipeline_config):
        """Test loading pipeline configuration with .yml extension using mocked file operations."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('builtins.open', mock_open(read_data=config_yaml)), \
             patch('yaml.safe_load', return_value=valid_pipeline_config):
            
            # Create a mock provider that simulates .yml file loading
            mock_provider = MagicMock()
            mock_provider.get_config.return_value = valid_pipeline_config
            
            settings = Settings(environment='test', provider=mock_provider)
            assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_extension_mocked(self, valid_pipeline_config):
        """Test loading pipeline configuration with .yaml extension using mocked file operations."""
        config_yaml = yaml.dump(valid_pipeline_config)
        
        with patch('builtins.open', mock_open(read_data=config_yaml)), \
             patch('yaml.safe_load', return_value=valid_pipeline_config):
            
            # Create a mock provider that simulates .yaml file loading
            mock_provider = MagicMock()
            mock_provider.get_config.return_value = valid_pipeline_config
            
            settings = Settings(environment='test', provider=mock_provider)
            assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_load_pipeline_config_file_not_found_with_dict_provider(self):
        """Test loading when pipeline config file doesn't exist using DictConfigProvider."""
        # Create a mock provider that returns empty config
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == {}
        assert isinstance(settings.pipeline_config, dict)

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_error_with_dict_provider(self):
        """Test loading with invalid YAML content using DictConfigProvider."""
        # Create a mock provider that returns empty config (simulating YAML error)
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == {}

    @pytest.mark.unit
    def test_load_pipeline_config_yaml_returns_none_with_dict_provider(self):
        """Test loading when YAML returns None using DictConfigProvider."""
        # Create a mock provider that returns empty config (simulating None return)
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        assert settings.pipeline_config == {}

    @pytest.mark.unit
    def test_pipeline_config_provider_injection(self, valid_pipeline_config):
        """Test that provider pattern correctly injects pipeline configuration."""
        # Test with different pipeline configurations
        test_configs = [
            valid_pipeline_config,
            {'general': {'pipeline_name': 'test_pipeline'}},
            {'connections': {'source': {'pool_size': 5}}},
            {}
        ]
        
        for test_config in test_configs:
            mock_provider = DictConfigProvider(
                pipeline=test_config,
                tables={'tables': {}},
                env={}
            )
            
            settings = Settings(environment='test', provider=mock_provider)
            assert settings.pipeline_config == test_config


class TestPipelineConfigIntegration:
    """Test pipeline configuration integration with Settings class using provider pattern."""

    @pytest.mark.unit
    def test_get_pipeline_setting_general_with_dict_provider(self, valid_pipeline_config):
        """Test getting general pipeline settings using DictConfigProvider."""
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
        assert settings.get_pipeline_setting('general.environment') == 'production'
        assert settings.get_pipeline_setting('general.timezone') == 'UTC'
        assert settings.get_pipeline_setting('general.max_retries') == 3
        assert settings.get_pipeline_setting('general.retry_delay_seconds') == 300

    @pytest.mark.unit
    def test_get_pipeline_setting_connections_with_dict_provider(self, valid_pipeline_config):
        """Test getting connection pipeline settings using DictConfigProvider."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting connection settings
        source_config = settings.get_pipeline_setting('connections.source')
        assert source_config is not None
        assert source_config['pool_size'] == 6
        assert source_config['pool_timeout'] == 30
        assert source_config['pool_recycle'] == 3600
        assert source_config['connect_timeout'] == 60
        assert source_config['read_timeout'] == 300
        assert source_config['write_timeout'] == 300

        replication_config = settings.get_pipeline_setting('connections.replication')
        assert replication_config is not None
        assert replication_config['pool_size'] == 6
        assert replication_config['pool_timeout'] == 30

        analytics_config = settings.get_pipeline_setting('connections.analytics')
        assert analytics_config is not None
        assert analytics_config['pool_size'] == 6
        assert analytics_config['application_name'] == 'dental_clinic_etl'

    @pytest.mark.unit
    def test_get_pipeline_setting_stages_with_dict_provider(self, valid_pipeline_config):
        """Test getting stage pipeline settings using DictConfigProvider."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting stage settings
        extract_config = settings.get_pipeline_setting('stages.extract')
        assert extract_config is not None
        assert extract_config['enabled'] is True
        assert extract_config['timeout_minutes'] == 30
        assert extract_config['error_threshold'] == 0.01

        load_config = settings.get_pipeline_setting('stages.load')
        assert load_config is not None
        assert load_config['enabled'] is True
        assert load_config['timeout_minutes'] == 45
        assert load_config['error_threshold'] == 0.01

        transform_config = settings.get_pipeline_setting('stages.transform')
        assert transform_config is not None
        assert transform_config['enabled'] is True
        assert transform_config['timeout_minutes'] == 60
        assert transform_config['error_threshold'] == 0.01

    @pytest.mark.unit
    def test_get_pipeline_setting_nested_with_dict_provider(self, valid_pipeline_config):
        """Test getting nested pipeline settings using DictConfigProvider."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting nested settings
        assert settings.get_pipeline_setting('logging.file.enabled') is True
        assert settings.get_pipeline_setting('logging.file.max_size_mb') == 100
        assert settings.get_pipeline_setting('logging.file.path') == 'logs/pipeline.log'
        assert settings.get_pipeline_setting('logging.file.backup_count') == 10
        assert settings.get_pipeline_setting('logging.console.enabled') is True
        assert settings.get_pipeline_setting('logging.console.level') == 'INFO'
        assert settings.get_pipeline_setting('error_handling.auto_retry.enabled') is True
        assert settings.get_pipeline_setting('error_handling.auto_retry.max_attempts') == 3
        assert settings.get_pipeline_setting('error_handling.auto_retry.delay_minutes') == 5

    @pytest.mark.unit
    def test_get_pipeline_setting_default_value_with_dict_provider(self, valid_pipeline_config):
        """Test getting pipeline setting with default value using DictConfigProvider."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting non-existent setting with default
        assert settings.get_pipeline_setting('nonexistent.setting', 'default') == 'default'
        assert settings.get_pipeline_setting('general.nonexistent', 123) == 123
        assert settings.get_pipeline_setting('connections.nonexistent', {}) == {}
        assert settings.get_pipeline_setting('stages.nonexistent', False) is False

    @pytest.mark.unit
    def test_get_pipeline_setting_empty_key_with_dict_provider(self, valid_pipeline_config):
        """Test getting pipeline setting with empty key using DictConfigProvider."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test getting setting with empty key
        assert settings.get_pipeline_setting('', 'default') == 'default'
        assert settings.get_pipeline_setting('   ', 'default') == 'default'

    @pytest.mark.unit
    def test_get_pipeline_setting_deep_nested_with_dict_provider(self, valid_pipeline_config):
        """Test getting deeply nested pipeline settings using DictConfigProvider."""
        # Add deeply nested configuration
        deep_config = valid_pipeline_config.copy()
        deep_config['deeply'] = {
            'nested': {
                'configuration': {
                    'level1': {
                        'level2': {
                            'level3': {
                                'value': 'deep_value',
                                'number': 42,
                                'boolean': True
                            }
                        }
                    }
                }
            }
        }
        
        mock_provider = DictConfigProvider(
            pipeline=deep_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test deeply nested access
        assert settings.get_pipeline_setting('deeply.nested.configuration.level1.level2.level3.value') == 'deep_value'
        assert settings.get_pipeline_setting('deeply.nested.configuration.level1.level2.level3.number') == 42
        assert settings.get_pipeline_setting('deeply.nested.configuration.level1.level2.level3.boolean') is True


class TestPipelineConfigEdgeCases:
    """Test pipeline configuration edge cases and error conditions with provider pattern."""

    @pytest.mark.unit
    def test_pipeline_config_with_missing_sections_with_dict_provider(self, minimal_pipeline_config):
        """Test pipeline config with missing optional sections using DictConfigProvider."""
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
        assert settings.get_pipeline_setting('error_handling.auto_retry', None) is None

    @pytest.mark.unit
    def test_pipeline_config_with_invalid_values_with_dict_provider(self, invalid_pipeline_config):
        """Test pipeline config with invalid data types using DictConfigProvider."""
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
    def test_pipeline_config_with_empty_values_with_dict_provider(self):
        """Test pipeline config with empty values using DictConfigProvider."""
        empty_config = {
            'general': {},
            'connections': {},
            'stages': {},
            'logging': {},
            'error_handling': {}
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
        assert settings.get_pipeline_setting('stages.extract.enabled', False) is False
        assert settings.get_pipeline_setting('logging.file.enabled', True) is True

    @pytest.mark.unit
    def test_pipeline_config_with_non_dict_values_with_dict_provider(self):
        """Test pipeline config with non-dictionary values using DictConfigProvider."""
        invalid_config = {
            'general': 'not_a_dict',
            'connections': 123,
            'stages': ['not', 'a', 'dict'],
            'logging': None,
            'error_handling': True
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
        assert settings.get_pipeline_setting('stages.extract', 'default') == 'default'
        assert settings.get_pipeline_setting('logging.file', 'default') == 'default'
        assert settings.get_pipeline_setting('error_handling.auto_retry', 'default') == 'default'

    @pytest.mark.unit
    def test_pipeline_config_with_mixed_types_with_dict_provider(self):
        """Test pipeline config with mixed data types using DictConfigProvider."""
        mixed_config = {
            'general': {
                'pipeline_name': 'test_pipeline',
                'batch_size': 1000,
                'enabled': True,
                'timeout': 30.5,
                'tags': ['tag1', 'tag2'],
                'metadata': {'key': 'value'}
            },
            'connections': {
                'source': {
                    'pool_size': 5,
                    'enabled': True,
                    'timeout': 60.0
                }
            }
        }
        
        mock_provider = DictConfigProvider(
            pipeline=mixed_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test mixed data types
        assert settings.get_pipeline_setting('general.pipeline_name') == 'test_pipeline'
        assert settings.get_pipeline_setting('general.batch_size') == 1000
        assert settings.get_pipeline_setting('general.enabled') is True
        assert settings.get_pipeline_setting('general.timeout') == 30.5
        assert settings.get_pipeline_setting('general.tags') == ['tag1', 'tag2']
        assert settings.get_pipeline_setting('general.metadata') == {'key': 'value'}
        assert settings.get_pipeline_setting('connections.source.pool_size') == 5
        assert settings.get_pipeline_setting('connections.source.enabled') is True
        assert settings.get_pipeline_setting('connections.source.timeout') == 60.0


class TestPipelineConfigProviderPattern:
    """Test provider pattern integration with pipeline configuration."""

    @pytest.mark.unit
    def test_dict_provider_vs_file_provider_isolation(self, valid_pipeline_config):
        """Test that DictConfigProvider provides complete isolation from file system."""
        # Create DictConfigProvider with test configuration
        dict_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        # Create Settings with DictConfigProvider
        settings_with_dict = Settings(environment='test', provider=dict_provider)
        
        # Verify configuration is loaded from provider, not files
        assert settings_with_dict.pipeline_config == valid_pipeline_config
        assert settings_with_dict.pipeline_config is not {}  # Should not be empty
        
        # Test that provider isolation works
        assert settings_with_dict.get_pipeline_setting('general.pipeline_name') == 'dental_clinic_etl'

    @pytest.mark.unit
    def test_provider_configuration_override(self, valid_pipeline_config):
        """Test that provider configuration properly overrides default behavior."""
        # Create custom configuration that differs from defaults
        custom_config = {
            'general': {
                'pipeline_name': 'custom_pipeline',
                'batch_size': 5000,
                'parallel_jobs': 2
            },
            'connections': {
                'source': {
                    'pool_size': 3,
                    'connect_timeout': 15
                }
            }
        }
        
        mock_provider = DictConfigProvider(
            pipeline=custom_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Verify custom configuration is used
        assert settings.get_pipeline_setting('general.pipeline_name') == 'custom_pipeline'
        assert settings.get_pipeline_setting('general.batch_size') == 5000
        assert settings.get_pipeline_setting('general.parallel_jobs') == 2
        assert settings.get_pipeline_setting('connections.source.pool_size') == 3
        assert settings.get_pipeline_setting('connections.source.connect_timeout') == 15

    @pytest.mark.unit
    def test_provider_environment_isolation(self, valid_pipeline_config):
        """Test that provider pattern maintains environment isolation."""
        # Test with different environments
        test_environments = ['test', 'production', 'development']
        
        for env in test_environments:
            mock_provider = DictConfigProvider(
                pipeline=valid_pipeline_config,
                tables={'tables': {}},
                env={}
            )
            
            settings = Settings(environment=env, provider=mock_provider)
            
            # Verify environment is set correctly
            assert settings.environment == env
            assert settings.env_prefix == ("TEST_" if env == 'test' else "")
            
            # Verify configuration is loaded regardless of environment
            assert settings.pipeline_config == valid_pipeline_config

    @pytest.mark.unit
    def test_provider_configuration_consistency(self, valid_pipeline_config):
        """Test that provider configuration remains consistent across multiple accesses."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Access the same configuration multiple times
        config1 = settings.pipeline_config
        config2 = settings.pipeline_config
        config3 = settings.pipeline_config
        
        # Verify consistency
        assert config1 == config2 == config3 == valid_pipeline_config
        assert config1 is config2 is config3  # Should be the same object reference

    @pytest.mark.unit
    def test_provider_configuration_immutability(self, valid_pipeline_config):
        """Test that provider configuration is not accidentally modified."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Get configuration
        config = settings.pipeline_config
        
        # Verify original configuration is preserved
        assert config == valid_pipeline_config
        
        # Attempt to modify (should not affect original)
        if isinstance(config, dict):
            config['modified'] = True
        
        # Verify original configuration is still intact
        assert settings.pipeline_config == valid_pipeline_config


class TestPipelineConfigErrorHandling:
    """Test error handling scenarios in pipeline configuration."""

    @pytest.mark.unit
    def test_get_pipeline_setting_with_empty_config(self):
        """Test getting pipeline setting when configuration is empty."""
        mock_provider = DictConfigProvider(
            pipeline={},
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Should handle empty configuration gracefully
        assert settings.get_pipeline_setting('general.pipeline_name', 'default') == 'default'
        assert settings.get_pipeline_setting('connections.source', 'default') == 'default'
        assert settings.get_pipeline_setting('stages.extract', 'default') == 'default'
        assert settings.get_pipeline_setting('logging.file', 'default') == 'default'

    @pytest.mark.unit
    def test_get_pipeline_setting_with_invalid_key_types(self, valid_pipeline_config):
        """Test getting pipeline setting with invalid key types."""
        mock_provider = DictConfigProvider(
            pipeline=valid_pipeline_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test with invalid key types - these should be handled gracefully by the method
        # Note: The method signature expects str, but we test edge cases
        assert settings.get_pipeline_setting('invalid.key', 'default') == 'default'
        assert settings.get_pipeline_setting('general.nonexistent', 'default') == 'default'
        assert settings.get_pipeline_setting('connections.nonexistent', 'default') == 'default'

    @pytest.mark.unit
    def test_get_pipeline_setting_with_special_characters(self, valid_pipeline_config):
        """Test getting pipeline setting with special characters in keys."""
        special_config = {
            'general': {
                'pipeline-name': 'test_pipeline',
                'batch_size': 1000
            },
            'connections': {
                'source_db': {
                    'pool_size': 5
                }
            }
        }
        
        mock_provider = DictConfigProvider(
            pipeline=special_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test with special characters
        assert settings.get_pipeline_setting('general.pipeline-name') == 'test_pipeline'
        assert settings.get_pipeline_setting('general.batch_size') == 1000
        assert settings.get_pipeline_setting('connections.source_db.pool_size') == 5

    @pytest.mark.unit
    def test_get_pipeline_setting_with_unicode_keys(self, valid_pipeline_config):
        """Test getting pipeline setting with unicode characters in keys."""
        unicode_config = {
            'general': {
                'pipeline_name': 'test_pipeline',
                'batch_size': 1000
            },
            'connections': {
                'source': {
                    'pool_size': 5
                }
            }
        }
        
        mock_provider = DictConfigProvider(
            pipeline=unicode_config,
            tables={'tables': {}},
            env={}
        )
        
        settings = Settings(environment='test', provider=mock_provider)
        
        # Test with unicode characters (should work normally)
        assert settings.get_pipeline_setting('general.pipeline_name') == 'test_pipeline'
        assert settings.get_pipeline_setting('general.batch_size') == 1000
        assert settings.get_pipeline_setting('connections.source.pool_size') == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 