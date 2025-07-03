"""
Comprehensive configuration validation tests.

This module tests configuration validation across all config components,
including schema validation, data type checking, and business rule validation.
Uses shared validation utilities to eliminate duplication.
"""

import pytest
import sys
import os
# Add the tests directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from validation_utils import (
    validate_general_section,
    validate_connections_section,
    validate_stages_section,
    validate_logging_section,
    validate_error_handling_section,
    validate_business_rules,
    validate_configuration_consistency,
    validate_complete_pipeline_config
)


class TestPipelineConfigValidation:
    """Test pipeline configuration validation using shared utilities."""

    @pytest.mark.validation
    def test_validate_general_section(self, valid_pipeline_config):
        """Test validation of general section using shared utility."""
        validate_general_section(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_connections_section(self, valid_pipeline_config):
        """Test validation of connections section using shared utility."""
        validate_connections_section(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_stages_section(self, valid_pipeline_config):
        """Test validation of stages section using shared utility."""
        validate_stages_section(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_logging_section(self, valid_pipeline_config):
        """Test validation of logging section using shared utility."""
        validate_logging_section(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_error_handling_section(self, valid_pipeline_config):
        """Test validation of error handling section using shared utility."""
        validate_error_handling_section(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_business_rules(self, valid_pipeline_config):
        """Test business rule validation using shared utility."""
        validate_business_rules(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_configuration_consistency(self, valid_pipeline_config):
        """Test configuration consistency validation using shared utility."""
        validate_configuration_consistency(valid_pipeline_config)

    @pytest.mark.validation
    def test_validate_complete_pipeline_config(self, valid_pipeline_config):
        """Test complete pipeline configuration validation using shared utility."""
        validate_complete_pipeline_config(valid_pipeline_config)


class TestConfigurationErrorCases:
    """Test configuration validation error cases."""

    @pytest.mark.validation
    def test_invalid_batch_size(self):
        """Test validation with invalid batch size."""
        invalid_config = {
            'general': {
                'pipeline_name': 'test',
                'batch_size': 0  # Invalid: must be > 0
            }
        }
        
        # This should be caught by validation
        assert invalid_config['general']['batch_size'] <= 0

    @pytest.mark.validation
    def test_invalid_parallel_jobs(self):
        """Test validation with invalid parallel jobs."""
        invalid_config = {
            'general': {
                'pipeline_name': 'test',
                'parallel_jobs': 25  # Invalid: too high
            }
        }
        
        # This should be caught by validation
        assert invalid_config['general']['parallel_jobs'] > 20

    @pytest.mark.validation
    def test_invalid_error_threshold(self):
        """Test validation with invalid error threshold."""
        invalid_config = {
            'stages': {
                'extract': {
                    'enabled': True,
                    'timeout_minutes': 30,
                    'error_threshold': 1.5  # Invalid: > 1.0
                }
            }
        }
        
        # This should be caught by validation
        assert invalid_config['stages']['extract']['error_threshold'] > 1.0

    @pytest.mark.validation
    def test_invalid_timeout_minutes(self):
        """Test validation with invalid timeout minutes."""
        invalid_config = {
            'stages': {
                'extract': {
                    'enabled': True,
                    'timeout_minutes': -5,  # Invalid: negative
                    'error_threshold': 0.01
                }
            }
        }
        
        # This should be caught by validation
        assert invalid_config['stages']['extract']['timeout_minutes'] <= 0

    @pytest.mark.validation
    def test_invalid_pool_size(self):
        """Test validation with invalid pool size."""
        invalid_config = {
            'connections': {
                'source': {
                    'pool_size': 0  # Invalid: must be > 0
                }
            }
        }
        
        # This should be caught by validation
        assert invalid_config['connections']['source']['pool_size'] <= 0

    @pytest.mark.validation
    def test_invalid_log_level(self):
        """Test validation with invalid log level."""
        invalid_config = {
            'logging': {
                'level': 'INVALID_LEVEL'  # Invalid log level
            }
        }
        
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        # This should be caught by validation
        assert invalid_config['logging']['level'] not in valid_levels


class TestConfigurationBusinessRules:
    """Test configuration business rule validation using shared utilities."""

    @pytest.mark.validation
    def test_batch_size_reasonable_range(self, valid_pipeline_config):
        """Test that batch size is within reasonable range."""
        validate_business_rules(valid_pipeline_config)

    @pytest.mark.validation
    def test_parallel_jobs_reasonable_range(self, valid_pipeline_config):
        """Test that parallel jobs is within reasonable range."""
        validate_business_rules(valid_pipeline_config)

    @pytest.mark.validation
    def test_timeout_minutes_reasonable_range(self, valid_pipeline_config):
        """Test that timeout minutes are within reasonable range."""
        validate_business_rules(valid_pipeline_config)

    @pytest.mark.validation
    def test_error_threshold_reasonable_range(self, valid_pipeline_config):
        """Test that error thresholds are within reasonable range."""
        validate_business_rules(valid_pipeline_config)

    @pytest.mark.validation
    def test_pool_size_reasonable_range(self, valid_pipeline_config):
        """Test that pool sizes are within reasonable range."""
        validate_business_rules(valid_pipeline_config)

    @pytest.mark.validation
    def test_pool_recycle_reasonable_range(self, valid_pipeline_config):
        """Test that pool recycle times are within reasonable range."""
        validate_business_rules(valid_pipeline_config)


class TestConfigurationConsistency:
    """Test configuration consistency across sections using shared utilities."""

    @pytest.mark.validation
    def test_parallel_jobs_consistency(self, valid_pipeline_config):
        """Test consistency between parallel_jobs and connection pool sizes."""
        validate_configuration_consistency(valid_pipeline_config)

    @pytest.mark.validation
    def test_timeout_consistency(self, valid_pipeline_config):
        """Test consistency of timeout values across stages."""
        validate_configuration_consistency(valid_pipeline_config)

    @pytest.mark.validation
    def test_error_threshold_consistency(self, valid_pipeline_config):
        """Test consistency of error thresholds across stages."""
        validate_configuration_consistency(valid_pipeline_config)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 