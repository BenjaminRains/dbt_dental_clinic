"""
Unit tests for OpenDentalSchemaAnalyzer initialization using provider pattern.

This module tests the OpenDentalSchemaAnalyzer initialization with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests initialization with different Settings configurations
    - Tests environment validation and FAIL FAST behavior
    - Validates database connection setup with provider pattern

Coverage Areas:
    - Settings injection for environment-agnostic initialization
    - Provider pattern dependency injection
    - Environment validation and FAIL FAST behavior
    - Database connection setup with provider pattern
    - Error handling during initialization

ETL Context:
    - Critical for ETL pipeline configuration generation
    - Tests with mocked dental clinic database schemas
    - Uses Settings injection with DictConfigProvider for unit testing
    - Generates test tables.yml for ETL pipeline configuration
"""
import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings


class TestOpenDentalSchemaAnalyzerInitialization:
    """Unit tests for OpenDentalSchemaAnalyzer initialization using provider pattern."""
    
    def test_analyzer_initialization_with_provider(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test analyzer initialization with provider pattern.
        
        AAA Pattern:
            Arrange: Set up mock settings with DictConfigProvider
            Act: Create OpenDentalSchemaAnalyzer instance
            Assert: Verify analyzer is properly initialized
            
        Validates:
            - Provider pattern dependency injection works correctly
            - Settings injection with DictConfigProvider for unit testing
            - Environment validation with injected configuration
            - Configuration validation with mocked provider
            - Provider pattern works with injected test configuration
            - Settings injection works for environment-agnostic connections
        """
        # Arrange: Set up mock settings with DictConfigProvider
        settings = mock_settings_with_dict_provider
        
        # Act: Create OpenDentalSchemaAnalyzer instance
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Assert: Verify analyzer is properly initialized
            assert analyzer.source_engine is not None
            assert analyzer.source_db == 'test_opendental'
            assert analyzer.dbt_project_root is not None
            assert os.path.exists(analyzer.dbt_project_root)

    def test_fail_fast_on_missing_environment(self):
        """
        Test that system fails fast when source database config is not available in Settings.
        
        AAA Pattern:
            Arrange: Mock Settings to return None for database configuration
            Act: Attempt to create OpenDentalSchemaAnalyzer with invalid settings
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior when source database not configured in Settings
            - Clear error message for missing database configuration
            - Settings injection validation
        """
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.get_settings') as mock_get_settings:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock Settings to return None for database (simulate missing config)
            mock_settings = Mock()
            mock_settings.get_source_connection_config.return_value = {'database': None}  # Missing database!
            mock_get_settings.return_value = mock_settings
            
            # Act & Assert: Verify system fails fast
            with pytest.raises(ValueError, match="Source database is not configured"):
                analyzer = OpenDentalSchemaAnalyzer() 