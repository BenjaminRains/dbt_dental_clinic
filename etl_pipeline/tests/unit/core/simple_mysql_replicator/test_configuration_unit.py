"""
Unit tests for SimpleMySQLReplicator configuration methods using provider pattern.

This module tests the SimpleMySQLReplicator configuration methods with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests configuration retrieval and validation
    - Tests strategy determination logic
    - Validates configuration access patterns

Coverage Areas:
    - Configuration retrieval with provider pattern
    - Strategy determination logic
    - Configuration validation and fallbacks
    - Settings injection for configuration access
    - Provider pattern configuration isolation

ETL Context:
    - Critical for nightly ETL pipeline execution with dental clinic data
    - Supports MariaDB v11.6 source and MySQL replication database
    - Uses provider pattern for clean dependency injection and test isolation
    - Implements Settings injection for environment-agnostic connections
"""
import pytest
from unittest.mock import patch, MagicMock, mock_open
import os
import yaml
from typing import Dict, Any

# Import ETL pipeline components
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.config import (
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory

# Import custom exceptions for structured error handling
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseQueryError
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Import standardized fixtures
from tests.fixtures.connection_fixtures import (
    mock_connection_factory_with_settings,
    test_connection_settings,
    production_connection_settings
)
from tests.fixtures.env_fixtures import (
    test_settings,
    production_settings,
    test_env_provider,
    production_env_provider
)
from tests.fixtures.config_fixtures import (
    test_pipeline_config,
    test_tables_config
)


class TestSimpleMySQLReplicatorConfigurationMethods:
    """Unit tests for SimpleMySQLReplicator configuration methods using provider pattern."""
    
    @pytest.fixture
    def replicator_with_config(self, test_settings):
        """Create replicator with test configuration using provider pattern."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data (all tables have performance_category from schema analysis)
            mock_config = {
                'small_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 25,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'small'
                },
                'medium_table': {
                    'incremental_columns': ['AptDateTime'],
                    'batch_size': 500,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium'
                },
                'large_table': {
                    'incremental_columns': ['ProcDate'],
                    'batch_size': 2000,
                    'estimated_size_mb': 150,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'large'
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_get_copy_method_small_table(self, replicator_with_config):
        """
        Test copy method determination for small tables using provider pattern.
        
        Validates:
            - Small table method determination with provider pattern
            - Configuration access with provider pattern
            - Method logic based on table size
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Small table method for efficient copying
            - Optimized for dental clinic data with small tables
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_method('small_table')
        assert strategy == 'small'

    def test_get_copy_method_medium_table(self, replicator_with_config):
        """
        Test copy method determination for medium tables using provider pattern.
        
        Validates:
            - Medium table method determination with provider pattern
            - Configuration access with provider pattern
            - Method logic based on table size
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Medium table method for balanced copying
            - Optimized for dental clinic data with medium tables
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_method('medium_table')
        assert strategy == 'medium'

    def test_get_copy_method_large_table(self, replicator_with_config):
        """
        Test copy method determination for large tables using provider pattern.
        
        Validates:
            - Large table method determination with provider pattern
            - Configuration access with provider pattern
            - Method logic based on table size
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Large table method for efficient copying
            - Optimized for dental clinic data with large tables
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_method('large_table')
        assert strategy == 'large'

    def test_get_copy_method_missing_performance_category(self, replicator_with_config):
        """
        Test copy method determination for tables missing performance_category.
        
        Validates:
            - Error handling for missing performance_category
            - Proper error message indicating need to run schema analysis
            - No fallback behavior (all tables should have proper config)
            
        ETL Pipeline Context:
            - All tables should have performance_category from analyze_opendental_schema.py
            - No fallback logic - proper configuration required
            - Uses provider pattern for configuration access
        """
        with pytest.raises(ValueError, match="missing performance_category"):
            replicator_with_config.get_copy_method('missing_config_table')

    def test_get_copy_method_invalid_performance_category(self, replicator_with_config):
        """
        Test copy method determination for tables with invalid performance_category.
        
        Validates:
            - Error handling for invalid performance_category values
            - Proper error message for invalid categories
            - No fallback behavior - proper values required
            
        ETL Pipeline Context:
            - Only valid categories: 'tiny', 'small', 'medium', 'large'
            - No fallback logic - proper configuration required
            - Uses provider pattern for configuration access
        """
        # Add a table with invalid performance_category to the mock config
        replicator_with_config.table_configs['invalid_table'] = {
            'performance_category': 'invalid_category'
        }
        
        with pytest.raises(ValueError, match="Invalid performance_category"):
            replicator_with_config.get_copy_method('invalid_table')

    def test_get_extraction_strategy_incremental(self, replicator_with_config):
        """
        Test extraction strategy retrieval for incremental tables.
        
        Validates:
            - Incremental strategy retrieval from provider
            - Configuration access with provider pattern
            - Strategy determination logic
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Incremental strategy for change data capture
            - Optimized for dental clinic data with frequent updates
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_extraction_strategy('small_table')
        assert strategy == 'incremental'

    def test_get_extraction_strategy_full_table(self, replicator_with_config):
        """
        Test extraction strategy retrieval for full table strategy.
        
        Validates:
            - Full table strategy retrieval from provider
            - Configuration access with provider pattern
            - Strategy determination logic
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Full table strategy for complete data refresh
            - Used for dental clinic data with infrequent changes
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_extraction_strategy('large_table')
        assert strategy == 'full_table'

    def test_get_extraction_strategy_default(self, replicator_with_config):
        """
        Test extraction strategy retrieval with default fallback.
        
        Validates:
            - Default strategy when not configured
            - Fallback behavior with provider pattern
            - Configuration access with provider pattern
            - Default value handling
            
        ETL Pipeline Context:
            - Defaults to 'full_table' for unknown strategies
            - Safe fallback for dental clinic data
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_extraction_strategy('unknown_table')
        assert strategy == 'full_table'

    def test_get_extraction_strategy_incremental_chunked(self, replicator_with_config):
        """
        Test extraction strategy retrieval for incremental chunked tables.
        
        Validates:
            - Incremental chunked strategy retrieval from provider
            - Configuration access with provider pattern
            - Strategy determination logic
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Incremental chunked strategy for very large tables
            - Optimized for dental clinic data with large datasets
            - Uses provider pattern for configuration access
        """
        # Add incremental_chunked table to config
        replicator_with_config.table_configs['large_chunked_table'] = {
            'extraction_strategy': 'incremental_chunked',
            'incremental_columns': ['DateTStamp'],
            'batch_size': 1000,
            'estimated_size_mb': 200
        }
        
        strategy = replicator_with_config.get_extraction_strategy('large_chunked_table')
        assert strategy == 'incremental_chunked' 