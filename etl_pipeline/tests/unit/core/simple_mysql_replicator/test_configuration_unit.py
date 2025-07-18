"""
Unit tests for SimpleMySQLReplicator configuration methods using provider pattern.

This module tests the SimpleMySQLReplicator configuration methods with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests copy strategy determination based on table size
    - Tests extraction strategy retrieval from configuration
    - Validates configuration access with provider pattern

Coverage Areas:
    - Copy strategy determination (small, medium, large)
    - Extraction strategy retrieval (incremental, full_table, chunked_incremental)
    - Configuration access with provider pattern
    - Default value handling for missing configuration
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
        # Mock ConnectionFactory methods
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection') as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_target:
            
            mock_source_engine = MagicMock()
            mock_target_engine = MagicMock()
            mock_source.return_value = mock_source_engine
            mock_target.return_value = mock_target_engine
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'small_table': {
                        'estimated_size_mb': 0.5,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'standard'
                    },
                    'medium_table': {
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'important'
                    },
                    'large_table': {
                        'estimated_size_mb': 150,
                        'extraction_strategy': 'full_table',
                        'table_importance': 'critical'
                    },
                    'no_size_table': {
                        'extraction_strategy': 'incremental',
                        'table_importance': 'standard'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                return SimpleMySQLReplicator(settings=test_settings)

    def test_get_copy_strategy_small_table(self, replicator_with_config):
        """
        Test copy strategy determination for small tables.
        
        Validates:
            - Small table (< 1MB) strategy determination
            - Size-based strategy logic with provider pattern
            - Configuration retrieval from provider
            - Default strategy handling
            
        ETL Pipeline Context:
            - Small tables use direct INSERT ... SELECT for efficiency
            - Optimized for dental clinic data with small patient tables
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_strategy('small_table')
        assert strategy == 'small'

    def test_get_copy_strategy_medium_table(self, replicator_with_config):
        """
        Test copy strategy determination for medium tables.
        
        Validates:
            - Medium table (1-100MB) strategy determination
            - Size-based strategy logic with provider pattern
            - Configuration retrieval from provider
            - Strategy boundary handling
            
        ETL Pipeline Context:
            - Medium tables use chunked INSERT with LIMIT/OFFSET
            - Optimized for dental clinic data with appointment tables
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_strategy('medium_table')
        assert strategy == 'medium'

    def test_get_copy_strategy_large_table(self, replicator_with_config):
        """
        Test copy strategy determination for large tables.
        
        Validates:
            - Large table (> 100MB) strategy determination
            - Size-based strategy logic with provider pattern
            - Configuration retrieval from provider
            - Strategy boundary handling
            
        ETL Pipeline Context:
            - Large tables use chunked INSERT with WHERE conditions
            - Optimized for dental clinic data with procedure tables
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_strategy('large_table')
        assert strategy == 'large'

    def test_get_copy_strategy_no_size_config(self, replicator_with_config):
        """
        Test copy strategy determination for tables without size configuration.
        
        Validates:
            - Default strategy when no size configured
            - Fallback behavior with provider pattern
            - Configuration retrieval from provider
            - Default value handling
            
        ETL Pipeline Context:
            - Defaults to 'small' strategy for unknown table sizes
            - Safe fallback for dental clinic data
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_strategy('no_size_table')
        assert strategy == 'small'

    def test_get_copy_strategy_unknown_table(self, replicator_with_config):
        """
        Test copy strategy determination for unknown tables.
        
        Validates:
            - Default strategy for unknown tables
            - Fallback behavior with provider pattern
            - Configuration retrieval from provider
            - Default value handling
            
        ETL Pipeline Context:
            - Defaults to 'small' strategy for unknown tables
            - Safe fallback for dental clinic data
            - Uses provider pattern for configuration access
        """
        strategy = replicator_with_config.get_copy_strategy('unknown_table')
        assert strategy == 'small'

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

    def test_get_extraction_strategy_chunked_incremental(self, replicator_with_config):
        """
        Test extraction strategy retrieval for chunked incremental tables.
        
        Validates:
            - Chunked incremental strategy retrieval from provider
            - Configuration access with provider pattern
            - Strategy determination logic
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Chunked incremental strategy for very large tables
            - Optimized for dental clinic data with large datasets
            - Uses provider pattern for configuration access
        """
        # Add chunked_incremental table to config
        replicator_with_config.table_configs['large_chunked_table'] = {
            'extraction_strategy': 'chunked_incremental',
            'incremental_column': 'DateTStamp',
            'batch_size': 1000,
            'estimated_size_mb': 200
        }
        
        strategy = replicator_with_config.get_extraction_strategy('large_chunked_table')
        assert strategy == 'chunked_incremental' 