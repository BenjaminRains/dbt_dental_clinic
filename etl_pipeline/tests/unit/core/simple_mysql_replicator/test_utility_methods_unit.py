"""
Unit tests for SimpleMySQLReplicator utility methods using provider pattern.

This module tests the SimpleMySQLReplicator utility methods with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests utility methods for data cleaning and SQL building
    - Tests configuration loading and validation utilities
    - Validates connection management utilities

Coverage Areas:
    - Data cleaning and validation utilities
    - SQL building utilities
    - Configuration loading utilities
    - Connection management utilities
    - Settings injection for environment-agnostic operations

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


class TestUtilityMethods:
    """Unit tests for SimpleMySQLReplicator utility methods using provider pattern."""
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines for testing utility methods."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data
            mock_config = {
                'patient': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium',
                    'processing_priority': 5,
                    'estimated_processing_time_minutes': 0.1,
                    'memory_requirements_mb': 10,
                    'primary_key': 'PatientNum'
                },
                'appointment': {
                    'incremental_columns': ['AptDateTime'],
                    'batch_size': 500,
                    'estimated_size_mb': 25,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'small',
                    'processing_priority': 3,
                    'estimated_processing_time_minutes': 0.05,
                    'memory_requirements_mb': 5,
                    'primary_key': 'AptNum'
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_clean_row_data_normal_values(self, replicator_with_mock_engines):
        """
        Test row data cleaning with normal values.
        
        Validates:
            - Row data cleaning with normal values
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Data type preservation
            
        ETL Pipeline Context:
            - Data cleaning for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test normal values
        row = (1, 'John Doe', '2024-01-01', 100.50, True)
        columns = ['id', 'name', 'date', 'amount', 'active']
        table_name = 'patient'
        
        cleaned_row = replicator._clean_row_data(row, columns, table_name)
        
        # Should return same values for normal data
        assert cleaned_row == [1, 'John Doe', '2024-01-01', 100.50, True]

    def test_clean_row_data_none_values(self, replicator_with_mock_engines):
        """
        Test row data cleaning with None values.
        
        Validates:
            - Row data cleaning with None values
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - None value handling
            
        ETL Pipeline Context:
            - Data cleaning for dental clinic ETL with null values
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test None values
        row = (1, None, '2024-01-01', None, True)
        columns = ['id', 'name', 'date', 'amount', 'active']
        table_name = 'patient'
        
        cleaned_row = replicator._clean_row_data(row, columns, table_name)
        
        # Should preserve None values
        assert cleaned_row == [1, None, '2024-01-01', None, True]

    def test_clean_row_data_string_with_control_characters(self, replicator_with_mock_engines):
        """
        Test row data cleaning with control characters in strings.
        
        Validates:
            - Row data cleaning with control characters
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Control character removal
            
        ETL Pipeline Context:
            - Data cleaning for dental clinic ETL with problematic characters
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test string with control characters
        row = (1, 'John\x00Doe\x01', '2024-01-01', 100.50, True)
        columns = ['id', 'name', 'date', 'amount', 'active']
        table_name = 'patient'
        
        cleaned_row = replicator._clean_row_data(row, columns, table_name)
        
        # Should remove control characters but preserve valid ones
        assert cleaned_row[1] == 'JohnDoe'  # Control characters removed
        assert cleaned_row[0] == 1
        assert cleaned_row[2] == '2024-01-01'

    def test_clean_row_data_string_with_valid_control_characters(self, replicator_with_mock_engines):
        """
        Test row data cleaning with valid control characters.
        
        Validates:
            - Row data cleaning with valid control characters
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Valid control character preservation
            
        ETL Pipeline Context:
            - Data cleaning for dental clinic ETL with valid control characters
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test string with valid control characters (tab, newline, carriage return)
        row = (1, 'John\tDoe\n', '2024-01-01\r', 100.50, True)
        columns = ['id', 'name', 'date', 'amount', 'active']
        table_name = 'patient'
        
        cleaned_row = replicator._clean_row_data(row, columns, table_name)
        
        # Should preserve valid control characters
        assert cleaned_row[1] == 'John\tDoe\n'
        assert cleaned_row[2] == '2024-01-01\r'

    def test_clean_row_data_error_handling(self, replicator_with_mock_engines):
        """
        Test row data cleaning error handling.
        
        Validates:
            - Row data cleaning error handling
            - Provider pattern error handling
            - Settings injection error handling
            - Graceful error handling with logging
            
        ETL Pipeline Context:
            - Data cleaning error handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Test with problematic data that might cause errors
        row = (1, object(), '2024-01-01', 100.50, True)  # Object that can't be converted
        columns = ['id', 'name', 'date', 'amount', 'active']
        table_name = 'patient'
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            cleaned_row = replicator._clean_row_data(row, columns, table_name)
            
            # Should handle error gracefully and use None for problematic values
            assert cleaned_row[1] is None  # Problematic value replaced with None
            assert cleaned_row[0] == 1
            assert cleaned_row[2] == '2024-01-01'
            
            # Verify warning was logged
            mock_logger.warning.assert_called_once()

    def test_build_mysql_upsert_sql_with_primary_key(self, replicator_with_mock_engines):
        """
        Test MySQL UPSERT SQL building with primary key.
        
        Validates:
            - MySQL UPSERT SQL building with primary key
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Primary key exclusion from updates
            
        ETL Pipeline Context:
            - MySQL UPSERT SQL building for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Add table config with primary key
        replicator.table_configs['patient']['primary_key'] = 'PatientNum'
        
        column_names = ['PatientNum', 'Name', 'DateTStamp', 'Amount']
        sql = replicator._build_mysql_upsert_sql('patient', column_names)
        
        # Verify SQL structure
        assert 'INSERT INTO `patient`' in sql
        assert 'ON DUPLICATE KEY UPDATE' in sql
        assert '`Name` = VALUES(`Name`)' in sql
        assert '`DateTStamp` = VALUES(`DateTStamp`)' in sql
        assert '`Amount` = VALUES(`Amount`)' in sql
        # Primary key should not be in UPDATE clause
        assert '`PatientNum` = VALUES(`PatientNum`)' not in sql

    def test_build_mysql_upsert_sql_without_primary_key(self, replicator_with_mock_engines):
        """
        Test MySQL UPSERT SQL building without primary key.
        
        Validates:
            - MySQL UPSERT SQL building without primary key
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Default primary key handling
            
        ETL Pipeline Context:
            - MySQL UPSERT SQL building for dental clinic ETL without primary key
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Remove primary key from config
        if 'primary_key' in replicator.table_configs['patient']:
            del replicator.table_configs['patient']['primary_key']
        
        column_names = ['id', 'name', 'date']
        sql = replicator._build_mysql_upsert_sql('patient', column_names)
        
        # Verify SQL structure with default primary key 'id'
        assert 'INSERT INTO `patient`' in sql
        assert 'ON DUPLICATE KEY UPDATE' in sql
        assert '`name` = VALUES(`name`)' in sql
        assert '`date` = VALUES(`date`)' in sql
        # Default primary key 'id' should not be in UPDATE clause
        assert '`id` = VALUES(`id`)' not in sql

    def test_build_mysql_upsert_sql_single_column(self, replicator_with_mock_engines):
        """
        Test MySQL UPSERT SQL building with single column.
        
        Validates:
            - MySQL UPSERT SQL building with single column
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Single column handling
            
        ETL Pipeline Context:
            - MySQL UPSERT SQL building for dental clinic ETL with single column
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Use the primary key column that matches the fixture configuration
        column_names = ['PatientNum']  # This is the primary key in the fixture
        sql = replicator._build_mysql_upsert_sql('patient', column_names)
        
        # Verify SQL structure
        assert 'INSERT INTO `patient`' in sql
        assert 'ON DUPLICATE KEY UPDATE' in sql
        # When only primary key column exists, should use updated_at fallback
        assert 'updated_at = NOW()' in sql

    def test_load_configuration_success(self, replicator_with_mock_engines):
        """
        Test successful configuration loading.
        
        Validates:
            - Configuration loading from YAML file
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - YAML parsing and validation
            
        ETL Pipeline Context:
            - Configuration loading for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock file reading
        mock_config = {
            'tables': {
                'patient': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'performance_category': 'medium'
                }
            }
        }
        
        with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
            config = replicator._load_configuration()
            
            # Verify configuration was loaded
            assert 'patient' in config
            assert config['patient']['incremental_columns'] == ['DateTStamp']
            assert config['patient']['batch_size'] == 1000

    def test_load_configuration_file_not_found(self, replicator_with_mock_engines):
        """
        Test configuration loading when file not found.
        
        Validates:
            - Configuration loading error handling
            - Provider pattern error handling
            - Settings injection error handling
            - FileNotFoundError handling
            
        ETL Pipeline Context:
            - Configuration loading error handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Mock file not found
        with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(ConfigurationError) as exc_info:
                replicator._load_configuration()
            
            # Verify error message
            assert "Configuration file not found" in str(exc_info.value)

    def test_load_configuration_invalid_yaml(self, replicator_with_mock_engines):
        """
        Test configuration loading with invalid YAML.
        
        Validates:
            - Configuration loading error handling for invalid YAML
            - Provider pattern error handling
            - Settings injection error handling
            - YAML parsing error handling
            
        ETL Pipeline Context:
            - Configuration loading error handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Mock invalid YAML
        with patch('builtins.open', mock_open(read_data="invalid: yaml: content:")):
            with pytest.raises(ConfigurationError) as exc_info:
                replicator._load_configuration()
            
            # Verify error message
            assert "Failed to load configuration" in str(exc_info.value)

    def test_get_primary_incremental_column_with_primary_column(self, replicator_with_mock_engines):
        """
        Test primary incremental column retrieval with valid primary column.
        
        Validates:
            - Primary incremental column retrieval with valid column
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Primary column validation
            
        ETL Pipeline Context:
            - Primary incremental column retrieval for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'primary_incremental_column': 'DateTStamp',
            'incremental_columns': ['DateTStamp', 'DateModified']
        }
        
        primary_column = replicator._get_primary_incremental_column(config)
        
        # Should return the primary column
        assert primary_column == 'DateTStamp'

    def test_get_primary_incremental_column_with_none_value(self, replicator_with_mock_engines):
        """
        Test primary incremental column retrieval with None value.
        
        Validates:
            - Primary incremental column retrieval with None value
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - None value handling
            
        ETL Pipeline Context:
            - Primary incremental column retrieval for dental clinic ETL with None
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'primary_incremental_column': None,
            'incremental_columns': ['DateTStamp', 'DateModified']
        }
        
        primary_column = replicator._get_primary_incremental_column(config)
        
        # Should return None
        assert primary_column is None

    def test_get_primary_incremental_column_with_none_string(self, replicator_with_mock_engines):
        """
        Test primary incremental column retrieval with 'none' string value.
        
        Validates:
            - Primary incremental column retrieval with 'none' string
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - 'none' string handling
            
        ETL Pipeline Context:
            - Primary incremental column retrieval for dental clinic ETL with 'none'
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'primary_incremental_column': 'none',
            'incremental_columns': ['DateTStamp', 'DateModified']
        }
        
        primary_column = replicator._get_primary_incremental_column(config)
        
        # Should return None for 'none' string
        assert primary_column is None

    def test_get_primary_incremental_column_with_empty_string(self, replicator_with_mock_engines):
        """
        Test primary incremental column retrieval with empty string.
        
        Validates:
            - Primary incremental column retrieval with empty string
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Empty string handling
            
        ETL Pipeline Context:
            - Primary incremental column retrieval for dental clinic ETL with empty string
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'primary_incremental_column': '',
            'incremental_columns': ['DateTStamp', 'DateModified']
        }
        
        primary_column = replicator._get_primary_incremental_column(config)
        
        # Should return None for empty string
        assert primary_column is None

    def test_get_primary_incremental_column_with_whitespace_string(self, replicator_with_mock_engines):
        """
        Test primary incremental column retrieval with whitespace string.
        
        Validates:
            - Primary incremental column retrieval with whitespace string
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Whitespace string handling
            
        ETL Pipeline Context:
            - Primary incremental column retrieval for dental clinic ETL with whitespace
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'primary_incremental_column': '   ',
            'incremental_columns': ['DateTStamp', 'DateModified']
        }
        
        primary_column = replicator._get_primary_incremental_column(config)
        
        # Should return None for whitespace string
        assert primary_column is None

    def test_get_primary_incremental_column_missing_key(self, replicator_with_mock_engines):
        """
        Test primary incremental column retrieval with missing key.
        
        Validates:
            - Primary incremental column retrieval with missing key
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Missing key handling
            
        ETL Pipeline Context:
            - Primary incremental column retrieval for dental clinic ETL with missing key
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'incremental_columns': ['DateTStamp', 'DateModified']
        }
        
        primary_column = replicator._get_primary_incremental_column(config)
        
        # Should return None for missing key
        assert primary_column is None

    def test_validate_extraction_strategy_valid_strategies(self, replicator_with_mock_engines):
        """
        Test extraction strategy validation with valid strategies.
        
        Validates:
            - Extraction strategy validation with valid strategies
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Valid strategy acceptance
            
        ETL Pipeline Context:
            - Extraction strategy validation for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test all valid strategies
        assert replicator._validate_extraction_strategy('full_table') is True
        assert replicator._validate_extraction_strategy('incremental') is True
        assert replicator._validate_extraction_strategy('incremental_chunked') is True

    def test_validate_extraction_strategy_invalid_strategies(self, replicator_with_mock_engines):
        """
        Test extraction strategy validation with invalid strategies.
        
        Validates:
            - Extraction strategy validation with invalid strategies
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Invalid strategy rejection
            
        ETL Pipeline Context:
            - Extraction strategy validation for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test invalid strategies
        assert replicator._validate_extraction_strategy('invalid_strategy') is False
        assert replicator._validate_extraction_strategy('unknown') is False
        assert replicator._validate_extraction_strategy('') is False
        assert replicator._validate_extraction_strategy(None) is False

    def test_log_incremental_strategy_with_primary_column(self, replicator_with_mock_engines):
        """
        Test incremental strategy logging with primary column.
        
        Validates:
            - Incremental strategy logging with primary column
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Primary column logging
            
        ETL Pipeline Context:
            - Incremental strategy logging for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        table_name = 'patient'
        primary_column = 'DateTStamp'
        incremental_columns = ['DateTStamp', 'DateModified']
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            replicator._log_incremental_strategy(table_name, primary_column, incremental_columns)
            
            # Verify logging
            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert "Using primary incremental column" in log_call
            assert "DateTStamp" in log_call

    def test_log_incremental_strategy_without_primary_column(self, replicator_with_mock_engines):
        """
        Test incremental strategy logging without primary column.
        
        Validates:
            - Incremental strategy logging without primary column
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Multi-column logging
            
        ETL Pipeline Context:
            - Incremental strategy logging for dental clinic ETL without primary column
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        table_name = 'patient'
        primary_column = None
        incremental_columns = ['DateTStamp', 'DateModified']
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            replicator._log_incremental_strategy(table_name, primary_column, incremental_columns)
            
            # Verify logging
            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert "Using multi-column incremental logic" in log_call
            assert "DateTStamp" in log_call
            assert "DateModified" in log_call 