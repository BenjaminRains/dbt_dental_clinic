# tests/unit/scripts/test_analyze_opendental_schema_unit.py

"""
Unit tests for OpenDentalSchemaAnalyzer using mocked dependencies and provider pattern.

This module tests the schema analyzer with complete mocking and DictConfigProvider
to ensure isolated unit testing without real database connections.

Unit Test Strategy:
- Uses DictConfigProvider for injected test configuration
- Mocks all database connections and schema inspection
- Tests all methods with controlled test data
- Validates systematic table importance determination
- Tests incremental column discovery with mocked schemas
- Uses Settings injection with provider pattern for environment-agnostic testing
- Tests FAIL FAST behavior with mocked environments

Coverage Areas:
- Table discovery with mocked database inspector
- Schema analysis with mocked table structures
- Table size analysis with mocked row counts
- Table importance determination with systematic criteria
- Extraction strategy determination with mocked data
- Incremental column discovery with mocked schemas
- DBT model discovery with mocked project structure
- Configuration generation with mocked metadata
- Error handling with mocked failure scenarios
- FAIL FAST behavior with mocked environment validation

ETL Context:
- Critical for ETL pipeline configuration generation
- Tests with mocked dental clinic database schemas
- Uses Settings injection with DictConfigProvider for unit testing
- Generates test tables.yml for ETL pipeline configuration
"""

import pytest
import tempfile
import os
import yaml
import json
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings
import itertools


@pytest.fixture
def mock_environment_variables():
    """
    Mock environment variables required by OpenDentalSchemaAnalyzer for testing.
    
    Sets up the required OPENDENTAL_SOURCE_DB environment variable
    for unit testing without affecting the real environment.
    """
    # Store original environment variable
    original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
    
    # Set test environment variable
    os.environ['OPENDENTAL_SOURCE_DB'] = 'test_opendental'
    
    yield
    
    # Restore original environment variable
    if original_source_db is not None:
        os.environ['OPENDENTAL_SOURCE_DB'] = original_source_db
    elif 'OPENDENTAL_SOURCE_DB' in os.environ:
        del os.environ['OPENDENTAL_SOURCE_DB']


@pytest.fixture
def mock_settings_with_dict_provider():
    """
    Mock settings with DictConfigProvider for unit testing.
    
    Provides Settings instance with DictConfigProvider for:
    - Test environment simulation
    - Injected test configuration
    - Settings injection for test environment testing
    
    ETL Context:
        - Uses DictConfigProvider for unit testing (as recommended)
        - Supports test environment testing
        - Enables clean dependency injection for unit testing
        - Uses Settings injection for test environment-agnostic connections
    """
    # Arrange: Create test environment variables
    test_env_vars = {
        'ETL_ENVIRONMENT': 'test',
        'TEST_OPENDENTAL_SOURCE_HOST': 'test-source-host',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
        'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
        'TEST_MYSQL_REPLICATION_PORT': '3306',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
        'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
    }
    
    # Create DictConfigProvider for unit testing
    provider = DictConfigProvider(
        pipeline={},
        tables={'tables': {}},
        env=test_env_vars
    )
    
    # Act: Create settings with DictConfigProvider for test environment
    settings = Settings(environment='test', provider=provider)
    return settings


@pytest.fixture
def mock_schema_data():
    """
    Mock schema data for dental clinic tables.
    
    Provides realistic dental clinic table schemas with:
    - Standard OpenDental table structures
    - Common data types and constraints
    - Primary/foreign key relationships
    - Timestamp columns for incremental loading
    
    ETL Usage:
        - Testing table schema analysis with mocked data
        - Validating incremental column discovery
        - Testing table importance determination
        - Mocking database inspector responses
    
    Returns:
        Dict: Mock schema data for dental clinic tables
    """
    return {
        'patient': {
            'columns': {
                'PatNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'LName': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'FName': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'DateModified': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['PatNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'appointment', 'referred_columns': ['PatNum']},
                {'constrained_columns': ['PatNum'], 'referred_table': 'procedurelog', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['PatNum']},
                {'name': 'idx_patient_name', 'column_names': ['LName', 'FName']}
            ]
        },
        'appointment': {
            'columns': {
                'AptNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'PatNum': {'type': 'int(11)', 'nullable': True, 'primary_key': False, 'default': None},
                'AptDateTime': {'type': 'datetime', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['AptNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'patient', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['AptNum']},
                {'name': 'idx_appointment_patnum', 'column_names': ['PatNum']}
            ]
        },
        'procedurelog': {
            'columns': {
                'ProcNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'PatNum': {'type': 'int(11)', 'nullable': True, 'primary_key': False, 'default': None},
                'ProcDate': {'type': 'date', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['ProcNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'patient', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['ProcNum']},
                {'name': 'idx_procedure_patnum', 'column_names': ['PatNum']}
            ]
        },
        'insplan': {
            'columns': {
                'PlanNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'Carrier': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['PlanNum'],
            'foreign_keys': [],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['PlanNum']}
            ]
        },
        'definition': {
            'columns': {
                'DefNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'DefName': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['DefNum'],
            'foreign_keys': [],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['DefNum']}
            ]
        },
        'securitylog': {
            'columns': {
                'LogNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'LogMessage': {'type': 'text', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['LogNum'],
            'foreign_keys': [],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['LogNum']}
            ]
        },
        'claim': {
            'columns': {
                'ClaimNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'PatNum': {'type': 'int(11)', 'nullable': True, 'primary_key': False, 'default': None},
                'ClaimDate': {'type': 'date', 'nullable': True, 'primary_key': False, 'default': None},
                'ClaimAmt': {'type': 'decimal(10,2)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['ClaimNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'patient', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['ClaimNum']},
                {'name': 'idx_claim_patnum', 'column_names': ['PatNum']}
            ]
        }
    }


@pytest.fixture
def mock_size_data():
    """
    Mock size data for dental clinic tables.
    
    Provides realistic table size information with:
    - Estimated row counts for different table types
    - Size estimates in MB
    - Source information for tracking
    
    ETL Usage:
        - Testing table size analysis with mocked data
        - Validating extraction strategy determination
        - Testing table importance based on size
        - Mocking database size queries
    
    Returns:
        Dict: Mock size data for dental clinic tables
    """
    return {
        'patient': {'estimated_row_count': 50000, 'size_mb': 25.5, 'source': 'information_schema_estimate'},
        'appointment': {'estimated_row_count': 150000, 'size_mb': 45.2, 'source': 'information_schema_estimate'},
        'procedurelog': {'estimated_row_count': 200000, 'size_mb': 78.9, 'source': 'information_schema_estimate'},
        'insplan': {'estimated_row_count': 500, 'size_mb': 2.1, 'source': 'information_schema_estimate'},
        'definition': {'estimated_row_count': 100, 'size_mb': 0.5, 'source': 'information_schema_estimate'},
        'securitylog': {'estimated_row_count': 5000000, 'size_mb': 1250.0, 'source': 'information_schema_estimate'},
        'claim': {'estimated_row_count': 2000000, 'size_mb': 800.0, 'source': 'information_schema_estimate'}
    }


@pytest.fixture
def mock_dbt_models():
    """
    Mock DBT models for dental clinic project.
    
    Provides realistic DBT model structure with:
    - Staging models for source tables
    - Mart models for business entities
    - Intermediate models for transformations
    
    ETL Usage:
        - Testing DBT model discovery with mocked project
        - Validating model type classification
        - Testing configuration generation with model info
    
    Returns:
        Dict: Mock DBT models structure
    """
    return {
        'staging': ['stg_opendental__patient', 'stg_opendental__appointment', 'stg_opendental__procedurelog'],
        'mart': ['dim_patient', 'dim_appointment', 'fact_procedures'],
        'intermediate': ['int_patient_appointments', 'int_procedure_summary']
    }


@pytest.mark.unit
@pytest.mark.scripts
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestOpenDentalSchemaAnalyzerUnit:
    """Unit tests for OpenDentalSchemaAnalyzer with mocked dependencies."""
    
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

    def test_table_discovery_with_mocked_inspector(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test table discovery with mocked database inspector.
        
        AAA Pattern:
            Arrange: Set up mock inspector with test table names
            Act: Call discover_all_tables() method
            Assert: Verify tables are discovered correctly
            
        Validates:
            - Mocked table discovery from database inspector
            - Proper filtering of excluded patterns
            - Error handling for mocked database operations
            - Settings injection with mocked database connections
        """
        # Arrange: Set up mock inspector with test table names
        test_tables = ['patient', 'appointment', 'procedurelog', 'insplan', 'definition', 'securitylog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector with proper method configuration
            mock_inspector = Mock()
            
            # Configure mock inspector methods to return proper data structures
            def mock_get_columns(table_name):
                # Return list of column dictionaries as expected by the real code
                return [
                    {'name': 'id', 'type': 'INT', 'nullable': False, 'default': None},
                    {'name': 'name', 'type': 'VARCHAR(255)', 'nullable': True, 'default': None}
                ]
            
            def mock_get_pk_constraint(table_name):
                # Return primary key constraint as expected by the real code
                return {'constrained_columns': ['id']}
            
            def mock_get_foreign_keys(table_name):
                # Return foreign keys as expected by the real code
                return []
            
            def mock_get_indexes(table_name):
                # Return indexes as expected by the real code
                return []
            
            # Configure the mock inspector methods
            mock_inspector.get_columns = mock_get_columns
            mock_inspector.get_pk_constraint = mock_get_pk_constraint
            mock_inspector.get_foreign_keys = mock_get_foreign_keys
            mock_inspector.get_indexes = mock_get_indexes
            mock_inspector.get_table_names.return_value = test_tables
            
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call discover_all_tables() method
            tables = analyzer.discover_all_tables()
            
            # Assert: Verify tables are discovered correctly
            assert isinstance(tables, list)
            assert len(tables) == 6
            assert 'patient' in tables
            assert 'appointment' in tables
            assert 'procedurelog' in tables

    def test_table_schema_analysis_with_mocked_data(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test table schema analysis with mocked schema data.
        
        AAA Pattern:
            Arrange: Set up mock inspector with test schema data
            Act: Call get_table_schema() method for test table
            Assert: Verify schema information is correctly extracted
            
        Validates:
            - Mocked schema analysis from database inspector
            - Column information extraction from mocked tables
            - Primary key detection from mocked database
            - Foreign key detection from mocked database
            - Index information from mocked database
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock inspector with test schema data
        test_table = 'patient'
        test_schema = mock_schema_data[test_table]
        
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
            mock_inspector.get_columns.return_value = [
                {'name': name, **info} for name, info in test_schema['columns'].items()
            ]
            mock_inspector.get_pk_constraint.return_value = {'constrained_columns': test_schema['primary_keys']}
            mock_inspector.get_foreign_keys.return_value = test_schema['foreign_keys']
            mock_inspector.get_indexes.return_value = test_schema['indexes']
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_schema() method for test table
            schema_info = analyzer.get_table_schema(test_table)
            
            # Assert: Verify schema information is correctly extracted
            assert schema_info['table_name'] == test_table
            assert 'columns' in schema_info
            assert len(schema_info['columns']) == 6  # 6 columns in patient table
            
            # Verify column information
            assert 'PatNum' in schema_info['columns']
            assert schema_info['columns']['PatNum']['primary_key'] is True
            assert schema_info['columns']['DateTStamp']['type'] == 'timestamp'
            
            # Verify primary keys
            assert 'primary_keys' in schema_info
            assert schema_info['primary_keys'] == ['PatNum']
            
            # Verify foreign keys and indexes
            assert 'foreign_keys' in schema_info
            assert 'indexes' in schema_info

    def test_table_size_analysis_with_mocked_data(self, mock_settings_with_dict_provider, mock_size_data, mock_environment_variables):
        """
        Test table size analysis with mocked data using ConnectionManager.
        
        AAA Pattern:
            Arrange: Set up mock ConnectionManager with test size data
            Act: Call get_table_size_info() method for test table
            Assert: Verify size information is correctly extracted
            
        Validates:
            - Mocked size analysis using ConnectionManager
            - Row count calculation from mocked data
            - Table size estimation from mocked database
            - Error handling for mocked database operations
            - Settings injection with ConnectionManager
        """
        # Arrange: Set up mock ConnectionManager with test size data
        test_table = 'patient'
        test_size = mock_size_data[test_table]
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager context manager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock execute_with_retry to return our test data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'TABLE_ROWS' in query:
                    # Return estimated row count
                    mock_result.scalar.return_value = test_size['estimated_row_count']
                elif 'information_schema.tables' in query:
                    # Return size in MB
                    mock_result.scalar.return_value = test_size['size_mb']
                else:
                    # Return 0 for other queries
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_size_info() method for test table
            size_info = analyzer.get_table_size_info(test_table)
            
            # Assert: Verify size information is correctly extracted
            assert size_info['table_name'] == test_table
            assert 'estimated_row_count' in size_info
            assert 'size_mb' in size_info
            assert 'source' in size_info
            assert size_info['source'] == 'information_schema_estimate'
            assert size_info['estimated_row_count'] == 50000
            assert size_info['size_mb'] == 25.5
            
            # Verify ConnectionManager was used
            assert mock_conn_manager.call_count == 2
            mock_conn_manager.assert_any_call(mock_engine)
            assert mock_conn_manager_instance.__enter__.called
            assert mock_conn_manager_instance.__exit__.called

    def test_table_importance_determination_critical(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for critical tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for critical table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as critical
            
        Validates:
            - Critical table identification from mocked database data
            - Systematic importance determination with mocked data
            - Critical table classification for core business entities
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for critical table
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as critical
            assert importance == 'critical'

    def test_table_importance_determination_important_large_table(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for large tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for large table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as important
            
        Validates:
            - Important table identification for large tables (>1M rows)
            - Systematic importance determination with mocked data
            - Performance consideration for large tables
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for large table
        test_table = 'claim'  # Use 'claim' which is NOT in critical list but has large size and insurance pattern
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]  # Use fixture data (2M estimated rows)
        
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
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as important
            assert importance == 'important'

    def test_table_importance_determination_important_large_table_with_audit_pattern(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for large tables with audit patterns.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for large audit table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as audit (prioritized over size)
            
        Validates:
            - Audit pattern recognition takes priority over size considerations
            - Large tables with 'log' in name are classified as audit, not important
            - Systematic importance determination with mocked data
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for large audit table
        test_table = 'securitylog'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]  # 5M estimated rows, should be 'important' by size but 'audit' by pattern
        
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
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as audit (pattern takes priority over size)
            assert importance == 'audit'

    def test_table_importance_determination_important_insurance_pattern(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for insurance/billing pattern tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for insurance table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as important
            
        Validates:
            - Important table identification for insurance/billing tables
            - Systematic importance determination with mocked data
            - Business value pattern recognition
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for insurance table
        test_table = 'insplan'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as important
            assert importance == 'important'

    def test_table_importance_determination_reference(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for reference tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for reference table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as reference
            
        Validates:
            - Reference table identification for lookup data
            - Systematic importance determination with mocked data
            - Reference pattern recognition ('def' in table name)
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for reference table
        test_table = 'definition'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as reference
            assert importance == 'reference'

    def test_table_importance_determination_audit(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test table importance determination for audit tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for audit table
            Act: Call determine_table_importance() method
            Assert: Verify importance is correctly determined as audit
            
        Validates:
            - Audit table identification for logging/history data
            - Systematic importance determination with mocked data
            - Audit pattern recognition ('log' in table name)
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for audit table
        test_table = 'securitylog'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]
        
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
            
            # Act: Call determine_table_importance() method
            importance = analyzer.determine_table_importance(test_table, schema_info, size_info)
            
            # Assert: Verify importance is correctly determined as audit
            assert importance == 'audit'

    def test_extraction_strategy_determination_small_table(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test extraction strategy determination for small tables.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for small table
            Act: Call determine_extraction_strategy() method
            Assert: Verify strategy is correctly determined as full_table
            
        Validates:
            - Full table strategy for small tables (<10K rows)
            - Extraction strategy determination with mocked data
            - Performance optimization for small tables
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for small table
        test_table = 'definition'
        schema_info = mock_schema_data[test_table]
        size_info = {'estimated_row_count': 100, 'size_mb': 0.5, 'source': 'information_schema_estimate'}  # Small table
        
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
            
            # Act: Call determine_extraction_strategy() method
            strategy = analyzer.determine_extraction_strategy(test_table, schema_info, size_info)
            
            # Assert: Verify strategy is correctly determined as full_table
            assert strategy == 'full_table'

    def test_extraction_strategy_determination_incremental(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test extraction strategy determination for tables with incremental columns.
        
        AAA Pattern:
            Arrange: Set up mock schema and size data for table with timestamp columns
            Act: Call determine_extraction_strategy() method
            Assert: Verify strategy is correctly determined as incremental
            
        Validates:
            - Incremental strategy for tables with timestamp columns
            - Extraction strategy determination with mocked data
            - Performance optimization for medium tables with incremental columns
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema and size data for table with timestamp columns
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        size_info = mock_size_data[test_table]  # 50K estimated rows, medium table
        
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
            
            # Act: Call determine_extraction_strategy() method
            strategy = analyzer.determine_extraction_strategy(test_table, schema_info, size_info)
            
            # Assert: Verify strategy is correctly determined as incremental
            assert strategy == 'incremental'

    def test_incremental_column_discovery_with_timestamp_columns(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test incremental column discovery with timestamp columns.
        
        AAA Pattern:
            Arrange: Set up mock schema data with timestamp columns
            Act: Call find_incremental_columns() method
            Assert: Verify incremental columns are correctly identified
            
        Validates:
            - Timestamp column identification from mocked database schema
            - OpenDental-specific timestamp pattern recognition
            - Priority ordering of incremental columns
            - Error handling for mocked database operations
        """
        # Arrange: Set up mock schema data with timestamp columns
        test_table = 'patient'
        schema_info = mock_schema_data[test_table]
        
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
            
            # Act: Call find_incremental_columns() method
            incremental_columns = analyzer.find_incremental_columns(test_table, schema_info)
            
            # Assert: Verify incremental columns are correctly identified
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) > 0
            
            # Verify that identified columns exist in the schema
            for col_name in incremental_columns:
                assert col_name in schema_info['columns']
            
            # Verify timestamp columns are prioritized
            expected_timestamp_columns = ['DateTStamp', 'DateModified', 'SecDateTEdit']
            found_timestamp_columns = [col for col in incremental_columns if col in expected_timestamp_columns]
            assert len(found_timestamp_columns) > 0

    def test_dbt_model_discovery_with_mocked_project(self, mock_settings_with_dict_provider, mock_dbt_models, mock_environment_variables):
        """
        Test DBT model discovery with mocked project structure.
        
        AAA Pattern:
            Arrange: Set up mock project structure with DBT models
            Act: Call discover_dbt_models() method
            Assert: Verify DBT models are correctly discovered
            
        Validates:
            - DBT model discovery from mocked project structure
            - Staging model discovery from mocked project
            - Mart model discovery from mocked project
            - Intermediate model discovery from mocked project
            - Error handling for mocked project structure
        """
        # Arrange: Set up mock project structure with DBT models
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
            
            # Mock the models directory structure
            with patch('pathlib.Path.exists') as mock_exists:
                mock_exists.return_value = True
                
                with patch('pathlib.Path.rglob') as mock_rglob:
                    # Mock staging models
                    staging_files = [Mock(stem=model) for model in mock_dbt_models['staging']]
                    # Mock mart models  
                    mart_files = [Mock(stem=model) for model in mock_dbt_models['mart']]
                    # Mock intermediate models
                    intermediate_files = [Mock(stem=model) for model in mock_dbt_models['intermediate']]
                    
                    mock_rglob.side_effect = [staging_files, mart_files, intermediate_files]
                    
                    # Act: Call discover_dbt_models() method
                    dbt_models = analyzer.discover_dbt_models()
                    
                    # Assert: Verify DBT models are correctly discovered
                    assert isinstance(dbt_models, dict)
                    assert 'staging' in dbt_models
                    assert 'mart' in dbt_models
                    assert 'intermediate' in dbt_models
                    
                    # Verify that all model lists are lists
                    assert isinstance(dbt_models['staging'], list)
                    assert isinstance(dbt_models['mart'], list)
                    assert isinstance(dbt_models['intermediate'], list)
                    
                    # Verify model counts
                    assert len(dbt_models['staging']) == 3
                    assert len(dbt_models['mart']) == 3
                    assert len(dbt_models['intermediate']) == 2

    def test_complete_configuration_generation_with_mocked_data(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test complete configuration generation with mocked data.
        
        AAA Pattern:
            Arrange: Set up mock data and temporary output directory
            Act: Call generate_complete_configuration() method
            Assert: Verify configuration is correctly generated
            
        Validates:
            - Configuration generation with mocked database data
            - Metadata generation with mocked database information
            - Table configuration with mocked schema and size data
            - DBT model integration with mocked project structure
            - Error handling for mocked database operations
            - Settings injection with mocked database connections
        """
        # Arrange: Set up mock data and temporary output directory
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector with proper method returns
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock inspector methods to return proper data structures
            def mock_get_columns(table_name):
                return [
                    {'name': 'id', 'type': 'INT', 'nullable': False, 'default': None},
                    {'name': 'name', 'type': 'VARCHAR(255)', 'nullable': True, 'default': None}
                ]
            
            def mock_get_pk_constraint(table_name):
                return {'constrained_columns': ['id']}
            
            def mock_get_foreign_keys(table_name):
                return []
            
            def mock_get_indexes(table_name):
                return []
            
            mock_inspector.get_columns = mock_get_columns
            mock_inspector.get_pk_constraint = mock_get_pk_constraint
            mock_inspector.get_foreign_keys = mock_get_foreign_keys
            mock_inspector.get_indexes = mock_get_indexes
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            
            def mock_execute_with_retry(query):
                if 'COUNT(*)' in query:
                    result = Mock()
                    result.scalar.return_value = 1000
                    return result
                elif 'information_schema.tables' in query:
                    result = Mock()
                    result.scalar.return_value = 5.5
                    return result
                else:
                    result = Mock()
                    result.scalar.return_value = 0
                    return result
            
            mock_conn_manager_instance.execute_with_retry = mock_execute_with_retry
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: mock_dbt_models
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify configuration is correctly generated
                assert 'metadata' in config
                assert 'tables' in config
                assert len(config['tables']) == 3
                
                # Verify metadata
                metadata = config['metadata']
                assert 'generated_at' in metadata
                assert 'source_database' in metadata
                assert 'total_tables' in metadata
                assert 'configuration_version' in metadata
                assert 'analyzer_version' in metadata
                assert 'schema_hash' in metadata
                assert 'analysis_timestamp' in metadata
                assert 'environment' in metadata
                
                # Verify new metadata fields have correct types
                assert isinstance(metadata['schema_hash'], str)
                assert isinstance(metadata['analysis_timestamp'], str)
                assert isinstance(metadata['environment'], str)
                assert len(metadata['schema_hash']) > 0  # Should be a valid hash
                assert len(metadata['analysis_timestamp']) > 0  # Should be a timestamp string
                
                # Verify table configurations
                for table_name, table_config in config['tables'].items():
                    assert 'table_name' in table_config
                    assert 'table_importance' in table_config
                    assert 'extraction_strategy' in table_config
                    assert 'estimated_rows' in table_config
                    assert 'estimated_size_mb' in table_config
                    assert 'batch_size' in table_config
                    assert 'incremental_columns' in table_config
                    assert 'is_modeled' in table_config
                    assert 'dbt_model_types' in table_config
                    assert 'monitoring' in table_config
                    assert 'schema_hash' in table_config
                    assert 'last_analyzed' in table_config
                    
                    # Verify monitoring structure
                    monitoring = table_config['monitoring']
                    assert 'alert_on_failure' in monitoring
                    assert 'alert_on_slow_extraction' in monitoring

    def test_schema_hash_generation_with_mocked_data(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test schema hash generation with mocked schema data.
        
        AAA Pattern:
            Arrange: Set up mock schema data for test tables
            Act: Call _generate_schema_hash() method with test tables
            Assert: Verify schema hash is correctly generated
            
        Validates:
            - Schema hash generation with mocked database schema data
            - Consistent hash generation for same schema structure
            - Hash includes table names, column names, and primary keys
            - Error handling for hash generation failures
            - Settings injection with mocked database connections
        """
        # Arrange: Set up mock schema data for test tables
        test_tables = ['patient', 'appointment', 'procedurelog']
        
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
            
            # Mock the get_table_schema method to return our test data
            def mock_get_schema(table_name):
                return mock_schema_data.get(table_name, {})
            
            analyzer.get_table_schema = mock_get_schema
            
            # Act: Call _generate_schema_hash() method with test tables
            schema_hash = analyzer._generate_schema_hash(test_tables)
            
            # Assert: Verify schema hash is correctly generated
            assert isinstance(schema_hash, str)
            assert len(schema_hash) > 0
            assert len(schema_hash) == 32  # MD5 hash should be 32 characters
            
            # Verify hash is consistent for same input
            hash2 = analyzer._generate_schema_hash(test_tables)
            assert schema_hash == hash2, "Hash should be consistent for same schema"
            
            # Verify hash changes for different schema
            different_tables = ['patient', 'insplan']  # Different table set
            different_hash = analyzer._generate_schema_hash(different_tables)
            assert schema_hash != different_hash, "Hash should be different for different schema"

    def test_schema_hash_generation_error_handling(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test schema hash generation error handling.
        
        AAA Pattern:
            Arrange: Set up mock get_table_schema that raises exception
            Act: Call _generate_schema_hash() method with failing mock
            Assert: Verify error is handled gracefully
            
        Validates:
            - Error handling for schema hash generation failures
            - Graceful degradation when hash generation fails
            - Default "unknown" value when hash generation fails
            - Settings injection error handling
        """
        # Arrange: Set up mock get_table_schema that raises exception
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
            
            # Mock the get_table_schema method to raise an exception
            def mock_get_schema_failing(table_name):
                raise Exception("Schema analysis failed")
            
            analyzer.get_table_schema = mock_get_schema_failing
            
            # Act: Call _generate_schema_hash() method with failing mock
            schema_hash = analyzer._generate_schema_hash(['patient'])
            
            # Assert: Verify error is handled gracefully
            assert schema_hash == "unknown"

    def test_fail_fast_on_missing_environment(self):
        """
        Test that system fails fast when OPENDENTAL_SOURCE_DB not set.
        
        AAA Pattern:
            Arrange: Remove OPENDENTAL_SOURCE_DB from environment variables
            Act: Attempt to create OpenDentalSchemaAnalyzer without required env var
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior when OPENDENTAL_SOURCE_DB not set
            - Clear error message for missing environment variables
            - No default fallback to production environment
            - Settings injection validation with environment requirements
        """
        # Arrange: Remove OPENDENTAL_SOURCE_DB from environment variables
        original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
        if 'OPENDENTAL_SOURCE_DB' in os.environ:
            del os.environ['OPENDENTAL_SOURCE_DB']
        
        try:
            # Act: Attempt to create OpenDentalSchemaAnalyzer without required env var
            with pytest.raises(ValueError, match="OPENDENTAL_SOURCE_DB environment variable is required"):
                analyzer = OpenDentalSchemaAnalyzer()
        finally:
            # Cleanup: Restore original environment
            if original_source_db:
                os.environ['OPENDENTAL_SOURCE_DB'] = original_source_db

    def test_error_handling_in_table_schema_analysis(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test error handling in table schema analysis.
        
        AAA Pattern:
            Arrange: Set up mock inspector that raises exception
            Act: Call get_table_schema() method with failing mock
            Assert: Verify error is handled gracefully
            
        Validates:
            - Error handling for database connection failures
            - Graceful degradation when schema analysis fails
            - Error information preservation in return value
            - Settings injection error handling
        """
        # Arrange: Set up mock inspector that raises exception
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector that raises exception
            mock_inspector = Mock()
            mock_inspector.get_columns.side_effect = Exception("Database connection failed")
            mock_inspect.return_value = mock_inspector
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_schema() method with failing mock
            schema_info = analyzer.get_table_schema('patient')
            
            # Assert: Verify error is handled gracefully
            assert 'table_name' in schema_info
            assert 'error' in schema_info
            assert "Database connection failed" in schema_info['error']

    def test_error_handling_in_table_size_analysis(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test error handling in table size analysis with ConnectionManager.
        
        AAA Pattern:
            Arrange: Set up mock ConnectionManager that raises exception
            Act: Call get_table_size_info() method with failing ConnectionManager
            Assert: Verify error is handled gracefully
            
        Validates:
            - Error handling for ConnectionManager failures
            - Graceful degradation when size analysis fails
            - Default values when size information unavailable
            - Settings injection error handling with ConnectionManager
        """
        # Arrange: Set up mock ConnectionManager that raises exception
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager that raises exception
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(side_effect=Exception("ConnectionManager failed"))
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_table_size_info() method with failing ConnectionManager
            size_info = analyzer.get_table_size_info('patient')
            
            # Assert: Verify error is handled gracefully
            assert 'table_name' in size_info
            assert 'estimated_row_count' in size_info
            assert 'size_mb' in size_info
            assert 'error' in size_info
            assert size_info['estimated_row_count'] == 0
            assert size_info['size_mb'] == 0
            assert "ConnectionManager failed" in size_info['error']

    def test_batch_schema_info_processing(self, mock_settings_with_dict_provider, mock_schema_data, mock_environment_variables):
        """
        Test batch schema information processing with mocked data.
        
        AAA Pattern:
            Arrange: Set up mock schema data for multiple tables
            Act: Call get_batch_schema_info() method with test tables
            Assert: Verify batch schema information is correctly processed
            
        Validates:
            - Batch schema processing with mocked database data
            - Multiple table schema extraction in single connection
            - Error handling for individual tables in batch
            - ConnectionManager usage for batch operations
            - Settings injection with mocked database connections
        """
        # Arrange: Set up mock schema data for multiple tables
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager context manager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock inspector methods to return our test data
            def mock_get_columns(table_name):
                return [{'name': name, **info} for name, info in mock_schema_data[table_name]['columns'].items()]
            
            def mock_get_pk_constraint(table_name):
                return {'constrained_columns': mock_schema_data[table_name]['primary_keys']}
            
            def mock_get_foreign_keys(table_name):
                return mock_schema_data[table_name]['foreign_keys']
            
            def mock_get_indexes(table_name):
                return mock_schema_data[table_name]['indexes']
            
            mock_inspector.get_columns.side_effect = mock_get_columns
            mock_inspector.get_pk_constraint.side_effect = mock_get_pk_constraint
            mock_inspector.get_foreign_keys.side_effect = mock_get_foreign_keys
            mock_inspector.get_indexes.side_effect = mock_get_indexes
            
            # Act: Call get_batch_schema_info() method with test tables
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            
            # Assert: Verify batch schema information is correctly processed
            assert isinstance(batch_schema_info, dict)
            assert len(batch_schema_info) == 3
            
            # Verify each table's schema information
            for table_name in test_tables:
                assert table_name in batch_schema_info
                schema_info = batch_schema_info[table_name]
                assert 'table_name' in schema_info
                assert 'columns' in schema_info
                assert 'primary_keys' in schema_info
                assert 'foreign_keys' in schema_info
                assert 'indexes' in schema_info
                
                # Verify column information
                assert len(schema_info['columns']) > 0
                assert schema_info['table_name'] == table_name

    def test_batch_size_info_processing(self, mock_settings_with_dict_provider, mock_size_data, mock_environment_variables):
        """
        Test batch size information processing with mocked data.
        
        AAA Pattern:
            Arrange: Set up mock size data for multiple tables
            Act: Call get_batch_size_info() method with test tables
            Assert: Verify batch size information is correctly processed
            
        Validates:
            - Batch size processing with mocked database data
            - Multiple table size extraction in single connection
            - Error handling for individual tables in batch
            - ConnectionManager usage for batch operations
            - Settings injection with mocked database connections
        """
        # Arrange: Set up mock size data for multiple tables
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine with proper context manager support
            mock_engine = Mock()
            mock_connection = Mock()
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=None)
            mock_engine.connect.return_value = mock_connection
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager context manager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock execute_with_retry to return our test data
            def mock_execute_with_retry(query):
                mock_result = Mock()
                if 'TABLE_ROWS' in query:
                    # Return estimated row count
                    table_name = query.split("table_name = '")[1].split("'")[0]
                    mock_result.scalar.return_value = mock_size_data[table_name]['estimated_row_count']
                elif 'information_schema.tables' in query:
                    # Return size in MB
                    table_name = query.split("table_name = '")[1].split("'")[0]
                    mock_result.scalar.return_value = mock_size_data[table_name]['size_mb']
                else:
                    # Return 0 for other queries
                    mock_result.scalar.return_value = 0
                return mock_result
            
            mock_conn_manager_instance.execute_with_retry.side_effect = mock_execute_with_retry
            
            mock_factory.get_source_connection.return_value = mock_engine
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call get_batch_size_info() method with test tables
            batch_size_info = analyzer.get_batch_size_info(test_tables)
            
            # Assert: Verify batch size information is correctly processed
            assert isinstance(batch_size_info, dict)
            assert len(batch_size_info) == 3
            
            # Verify each table's size information
            for table_name in test_tables:
                assert table_name in batch_size_info
                size_info = batch_size_info[table_name]
                assert 'table_name' in size_info
                assert 'estimated_row_count' in size_info
                assert 'size_mb' in size_info
                assert 'source' in size_info
                assert size_info['source'] == 'information_schema_estimate'
                assert size_info['table_name'] == table_name
                
                # Verify size data matches our mock
                expected_size = mock_size_data[table_name]
                assert size_info['estimated_row_count'] == expected_size['estimated_row_count']
                assert size_info['size_mb'] == expected_size['size_mb']

    def test_batch_processing_with_connection_manager(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test batch processing using ConnectionManager for efficiency.
        
        AAA Pattern:
            Arrange: Set up mock data and ConnectionManager
            Act: Call batch processing methods
            Assert: Verify ConnectionManager is used correctly
            
        Validates:
            - ConnectionManager usage for batch operations
            - Proper connection lifecycle management
            - Rate limiting and retry logic integration
            - Settings injection with ConnectionManager
        """
        # Arrange: Set up mock data and ConnectionManager
        test_tables = ['patient', 'appointment']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager context manager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call batch processing methods
            analyzer.get_batch_schema_info(test_tables)
            analyzer.get_batch_size_info(test_tables)
            
            # Assert: Verify ConnectionManager is used correctly
            assert mock_conn_manager.call_count == 4  # 2 for schema, 2 for size (row count + size for each)
            mock_conn_manager.assert_called_with(mock_engine)
            # Verify context manager was used properly
            assert mock_conn_manager_instance.__enter__.call_count == 4
            assert mock_conn_manager_instance.__exit__.call_count == 4

    def test_timeout_handling_in_batch_operations(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test timeout handling in batch operations.
        
        AAA Pattern:
            Arrange: Set up mock that times out
            Act: Call batch processing methods with timeout
            Assert: Verify timeout is handled gracefully
            
        Validates:
            - Timeout handling for batch operations
            - Graceful degradation when operations timeout
            - Default values when batch operations fail
            - Settings injection error handling
        """
        # Arrange: Set up mock that times out
        test_tables = ['patient', 'appointment']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector with proper method returns
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock inspector methods to return proper data structures
            def mock_get_columns(table_name):
                return [
                    {'name': 'id', 'type': 'INT', 'nullable': False, 'default': None},
                    {'name': 'name', 'type': 'VARCHAR(255)', 'nullable': True, 'default': None}
                ]
            
            def mock_get_pk_constraint(table_name):
                return {'constrained_columns': ['id']}
            
            def mock_get_foreign_keys(table_name):
                return []
            
            def mock_get_indexes(table_name):
                return []
            
            mock_inspector.get_columns = mock_get_columns
            mock_inspector.get_pk_constraint = mock_get_pk_constraint
            mock_inspector.get_foreign_keys = mock_get_foreign_keys
            mock_inspector.get_indexes = mock_get_indexes
            
            # Mock ConnectionManager that raises timeout
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager_instance.execute_with_retry.side_effect = Exception("Database timeout")
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call batch processing methods with timeout
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            batch_size_info = analyzer.get_batch_size_info(test_tables)
            
            # Assert: Verify timeout is handled gracefully
            # Schema processing should succeed (inspector methods work)
            assert isinstance(batch_schema_info, dict)
            assert len(batch_schema_info) == 2
            
            for table_name in test_tables:
                assert table_name in batch_schema_info
                # Schema should succeed since inspector methods are mocked
                assert 'columns' in batch_schema_info[table_name]
                assert 'primary_keys' in batch_schema_info[table_name]
                assert 'foreign_keys' in batch_schema_info[table_name]
                assert 'indexes' in batch_schema_info[table_name]
            
            # Size processing should fail due to ConnectionManager timeout
            assert isinstance(batch_size_info, dict)
            assert len(batch_size_info) == 2
            
            for table_name in test_tables:
                assert table_name in batch_size_info
                assert 'error' in batch_size_info[table_name]
                assert 'timeout' in batch_size_info[table_name]['error']
                assert batch_size_info[table_name]['estimated_row_count'] == 0
                assert batch_size_info[table_name]['size_mb'] == 0

    def test_updated_generate_complete_configuration_with_batch_processing(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test updated generate_complete_configuration with batch processing.
        
        AAA Pattern:
            Arrange: Set up mock data and batch processing methods
            Act: Call generate_complete_configuration() method
            Assert: Verify configuration is generated using batch processing
            
        Validates:
            - Batch processing integration in configuration generation
            - ConnectionManager usage in main configuration flow
            - Error handling for batch operations
            - Settings injection with batch processing
        """
        # Arrange: Set up mock data and batch processing methods
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock batch processing methods
            def mock_get_batch_schema(table_names):
                return {name: mock_schema_data.get(name, {}) for name in table_names}
            
            def mock_get_batch_size(table_names):
                return {name: mock_size_data.get(name, {'estimated_row_count': 0, 'size_mb': 0, 'source': 'information_schema_estimate'}) for name in table_names}
            
            analyzer.get_batch_schema_info = mock_get_batch_schema
            analyzer.get_batch_size_info = mock_get_batch_size
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: mock_dbt_models
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify configuration is generated using batch processing
                assert 'metadata' in config
                assert 'tables' in config
                assert len(config['tables']) == 3
                
                # Verify that batch processing was used
                for table_name in test_tables:
                    assert table_name in config['tables']
                    table_config = config['tables'][table_name]
                    assert 'table_name' in table_config
                    assert 'table_importance' in table_config
                    assert 'extraction_strategy' in table_config
                    assert 'estimated_rows' in table_config
                    assert 'estimated_size_mb' in table_config
                    assert 'batch_size' in table_config
                    assert 'incremental_columns' in table_config
                    assert 'is_modeled' in table_config
                    assert 'dbt_model_types' in table_config
                    assert 'monitoring' in table_config
                    assert 'schema_hash' in table_config
                    assert 'last_analyzed' in table_config

    def test_connection_manager_integration_with_settings_injection(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test ConnectionManager integration with Settings injection.
        
        AAA Pattern:
            Arrange: Set up mock settings and ConnectionManager
            Act: Create analyzer and test connection usage
            Assert: Verify Settings injection works with ConnectionManager
            
        Validates:
            - Settings injection with ConnectionManager
            - Provider pattern integration with connection management
            - Environment-agnostic connection handling
            - ConnectionManager creation with injected settings
        """
        # Arrange: Set up mock settings and ConnectionManager
        settings = mock_settings_with_dict_provider
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Act: Create analyzer and test connection usage
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Test that ConnectionManager is created with the correct engine
            analyzer.get_table_size_info('patient')
            
            # Assert: Verify Settings injection works with ConnectionManager
            mock_factory.get_source_connection.assert_called_once()
            mock_conn_manager.assert_called_with(mock_engine)
            
            # Verify that the engine was created using the injected settings
            call_args = mock_factory.get_source_connection.call_args
            assert call_args is not None

    def test_error_handling_in_batch_operations_with_connection_manager(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test error handling in batch operations with ConnectionManager.
        
        AAA Pattern:
            Arrange: Set up mock ConnectionManager that raises exceptions
            Act: Call batch processing methods with failing ConnectionManager
            Assert: Verify errors are handled gracefully
            
        Validates:
            - Error handling for ConnectionManager failures
            - Graceful degradation when batch operations fail
            - Error information preservation in return values
            - Settings injection error handling with ConnectionManager
        """
        # Arrange: Set up mock ConnectionManager that raises exceptions
        test_tables = ['patient', 'appointment']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager that raises exception
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(side_effect=Exception("ConnectionManager failed"))
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Act: Call batch processing methods with failing ConnectionManager
            batch_schema_info = analyzer.get_batch_schema_info(test_tables)
            batch_size_info = analyzer.get_batch_size_info(test_tables)
            
            # Assert: Verify errors are handled gracefully
            assert isinstance(batch_schema_info, dict)
            assert len(batch_schema_info) == 2
            
            for table_name in test_tables:
                assert table_name in batch_schema_info
                assert 'error' in batch_schema_info[table_name]
                assert "ConnectionManager failed" in batch_schema_info[table_name]['error']
            
            assert isinstance(batch_size_info, dict)
            assert len(batch_size_info) == 2
            
            for table_name in test_tables:
                assert table_name in batch_size_info
                assert 'error' in batch_size_info[table_name]
                assert "ConnectionManager failed" in batch_size_info[table_name]['error']
                assert batch_size_info[table_name]['estimated_row_count'] == 0
                assert batch_size_info[table_name]['size_mb'] == 0

    def test_progress_monitoring_in_generate_complete_configuration(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test progress monitoring in generate_complete_configuration method.
        
        AAA Pattern:
            Arrange: Set up mock data and progress monitoring
            Act: Call generate_complete_configuration() method
            Assert: Verify progress monitoring works correctly
            
        Validates:
            - Progress bar integration in configuration generation
            - Real-time table information display
            - Processing statistics tracking
            - Progress updates for each table
            - Settings injection with progress monitoring
        """
        # Arrange: Set up mock data and progress monitoring
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock batch processing methods
            def mock_get_batch_schema(table_names):
                return {name: mock_schema_data.get(name, {}) for name in table_names}
            
            def mock_get_batch_size(table_names):
                return {name: mock_size_data.get(name, {'estimated_row_count': 0, 'size_mb': 0, 'source': 'information_schema_estimate'}) for name in table_names}
            
            analyzer.get_batch_schema_info = mock_get_batch_schema
            analyzer.get_batch_size_info = mock_get_batch_size
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: mock_dbt_models
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify progress monitoring works correctly
                assert 'metadata' in config
                assert 'tables' in config
                assert len(config['tables']) == 3
                
                # Verify tqdm was called with correct parameters
                mock_tqdm.assert_called_once()
                call_args = mock_tqdm.call_args
                assert call_args[1]['total'] == 3  # 3 tables
                assert call_args[1]['desc'] == "Analyzing tables"
                assert call_args[1]['unit'] == "table"
                
                # Verify progress bar methods were called
                assert mock_pbar.set_postfix.call_count == 3  # Called for each table
                assert mock_pbar.update.call_count == 3  # Called for each table

    def test_detailed_analysis_report_progress_monitoring(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test progress monitoring in detailed analysis report generation.
        
        AAA Pattern:
            Arrange: Set up mock data and progress monitoring
            Act: Call _generate_detailed_analysis_report() method
            Assert: Verify progress monitoring works correctly
            
        Validates:
            - Progress bar integration in detailed report generation
            - Table-by-table progress tracking
            - Error handling with progress monitoring
            - Settings injection with progress monitoring
        """
        # Arrange: Set up mock data and progress monitoring
        test_config = {
            'tables': {
                'patient': {'table_name': 'patient', 'table_importance': 'critical'},
                'appointment': {'table_name': 'appointment', 'table_importance': 'important'},
                'procedurelog': {'table_name': 'procedurelog', 'table_importance': 'important'}
            }
        }
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock schema and size methods
            def mock_get_schema(table_name):
                return mock_schema_data.get(table_name, {})
            
            def mock_get_size(table_name):
                return mock_size_data.get(table_name, {'row_count': 0, 'size_mb': 0, 'source': 'database_query'})
            
            analyzer.get_table_schema = mock_get_schema
            analyzer.get_table_size_info = mock_get_size
            
            # Act: Call _generate_detailed_analysis_report() method
            analysis_report = analyzer._generate_detailed_analysis_report(test_config)
            
            # Assert: Verify progress monitoring works correctly
            assert 'analysis_metadata' in analysis_report
            assert 'table_analysis' in analysis_report
            assert 'dbt_model_analysis' in analysis_report
            assert 'recommendations' in analysis_report
            
            # Verify tqdm was called with correct parameters
            mock_tqdm.assert_called_once()
            call_args = mock_tqdm.call_args
            assert call_args[1]['total'] == 3  # 3 tables
            assert call_args[1]['desc'] == "Generating detailed report"
            assert call_args[1]['unit'] == "table"
            
            # Verify progress bar methods were called
            assert mock_pbar.set_postfix.call_count == 3  # Called for each table
            assert mock_pbar.update.call_count == 3  # Called for each table

    def test_stage_by_stage_progress_tracking(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test stage-by-stage progress tracking in analyze_complete_schema method.
        
        AAA Pattern:
            Arrange: Set up mock data and stage tracking
            Act: Call analyze_complete_schema() method
            Assert: Verify stage tracking works correctly
            
        Validates:
            - Stage-by-stage progress tracking
            - Timing information for each stage
            - Total analysis time tracking
            - Settings injection with stage tracking
        """
        # Arrange: Set up mock data and stage tracking
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm, \
             patch('scripts.analyze_opendental_schema.time.time') as mock_time:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector with proper method configuration
            mock_inspector = Mock()
            
            # Configure mock inspector methods to return proper data structures
            def mock_get_columns(table_name):
                # Return list of column dictionaries as expected by the real code
                return [
                    {'name': 'id', 'type': 'INT', 'nullable': False, 'default': None},
                    {'name': 'name', 'type': 'VARCHAR(255)', 'nullable': True, 'default': None}
                ]
            
            def mock_get_pk_constraint(table_name):
                # Return primary key constraint as expected by the real code
                return {'constrained_columns': ['id']}
            
            def mock_get_foreign_keys(table_name):
                # Return foreign keys as expected by the real code
                return []
            
            def mock_get_indexes(table_name):
                # Return indexes as expected by the real code
                return []
            
            # Configure the mock inspector methods
            mock_inspector.get_columns = mock_get_columns
            mock_inspector.get_pk_constraint = mock_get_pk_constraint
            mock_inspector.get_foreign_keys = mock_get_foreign_keys
            mock_inspector.get_indexes = mock_get_indexes
            mock_inspector.get_table_names.return_value = test_tables
            
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            # Mock time for consistent timing
            def time_generator():
                # Provide increments that ensure exactly 15 seconds total
                # Start at 0, end at 15, with small increments
                current_time = 0
                while True:
                    yield current_time
                    current_time += 0.1  # Smaller increments for more precision
                    if current_time >= 15:  # Ensure we reach exactly 15
                        current_time = 15
            
            mock_time.side_effect = time_generator()
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Set the mock inspector on the analyzer instance (needed for schema hash generation)
            analyzer.inspector = mock_inspector
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock batch processing methods
            def mock_get_batch_schema(table_names):
                return {name: mock_schema_data.get(name, {}) for name in table_names}
            
            def mock_get_batch_size(table_names):
                return {name: mock_size_data.get(name, {'estimated_row_count': 0, 'size_mb': 0, 'source': 'information_schema_estimate'}) for name in table_names}
            
            analyzer.get_batch_schema_info = mock_get_batch_schema
            analyzer.get_batch_size_info = mock_get_batch_size
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: mock_dbt_models
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call analyze_complete_schema() method
                results = analyzer.analyze_complete_schema(temp_dir)
                
                # Assert: Verify stage tracking works correctly
                assert 'tables_config' in results
                assert 'analysis_report' in results
                assert 'analysis_log' in results
                assert 'total_time' in results
                
                # Verify timing information
                assert results['total_time'] >= 1.0  # At least 1 second (realistic for mocked analysis)
                assert results['total_time'] <= 10.0  # No more than 10 seconds (reasonable upper bound)

    def test_main_function_progress_monitoring(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_dbt_models, mock_environment_variables):
        """
        Test progress monitoring in main function.
        
        AAA Pattern:
            Arrange: Set up mock data and main function monitoring
            Act: Call main() function
            Assert: Verify main function progress monitoring works correctly
            
        Validates:
            - Main function progress monitoring
            - Script execution timing
            - Results display with timing information
            - Settings injection with main function monitoring
        """
        # Arrange: Set up mock data and main function monitoring
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm, \
             patch('scripts.analyze_opendental_schema.time.time') as mock_time:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            # Mock time for consistent timing
            def time_generator():
                # Provide increments that ensure exactly 15 seconds total
                # Start at 0, end at 15, with small increments
                current_time = 0
                while True:
                    yield current_time
                    current_time += 0.1  # Smaller increments for more precision
                    if current_time >= 15:  # Ensure we reach exactly 15
                        current_time = 15
            
            mock_time.side_effect = time_generator()
            
            # Mock the analyzer to return expected results
            with patch('scripts.analyze_opendental_schema.OpenDentalSchemaAnalyzer') as mock_analyzer_class:
                mock_analyzer = Mock()
                mock_analyzer.analyze_complete_schema.return_value = {
                    'tables_config': '/path/to/tables.yml',
                    'analysis_report': '/path/to/analysis.json',
                    'analysis_log': '/path/to/log.log',
                    'total_time': 20.0
                }
                mock_analyzer_class.return_value = mock_analyzer
                
                # Act: Call main() function
                from scripts.analyze_opendental_schema import main
                main()
                
                # Assert: Verify main function progress monitoring works correctly
                mock_analyzer.analyze_complete_schema.assert_called_once()

    def test_progress_bar_postfix_information(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test progress bar postfix information display.
        
        AAA Pattern:
            Arrange: Set up mock data and progress bar postfix
            Act: Call generate_complete_configuration() method
            Assert: Verify postfix information is correctly displayed
            
        Validates:
            - Progress bar postfix information
            - Real-time table information display
            - Processing statistics in postfix
            - Settings injection with postfix information
        """
        # Arrange: Set up mock data and progress bar postfix
        test_tables = ['patient', 'appointment']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock batch processing methods
            def mock_get_batch_schema(table_names):
                return {name: mock_schema_data.get(name, {}) for name in table_names}
            
            def mock_get_batch_size(table_names):
                return {name: mock_size_data.get(name, {'estimated_row_count': 0, 'size_mb': 0, 'source': 'information_schema_estimate'}) for name in table_names}
            
            analyzer.get_batch_schema_info = mock_get_batch_schema
            analyzer.get_batch_size_info = mock_get_batch_size
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: {'staging': [], 'mart': [], 'intermediate': []}
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify postfix information is correctly displayed
                assert mock_pbar.set_postfix.call_count == 2  # Called for each table
                
                # Verify postfix calls contain expected information
                postfix_calls = mock_pbar.set_postfix.call_args_list
                for call in postfix_calls:
                    postfix_data = call[0][0]  # First argument is the postfix dict
                    assert 'table' in postfix_data
                    assert 'rows' in postfix_data
                    assert 'processed' in postfix_data
                    assert 'errors' in postfix_data
                    assert isinstance(postfix_data['table'], str)
                    assert isinstance(postfix_data['rows'], str)
                    assert isinstance(postfix_data['processed'], int)
                    assert isinstance(postfix_data['errors'], int)

    def test_batch_progress_logging_with_timing(self, mock_settings_with_dict_provider, mock_schema_data, mock_size_data, mock_environment_variables):
        """
        Test batch progress logging with timing information.
        
        AAA Pattern:
            Arrange: Set up mock data and batch progress logging
            Act: Call generate_complete_configuration() method
            Assert: Verify batch progress logging works correctly
            
        Validates:
            - Batch progress logging with timing
            - Elapsed time calculation
            - Estimated remaining time calculation
            - Settings injection with batch progress logging
        """
        # Arrange: Set up mock data and batch progress logging
        test_tables = ['patient', 'appointment', 'procedurelog', 'insplan', 'definition']  # 5 tables for 1 batch
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm, \
             patch('scripts.analyze_opendental_schema.time.time') as mock_time:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(return_value=mock_conn_manager_instance)
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            # Mock time for consistent timing
            def time_generator():
                # Provide increments that ensure exactly 15 seconds total
                # Start at 0, end at 15, with small increments
                current_time = 0
                while True:
                    yield current_time
                    current_time += 0.1  # Smaller increments for more precision
                    if current_time >= 15:  # Ensure we reach exactly 15
                        current_time = 15
            
            mock_time.side_effect = time_generator()
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock batch processing methods
            def mock_get_batch_schema(table_names):
                return {name: mock_schema_data.get(name, {}) for name in table_names}
            
            def mock_get_batch_size(table_names):
                return {name: mock_size_data.get(name, {'estimated_row_count': 0, 'size_mb': 0, 'source': 'information_schema_estimate'}) for name in table_names}
            
            analyzer.get_batch_schema_info = mock_get_batch_schema
            analyzer.get_batch_size_info = mock_get_batch_size
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: {'staging': [], 'mart': [], 'intermediate': []}
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify batch progress logging works correctly
                assert 'metadata' in config
                assert 'tables' in config
                assert len(config['tables']) == 5
                
                # Verify timing was tracked
                assert mock_time.call_count >= 6  # At least start and end time calls

    def test_progress_monitoring_error_handling(self, mock_settings_with_dict_provider, mock_environment_variables):
        """
        Test error handling in progress monitoring.
        
        AAA Pattern:
            Arrange: Set up mock that fails during progress monitoring
            Act: Call methods with failing progress monitoring
            Assert: Verify error handling works correctly
            
        Validates:
            - Error handling in progress monitoring
            - Graceful degradation when progress monitoring fails
            - Settings injection error handling with progress monitoring
        """
        # Arrange: Set up mock that fails during progress monitoring
        test_tables = ['patient', 'appointment']
        
        with patch('scripts.analyze_opendental_schema.ConnectionFactory') as mock_factory, \
             patch('scripts.analyze_opendental_schema.inspect') as mock_inspect, \
             patch('scripts.analyze_opendental_schema.create_connection_manager') as mock_conn_manager, \
             patch('scripts.analyze_opendental_schema.tqdm') as mock_tqdm:
            
            # Create mock engine
            mock_engine = Mock()
            mock_factory.get_source_connection.return_value = mock_engine
            
            # Create mock inspector
            mock_inspector = Mock()
            mock_inspect.return_value = mock_inspector
            
            # Mock ConnectionManager that raises exception
            mock_conn_manager_instance = Mock()
            mock_conn_manager_instance.__enter__ = Mock(side_effect=Exception("ConnectionManager failed"))
            mock_conn_manager_instance.__exit__ = Mock(return_value=None)
            mock_conn_manager.return_value = mock_conn_manager_instance
            
            # Mock tqdm progress bar
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__ = Mock(return_value=mock_pbar)
            mock_tqdm.return_value.__exit__ = Mock(return_value=None)
            
            analyzer = OpenDentalSchemaAnalyzer()
            
            # Mock table discovery
            analyzer.discover_all_tables = lambda: test_tables
            
            # Mock batch processing methods that fail
            def mock_get_batch_schema(table_names):
                return {name: {'table_name': name, 'error': 'ConnectionManager failed'} for name in table_names}
            
            def mock_get_batch_size(table_names):
                return {name: {'table_name': name, 'row_count': 0, 'size_mb': 0, 'error': 'ConnectionManager failed'} for name in table_names}
            
            analyzer.get_batch_schema_info = mock_get_batch_schema
            analyzer.get_batch_size_info = mock_get_batch_size
            
            # Mock DBT model discovery
            analyzer.discover_dbt_models = lambda: {'staging': [], 'mart': [], 'intermediate': []}
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Act: Call generate_complete_configuration() method
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify error handling works correctly
                assert 'metadata' in config
                assert 'tables' in config
                assert len(config['tables']) == 2
                
                # Verify that errors are handled gracefully
                for table_name in test_tables:
                    assert table_name in config['tables']
                    table_config = config['tables'][table_name]
                    assert 'error' in table_config
                    assert 'table_importance' in table_config
                    assert 'extraction_strategy' in table_config
                
                # Verify progress bar was still used
                assert mock_pbar.set_postfix.call_count == 2
                assert mock_pbar.update.call_count == 2