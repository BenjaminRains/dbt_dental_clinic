"""
Unit tests for PostgresLoader data quality validation and filtering.

This module contains tests for:
    - Data quality validation and filtering
    - Incremental column validation
    - UPSERT SQL generation
    - Data quality edge cases
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Import ETL pipeline components
from etl_pipeline.exceptions.configuration import ConfigurationError
from etl_pipeline.config import DatabaseType

# Import PostgresLoader for testing
try:
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    POSTGRES_LOADER_AVAILABLE = True
except ImportError:
    POSTGRES_LOADER_AVAILABLE = False
    PostgresLoader = None

# Import fixtures
try:
    from tests.fixtures.loader_fixtures import sample_table_data
    from tests.fixtures.env_fixtures import test_env_vars, load_test_environment_file
    FIXTURES_AVAILABLE = True
except ImportError:
    FIXTURES_AVAILABLE = False
    sample_table_data = None
    test_env_vars = None
    load_test_environment_file = None


@pytest.fixture
def real_postgres_loader_instance():
    """Create a PostgresLoader instance with mocked dependencies for testing real methods."""
    if not POSTGRES_LOADER_AVAILABLE:
        pytest.skip("PostgresLoader not available")
    
    # Create a mock PostgresLoader instance that allows testing real methods
    mock_loader = MagicMock()
    
    # Set up basic attributes
    mock_loader.analytics_schema = 'raw'
    mock_loader.settings = MagicMock()
    mock_loader.table_configs = {
        'patient': {'incremental_columns': ['DateModified'], 'primary_key': 'PatientNum'},
        'appointment': {'batch_size': 500, 'incremental_columns': ['DateCreated', 'DateModified'], 'primary_key': 'AptNum'},
        'procedurelog': {'primary_key': 'ProcNum'},
        'periomeasure': {
            'incremental_columns': ['DateEntered'],
            'estimated_size_mb': 57,
            'performance_strategy': 'streaming',
            'bulk_insert_buffer': 50000
        },
        'large_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 500
        },
        'empty_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 100
        },
        'test_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 50
        }
    }
    
    # Mock the schema adapter
    mock_schema_adapter = MagicMock()
    mock_schema_adapter.get_table_schema_from_mysql.return_value = {'columns': []}
    mock_schema_adapter.ensure_table_exists.return_value = True
    mock_schema_adapter.convert_row_data_types.return_value = {'id': 1, 'data': 'test'}
    mock_loader.schema_adapter = mock_schema_adapter
    
    # Mock database engines
    mock_replication_engine = MagicMock()
    mock_analytics_engine = MagicMock()
    mock_loader.replication_engine = mock_replication_engine
    mock_loader.analytics_engine = mock_analytics_engine
    
    # Configure get_table_config to return actual table configs
    def get_table_config_side_effect(table_name):
        return mock_loader.table_configs.get(table_name, {})
    
    mock_loader.get_table_config = MagicMock(side_effect=get_table_config_side_effect)
    
    # Mock methods that are called internally but don't need testing
    mock_loader._build_load_query = MagicMock(return_value="SELECT * FROM test_table")
    mock_loader._update_load_status = MagicMock(return_value=True)
    mock_loader._get_loaded_at_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.stream_mysql_data = MagicMock(return_value=[[{'id': 1, 'data': 'test'}]])
    mock_loader.load_table_copy_csv = MagicMock(return_value=True)
    mock_loader.load_table_standard = MagicMock(return_value=True)
    mock_loader.load_table_streaming = MagicMock(return_value=True)
    mock_loader.load_table_chunked = MagicMock(return_value=True)
    
    # Import the real PostgresLoader class to get access to its methods
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    import logging
    
    # Set up logger for the mock loader
    mock_loader.logger = logging.getLogger('etl_pipeline.loaders.postgres_loader')
    
    # Bind real methods to the mock loader for testing
    mock_loader._filter_valid_incremental_columns = PostgresLoader._filter_valid_incremental_columns.__get__(mock_loader, PostgresLoader)
    mock_loader._build_upsert_sql = PostgresLoader._build_upsert_sql.__get__(mock_loader, PostgresLoader)
    mock_loader._validate_incremental_integrity = PostgresLoader._validate_incremental_integrity.__get__(mock_loader, PostgresLoader)
    mock_loader._convert_sqlalchemy_row_to_dict = PostgresLoader._convert_sqlalchemy_row_to_dict.__get__(mock_loader, PostgresLoader)
    
    return mock_loader


@pytest.mark.unit
class TestPostgresLoaderDataQuality:
    """
    Unit tests for PostgresLoader data quality validation and filtering.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on data quality validation and filtering logic
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_filter_valid_incremental_columns_success(self, real_postgres_loader_instance):
        """
        Test filtering valid incremental columns based on data quality.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Mock the replication engine connection to return good data quality
        mock_conn = MagicMock()
        # Return data that should pass validation: reasonable dates, many records
        mock_conn.execute.return_value.fetchone.return_value = (datetime(2020, 1, 1), datetime(2024, 12, 31), 1000)
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act - call the real method
        result = loader._filter_valid_incremental_columns('patient', ['DateModified', 'DateCreated'])
        
        # Assert - should keep good quality columns
        assert 'DateModified' in result
        assert 'DateCreated' in result
    
    def test_filter_valid_incremental_columns_poor_quality(self, real_postgres_loader_instance):
        """
        Test filtering incremental columns with poor data quality.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Mock the replication engine connection to return poor data quality
        mock_conn = MagicMock()
        # Return data that should be filtered out: dates before 2000, only 50 records
        mock_conn.execute.return_value.fetchone.return_value = (datetime(1990, 1, 1), datetime(1995, 12, 31), 50)
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act - call the real method
        result = loader._filter_valid_incremental_columns('patient', ['DateModified'])
        
        # Assert - should filter out poor quality columns
        assert len(result) == 0  # Should filter out poor quality columns
    
    def test_build_upsert_sql_success(self, real_postgres_loader_instance):
        """
        Test building PostgreSQL UPSERT SQL statement.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Act - call the real method (primary key is already configured in the fixture)
        result = loader._build_upsert_sql('patient', ['PatientNum', 'LName', 'FName'])
        
        # Assert - verify the generated SQL contains expected elements
        assert 'INSERT INTO raw.patient' in result
        assert '"PatientNum"' in result
        assert '"LName"' in result
        assert '"FName"' in result
        assert 'ON CONFLICT ("PatientNum")' in result
        assert 'DO UPDATE SET' in result
    
    def test_validate_incremental_integrity_success(self, real_postgres_loader_instance):
        """
        Test incremental integrity validation.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 5  # 5 new records
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act - call the real method
        result = loader._validate_incremental_integrity('patient', ['DateModified'], datetime(2024, 1, 1, 10, 0, 0))
        
        # Assert - should return True for successful validation
        assert result is True
    
    def test_validate_incremental_integrity_no_columns(self, real_postgres_loader_instance):
        """
        Test incremental integrity validation with no incremental columns.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Act - no columns should skip validation
        result = loader._validate_incremental_integrity('patient', [], datetime(2024, 1, 1, 10, 0, 0))
        
        # Assert
        assert result is True  # Should skip validation when no columns
    
    def test_data_quality_validation_edge_cases(self, real_postgres_loader_instance):
        """
        Test data quality validation with edge cases.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Mock the replication engine connection to return None (no data)
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act - call the real method
        result = loader._filter_valid_incremental_columns('patient', ['DateModified'])
        
        # Assert - should filter out columns with no data
        assert len(result) == 0  # Should filter out columns with no data
    
    def test_upsert_sql_with_different_primary_keys(self, real_postgres_loader_instance):
        """
        Test UPSERT SQL generation with different primary key configurations.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = real_postgres_loader_instance
        
        # Act - call the real methods (primary keys are already configured in the fixture)
        patient_sql = loader._build_upsert_sql('patient', ['PatientNum', 'LName'])
        appointment_sql = loader._build_upsert_sql('appointment', ['AptNum', 'AptDateTime'])
        
        # Assert - verify correct primary keys are used in ON CONFLICT clauses
        assert 'ON CONFLICT ("PatientNum")' in patient_sql
        assert 'ON CONFLICT ("AptNum")' in appointment_sql
        assert 'ON CONFLICT' in patient_sql
        assert 'ON CONFLICT' in appointment_sql 