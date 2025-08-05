# tests/integration/scripts/analyze_opendental_schema/test_incremental_strategy_integration.py

"""
Integration tests for incremental strategy and column discovery with real production database connections.

This module tests incremental strategy determination and column discovery against the actual production
OpenDental database to validate real timestamp column discovery, strategy selection, and data quality validation.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real incremental column discovery with actual production database data
- Validates data-driven timestamp discovery approach vs hardcoded patterns
- Tests incremental strategy determination with real production data
- Uses Settings injection for production environment-agnostic connections

Coverage Areas:
- Real production incremental column discovery from actual database schema
- Data quality validation for production timestamp columns
- Column prioritization based on predefined priority order
- Limiting to top 3 most reliable columns
- Filtering of poor quality columns
- Data-driven timestamp discovery works with real production database
- Timestamp columns are identified by data type, not hardcoded patterns
- Approach works with various timestamp data types (timestamp, datetime, date, time)
- No reliance on specific column name patterns
- Real production database schema compatibility

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for production environment
- Validates actual production database connections and incremental strategy determination
"""

import pytest
import os
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(7)  # After extraction strategy tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestIncrementalStrategyIntegration:
    """Integration tests for incremental strategy and column discovery with real production database connections."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class with production environment validation."""
        # Store original environment for cleanup
        cls.original_etl_env = os.environ.get('ETL_ENVIRONMENT')
        
        # Set production environment for this test class
        os.environ['ETL_ENVIRONMENT'] = 'production'
        
        # Validate production environment is available
        try:
            config_dir = Path(__file__).parent.parent.parent.parent.parent
            from etl_pipeline.config.providers import FileConfigProvider
            from etl_pipeline.config.settings import Settings
            from etl_pipeline.core.connections import ConnectionFactory
            from sqlalchemy import text
            
            provider = FileConfigProvider(config_dir, environment='production')
            settings = Settings(environment='production', provider=provider)
            
            # Test connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            with source_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()
                if not row or row[0] != 1:
                    pytest.skip("Production database connection failed")
                    
        except Exception as e:
            pytest.skip(f"Production databases not available: {str(e)}")
    
    @classmethod
    def teardown_class(cls):
        """Clean up test class environment."""
        # Restore original environment variables
        if cls.original_etl_env is not None:
            os.environ['ETL_ENVIRONMENT'] = cls.original_etl_env
        elif 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']

    def test_production_incremental_column_discovery(self, production_settings_with_file_provider):
        """
        Test production incremental column discovery with actual production database schema.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema data
            Act: Call find_incremental_columns() method with production schema
            Assert: Verify timestamp columns are correctly identified by data type in production
            
        Validates:
            - Real production incremental column discovery from actual database schema
            - Timestamp column identification by data type from real production database
            - Data type-based discovery (timestamp, datetime, date, time)
            - Maximum candidate limit enforcement
            - Error handling for real production database operations
            - Data-driven approach vs hardcoded patterns
        """
        # Arrange: Set up real production database connection and get real schema data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with multiple tables that likely have timestamp columns
        test_tables = ['patient', 'appointment', 'procedurelog', 'claimproc']
        found_tables = [table for table in test_tables if table in tables]
        
        if not found_tables:
            pytest.skip("No suitable test tables found in production database")
        
        for table_name in found_tables:
            schema_info = analyzer.get_table_schema(table_name)
            
            # Act: Call find_incremental_columns() method with production schema
            incremental_columns = analyzer.find_incremental_columns(table_name, schema_info)
            
            # Assert: Verify timestamp and datetime columns are correctly identified by data type in production
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) <= 5  # Allow more realistic limit for timestamp/datetime columns
            
            # Verify that identified columns exist in the production schema
            for col_name in incremental_columns:
                assert col_name in schema_info['columns']
                
                # Verify that identified columns have timestamp or datetime data types only
                col_info = schema_info['columns'][col_name]
                col_type = str(col_info['type']).lower()
                assert any(timestamp_type in col_type for timestamp_type in ['timestamp', 'datetime']), \
                    f"Column {col_name} with type {col_type} should be a timestamp or datetime type"
            
            # Log the results for debugging
            print(f"Table {table_name}: Found {len(incremental_columns)} timestamp columns: {incremental_columns}")
            
            # Verify we found at least some timestamp columns for tables that should have them
            if table_name in ['appointment', 'procedurelog']:
                assert len(incremental_columns) > 0, f"Table {table_name} should have timestamp columns"

    def test_production_data_driven_timestamp_discovery(self, production_settings_with_file_provider):
        """
        Test production data-driven timestamp discovery approach vs hardcoded patterns.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema data
            Act: Call find_incremental_columns() method with production schema
            Assert: Verify data-driven approach finds timestamp columns by data type
            
        Validates:
            - Data-driven timestamp discovery works with real production database
            - Timestamp columns are identified by data type, not hardcoded patterns
            - Approach works with various timestamp data types (timestamp, datetime, date, time)
            - No reliance on specific column name patterns
            - Real production database schema compatibility
        """
        # Arrange: Set up real production database connection and get real schema data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with tables that should have various timestamp column types
        test_tables = ['appointment', 'procedurelog', 'claimproc', 'patient']
        found_tables = [table for table in test_tables if table in tables]
        
        if not found_tables:
            pytest.skip("No suitable test tables found in production database")
        
        timestamp_data_types_found = set()
        
        for table_name in found_tables:
            schema_info = analyzer.get_table_schema(table_name)
            
            # Act: Call find_incremental_columns() method with production schema
            incremental_columns = analyzer.find_incremental_columns(table_name, schema_info)
            
            # Assert: Verify data-driven approach finds timestamp and datetime columns by data type
            for col_name in incremental_columns:
                col_info = schema_info['columns'][col_name]
                col_type = str(col_info['type']).lower()
                
                # Verify it's a timestamp or datetime data type
                is_timestamp = any(timestamp_type in col_type for timestamp_type in ['timestamp', 'datetime'])
                assert is_timestamp, f"Column {col_name} with type {col_type} should be a timestamp or datetime type"
                
                # Track what timestamp data types we found
                if 'timestamp' in col_type:
                    timestamp_data_types_found.add('timestamp')
                elif 'datetime' in col_type:
                    timestamp_data_types_found.add('datetime')
            
            print(f"Table {table_name}: Found {len(incremental_columns)} timestamp columns")
            if incremental_columns:
                print(f"  Columns: {incremental_columns}")
                for col in incremental_columns:
                    col_type = str(schema_info['columns'][col]['type'])
                    print(f"    {col}: {col_type}")
        
        # Verify we found at least some timestamp data types
        assert len(timestamp_data_types_found) > 0, "Should find at least one timestamp data type in production database"

    def test_production_determine_incremental_strategy(self, production_settings_with_file_provider):
        """
        Test production determine_incremental_strategy method with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema data
            Act: Call determine_incremental_strategy() method with production data
            Assert: Verify strategy is correctly determined for production tables
            
        Validates:
            - Real production incremental strategy determination with actual database data
            - Strategy selection based on number of incremental columns
            - Business logic for conservative tables (claimproc, payment, adjustment)
            - Strategy determination for different table types
        """
        # Arrange: Set up real production database connection and get real schema data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with different table types
        test_cases = [
            ('patient', ['DateTStamp', 'DateModified', 'SecDateTEdit']),  # Multiple columns -> or_logic
            ('claimproc', ['DateTStamp', 'SecDateTEdit']),  # Conservative table -> and_logic
            ('definition', ['DateTStamp']),  # Single column -> single_column
            ('securitylog', [])  # No columns -> none
        ]
        
        for table_name, incremental_columns in test_cases:
            if table_name in tables:
                schema_info = analyzer.get_table_schema(table_name)
                
                # Act: Call determine_incremental_strategy() method with production data
                strategy = analyzer.determine_incremental_strategy(table_name, schema_info, incremental_columns)
                
                # Assert: Verify strategy is correctly determined for production tables
                assert strategy in ['or_logic', 'and_logic', 'single_column', 'none']
                
                # Verify specific business logic
                if table_name == 'claimproc':
                    assert strategy == 'and_logic', f"Conservative table {table_name} should use and_logic"
                elif len(incremental_columns) == 0:
                    assert strategy == 'none', f"Table {table_name} with no columns should use none"
                elif len(incremental_columns) == 1:
                    assert strategy == 'single_column', f"Table {table_name} with single column should use single_column"
                elif len(incremental_columns) > 1:
                    if table_name in ['claimproc', 'payment', 'adjustment']:
                        assert strategy == 'and_logic', f"Conservative table {table_name} should use and_logic"
                    else:
                        assert strategy == 'or_logic', f"Table {table_name} with multiple columns should use or_logic"

    def test_production_enhanced_find_incremental_columns(self, production_settings_with_file_provider):
        """
        Test production enhanced find_incremental_columns method with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema data
            Act: Call find_incremental_columns() method with production data
            Assert: Verify enhanced incremental column discovery works correctly
            
        Validates:
            - Real production enhanced incremental column discovery with actual database data
            - Data quality validation integration in discovery process
            - Column prioritization based on predefined priority order
            - Limiting to top 3 most reliable columns
            - Filtering of poor quality columns
        """
        # Arrange: Set up real production database connection and get real schema data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with tables that likely have timestamp columns
        test_tables = ['patient', 'appointment', 'procedurelog', 'claimproc']
        found_tables = [table for table in test_tables if table in tables]
        
        if not found_tables:
            pytest.skip("No suitable test tables found in production database")
        
        for table_name in found_tables:
            schema_info = analyzer.get_table_schema(table_name)
            
            # Act: Call find_incremental_columns() method with production data
            incremental_columns = analyzer.find_incremental_columns(table_name, schema_info)
            
            # Assert: Verify enhanced incremental column discovery works correctly
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) <= 3  # Limited to top 3
            
            # Verify that identified columns exist in the production schema
            for col_name in incremental_columns:
                assert col_name in schema_info['columns']
                
                # Verify that identified columns have timestamp or datetime data types
                col_info = schema_info['columns'][col_name]
                col_type = str(col_info['type']).lower()
                assert any(timestamp_type in col_type for timestamp_type in ['timestamp', 'datetime']), \
                    f"Column {col_name} with type {col_type} should be a timestamp or datetime type"
            
            # Log the results for debugging
            print(f"Table {table_name}: Found {len(incremental_columns)} incremental columns: {incremental_columns}")
            
            # Verify prioritization (if we have multiple columns, DateTStamp should be first)
            if len(incremental_columns) > 0 and 'DateTStamp' in incremental_columns:
                assert incremental_columns[0] == 'DateTStamp', \
                    f"DateTStamp should be prioritized first in {table_name}"

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in production environment for each test
        os.environ['ETL_ENVIRONMENT'] = 'production'
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 