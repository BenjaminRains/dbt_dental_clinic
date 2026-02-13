# tests/integration/scripts/analyze_opendental_schema/test_configuration_generation_integration.py

"""
Integration tests for configuration file generation with real production database connections.

This module tests configuration generation against the actual production OpenDental database
to validate real configuration file generation, metadata creation, and tables.yml output.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real configuration generation with actual production database data
- Validates metadata generation with real production database information
- Tests table configuration with real production schema and size data
- Uses Settings injection for clinic environment-agnostic connections

Coverage Areas:
- Real production configuration generation with actual database data
- Metadata generation with real production database information
- Table configuration with real production schema and size data
- DBT model integration with real project structure
- Error handling for real production database operations
- Settings injection with real production database connections
- Primary key detection in tables.yml
- Incremental strategy integration in configuration
- Versioned tables.yml output with internal versioning metadata

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for clinic environment
- Validates actual production database connections and configuration generation
"""

import pytest
import os
import tempfile
import yaml
from pathlib import Path
from decimal import Decimal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(9)  # After DBT integration tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestConfigurationGenerationIntegration:
    """Integration tests for configuration file generation with real production database connections."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class for clinic environment validation."""
        # Store original environment for cleanup
        cls.original_etl_env = os.environ.get('ETL_ENVIRONMENT')
        cls.original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
        
        # Set environment to clinic for these tests
        os.environ['ETL_ENVIRONMENT'] = 'clinic'
        
        # Validate clinic environment is available
        try:
            config_dir = Path(__file__).parent.parent.parent.parent.parent
            from etl_pipeline.config.providers import FileConfigProvider
            from etl_pipeline.config.settings import Settings
            from etl_pipeline.core.connections import ConnectionFactory
            from sqlalchemy import text
            
            provider = FileConfigProvider(config_dir)
            settings = Settings(environment='clinic', provider=provider)
            
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
            
        if cls.original_source_db is not None:
            os.environ['OPENDENTAL_SOURCE_DB'] = cls.original_source_db
        elif 'OPENDENTAL_SOURCE_DB' in os.environ:
            del os.environ['OPENDENTAL_SOURCE_DB']

    def test_production_complete_configuration_generation(self, production_settings_with_file_provider):
        """
        Test production complete configuration generation with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and temporary output directory
            Act: Call generate_complete_configuration() method with production data
            Assert: Verify configuration is correctly generated with production metadata
            
        Validates:
            - Real production configuration generation with actual database data
            - Metadata generation with real production database information
            - Table configuration with real production schema and size data
            - DBT model integration with real project structure
            - Error handling for real production database operations
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and temporary output directory
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Store original method for cleanup
        original_discover = analyzer.discover_all_tables
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Mock the discover_all_tables method to return key tables
                analyzer.discover_all_tables = lambda: ['patient', 'appointment', 'procedurelog']
                
                # Act: Call generate_complete_configuration() method with production data
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify configuration is correctly generated with production metadata
                assert 'metadata' in config
                assert 'tables' in config
                assert len(config['tables']) > 0
                assert len(config['tables']) == 3  # Should process exactly 3 tables
                
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
                
                # Verify new metadata fields have correct types and values
                assert isinstance(metadata['schema_hash'], str)
                assert isinstance(metadata['analysis_timestamp'], str)
                assert isinstance(metadata['environment'], str)
                assert len(metadata['schema_hash']) > 0  # Should be a valid hash
                assert len(metadata['analysis_timestamp']) > 0  # Should be a timestamp string
                assert metadata['environment'] in ['clinic', 'test', 'unknown']  # Should be valid environment
                
                # Verify table configurations
                for table_name, table_config in config['tables'].items():
                    assert 'table_name' in table_config
                    # Removed table_importance assertion - field has been deprecated
                    assert 'extraction_strategy' in table_config
                    assert 'estimated_rows' in table_config
                    assert 'estimated_size_mb' in table_config
                    assert 'is_modeled' in table_config
                    assert 'dbt_model_types' in table_config
                    
                # Verify configuration object structure and data types
                assert isinstance(config, dict)
                assert isinstance(config['metadata'], dict)
                assert isinstance(config['tables'], dict)
                
                # Verify metadata data types
                metadata = config['metadata']
                assert isinstance(metadata['generated_at'], str)
                assert isinstance(metadata['source_database'], str)
                assert isinstance(metadata['total_tables'], int)
                assert isinstance(metadata['configuration_version'], str)
                assert isinstance(metadata['analyzer_version'], str)
                
                # Verify table configuration data types
                for table_name, table_config in config['tables'].items():
                    assert isinstance(table_config['table_name'], str)
                    # Removed table_importance type assertion - field has been deprecated
                    assert isinstance(table_config['extraction_strategy'], str)
                    assert isinstance(table_config['estimated_rows'], int)
                    assert isinstance(table_config['estimated_size_mb'], (int, float, Decimal))
                    assert isinstance(table_config['batch_size'], int)
                    assert isinstance(table_config['incremental_columns'], list)
                    assert isinstance(table_config['is_modeled'], bool)
                    assert isinstance(table_config['dbt_model_types'], list)
                    assert isinstance(table_config['monitoring'], dict)
                    assert isinstance(table_config['schema_hash'], int)  # Should be integer from hash() function
                    assert isinstance(table_config['last_analyzed'], str)
                    
                    # Verify incremental columns are timestamp/datetime OR auto-incrementing primary keys
                    if table_config['incremental_columns']:
                        # Get the actual schema info for this table to verify data types
                        table_schema = analyzer.get_table_schema(table_name)
                        for col_name in table_config['incremental_columns']:
                            assert col_name in table_schema['columns'], f"Column {col_name} not found in schema for {table_name}"
                            col_info = table_schema['columns'][col_name]
                            col_type = str(col_info['type']).lower()
                            is_primary_key = col_info.get('primary_key', False)
                            is_timestamp = any(timestamp_type in col_type for timestamp_type in ['timestamp', 'datetime'])
                            is_auto_increment_pk = any(int_type in col_type for int_type in ['int', 'bigint']) and is_primary_key
                            
                            assert is_timestamp or is_auto_increment_pk, \
                                f"Column {col_name} with type {col_type} should be timestamp/datetime or auto-incrementing PK for table {table_name}"
                    
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_primary_key_detection_in_tables_yml(self, production_settings_with_file_provider):
        """Test that primary_key is correctly detected and written for key tables in tables.yml."""
        import yaml
        from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
        import os
        import time
        import threading
        
        # Use a temp directory to avoid overwriting production config
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # Windows-compatible timeout mechanism
            timeout_occurred = threading.Event()
            result = {'config': None, 'error': None}
            
            def run_with_timeout():
                try:
                    analyzer = OpenDentalSchemaAnalyzer()
                    
                    # Mock the discover_all_tables method to return only the tables we need for this test
                    original_discover = analyzer.discover_all_tables
                    analyzer.discover_all_tables = lambda: ['procedurelog', 'appointment', 'patient']
                    
                    # Add timeout to the configuration generation
                    start_time = time.time()
                    # Use analyze_complete_schema instead of generate_complete_configuration to get tables.yml
                    results = analyzer.analyze_complete_schema(tmpdir)
                    elapsed_time = time.time() - start_time
                    
                    # Log timing information
                    print(f"Configuration generation took {elapsed_time:.2f} seconds")
                    
                    result['config'] = results
                    
                    # Restore original method
                    analyzer.discover_all_tables = original_discover
                    
                except Exception as e:
                    result['error'] = e
            
            # Run the configuration generation in a separate thread with timeout
            thread = threading.Thread(target=run_with_timeout)
            thread.daemon = True
            thread.start()
            thread.join(timeout=120)  # 2 minute timeout
            
            if thread.is_alive():
                print("Test timed out after 120 seconds")
                raise TimeoutError("Test timed out after 120 seconds")
            
            if result['error']:
                print(f"Test failed with error: {result['error']}")
                raise result['error']
            
            config = result['config']
            assert config is not None, "Configuration generation failed"
            
            tables_yml_path = os.path.join(tmpdir, 'tables.yml')
            assert os.path.exists(tables_yml_path), f"tables.yml not found at {tables_yml_path}"
            with open(tables_yml_path, 'r') as f:
                config_data = yaml.safe_load(f)
            tables = config_data.get('tables', {})

            # Check procedurelog
            proc = tables.get('procedurelog')
            assert proc is not None, "procedurelog table not found in config"
            assert 'primary_key' in proc, "primary_key missing for procedurelog"
            assert proc['primary_key'] == 'ProcNum', f"Expected primary_key 'ProcNum' for procedurelog, got {proc['primary_key']}"

            # Check appointment
            appt = tables.get('appointment')
            assert appt is not None, "appointment table not found in config"
            assert 'primary_key' in appt, "primary_key missing for appointment"
            assert appt['primary_key'] == 'AptNum', f"Expected primary_key 'AptNum' for appointment, got {appt['primary_key']}"

            # Optionally check a table with a composite or no primary key
            # ... add more assertions as needed

    def test_production_complete_configuration_with_incremental_strategy(self, production_settings_with_file_provider):
        """
        Test production complete configuration generation includes incremental_strategy.
        
        AAA Pattern:
            Arrange: Set up real production database connection and generate configuration
            Act: Call generate_complete_configuration() method with production data
            Assert: Verify incremental_strategy is included in table configuration
            
        Validates:
            - Real production configuration generation includes incremental_strategy
            - Strategy determination is called for each table
            - Configuration includes the determined strategy
            - Integration of incremental strategy in production configuration generation
        """
        # Arrange: Set up real production database connection and generate configuration
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Store original method for cleanup
        original_discover = analyzer.discover_all_tables
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Mock the discover_all_tables method to return key tables
                analyzer.discover_all_tables = lambda: ['patient', 'appointment', 'procedurelog']
                
                # Act: Call generate_complete_configuration() method with production data
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Assert: Verify incremental_strategy is included in table configuration
                assert 'tables' in config
                assert len(config['tables']) == 3
                
                # Verify each table has incremental_strategy
                for table_name, table_config in config['tables'].items():
                    assert 'incremental_strategy' in table_config
                    assert isinstance(table_config['incremental_strategy'], str)
                    assert table_config['incremental_strategy'] in ['or_logic', 'and_logic', 'single_column', 'none']
                    
                    # Verify that the strategy makes sense for the table
                    if table_name == 'procedurelog':  # Conservative table
                        assert table_config['incremental_strategy'] == 'and_logic', \
                            f"Conservative table {table_name} should use and_logic"
                    elif len(table_config.get('incremental_columns', [])) == 0:
                        assert table_config['incremental_strategy'] == 'none', \
                            f"Table {table_name} with no incremental columns should use none"
                    elif len(table_config.get('incremental_columns', [])) == 1:
                        assert table_config['incremental_strategy'] == 'single_column', \
                            f"Table {table_name} with single incremental column should use single_column"
                    elif len(table_config.get('incremental_columns', [])) > 1:
                        if table_name in ['claimproc', 'payment', 'adjustment', 'procedurelog']:
                            assert table_config['incremental_strategy'] == 'and_logic', \
                                f"Conservative table {table_name} should have 'and_logic' strategy"
                        else:
                            assert table_config['incremental_strategy'] == 'or_logic', \
                                f"Table {table_name} with multiple incremental columns should have 'or_logic' strategy"
                
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_production_tables_yml_incremental_strategy_integration(self, production_settings_with_file_provider):
        """
        Test that tables.yml includes incremental_strategy in production configuration.
        
        AAA Pattern:
            Arrange: Set up real production database connection and generate tables.yml
            Act: Generate complete configuration and check tables.yml content
            Assert: Verify incremental_strategy is included in tables.yml
            
        Validates:
            - tables.yml includes incremental_strategy field
            - Strategy determination works in clinic environment
            - Configuration file generation includes new field
            - Integration of incremental strategy in production tables.yml
        """
        # Arrange: Set up real production database connection and generate tables.yml
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Store original method for cleanup
        original_discover = analyzer.discover_all_tables
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Mock the discover_all_tables method to return key tables
                analyzer.discover_all_tables = lambda: ['patient', 'appointment', 'procedurelog']
                
                # Act: Generate complete configuration and check tables.yml content
                results = analyzer.analyze_complete_schema(temp_dir)
                
                # Assert: Verify incremental_strategy is included in tables.yml
                assert results['tables_config'].endswith('tables.yml')
                assert os.path.exists(results['tables_config'])
                
                # Verify tables.yml content has incremental_strategy
                with open(results['tables_config'], 'r') as f:
                    config = yaml.safe_load(f)
                    assert 'tables' in config
                    
                    # Verify each table has incremental_strategy
                    for table_name, table_config in config['tables'].items():
                        assert 'incremental_strategy' in table_config
                        assert isinstance(table_config['incremental_strategy'], str)
                        assert table_config['incremental_strategy'] in ['or_logic', 'and_logic', 'single_column', 'none']
                        
                        # Verify that the strategy is consistent with the table characteristics
                        if 'incremental_columns' in table_config:
                            incremental_columns = table_config['incremental_columns']
                            strategy = table_config['incremental_strategy']
                            
                            if len(incremental_columns) == 0:
                                assert strategy == 'none', \
                                    f"Table {table_name} with no incremental columns should have 'none' strategy"
                            elif len(incremental_columns) == 1:
                                assert strategy == 'single_column', \
                                    f"Table {table_name} with single incremental column should have 'single_column' strategy"
                            elif len(incremental_columns) > 1:
                                if table_name in ['claimproc', 'payment', 'adjustment', 'procedurelog']:
                                    assert strategy == 'and_logic', \
                                        f"Conservative table {table_name} should have 'and_logic' strategy"
                                else:
                                    assert strategy == 'or_logic', \
                                        f"Table {table_name} with multiple incremental columns should have 'or_logic' strategy"
                
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_versioned_tables_yml_output(self, production_settings_with_file_provider):
        """
        Test that tables.yml is created with internal versioning metadata.
        
        AAA Pattern:
            Arrange: Set up real production database connection and temporary output directory
            Act: Run complete schema analysis
            Assert: Verify tables.yml is created with proper metadata
            
        Validates:
            - tables.yml is created (not versioned files)
            - Internal versioning metadata is included
            - Metadata includes schema hash and environment info
            - Only tables.yml is created (no versioned files)
        """
        # Arrange: Set up real production database connection and temporary output directory
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Store original method for cleanup
        original_discover = analyzer.discover_all_tables
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Mock the discover_all_tables method to return key tables
                analyzer.discover_all_tables = lambda: ['patient', 'appointment', 'procedurelog']
                
                # Act: Run complete schema analysis
                results = analyzer.analyze_complete_schema(temp_dir)
                
                # Assert: Verify tables.yml is created with proper metadata
                assert results['tables_config'].endswith('tables.yml')
                assert os.path.exists(results['tables_config'])
                
                # Verify tables.yml content has internal versioning
                with open(results['tables_config'], 'r') as f:
                    config = yaml.safe_load(f)
                    assert 'metadata' in config
                    metadata = config['metadata']
                    assert 'schema_hash' in metadata
                    assert 'analysis_timestamp' in metadata
                    assert 'environment' in metadata
                    assert 'configuration_version' in metadata
                    assert 'analyzer_version' in metadata
                
                # Verify only tables.yml is created (no versioned files)
                import glob
                versioned_files = glob.glob(os.path.join(temp_dir, 'tables_*.yml'))
                assert len(versioned_files) == 0, "No versioned files should be created"
                # tables.yml should be the only configuration file
                tables_yml_path = os.path.join(temp_dir, 'tables.yml')
                assert os.path.exists(tables_yml_path), "tables.yml should be created"
                
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in the current environment for each test
        current_env = os.environ.get('ETL_ENVIRONMENT', 'test')
        os.environ['ETL_ENVIRONMENT'] = current_env
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 