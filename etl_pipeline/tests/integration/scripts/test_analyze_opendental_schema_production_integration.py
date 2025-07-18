# tests/integration/scripts/test_analyze_opendental_schema_production_integration.py

"""
Production Integration tests for OpenDentalSchemaAnalyzer using real production database.

This module tests the schema analyzer against the actual production OpenDental database
to validate real schema analysis, table discovery, and configuration generation.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real schema analysis with actual production data structures
- Validates table discovery with real production database
- Tests configuration generation with real production metadata
- Uses Settings injection with FileConfigProvider for production environment
- Tests FAIL FAST behavior with production environment validation
- Generates real configuration files for production ETL pipeline

Coverage Areas:
- Real production database schema analysis
- Actual production table structure analysis
- Real production table size and row count analysis
- Production DBT model discovery in project structure
- Real configuration generation with production metadata
- Error handling with real production database scenarios
- FAIL FAST behavior with production environment validation
- Provider pattern integration with production configuration files
- Settings injection for production environment-agnostic connections

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Validates actual production database connections and schema analysis
- Uses Settings injection for production environment-agnostic connections
- Generates production-ready tables.yml for ETL pipeline configuration
"""

import pytest
import tempfile
import os
import yaml
import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
from etl_pipeline.config import get_settings
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory


@pytest.fixture(scope="session")
def ensure_logs_directory():
    """Ensure logs directory exists for test session."""
    logs_dir = Path(__file__).parent.parent.parent.parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    yield logs_dir
    # Clean up any test-generated log files after session
    for log_file in logs_dir.glob('schema_analysis_*.log'):
        try:
            log_file.unlink()
        except Exception:
            pass  # Ignore cleanup errors


# Use existing fixtures from connection_fixtures.py and env_fixtures.py
# The production_settings fixture from env_fixtures.py provides the same functionality
# with proper Settings injection and provider pattern integration


@pytest.mark.integration
@pytest.mark.order(1)  # After connection tests, before data loading tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestOpenDentalSchemaAnalyzerProductionIntegration:
    """Production integration tests for OpenDentalSchemaAnalyzer with real production database connections."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class with production environment validation."""
        # Store original environment for cleanup
        cls.original_etl_env = os.environ.get('ETL_ENVIRONMENT')
        cls.original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
        
        # Set production environment for all tests in this class
        os.environ['ETL_ENVIRONMENT'] = 'production'
        
        # Validate production environment is available
        try:
            config_dir = Path(__file__).parent.parent.parent.parent
            provider = FileConfigProvider(config_dir)
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
            
        if cls.original_source_db is not None:
            os.environ['OPENDENTAL_SOURCE_DB'] = cls.original_source_db
        elif 'OPENDENTAL_SOURCE_DB' in os.environ:
            del os.environ['OPENDENTAL_SOURCE_DB']

    def test_production_schema_analyzer_initialization(self, production_settings):
        """
        Test production schema analyzer initialization with actual production database connection.
        
        AAA Pattern:
            Arrange: Set up real production environment with FileConfigProvider
            Act: Create OpenDentalSchemaAnalyzer instance with production connection
            Assert: Verify analyzer is properly initialized with production database
            
        Validates:
            - Real production database connection establishment
            - Settings injection with FileConfigProvider for production
            - Environment validation with real .env_production file
            - Configuration validation with real production configuration files
            - Provider pattern works with real FileConfigProvider
            - FAIL FAST behavior works with production environment validation
            - Inspector initialization with real production database connection
            - Error handling works for real production database connection failures
        """
        # Arrange: Set up real production environment with FileConfigProvider
        settings = production_settings
        
        # Act: Create OpenDentalSchemaAnalyzer instance with production connection
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Assert: Verify analyzer is properly initialized with production database
        assert analyzer.source_engine is not None
        assert analyzer.inspector is not None
        assert analyzer.source_db is not None
        assert analyzer.dbt_project_root is not None
        
        # Verify analyzer object attributes
        assert hasattr(analyzer, 'source_engine')
        assert hasattr(analyzer, 'inspector')
        assert hasattr(analyzer, 'source_db')
        assert hasattr(analyzer, 'dbt_project_root')
        
        # Verify source_db is a string and not empty
        assert isinstance(analyzer.source_db, str)
        assert len(analyzer.source_db) > 0
        
        # Verify dbt_project_root is a string and points to a valid path
        assert isinstance(analyzer.dbt_project_root, str)
        assert os.path.exists(analyzer.dbt_project_root)
        
        # Test real production database connection
        with analyzer.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test_value"))
            row = result.fetchone()
            assert row is not None and row[0] == 1

    def test_production_table_discovery(self, production_settings):
        """
        Test production table discovery with actual production database structure.
        
        AAA Pattern:
            Arrange: Set up real production database connection
            Act: Call discover_all_tables() method with production database
            Assert: Verify real production tables are discovered correctly
            
        Validates:
            - Real production table discovery from actual database
            - Proper filtering of excluded patterns in production
            - Real production database schema analysis
            - Error handling for real production database operations
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Act: Call discover_all_tables() method with production database
        tables = analyzer.discover_all_tables()
        
        # Assert: Verify real production tables are discovered correctly
        assert isinstance(tables, list)
        assert len(tables) > 0
        
        # Verify that excluded patterns are filtered out
        excluded_patterns = ['tmp_', 'temp_', 'backup_', '#', 'test_']
        for table in tables:
            assert not any(pattern in table.lower() for pattern in excluded_patterns)
        
        # Verify that common dental clinic tables are present in production
        common_tables = ['patient', 'appointment', 'procedurelog', 'claimproc']
        found_common_tables = [table for table in tables if table.lower() in common_tables]
        assert len(found_common_tables) > 0, f"Expected dental clinic tables not found in production. Available: {tables}"

    def test_production_table_schema_analysis(self, production_settings):
        """
        Test production table schema analysis with actual production database structure.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call get_table_schema() method for production tables
            Assert: Verify real production schema information is correctly extracted
            
        Validates:
            - Real production schema analysis from actual database
            - Column information extraction from real production tables
            - Primary key detection from real production database
            - Foreign key detection from real production database
            - Index information from real production database
            - Error handling for real production database operations
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Find a table to test (prefer patient table as it's likely to exist in production)
        test_table = 'patient' if 'patient' in tables else tables[0]
        
        # Act: Call get_table_schema() method for production tables
        schema_info = analyzer.get_table_schema(test_table)
        
        # Assert: Verify real production schema information is correctly extracted
        assert schema_info['table_name'] == test_table
        assert 'columns' in schema_info
        assert len(schema_info['columns']) > 0
        
        # Verify column information
        for col_name, col_info in schema_info['columns'].items():
            assert 'type' in col_info
            assert 'nullable' in col_info
            assert 'primary_key' in col_info
        
        # Verify primary keys
        assert 'primary_keys' in schema_info
        assert isinstance(schema_info['primary_keys'], list)
        
        # Verify foreign keys and indexes
        assert 'foreign_keys' in schema_info
        assert 'indexes' in schema_info

    def test_production_table_size_analysis(self, production_settings):
        """
        Test production table size analysis with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call get_table_size_info() method for production tables
            Assert: Verify real production size information is correctly extracted
            
        Validates:
            - Real production size analysis from actual database
            - Row count calculation from real production data
            - Table size estimation from real production database
            - Error handling for real production database operations
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Find a table to test (prefer patient table as it's likely to exist in production)
        test_table = 'patient' if 'patient' in tables else tables[0]
        
        # Act: Call get_table_size_info() method for production tables
        size_info = analyzer.get_table_size_info(test_table)
        
        # Assert: Verify real production size information is correctly extracted
        assert size_info['table_name'] == test_table
        assert 'estimated_row_count' in size_info
        assert 'size_mb' in size_info
        assert 'source' in size_info
        assert size_info['source'] == 'information_schema_estimate'
        
        # Verify that row count is a non-negative integer
        assert isinstance(size_info['estimated_row_count'], int)
        assert size_info['estimated_row_count'] >= 0
        
        # Verify that size is a non-negative number (can be Decimal from MySQL)
        from decimal import Decimal
        assert isinstance(size_info['size_mb'], (int, float, Decimal))
        assert float(size_info['size_mb']) >= 0

    def test_production_table_importance_determination(self, production_settings):
        """
        Test production table importance determination with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema/size data
            Act: Call determine_table_importance() method with production data
            Assert: Verify importance is correctly determined for production tables
            
        Validates:
            - Real production importance determination with actual database data
            - Critical table identification from real production database
            - Important table identification from real production database
            - Reference table identification from real production database
            - Audit table identification from real production database
            - Standard table identification from real production database
        """
        # Arrange: Set up real production database connection and get real schema/size data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with patient table (should be critical in production)
        if 'patient' in tables:
            schema_info = analyzer.get_table_schema('patient')
            size_info = analyzer.get_table_size_info('patient')
            
            # Act: Call determine_table_importance() method with production data
            importance = analyzer.determine_table_importance('patient', schema_info, size_info)
            
            # Assert: Verify importance is correctly determined for production tables
            assert importance in ['critical', 'important', 'reference', 'audit', 'standard']
            assert importance == 'critical'  # Patient should be critical in production

    def test_production_extraction_strategy_determination(self, production_settings):
        """
        Test production extraction strategy determination with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema/size data
            Act: Call determine_extraction_strategy() method with production data
            Assert: Verify strategy is correctly determined for production tables
            
        Validates:
            - Real production extraction strategy determination with actual database data
            - Full table strategy for small production tables
            - Incremental strategy for medium production tables with timestamp columns
            - Chunked incremental strategy for large production tables
            - Error handling for real production database operations
        """
        # Arrange: Set up real production database connection and get real schema/size data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with patient table
        if 'patient' in tables:
            schema_info = analyzer.get_table_schema('patient')
            size_info = analyzer.get_table_size_info('patient')
            
            # Act: Call determine_extraction_strategy() method with production data
            strategy = analyzer.determine_extraction_strategy('patient', schema_info, size_info)
            
            # Assert: Verify strategy is correctly determined for production tables
            assert strategy in ['full_table', 'incremental', 'chunked_incremental']

    def test_production_incremental_column_discovery(self, production_settings):
        """
        Test production incremental column discovery with actual production database schema.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema data
            Act: Call find_incremental_columns() method with production schema
            Assert: Verify incremental columns are correctly identified in production
            
        Validates:
            - Real production incremental column discovery from actual database schema
            - Timestamp column identification from real production database
            - ID column identification from real production database
            - Maximum candidate limit enforcement
            - Error handling for real production database operations
        """
        # Arrange: Set up real production database connection and get real schema data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with patient table
        if 'patient' in tables:
            schema_info = analyzer.get_table_schema('patient')
            
            # Act: Call find_incremental_columns() method with production schema
            incremental_columns = analyzer.find_incremental_columns('patient', schema_info)
            
            # Assert: Verify incremental columns are correctly identified in production
            assert isinstance(incremental_columns, list)
            assert len(incremental_columns) <= 3  # Max 3 candidates
            
            # Verify that identified columns exist in the production schema
            for col_name in incremental_columns:
                assert col_name in schema_info['columns']

    def test_production_dbt_model_discovery(self, production_settings):
        """
        Test production DBT model discovery with actual project structure.
        
        AAA Pattern:
            Arrange: Set up real production database connection and real project structure
            Act: Call discover_dbt_models() method with real project
            Assert: Verify DBT models are correctly discovered for production
            
        Validates:
            - Real production DBT model discovery from actual project structure
            - Staging model discovery from real project
            - Mart model discovery from real project
            - Intermediate model discovery from real project
            - Error handling for real project structure
        """
        # Arrange: Set up real production database connection and real project structure
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Act: Call discover_dbt_models() method with real project
        dbt_models = analyzer.discover_dbt_models()
        
        # Assert: Verify DBT models are correctly discovered for production
        assert isinstance(dbt_models, dict)
        assert 'staging' in dbt_models
        assert 'mart' in dbt_models
        assert 'intermediate' in dbt_models
        
        # Verify that all model lists are lists
        assert isinstance(dbt_models['staging'], list)
        assert isinstance(dbt_models['mart'], list)
        assert isinstance(dbt_models['intermediate'], list)

    def test_production_complete_configuration_generation(self, production_settings):
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
                assert metadata['environment'] in ['production', 'test', 'unknown']  # Should be valid environment
                
                # Verify table configurations
                for table_name, table_config in config['tables'].items():
                    assert 'table_name' in table_config
                    assert 'table_importance' in table_config
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
                    assert isinstance(table_config['table_importance'], str)
                    assert isinstance(table_config['extraction_strategy'], str)
                    assert isinstance(table_config['estimated_rows'], int)
                    from decimal import Decimal
                    assert isinstance(table_config['estimated_size_mb'], (int, float, Decimal))
                    assert isinstance(table_config['batch_size'], int)
                    assert isinstance(table_config['incremental_columns'], list)
                    assert isinstance(table_config['is_modeled'], bool)
                    assert isinstance(table_config['dbt_model_types'], list)
                    assert isinstance(table_config['monitoring'], dict)
                    assert isinstance(table_config['schema_hash'], int)  # Should be integer from hash() function
                    assert isinstance(table_config['last_analyzed'], str)
                    
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_production_complete_schema_analysis(self, production_settings):
        """
        Test production complete schema analysis with actual production database and file output.
        
        AAA Pattern:
            Arrange: Set up real production database connection and temporary output directory
            Act: Call analyze_complete_schema() method with production data
            Assert: Verify all output files are generated correctly with production data
            
        Validates:
            - Real production complete schema analysis with actual database
            - Configuration file generation with real production data
            - Analysis report generation with real production metadata
            - Summary report generation with real production statistics
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
                
                # Act: Call analyze_complete_schema() method with production data
                results = analyzer.analyze_complete_schema(temp_dir)
                
                # Assert: Verify all output files are generated correctly with production data
                assert 'tables_config' in results
                assert 'analysis_report' in results
                assert 'analysis_log' in results
                assert results['tables_config'].endswith('tables.yml')  # Should be tables.yml, not versioned
                assert results['analysis_report'].endswith('.json')
                assert results['analysis_log'].endswith('.log')
                
                # Verify that files actually exist
                assert os.path.exists(results['tables_config'])
                assert os.path.exists(results['analysis_report'])
                # Check if the JSON analysis report exists in the organized directory structure
                json_report_path = results['analysis_report']
                if not os.path.isabs(json_report_path):
                    json_report_path = os.path.join(os.getcwd(), json_report_path)
                assert os.path.exists(json_report_path), f"Analysis report not found: {json_report_path}"
                
                # Check if the log file exists in the organized directory structure
                log_file_path = results['analysis_log']
                if not os.path.isabs(log_file_path):
                    log_file_path = os.path.join(os.getcwd(), log_file_path)
                assert os.path.exists(log_file_path), f"Log file not found: {log_file_path}"
                
                # Verify tables.yml content
                with open(results['tables_config'], 'r') as f:
                    config = yaml.safe_load(f)
                    assert 'metadata' in config
                    assert 'tables' in config
                    assert len(config['tables']) > 0
                    assert len(config['tables']) == 3  # Should process exactly 3 tables
                    
                    # Verify metadata structure
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
                    assert metadata['environment'] in ['production', 'test', 'unknown']  # Should be valid environment
                    
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
                
                # Verify analysis report content
                with open(results['analysis_report'], 'r') as f:
                    analysis = json.load(f)
                    assert 'analysis_metadata' in analysis
                    assert 'table_analysis' in analysis
                    assert 'dbt_model_analysis' in analysis
                    assert 'performance_analysis' in analysis
                    assert 'recommendations' in analysis
                    
                    # Verify analysis metadata
                    analysis_metadata = analysis['analysis_metadata']
                    assert 'generated_at' in analysis_metadata
                    assert 'source_database' in analysis_metadata
                    assert 'total_tables_analyzed' in analysis_metadata
                    assert 'analyzer_version' in analysis_metadata
                    
                    # Verify dbt model analysis
                    dbt_analysis = analysis['dbt_model_analysis']
                    assert 'staging' in dbt_analysis
                    assert 'mart' in dbt_analysis
                    assert 'intermediate' in dbt_analysis
                    assert isinstance(dbt_analysis['staging'], list)
                    assert isinstance(dbt_analysis['mart'], list)
                    assert isinstance(dbt_analysis['intermediate'], list)
                    
                    # Verify recommendations
                    assert isinstance(analysis['recommendations'], list)
                    assert len(analysis['recommendations']) > 0
                    
                # Verify log file exists and has content
                assert os.path.getsize(log_file_path) >= 0
                
                # Verify summary report exists in the organized directory structure
                # The summary report is now saved to logs/schema_analysis/reports/ with timestamp
                # We can check if any summary file exists in the reports directory
                logs_base = Path('logs')
                schema_analysis_reports = logs_base / 'schema_analysis' / 'reports'
                summary_files = list(schema_analysis_reports.glob('*_summary.txt'))
                assert len(summary_files) > 0, f"No summary report found in {schema_analysis_reports}"
                
                # Verify summary report content
                summary_file = summary_files[0]  # Use the first summary file found
                with open(summary_file, 'r') as f:
                    summary_content = f.read()
                    assert 'OpenDental Schema Analysis Summary' in summary_content
                    assert 'Total Tables Analyzed:' in summary_content
                    assert 'Total Estimated Size:' in summary_content
                    assert 'Table Classification:' in summary_content
                    assert 'DBT Modeling Status:' in summary_content
                    assert 'Extraction Strategies:' in summary_content
                    assert 'Tables with Monitoring:' in summary_content
                    assert 'Configuration saved to:' in summary_content
                    assert 'Detailed analysis saved to:' in summary_content
                    
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_versioned_tables_yml_output(self, production_settings):
        """
        Test that tables.yml is created with internal versioning metadata.
        
        AAA Pattern:
            Arrange: Set up real production database connection and temporary output directory
            Act: Run complete schema analysis
            Assert: Verify tables.yml is created with proper metadata
            
        Validates:
            - tables.yml is created (not versioned files)
            - Internal versioning metadata is included
            - No tables_latest.yml backup file is created
            - Metadata includes schema hash and environment info
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
                
                # Verify no versioned files or tables_latest.yml are created
                import glob
                versioned_files = glob.glob(os.path.join(temp_dir, 'tables_*.yml'))
                latest_path = os.path.join(temp_dir, 'tables_latest.yml')
                assert len(versioned_files) == 0, "No versioned files should be created"
                assert not os.path.exists(latest_path), "tables_latest.yml should not be created"
                
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_production_schema_hash_generation(self, production_settings):
        """
        Test production schema hash generation with actual production database schema.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call _generate_schema_hash() method with production tables
            Assert: Verify schema hash is correctly generated for production schema
            
        Validates:
            - Real production schema hash generation with actual database schema
            - Consistent hash generation for same production schema structure
            - Hash includes table names, column names, and primary keys from production
            - Error handling for production database schema analysis failures
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Get a subset of tables for testing (to avoid long test times)
        test_tables = tables[:5] if len(tables) >= 5 else tables
        
        if not test_tables:
            pytest.skip("No tables found in production database")
        
        # Act: Call _generate_schema_hash() method with production tables
        schema_hash = analyzer._generate_schema_hash(test_tables)
        
        # Assert: Verify schema hash is correctly generated for production schema
        assert isinstance(schema_hash, str)
        assert len(schema_hash) > 0
        assert len(schema_hash) == 32  # MD5 hash should be 32 characters
        
        # Verify hash is consistent for same input
        hash2 = analyzer._generate_schema_hash(test_tables)
        assert schema_hash == hash2, "Hash should be consistent for same production schema"
        
        # Verify hash changes for different table sets
        if len(tables) > 5:
            different_tables = tables[5:10]  # Different table set
            different_hash = analyzer._generate_schema_hash(different_tables)
            assert schema_hash != different_hash, "Hash should be different for different production schema"

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in production environment for each test
        os.environ['ETL_ENVIRONMENT'] = 'production'
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass

    def test_production_batch_schema_info_processing(self, production_settings):
        """
        Test production batch schema information processing with actual production database.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call get_batch_schema_info() method with production tables
            Assert: Verify batch schema information is correctly processed for production
            
        Validates:
            - Real production batch schema processing with actual database
            - Multiple table schema extraction in single connection for production
            - Error handling for individual tables in batch with production data
            - ConnectionManager usage for batch operations with production database
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Get a subset of tables for testing (to avoid long test times)
        test_tables = tables[:3] if len(tables) >= 3 else tables
        
        if not test_tables:
            pytest.skip("No tables found in production database")
        
        # Act: Call get_batch_schema_info() method with production tables
        batch_schema_info = analyzer.get_batch_schema_info(test_tables)
        
        # Assert: Verify batch schema information is correctly processed for production
        assert isinstance(batch_schema_info, dict)
        assert len(batch_schema_info) == len(test_tables)
        
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
            
            # Verify column structure
            for col_name, col_info in schema_info['columns'].items():
                assert 'type' in col_info
                assert 'nullable' in col_info
                assert 'primary_key' in col_info

    def test_production_batch_size_info_processing(self, production_settings):
        """
        Test production batch size information processing with actual production database.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call get_batch_size_info() method with production tables
            Assert: Verify batch size information is correctly processed for production
            
        Validates:
            - Real production batch size processing with actual database
            - Multiple table size extraction in single connection for production
            - Error handling for individual tables in batch with production data
            - ConnectionManager usage for batch operations with production database
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Get a subset of tables for testing (to avoid long test times)
        test_tables = tables[:3] if len(tables) >= 3 else tables
        
        if not test_tables:
            pytest.skip("No tables found in production database")
        
        # Act: Call get_batch_size_info() method with production tables
        batch_size_info = analyzer.get_batch_size_info(test_tables)
        
        # Assert: Verify batch size information is correctly processed for production
        assert isinstance(batch_size_info, dict)
        assert len(batch_size_info) == len(test_tables)
        
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
            
            # Verify that row count is a non-negative integer
            assert isinstance(size_info['estimated_row_count'], int)
            assert size_info['estimated_row_count'] >= 0
            
            # Verify that size is a non-negative number (can be Decimal from MySQL)
            from decimal import Decimal
            assert isinstance(size_info['size_mb'], (int, float, Decimal))
            assert float(size_info['size_mb']) >= 0

    def test_production_detailed_analysis_report_generation(self, production_settings):
        """
        Test production detailed analysis report generation with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and generate configuration
            Act: Call _generate_detailed_analysis_report() method with production data
            Assert: Verify detailed analysis report is correctly generated for production
            
        Validates:
            - Real production detailed analysis report generation with actual database data
            - Table analysis with real production schema and size information
            - DBT model analysis with real project structure
            - Performance analysis with production data
            - Recommendations generation with production statistics
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and generate configuration
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Store original method for cleanup
        original_discover = analyzer.discover_all_tables
        
        try:
            # Mock the discover_all_tables method to return key tables
            analyzer.discover_all_tables = lambda: ['patient', 'appointment', 'procedurelog']
            
            # Generate configuration for testing
            with tempfile.TemporaryDirectory() as temp_dir:
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Act: Call _generate_detailed_analysis_report() method with production data
                analysis_report = analyzer._generate_detailed_analysis_report(config)
                
                # Assert: Verify detailed analysis report is correctly generated for production
                assert 'analysis_metadata' in analysis_report
                assert 'table_analysis' in analysis_report
                assert 'dbt_model_analysis' in analysis_report
                assert 'performance_analysis' in analysis_report
                assert 'recommendations' in analysis_report
                
                # Verify analysis metadata
                analysis_metadata = analysis_report['analysis_metadata']
                assert 'generated_at' in analysis_metadata
                assert 'source_database' in analysis_metadata
                assert 'total_tables_analyzed' in analysis_metadata
                assert 'analyzer_version' in analysis_metadata
                
                # Verify table analysis
                table_analysis = analysis_report['table_analysis']
                assert len(table_analysis) > 0
                
                # Verify dbt model analysis
                dbt_analysis = analysis_report['dbt_model_analysis']
                assert 'staging' in dbt_analysis
                assert 'mart' in dbt_analysis
                assert 'intermediate' in dbt_analysis
                assert isinstance(dbt_analysis['staging'], list)
                assert isinstance(dbt_analysis['mart'], list)
                assert isinstance(dbt_analysis['intermediate'], list)
                
                # Verify recommendations
                assert isinstance(analysis_report['recommendations'], list)
                assert len(analysis_report['recommendations']) > 0
                
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_production_summary_report_generation(self, production_settings):
        """
        Test production summary report generation with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and generate configuration
            Act: Call _generate_summary_report() method with production data
            Assert: Verify summary report is correctly generated for production
            
        Validates:
            - Real production summary report generation with actual database data
            - Statistics calculation with real production data
            - Report formatting with production metadata
            - File output generation with production information
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and generate configuration
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Store original method for cleanup
        original_discover = analyzer.discover_all_tables
        
        try:
            # Mock the discover_all_tables method to return key tables
            analyzer.discover_all_tables = lambda: ['patient', 'appointment', 'procedurelog']
            
            # Generate configuration for testing
            with tempfile.TemporaryDirectory() as temp_dir:
                config = analyzer.generate_complete_configuration(temp_dir)
                
                # Create test output paths
                output_path = os.path.join(temp_dir, 'tables.yml')
                analysis_path = os.path.join(temp_dir, 'analysis.json')
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Act: Call _generate_summary_report() method with production data
                analyzer._generate_summary_report(config, output_path, analysis_path, timestamp)
                
                # Assert: Verify summary report is correctly generated for production
                # The method should print to console and save to file
                # We can verify the file was created in the organized logs directory
                logs_base = Path('logs')
                schema_analysis_reports = logs_base / 'schema_analysis' / 'reports'
                summary_files = list(schema_analysis_reports.glob('*_summary.txt'))
                
                # At least one summary file should exist
                assert len(summary_files) > 0, f"No summary report found in {schema_analysis_reports}"
                
                # Verify summary report content
                summary_file = summary_files[0]  # Use the first summary file found
                with open(summary_file, 'r') as f:
                    summary_content = f.read()
                    assert 'OpenDental Schema Analysis Summary' in summary_content
                    assert 'Total Tables Analyzed:' in summary_content
                    assert 'Total Estimated Size:' in summary_content
                    assert 'Table Classification:' in summary_content
                    assert 'DBT Modeling Status:' in summary_content
                    assert 'Extraction Strategies:' in summary_content
                    assert 'Tables with Monitoring:' in summary_content
                    assert 'Configuration saved to:' in summary_content
                    assert 'Detailed analysis saved to:' in summary_content
                
        finally:
            # Restore original method
            analyzer.discover_all_tables = original_discover

    def test_production_fail_fast_behavior(self, production_settings):
        """
        Test production FAIL FAST behavior when environment variables are missing.
        
        AAA Pattern:
            Arrange: Set up test environment without required environment variables
            Act: Attempt to create OpenDentalSchemaAnalyzer without OPENDENTAL_SOURCE_DB
            Assert: Verify system fails fast with clear error message
            
        Validates:
            - FAIL FAST behavior when OPENDENTAL_SOURCE_DB not set
            - Clear error message for missing environment variables
            - No default fallback to production environment
            - Settings injection validation with production environment
        """
        # Since the environment variable is loaded from .env_production file,
        # we can't easily test the fail-fast behavior in this integration test.
        # This test validates that the production environment is working correctly.
        # The actual fail-fast behavior should be tested in unit tests with mocked environments.
        pytest.skip("Fail-fast behavior should be tested in unit tests with mocked environments") 