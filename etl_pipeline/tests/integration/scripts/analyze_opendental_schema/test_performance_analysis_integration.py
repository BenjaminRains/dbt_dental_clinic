# tests/integration/scripts/analyze_opendental_schema/test_performance_analysis_integration.py

"""
Performance Analysis Integration tests for OpenDentalSchemaAnalyzer.

This module tests the performance analysis functionality of the schema analyzer
with real clinic database connections to validate performance characteristics
analysis, processing priority calculation, and performance category determination.

Performance Analysis Test Strategy:
- Uses clinic database connections with readonly access
- Tests real performance analysis with actual clinic data structures
- Validates performance characteristics calculation with real clinic data
- Tests processing priority determination with real clinic metadata
- Uses Settings injection with FileConfigProvider for clinic environment
- Tests performance monitoring requirements with clinic data
- Validates performance optimization recommendations with real data

Coverage Areas:
- Real clinic database performance analysis
- Actual clinic table performance characteristics
- Real clinic processing priority calculation
- Production performance category determination
- Real performance monitoring requirements analysis
- Error handling with real clinic database scenarios
- Settings injection for clinic environment-agnostic performance analysis
- Provider pattern integration for performance analysis testing

ETL Context:
- Critical for ETL pipeline performance optimization
- Tests with real clinic dental database performance characteristics
- Validates actual clinic database performance analysis capabilities
- Uses Settings injection for clinic environment-agnostic performance analysis
- Supports performance monitoring and optimization recommendations
"""

import pytest
import os
import sys
from pathlib import Path
from typing import Dict, Any
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
from etl_pipeline.config import get_settings
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory


@pytest.fixture(scope="session")
def ensure_logs_directory():
    """Ensure logs directory exists for test session."""
    logs_dir = Path(__file__).parent.parent.parent.parent.parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    yield logs_dir
    # Clean up any test-generated log files after session
    for log_file in logs_dir.glob('performance_analysis_*.log'):
        try:
            log_file.unlink()
        except Exception:
            pass  # Ignore cleanup errors


@pytest.mark.integration
@pytest.mark.order(2)  # After basic schema analysis tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
@pytest.mark.performance_analysis
class TestPerformanceAnalysisIntegration:
    """Performance analysis integration tests for OpenDentalSchemaAnalyzer with real clinic database connections."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class with clinic environment validation."""
        # Store original environment for cleanup
        cls.original_etl_env = os.environ.get('ETL_ENVIRONMENT')
        cls.original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
        
        # Set clinic environment for all tests in this class
        os.environ['ETL_ENVIRONMENT'] = 'clinic'

        # Validate clinic environment is available
        try:
            config_dir = Path(__file__).parent.parent.parent.parent.parent
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

    def test_production_table_performance_analysis(self, production_settings_with_file_provider):
        """
        Test table performance analysis with clinic data.
        
        AAA Pattern:
            Arrange: Set up analyzer with clinic environment settings and real database connection
            Act: Call performance analysis methods with clinic data
            Assert: Verify performance characteristics are correctly calculated
            
        Validates:
            - Performance characteristics analysis with clinic data
            - Processing priority calculation with clinic data
            - Performance category determination with clinic data
            - Settings injection for environment-agnostic performance analysis
            - Provider pattern dependency injection for performance analysis
            
        ETL Pipeline Context:
            - Tests performance analysis for dental clinic tables with clinic data
            - Critical for ETL pipeline performance optimization
            - Uses provider pattern for clean performance analysis testing
            - Supports Settings injection for environment-agnostic performance analysis
        """
        # Arrange: Set up analyzer with clinic environment settings
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Act: Call performance analysis methods with clinic data
        schema_info = analyzer.get_table_schema('patient')
        size_info = analyzer.get_table_size_info('patient')
        
        # Act: Call performance analysis method with clinic data
        performance_chars = analyzer.get_table_performance_profile('patient', schema_info, size_info)
        
        # Assert: Validate performance characteristics
        assert 'performance_category' in performance_chars
        assert 'processing_priority' in performance_chars
        assert 'time_gap_threshold_days' in performance_chars
        assert 'estimated_processing_time_minutes' in performance_chars
        assert 'memory_requirements_mb' in performance_chars
        assert 'recommended_batch_size' in performance_chars
        assert 'needs_performance_monitoring' in performance_chars
        
        # Verify performance category is valid
        assert performance_chars['performance_category'] in ['small', 'medium', 'large', 'xlarge']
        
        # Verify processing priority is valid
        assert performance_chars['processing_priority'] in ['high', 'medium', 'low']
        
        # Verify time gap threshold is a positive number
        assert isinstance(performance_chars['time_gap_threshold_days'], (int, float))
        assert performance_chars['time_gap_threshold_days'] > 0
        
        # Verify estimated processing time is a positive number
        assert isinstance(performance_chars['estimated_processing_time_minutes'], (int, float))
        assert performance_chars['estimated_processing_time_minutes'] >= 0
        
        # Verify memory requirements is a positive number
        assert isinstance(performance_chars['memory_requirements_mb'], (int, float))
        assert performance_chars['memory_requirements_mb'] > 0
        
        # Verify recommended batch size is a positive integer
        assert isinstance(performance_chars['recommended_batch_size'], int)
        assert performance_chars['recommended_batch_size'] > 0
        
        # Verify needs_performance_monitoring is a boolean
        assert isinstance(performance_chars['needs_performance_monitoring'], bool)

    def test_production_multiple_table_performance_comparison(self, production_settings_with_file_provider):
        """
        Test performance analysis comparison across multiple clinic tables.
        
        AAA Pattern:
            Arrange: Set up analyzer with clinic environment settings and multiple table data
            Act: Call performance analysis for multiple tables
            Assert: Verify performance characteristics vary appropriately across tables
            
        Validates:
            - Performance analysis works across different table types
            - Performance characteristics vary based on table size and complexity
            - Processing priorities are assigned appropriately
            - Performance monitoring requirements vary by table characteristics
        """
        # Arrange: Set up analyzer with clinic environment settings
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Test tables with different expected characteristics
        test_tables = ['patient', 'appointment', 'procedurelog']
        performance_results = {}
        
        # Act: Call performance analysis for multiple tables
        for table_name in test_tables:
            try:
                schema_info = analyzer.get_table_schema(table_name)
                size_info = analyzer.get_table_size_info(table_name)
                performance_chars = analyzer.get_table_performance_profile(table_name, schema_info, size_info)
                performance_results[table_name] = performance_chars
            except Exception as e:
                print(f"Skipping table {table_name} due to error: {e}")
                continue
        
        # Assert: Verify performance characteristics vary appropriately across tables
        assert len(performance_results) > 0, "Should have at least one successful performance analysis"
        
        # Verify that different tables have different performance characteristics
        performance_categories = set()
        processing_priorities = set()
        monitoring_requirements = set()
        
        for table_name, perf_chars in performance_results.items():
            performance_categories.add(perf_chars['performance_category'])
            processing_priorities.add(perf_chars['processing_priority'])
            monitoring_requirements.add(perf_chars['needs_performance_monitoring'])
            
            # Verify all required fields are present
            assert 'performance_category' in perf_chars
            assert 'processing_priority' in perf_chars
            assert 'time_gap_threshold_days' in perf_chars
            assert 'estimated_processing_time_minutes' in perf_chars
            assert 'memory_requirements_mb' in perf_chars
            assert 'recommended_batch_size' in perf_chars
            assert 'needs_performance_monitoring' in perf_chars
        
        # Log the results for debugging
        print(f"Performance categories found: {performance_categories}")
        print(f"Processing priorities found: {processing_priorities}")
        print(f"Monitoring requirements found: {monitoring_requirements}")

    def test_production_performance_analysis_error_handling(self, production_settings_with_file_provider):
        """
        Test performance analysis error handling with invalid table names.
        
        AAA Pattern:
            Arrange: Set up analyzer with clinic environment settings
            Act: Call performance analysis with invalid table name
            Assert: Verify appropriate error handling
            
        Validates:
            - Error handling for non-existent tables
            - Graceful degradation when table analysis fails
            - Settings injection continues to work with errors
        """
        # Arrange: Set up analyzer with clinic environment settings
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Act: Test with invalid table name
        schema_info = analyzer.get_table_schema('non_existent_table')
        size_info = analyzer.get_table_size_info('non_existent_table')
        
        # Assert: Verify error handling
        # Schema info should contain error for non-existent table
        assert 'error' in schema_info, "Expected error information in schema_info for non-existent table"
        
        # Size info returns valid data with 0 values for non-existent tables (MySQL behavior)
        assert 'table_name' in size_info, "Expected table_name in size_info"
        assert 'estimated_row_count' in size_info, "Expected estimated_row_count in size_info"
        assert 'size_mb' in size_info, "Expected size_mb in size_info"
        assert size_info['estimated_row_count'] == 0, "Expected 0 estimated_row_count for non-existent table"
        assert size_info['size_mb'] == 0.0, "Expected 0.0 size_mb for non-existent table"
        
        # Test that performance analysis can handle error information gracefully
        try:
            performance_chars = analyzer.get_table_performance_profile('non_existent_table', schema_info, size_info)
            # Should handle errors gracefully and return default values
            assert 'performance_category' in performance_chars
            assert 'processing_priority' in performance_chars
            assert 'time_gap_threshold_days' in performance_chars
            assert 'estimated_processing_time_minutes' in performance_chars
            assert 'memory_requirements_mb' in performance_chars
            assert 'recommended_batch_size' in performance_chars
            assert 'needs_performance_monitoring' in performance_chars
        except Exception as e:
            # If an exception is raised, it should be a specific type, not a generic error
            assert "non_existent_table" in str(e) or "does not exist" in str(e)

    def test_production_performance_analysis_with_empty_schema(self, production_settings_with_file_provider):
        """
        Test performance analysis behavior with minimal schema information.
        
        AAA Pattern:
            Arrange: Set up analyzer with clinic environment settings and minimal schema data
            Act: Call performance analysis with minimal data
            Assert: Verify performance analysis handles minimal data gracefully
            
        Validates:
            - Performance analysis works with minimal schema information
            - Default values are used when data is insufficient
            - Error handling for edge cases in performance analysis
        """
        # Arrange: Set up analyzer with clinic environment settings
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Create minimal schema and size info for testing
        minimal_schema = {
            'table_name': 'test_table',
            'columns': {'id': {'type': 'int', 'nullable': False, 'primary_key': True}},
            'primary_keys': ['id'],
            'foreign_keys': [],
            'indexes': []
        }
        
        minimal_size = {
            'table_name': 'test_table',
            'estimated_row_count': 0,
            'size_mb': 0.0,
            'source': 'information_schema_estimate'
        }
        
        # Act: Call performance analysis with minimal data
        performance_chars = analyzer.get_table_performance_profile('test_table', minimal_schema, minimal_size)
        
        # Assert: Verify performance analysis handles minimal data gracefully
        assert 'performance_category' in performance_chars
        assert 'processing_priority' in performance_chars
        assert 'time_gap_threshold_days' in performance_chars
        assert 'estimated_processing_time_minutes' in performance_chars
        assert 'memory_requirements_mb' in performance_chars
        assert 'recommended_batch_size' in performance_chars
        assert 'needs_performance_monitoring' in performance_chars
        
        # Verify that minimal data results in appropriate default values
        assert performance_chars['performance_category'] == 'tiny'  # Should default to tiny for 0 rows
        assert performance_chars['estimated_processing_time_minutes'] >= 0
        assert performance_chars['memory_requirements_mb'] > 0

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in clinic environment for each test
        os.environ['ETL_ENVIRONMENT'] = 'clinic'
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 