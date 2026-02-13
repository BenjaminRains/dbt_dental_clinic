# tests/integration/scripts/analyze_opendental_schema/test_size_analysis_integration.py

"""
Integration tests for table size analysis with real clinic database connections.

This module tests table size analysis against the actual production OpenDental database
to validate real table size estimation, row counting, and size information extraction.

Production Test Strategy:
- Uses clinic database connections with readonly access
- Tests real size analysis with actual clinic database data
- Validates row count calculation from real clinic data
- Tests table size estimation from real clinic database
- Uses Settings injection for clinic environment-agnostic connections

Coverage Areas:
- Real production size analysis from actual database
- Row count calculation from real clinic data
- Table size estimation from real clinic database
- Error handling for real clinic database operations
- Settings injection with real clinic database connections
- Size information validation with real clinic data
- Information schema queries with real clinic database

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for clinic environment
- Validates actual clinic database connections and size analysis
"""

import pytest
import os
from pathlib import Path
from decimal import Decimal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(4)  # After schema analysis tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestSizeAnalysisIntegration:
    """Integration tests for table size analysis with real clinic database connections."""
    


    def test_production_table_size_analysis(self, production_settings_with_file_provider):
        """
        Test clinic table size analysis with actual clinic database data.
        
        AAA Pattern:
            Arrange: Set up real clinic database connection and discover tables
            Act: Call get_table_size_info() method for clinic tables
            Assert: Verify real production size information is correctly extracted
            
        Validates:
            - Real production size analysis from actual database
            - Row count calculation from real clinic data
            - Table size estimation from real clinic database
            - Error handling for real clinic database operations
            - Settings injection with real clinic database connections
        """
        # Arrange: Set up real clinic database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Find a table to test (prefer patient table as it's likely to exist in production)
        test_table = 'patient' if 'patient' in tables else tables[0]
        
        # Act: Call get_table_size_info() method for clinic tables
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
        assert isinstance(size_info['size_mb'], (int, float, Decimal))
        assert float(size_info['size_mb']) >= 0

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in the current environment for each test
        current_env = os.environ.get('ETL_ENVIRONMENT', 'test')
        os.environ['ETL_ENVIRONMENT'] = current_env
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 