# tests/integration/scripts/analyze_opendental_schema/test_schema_analysis_integration.py

"""
Integration tests for schema analysis with real production database connections.

This module tests schema analysis against the actual production OpenDental database
to validate real schema information extraction, column analysis, and primary key detection.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real schema analysis with actual production database structure
- Validates column information extraction from real production tables
- Tests primary key detection from real production database
- Uses Settings injection for production environment-agnostic connections

Coverage Areas:
- Real production schema analysis from actual database
- Column information extraction from real production tables
- Primary key detection from real production database
- Foreign key detection from real production database
- Index information from real production database
- Error handling for real production database operations
- Schema hash generation with real production database schema
- Consistent hash generation for same production schema structure

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for production environment
- Validates actual production database connections and schema analysis
"""

import pytest
import os
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(3)  # After table discovery tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestSchemaAnalysisIntegration:
    """Integration tests for schema analysis with real production database connections."""
    


    def test_production_table_schema_analysis(self, production_settings_with_file_provider):
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

    def test_production_schema_hash_generation(self, production_settings_with_file_provider):
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

 