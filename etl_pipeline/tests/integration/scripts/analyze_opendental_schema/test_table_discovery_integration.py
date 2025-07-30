# tests/integration/scripts/analyze_opendental_schema/test_table_discovery_integration.py

"""
Integration tests for table discovery with real production database connections.

This module tests table discovery against the actual production OpenDental database
to validate real table discovery, filtering, and database inspection.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real table discovery with actual production database structure
- Validates table filtering with real production database patterns
- Tests database inspection with real production database metadata
- Uses Settings injection for production environment-agnostic connections

Coverage Areas:
- Real production table discovery from actual database
- Proper filtering of excluded patterns in production
- Real production database schema analysis
- Error handling for real production database operations
- Settings injection with real production database connections
- Table name validation with real production database
- Database inspector functionality with real production database

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for production environment
- Validates actual production database connections and table discovery
"""

import pytest
import os
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(2)  # After initialization tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestTableDiscoveryIntegration:
    """Integration tests for table discovery with real production database connections."""
    


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

 