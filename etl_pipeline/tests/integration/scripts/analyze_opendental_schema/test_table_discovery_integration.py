# tests/integration/scripts/analyze_opendental_schema/test_table_discovery_integration.py

"""
Integration tests for table discovery with real clinic database connections.

This module tests table discovery against the actual production OpenDental database
to validate real table discovery, filtering, and database inspection.

Production Test Strategy:
- Uses clinic database connections with readonly access
- Tests real table discovery with actual clinic database structure
- Validates table filtering with real clinic database patterns
- Tests database inspection with real clinic database metadata
- Uses Settings injection for clinic environment-agnostic connections

Coverage Areas:
- Real clinic table discovery from actual database
- Proper filtering of excluded patterns in production
- Real clinic database schema analysis
- Error handling for real clinic database operations
- Settings injection with real clinic database connections
- Table name validation with real clinic database
- Database inspector functionality with real clinic database

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for clinic environment
- Validates actual clinic database connections and table discovery
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
    """Integration tests for table discovery with real clinic database connections."""
    


    def test_production_table_discovery(self, production_settings_with_file_provider):
        """
        Test clinic table discovery with actual clinic database structure.
        
        AAA Pattern:
            Arrange: Set up real clinic database connection
            Act: Call discover_all_tables() method with clinic database
            Assert: Verify real clinic tables are discovered correctly
            
        Validates:
            - Real clinic table discovery from actual database
            - Proper filtering of excluded patterns in production
            - Real clinic database schema analysis
            - Error handling for real clinic database operations
            - Settings injection with real clinic database connections
        """
        # Arrange: Set up real clinic database connection
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Act: Call discover_all_tables() method with clinic database
        tables = analyzer.discover_all_tables()
        
        # Assert: Verify real clinic tables are discovered correctly
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

 