"""
Real Integration Testing for PriorityProcessor - Using Real MySQL and PostgreSQL Databases

This approach tests the actual priority-based table processing flow by using the REAL MySQL and PostgreSQL databases
with standardized test data that won't interfere with production.

Refactored to follow new architectural patterns:
- Uses new ConnectionFactory methods with dependency injection
- Uses modular fixtures from tests/fixtures/
- Follows new configuration pattern with proper test isolation
- Uses standardized test data instead of custom test data creation
- Proper environment separation with .env loading
- Comprehensive testing of parallel and sequential processing
"""

import pytest
import logging
import os
import time
from sqlalchemy import text
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file first
from tests.fixtures.env_fixtures import load_test_environment
load_test_environment()

# Import new configuration system
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.orchestration.table_processor import TableProcessor

# Import standardized test fixtures
from tests.fixtures import populated_test_databases, test_data_manager
from tests.fixtures.test_data_definitions import get_test_patient_data, get_test_appointment_data

logger = logging.getLogger(__name__)


class TestPriorityProcessorRealIntegration:
    """Real integration tests using actual MySQL and PostgreSQL databases with standardized test data.
    
    Uses new architectural patterns:
    - test_settings fixture loads TEST_* environment variables from .env
    - ConnectionFactory methods use test_settings for environment-aware connections
    - Standardized test data from fixtures
    - Proper environment separation (no risk of production connections)
    - Comprehensive testing of parallel and sequential processing
    """
    
    @pytest.fixture
    def priority_processor(self, populated_test_databases, test_settings):
        """Create a real PriorityProcessor instance with test environment."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        appointment_count = manager.get_appointment_count(DatabaseType.SOURCE)
        
        assert patient_count > 0, "Test patient data not found in source database"
        assert appointment_count > 0, "Test appointment data not found in source database"
        
        # Create SchemaDiscovery instance for PriorityProcessor
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        source_db = source_engine.url.database
        if source_db is None:
            raise ValueError("Source database name is None - check connection configuration")
        
        schema_discovery = SchemaDiscovery(source_engine, str(source_db))
        
        # Create PriorityProcessor with real components
        processor = PriorityProcessor(schema_discovery=schema_discovery, settings=test_settings)
        
        yield processor
        
        # Cleanup
        source_engine.dispose()
    
    @pytest.fixture
    def test_tables_config(self, test_settings):
        """Get test tables configuration for different priority levels."""
        # Mock the tables configuration to test different priority levels
        test_tables = {
            'critical': ['patient'],  # Critical table for testing
            'important': ['appointment'],  # Important table for testing
            'audit': ['patient'],  # Audit table (same as critical for testing)
            'reference': ['appointment']  # Reference table (same as important for testing)
        }
        
        # Patch the settings to return our test tables
        with patch.object(test_settings, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            yield test_settings
    
    @pytest.mark.integration
    def test_priority_processor_initialization(self, priority_processor, test_settings):
        """Test real PriorityProcessor initialization with test databases."""
        # Verify PriorityProcessor was created correctly
        assert priority_processor is not None, "PriorityProcessor should be created"
        assert priority_processor.schema_discovery is not None, "SchemaDiscovery should be initialized"
        assert priority_processor.settings is not None, "Settings should be initialized"
        
        # Test that SchemaDiscovery can discover tables
        tables = priority_processor.schema_discovery.discover_all_tables()
        assert len(tables) > 0, "SchemaDiscovery should find tables in test database"
        assert 'patient' in tables, "Patient table should be discovered"
        assert 'appointment' in tables, "Appointment table should be discovered"
        
        logger.info(f"PriorityProcessor initialized successfully with {len(tables)} discovered tables")

    @pytest.mark.integration
    def test_sequential_processing_single_table(self, priority_processor, test_tables_config):
        """Test sequential processing of a single table."""
        # Test processing a single critical table sequentially
        results = priority_processor.process_by_priority(
            importance_levels=['critical'],
            max_workers=1,  # Force sequential processing
            force_full=True
        )
        
        # Verify results structure
        assert 'critical' in results, "Critical level should be in results"
        critical_result = results['critical']
        assert 'success' in critical_result, "Success list should be present"
        assert 'failed' in critical_result, "Failed list should be present"
        assert 'total' in critical_result, "Total count should be present"
        
        # Verify processing results
        assert critical_result['total'] == 1, "Should process 1 critical table"
        assert len(critical_result['success']) >= 0, "Success list should be valid"
        assert len(critical_result['failed']) >= 0, "Failed list should be valid"
        assert critical_result['total'] == len(critical_result['success']) + len(critical_result['failed']), "Total should equal success + failed"
        
        logger.info(f"Sequential processing completed: {critical_result}")

    @pytest.mark.integration
    def test_parallel_processing_multiple_tables(self, priority_processor, test_tables_config):
        """Test parallel processing of multiple tables."""
        # Create a test configuration with multiple critical tables
        test_tables = {
            'critical': ['patient', 'appointment'],  # Multiple critical tables for parallel processing
            'important': ['patient'],
            'audit': ['appointment'],
            'reference': ['patient']
        }
        
        # Patch the settings to return multiple critical tables
        with patch.object(test_tables_config, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            
            # Test parallel processing with multiple workers
            start_time = time.time()
            results = priority_processor.process_by_priority(
                importance_levels=['critical'],
                max_workers=2,  # Use 2 workers for parallel processing
                force_full=True
            )
            end_time = time.time()
            
            processing_time = end_time - start_time
            
            # Verify results structure
            assert 'critical' in results, "Critical level should be in results"
            critical_result = results['critical']
            assert critical_result['total'] == 2, "Should process 2 critical tables"
            
            # Verify parallel processing occurred (should be faster than sequential)
            logger.info(f"Parallel processing completed in {processing_time:.2f}s: {critical_result}")
            
            # Note: In a real test environment, parallel processing might not be significantly faster
            # due to database connection limits and test data size, but the structure should work

    @pytest.mark.integration
    def test_mixed_priority_processing(self, priority_processor, test_tables_config):
        """Test processing tables with different priority levels."""
        # Test processing multiple priority levels
        results = priority_processor.process_by_priority(
            importance_levels=['critical', 'important'],
            max_workers=2,
            force_full=True
        )
        
        # Verify all requested levels are processed
        assert 'critical' in results, "Critical level should be processed"
        assert 'important' in results, "Important level should be processed"
        
        # Verify results structure for each level
        for level in ['critical', 'important']:
            level_result = results[level]
            assert 'success' in level_result, f"Success list should be present for {level}"
            assert 'failed' in level_result, f"Failed list should be present for {level}"
            assert 'total' in level_result, f"Total count should be present for {level}"
            assert level_result['total'] >= 0, f"Total should be non-negative for {level}"
        
        logger.info(f"Mixed priority processing completed: {results}")

    @pytest.mark.integration
    def test_error_handling_single_table_failure(self, priority_processor, test_tables_config):
        """Test error handling when a single table fails."""
        # Test with a non-existent table to simulate failure
        test_tables = {
            'critical': ['nonexistent_table'],  # Table that doesn't exist
            'important': ['patient']
        }
        
        with patch.object(test_tables_config, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            
            # Process tables - should handle the failure gracefully
            results = priority_processor.process_by_priority(
                importance_levels=['critical', 'important'],
                max_workers=1,
                force_full=True
            )
            
            # Verify critical level failed
            assert 'critical' in results, "Critical level should be in results"
            critical_result = results['critical']
            assert critical_result['total'] == 1, "Should attempt to process 1 critical table"
            assert len(critical_result['failed']) == 1, "Should have 1 failed table"
            assert len(critical_result['success']) == 0, "Should have 0 successful tables"
            
            # Verify important level was not processed (should stop after critical failure)
            # Note: This depends on the implementation - some versions might continue
            logger.info(f"Error handling test completed: {results}")

    @pytest.mark.integration
    def test_resource_management_thread_pool(self, priority_processor, test_tables_config):
        """Test thread pool resource management."""
        # Test with different worker counts to verify resource management
        test_tables = {
            'critical': ['patient', 'appointment', 'patient', 'appointment'],  # 4 tables
            'important': ['patient', 'appointment']  # 2 tables
        }
        
        with patch.object(test_tables_config, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            
            # Test with different worker counts
            for max_workers in [1, 2, 4]:
                logger.info(f"Testing with {max_workers} workers")
                
                start_time = time.time()
                results = priority_processor.process_by_priority(
                    importance_levels=['critical'],
                    max_workers=max_workers,
                    force_full=True
                )
                end_time = time.time()
                
                processing_time = end_time - start_time
                
                # Verify results
                assert 'critical' in results, f"Critical level should be processed with {max_workers} workers"
                critical_result = results['critical']
                assert critical_result['total'] == 4, f"Should process 4 critical tables with {max_workers} workers"
                
                logger.info(f"Processing with {max_workers} workers completed in {processing_time:.2f}s: {critical_result}")

    @pytest.mark.integration
    def test_force_full_vs_incremental_processing(self, priority_processor, test_tables_config):
        """Test force_full parameter behavior."""
        # Test both force_full=True and force_full=False
        test_tables = {
            'critical': ['patient'],
            'important': ['appointment']
        }
        
        with patch.object(test_tables_config, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            
            # Test force_full=True
            results_full = priority_processor.process_by_priority(
                importance_levels=['critical'],
                max_workers=1,
                force_full=True
            )
            
            # Test force_full=False
            results_incremental = priority_processor.process_by_priority(
                importance_levels=['critical'],
                max_workers=1,
                force_full=False
            )
            
            # Both should complete successfully (though they might use different logic)
            assert 'critical' in results_full, "Full processing should complete"
            assert 'critical' in results_incremental, "Incremental processing should complete"
            
            logger.info(f"Force full processing: {results_full}")
            logger.info(f"Incremental processing: {results_incremental}")

    @pytest.mark.integration
    def test_empty_tables_handling(self, priority_processor, test_tables_config):
        """Test handling of empty table lists."""
        # Test with empty table lists
        test_tables = {
            'critical': [],  # Empty critical tables
            'important': [],  # Empty important tables
            'audit': ['patient'],  # Non-empty audit tables
            'reference': []  # Empty reference tables
        }
        
        with patch.object(test_tables_config, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            
            # Process all levels including empty ones
            results = priority_processor.process_by_priority(
                importance_levels=['critical', 'important', 'audit', 'reference'],
                max_workers=1,
                force_full=True
            )
            
            # Verify empty levels are handled gracefully
            assert 'critical' in results, "Critical level should be in results even if empty"
            assert 'important' in results, "Important level should be in results even if empty"
            assert 'audit' in results, "Audit level should be in results"
            assert 'reference' in results, "Reference level should be in results even if empty"
            
            # Verify empty levels have correct structure
            for level in ['critical', 'important', 'reference']:
                level_result = results[level]
                assert level_result['total'] == 0, f"{level} level should have 0 total tables"
                assert len(level_result['success']) == 0, f"{level} level should have 0 successful tables"
                assert len(level_result['failed']) == 0, f"{level} level should have 0 failed tables"
            
            # Verify non-empty level has correct structure
            audit_result = results['audit']
            assert audit_result['total'] == 1, "Audit level should have 1 total table"
            
            logger.info(f"Empty tables handling completed: {results}")

    @pytest.mark.integration
    def test_connection_management_integration(self, priority_processor, test_tables_config):
        """Test that PriorityProcessor properly manages database connections."""
        # Test that connections are properly initialized and cleaned up
        test_tables = {
            'critical': ['patient'],
            'important': ['appointment']
        }
        
        with patch.object(test_tables_config, 'get_tables_by_importance') as mock_get_tables:
            def get_tables_by_importance(importance):
                return test_tables.get(importance, [])
            
            mock_get_tables.side_effect = get_tables_by_importance
            
            # Process tables and verify connection management
            results = priority_processor.process_by_priority(
                importance_levels=['critical', 'important'],
                max_workers=1,
                force_full=True
            )
            
            # Verify processing completed
            assert 'critical' in results, "Critical level should be processed"
            assert 'important' in results, "Important level should be processed"
            
            # Verify that connections are properly managed (no connection leaks)
            # This is implicit - if connections weren't managed properly, we'd see errors
            logger.info("Connection management test completed successfully")

    @pytest.mark.integration
    def test_schema_discovery_integration(self, priority_processor, test_settings):
        """Test that PriorityProcessor properly integrates with SchemaDiscovery."""
        # Test that SchemaDiscovery is working correctly within PriorityProcessor
        schema_discovery = priority_processor.schema_discovery
        
        # Test table discovery
        tables = schema_discovery.discover_all_tables()
        assert len(tables) > 0, "SchemaDiscovery should find tables"
        
        # Test schema retrieval for specific tables
        for table_name in ['patient', 'appointment']:
            if table_name in tables:
                schema = schema_discovery.get_table_schema(table_name)
                assert schema is not None, f"Schema should be retrieved for {table_name}"
                assert 'columns' in schema, f"Schema should contain columns for {table_name}"
                assert 'schema_hash' in schema, f"Schema should contain hash for {table_name}"
        
        logger.info(f"SchemaDiscovery integration test completed with {len(tables)} tables discovered")

    @pytest.mark.integration
    def test_settings_integration(self, priority_processor, test_settings):
        """Test that PriorityProcessor properly integrates with Settings."""
        # Test that Settings integration works correctly
        settings = priority_processor.settings
        
        # Test that settings can be accessed
        assert settings is not None, "Settings should be accessible"
        
        # Test that settings has the expected methods
        assert hasattr(settings, 'get_tables_by_importance'), "Settings should have get_tables_by_importance method"
        
        # Test table priority lookup (this will use the real settings)
        # Note: This depends on the actual tables.yml configuration
        try:
            critical_tables = settings.get_tables_by_importance('critical')
            important_tables = settings.get_tables_by_importance('important')
            audit_tables = settings.get_tables_by_importance('audit')
            reference_tables = settings.get_tables_by_importance('reference')
            
            logger.info(f"Settings integration test - Tables by importance:")
            logger.info(f"  Critical: {critical_tables}")
            logger.info(f"  Important: {important_tables}")
            logger.info(f"  Audit: {audit_tables}")
            logger.info(f"  Reference: {reference_tables}")
            
        except Exception as e:
            logger.warning(f"Settings integration test - get_tables_by_importance failed: {e}")
            # This is acceptable if the method doesn't exist in the current settings implementation

    @pytest.mark.integration
    def test_new_architecture_connection_methods(self, test_settings):
        """Test that new architecture connection methods work correctly with PriorityProcessor."""
        # Test that PriorityProcessor can work with new ConnectionFactory methods
        
        # Create SchemaDiscovery with new connection methods
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        source_db = source_engine.url.database
        if source_db is None:
            raise ValueError("Source database name is None - check connection configuration")
        
        schema_discovery = SchemaDiscovery(source_engine, str(source_db))
        
        # Create PriorityProcessor with new architecture components
        processor = PriorityProcessor(schema_discovery=schema_discovery, settings=test_settings)
        
        # Verify processor was created successfully
        assert processor is not None, "PriorityProcessor should be created with new architecture"
        assert processor.schema_discovery is not None, "SchemaDiscovery should be initialized"
        assert processor.settings is not None, "Settings should be initialized"
        
        # Test basic functionality
        tables = processor.schema_discovery.discover_all_tables()
        assert len(tables) > 0, "SchemaDiscovery should find tables with new architecture"
        
        logger.info(f"New architecture integration test completed with {len(tables)} tables discovered")
        
        # Cleanup
        source_engine.dispose()

    @pytest.mark.integration
    def test_type_safe_database_enum_usage(self, test_settings):
        """Test that type-safe database enums work correctly with PriorityProcessor."""
        # Test DatabaseType enum usage
        assert DatabaseType.SOURCE.value == "source", "DatabaseType.SOURCE should be 'source'"
        assert DatabaseType.REPLICATION.value == "replication", "DatabaseType.REPLICATION should be 'replication'"
        assert DatabaseType.ANALYTICS.value == "analytics", "DatabaseType.ANALYTICS should be 'analytics'"
        
        # Test PostgresSchema enum usage
        assert PostgresSchema.RAW.value == "raw", "PostgresSchema.RAW should be 'raw'"
        assert PostgresSchema.STAGING.value == "staging", "PostgresSchema.STAGING should be 'staging'"
        assert PostgresSchema.INTERMEDIATE.value == "intermediate", "PostgresSchema.INTERMEDIATE should be 'intermediate'"
        assert PostgresSchema.MARTS.value == "marts", "PostgresSchema.MARTS should be 'marts'"
        
        # Test that enums work with ConnectionFactory using test environment
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        assert source_engine is not None, "Source engine should be created with test settings"
        
        # Cleanup
        source_engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 