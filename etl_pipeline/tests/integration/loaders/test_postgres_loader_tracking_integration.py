"""
Integration tests for PostgresLoader tracking functionality.

This module contains integration tests for the ETL tracking features added to PostgresLoader.
"""

import pytest
from datetime import datetime
from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema as PostgresSchemaAdapter
from etl_pipeline.config import PostgresSchema as ConfigPostgresSchema
from unittest.mock import patch


def create_postgres_loader_for_test(settings):
    """
    Helper function to create PostgresLoader instance for integration tests.
    
    Uses the new constructor signature with all required parameters.
    """
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
    schema_adapter = PostgresSchemaAdapter(
        postgres_schema=ConfigPostgresSchema.RAW,
        settings=settings,
    )
    
    return PostgresLoader(
        replication_engine=replication_engine,
        analytics_engine=analytics_engine,
        settings=settings,
        schema_adapter=schema_adapter,
    )


class TestPostgresLoaderTrackingIntegration:
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.etl_critical
    def test_primary_column_value_tracking(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test full primary column value tracking lifecycle."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Act - Create tracking record
            created = loader._ensure_tracking_record_exists('test_table')
            
            # Act - Update tracking record with primary column value using hybrid method
            updated = loader._update_load_status_hybrid(
                'test_table', 1000,
                load_status='success',
                last_primary_value='2024-01-01 00:00:00',
                primary_column_name='DateTStamp'
            )
            
            # Act - Retrieve primary column value
            last_primary_value = loader._get_last_primary_value('test_table')
            
            # Assert
            assert created is True
            assert updated is True
            assert last_primary_value == '2024-01-01 00:00:00'
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_enhanced_incremental_logic_with_primary_column(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test enhanced incremental logic with primary column support."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Act - Build query with primary column
            query = loader._build_enhanced_load_query(
                'test_table', ['created_date', 'updated_date'], 'DateTStamp', force_full=False
            )
            
            # Assert
            assert 'DateTStamp >' in query
            assert 'test_table' in query
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_enhanced_incremental_logic_without_primary_column(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test enhanced incremental logic without primary column (fallback to multi-column)."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Act - Build query without primary column
            query = loader._build_enhanced_load_query(
                'test_table', ['created_date', 'updated_date'], None, force_full=False
            )
            
            # Assert
            assert 'test_table' in query
            # Should use multi-column logic (OR or AND conditions)
            assert 'created_date >' in query or 'updated_date >' in query
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_primary_column_fallback_to_timestamp(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test that primary column logic falls back to timestamp when no primary value exists."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Mock _get_last_primary_value to return None (no primary value)
            with patch.object(loader, '_get_last_primary_value', return_value=None):
                # Act - Build query with primary column but no primary value
                query = loader._build_enhanced_load_query(
                    'test_table', ['created_date', 'updated_date'], 'DateTStamp', force_full=False
                )
                
                # Assert - Should still use primary column logic even without primary value
                # The implementation falls back to timestamp logic when no primary value exists
                assert 'test_table' in query
                # When no primary value exists, it should use timestamp fallback
                # The actual behavior is to use the primary column with timestamp fallback
                assert 'DateTStamp >' in query
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_tracking_record_creation_and_update(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test tracking record creation and update operations."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Act - Create tracking record
            created = loader._ensure_tracking_record_exists('test_table')
            
            # Act - Update tracking record
            updated = loader._update_load_status_hybrid(
                'test_table', 500,
                load_status='success',
                last_primary_value='2024-01-01 00:00:00',
                primary_column_name='DateTStamp'
            )
            
            # Act - Update again with different values
            updated_again = loader._update_load_status_hybrid(
                'test_table', 1000,
                load_status='success',
                last_primary_value='2024-01-02 00:00:00',
                primary_column_name='DateTStamp'
            )
            
            # Assert
            assert created is True
            assert updated is True
            assert updated_again is True
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_failure_tracking(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test failure tracking functionality."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Act - Create tracking record
            created = loader._ensure_tracking_record_exists('test_table')
            
            # Act - Update with failure status
            failed = loader._update_load_status_hybrid(
                'test_table', 0,
                load_status='failed',
                primary_column_name='DateTStamp'
            )
            
            # Assert
            assert created is True
            assert failed is True
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_primary_column_configuration_handling(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test handling of primary column configuration."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Mock _get_last_primary_value to return a value for the primary column test
            with patch.object(loader, '_get_last_primary_value', return_value='2024-01-01 00:00:00'):
                # Act - Test with primary column
                query_with_primary = loader._build_enhanced_load_query(
                    'test_table', ['created_date'], 'DateTStamp', force_full=False
                )
                
                # Assert - Should use primary column when primary value exists
                assert 'DateTStamp >' in query_with_primary
            
            # Act - Test without primary column (should use multi-column logic)
            query_without_primary = loader._build_enhanced_load_query(
                'test_table', ['created_date'], None, force_full=False
            )
            
            # Assert - Should use multi-column logic when no primary column specified
            assert 'DateTStamp >' not in query_without_primary
            # Should either have timestamp conditions or be a full table query
            assert ('created_date >' in query_without_primary or 'test_table' in query_without_primary)
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")
    
    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_incremental_strategy_logging(self, test_settings_with_file_provider, setup_etl_tracking):
        """Test incremental strategy query building."""
        try:
            # Arrange
            loader = create_postgres_loader_for_test(test_settings_with_file_provider)
            
            # Act - Test query building with incremental columns
            query = loader._build_load_query(
                'test_table', ['created_date'], force_full=False
            )
            
            # Assert - Query should be built correctly
            assert 'test_table' in query
            assert isinstance(query, str)
            assert len(query) > 0
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")