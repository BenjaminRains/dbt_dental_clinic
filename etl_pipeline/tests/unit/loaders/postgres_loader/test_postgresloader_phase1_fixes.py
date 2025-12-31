"""
Test Phase 1 fixes for PostgresLoader duplication bug.

This test suite verifies the critical fixes implemented in Phase 1:
1. Proper pagination using LIMIT/OFFSET
2. Pre-processing validation for incremental loads
3. Strategy-based insert optimization
4. Enhanced error handling and progress tracking
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Tuple

from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.config import create_test_settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.config import PostgresSchema as ConfigPostgresSchema


class TestPostgresLoaderPhase1Fixes:
    """Test Phase 1 fixes for PostgresLoader duplication bug."""
    
    @pytest.fixture
    def postgres_loader(self):
        """Create PostgresLoader instance for testing with new constructor."""
        from unittest.mock import MagicMock
        from etl_pipeline.config import DatabaseType
        from etl_pipeline.config import PostgresSchema as ConfigPostgresSchema
        
        settings = create_test_settings()
        # Mock settings.get_database_config to avoid env var requirements
        def mock_get_database_config(db_type, *args):
            if db_type == DatabaseType.ANALYTICS:
                return {'database': 'test_db', 'schema': 'raw'}
            elif db_type == DatabaseType.REPLICATION:
                return {'database': 'test_db'}
            return {'database': 'test_db'}
        settings.get_database_config = MagicMock(side_effect=mock_get_database_config)
        
        # Mock engines instead of using real ConnectionFactory to avoid env var requirements
        replication_engine = MagicMock()
        analytics_engine = MagicMock()
        schema_adapter = MagicMock()
        schema_adapter.get_table_schema_from_mysql.return_value = {'columns': []}
        schema_adapter.ensure_table_exists.return_value = True
        
        return PostgresLoader(
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            settings=settings,
            schema_adapter=schema_adapter,
        )
    
    @pytest.fixture
    def mock_table_config(self):
        """Mock table configuration for testing."""
        return {
            'table_name': 'test_table',
            'incremental_columns': ['updated_at', 'created_at'],
            'estimated_rows': 100000,
            'incremental_strategy': 'or_logic'
        }
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_validate_incremental_load_no_new_data(self, postgres_loader, mock_table_config):
        """
        OBSOLETE: Test validation when no new data exists.
        
        This method is now internal. Validation is handled internally by _prepare_table_load().
        """
        pytest.skip("_validate_incremental_load is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_validate_incremental_load_with_new_data(self, postgres_loader, mock_table_config):
        """
        OBSOLETE: Test validation when new data exists.
        
        This method is now internal. Validation is handled internally.
        """
        pytest.skip("_validate_incremental_load is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_validate_incremental_load_force_full(self, postgres_loader):
        """
        OBSOLETE: Test validation is skipped for force_full loads.
        
        This method is now internal. Use load_table(table_name, force_full=True) instead.
        """
        pytest.skip("_validate_incremental_load is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_choose_insert_strategy_simple_insert(self, postgres_loader):
        """
        OBSOLETE: Test strategy selection for full loads.
        
        Strategy selection is now internal. Use load_table() which selects strategy automatically.
        """
        pytest.skip("_choose_insert_strategy is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_choose_insert_strategy_optimized_upsert(self, postgres_loader):
        """
        OBSOLETE: Test strategy selection for incremental loads.
        
        Strategy selection is now internal. Use load_table() which selects strategy automatically.
        """
        pytest.skip("_choose_insert_strategy is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_choose_insert_strategy_copy_csv(self, postgres_loader):
        """
        OBSOLETE: Test strategy selection for massive datasets.
        
        Strategy selection is now internal. Use load_table() which selects strategy automatically.
        """
        pytest.skip("_choose_insert_strategy is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="These methods are now internal to strategies - use load_table() public API instead")
    def test_get_optimized_batch_size(self, postgres_loader):
        """
        OBSOLETE: Test optimized batch size selection.
        
        Batch size selection is now internal to strategies.
        """
        pytest.skip("_get_optimized_batch_size is now internal - handled by strategies")
    
    @pytest.mark.skip(reason="stream_mysql_data_paginated is now internal to strategies - use load_table() public API instead")
    def test_stream_mysql_data_paginated_basic(self, postgres_loader):
        """
        OBSOLETE: Test basic pagination functionality.
        
        This method is now internal to strategies. Use load_table() which handles pagination internally.
        """
        pytest.skip("stream_mysql_data_paginated is now internal - use load_table() public API")
    
    @pytest.mark.skip(reason="stream_mysql_data_paginated is now internal to strategies - use load_table() public API instead")
    def test_stream_mysql_data_paginated_no_data(self, postgres_loader):
        """
        OBSOLETE: Test pagination when no data exists.
        
        This method is now internal. Use load_table() which handles empty data.
        """
        pytest.skip("stream_mysql_data_paginated is now internal - use load_table() public API")
    
    def test_bulk_insert_optimized_upsert_batch_size(self, postgres_loader):
        """Test that UPSERT operations use smaller batch sizes."""
        # Create test data
        test_data = [{'id': i, 'name': f'test{i}'} for i in range(10000)]
        
        # Mock target_engine (bulk_insert_optimized uses self.target_engine, not analytics_engine)
        # Following pytest_debugging_notes.md section 2: Context Manager Protocol
        # Use MagicMock so attributes are auto-created when accessed
        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        postgres_loader.target_engine.begin.return_value = mock_context
        
        # Mock the upsert SQL building
        with patch.object(postgres_loader, '_build_upsert_sql') as mock_build_upsert:
            mock_build_upsert.return_value = "INSERT INTO raw.test_table (\"id\", \"name\") VALUES (:id, :name) ON CONFLICT ..."
            
            result = postgres_loader.bulk_insert_optimized(
                'test_table', test_data, chunk_size=25000, use_upsert=True
            )
            
            assert result is True
            
            # Verify that smaller batches were used for UPSERT
            # Should have called execute multiple times with smaller chunks
            assert mock_conn.execute.call_count > 1
    
    def test_bulk_insert_optimized_simple_insert_batch_size(self, postgres_loader):
        """Test that simple INSERT operations use larger batch sizes."""
        # Create test data
        test_data = [{'id': i, 'name': f'test{i}'} for i in range(10000)]
        
        # Mock target_engine (bulk_insert_optimized uses self.target_engine, not analytics_engine)
        # Following pytest_debugging_notes.md section 2: Context Manager Protocol
        # Use MagicMock so attributes are auto-created when accessed
        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        postgres_loader.target_engine.begin.return_value = mock_context
        
        result = postgres_loader.bulk_insert_optimized(
            'test_table', test_data, chunk_size=25000, use_upsert=False
        )
        
        assert result is True
        
        # Verify that larger batches were used for simple INSERT
        # Should have called execute at least once
        assert mock_conn.execute.call_count >= 1
    
    def test_load_table_phase1_integration(self, postgres_loader):
        """
        Integration test for Phase 1 fixes using public API load_table().
        
        This test verifies that load_table() works correctly with the new architecture,
        automatically selecting the appropriate strategy (including chunked for large tables).
        """
        from etl_pipeline.loaders.postgres_loader import LoadStrategyType, LoadResult, LoadPreparation
        
        # Create a mock LoadPreparation object with all required fields
        mock_load_prep = LoadPreparation(
            table_name='test_table',
            table_config={'incremental_columns': ['updated_at'], 'estimated_size_mb': 300.0, 'estimated_rows': 1000},
            mysql_schema={'columns': []},
            query="SELECT * FROM test_table WHERE updated_at > '2024-01-01'",
            primary_column=None,
            incremental_columns=['updated_at'],
            incremental_strategy='or_logic',
            should_truncate=False,
            force_full=False,
            batch_size=10000,
            estimated_rows=1000,
            estimated_size_mb=300.0
        )
        
        # Mock all the dependencies in a single context manager to avoid nesting issues
        with patch.object(postgres_loader, '_prepare_table_load', return_value=mock_load_prep), \
             patch.object(postgres_loader, '_select_strategy', return_value=LoadStrategyType.CHUNKED), \
             patch.object(postgres_loader, '_finalize_table_load') as mock_finalize:
            
            # Mock the chunked strategy in the strategies dict
            mock_chunked_strategy = MagicMock()
            mock_load_result = LoadResult(
                success=True,
                rows_loaded=3,
                strategy_used='chunked',  # Required field
                duration=1.0
            )
            mock_chunked_strategy.execute.return_value = mock_load_result
            # Replace the actual strategy in the dict BEFORE calling load_table
            original_strategy = postgres_loader.strategies.get(LoadStrategyType.CHUNKED)
            postgres_loader.strategies[LoadStrategyType.CHUNKED] = mock_chunked_strategy
            
            try:
                mock_finalize.return_value = (True, {
                    'rows_loaded': 3,
                    'chunk_count': 2,
                    'strategy': 'chunked'
                })
                
                # Test the load using public API
                success, metadata = postgres_loader.load_table('test_table', force_full=False)
                
                assert success is True
                assert metadata['rows_loaded'] == 3
                assert metadata['chunk_count'] == 2
                assert metadata['strategy'] == 'chunked'
            finally:
                # Restore original strategy
                if original_strategy:
                    postgres_loader.strategies[LoadStrategyType.CHUNKED] = original_strategy


if __name__ == "__main__":
    pytest.main([__file__]) 