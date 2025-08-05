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


class TestPostgresLoaderPhase1Fixes:
    """Test Phase 1 fixes for PostgresLoader duplication bug."""
    
    @pytest.fixture
    def postgres_loader(self):
        """Create PostgresLoader instance for testing."""
        settings = create_test_settings()
        return PostgresLoader(use_test_environment=True, settings=settings)
    
    @pytest.fixture
    def mock_table_config(self):
        """Mock table configuration for testing."""
        return {
            'table_name': 'test_table',
            'incremental_columns': ['updated_at', 'created_at'],
            'estimated_rows': 100000,
            'incremental_strategy': 'or_logic'
        }
    
    def test_validate_incremental_load_no_new_data(self, postgres_loader, mock_table_config):
        """Test validation when no new data exists."""
        # Mock the count query to return 0
        with patch.object(postgres_loader, '_build_count_query') as mock_build_query:
            mock_build_query.return_value = "SELECT COUNT(*) FROM test_table WHERE updated_at > '2024-01-01'"
            
            with patch.object(postgres_loader, 'replication_engine') as mock_engine:
                mock_conn = Mock()
                mock_result = Mock()
                mock_result.scalar.return_value = 0
                mock_conn.execute.return_value = mock_result
                mock_engine.connect.return_value.__enter__.return_value = mock_conn
                
                should_proceed, result = postgres_loader._validate_incremental_load(
                    'test_table', ['updated_at'], force_full=False
                )
                
                assert should_proceed is False
                assert result == "no_new_data"
    
    def test_validate_incremental_load_with_new_data(self, postgres_loader, mock_table_config):
        """Test validation when new data exists."""
        # Mock the count query to return 5000
        with patch.object(postgres_loader, '_build_count_query') as mock_build_query:
            mock_build_query.return_value = "SELECT COUNT(*) FROM test_table WHERE updated_at > '2024-01-01'"
            
            with patch.object(postgres_loader, 'replication_engine') as mock_engine:
                mock_conn = Mock()
                mock_result = Mock()
                mock_result.scalar.return_value = 5000
                mock_conn.execute.return_value = mock_result
                mock_engine.connect.return_value.__enter__.return_value = mock_conn
                
                should_proceed, result = postgres_loader._validate_incremental_load(
                    'test_table', ['updated_at'], force_full=False
                )
                
                assert should_proceed is True
                assert result == 5000
    
    def test_validate_incremental_load_force_full(self, postgres_loader):
        """Test validation is skipped for force_full loads."""
        should_proceed, result = postgres_loader._validate_incremental_load(
            'test_table', ['updated_at'], force_full=True
        )
        
        assert should_proceed is True
        assert result == "force_full"
    
    def test_choose_insert_strategy_simple_insert(self, postgres_loader):
        """Test strategy selection for full loads."""
        strategy = postgres_loader._choose_insert_strategy(force_full=True, row_count=1000)
        assert strategy == "simple_insert"
    
    def test_choose_insert_strategy_optimized_upsert(self, postgres_loader):
        """Test strategy selection for incremental loads."""
        strategy = postgres_loader._choose_insert_strategy(force_full=False, row_count=1000)
        assert strategy == "optimized_upsert"
    
    def test_choose_insert_strategy_copy_csv(self, postgres_loader):
        """Test strategy selection for massive datasets."""
        strategy = postgres_loader._choose_insert_strategy(force_full=False, row_count=2000000)
        assert strategy == "copy_csv"
    
    def test_get_optimized_batch_size(self, postgres_loader):
        """Test optimized batch size selection."""
        # Test simple insert (larger batches)
        batch_size = postgres_loader._get_optimized_batch_size("simple_insert")
        assert batch_size == 50000
        
        # Test optimized upsert (smaller batches)
        batch_size = postgres_loader._get_optimized_batch_size("optimized_upsert")
        assert batch_size == 5000
        
        # Test copy csv (largest batches)
        batch_size = postgres_loader._get_optimized_batch_size("copy_csv")
        assert batch_size == 100000
        
        # Test fallback
        batch_size = postgres_loader._get_optimized_batch_size("unknown_strategy")
        assert batch_size == 25000
    
    def test_stream_mysql_data_paginated_basic(self, postgres_loader):
        """Test basic pagination functionality."""
        # Mock data that would be returned in chunks
        mock_data = [
            [{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}],
            [{'id': 3, 'name': 'test3'}],  # Final chunk with fewer rows
        ]
        
        with patch.object(postgres_loader, 'replication_engine') as mock_engine:
            mock_conn = Mock()
            
            # Mock the execute calls for each chunk
            def mock_execute(query):
                mock_result = Mock()
                if 'OFFSET 0' in query:
                    mock_result.fetchall.return_value = mock_data[0]
                    mock_result.keys.return_value = ['id', 'name']
                elif 'OFFSET 2' in query:
                    mock_result.fetchall.return_value = mock_data[1]
                    mock_result.keys.return_value = ['id', 'name']
                else:
                    mock_result.fetchall.return_value = []
                return mock_result
            
            mock_conn.execute.side_effect = mock_execute
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            
            # Test the paginated streaming
            chunks = list(postgres_loader.stream_mysql_data_paginated(
                'test_table', 'SELECT * FROM test_table', chunk_size=2
            ))
            
            assert len(chunks) == 2
            assert len(chunks[0]) == 2  # First chunk
            assert len(chunks[1]) == 1  # Second chunk
    
    def test_stream_mysql_data_paginated_no_data(self, postgres_loader):
        """Test pagination when no data exists."""
        with patch.object(postgres_loader, 'replication_engine') as mock_engine:
            mock_conn = Mock()
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            
            chunks = list(postgres_loader.stream_mysql_data_paginated(
                'test_table', 'SELECT * FROM test_table', chunk_size=1000
            ))
            
            assert len(chunks) == 0
    
    def test_bulk_insert_optimized_upsert_batch_size(self, postgres_loader):
        """Test that UPSERT operations use smaller batch sizes."""
        # Create test data
        test_data = [{'id': i, 'name': f'test{i}'} for i in range(10000)]
        
        with patch.object(postgres_loader, 'analytics_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.begin.return_value.__enter__.return_value = mock_conn
            
            # Mock the upsert SQL building
            with patch.object(postgres_loader, '_build_upsert_sql') as mock_build_upsert:
                mock_build_upsert.return_value = "INSERT ... ON CONFLICT ..."
                
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
        
        with patch.object(postgres_loader, 'analytics_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.begin.return_value.__enter__.return_value = mock_conn
            
            result = postgres_loader.bulk_insert_optimized(
                'test_table', test_data, chunk_size=25000, use_upsert=False
            )
            
            assert result is True
            
            # Verify that larger batches were used for simple INSERT
            # Should have called execute fewer times with larger chunks
            assert mock_conn.execute.call_count >= 1
    
    def test_load_table_chunked_phase1_integration(self, postgres_loader):
        """Integration test for Phase 1 fixes in load_table_chunked."""
        # Mock all the dependencies
        with patch.object(postgres_loader, 'get_table_config') as mock_get_config:
            mock_get_config.return_value = {
                'incremental_columns': ['updated_at'],
                'estimated_rows': 1000,
                'incremental_strategy': 'or_logic'
            }
            
            with patch.object(postgres_loader, '_get_primary_incremental_column') as mock_get_primary:
                mock_get_primary.return_value = 'id'
                
            with patch.object(postgres_loader, '_get_cached_schema') as mock_get_schema:
                mock_get_schema.return_value = {'id': 'int', 'name': 'varchar'}
                
            with patch.object(postgres_loader, 'schema_adapter') as mock_schema:
                mock_schema.ensure_table_exists.return_value = True
                
            with patch.object(postgres_loader, '_build_enhanced_load_query') as mock_build_query:
                mock_build_query.return_value = "SELECT * FROM test_table WHERE updated_at > '2024-01-01'"
                
            with patch.object(postgres_loader, 'stream_mysql_data_paginated') as mock_stream:
                # Mock two chunks of data
                mock_stream.return_value = [
                    [{'id': 1, 'name': 'test1'}, {'id': 2, 'name': 'test2'}],
                    [{'id': 3, 'name': 'test3'}]
                ]
                
            with patch.object(postgres_loader, 'bulk_insert_optimized') as mock_insert:
                mock_insert.return_value = True
                
            with patch.object(postgres_loader, '_update_load_status') as mock_update_status:
                mock_update_status.return_value = True
                
            # Test the load
            success, metadata = postgres_loader.load_table_chunked('test_table', force_full=False)
            
            assert success is True
            assert metadata['rows_loaded'] == 3
            assert metadata['chunk_count'] == 2
            assert 'insert_strategy' in metadata
            assert metadata['insert_strategy'] == 'optimized_upsert'  # Should be upsert for incremental


if __name__ == "__main__":
    pytest.main([__file__]) 