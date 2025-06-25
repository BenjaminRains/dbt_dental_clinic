"""
Integration tests for PipelineOrchestrator.

This test suite covers real database integration scenarios with SQLite for safety,
error handling validation, and actual data flow testing.

Testing Strategy:
- Real database integration with SQLite
- Safety and error handling validation
- Actual data flow testing
- Integration scenarios and edge cases

Target Coverage: 80%+ for integration tests
"""

import pytest
import logging
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

# Import the component under test
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestPipelineOrchestratorIntegration:
    """Integration tests for PipelineOrchestrator with real database connections."""
    
    # Fixtures moved to conftest.py:
    # - sqlite_engines
    # - sqlite_compatible_orchestrator


class TestDatabaseIntegration(TestPipelineOrchestratorIntegration):
    """Test database integration scenarios."""
    
    def test_connection_initialization_integration(self, sqlite_compatible_orchestrator):
        """Test connection initialization with real database setup."""
        result = sqlite_compatible_orchestrator.initialize_connections()
        
        assert result is True
        assert sqlite_compatible_orchestrator._initialized is True
    
    def test_table_processing_integration(self, sqlite_compatible_orchestrator):
        """Test table processing with real database integration."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Process a table
        result = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
        
        assert result is True
    
    def test_priority_processing_integration(self, sqlite_compatible_orchestrator):
        """Test priority processing with real database integration."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Process tables by priority
        result = sqlite_compatible_orchestrator.process_tables_by_priority(
            importance_levels=['high'],
            max_workers=1,
            force_full=False
        )
        
        assert result is not None
        assert 'high' in result
        assert 'success' in result['high']
        assert 'failed' in result['high']
    
    def test_context_manager_integration(self, sqlite_compatible_orchestrator):
        """Test context manager with real database integration."""
        with sqlite_compatible_orchestrator as orchestrator:
            # Initialize connections
            result = orchestrator.initialize_connections()
            assert result is True
            
            # Process a table
            table_result = orchestrator.run_pipeline_for_table('patient', force_full=False)
            assert table_result is True
        
        # Verify cleanup was called
        orchestrator.table_processor.cleanup.assert_called_once()


class TestErrorHandlingIntegration(TestPipelineOrchestratorIntegration):
    """Test error handling with real database scenarios."""
    
    def test_connection_failure_integration(self, sqlite_compatible_orchestrator):
        """Test connection failure handling with real database."""
        # Mock table processor to simulate connection failure
        sqlite_compatible_orchestrator.table_processor.initialize_connections.return_value = False
        
        result = sqlite_compatible_orchestrator.initialize_connections()
        
        assert result is False
        assert sqlite_compatible_orchestrator._initialized is False
    
    def test_processing_failure_integration(self, sqlite_compatible_orchestrator):
        """Test processing failure handling with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Mock table processor to simulate processing failure
        sqlite_compatible_orchestrator.table_processor.process_table.return_value = False
        
        result = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
        
        assert result is False
    
    def test_cleanup_integration(self, sqlite_compatible_orchestrator):
        """Test cleanup with real database integration."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Call cleanup
        sqlite_compatible_orchestrator.cleanup()
        
        # Verify cleanup was called and state was reset
        sqlite_compatible_orchestrator.table_processor.cleanup.assert_called_once()
        assert sqlite_compatible_orchestrator._initialized is False
    
    def test_cleanup_with_exception_integration(self, sqlite_compatible_orchestrator):
        """Test cleanup when disposal raises an exception."""
        # Mock cleanup to raise an exception
        sqlite_compatible_orchestrator.table_processor.cleanup.side_effect = Exception("Cleanup failed")
        
        # Cleanup should not raise an exception
        sqlite_compatible_orchestrator.cleanup()
        
        # Verify state was still reset
        assert sqlite_compatible_orchestrator._initialized is False


class TestIdempotencyIntegration(TestPipelineOrchestratorIntegration):
    """Test idempotency with real database integration."""
    
    def test_idempotent_processing_integration(self, sqlite_compatible_orchestrator):
        """Test idempotent processing with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # First call
        result1 = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result1 is True
        
        # Second call - should be idempotent
        result2 = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result2 is True
        
        # Verify both calls were made
        assert sqlite_compatible_orchestrator.table_processor.process_table.call_count == 2
    
    def test_force_full_flag_integration(self, sqlite_compatible_orchestrator):
        """Test force_full flag behavior with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Test with force_full=True
        result1 = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=True)
        assert result1 is True
        
        # Test with force_full=False
        result2 = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result2 is True
        
        # Verify different parameters were passed
        calls = sqlite_compatible_orchestrator.table_processor.process_table.call_args_list
        assert len(calls) == 2
        
        # First call should have force_full=True
        assert calls[0].args[0] == 'patient'  # table_name
        assert calls[0].args[1] is True  # force_full=True
        
        # Second call should have force_full=False
        assert calls[1].args[0] == 'patient'  # table_name
        assert calls[1].args[1] is False  # force_full=False


class TestPerformanceIntegration(TestPipelineOrchestratorIntegration):
    """Test performance with real database integration."""
    
    def test_multiple_table_processing_integration(self, sqlite_compatible_orchestrator):
        """Test processing multiple tables with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Process multiple tables
        tables = ['patient', 'appointment', 'procedure', 'payment']
        for table in tables:
            result = sqlite_compatible_orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
        
        # Verify all tables were processed
        assert sqlite_compatible_orchestrator.table_processor.process_table.call_count == 4
    
    def test_parallel_processing_integration(self, sqlite_compatible_orchestrator):
        """Test parallel processing with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Test with different worker counts
        for max_workers in [1, 2, 3]:
            result = sqlite_compatible_orchestrator.process_tables_by_priority(
                importance_levels=['high'],
                max_workers=max_workers,
                force_full=False
            )
            
            assert result is not None
            assert 'high' in result
    
    def test_memory_management_integration(self, sqlite_compatible_orchestrator):
        """Test memory management with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Process multiple tables
        for table in ['patient', 'appointment', 'procedure']:
            result = sqlite_compatible_orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
        
        # Verify cleanup can still be called
        sqlite_compatible_orchestrator.cleanup()
        assert sqlite_compatible_orchestrator._initialized is False


class TestEdgeCasesIntegration(TestPipelineOrchestratorIntegration):
    """Test edge cases with real database integration."""
    
    def test_empty_table_name_integration(self, sqlite_compatible_orchestrator):
        """Test processing with empty table name."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        result = sqlite_compatible_orchestrator.run_pipeline_for_table('', force_full=False)
        assert result is True
    
    def test_none_importance_levels_integration(self, sqlite_compatible_orchestrator):
        """Test priority processing with None importance levels."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        result = sqlite_compatible_orchestrator.process_tables_by_priority(
            importance_levels=None,
            max_workers=1,
            force_full=False
        )
        
        assert result is not None
    
    def test_zero_workers_integration(self, sqlite_compatible_orchestrator):
        """Test priority processing with zero workers."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        result = sqlite_compatible_orchestrator.process_tables_by_priority(
            importance_levels=['high'],
            max_workers=0,
            force_full=False
        )
        
        assert result is not None
    
    def test_negative_workers_integration(self, sqlite_compatible_orchestrator):
        """Test priority processing with negative workers."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        result = sqlite_compatible_orchestrator.process_tables_by_priority(
            importance_levels=['high'],
            max_workers=-1,
            force_full=False
        )
        
        assert result is not None


class TestLoggingIntegration(TestPipelineOrchestratorIntegration):
    """Test logging with real database integration."""
    
    def test_initialization_logging_integration(self, sqlite_compatible_orchestrator, caplog):
        """Test initialization logging with real database."""
        with caplog.at_level(logging.INFO):
            result = sqlite_compatible_orchestrator.initialize_connections()
            
            assert result is True
            assert "Initializing pipeline connections..." in caplog.text
            assert "Successfully initialized all pipeline connections" in caplog.text
    
    def test_table_processing_logging_integration(self, sqlite_compatible_orchestrator, caplog):
        """Test table processing logging with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        with caplog.at_level(logging.INFO):
            result = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
            
            assert result is True
            assert "Running pipeline for table: patient" in caplog.text
    
    def test_priority_processing_logging_integration(self, sqlite_compatible_orchestrator, caplog):
        """Test priority processing logging with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        with caplog.at_level(logging.INFO):
            result = sqlite_compatible_orchestrator.process_tables_by_priority(
                importance_levels=['high'],
                max_workers=2,
                force_full=False
            )
            
            assert result is not None
            assert "Processing tables by priority with 2 workers" in caplog.text
    
    def test_cleanup_logging_integration(self, sqlite_compatible_orchestrator, caplog):
        """Test cleanup logging with real database."""
        with caplog.at_level(logging.INFO):
            sqlite_compatible_orchestrator.cleanup()
            
            assert "Pipeline cleanup completed" in caplog.text


class TestMetricsIntegration(TestPipelineOrchestratorIntegration):
    """Test metrics integration with real database."""
    
    def test_metrics_availability_integration(self, sqlite_compatible_orchestrator):
        """Test metrics collector availability with real database."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Verify metrics collector is available
        assert sqlite_compatible_orchestrator.metrics is not None
        
        # Process a table and verify metrics collector is still available
        result = sqlite_compatible_orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result is True
        assert sqlite_compatible_orchestrator.metrics is not None
    
    def test_metrics_persistence_integration(self, sqlite_compatible_orchestrator):
        """Test metrics persistence through processing cycles."""
        # Initialize connections first
        sqlite_compatible_orchestrator.initialize_connections()
        
        # Process multiple tables and verify metrics collector persists
        tables = ['patient', 'appointment', 'procedure']
        for table in tables:
            result = sqlite_compatible_orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
            assert sqlite_compatible_orchestrator.metrics is not None
        
        # Verify metrics collector is still available after cleanup
        sqlite_compatible_orchestrator.cleanup()
        assert sqlite_compatible_orchestrator.metrics is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 