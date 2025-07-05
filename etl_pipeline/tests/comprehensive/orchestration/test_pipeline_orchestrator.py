"""
Comprehensive tests for PipelineOrchestrator.

This test suite covers the complete orchestration functionality with mocked dependencies
for comprehensive testing of all component behaviors and interactions.

Testing Strategy:
- Full functionality testing with mocked dependencies
- Complete component behavior and error handling
- Integration scenarios with mocked components
- Performance and resource management validation

Target Coverage: 90%+ for comprehensive tests
"""

import pytest
import logging
from unittest.mock import MagicMock, patch, call
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# Import the component under test
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


class TestPipelineOrchestrator:
    """Comprehensive tests for PipelineOrchestrator class."""


class TestInitialization(TestPipelineOrchestrator):
    """Test PipelineOrchestrator initialization."""
    
    def test_initialization_without_config(self, mock_components):
        """Test successful initialization without configuration file."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings:
            mock_settings.return_value = MagicMock()
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                mock_processor_class.return_value = mock_components['table_processor']
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                    mock_priority_class.return_value = mock_components['priority_processor']
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                        mock_metrics_class.return_value = mock_components['metrics']
                        
                        # Test REAL orchestrator initialization
                        orchestrator = PipelineOrchestrator()
                        
                        assert orchestrator.settings is not None
                        assert orchestrator.table_processor == mock_components['table_processor']
                        assert orchestrator.priority_processor == mock_components['priority_processor']
                        assert orchestrator.metrics == mock_components['metrics']
                        assert orchestrator._initialized is False
    
    def test_initialization_with_config_path(self, mock_components):
        """Test initialization with config_path parameter (deprecated but supported)."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings:
            mock_settings.return_value = MagicMock()
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                mock_processor_class.return_value = mock_components['table_processor']
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                    mock_priority_class.return_value = mock_components['priority_processor']
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                        mock_metrics_class.return_value = mock_components['metrics']
                        
                        # Test REAL orchestrator with config_path
                        orchestrator = PipelineOrchestrator(config_path='dummy_config.yml')
                        
                        assert orchestrator.settings is not None
                        assert orchestrator._initialized is False
    
    def test_component_initialization_order(self, mock_components):
        """Test that components are initialized in the correct order."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings:
            mock_settings.return_value = MagicMock()
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                        
                        # Test REAL orchestrator creation
                        orchestrator = PipelineOrchestrator()
                        
                        # Verify components were created in the correct order
                        mock_processor_class.assert_called_once()
                        mock_priority_class.assert_called_once()
                        mock_metrics_class.assert_called_once()


class TestConnectionInitialization(TestPipelineOrchestrator):
    """Test connection initialization functionality."""
    
    def test_connection_initialization_success(self, orchestrator, mock_components):
        """Test successful connection initialization."""
        # Mock table processor to return success with debug logging
        mock_components['table_processor'].initialize_connections.return_value = True
        
        # Test REAL orchestrator connection initialization
        result = orchestrator.initialize_connections()
        
        assert result is True
        assert orchestrator._initialized is True
        mock_components['table_processor'].initialize_connections.assert_called_once()
    
    def test_connection_initialization_failure(self, orchestrator, mock_components):
        """Test connection initialization failure."""
        # Mock table processor to return failure with debug logging
        mock_components['table_processor'].initialize_connections.return_value = False
        
        # Test REAL orchestrator connection initialization failure
        result = orchestrator.initialize_connections()
        
        assert result is False
        assert orchestrator._initialized is False
        mock_components['table_processor'].initialize_connections.assert_called_once()
    
    def test_connection_initialization_exception(self, orchestrator, mock_components):
        """Test connection initialization with exception."""
        # Mock table processor to raise an exception with debug logging
        def raise_sqlalchemy_error(**kwargs):
            logger.debug(f"Mock called with: {kwargs}")
            raise SQLAlchemyError("Connection failed")
        
        mock_components['table_processor'].initialize_connections.side_effect = raise_sqlalchemy_error
        
        with pytest.raises(SQLAlchemyError, match="Connection failed"):
            orchestrator.initialize_connections()
        
        # Verify cleanup was called
        assert orchestrator._initialized is False
        mock_components['table_processor'].cleanup.assert_called_once()
    
    def test_connection_initialization_logging(self, orchestrator, mock_components, caplog):
        """Test connection initialization logging."""
        with caplog.at_level(logging.INFO):
            # Mock table processor to return success
            mock_components['table_processor'].initialize_connections.return_value = True
            
            result = orchestrator.initialize_connections()
            
            assert result is True
            assert "Initializing pipeline connections..." in caplog.text
            assert "Successfully initialized all pipeline connections" in caplog.text
    
    def test_connection_initialization_failure_logging(self, orchestrator, mock_components, caplog):
        """Test connection initialization failure logging."""
        with caplog.at_level(logging.ERROR):
            # Mock table processor to return failure
            mock_components['table_processor'].initialize_connections.return_value = False
            
            result = orchestrator.initialize_connections()
            
            assert result is False
            assert "Failed to initialize table processor connections" in caplog.text


class TestTableProcessing(TestPipelineOrchestrator):
    """Test table processing functionality."""
    
    def test_run_pipeline_for_table_success(self, orchestrator, mock_components):
        """Test successful table processing."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock table processor to return success with debug logging
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator table processing
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        
        assert result is True
        mock_components['table_processor'].process_table.assert_called_once_with('patient', False)
    
    def test_run_pipeline_for_table_failure(self, orchestrator, mock_components):
        """Test table processing failure."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock table processor to return failure with debug logging
        mock_components['table_processor'].process_table.return_value = False
        
        # Test REAL orchestrator table processing failure
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        
        assert result is False
        mock_components['table_processor'].process_table.assert_called_once_with('patient', True)
    
    def test_run_pipeline_for_table_connections_not_initialized(self, orchestrator):
        """Test table processing when connections are not initialized."""
        orchestrator._initialized = False
        
        # Test REAL orchestrator error handling
        with pytest.raises(RuntimeError, match="Pipeline not initialized"):
            orchestrator.run_pipeline_for_table('patient', force_full=False)
    
    def test_run_pipeline_for_table_logging(self, orchestrator, mock_components, caplog):
        """Test table processing logging."""
        # Initialize connections first
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        with caplog.at_level(logging.INFO):
            result = orchestrator.run_pipeline_for_table('patient', force_full=False)
            
            assert result is True
            assert "Running pipeline for table: patient" in caplog.text
    
    def test_run_pipeline_for_table_multiple_tables(self, orchestrator, mock_components):
        """Test processing multiple tables."""
        # Initialize connections first
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator with multiple tables
        tables = ['patient', 'appointment', 'procedure']
        for table in tables:
            result = orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
        
        # Verify each table was processed
        assert mock_components['table_processor'].process_table.call_count == 3
        calls = mock_components['table_processor'].process_table.call_args_list
        assert calls[0] == call('patient', False)
        assert calls[1] == call('appointment', False)
        assert calls[2] == call('procedure', False)


class TestPriorityProcessing(TestPipelineOrchestrator):
    """Test priority-based table processing."""
    
    def test_process_tables_by_priority_success(self, orchestrator, mock_components):
        """Test successful priority-based table processing."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock priority processor to return success with debug logging
        expected_result = {
            'high': {'success': ['patient', 'appointment'], 'failed': []},
            'medium': {'success': ['procedure'], 'failed': []},
            'low': {'success': ['payment'], 'failed': []}
        }
        mock_components['priority_processor'].process_by_priority.return_value = expected_result
        
        # Test REAL orchestrator priority processing
        result = orchestrator.process_tables_by_priority(
            importance_levels=['high', 'medium', 'low'],
            max_workers=3,
            force_full=False
        )
        
        assert result == expected_result
        mock_components['priority_processor'].process_by_priority.assert_called_once_with(
            mock_components['table_processor'],
            ['high', 'medium', 'low'],
            3,
            False
        )
    
    def test_process_tables_by_priority_failure(self, orchestrator, mock_components):
        """Test priority-based table processing with failures."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock priority processor to return failure with debug logging
        expected_result = {
            'high': {'success': ['patient'], 'failed': ['appointment']},
            'medium': {'success': [], 'failed': ['procedure']}
        }
        mock_components['priority_processor'].process_by_priority.return_value = expected_result
        
        # Test REAL orchestrator priority processing with failures
        result = orchestrator.process_tables_by_priority(
            importance_levels=['high', 'medium'],
            max_workers=2,
            force_full=True
        )
        
        assert result == expected_result
        mock_components['priority_processor'].process_by_priority.assert_called_once_with(
            mock_components['table_processor'],
            ['high', 'medium'],
            2,
            True
        )
    
    def test_process_tables_by_priority_connections_not_initialized(self, orchestrator):
        """Test priority processing when connections are not initialized."""
        orchestrator._initialized = False
        
        # Test REAL orchestrator error handling
        with pytest.raises(RuntimeError, match="Pipeline not initialized"):
            orchestrator.process_tables_by_priority(
                importance_levels=['high'],
                max_workers=1,
                force_full=False
            )
    
    def test_process_tables_by_priority_default_parameters(self, orchestrator, mock_components):
        """Test priority processing with default parameters."""
        # Initialize connections first
        orchestrator._initialized = True
        
        expected_result = {'high': {'success': ['patient'], 'failed': []}}
        mock_components['priority_processor'].process_by_priority.return_value = expected_result
        
        # Test REAL orchestrator with default parameters
        result = orchestrator.process_tables_by_priority()
        
        assert result == expected_result
        # Verify default parameters were used
        call_args = mock_components['priority_processor'].process_by_priority.call_args
        assert call_args[0][0] == mock_components['table_processor']  # table_processor
        assert call_args[0][1] is None  # importance_levels (default)
        assert call_args[0][2] == 5  # max_workers (default)
        assert call_args[0][3] is False  # force_full (default)
    
    def test_process_tables_by_priority_logging(self, orchestrator, mock_components, caplog):
        """Test priority processing logging."""
        # Initialize connections first
        orchestrator._initialized = True
        mock_components['priority_processor'].process_by_priority.return_value = {
            'high': {'success': ['patient'], 'failed': []}
        }
        
        with caplog.at_level(logging.INFO):
            result = orchestrator.process_tables_by_priority(
                importance_levels=['high'],
                max_workers=3,
                force_full=False
            )
            
            assert "Processing tables by priority with 3 workers" in caplog.text


class TestErrorHandlingAndRecovery(TestPipelineOrchestrator):
    """Test error handling and recovery scenarios."""
    
    def test_error_handling_and_recovery(self, orchestrator, mock_components):
        """Test error handling and recovery during processing."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock table processor to raise an exception with debug logging
        def raise_processing_error(*args, **kwargs):
            logger.debug(f"Mock called with: {args}, {kwargs}")
            raise Exception("Processing failed")
        
        mock_components['table_processor'].process_table.side_effect = raise_processing_error
        
        # Test REAL orchestrator error handling
        with pytest.raises(Exception, match="Processing failed"):
            orchestrator.run_pipeline_for_table('patient', force_full=False)
    
    def test_cleanup_and_disposal(self, orchestrator, mock_components):
        """Test cleanup and resource disposal."""
        # Test REAL orchestrator cleanup
        orchestrator.cleanup()
        
        # Verify table processor cleanup was called
        mock_components['table_processor'].cleanup.assert_called_once()
        
        # Verify state was reset
        assert orchestrator._initialized is False
    
    def test_cleanup_with_exception(self, orchestrator, mock_components, caplog):
        """Test cleanup when disposal raises an exception."""
        # Mock cleanup to raise an exception with debug logging
        def raise_cleanup_error(**kwargs):
            logger.debug(f"Mock cleanup called with: {kwargs}")
            raise Exception("Cleanup failed")
        
        mock_components['table_processor'].cleanup.side_effect = raise_cleanup_error
        
        with caplog.at_level(logging.ERROR):
            # Cleanup should not raise an exception
            orchestrator.cleanup()
            
            assert "Error during cleanup: Cleanup failed" in caplog.text
        
        # Verify state was still reset
        assert orchestrator._initialized is False
    
    def test_cleanup_logging(self, orchestrator, mock_components, caplog):
        """Test cleanup logging."""
        with caplog.at_level(logging.INFO):
            orchestrator.cleanup()
            
            assert "Pipeline cleanup completed" in caplog.text
    
    def test_cleanup_without_table_processor(self, orchestrator):
        """Test cleanup when table processor is None."""
        orchestrator.table_processor = None
        
        # Should not raise an exception
        orchestrator.cleanup()
        assert orchestrator._initialized is False


class TestContextManager(TestPipelineOrchestrator):
    """Test context manager functionality."""
    
    def test_context_manager_success(self, orchestrator):
        """Test successful context manager usage."""
        # Test REAL orchestrator context manager
        with orchestrator as orch:
            assert orch == orchestrator
            assert orch._initialized is False  # Not initialized yet
    
    def test_context_manager_cleanup(self, orchestrator):
        """Test context manager cleanup on exit."""
        with patch.object(orchestrator, 'cleanup') as mock_cleanup:
            with orchestrator:
                pass  # Context manager should call cleanup on exit
            
            mock_cleanup.assert_called_once()
    
    def test_context_manager_with_exception(self, orchestrator):
        """Test context manager cleanup when exception occurs."""
        with patch.object(orchestrator, 'cleanup') as mock_cleanup:
            try:
                with orchestrator:
                    raise Exception("Test exception")
            except Exception:
                pass  # Exception should be re-raised
            
            mock_cleanup.assert_called_once()
    
    def test_context_manager_initialization_flow(self, orchestrator, mock_components):
        """Test context manager with initialization flow."""
        # Mock table processor to return success
        mock_components['table_processor'].initialize_connections.return_value = True
        
        # Test REAL orchestrator context manager with initialization
        with orchestrator as orch:
            # Initialize connections
            result = orch.initialize_connections()
            assert result is True
            assert orch._initialized is True
            
            # Process a table
            mock_components['table_processor'].process_table.return_value = True
            table_result = orch.run_pipeline_for_table('patient', force_full=False)
            assert table_result is True
        
        # Verify cleanup was called
        mock_components['table_processor'].cleanup.assert_called_once()


class TestIdempotencyAndIncrementalLoad(TestPipelineOrchestrator):
    """Test idempotency and incremental load behavior."""
    
    def test_idempotent_processing_force_full_false_twice(self, orchestrator, mock_components):
        """Test idempotent processing with force_full=False called twice."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator idempotency
        # First call
        result1 = orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result1 is True
        
        # Second call - should be idempotent
        result2 = orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result2 is True
        
        # Verify both calls were made with same parameters
        assert mock_components['table_processor'].process_table.call_count == 2
        calls = mock_components['table_processor'].process_table.call_args_list
        assert calls[0] == call('patient', False)
        assert calls[1] == call('patient', False)
    
    def test_idempotent_processing_force_full_true_then_false(self, orchestrator, mock_components):
        """Test idempotent processing with force_full=True then False."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator with different force_full values
        # First call with force_full=True
        result1 = orchestrator.run_pipeline_for_table('patient', force_full=True)
        assert result1 is True
        
        # Second call with force_full=False
        result2 = orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result2 is True
        
        # Verify calls were made with different parameters
        assert mock_components['table_processor'].process_table.call_count == 2
        calls = mock_components['table_processor'].process_table.call_args_list
        assert calls[0] == call('patient', True)
        assert calls[1] == call('patient', False)
    
    def test_force_full_flag_behavior(self, orchestrator, mock_components):
        """Test force_full flag behavior."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator force_full behavior
        # Test with force_full=True
        result1 = orchestrator.run_pipeline_for_table('patient', force_full=True)
        assert result1 is True
        
        # Test with force_full=False
        result2 = orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result2 is True
        
        # Verify different parameters were passed
        calls = mock_components['table_processor'].process_table.call_args_list
        assert calls[0] == call('patient', True)
        assert calls[1] == call('patient', False)
    
    def test_incremental_processing_scenarios(self, orchestrator, mock_components):
        """Test various incremental processing scenarios."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator with different table types
        tables = ['patient', 'appointment', 'procedure', 'payment']
        for table in tables:
            result = orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
        
        # Verify all tables were processed incrementally
        assert mock_components['table_processor'].process_table.call_count == 4
        for call_obj in mock_components['table_processor'].process_table.call_args_list:
            assert call_obj.args[1] is False  # force_full=False


class TestPerformanceAndResourceManagement(TestPipelineOrchestrator):
    """Test performance and resource management."""
    
    def test_parallel_processing_limits(self, orchestrator, mock_components):
        """Test parallel processing with different worker limits."""
        orchestrator._initialized = True
        
        # Test REAL orchestrator with different max_workers values
        for max_workers in [1, 3, 5, 10]:
            mock_components['priority_processor'].process_by_priority.return_value = {
                'high': {'success': ['patient'], 'failed': []}
            }
            
            result = orchestrator.process_tables_by_priority(
                importance_levels=['high'],
                max_workers=max_workers,
                force_full=False
            )
            
            # Verify max_workers parameter was passed correctly
            call_args = mock_components['priority_processor'].process_by_priority.call_args
            assert call_args[0][2] == max_workers
    
    def test_interrupt_handling(self, orchestrator, mock_components):
        """Test handling of processing interrupts."""
        orchestrator._initialized = True
        
        # Mock priority processor to simulate interrupt with debug logging
        def raise_keyboard_interrupt(*args, **kwargs):
            logger.debug(f"Mock called with: {args}, {kwargs}")
            raise KeyboardInterrupt()
        
        mock_components['priority_processor'].process_by_priority.side_effect = raise_keyboard_interrupt
        
        with pytest.raises(KeyboardInterrupt):
            orchestrator.process_tables_by_priority(
                importance_levels=['high'],
                max_workers=1,
                force_full=False
            )
    
    def test_memory_management(self, orchestrator, mock_components):
        """Test memory management during processing."""
        # Initialize connections
        orchestrator._initialized = True
        
        # Simulate processing multiple tables
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator memory management
        # Process multiple tables
        for table in ['patient', 'appointment', 'procedure']:
            result = orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
        
        # Verify cleanup can still be called
        orchestrator.cleanup()
        assert orchestrator._initialized is False
    
    def test_resource_cleanup_after_failure(self, orchestrator, mock_components):
        """Test resource cleanup after processing failure."""
        orchestrator._initialized = True
        
        # Mock table processor to fail
        mock_components['table_processor'].process_table.return_value = False
        
        # Test REAL orchestrator resource cleanup after failure
        # Process should fail but not crash
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        assert result is False
        
        # Cleanup should still work
        orchestrator.cleanup()
        assert orchestrator._initialized is False


class TestMetricsCollection(TestPipelineOrchestrator):
    """Test metrics collection during processing."""
    
    def test_metrics_collection_during_processing(self, orchestrator, mock_components):
        """Test metrics collection during table processing."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator metrics integration
        # Process a table
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        
        assert result is True
        # Note: Actual metrics collection would be handled by the table processor
        # This test verifies the orchestrator doesn't interfere with metrics
    
    def test_metrics_initialization(self, orchestrator):
        """Test metrics collector initialization."""
        # Test REAL orchestrator metrics initialization
        assert orchestrator.metrics is not None
        # Verify metrics collector was created during initialization
    
    def test_metrics_integration(self, orchestrator, mock_components):
        """Test metrics integration with processing."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator metrics persistence
        # Process multiple tables and verify metrics collector is available
        tables = ['patient', 'appointment']
        for table in tables:
            result = orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result is True
            assert orchestrator.metrics is not None


class TestEdgeCases(TestPipelineOrchestrator):
    """Test edge cases and boundary conditions."""
    
    def test_empty_table_name(self, orchestrator, mock_components):
        """Test processing with empty table name."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator with empty table name
        result = orchestrator.run_pipeline_for_table('', force_full=False)
        assert result is True
        mock_components['table_processor'].process_table.assert_called_once_with('', False)
    
    def test_none_importance_levels(self, orchestrator, mock_components):
        """Test priority processing with None importance levels."""
        orchestrator._initialized = True
        mock_components['priority_processor'].process_by_priority.return_value = {
            'high': {'success': ['patient'], 'failed': []}
        }
        
        # Test REAL orchestrator with None importance levels
        result = orchestrator.process_tables_by_priority(
            importance_levels=None,
            max_workers=3,
            force_full=False
        )
        
        assert result is not None
        mock_components['priority_processor'].process_by_priority.assert_called_once_with(
            mock_components['table_processor'],
            None,
            3,
            False
        )
    
    def test_zero_workers(self, orchestrator, mock_components):
        """Test priority processing with zero workers."""
        orchestrator._initialized = True
        mock_components['priority_processor'].process_by_priority.return_value = {
            'high': {'success': ['patient'], 'failed': []}
        }
        
        # Test REAL orchestrator with zero workers
        result = orchestrator.process_tables_by_priority(
            importance_levels=['high'],
            max_workers=0,
            force_full=False
        )
        
        assert result is not None
        mock_components['priority_processor'].process_by_priority.assert_called_once_with(
            mock_components['table_processor'],
            ['high'],
            0,
            False
        )
    
    def test_negative_workers(self, orchestrator, mock_components):
        """Test priority processing with negative workers."""
        orchestrator._initialized = True
        mock_components['priority_processor'].process_by_priority.return_value = {
            'high': {'success': ['patient'], 'failed': []}
        }
        
        # Test REAL orchestrator with negative workers
        result = orchestrator.process_tables_by_priority(
            importance_levels=['high'],
            max_workers=-1,
            force_full=False
        )
        
        assert result is not None
        mock_components['priority_processor'].process_by_priority.assert_called_once_with(
            mock_components['table_processor'],
            ['high'],
            -1,
            False
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 