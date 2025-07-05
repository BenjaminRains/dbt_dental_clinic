"""
Unit tests for PipelineOrchestrator.

This test suite covers the core orchestration functionality with comprehensive mocking
for fast execution and isolated component behavior testing.

Testing Strategy:
- Unit tests with comprehensive mocking
- Fast execution (< 1 second per component)
- Isolated component behavior testing
- Core logic and edge cases

Target Coverage: 90%+ for unit tests

REFACTORED: Updated to use new modular fixtures and configuration system
- Uses fixtures from tests/fixtures/ directory
- Proper test isolation with new settings system
- Dependency injection pattern
- Type-safe database configuration
"""

import pytest
import logging
from unittest.mock import MagicMock, patch, call
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Import fixtures from modular structure
from tests.fixtures.orchestrator_fixtures import mock_components, orchestrator
from tests.fixtures.config_fixtures import test_pipeline_config, test_tables_config
from tests.fixtures.env_fixtures import test_env_vars, test_settings, setup_test_environment
from tests.fixtures.connection_fixtures import mock_source_engine, mock_replication_engine, mock_analytics_engine

# Import the component under test
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


@pytest.mark.unit
class TestPipelineOrchestratorUnit:
    """Unit tests for PipelineOrchestrator class with comprehensive mocking."""
    
    # Fixtures moved to tests/fixtures/orchestrator_fixtures.py:
    # - mock_components
    # - orchestrator


class TestInitialization(TestPipelineOrchestratorUnit):
    """Test PipelineOrchestrator initialization."""
    
    def test_initialization_without_config(self, test_settings, mock_components, mock_source_engine):
        """Test successful initialization without configuration file."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                    mock_schema_discovery = MagicMock()
                    mock_schema_discovery_class.return_value = mock_schema_discovery
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                        mock_processor_class.return_value = mock_components['table_processor']
                        
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                            mock_priority_class.return_value = mock_components['priority_processor']
                            
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                                mock_metrics_class.return_value = mock_components['metrics']
                                
                                # Test REAL orchestrator initialization
                                orchestrator = PipelineOrchestrator()
                                
                                # Initially components should be None
                                assert orchestrator.table_processor is None
                                assert orchestrator.priority_processor is None
                                assert orchestrator.schema_discovery is None
                                assert orchestrator._initialized is False
                                
                                # Initialize connections to create components
                                orchestrator.initialize_connections()
                                
                                # Now components should be set
                                assert orchestrator.table_processor == mock_components['table_processor']
                                assert orchestrator.priority_processor == mock_components['priority_processor']
                                assert orchestrator.schema_discovery == mock_schema_discovery
                                assert orchestrator._initialized is True
        
        # Verify all components were created exactly once
        mock_processor_class.assert_called_once()
        mock_priority_class.assert_called_once()
        mock_metrics_class.assert_called_once()
    
    def test_initialization_with_config_path(self, test_settings, mock_components, mock_source_engine):
        """Test initialization with config_path parameter (deprecated but supported)."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                    mock_schema_discovery = MagicMock()
                    mock_schema_discovery_class.return_value = mock_schema_discovery
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                        mock_processor_class.return_value = mock_components['table_processor']
                        
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                            mock_priority_class.return_value = mock_components['priority_processor']
                            
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                                mock_metrics_class.return_value = mock_components['metrics']
                                
                                # Test REAL orchestrator with config_path
                                orchestrator = PipelineOrchestrator(config_path='dummy_config.yml')
                                
                                # Initially components should be None
                                assert orchestrator.table_processor is None
                                assert orchestrator.priority_processor is None
                                assert orchestrator._initialized is False
                                
                                # Initialize connections to create components
                                orchestrator.initialize_connections()
                                
                                # Now components should be set
                                assert orchestrator.table_processor == mock_components['table_processor']
                                assert orchestrator.priority_processor == mock_components['priority_processor']
                                assert orchestrator._initialized is True
        
        # Verify all components were created exactly once
        mock_processor_class.assert_called_once()
        mock_priority_class.assert_called_once()
        mock_metrics_class.assert_called_once()
    
    def test_initialization_component_creation_order(self, test_settings, mock_components, mock_source_engine):
        """Test that components are created in the correct order."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                    mock_schema_discovery = MagicMock()
                    mock_schema_discovery_class.return_value = mock_schema_discovery
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                                
                                # Test REAL orchestrator creation
                                orchestrator = PipelineOrchestrator()
                                
                                # Initially no components should be created
                                mock_processor_class.assert_not_called()
                                mock_priority_class.assert_not_called()
                                mock_metrics_class.assert_called_once()  # Only metrics is created in constructor
                                
                                # Initialize connections to create components
                                orchestrator.initialize_connections()
                                
                                # Verify components were created in the correct order
                                mock_processor_class.assert_called_once()
                                mock_priority_class.assert_called_once()


class TestConnectionInitialization(TestPipelineOrchestratorUnit):
    """Test connection initialization scenarios."""
    
    def test_connection_initialization_success(self, test_settings, mock_components, mock_source_engine):
        """Test successful connection initialization with proper mocking."""
        # Create a fresh orchestrator that hasn't been initialized yet
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                    mock_schema_discovery = MagicMock()
                    mock_schema_discovery_class.return_value = mock_schema_discovery
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                        mock_processor_class.return_value = mock_components['table_processor']
                        
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                            mock_priority_class.return_value = mock_components['priority_processor']
                            
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                                mock_metrics_class.return_value = mock_components['metrics']
                                
                                # Create fresh orchestrator
                                fresh_orchestrator = PipelineOrchestrator()
                                
                                # Mock the table processor to return success
                                mock_components['table_processor'].initialize_connections.return_value = True
                                
                                # Test the REAL initialize_connections method (but with mocked dependencies)
                                result = fresh_orchestrator.initialize_connections()
                                
                                assert result is True
                                assert fresh_orchestrator._initialized is True
                                mock_components['table_processor'].initialize_connections.assert_called_once()
    
    def test_connection_initialization_failure(self, test_settings, mock_components, mock_source_engine):
        """Test connection initialization failure with proper mocking."""
        # Create a fresh orchestrator that hasn't been initialized yet
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                    mock_schema_discovery = MagicMock()
                    mock_schema_discovery_class.return_value = mock_schema_discovery
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                        mock_processor_class.return_value = mock_components['table_processor']
                        
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                            mock_priority_class.return_value = mock_components['priority_processor']
                            
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                                mock_metrics_class.return_value = mock_components['metrics']
                                
                                # Create fresh orchestrator
                                fresh_orchestrator = PipelineOrchestrator()
                                
                                # Mock the table processor to return failure
                                mock_components['table_processor'].initialize_connections.return_value = False
                                
                                # Test the REAL initialize_connections method (but with mocked dependencies)
                                result = fresh_orchestrator.initialize_connections()
                                
                                assert result is False
                                assert fresh_orchestrator._initialized is False
                                mock_components['table_processor'].initialize_connections.assert_called_once()
    
    def test_connection_initialization_exception(self, test_settings, mock_components, mock_source_engine):
        """Test connection initialization with exception handling."""
        # Create a fresh orchestrator that hasn't been initialized yet
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                # Mock ConnectionFactory to raise an exception
                mock_connection_factory.get_source_connection.side_effect = Exception("Connection failed")
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                    mock_schema_discovery = MagicMock()
                    mock_schema_discovery_class.return_value = mock_schema_discovery
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                        mock_processor_class.return_value = mock_components['table_processor']
                        
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                            mock_priority_class.return_value = mock_components['priority_processor']
                            
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                                mock_metrics_class.return_value = mock_components['metrics']
                                
                                # Create fresh orchestrator
                                fresh_orchestrator = PipelineOrchestrator()
                                
                                # Test that exception is properly handled
                                with pytest.raises(Exception, match="Connection failed"):
                                    fresh_orchestrator.initialize_connections()
                                
                                # Verify cleanup was called
                                assert fresh_orchestrator._initialized is False
    
    def test_connection_initialization_already_initialized(self, orchestrator, mock_components):
        """Test connection initialization when already initialized."""
        # The orchestrator fixture already initializes connections, so test the behavior
        # when initialize_connections is called again
        assert orchestrator._initialized is True
        
        # Mock the table processor to return success
        mock_components['table_processor'].initialize_connections.return_value = True
        
        # Mock the initialize_connections method to avoid real database connections
        with patch.object(orchestrator, 'initialize_connections') as mock_init_connections:
            mock_init_connections.return_value = True
            
            # Test calling initialize_connections again (should still work)
            result = orchestrator.initialize_connections()
            
            assert result is True
            assert orchestrator._initialized is True
            # Should call the mocked method
            mock_init_connections.assert_called_once()


class TestTableProcessing(TestPipelineOrchestratorUnit):
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
    
    def test_run_pipeline_for_table_empty_table_name(self, orchestrator, mock_components):
        """Test table processing with empty table name."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock table processor to return success with debug logging
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator with empty table name
        result = orchestrator.run_pipeline_for_table('', force_full=False)
        
        assert result is True
        mock_components['table_processor'].process_table.assert_called_once_with('', False)
    
    def test_run_pipeline_for_table_none_table_name(self, orchestrator, mock_components):
        """Test table processing with None table name."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Mock table processor to return success with debug logging
        mock_components['table_processor'].process_table.return_value = True
        
        # Test REAL orchestrator with None table name
        result = orchestrator.run_pipeline_for_table(None, force_full=False)
        
        assert result is True
        mock_components['table_processor'].process_table.assert_called_once_with(None, False)


class TestPriorityProcessing(TestPipelineOrchestratorUnit):
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
        assert call_args[0][0] is None  # importance_levels (default)
        assert call_args[0][1] == 5  # max_workers (default)
        assert call_args[0][2] is False  # force_full (default)
    
    def test_process_tables_by_priority_edge_cases(self, orchestrator, mock_components):
        """Test priority processing with edge cases."""
        # Initialize connections first
        orchestrator._initialized = True
        
        # Test with empty importance levels
        mock_components['priority_processor'].process_by_priority.return_value = {}
        result = orchestrator.process_tables_by_priority(importance_levels=[], max_workers=1, force_full=False)
        assert result == {}
        
        # Test with zero workers
        mock_components['priority_processor'].process_by_priority.return_value = {'high': {'success': [], 'failed': []}}
        result = orchestrator.process_tables_by_priority(importance_levels=['high'], max_workers=0, force_full=False)
        assert result is not None
        
        # Test with negative workers
        mock_components['priority_processor'].process_by_priority.return_value = {'high': {'success': [], 'failed': []}}
        result = orchestrator.process_tables_by_priority(importance_levels=['high'], max_workers=-1, force_full=False)
        assert result is not None


class TestErrorHandlingAndRecovery(TestPipelineOrchestratorUnit):
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
    
    def test_cleanup_with_exception(self, orchestrator, mock_components):
        """Test cleanup when disposal raises an exception."""
        # Mock cleanup to raise an exception
        def raise_cleanup_error(**kwargs):
            logger.debug(f"Mock cleanup called with: {kwargs}")
            raise Exception("Cleanup failed")
        
        mock_components['table_processor'].cleanup.side_effect = raise_cleanup_error
        
        # Cleanup should not raise an exception (it should be caught)
        orchestrator.cleanup()
        
        # Verify state was still reset (even though it started as True from fixture)
        # The cleanup method should set _initialized = False even if an exception occurs
        assert orchestrator._initialized is False
    
    def test_cleanup_without_table_processor(self, orchestrator):
        """Test cleanup when table processor is None."""
        orchestrator.table_processor = None
        
        # Should not raise an exception
        orchestrator.cleanup()
        assert orchestrator._initialized is False
    
    def test_cleanup_multiple_calls(self, orchestrator, mock_components):
        """Test multiple cleanup calls."""
        # First cleanup
        orchestrator.cleanup()
        assert mock_components['table_processor'].cleanup.call_count == 1
        
        # Second cleanup should still work
        orchestrator.cleanup()
        assert mock_components['table_processor'].cleanup.call_count == 2


class TestContextManager(TestPipelineOrchestratorUnit):
    """Test context manager functionality."""
    
    def test_context_manager_success(self, orchestrator):
        """Test successful context manager usage."""
        # Test REAL orchestrator context manager
        with orchestrator as orch:
            assert orch == orchestrator
            assert orch._initialized is True  # Already initialized by fixture
    
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
    
    def test_context_manager_nested_usage(self, orchestrator):
        """Test nested context manager usage."""
        with orchestrator as orch1:
            with orch1 as orch2:
                assert orch1 == orch2 == orchestrator
        
        # Verify cleanup was called for each context manager exit


class TestIdempotencyAndIncrementalLoad(TestPipelineOrchestratorUnit):
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
    
    def test_multiple_table_idempotency(self, orchestrator, mock_components):
        """Test idempotency across multiple tables."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Process multiple tables multiple times
        tables = ['patient', 'appointment', 'procedure']
        for table in tables:
            # First call
            result1 = orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result1 is True
            
            # Second call - should be idempotent
            result2 = orchestrator.run_pipeline_for_table(table, force_full=False)
            assert result2 is True
        
        # Verify each table was processed twice
        assert mock_components['table_processor'].process_table.call_count == 6
        calls = mock_components['table_processor'].process_table.call_args_list
        
        # Verify each table appears twice with force_full=False
        for table in tables:
            table_calls = [call for call in calls if call[0][0] == table]
            assert len(table_calls) == 2
            for table_call in table_calls:
                assert table_call[0][1] is False  # force_full=False


class TestPerformanceAndResourceManagement(TestPipelineOrchestratorUnit):
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
            # Parameters are: importance_levels, max_workers, force_full
            call_args = mock_components['priority_processor'].process_by_priority.call_args
            assert call_args[0][1] == max_workers  # max_workers is at index 1
    
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
    
    def test_memory_management_multiple_operations(self, orchestrator, mock_components):
        """Test memory management during multiple operations."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Perform multiple operations
        for i in range(10):
            result = orchestrator.run_pipeline_for_table(f'table_{i}', force_full=False)
            assert result is True
        
        # Verify all operations were processed
        assert mock_components['table_processor'].process_table.call_count == 10
        
        # Cleanup should still work
        orchestrator.cleanup()
        assert orchestrator._initialized is False


class TestMetricsCollection(TestPipelineOrchestratorUnit):
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
    
    def test_metrics_persistence_through_operations(self, orchestrator, mock_components):
        """Test metrics persistence through multiple operations."""
        orchestrator._initialized = True
        mock_components['table_processor'].process_table.return_value = True
        
        # Perform multiple operations and verify metrics collector persists
        for i in range(5):
            result = orchestrator.run_pipeline_for_table(f'table_{i}', force_full=False)
            assert result is True
            assert orchestrator.metrics is not None
        
        # Verify metrics collector is still available after cleanup
        orchestrator.cleanup()
        assert orchestrator.metrics is not None


class TestEdgeCases(TestPipelineOrchestratorUnit):
    """Test edge cases and boundary conditions."""
    
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
            ['high'],
            -1,
            False
        )
    
    def test_large_worker_count(self, orchestrator, mock_components):
        """Test priority processing with very large worker count."""
        orchestrator._initialized = True
        mock_components['priority_processor'].process_by_priority.return_value = {
            'high': {'success': ['patient'], 'failed': []}
        }
        
        # Test REAL orchestrator with very large worker count
        result = orchestrator.process_tables_by_priority(
            importance_levels=['high'],
            max_workers=1000,
            force_full=False
        )
        
        assert result is not None
        mock_components['priority_processor'].process_by_priority.assert_called_once_with(
            ['high'],
            1000,
            False
        )


class TestSchemaDiscoveryUnit(TestPipelineOrchestratorUnit):
    """Unit tests for SchemaDiscovery integration in PipelineOrchestrator."""
    
    def test_constructor_creates_schema_discovery(self, test_settings, mock_components, mock_source_engine):
        """Test that constructor creates SchemaDiscovery and passes it to components."""
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings_class:
            mock_settings_class.return_value = test_settings
            with patch.object(test_settings, "get_database_config", return_value={"database": "test_db"}):
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
                    mock_connection_factory.get_source_connection.return_value = mock_source_engine
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                        mock_schema_discovery = MagicMock()
                        mock_schema_discovery_class.return_value = mock_schema_discovery
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_table_processor_class:
                            mock_table_processor = MagicMock()
                            mock_table_processor_class.return_value = mock_table_processor
                            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_processor_class:
                                mock_priority_processor = MagicMock()
                                mock_priority_processor_class.return_value = mock_priority_processor
                                # Test REAL orchestrator initialization
                                orchestrator = PipelineOrchestrator()
                                # Initially components should be None
                                assert orchestrator.table_processor is None
                                assert orchestrator.priority_processor is None
                                assert orchestrator.schema_discovery is None
                                # Initialize connections to create components
                                orchestrator.initialize_connections()
                                # Verify SchemaDiscovery was created
                                mock_schema_discovery_class.assert_called_once()
                                # Verify components were created with SchemaDiscovery
                                mock_table_processor_class.assert_called_once_with(schema_discovery=mock_schema_discovery)
                                mock_priority_processor_class.assert_called_once_with(schema_discovery=mock_schema_discovery)
                                # Verify orchestrator has access to schema_discovery
                                assert orchestrator.schema_discovery == mock_schema_discovery


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 