"""
Unit tests for PriorityProcessor focused on pure unit testing with comprehensive mocking.

This test suite follows the hybrid testing strategy:
- Unit tests with comprehensive mocking
- Fast execution (< 1 second per component)
- Isolated component behavior testing
- Core logic and edge cases

Testing Strategy:
- Pure unit tests with mocked dependencies
- Fast execution for rapid feedback
- Focus on core logic and edge cases
- Comprehensive mocking at appropriate abstraction level
"""

import pytest
import time
from unittest.mock import MagicMock, patch, call
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Import the component under test
from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.config.settings import Settings


class TestPriorityProcessorUnit:
    """Pure unit tests for PriorityProcessor class."""
    
    # Fixtures are now provided by conftest.py:
    # - mock_priority_processor_settings (renamed from mock_settings)
    # - mock_priority_processor_table_processor (renamed from mock_table_processor)
    # - priority_processor


class TestInitializationUnit(TestPriorityProcessorUnit):
    """Unit tests for PriorityProcessor initialization."""
    
    @pytest.mark.unit
    def test_initialization_with_settings(self, mock_priority_processor_settings):
        """Test successful initialization with provided settings."""
        processor = PriorityProcessor(settings=mock_priority_processor_settings)
        
        assert processor.settings == mock_priority_processor_settings
    
    @pytest.mark.unit
    def test_initialization_without_settings(self):
        """Test initialization with default settings."""
        with patch('etl_pipeline.orchestration.priority_processor.Settings') as mock_settings_class:
            mock_settings = MagicMock(spec=Settings)
            mock_settings_class.return_value = mock_settings
            
            processor = PriorityProcessor()
            
            assert processor.settings == mock_settings
            mock_settings_class.assert_called_once()


class TestCoreLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for core PriorityProcessor logic."""
    
    @pytest.mark.unit
    def test_get_tables_by_priority_calls_settings(self, priority_processor, mock_priority_processor_table_processor):
        """Test that settings.get_tables_by_priority is called for each importance level."""
        priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify settings was called for each importance level
        expected_calls = [
            call('critical'),
            call('important'),
            call('audit'),
            call('reference')
        ]
        priority_processor.settings.get_tables_by_priority.assert_has_calls(expected_calls)
        assert priority_processor.settings.get_tables_by_priority.call_count == 4
    
    @pytest.mark.unit
    def test_empty_tables_skipped(self, priority_processor, mock_priority_processor_table_processor):
        """Test that empty table lists are properly handled."""
        # Mock settings to return empty list for 'critical'
        priority_processor.settings.get_tables_by_priority.side_effect = lambda importance, **kwargs: []
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'])
        
        # Verify critical level is skipped entirely (not added to results)
        assert 'critical' not in results
        assert results == {}
        
        # Reset the mock to restore original behavior for other tests
        priority_processor.settings.get_tables_by_priority.side_effect = lambda importance, **kwargs: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
    
    @pytest.mark.unit
    def test_custom_importance_levels(self, priority_processor, mock_priority_processor_table_processor):
        """Test processing with custom importance levels."""
        custom_levels = ['critical', 'important']
        
        results = priority_processor.process_by_priority(
            mock_priority_processor_table_processor, 
            importance_levels=custom_levels
        )
        
        # Verify only specified levels were processed
        assert 'critical' in results
        assert 'important' in results
        assert 'audit' not in results
        assert 'reference' not in results
        
        # Verify settings was called for each custom level
        assert priority_processor.settings.get_tables_by_priority.call_count == 2
    
    @pytest.mark.unit
    def test_empty_importance_levels(self, priority_processor, mock_priority_processor_table_processor):
        """Test processing with empty importance levels."""
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=[])
        
        assert results == {}


class TestParallelProcessingLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for parallel processing logic."""
    
    @pytest.mark.unit
    def test_parallel_processing_decision_logic(self, priority_processor, mock_priority_processor_table_processor):
        """Test the decision logic for parallel vs sequential processing."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures with proper side effects
            mock_futures = []
            for i in range(3):
                mock_future = MagicMock()
                mock_future.result.return_value = True
                mock_futures.append(mock_future)
            
            mock_executor.submit.side_effect = mock_futures
            mock_as_completed.return_value = mock_futures
            
            # Only process critical tables to test parallel processing decision
            priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'], max_workers=3)
            
            # Verify ThreadPoolExecutor was used for critical tables (3 tables > 1)
            mock_executor_class.assert_called_once_with(max_workers=3)
            assert mock_executor.submit.call_count == 3
    
    @pytest.mark.unit
    def test_sequential_processing_decision_logic(self, priority_processor, mock_priority_processor_table_processor):
        """Test that single critical table uses sequential processing."""
        # Mock settings to return only one critical table
        priority_processor.settings.get_tables_by_priority.side_effect = lambda importance, **kwargs: {
            'critical': ['patient'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class:
            priority_processor.process_by_priority(mock_priority_processor_table_processor)
            
            # Verify ThreadPoolExecutor was NOT used (single table)
            mock_executor_class.assert_not_called()
    
    @pytest.mark.unit
    def test_parallel_processing_future_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test handling of futures in parallel processing."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures with mixed results
            mock_future1 = MagicMock()
            mock_future2 = MagicMock()
            mock_future3 = MagicMock()
            
            mock_future1.result.return_value = True
            mock_future2.result.return_value = False
            mock_future3.result.return_value = True
            
            mock_executor.submit.side_effect = [mock_future1, mock_future2, mock_future3]
            mock_as_completed.return_value = [mock_future1, mock_future2, mock_future3]
            
            # Only process critical tables to test parallel processing
            results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'])
            
            # Verify futures were processed correctly
            critical_result = results['critical']
            assert len(critical_result['success']) == 2
            assert len(critical_result['failed']) == 1
    
    @pytest.mark.unit
    def test_parallel_processing_exception_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test exception handling in parallel processing."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock future that raises exception
            mock_future = MagicMock()
            mock_future.result.side_effect = Exception("Processing failed")
            
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Only process critical tables to test parallel processing
            results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'])
            
            # Verify exception was handled and table marked as failed
            critical_result = results['critical']
            assert len(critical_result['failed']) == 1
            assert len(critical_result['success']) == 0


class TestSequentialProcessingLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for sequential processing logic."""
    
    @pytest.mark.unit
    def test_sequential_processing_calls(self, priority_processor, mock_priority_processor_table_processor):
        """Test that sequential processing calls process_table for each table."""
        priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify process_table was called for each table
        # 3 critical (parallel) + 3 important + 2 audit + 2 reference = 10 total
        assert mock_priority_processor_table_processor.process_table.call_count == 10
        
        # Verify calls were made with correct parameters (positional arguments)
        expected_calls = [
            call('patient', False),
            call('appointment', False),
            call('procedurelog', False),
            call('payment', False),
            call('claim', False),
            call('insplan', False),
            call('securitylog', False),
            call('entrylog', False),
            call('zipcode', False),
            call('definition', False)
        ]
        mock_priority_processor_table_processor.process_table.assert_has_calls(expected_calls)
    
    @pytest.mark.unit
    def test_sequential_processing_force_full_parameter(self, priority_processor, mock_priority_processor_table_processor):
        """Test that force_full parameter is passed correctly in sequential processing."""
        priority_processor.process_by_priority(mock_priority_processor_table_processor, force_full=True)
        
        # Verify force_full=True was passed to all process_table calls
        for call_args in mock_priority_processor_table_processor.process_table.call_args_list:
            # force_full is the second positional argument
            assert call_args[0][1] is True  # args[1] is force_full
    
    @pytest.mark.unit
    def test_sequential_processing_failure_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test failure handling in sequential processing."""
        # Mock table processor to fail for some tables
        def mock_process_table(table, force_full, **kwargs):
            return table not in ['payment', 'claim']  # Only insplan succeeds
        
        mock_priority_processor_table_processor.process_table.side_effect = mock_process_table
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify important tables show failures
        important_result = results['important']
        assert len(important_result['success']) == 1
        assert len(important_result['failed']) == 2
    
    @pytest.mark.unit
    def test_sequential_processing_exception_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test exception handling in sequential processing."""
        # Mock table processor to raise exception
        mock_priority_processor_table_processor.process_table.side_effect = Exception("Processing failed")
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify exception was handled and table marked as failed
        # Critical tables use parallel processing, so all 3 fail
        critical_result = results['critical']
        assert len(critical_result['failed']) == 3  # All critical tables fail in parallel
        assert len(critical_result['success']) == 0
        
        # Processing stops after critical failure, so no other levels processed
        assert len(results) == 1


class TestResourceManagementUnit(TestPriorityProcessorUnit):
    """Unit tests for resource management."""
    
    @pytest.mark.unit
    def test_thread_pool_context_manager(self, priority_processor, mock_priority_processor_table_processor):
        """Test that ThreadPoolExecutor uses context manager properly."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures
            mock_future = MagicMock()
            mock_future.result.return_value = True
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Only process critical tables to test parallel processing
            priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'])
            
            # Verify context manager was used (cleanup)
            mock_executor.__enter__.assert_called_once()
            mock_executor.__exit__.assert_called_once()
    
    @pytest.mark.unit
    def test_max_workers_parameter(self, priority_processor, mock_priority_processor_table_processor):
        """Test that max_workers parameter is passed correctly."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures
            mock_future = MagicMock()
            mock_future.result.return_value = True
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Only process critical tables to test parallel processing
            priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'], max_workers=10)
            
            # Verify ThreadPoolExecutor was created with correct max_workers
            mock_executor_class.assert_called_once_with(max_workers=10)
    
    @pytest.mark.unit
    def test_default_max_workers(self, priority_processor, mock_priority_processor_table_processor):
        """Test that default max_workers is used when not specified."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures
            mock_future = MagicMock()
            mock_future.result.return_value = True
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Only process critical tables to test parallel processing
            priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'])
            
            # Verify default max_workers=5 was used
            mock_executor_class.assert_called_once_with(max_workers=5)


class TestErrorHandlingUnit(TestPriorityProcessorUnit):
    """Unit tests for error handling."""
    
    @pytest.mark.unit
    def test_settings_exception_propagation(self, priority_processor, mock_priority_processor_table_processor):
        """Test that settings exceptions are properly propagated."""
        # Mock settings to raise exception
        priority_processor.settings.get_tables_by_priority.side_effect = Exception("Settings error")
        
        with pytest.raises(Exception, match="Settings error"):
            priority_processor.process_by_priority(mock_priority_processor_table_processor)
    
    @pytest.mark.unit
    def test_table_processor_exception_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test that table processor exceptions are properly handled."""
        # Mock table processor to raise exception
        mock_priority_processor_table_processor.process_table.side_effect = Exception("Table processing failed")
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify exception was handled and table marked as failed
        # Critical tables use parallel processing, so all 3 fail
        critical_result = results['critical']
        assert len(critical_result['failed']) == 3  # All critical tables fail in parallel
        assert len(critical_result['success']) == 0
        
        # Processing stops after critical failure, so no other levels processed
        assert len(results) == 1
    
    @pytest.mark.unit
    def test_invalid_importance_levels(self, priority_processor, mock_priority_processor_table_processor):
        """Test processing with invalid importance levels."""
        # Mock settings to return empty list for invalid levels
        priority_processor.settings.get_tables_by_priority.side_effect = lambda importance, **kwargs: []
        
        results = priority_processor.process_by_priority(
            mock_priority_processor_table_processor, 
            importance_levels=['invalid_level']
        )
        
        # Verify invalid level is skipped entirely (not added to results)
        assert 'invalid_level' not in results
        assert results == {}
        
        # Reset the mock to restore original behavior for other tests
        priority_processor.settings.get_tables_by_priority.side_effect = lambda importance, **kwargs: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])


class TestCriticalFailureLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for critical failure handling logic."""
    
    @pytest.mark.unit
    def test_critical_failure_stops_processing(self, priority_processor, mock_priority_processor_table_processor):
        """Test that critical table failures stop processing."""
        # Mock table processor to fail for critical tables
        mock_priority_processor_table_processor.process_table.side_effect = lambda table, force_full: False
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify critical tables failed
        critical_result = results['critical']
        assert len(critical_result['failed']) == 3
        assert len(critical_result['success']) == 0
        
        # Verify processing stopped after critical failure
        # Only critical level should be in results
        assert len(results) == 1
        assert 'important' not in results
        assert 'audit' not in results
        assert 'reference' not in results
    
    @pytest.mark.unit
    def test_non_critical_failures_continue_processing(self, priority_processor, mock_priority_processor_table_processor):
        """Test that non-critical failures don't stop processing."""
        # Mock table processor to fail for important tables but succeed for critical
        def mock_process_table(table, force_full, **kwargs):
            if table in ['payment', 'claim']:
                return False
            return True
        
        mock_priority_processor_table_processor.process_table.side_effect = mock_process_table
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify critical tables succeeded
        critical_result = results['critical']
        assert len(critical_result['success']) == 3
        assert len(critical_result['failed']) == 0
        
        # Verify important tables show failures but processing continued
        important_result = results['important']
        assert len(important_result['success']) == 1
        assert len(important_result['failed']) == 2
        
        # Verify processing continued to audit level
        assert 'audit' in results


class TestPerformanceUnit(TestPriorityProcessorUnit):
    """Unit tests for performance characteristics."""
    
    @pytest.mark.unit
    @pytest.mark.performance
    def test_processing_speed(self, priority_processor, mock_priority_processor_table_processor):
        """Test that processing completes within reasonable time."""
        start_time = time.time()
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        end_time = time.time()
        
        # Verify processing completed
        assert 'critical' in results
        assert 'important' in results
        
        # Verify reasonable processing time (should be very fast with mocks)
        processing_time = end_time - start_time
        assert processing_time < 0.1  # Should be very fast with mocks
    
    @pytest.mark.unit
    @pytest.mark.performance
    def test_memory_efficiency(self, priority_processor, mock_priority_processor_table_processor):
        """Test that processing doesn't consume excessive memory."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Process tables
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Verify reasonable memory usage (should be minimal with mocks)
        assert memory_increase < 10 * 1024 * 1024  # Less than 10MB increase


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "unit"]) 