"""
Comprehensive tests for PriorityProcessor focused on intelligent parallelization and priority-based processing.

This test suite addresses critical testing gaps identified in TESTING_PLAN.md:
- Current coverage: 0% → Target: 90%
- Critical for production deployment
- Tests parallel processing, sequential processing, and error handling

Testing Strategy:
- Comprehensive tests with mocked components
- Full functionality testing with mocked dependencies
- Error handling and failure propagation tests
- Resource management and cleanup validation
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
from etl_pipeline.core.schema_discovery import SchemaDiscovery


class TestPriorityProcessor:
    """Comprehensive tests for PriorityProcessor class."""


class TestInitialization(TestPriorityProcessor):
    """Test PriorityProcessor initialization."""
    
    @pytest.mark.unit
    def test_initialization_with_settings(self, mock_priority_processor_settings):
        """Test successful initialization with provided settings."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_priority_processor_settings)
        
        assert processor.settings == mock_priority_processor_settings
        assert processor.schema_discovery == mock_schema_discovery
    
    @pytest.mark.unit
    def test_initialization_without_settings(self):
        """Test initialization with default settings."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        with patch('etl_pipeline.orchestration.priority_processor.Settings') as mock_settings_class:
            mock_settings = MagicMock(spec=Settings)
            mock_settings_class.return_value = mock_settings
            
            processor = PriorityProcessor(schema_discovery=mock_schema_discovery)
            
            assert processor.settings == mock_settings
            assert processor.schema_discovery == mock_schema_discovery
            mock_settings_class.assert_called_once()


class TestProcessByPriority(TestPriorityProcessor):
    """Test process_by_priority method."""
    
    @pytest.mark.unit
    def test_process_by_priority_success(self, priority_processor, mock_priority_processor_table_processor):
        """Test successful processing of tables by priority."""
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify results structure
        assert 'critical' in results
        assert 'important' in results
        assert 'audit' in results
        assert 'reference' in results
        
        # Verify critical tables were processed
        critical_result = results['critical']
        assert critical_result['total'] == 3
        assert len(critical_result['success']) == 3
        assert len(critical_result['failed']) == 0
        
        # Verify settings was called for each importance level
        assert priority_processor.settings.get_tables_by_importance.call_count == 4
    
    @pytest.mark.unit
    def test_process_by_priority_custom_levels(self, priority_processor, mock_priority_processor_table_processor):
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
        assert priority_processor.settings.get_tables_by_importance.call_count == 2
    
    @pytest.mark.unit
    def test_process_by_priority_no_tables(self, priority_processor, mock_priority_processor_table_processor):
        """Test processing when no tables are found for an importance level."""
        # Mock settings to return empty list for 'critical'
        priority_processor.settings.get_tables_by_importance.side_effect = lambda importance: []
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'])
        
        # Verify critical level is skipped entirely (not added to results)
        assert 'critical' not in results
        assert results == {}
        
        # Reset the mock to restore original behavior for other tests
        priority_processor.settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
    
    @pytest.mark.unit
    def test_process_by_priority_critical_failure_stops_processing(self, priority_processor, mock_priority_processor_table_processor):
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
    def test_process_by_priority_force_full_parameter(self, priority_processor, mock_priority_processor_table_processor):
        """Test that force_full parameter is passed to table processor."""
        priority_processor.process_by_priority(mock_priority_processor_table_processor, force_full=True)
        
        # Verify force_full=True was passed to all process_table calls
        for call_args in mock_priority_processor_table_processor.process_table.call_args_list:
            # force_full is the second positional argument
            assert call_args[0][1] is True  # args[1] is force_full
    
    @pytest.mark.unit
    def test_process_by_priority_empty_importance_levels(self, priority_processor, mock_priority_processor_table_processor):
        """Test processing with empty importance levels."""
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=[])
        
        assert results == {}


class TestParallelProcessing(TestPriorityProcessor):
    """Test parallel processing functionality."""
    
    @pytest.mark.unit
    def test_parallel_processing_critical_tables(self, priority_processor, mock_priority_processor_table_processor):
        """Test that critical tables are processed in parallel."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures
            mock_future1 = MagicMock()
            mock_future2 = MagicMock()
            mock_future3 = MagicMock()
            
            mock_future1.result.return_value = True
            mock_future2.result.return_value = True
            mock_future3.result.return_value = True
            
            mock_executor.submit.side_effect = [mock_future1, mock_future2, mock_future3]
            mock_as_completed.return_value = [mock_future1, mock_future2, mock_future3]
            
            # Only process critical tables to test parallel processing
            results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'], max_workers=3)
            
            # Verify ThreadPoolExecutor was used
            mock_executor_class.assert_called_once_with(max_workers=3)
            
            # Verify all critical tables were submitted
            assert mock_executor.submit.call_count == 3
            
            # Verify results
            critical_result = results['critical']
            assert len(critical_result['success']) == 3
            assert len(critical_result['failed']) == 0
    
    @pytest.mark.unit
    def test_parallel_processing_with_failures(self, priority_processor, mock_priority_processor_table_processor):
        """Test parallel processing with some failures."""
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
            
            # Verify results show mixed success/failure
            critical_result = results['critical']
            assert len(critical_result['success']) == 2
            assert len(critical_result['failed']) == 1
    
    @pytest.mark.unit
    def test_parallel_processing_exception_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test parallel processing with exceptions."""
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
    
    @pytest.mark.unit
    def test_parallel_processing_single_critical_table(self, priority_processor, mock_priority_processor_table_processor):
        """Test that single critical table is processed sequentially (not parallel)."""
        # Mock settings to return only one critical table
        priority_processor.settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify single critical table was processed (not in parallel)
        critical_result = results['critical']
        assert critical_result['total'] == 1
        assert len(critical_result['success']) == 1
        assert len(critical_result['failed']) == 0
        
        # Verify process_table was called for all tables: 1 + 3 + 2 + 2 = 8
        assert mock_priority_processor_table_processor.process_table.call_count == 8  # All tables processed
        
        # Reset the mock to restore original behavior for other tests
        priority_processor.settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])


class TestSequentialProcessing(TestPriorityProcessor):
    """Test sequential processing functionality."""
    
    @pytest.mark.unit
    def test_sequential_processing_non_critical_tables(self, priority_processor, mock_priority_processor_table_processor):
        """Test that non-critical tables are processed sequentially."""
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify important tables were processed sequentially
        important_result = results['important']
        assert important_result['total'] == 3
        assert len(important_result['success']) == 3
        assert len(important_result['failed']) == 0
        
        # Verify process_table was called for all tables: 3 + 3 + 2 + 2 = 10
        assert mock_priority_processor_table_processor.process_table.call_count == 10  # All tables processed
    
    @pytest.mark.unit
    def test_sequential_processing_with_failures(self, priority_processor, mock_priority_processor_table_processor):
        """Test sequential processing with failures."""
        # Mock table processor to fail for important tables
        def mock_process_table(table, force_full, **kwargs):
            return table not in ['payment']  # Only payment fails
        
        mock_priority_processor_table_processor.process_table.side_effect = mock_process_table
        
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify important tables show failures
        important_result = results['important']
        assert len(important_result['success']) == 2  # claim and insplan succeed
        assert len(important_result['failed']) == 1   # payment fails
    
    @pytest.mark.unit
    def test_sequential_processing_exception_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test sequential processing with exceptions."""
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
    
    @pytest.mark.unit
    def test_sequential_processing_order(self, priority_processor, mock_priority_processor_table_processor):
        """Test that sequential processing maintains order."""
        processed_tables = []
        
        def mock_process_table(table, force_full, **kwargs):
            processed_tables.append(table)
            return True
        
        mock_priority_processor_table_processor.process_table.side_effect = mock_process_table
        
        priority_processor.process_by_priority(mock_priority_processor_table_processor)
        
        # Verify tables were processed in order (all levels)
        expected_order = ['patient', 'appointment', 'procedurelog',  # critical (parallel)
                         'payment', 'claim', 'insplan',              # important (sequential)
                         'securitylog', 'entrylog',                  # audit (sequential)
                         'zipcode', 'definition']                    # reference (sequential)
        assert processed_tables == expected_order


class TestResourceManagement(TestPriorityProcessor):
    """Test resource management and cleanup."""
    
    @pytest.mark.unit
    def test_thread_pool_cleanup(self, priority_processor, mock_priority_processor_table_processor):
        """Test that ThreadPoolExecutor is properly cleaned up."""
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
    def test_max_workers_configuration(self, priority_processor, mock_priority_processor_table_processor):
        """Test that max_workers is properly configured."""
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


class TestErrorHandling(TestPriorityProcessor):
    """Test error handling and edge cases."""
    
    @pytest.mark.unit
    def test_invalid_importance_levels(self, priority_processor, mock_priority_processor_table_processor):
        """Test processing with invalid importance levels."""
        # Mock settings to return empty list for invalid levels
        priority_processor.settings.get_tables_by_importance.side_effect = lambda importance: []
        
        results = priority_processor.process_by_priority(
            mock_priority_processor_table_processor, 
            importance_levels=['invalid_level']
        )
        
        # Verify invalid level is skipped entirely (not added to results)
        assert 'invalid_level' not in results
        assert results == {}
        
        # Reset the mock to restore original behavior for other tests
        priority_processor.settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
    
    @pytest.mark.unit
    def test_settings_exception_handling(self, priority_processor, mock_priority_processor_table_processor):
        """Test handling of settings exceptions."""
        # Mock settings to raise exception
        priority_processor.settings.get_tables_by_importance.side_effect = Exception("Settings error")
        
        with pytest.raises(Exception, match="Settings error"):
            priority_processor.process_by_priority(mock_priority_processor_table_processor)
    
    @pytest.mark.unit
    def test_table_processor_exception_propagation(self, priority_processor, mock_priority_processor_table_processor):
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


class TestPerformance(TestPriorityProcessor):
    """Performance tests."""
    
    @pytest.mark.performance
    def test_parallel_processing_performance(self, priority_processor, mock_priority_processor_table_processor):
        """Test parallel processing performance."""
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock futures with timing
            mock_futures = []
            for i in range(3):
                mock_future = MagicMock()
                mock_future.result.return_value = True
                mock_futures.append(mock_future)
            
            mock_executor.submit.side_effect = mock_futures
            mock_as_completed.return_value = mock_futures
            
            start_time = time.time()
            # Only process critical tables to test parallel processing
            results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['critical'], max_workers=3)
            end_time = time.time()
            
            # Verify processing completed
            assert 'critical' in results
            assert results['critical']['total'] == 3
            
            # Verify reasonable processing time (should be fast with mocks)
            processing_time = end_time - start_time
            assert processing_time < 1.0  # Should be very fast with mocks
    
    @pytest.mark.performance
    def test_sequential_processing_performance(self, priority_processor, mock_priority_processor_table_processor):
        """Test sequential processing performance."""
        start_time = time.time()
        # Only process important tables to test sequential processing
        results = priority_processor.process_by_priority(mock_priority_processor_table_processor, importance_levels=['important'])
        end_time = time.time()
        
        # Verify processing completed
        assert 'important' in results
        assert results['important']['total'] == 3
        
        # Verify reasonable processing time
        processing_time = end_time - start_time
        assert processing_time < 1.0  # Should be fast with mocks


if __name__ == "__main__":
    pytest.main([__file__, "-v"])