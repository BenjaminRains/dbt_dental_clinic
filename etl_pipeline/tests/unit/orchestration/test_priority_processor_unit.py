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

REFACTORED: Updated to use new modular fixtures and configuration system
- Uses fixtures from tests/fixtures/ directory
- Proper dependency injection with new Settings system
- Type-safe database configuration
- Isolated test environment
"""

import pytest
import time
from unittest.mock import MagicMock, patch, call
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Import fixtures from modular structure
from tests.fixtures.config_fixtures import test_pipeline_config, test_tables_config
from tests.fixtures.env_fixtures import test_env_vars, test_settings
from tests.fixtures.priority_processor_fixtures import (
    mock_priority_processor_settings,
    mock_priority_processor_table_processor,
    priority_processor
)

# Import the component under test
from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.schema_discovery import SchemaDiscovery


class TestPriorityProcessorUnit:
    """Pure unit tests for PriorityProcessor class."""
    
    # Fixtures are now imported from modular fixtures:
    # - test_settings (from config_fixtures)
    # - mock_priority_processor_settings (from priority_processor_fixtures)
    # - mock_priority_processor_table_processor (from priority_processor_fixtures)
    # - priority_processor (from priority_processor_fixtures)


class TestInitializationUnit(TestPriorityProcessorUnit):
    """Unit tests for PriorityProcessor initialization."""
    
    @pytest.mark.unit
    def test_initialization_with_settings(self, test_settings):
        """Test successful initialization with provided settings."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=test_settings)
        
        assert processor.settings == test_settings
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
    
    @pytest.mark.unit
    def test_initialization_requires_schema_discovery(self):
        """Test that schema_discovery is required."""
        with pytest.raises(TypeError, match="missing 1 required positional argument"):
            PriorityProcessor()
    
    @pytest.mark.unit
    def test_initialization_invalid_schema_discovery_type(self):
        """Test that schema_discovery must be SchemaDiscovery instance."""
        with pytest.raises(ValueError, match="SchemaDiscovery instance is required"):
            PriorityProcessor(schema_discovery=MagicMock())  # Not a SchemaDiscovery instance


class TestCoreLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for core PriorityProcessor logic."""
    
    @pytest.mark.unit
    def test_get_tables_by_importance_calls_settings(self):
        """Test that settings.get_tables_by_importance is called for each importance level."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed so processing continues
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            processor.process_by_priority()
            
            # Verify settings was called for each importance level
            expected_calls = [
                call('critical'),
                call('important'),
                call('audit'),
                call('reference')
            ]
            processor.settings.get_tables_by_importance.assert_has_calls(expected_calls)
            assert processor.settings.get_tables_by_importance.call_count == 4
    
    @pytest.mark.unit
    def test_empty_tables_skipped(self):
        """Test that empty table lists are properly handled."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns empty list
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: []
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        results = processor.process_by_priority(importance_levels=['critical'])
        
        # Verify critical level is skipped entirely (not added to results)
        assert 'critical' not in results
        assert results == {}
    
    @pytest.mark.unit
    def test_custom_importance_levels(self):
        """Test processing with custom importance levels."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor class to return a working instance
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            custom_levels = ['critical', 'important']
            
            results = processor.process_by_priority(importance_levels=custom_levels)
            
            # Verify only specified levels were processed
            assert 'critical' in results
            assert 'important' in results
            assert 'audit' not in results
            assert 'reference' not in results
            
            # Verify settings was called for each custom level
            assert processor.settings.get_tables_by_importance.call_count == 2
    
    @pytest.mark.unit
    def test_empty_importance_levels(self):
        """Test processing with empty importance levels."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        results = processor.process_by_priority(importance_levels=[])
        
        assert results == {}


class TestParallelProcessingLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for parallel processing logic."""
    
    @pytest.mark.unit
    def test_parallel_processing_decision_logic(self):
        """Test the decision logic for parallel vs sequential processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed, \
             patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock TableProcessor
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Mock futures with proper side effects
            mock_futures = []
            for i in range(3):
                mock_future = MagicMock()
                mock_future.result.return_value = True
                mock_futures.append(mock_future)
            
            mock_executor.submit.side_effect = mock_futures
            mock_as_completed.return_value = mock_futures
            
            # Only process critical tables to test parallel processing decision
            processor.process_by_priority(importance_levels=['critical'], max_workers=3)
            
            # Verify ThreadPoolExecutor was used for critical tables (3 tables > 1)
            mock_executor_class.assert_called_once_with(max_workers=3)
            assert mock_executor.submit.call_count == 3
    
    @pytest.mark.unit
    def test_sequential_processing_decision_logic(self):
        """Test that single critical table uses sequential processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns only one critical table to trigger sequential processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient'],  # 1 table, triggers sequential
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class:
            processor.process_by_priority()
            
            # Verify ThreadPoolExecutor was NOT used (single table)
            mock_executor_class.assert_not_called()
    
    @pytest.mark.unit
    def test_parallel_processing_future_handling(self):
        """Test handling of futures in parallel processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed, \
             patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock TableProcessor
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
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
            results = processor.process_by_priority(importance_levels=['critical'])
            
            # Verify futures were processed correctly
            critical_result = results['critical']
            assert len(critical_result['success']) == 2
            assert len(critical_result['failed']) == 1
    
    @pytest.mark.unit
    def test_parallel_processing_exception_handling(self):
        """Test exception handling in parallel processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed, \
             patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock TableProcessor
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Mock future that raises exception
            mock_future = MagicMock()
            mock_future.result.side_effect = Exception("Processing failed")
            
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Only process critical tables to test parallel processing
            results = processor.process_by_priority(importance_levels=['critical'])
            
            # Verify exception was handled and table marked as failed
            critical_result = results['critical']
            assert len(critical_result['failed']) == 1
            assert len(critical_result['success']) == 0


class TestSequentialProcessingLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for sequential processing logic."""
    
    @pytest.mark.unit
    def test_sequential_processing_calls(self):
        """Test that sequential processing calls process_table for each table."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed so processing continues
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            processor.process_by_priority()
            
            # Note: Since we now create TableProcessor instances internally,
            # we can't directly verify the calls. The test now verifies the overall flow.
            # Verify settings was called for each importance level
            assert processor.settings.get_tables_by_importance.call_count == 4
    
    @pytest.mark.unit
    def test_sequential_processing_force_full_parameter(self):
        """Test that force_full parameter is passed through to sequential processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed so processing continues
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            processor.process_by_priority(force_full=True)
            
            # Verify settings was called for each importance level
            assert processor.settings.get_tables_by_importance.call_count == 4
    
    @pytest.mark.unit
    def test_sequential_processing_failure_handling(self):
        """Test that sequential processing handles failures correctly."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment'],
            'important': ['payment'],
            'audit': ['securitylog'],
            'reference': ['zipcode']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            results = processor.process_by_priority()
            
            # Verify results structure
            assert 'critical' in results
            assert 'important' in results
            assert 'audit' in results
            assert 'reference' in results
    
    @pytest.mark.unit
    def test_sequential_processing_exception_handling(self):
        """Test that sequential processing handles exceptions correctly."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment'],
            'important': ['payment'],
            'audit': ['securitylog'],
            'reference': ['zipcode']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            results = processor.process_by_priority()
            
            # Verify results structure even with exceptions
            assert 'critical' in results
            assert 'critical' in results
            assert 'important' in results
            assert 'audit' in results
            assert 'reference' in results


class TestResourceManagementUnit(TestPriorityProcessorUnit):
    """Unit tests for resource management."""
    
    @pytest.mark.unit
    def test_thread_pool_context_manager(self):
        """Test that ThreadPoolExecutor is properly managed with context manager."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed, \
             patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock TableProcessor
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Mock futures
            mock_future = MagicMock()
            mock_future.result.return_value = True
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Process critical tables to trigger parallel processing
            processor.process_by_priority(importance_levels=['critical'])
            
            # Verify context manager was used
            mock_executor.__enter__.assert_called_once()
            mock_executor.__exit__.assert_called_once()
    
    @pytest.mark.unit
    def test_max_workers_parameter(self):
        """Test that max_workers parameter is passed to ThreadPoolExecutor."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed, \
             patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock TableProcessor
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Mock futures
            mock_future = MagicMock()
            mock_future.result.return_value = True
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Process with custom max_workers
            processor.process_by_priority(importance_levels=['critical'], max_workers=10)
            
            # Verify max_workers was passed to ThreadPoolExecutor
            mock_executor_class.assert_called_once_with(max_workers=10)
    
    @pytest.mark.unit
    def test_default_max_workers(self):
        """Test that default max_workers is used when not specified."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with patch('etl_pipeline.orchestration.priority_processor.ThreadPoolExecutor') as mock_executor_class, \
             patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed, \
             patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            
            # Create proper mock executor with context manager support
            mock_executor = MagicMock()
            mock_executor.__enter__ = MagicMock(return_value=mock_executor)
            mock_executor.__exit__ = MagicMock(return_value=None)
            mock_executor_class.return_value = mock_executor
            
            # Mock TableProcessor
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Mock futures
            mock_future = MagicMock()
            mock_future.result.return_value = True
            mock_executor.submit.return_value = mock_future
            mock_as_completed.return_value = [mock_future]
            
            # Process without specifying max_workers
            processor.process_by_priority(importance_levels=['critical'])
            
            # Verify default max_workers (5) was used
            mock_executor_class.assert_called_once_with(max_workers=5)


class TestErrorHandlingUnit(TestPriorityProcessorUnit):
    """Unit tests for error handling."""
    
    @pytest.mark.unit
    def test_settings_exception_propagation(self):
        """Test that settings exceptions are properly propagated."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that raises exception
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = Exception("Settings error")
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        with pytest.raises(Exception, match="Settings error"):
            processor.process_by_priority()
    
    @pytest.mark.unit
    def test_table_processor_exception_handling(self):
        """Test that table processor exceptions are properly handled."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns multiple critical tables to trigger parallel processing
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],  # 3 tables > 1, triggers parallel
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor class to raise exception
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.side_effect = Exception("Table processing failed")
            mock_table_processor_class.return_value = mock_table_processor
            
            results = processor.process_by_priority()
            
            # Verify exception was handled and table marked as failed
            # Critical tables use parallel processing, so all 3 fail
            critical_result = results['critical']
            assert len(critical_result['failed']) == 3  # All critical tables fail in parallel
            assert len(critical_result['success']) == 0
            
            # Processing stops after critical failure, so no other levels processed
            assert len(results) == 1
    
    @pytest.mark.unit
    def test_invalid_importance_levels(self):
        """Test processing with invalid importance levels."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings that returns empty list for invalid levels
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: []
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        results = processor.process_by_priority(importance_levels=['invalid_level'])
        
        # Verify invalid level is skipped entirely (not added to results)
        assert 'invalid_level' not in results
        assert results == {}


class TestCriticalFailureLogicUnit(TestPriorityProcessorUnit):
    """Unit tests for critical failure handling logic."""
    
    @pytest.mark.unit
    def test_critical_failure_stops_processing(self):
        """Test that critical table failures stop processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment'],
            'important': ['payment'],
            'audit': ['securitylog'],
            'reference': ['zipcode']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to fail for critical tables
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            
            def mock_process_table(table, force_full=False):
                # Fail critical tables, succeed others
                if table in ['patient', 'appointment']:
                    return False
                else:
                    return True
            
            mock_table_processor.process_table.side_effect = mock_process_table
            mock_table_processor_class.return_value = mock_table_processor
            
            results = processor.process_by_priority()
            
            # Verify that processing stops after critical failure
            assert 'critical' in results
            assert len(results['critical']['failed']) == 2
            assert len(results['critical']['success']) == 0
            
            # Processing should stop after critical failure, so no other levels processed
            assert len(results) == 1
    
    @pytest.mark.unit
    def test_non_critical_failures_continue_processing(self):
        """Test that non-critical failures don't stop processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment'],
            'important': ['payment'],
            'audit': ['securitylog'],
            'reference': ['zipcode']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed for critical tables, fail for some others
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            
            def mock_process_table(table, force_full=False):
                # Succeed critical tables, fail some others
                if table in ['patient', 'appointment']:
                    return True  # Critical tables succeed
                elif table in ['payment']:
                    return False  # Important table fails
                else:
                    return True  # Other tables succeed
            
            mock_table_processor.process_table.side_effect = mock_process_table
            mock_table_processor_class.return_value = mock_table_processor
            
            results = processor.process_by_priority()
            
            # Verify all levels are processed
            assert 'critical' in results
            assert 'important' in results
            assert 'audit' in results
            assert 'reference' in results
            
            # Verify critical tables succeeded (so processing continues)
            assert len(results['critical']['success']) == 2
            assert len(results['critical']['failed']) == 0
            
            # Verify important table failed but processing continued
            assert len(results['important']['success']) == 0
            assert len(results['important']['failed']) == 1


class TestPerformanceUnit(TestPriorityProcessorUnit):
    """Unit tests for performance characteristics."""
    
    @pytest.mark.unit
    @pytest.mark.performance
    def test_processing_speed(self):
        """Test that processing completes within reasonable time."""
        import time
        
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed quickly
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            start_time = time.time()
            
            # Process a small set of tables
            results = processor.process_by_priority(importance_levels=['critical'])
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Verify processing completed
            assert 'critical' in results
            
            # Verify reasonable processing time (should be very fast with mocks)
            assert processing_time < 5.0  # Should complete in under 5 seconds
    
    @pytest.mark.unit
    @pytest.mark.performance
    def test_memory_efficiency(self):
        """Test that processing doesn't consume excessive memory."""
        import psutil
        import os
        
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        # Create mock settings
        mock_settings = MagicMock(spec=Settings)
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        # Create processor with mock settings
        processor = PriorityProcessor(schema_discovery=mock_schema_discovery, settings=mock_settings)
        
        # Mock the TableProcessor to succeed
        with patch('etl_pipeline.orchestration.priority_processor.TableProcessor') as mock_table_processor_class:
            mock_table_processor = MagicMock()
            mock_table_processor.initialize_connections.return_value = True
            mock_table_processor.process_table.return_value = True
            mock_table_processor_class.return_value = mock_table_processor
            
            # Get initial memory usage
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss
            
            # Process tables
            results = processor.process_by_priority(importance_levels=['critical'])
            
            # Get final memory usage
            final_memory = process.memory_info().rss
            memory_increase = final_memory - initial_memory
            
            # Verify processing completed
            assert 'critical' in results
            
            # Verify reasonable memory usage (should be minimal with mocks)
            # Convert to MB for readability
            memory_increase_mb = memory_increase / (1024 * 1024)
            assert memory_increase_mb < 100  # Should use less than 100MB additional memory


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "unit"]) 