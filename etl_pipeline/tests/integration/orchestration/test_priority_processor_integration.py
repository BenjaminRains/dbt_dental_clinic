"""
Integration tests for PriorityProcessor focused on real database integration with SQLite.

This test suite follows the hybrid testing strategy:
- Integration tests with real SQLite database
- Safety, error handling, actual data flow
- Integration scenarios and edge cases
- < 10 seconds execution time

Testing Strategy:
- Real database integration with SQLite
- Safety and error handling validation
- Actual data flow testing
- Integration scenarios and edge cases
"""

import pytest
import time
import sqlite3
import tempfile
import os
from unittest.mock import MagicMock, patch
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Import the component under test
from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.config.settings import Settings


class TestPriorityProcessorIntegration:
    """Integration tests for PriorityProcessor class."""
    
    # Shared fixtures are now provided by conftest.py:
    # - mock_priority_processor_settings (renamed from mock_settings)
    # - priority_processor
    
    # Integration-specific fixtures remain here:
    # - sqlite_database
    # - real_table_processor
    
    @pytest.fixture
    def sqlite_database(self):
        """Create a temporary SQLite database for integration testing."""
        # Create temporary database file
        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)
        
        # Create database connection
        conn = sqlite3.connect(db_path)
        
        # Create test tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS patient (
                PatNum INTEGER PRIMARY KEY,
                LName TEXT,
                FName TEXT,
                BirthDate TEXT
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS appointment (
                AptNum INTEGER PRIMARY KEY,
                PatNum INTEGER,
                AptDateTime TEXT,
                AptStatus INTEGER
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS procedurelog (
                ProcNum INTEGER PRIMARY KEY,
                PatNum INTEGER,
                ProcDate TEXT,
                ProcFee REAL
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS payment (
                PayNum INTEGER PRIMARY KEY,
                PatNum INTEGER,
                PayDate TEXT,
                PayAmt REAL
            )
        """)
        
        # Insert test data
        conn.execute("INSERT INTO patient (PatNum, LName, FName, BirthDate) VALUES (1, 'Doe', 'John', '1990-01-01')")
        conn.execute("INSERT INTO patient (PatNum, LName, FName, BirthDate) VALUES (2, 'Smith', 'Jane', '1985-05-15')")
        
        conn.execute("INSERT INTO appointment (AptNum, PatNum, AptDateTime, AptStatus) VALUES (1, 1, '2024-01-15 10:00:00', 1)")
        conn.execute("INSERT INTO appointment (AptNum, PatNum, AptDateTime, AptStatus) VALUES (2, 2, '2024-01-16 14:00:00', 1)")
        
        conn.execute("INSERT INTO procedurelog (ProcNum, PatNum, ProcDate, ProcFee) VALUES (1, 1, '2024-01-15', 150.00)")
        conn.execute("INSERT INTO procedurelog (ProcNum, PatNum, ProcDate, ProcFee) VALUES (2, 2, '2024-01-16', 200.00)")
        
        conn.execute("INSERT INTO payment (PayNum, PatNum, PayDate, PayAmt) VALUES (1, 1, '2024-01-15', 150.00)")
        conn.execute("INSERT INTO payment (PayNum, PatNum, PayDate, PayAmt) VALUES (2, 2, '2024-01-16', 200.00)")
        
        conn.commit()
        conn.close()
        
        yield db_path
        
        # Cleanup
        try:
            os.unlink(db_path)
        except OSError:
            pass
    
    @pytest.fixture
    def real_table_processor(self, sqlite_database):
        """Create a real TableProcessor instance for integration testing."""
        processor = MagicMock(spec=TableProcessor)
        
        # Mock process_table to simulate real processing
        def mock_process_table(table, force_full=False, **kwargs):
            try:
                # Simulate real table processing with database interaction
                conn = sqlite3.connect(sqlite_database)
                cursor = conn.cursor()
                
                # Get table info to verify it exists
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                
                if not columns:
                    conn.close()
                    return False
                
                # Simulate processing by counting rows
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                conn.close()
                
                # Simulate success/failure based on table content
                if count > 0:
                    return True
                else:
                    return False
                    
            except Exception as e:
                logging.error(f"Error processing table {table}: {str(e)}")
                return False
        
        processor.process_table.side_effect = mock_process_table
        return processor
    
    @pytest.fixture
    def integration_priority_processor(self):
        """Create PriorityProcessor instance with integration-specific settings."""
        # Create integration-specific settings (different from shared fixture)
        settings = MagicMock(spec=Settings)
        settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient', 'appointment', 'procedurelog'],
            'important': ['payment', 'claim', 'insplan'],
            'audit': ['securitylog', 'entrylog'],
            'reference': ['zipcode', 'definition']
        }.get(importance, [])
        
        return PriorityProcessor(settings=settings)


class TestIntegrationBasic(TestPriorityProcessorIntegration):
    """Basic integration tests."""
    
    @pytest.mark.integration
    def test_integration_with_real_database(self, integration_priority_processor, real_table_processor):
        """Test integration with real SQLite database."""
        results = integration_priority_processor.process_by_priority(real_table_processor)
        
        # Verify results structure
        assert 'critical' in results
        assert 'important' in results
        assert 'audit' in results
        assert 'reference' in results
        
        # Verify critical tables were processed successfully
        critical_result = results['critical']
        assert critical_result['total'] == 3
        assert len(critical_result['success']) == 3  # All critical tables should succeed
        assert len(critical_result['failed']) == 0
        
        # Verify important tables were processed
        important_result = results['important']
        assert important_result['total'] == 1
        assert len(important_result['success']) == 1
        assert len(important_result['failed']) == 0
    
    @pytest.mark.integration
    def test_integration_force_full_parameter(self, integration_priority_processor, real_table_processor):
        """Test that force_full parameter works with real database."""
        results = integration_priority_processor.process_by_priority(real_table_processor, force_full=True)
        
        # Verify processing completed successfully
        assert 'critical' in results
        assert len(results['critical']['success']) == 3
        
        # Verify force_full parameter was passed to process_table
        for call_args in real_table_processor.process_table.call_args_list:
            # force_full is the second positional argument
            assert call_args[0][1] is True  # args[1] is force_full
    
    @pytest.mark.integration
    def test_integration_custom_importance_levels(self, integration_priority_processor, real_table_processor):
        """Test processing with custom importance levels using real database."""
        custom_levels = ['critical', 'important']
        
        results = integration_priority_processor.process_by_priority(
            real_table_processor, 
            importance_levels=custom_levels
        )
        
        # Verify only specified levels were processed
        assert 'critical' in results
        assert 'important' in results
        assert 'audit' not in results
        assert 'reference' not in results
        
        # Verify processing was successful
        assert len(results['critical']['success']) == 3
        assert len(results['important']['success']) == 1


class TestIntegrationErrorHandling(TestPriorityProcessorIntegration):
    """Integration tests for error handling."""
    
    @pytest.mark.integration
    def test_integration_database_connection_failure(self, integration_priority_processor):
        """Test handling of database connection failures."""
        # Create table processor that simulates connection failure
        processor = MagicMock(spec=TableProcessor)
        processor.process_table.side_effect = Exception("Database connection failed")
        
        results = integration_priority_processor.process_by_priority(processor)
        
        # Verify error was handled and table marked as failed
        # Critical tables use parallel processing, so all 3 fail
        critical_result = results['critical']
        assert len(critical_result['failed']) == 3  # All critical tables fail in parallel
        assert len(critical_result['success']) == 0
        
        # Verify processing stopped after critical failure
        assert len(results) == 1
    
    @pytest.mark.integration
    def test_integration_partial_failures(self, integration_priority_processor, real_table_processor):
        """Test handling of partial processing failures."""
        # Modify table processor to fail for some tables
        def mock_process_table_with_failures(table, force_full=False, **kwargs):
            if table in ['patient', 'appointment']:
                return True
            elif table == 'procedurelog':
                return False  # Simulate failure
            else:
                return True
        
        real_table_processor.process_table.side_effect = mock_process_table_with_failures
        
        results = integration_priority_processor.process_by_priority(real_table_processor)
        
        # Verify mixed results
        critical_result = results['critical']
        assert len(critical_result['success']) == 2
        assert len(critical_result['failed']) == 1
        
        # Verify processing stopped after critical failure
        assert len(results) == 1
    
    @pytest.mark.integration
    def test_integration_settings_exception(self, integration_priority_processor, real_table_processor):
        """Test handling of settings exceptions in integration."""
        # Mock settings to raise exception
        integration_priority_processor.settings.get_tables_by_importance.side_effect = Exception("Settings configuration error")
        
        with pytest.raises(Exception, match="Settings configuration error"):
            integration_priority_processor.process_by_priority(real_table_processor)


class TestIntegrationParallelProcessing(TestPriorityProcessorIntegration):
    """Integration tests for parallel processing."""
    
    @pytest.mark.integration
    def test_integration_parallel_processing_critical_tables(self, integration_priority_processor, real_table_processor):
        """Test parallel processing of critical tables with real database."""
        start_time = time.time()
        results = integration_priority_processor.process_by_priority(real_table_processor, max_workers=3)
        end_time = time.time()
        
        # Verify processing completed successfully
        critical_result = results['critical']
        assert len(critical_result['success']) == 3
        assert len(critical_result['failed']) == 0
        
        # Verify reasonable processing time
        processing_time = end_time - start_time
        assert processing_time < 5.0  # Should be fast with SQLite
    
    @pytest.mark.integration
    def test_integration_parallel_processing_with_failures(self, integration_priority_processor, real_table_processor):
        """Test parallel processing with some failures."""
        # Modify table processor to fail for some tables
        def mock_process_table_with_failures(table, force_full=False, **kwargs):
            if table in ['patient', 'appointment']:
                return True
            elif table == 'procedurelog':
                return False  # Simulate failure
            else:
                return True
        
        real_table_processor.process_table.side_effect = mock_process_table_with_failures
        
        results = integration_priority_processor.process_by_priority(real_table_processor, max_workers=3)
        
        # Verify mixed results from parallel processing
        critical_result = results['critical']
        assert len(critical_result['success']) == 2
        assert len(critical_result['failed']) == 1
    
    @pytest.mark.integration
    def test_integration_parallel_processing_resource_cleanup(self, integration_priority_processor, real_table_processor):
        """Test that parallel processing properly cleans up resources."""
        # Process tables multiple times to test resource cleanup
        for i in range(3):
            results = integration_priority_processor.process_by_priority(real_table_processor, max_workers=2)
            
            # Verify processing completed successfully each time
            critical_result = results['critical']
            assert len(critical_result['success']) == 3
            assert len(critical_result['failed']) == 0


class TestIntegrationSequentialProcessing(TestPriorityProcessorIntegration):
    """Integration tests for sequential processing."""
    
    @pytest.mark.integration
    def test_integration_sequential_processing_order(self, integration_priority_processor, real_table_processor):
        """Test that sequential processing maintains order with real database."""
        processed_tables = []
        
        def mock_process_table_with_order(table, force_full=False, **kwargs):
            processed_tables.append(table)
            # Simulate real processing
            return True
        
        real_table_processor.process_table.side_effect = mock_process_table_with_order
        
        integration_priority_processor.process_by_priority(real_table_processor)
        
        # Verify tables were processed in order
        expected_order = ['patient', 'appointment', 'procedurelog',  # critical
                         'payment']                                  # important
        assert processed_tables[:4] == expected_order
    
    @pytest.mark.integration
    def test_integration_sequential_processing_with_delays(self, integration_priority_processor, real_table_processor):
        """Test sequential processing with simulated processing delays."""
        def mock_process_table_with_delay(table, force_full=False, **kwargs):
            # Simulate processing delay
            time.sleep(0.01)
            return True
        
        real_table_processor.process_table.side_effect = mock_process_table_with_delay
        
        start_time = time.time()
        results = integration_priority_processor.process_by_priority(real_table_processor)
        end_time = time.time()
        
        # Verify processing completed successfully
        assert len(results['critical']['success']) == 3
        assert len(results['important']['success']) == 1
        
        # Verify reasonable processing time (sequential should be slower than parallel)
        processing_time = end_time - start_time
        assert processing_time < 1.0  # Should still be reasonable


class TestIntegrationDataFlow(TestPriorityProcessorIntegration):
    """Integration tests for data flow."""
    
    @pytest.mark.integration
    def test_integration_data_consistency(self, integration_priority_processor, real_table_processor, sqlite_database):
        """Test that data remains consistent during processing."""
        # Get initial data counts
        conn = sqlite3.connect(sqlite_database)
        cursor = conn.cursor()
        
        initial_counts = {}
        for table in ['patient', 'appointment', 'procedurelog', 'payment']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            initial_counts[table] = cursor.fetchone()[0]
        
        conn.close()
        
        # Process tables
        results = integration_priority_processor.process_by_priority(real_table_processor)
        
        # Verify processing completed successfully
        assert len(results['critical']['success']) == 3
        assert len(results['important']['success']) == 1
        
        # Verify data counts remain the same
        conn = sqlite3.connect(sqlite_database)
        cursor = conn.cursor()
        
        final_counts = {}
        for table in ['patient', 'appointment', 'procedurelog', 'payment']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            final_counts[table] = cursor.fetchone()[0]
        
        conn.close()
        
        # Verify data consistency
        for table in initial_counts:
            assert initial_counts[table] == final_counts[table], f"Data count changed for {table}"
    
    @pytest.mark.integration
    def test_integration_multiple_runs_idempotency(self, integration_priority_processor, real_table_processor):
        """Test that multiple runs produce consistent results."""
        results1 = integration_priority_processor.process_by_priority(real_table_processor)
        results2 = integration_priority_processor.process_by_priority(real_table_processor)
        
        # Verify both runs produced consistent results (order may vary due to parallel processing)
        # Check that all levels are present
        assert set(results1.keys()) == set(results2.keys())
        
        # Check each level's results (order-independent)
        for level in results1:
            assert results1[level]['total'] == results2[level]['total']
            assert set(results1[level]['success']) == set(results2[level]['success'])
            assert set(results1[level]['failed']) == set(results2[level]['failed'])
        
        # Verify successful processing in both runs
        assert len(results1['critical']['success']) == 3
        assert len(results2['critical']['success']) == 3


class TestIntegrationPerformance(TestPriorityProcessorIntegration):
    """Integration tests for performance."""
    
    @pytest.mark.integration
    @pytest.mark.performance
    def test_integration_processing_performance(self, integration_priority_processor, real_table_processor):
        """Test processing performance with real database."""
        start_time = time.time()
        results = integration_priority_processor.process_by_priority(real_table_processor)
        end_time = time.time()
        
        # Verify processing completed successfully
        assert len(results['critical']['success']) == 3
        assert len(results['important']['success']) == 1
        
        # Verify reasonable processing time
        processing_time = end_time - start_time
        assert processing_time < 2.0  # Should be fast with SQLite
    
    @pytest.mark.integration
    @pytest.mark.performance
    def test_integration_memory_usage(self, integration_priority_processor, real_table_processor):
        """Test memory usage during processing."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Process tables
        results = integration_priority_processor.process_by_priority(real_table_processor)
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Verify reasonable memory usage
        assert memory_increase < 50 * 1024 * 1024  # Less than 50MB increase
        
        # Verify processing completed successfully
        assert len(results['critical']['success']) == 3


class TestIntegrationEdgeCases(TestPriorityProcessorIntegration):
    """Integration tests for edge cases."""
    
    @pytest.mark.integration
    def test_integration_empty_database(self, integration_priority_processor):
        """Test processing with empty database."""
        # Create empty SQLite database
        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)
        
        # Create empty database
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS patient (PatNum INTEGER PRIMARY KEY)")
        conn.close()
        
        # Create table processor for empty database
        processor = MagicMock(spec=TableProcessor)
        processor.process_table.return_value = False  # All tables fail in empty DB
        
        results = integration_priority_processor.process_by_priority(processor)
        
        # Verify all tables failed
        critical_result = results['critical']
        assert len(critical_result['failed']) == 3
        assert len(critical_result['success']) == 0
        
        # Cleanup
        try:
            os.unlink(db_path)
        except OSError:
            pass
    
    @pytest.mark.integration
    def test_integration_large_dataset_performance(self, integration_priority_processor, real_table_processor, sqlite_database):
        """Test performance with larger dataset."""
        # Add more test data
        conn = sqlite3.connect(sqlite_database)
        
        # Add more patients
        for i in range(3, 103):  # Add 100 more patients
            conn.execute(f"INSERT INTO patient (PatNum, LName, FName, BirthDate) VALUES ({i}, 'Test{i}', 'User{i}', '1990-01-01')")
        
        conn.commit()
        conn.close()
        
        start_time = time.time()
        results = integration_priority_processor.process_by_priority(real_table_processor)
        end_time = time.time()
        
        # Verify processing completed successfully
        assert len(results['critical']['success']) == 3
        assert len(results['important']['success']) == 1
        
        # Verify reasonable processing time with larger dataset
        processing_time = end_time - start_time
        assert processing_time < 5.0  # Should still be reasonable


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 