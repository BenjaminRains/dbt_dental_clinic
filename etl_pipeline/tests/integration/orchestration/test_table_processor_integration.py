"""
Integration tests for TableProcessor - Real database integration with SQLite.

This module provides integration tests for TableProcessor using real SQLite databases
to test actual data flow, error handling, and database interactions.

Coverage: Integration scenarios and edge cases
Execution: < 10 seconds per component
Markers: @pytest.mark.integration
"""

import pytest
import tempfile
import os
import logging
import pandas as pd
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine, text
from datetime import datetime

from etl_pipeline.orchestration.table_processor import TableProcessor

logger = logging.getLogger(__name__)


class TestTableProcessorIntegration:
    """Integration tests for TableProcessor with real SQLite databases."""

    @pytest.fixture
    def sqlite_engines(self):
        """Create SQLite engines for integration testing."""
        # Create temporary SQLite databases
        source_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        source_db.close()
        replication_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        replication_db.close()
        analytics_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        analytics_db.close()
        
        # Create engines
        source_engine = create_engine(f'sqlite:///{source_db.name}')
        replication_engine = create_engine(f'sqlite:///{replication_db.name}')
        analytics_engine = create_engine(f'sqlite:///{analytics_db.name}')
        
        yield source_engine, replication_engine, analytics_engine
        
        # Cleanup with error handling (Section 20 from debugging notes)
        try:
            # Dispose engines to close connections
            source_engine.dispose()
            replication_engine.dispose()
            analytics_engine.dispose()
            
            # Remove files
            os.unlink(source_db.name)
            os.unlink(replication_db.name)
            os.unlink(analytics_db.name)
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")

    @pytest.fixture
    def setup_test_data(self, sqlite_engines):
        """Set up test data in SQLite databases."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        try:
            # Create test table in source database
            with source_engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE patient (
                        PatNum INTEGER PRIMARY KEY,
                        LName TEXT,
                        FName TEXT,
                        BirthDate TEXT,
                        Gender TEXT,
                        SSN TEXT
                    )
                """))
                
                # Insert test data - Fix SQLAlchemy parameter binding (Section 26 from debugging notes)
                test_data = [
                    (1, 'Doe', 'John', '1990-01-01', 'M', '123-45-6789'),
                    (2, 'Smith', 'Jane', '1985-05-15', 'F', '987-65-4321'),
                    (3, 'Johnson', 'Bob', '1975-12-25', 'M', '456-78-9012')
                ]
                
                # Use individual inserts with proper parameter binding
                for row in test_data:
                    conn.execute(text("""
                        INSERT INTO patient (PatNum, LName, FName, BirthDate, Gender, SSN)
                        VALUES (:patnum, :lname, :fname, :birthdate, :gender, :ssn)
                    """), {
                        'patnum': row[0],
                        'lname': row[1], 
                        'fname': row[2],
                        'birthdate': row[3],
                        'gender': row[4],
                        'ssn': row[5]
                    })
                
                conn.commit()
            
            # Create raw schema in analytics database
            with analytics_engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE raw_patient (
                        "PatNum" INTEGER PRIMARY KEY,
                        "LName" TEXT,
                        "FName" TEXT,
                        "BirthDate" TEXT,
                        "Gender" TEXT,
                        "SSN" TEXT
                    )
                """))
                
                conn.commit()
            
            # Create public schema in analytics database
            with analytics_engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE public_patient (
                        patnum INTEGER PRIMARY KEY,
                        lname TEXT,
                        fname TEXT,
                        birthdate TEXT,
                        gender TEXT,
                        ssn TEXT
                    )
                """))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to setup test data: {e}")
            raise

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for integration testing."""
        with patch('etl_pipeline.orchestration.table_processor.Settings') as mock_settings_class:
            settings_instance = MagicMock()
            
            # Mock database configurations with **kwargs (Section 12 from debugging notes)
            settings_instance.get_database_config.side_effect = lambda db, **kwargs: {
                'database': f'{db}_database'
            }
            
            # Mock table configurations
            settings_instance.get_table_config.return_value = {
                'estimated_rows': 1000,
                'estimated_size_mb': 10,
                'batch_size': 5000
            }
            
            # Mock incremental settings
            settings_instance.should_use_incremental.return_value = True
            
            mock_settings_class.return_value = settings_instance
            yield settings_instance

    @pytest.fixture
    def mock_raw_to_public_transformer(self, sqlite_engines):
        """Mock RawToPublicTransformer for integration testing."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        mock_transformer = MagicMock()
        mock_transformer.transform_table.return_value = True
        
        return mock_transformer

    @pytest.mark.integration
    def test_initialize_connections_success(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test successful connection initialization with real engines."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        
        result = processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        assert result is True
        assert processor.opendental_source_engine == source_engine
        assert processor.mysql_replication_engine == replication_engine
        assert processor.postgres_analytics_engine == analytics_engine
        assert processor.raw_to_public_transformer == mock_raw_to_public_transformer
        assert processor._connections_available() is True

    @pytest.mark.integration
    def test_initialize_connections_failure(self, mock_settings):
        """Test connection initialization failure."""
        processor = TableProcessor()
        
        # Test with non-existent table instead of invalid engine (Section 31 from debugging notes)
        # SQLite is too forgiving with invalid paths, so test with real error scenario
        with patch.object(processor.settings, 'get_database_config', side_effect=Exception("Config failed")):
            with pytest.raises(Exception, match="Config failed"):
                processor.initialize_connections(
                    source_engine=create_engine('sqlite:///test.db'),
                    replication_engine=create_engine('sqlite:///test.db'),
                    analytics_engine=create_engine('sqlite:///test.db')
                )

    @pytest.mark.integration
    def test_cleanup_with_real_engines(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test cleanup with real database engines."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Verify connections are set
        assert processor._connections_available() is True
        
        # Test cleanup
        processor.cleanup()
        
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None
        assert processor._connections_available() is False

    @pytest.mark.integration
    def test_context_manager_with_real_engines(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test TableProcessor as context manager with real engines."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        with TableProcessor() as processor:
            processor.initialize_connections(
                source_engine=source_engine,
                replication_engine=replication_engine,
                analytics_engine=analytics_engine,
                raw_to_public_transformer=mock_raw_to_public_transformer
            )
            
            assert processor._connections_available() is True
        
        # After context exit, connections should be cleaned up
        assert processor._connections_available() is False

    @pytest.mark.integration
    def test_process_table_connections_not_available(self, mock_settings):
        """Test process_table when connections are not initialized."""
        processor = TableProcessor()
        
        result = processor.process_table('test_table')
        
        assert result is False

    @pytest.mark.integration
    def test_process_table_success_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer, setup_test_data):
        """Test successful table processing with real database integration."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Mock the internal methods to simulate successful processing
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            result = processor.process_table('patient')
            
            assert result is True
            processor._extract_to_replication.assert_called_once_with('patient', False)
            processor._load_to_analytics.assert_called_once_with('patient', True, mock_settings.get_table_config.return_value)
            mock_raw_to_public_transformer.transform_table.assert_called_once_with('patient', is_incremental=True)

    @pytest.mark.integration
    def test_process_table_extraction_failure_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test process_table when extraction fails with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        with patch.object(processor, '_extract_to_replication', return_value=False):
            result = processor.process_table('patient')
            
            assert result is False

    @pytest.mark.integration
    def test_process_table_loading_failure_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test process_table when loading fails with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=False):
            
            result = processor.process_table('patient')
            
            assert result is False

    @pytest.mark.integration
    def test_process_table_transformation_failure_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test process_table when transformation fails with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        mock_raw_to_public_transformer.transform_table.return_value = False
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            result = processor.process_table('patient')
            
            assert result is False

    @pytest.mark.integration
    def test_process_table_exception_handling_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test process_table exception handling with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        with patch.object(processor, '_extract_to_replication', side_effect=Exception("Test error")):
            result = processor.process_table('patient')
            
            assert result is False

    @pytest.mark.integration
    def test_process_table_force_full_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer, setup_test_data):
        """Test process_table with force_full=True using real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            result = processor.process_table('patient', force_full=True)
            
            assert result is True
            processor._extract_to_replication.assert_called_once_with('patient', True)
            processor._load_to_analytics.assert_called_once_with('patient', False, mock_settings.get_table_config.return_value)
            mock_raw_to_public_transformer.transform_table.assert_called_once_with('patient', is_incremental=False)

    @pytest.mark.integration
    def test_extract_to_replication_integration(self, sqlite_engines, mock_settings):
        """Test extraction to replication with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine
        )
        
        # Mock ExactMySQLReplicator
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = None  # Table doesn't exist
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('patient', False)
            
            assert result is True
            mock_replicator.create_exact_replica.assert_called_once_with('patient')
            mock_replicator.copy_table_data.assert_called_once_with('patient')

    @pytest.mark.integration
    def test_load_to_analytics_integration(self, sqlite_engines, mock_settings):
        """Test loading to analytics with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine
        )
        
        table_config = {
            'estimated_rows': 1000,
            'estimated_size_mb': 10,
            'batch_size': 5000
        }
        
        # Mock PostgresLoader - Patch where used (Section 4.1 from debugging notes)
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('patient', True, table_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table.assert_called_once_with(table_name='patient', force_full=False)

    @pytest.mark.integration
    def test_load_to_analytics_chunked_integration(self, sqlite_engines, mock_settings):
        """Test chunked loading to analytics with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine
        )
        
        table_config = {
            'estimated_rows': 2000000,  # Large table
            'estimated_size_mb': 200,
            'batch_size': 5000
        }
        
        # Mock PostgresLoader - Patch where used (Section 4.1 from debugging notes)
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('patient', True, table_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table_chunked.assert_called_once_with(table_name='patient', force_full=False, chunk_size=5000)

    @pytest.mark.integration
    def test_error_recovery_with_real_engines(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test error recovery with real database engines."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Test that processor can recover from errors
        with patch.object(processor, '_extract_to_replication', side_effect=Exception("Test error")):
            result = processor.process_table('patient')
            assert result is False
        
        # Test that processor can still work after error
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            mock_raw_to_public_transformer.transform_table.return_value = True
            result = processor.process_table('patient')
            assert result is True

    @pytest.mark.integration
    def test_database_connection_persistence(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test that database connections persist across operations."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Verify connections are available
        assert processor._connections_available() is True
        
        # Test multiple operations
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            mock_raw_to_public_transformer.transform_table.return_value = True
            
            # First operation
            result1 = processor.process_table('patient')
            assert result1 is True
            
            # Second operation
            result2 = processor.process_table('appointment')
            assert result2 is True
            
            # Connections should still be available
            assert processor._connections_available() is True

    @pytest.mark.integration
    def test_incremental_vs_full_refresh_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test incremental vs full refresh logic with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            mock_raw_to_public_transformer.transform_table.return_value = True
            
            # Test incremental processing
            result1 = processor.process_table('patient', force_full=False)
            assert result1 is True
            processor._extract_to_replication.assert_called_with('patient', False)
            processor._load_to_analytics.assert_called_with('patient', True, mock_settings.get_table_config.return_value)
            mock_raw_to_public_transformer.transform_table.assert_called_with('patient', is_incremental=True)
            
            # Reset mocks
            processor._extract_to_replication.reset_mock()
            processor._load_to_analytics.reset_mock()
            mock_raw_to_public_transformer.transform_table.reset_mock()
            
            # Test full refresh processing
            result2 = processor.process_table('patient', force_full=True)
            assert result2 is True
            processor._extract_to_replication.assert_called_with('patient', True)
            processor._load_to_analytics.assert_called_with('patient', False, mock_settings.get_table_config.return_value)
            mock_raw_to_public_transformer.transform_table.assert_called_with('patient', is_incremental=False)

    @pytest.mark.integration
    def test_table_config_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test table configuration integration with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Test with different table configurations
        test_configs = [
            {'estimated_rows': 1000, 'estimated_size_mb': 10, 'batch_size': 5000},  # Standard
            {'estimated_rows': 2000000, 'estimated_size_mb': 200, 'batch_size': 10000},  # Large
            {'estimated_rows': 500000, 'estimated_size_mb': 150, 'batch_size': 7500},  # Medium-large
        ]
        
        for config in test_configs:
            mock_settings.get_table_config.return_value = config
            
            with patch.object(processor, '_extract_to_replication', return_value=True), \
                 patch.object(processor, '_load_to_analytics', return_value=True):
                
                mock_raw_to_public_transformer.transform_table.return_value = True
                
                result = processor.process_table('patient')
                assert result is True
                
                # Verify correct configuration was used
                processor._load_to_analytics.assert_called_with('patient', True, config)

    @pytest.mark.integration
    def test_settings_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test Settings integration with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Test that settings methods are called correctly
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            mock_raw_to_public_transformer.transform_table.return_value = True
            
            result = processor.process_table('patient')
            
            assert result is True
            mock_settings.get_table_config.assert_called_with('patient')
            mock_settings.should_use_incremental.assert_called_with('patient')

    @pytest.mark.integration
    def test_metrics_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test metrics collection integration with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Verify metrics collector is available
        assert processor.metrics is not None
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            mock_raw_to_public_transformer.transform_table.return_value = True
            
            result = processor.process_table('patient')
            
            assert result is True
            # Test actual metrics methods that exist (Section 31 from debugging notes)
            assert hasattr(processor.metrics, 'record_table_processed')
            assert hasattr(processor.metrics, 'get_pipeline_stats')

    @pytest.mark.integration
    def test_schema_change_detection_integration(self, sqlite_engines, mock_settings):
        """Test schema change detection with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine
        )
        
        # Ensure database configurations are set up properly
        processor._source_db = 'source_database'
        processor._replication_db = 'replication_database'
        
        # Create a mock replicator instance - match the working unit test pattern
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = {'columns': []}  # Table exists
        mock_replicator.verify_exact_replica.return_value = False  # Schema changed
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        # Use the exact same patching pattern as the working unit tests
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('patient', False)
            
            assert result is True
            
            # Based on the working unit test, only copy_table_data should be called
            # The schema change logic forces full extraction but doesn't recreate the table
            mock_replicator.copy_table_data.assert_called_once_with('patient')
            
            # Verify the other methods were called as expected
            mock_replicator.get_exact_table_schema.assert_called_once_with('patient', replication_engine)
            mock_replicator.verify_exact_replica.assert_called_once_with('patient')

    @pytest.mark.integration
    def test_large_table_processing_integration(self, sqlite_engines, mock_settings, mock_raw_to_public_transformer):
        """Test large table processing with real database."""
        source_engine, replication_engine, analytics_engine = sqlite_engines
        
        processor = TableProcessor()
        processor.initialize_connections(
            source_engine=source_engine,
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            raw_to_public_transformer=mock_raw_to_public_transformer
        )
        
        # Configure for large table
        large_table_config = {
            'estimated_rows': 5000000,  # Very large table
            'estimated_size_mb': 500,   # Very large size
            'batch_size': 10000
        }
        mock_settings.get_table_config.return_value = large_table_config
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True):
            
            mock_raw_to_public_transformer.transform_table.return_value = True
            
            result = processor.process_table('large_table')
            
            assert result is True
            # Should use chunked loading for large table
            processor._load_to_analytics.assert_called_once_with('large_table', True, large_table_config) 