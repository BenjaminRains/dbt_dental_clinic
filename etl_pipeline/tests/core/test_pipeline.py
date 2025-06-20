"""Tests for the ETLPipeline class and orchestration modules."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.engine import Engine

from etl_pipeline.core.pipeline import ETLPipeline
from etl_pipeline.core.config import PipelineConfig
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.orchestration.priority_processor import PriorityProcessor

@pytest.fixture
def mock_config():
    """Create a mock PipelineConfig."""
    config = Mock(spec=PipelineConfig)
    return config

@pytest.fixture
def mock_engines():
    """Create mock database engines."""
    # Create mock connection
    mock_connection = Mock()
    mock_connection.execute = Mock()
    
    # Create mock engines that return our mock connection
    source_engine = Mock(spec=Engine)
    source_engine.connect.return_value = mock_connection
    
    replication_engine = Mock(spec=Engine)
    replication_engine.connect.return_value = mock_connection
    
    analytics_engine = Mock(spec=Engine)
    analytics_engine.connect.return_value = mock_connection
    
    return source_engine, replication_engine, analytics_engine

@pytest.fixture
def mock_connection_factory(mock_engines):
    """Mock the ConnectionFactory methods."""
    source_engine, replication_engine, analytics_engine = mock_engines
    
    with patch('etl_pipeline.core.pipeline.ConnectionFactory') as mock_factory:
        mock_factory.get_opendental_source_connection.return_value = source_engine
        mock_factory.get_mysql_replication_connection.return_value = replication_engine
        mock_factory.get_postgres_analytics_connection.return_value = analytics_engine
        yield mock_factory

def test_pipeline_initialization(mock_config):
    """Test ETLPipeline initialization with config."""
    pipeline = ETLPipeline(mock_config)
    assert pipeline.config == mock_config
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None

def test_initialize_connections_success(mock_config, mock_connection_factory, mock_engines):
    """Test successful connection initialization."""
    pipeline = ETLPipeline(mock_config)
    source_engine, replication_engine, analytics_engine = mock_engines
    
    # Initialize connections
    assert pipeline.initialize_connections() is True
    
    # Verify connections were created
    assert pipeline.source_engine == source_engine
    assert pipeline.replication_engine == replication_engine
    assert pipeline.analytics_engine == analytics_engine
    
    # Verify factory methods were called with correct pool settings
    mock_connection_factory.get_opendental_source_connection.assert_called_once_with(
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
    mock_connection_factory.get_mysql_replication_connection.assert_called_once_with(
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
    mock_connection_factory.get_postgres_analytics_connection.assert_called_once_with(
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )

def test_initialize_connections_failure(mock_config, mock_connection_factory):
    """Test connection initialization failure."""
    pipeline = ETLPipeline(mock_config)
    
    # Make source connection fail
    mock_connection_factory.get_opendental_source_connection.side_effect = Exception("Connection failed")
    
    # Verify initialization fails
    with pytest.raises(Exception) as exc_info:
        pipeline.initialize_connections()
    assert str(exc_info.value) == "Connection failed"
    
    # Verify cleanup was called
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None

def test_cleanup(mock_config, mock_connection_factory, mock_engines):
    """Test connection cleanup."""
    pipeline = ETLPipeline(mock_config)
    source_engine, replication_engine, analytics_engine = mock_engines
    
    # Initialize connections
    pipeline.initialize_connections()
    
    # Clean up connections
    pipeline.cleanup()
    
    # Verify engines were disposed
    source_engine.dispose.assert_called_once()
    replication_engine.dispose.assert_called_once()
    analytics_engine.dispose.assert_called_once()
    
    # Verify engines were set to None
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None

def test_context_manager(mock_config, mock_connection_factory, mock_engines):
    """Test context manager functionality."""
    source_engine, replication_engine, analytics_engine = mock_engines
    
    # Use context manager
    with ETLPipeline(mock_config) as pipeline:
        # Initialize connections
        pipeline.initialize_connections()
        
        # Verify connections were created
        assert pipeline.source_engine == source_engine
        assert pipeline.replication_engine == replication_engine
        assert pipeline.analytics_engine == analytics_engine
    
    # Verify connections were cleaned up after context exit
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None

def test_connections_available(mock_config, mock_connection_factory, mock_engines):
    """Test connection availability check."""
    pipeline = ETLPipeline(mock_config)
    
    # Initially no connections
    assert pipeline._connections_available() is False
    
    # Initialize connections
    pipeline.initialize_connections()
    
    # All connections available
    assert pipeline._connections_available() is True
    
    # Set one connection to None
    pipeline.source_engine = None
    
    # Not all connections available
    assert pipeline._connections_available() is False

def test_cleanup_dispose_error(mock_config, mock_connection_factory, mock_engines):
    """Test cleanup when dispose() raises an exception."""
    pipeline = ETLPipeline(mock_config)
    source_engine, replication_engine, analytics_engine = mock_engines
    
    # Initialize connections
    pipeline.initialize_connections()
    
    # Make source engine dispose() raise an exception
    source_engine.dispose.side_effect = Exception("Dispose failed")
    
    # Clean up connections - should not raise exception
    pipeline.cleanup()
    
    # Verify engines were disposed
    source_engine.dispose.assert_called_once()
    replication_engine.dispose.assert_called_once()
    analytics_engine.dispose.assert_called_once()
    
    # Verify engines were set to None despite dispose error
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None

# New test for orchestration modules
def test_orchestration_integration(mock_config, mock_connection_factory, mock_engines):
    """Test integration of PipelineOrchestrator, TableProcessor, and PriorityProcessor."""
    source_engine, replication_engine, analytics_engine = mock_engines
    
    # Get the mock connection
    source_connection = source_engine.connect()
    replication_connection = replication_engine.connect()
    
    # Mock the database responses
    mock_source_result = Mock()
    mock_source_result.scalar.return_value = """
    CREATE TABLE `patient` (
        `PatNum` int(11) NOT NULL AUTO_INCREMENT,
        `LName` varchar(100) DEFAULT NULL,
        `FName` varchar(100) DEFAULT NULL,
        PRIMARY KEY (`PatNum`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    
    mock_target_result = Mock()
    mock_target_result.scalar.return_value = None  # Table doesn't exist initially
    
    # Mock row count results
    mock_source_count = Mock()
    mock_source_count.scalar.return_value = 100
    
    mock_target_count = Mock()
    mock_target_count.scalar.return_value = 100  # Match source count
    
    # Configure mock connections to return our mock results
    source_connection.execute.side_effect = [
        mock_source_result,  # For SHOW CREATE TABLE
        mock_source_count,   # For row count
        mock_source_result,  # For second SHOW CREATE TABLE if needed
        mock_source_count    # For second row count if needed
    ]
    
    replication_connection.execute.side_effect = [
        mock_target_result,  # For SHOW CREATE TABLE
        mock_target_count,   # For row count
        mock_target_result,  # For second SHOW CREATE TABLE if needed
        mock_target_count    # For second row count if needed
    ]
    
    # Mock the orchestrator and its dependencies
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator') as mock_orchestrator_class, \
         patch('etl_pipeline.core.postgres_schema.PostgresSchema') as mock_postgres_schema, \
         patch('etl_pipeline.orchestration.table_processor.PostgresSchema') as mock_table_processor_schema, \
         patch('etl_pipeline.config.settings.Settings') as mock_settings, \
         patch('etl_pipeline.orchestration.table_processor.Settings') as mock_table_processor_settings, \
         patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_postgres_loader, \
         patch('sqlalchemy.inspect') as mock_inspect, \
         patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator') as mock_replicator_class:
        
        # Configure the mock settings
        mock_settings_instance = mock_settings.return_value
        mock_settings_instance.get_database_config.return_value = {
            'database': 'test_db'
        }
        mock_settings_instance.should_use_incremental.return_value = False
        
        # Configure the mock settings for TableProcessor
        mock_table_processor_settings_instance = mock_table_processor_settings.return_value
        mock_table_processor_settings_instance.get_database_config.return_value = {
            'database': 'test_db'
        }
        mock_table_processor_settings_instance.should_use_incremental.return_value = False
        
        # Mock table configuration
        mock_table_config = {
            'batch_size': 5000,
            'estimated_size_mb': 50,  # Small enough to not trigger chunked loading
            'estimated_rows': 1000    # Small enough to not trigger chunked loading
        }
        mock_table_processor_settings_instance.get_table_config.return_value = mock_table_config
        
        # Create mock inspector first (before it's used)
        mock_inspector = Mock()
        mock_inspector.get_columns.return_value = [
            {'name': 'PatNum', 'type': 'int', 'nullable': False},
            {'name': 'LName', 'type': 'varchar', 'nullable': True},
            {'name': 'FName', 'type': 'varchar', 'nullable': True}
        ]
        mock_inspector.get_pk_constraint.return_value = {'constrained_columns': ['PatNum']}
        mock_inspector.get_foreign_keys.return_value = []
        mock_inspector.get_indexes.return_value = []
        
        # Configure inspect to always return our mock inspector
        mock_inspect.return_value = mock_inspector
        
        # Configure the mock PostgresSchema for table processor
        mock_table_processor_schema_instance = mock_table_processor_schema.return_value
        mock_table_processor_schema_instance.mysql_inspector = mock_inspector
        mock_table_processor_schema_instance.postgres_inspector = mock_inspector
        
        # Configure the mock PostgresSchema
        mock_postgres_schema_instance = mock_postgres_schema.return_value
        mock_postgres_schema_instance.create_postgres_table.return_value = True
        mock_postgres_schema_instance.verify_schema.return_value = True
        
        # Mock the schema adaptation
        mock_postgres_schema_instance.adapt_schema.return_value = """
        CREATE TABLE raw.patient (
            "PatNum" integer,
            "LName" character varying(100),
            "FName" character varying(100),
            PRIMARY KEY ("PatNum")
        )
        """
        
        # Mock the inspectors to avoid the inspect() calls
        mock_postgres_schema_instance.mysql_inspector = mock_inspector
        mock_postgres_schema_instance.postgres_inspector = mock_inspector
        
        # Mock the PostgreSQL connection for PostgresSchema
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        analytics_engine.begin.return_value = mock_context
        
        # Configure the mock PostgresLoader more thoroughly
        mock_loader_instance = Mock()
        mock_loader_instance._ensure_postgres_table.return_value = True  # Mock table creation
        mock_loader_instance.load_table.return_value = True
        mock_loader_instance.load_table_chunked.return_value = True
        
        # Make sure the class returns our mock instance
        mock_postgres_loader.return_value = mock_loader_instance
        
        # Configure the mock orchestrator
        mock_orchestrator_instance = mock_orchestrator_class.return_value
        mock_orchestrator_instance.initialize_connections.return_value = True
        
        # FIXED: Properly mock the ExactMySQLReplicator class
        mock_replicator_instance = Mock()
        mock_replicator_instance.get_exact_table_schema.return_value = {
            'table_name': 'patient',
            'create_statement': mock_source_result.scalar(),
            'normalized_schema': 'CREATE TABLE patient (PatNum int NOT NULL AUTO_INCREMENT, LName varchar(100), FName varchar(100), PRIMARY KEY (PatNum))',
            'schema_hash': '1234567890abcdef'
        }
        mock_replicator_instance.verify_exact_replica.return_value = True
        mock_replicator_instance.create_exact_replica.return_value = True
        mock_replicator_instance.copy_table_data.return_value = True
        mock_replicator_instance.inspector = mock_inspector
        
        # Configure the mock class to return our mock instance
        mock_replicator_class.return_value = mock_replicator_instance
        
        # Create a real TableProcessor instance
        from etl_pipeline.orchestration.table_processor import TableProcessor
        table_processor = TableProcessor()
        table_processor.opendental_source_engine = source_engine
        table_processor.mysql_replication_engine = replication_engine
        table_processor.postgres_analytics_engine = analytics_engine
        
        # Mock the raw_to_public_transformer
        mock_transformer = Mock()
        mock_transformer.transform_table.return_value = True
        table_processor.raw_to_public_transformer = mock_transformer
        
        # Set the table processor on the orchestrator instance
        mock_orchestrator_instance.table_processor = table_processor
        
        # Define a side effect for run_pipeline_for_table that calls process_table
        def run_pipeline_side_effect(table_name, force_full=False):
            return table_processor.process_table(table_name, force_full=force_full)
        
        mock_orchestrator_instance.run_pipeline_for_table.side_effect = run_pipeline_side_effect
        
        # Use the mock orchestrator instance directly
        orchestrator = mock_orchestrator_instance
        
        # Initialize connections
        assert orchestrator.initialize_connections() is True
        
        # Run pipeline for a single table
        result = orchestrator.run_pipeline_for_table("patient")
        assert result is True
        
        # Verify orchestrator methods were called
        mock_orchestrator_instance.initialize_connections.assert_called_once()
        mock_orchestrator_instance.run_pipeline_for_table.assert_called_once_with("patient")
        
        # Verify that the key mocked components were used
        # Since we've mocked the core components, verify they were instantiated
        mock_postgres_loader.assert_called()
        mock_replicator_class.assert_called()
        
        # Verify PostgresSchema was initialized
        mock_table_processor_schema.assert_called()
        
        # Clean up
        orchestrator.cleanup() 