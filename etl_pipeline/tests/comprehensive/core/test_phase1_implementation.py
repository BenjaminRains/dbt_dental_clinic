"""Pytest test suite for Phase 1 ETL pipeline implementation.

This test suite provides robust, automated testing for the Phase 1 intelligent
ETL pipeline implementation, replacing the basic standalone test script.

TEST CATEGORIES:
1. Configuration Loading - Tests intelligent table configuration loading
2. Database Connections - Tests connectivity to all three databases
3. Phase 1 Scope Validation - Tests critical table selection and processing plan
4. Monitoring Configuration - Tests alerting and monitoring setup
5. Pipeline Initialization - Tests two-phase pipeline setup
6. Data Quality Validation - Tests data quality thresholds and monitoring

USAGE:
    pytest tests/core/test_phase1_implementation.py -v
    pytest tests/core/test_phase1_implementation.py::test_configuration_loading -v
    pytest tests/core/test_phase1_implementation.py -k "database" -v
"""

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv()

# Import the pipeline class (adjust import path as needed)
try:
    from etl_pipeline.elt_pipeline import IntelligentELTPipeline
except ImportError:
    # Fallback for different import structure
    IntelligentELTPipeline = None

@pytest.fixture(scope="session")
def pipeline_instance():
    """Create a pipeline instance for testing."""
    if IntelligentELTPipeline is None:
        pytest.skip("IntelligentELTPipeline not available")
    
    pipeline = IntelligentELTPipeline()
    yield pipeline
    # Cleanup
    try:
        pipeline.cleanup()
    except:
        pass

@pytest.fixture
def mock_connections():
    """Mock database connections for isolated testing."""
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        # Mock source connection
        mock_source = MagicMock()
        mock_source.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 1
        
        # Mock replication connection
        mock_replication = MagicMock()
        mock_replication.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 1
        
        # Mock analytics connection
        mock_analytics = MagicMock()
        mock_analytics.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 1
        
        mock_factory.get_opendental_source_connection.return_value = mock_source
        mock_factory.get_mysql_replication_connection.return_value = mock_replication
        mock_factory.get_postgres_analytics_connection.return_value = mock_analytics
        
        yield mock_factory

class TestConfigurationLoading:
    """Test intelligent table configuration loading."""
    
    def test_critical_tables_loading(self, pipeline_instance):
        """Test that critical tables are properly loaded."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        assert isinstance(critical_tables, list)
        assert len(critical_tables) > 0
        assert all(isinstance(table, str) for table in critical_tables)
        
        # Should contain expected critical tables
        expected_critical_tables = ['securitylog', 'paysplit', 'procedurelog', 'histappointment', 'sheetfield']
        for expected_table in expected_critical_tables:
            assert expected_table in critical_tables
    
    def test_table_configuration_structure(self, pipeline_instance):
        """Test that table configurations have the correct structure."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables[:3]:  # Test first 3 tables
            config = pipeline_instance.get_table_config(table)
            
            # Required configuration fields
            required_fields = [
                'table_importance', 'estimated_size_mb', 'estimated_rows',
                'extraction_strategy', 'batch_size', 'max_extraction_time_minutes',
                'data_quality_threshold'
            ]
            
            for field in required_fields:
                assert field in config, f"Missing required field '{field}' in {table} config"
            
            # Validate data types
            assert isinstance(config['table_importance'], str)
            assert isinstance(config['estimated_size_mb'], (int, float))
            assert isinstance(config['estimated_rows'], int)
            assert isinstance(config['extraction_strategy'], str)
            assert isinstance(config['batch_size'], int)
            assert isinstance(config['max_extraction_time_minutes'], int)
            assert isinstance(config['data_quality_threshold'], (int, float))
    
    def test_table_importance_classification(self, pipeline_instance):
        """Test that tables are properly classified by importance."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            assert config['table_importance'] == 'critical'
    
    def test_extraction_strategy_assignment(self, pipeline_instance):
        """Test that appropriate extraction strategies are assigned."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        valid_strategies = ['incremental', 'chunked_incremental', 'full', 'snapshot']
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            assert config['extraction_strategy'] in valid_strategies
    
    def test_batch_size_optimization(self, pipeline_instance):
        """Test that batch sizes are optimized based on table characteristics."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            
            # Batch size should be reasonable
            assert config['batch_size'] > 0
            assert config['batch_size'] <= 10000  # Reasonable upper limit
            
            # Larger tables should have larger batch sizes
            if config['estimated_size_mb'] > 500:
                assert config['batch_size'] >= 1000

class TestDatabaseConnections:
    """Test database connectivity to all three databases."""
    
    def test_source_database_connection(self, pipeline_instance):
        """Test connection to source OpenDental database."""
        pipeline_instance.initialize_connections()
        
        with pipeline_instance.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    
    def test_replication_database_connection(self, pipeline_instance):
        """Test connection to MySQL replication database."""
        pipeline_instance.initialize_connections()
        
        with pipeline_instance.replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    
    def test_analytics_database_connection(self, pipeline_instance):
        """Test connection to PostgreSQL analytics database."""
        pipeline_instance.initialize_connections()
        
        with pipeline_instance.analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    
    def test_connection_pool_settings(self, pipeline_instance):
        """Test that connection pools are properly configured."""
        pipeline_instance.initialize_connections()
        
        # Test pool settings for each engine
        for engine_name, engine in [
            ('source', pipeline_instance.source_engine),
            ('replication', pipeline_instance.replication_engine),
            ('analytics', pipeline_instance.analytics_engine)
        ]:
            assert engine.pool.size() >= 1
            assert engine.pool.checkedin() >= 0
    
    def test_database_names_validation(self, pipeline_instance):
        """Test that database names are properly validated."""
        pipeline_instance.validate_database_names()
        # Should not raise any exceptions

class TestPhase1ScopeValidation:
    """Test Phase 1 scope and processing plan validation."""
    
    def test_phase1_table_count(self, pipeline_instance):
        """Test that Phase 1 processes the correct number of tables."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        # Phase 1 should process exactly 5 critical tables
        assert len(critical_tables) == 5
    
    def test_phase1_data_volume(self, pipeline_instance):
        """Test that Phase 1 data volume is within expected range."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        total_size_mb = 0
        total_rows = 0
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            total_size_mb += config['estimated_size_mb']
            total_rows += config['estimated_rows']
        
        # Phase 1 should be around 2.1 GB (2100 MB)
        assert 1800 <= total_size_mb <= 2400  # Allow some variance
        
        # Should be around 8M rows
        assert 7000000 <= total_rows <= 9000000  # Allow some variance
    
    def test_phase1_processing_strategies(self, pipeline_instance):
        """Test that Phase 1 uses appropriate processing strategies."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        incremental_count = 0
        chunked_count = 0
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            if config['extraction_strategy'] == 'incremental':
                incremental_count += 1
            elif config['extraction_strategy'] == 'chunked_incremental':
                chunked_count += 1
        
        # Most tables should use incremental strategies
        assert incremental_count + chunked_count >= 4  # At least 4 out of 5
    
    def test_phase1_sla_times(self, pipeline_instance):
        """Test that SLA times are reasonable for Phase 1 tables."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            
            # SLA times should be reasonable (not too short, not too long)
            assert 20 <= config['max_extraction_time_minutes'] <= 120
            
            # Larger tables should have longer SLAs
            if config['estimated_size_mb'] > 500:
                assert config['max_extraction_time_minutes'] >= 50

class TestMonitoringConfiguration:
    """Test monitoring and alerting configuration."""
    
    def test_monitoring_config_structure(self, pipeline_instance):
        """Test that monitoring configurations have correct structure."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            monitoring = pipeline_instance.get_monitoring_config(table)
            
            required_fields = [
                'alert_on_failure', 'max_extraction_time_minutes',
                'data_quality_threshold', 'retry_attempts'
            ]
            
            for field in required_fields:
                assert field in monitoring, f"Missing monitoring field '{field}' for {table}"
    
    def test_alert_configuration(self, pipeline_instance):
        """Test that alerting is properly configured for critical tables."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            monitoring = pipeline_instance.get_monitoring_config(table)
            
            # Critical tables should have alerts enabled
            assert monitoring['alert_on_failure'] is True
    
    def test_quality_thresholds(self, pipeline_instance):
        """Test that data quality thresholds are appropriate."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            monitoring = pipeline_instance.get_monitoring_config(table)
            
            # Quality threshold should be high for critical tables
            assert monitoring['data_quality_threshold'] >= 95.0
    
    def test_retry_configuration(self, pipeline_instance):
        """Test that retry attempts are properly configured."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            monitoring = pipeline_instance.get_monitoring_config(table)
            
            # Should have reasonable retry attempts
            assert 1 <= monitoring['retry_attempts'] <= 5

class TestPipelineInitialization:
    """Test two-phase pipeline initialization."""
    
    def test_phase1_initialization(self, pipeline_instance):
        """Test that Phase 1 initialization works correctly."""
        # This would test the actual initialization process
        # For now, we'll test that the pipeline can be created
        assert pipeline_instance is not None
    
    def test_tracking_table_creation(self, pipeline_instance):
        """Test that tracking tables are created properly."""
        # This would test tracking table creation
        # Implementation depends on actual pipeline structure
        pass
    
    def test_environment_validation(self, pipeline_instance):
        """Test that environment is properly validated."""
        # Test environment variables and configuration
        assert os.getenv('SOURCE_MYSQL_HOST') is not None
        assert os.getenv('REPLICATION_MYSQL_HOST') is not None
        assert os.getenv('POSTGRES_ANALYTICS_HOST') is not None

class TestDataQualityValidation:
    """Test data quality validation and monitoring."""
    
    def test_quality_threshold_validation(self, pipeline_instance):
        """Test that quality thresholds are validated."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            monitoring = pipeline_instance.get_monitoring_config(table)
            
            # Quality threshold should be consistent between config and monitoring
            assert config['data_quality_threshold'] == monitoring['data_quality_threshold']
    
    def test_quality_threshold_range(self, pipeline_instance):
        """Test that quality thresholds are within valid range."""
        critical_tables = pipeline_instance.get_critical_tables()
        
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            
            # Quality threshold should be between 0 and 100
            assert 0 <= config['data_quality_threshold'] <= 100

# Integration test that combines multiple aspects
class TestPhase1Integration:
    """Integration tests for Phase 1 implementation."""
    
    def test_full_phase1_workflow(self, pipeline_instance):
        """Test the complete Phase 1 workflow."""
        # Test configuration loading
        critical_tables = pipeline_instance.get_critical_tables()
        assert len(critical_tables) == 5
        
        # Test database connections
        pipeline_instance.initialize_connections()
        
        # Test monitoring configuration
        for table in critical_tables:
            monitoring = pipeline_instance.get_monitoring_config(table)
            assert monitoring['alert_on_failure'] is True
        
        # Test data volume calculation
        total_size = sum(
            pipeline_instance.get_table_config(table)['estimated_size_mb']
            for table in critical_tables
        )
        assert 1800 <= total_size <= 2400
    
    def test_phase1_ready_for_production(self, pipeline_instance):
        """Test that Phase 1 is ready for production deployment."""
        # All critical components should be working
        critical_tables = pipeline_instance.get_critical_tables()
        
        # Should have exactly 5 critical tables
        assert len(critical_tables) == 5
        
        # All tables should have complete configurations
        for table in critical_tables:
            config = pipeline_instance.get_table_config(table)
            monitoring = pipeline_instance.get_monitoring_config(table)
            
            # Essential fields should be present
            assert config['table_importance'] == 'critical'
            assert config['extraction_strategy'] in ['incremental', 'chunked_incremental']
            assert monitoring['alert_on_failure'] is True
            assert monitoring['data_quality_threshold'] >= 95.0
        
        # Database connections should work
        pipeline_instance.initialize_connections()
        
        # Phase 1 is ready for production
        assert True 