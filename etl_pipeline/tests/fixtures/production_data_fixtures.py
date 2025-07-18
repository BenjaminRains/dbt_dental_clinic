"""
Production Data Fixtures for E2E Testing

This module provides fixtures for safely working with production data in E2E tests.
All fixtures ensure readonly access to production and complete isolation of test data.

Features:
- Safe production data sampling with readonly access
- Data anonymization for privacy protection
- Pipeline validation across all stages
- Automatic cleanup of test databases only
"""

import pytest
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def production_settings():
    """
    Production settings for readonly access to source database.
    
    Uses .env_production file with non-prefixed environment variables.
    Provides readonly access to production OpenDental database.
    """
    return Settings(environment='production')


@pytest.fixture(scope="session")
def test_settings():
    """
    Test settings for test replication and analytics databases.
    
    Uses .env_test file with TEST_ prefixed environment variables.
    Provides read/write access to test databases only.
    """
    return Settings(environment='test')


# Removed production_data_sampler fixture - ProductionDataSampler class no longer exists


# Removed data_anonymizer fixture - DataAnonymizer class no longer exists


@pytest.fixture(scope="session")
def pipeline_validator(test_settings):
    """
    Fixture for validating pipeline transformations.
    
    Provides methods to validate data integrity across:
    - Test replication database
    - Test analytics database
    - Cross-database consistency
    """
    from ..e2e.test_production_data_pipeline_e2e import PipelineDataValidator
    return PipelineDataValidator(test_settings)


@pytest.fixture(scope="session")
def test_data_cleanup(test_settings):
    """
    Fixture for cleaning up test data.
    
    SAFETY: Only cleans test databases, never touches production.
    Automatically cleans up after test session completion.
    """
    from ..e2e.test_production_data_pipeline_e2e import TestDataCleanup
    
    cleanup = TestDataCleanup(test_settings)
    yield cleanup
    
    # Cleanup after session
    cleanup.cleanup_test_data()


@pytest.fixture(scope="session")
def production_database_engines(production_settings):
    """
    Fixture providing production database engines for readonly access.
    
    Returns:
        Dict with readonly engines for production databases
    """
    return {
        'source': ConnectionFactory.get_source_connection(production_settings)
    }


@pytest.fixture(scope="session")
def test_database_engines(test_settings):
    """
    Fixture providing test database engines for read/write access.
    
    Returns:
        Dict with read/write engines for test databases
    """
    return {
        'replication': ConnectionFactory.get_replication_connection(test_settings),
        'analytics_raw': ConnectionFactory.get_analytics_raw_connection(test_settings),
        'analytics_staging': ConnectionFactory.get_analytics_staging_connection(test_settings),
        'analytics_intermediate': ConnectionFactory.get_analytics_intermediate_connection(test_settings),
        'analytics_marts': ConnectionFactory.get_analytics_marts_connection(test_settings)
    }


# Removed sampled_dental_clinic_data fixture - depends on ProductionDataSampler


# Removed large_sampled_dental_clinic_data fixture - depends on ProductionDataSampler


@pytest.fixture(scope="function")
def pipeline_performance_tracker():
    """
    Fixture for tracking pipeline performance metrics.
    
    Provides utilities to measure and validate:
    - Pipeline execution times
    - Data throughput rates
    - Memory usage patterns
    - Error rates and recovery times
    """
    import time
    import psutil
    import os
    
    class PipelinePerformanceTracker:
        def __init__(self):
            self.start_time = None
            self.end_time = None
            self.metrics = {}
            self.process = psutil.Process(os.getpid())
        
        def start_tracking(self, operation_name: str):
            """Start tracking performance for an operation."""
            self.start_time = time.time()
            self.metrics[operation_name] = {
                'start_time': self.start_time,
                'start_memory': self.process.memory_info().rss / 1024 / 1024  # MB
            }
            logger.info(f"Started tracking performance for: {operation_name}")
        
        def end_tracking(self, operation_name: str, record_count: int = 0):
            """End tracking and record final metrics."""
            self.end_time = time.time()
            end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            
            duration = self.end_time - self.metrics[operation_name]['start_time']
            memory_delta = end_memory - self.metrics[operation_name]['start_memory']
            
            self.metrics[operation_name].update({
                'end_time': self.end_time,
                'duration': duration,
                'end_memory': end_memory,
                'memory_delta': memory_delta,
                'record_count': record_count,
                'throughput': record_count / duration if duration > 0 else 0
            })
            
            logger.info(f"Completed tracking for {operation_name}: {duration:.2f}s, {record_count} records")
            return self.metrics[operation_name]
        
        def get_summary(self) -> Dict[str, Any]:
            """Get summary of all tracked operations."""
            return {
                'total_operations': len(self.metrics),
                'operations': self.metrics,
                'total_duration': sum(m.get('duration', 0) for m in self.metrics.values()),
                'total_records': sum(m.get('record_count', 0) for m in self.metrics.values())
            }
    
    return PipelinePerformanceTracker()


@pytest.fixture(scope="function")
def data_quality_validator(test_settings):
    """
    Fixture for validating data quality in pipeline.
    
    Provides utilities to validate:
    - Data type consistency
    - Field mapping accuracy
    - Business rule compliance
    - Data completeness
    """
    from sqlalchemy import text
    
    class DataQualityValidator:
        def __init__(self, test_settings: Settings):
            self.test_settings = test_settings
            self.replication_engine = ConnectionFactory.get_replication_connection(test_settings)
            self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
        
        def validate_patient_data_quality(self) -> Dict[str, Any]:
            """Validate patient data quality across pipeline stages."""
            validation_results = {}
            
            # Check replication data quality
            with self.replication_engine.connect() as conn:
                # Check for null values in required fields
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN LName IS NULL OR LName = '' THEN 1 ELSE 0 END) as null_lname,
                        SUM(CASE WHEN FName IS NULL OR FName = '' THEN 1 ELSE 0 END) as null_fname,
                        SUM(CASE WHEN PatNum IS NULL THEN 1 ELSE 0 END) as null_patnum
                    FROM patient 
                    WHERE LName LIKE 'E2ETestLast%'
                """))
                row = result.fetchone()
                replication_quality = dict(row._mapping) if row else {}
            
            # Check analytics data quality
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN "LName" IS NULL OR "LName" = '' THEN 1 ELSE 0 END) as null_lname,
                        SUM(CASE WHEN "FName" IS NULL OR "FName" = '' THEN 1 ELSE 0 END) as null_fname,
                        SUM(CASE WHEN "PatNum" IS NULL THEN 1 ELSE 0 END) as null_patnum
                    FROM raw.patient 
                    WHERE "LName" LIKE 'E2ETestLast%'
                """))
                row = result.fetchone()
                analytics_quality = dict(row._mapping) if row else {}
            
            validation_results = {
                'replication_quality': replication_quality,
                'analytics_quality': analytics_quality,
                'quality_consistent': replication_quality == analytics_quality
            }
            
            return validation_results
        
        def validate_appointment_data_quality(self) -> Dict[str, Any]:
            """Validate appointment data quality across pipeline stages."""
            validation_results = {}
            
            # Check for data completeness and consistency
            with self.replication_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN AptDateTime IS NULL THEN 1 ELSE 0 END) as null_datetime,
                        SUM(CASE WHEN PatNum IS NULL THEN 1 ELSE 0 END) as null_patnum,
                        SUM(CASE WHEN AptStatus IS NULL THEN 1 ELSE 0 END) as null_status
                    FROM appointment 
                    WHERE Note LIKE 'E2E Test Appointment%'
                """))
                row = result.fetchone()
                replication_quality = dict(row._mapping) if row else {}
            
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN "AptDateTime" IS NULL THEN 1 ELSE 0 END) as null_datetime,
                        SUM(CASE WHEN "PatNum" IS NULL THEN 1 ELSE 0 END) as null_patnum,
                        SUM(CASE WHEN "AptStatus" IS NULL THEN 1 ELSE 0 END) as null_status
                    FROM raw.appointment 
                    WHERE "Note" LIKE 'E2E Test Appointment%'
                """))
                row = result.fetchone()
                analytics_quality = dict(row._mapping) if row else {}
            
            validation_results = {
                'replication_quality': replication_quality,
                'analytics_quality': analytics_quality,
                'quality_consistent': replication_quality == analytics_quality
            }
            
            return validation_results
    
    return DataQualityValidator(test_settings)


@pytest.fixture(scope="function")
def error_injection_simulator():
    """
    Fixture for simulating error conditions in pipeline.
    
    Provides utilities to simulate:
    - Database connection failures
    - Data corruption scenarios
    - Network interruptions
    - Resource exhaustion conditions
    """
    import random
    from unittest.mock import patch
    
    class ErrorInjectionSimulator:
        def __init__(self):
            self.active_patches = []
        
        def simulate_connection_failure(self, component: str, failure_rate: float = 0.1):
            """Simulate database connection failures."""
            # This is a placeholder - would need proper mock implementation
            logger.info(f"Simulating connection failures for {component} with rate {failure_rate}")
        
        def simulate_data_corruption(self, corruption_rate: float = 0.05):
            """Simulate data corruption scenarios."""
            def corrupt_data(data):
                if random.random() < corruption_rate:
                    # Simulate data corruption
                    if isinstance(data, dict) and 'PatNum' in data:
                        data['PatNum'] = None  # Corrupt primary key
                return data
            
            logger.info(f"Simulating data corruption with rate {corruption_rate}")
            return corrupt_data
        
        def cleanup_simulations(self):
            """Clean up all active error simulations."""
            for patch_obj in self.active_patches:
                patch_obj.stop()
            self.active_patches.clear()
    
    simulator = ErrorInjectionSimulator()
    yield simulator
    simulator.cleanup_simulations()


# Performance testing markers - commented out for future development
# pytest.mark.performance = pytest.mark.mark(
#     "performance", 
#     "Performance testing with realistic data volumes"
# )
# 
# pytest.mark.quality = pytest.mark.mark(
#     "quality", 
#     "Data quality validation testing"
# )
# 
# pytest.mark.resilience = pytest.mark.mark(
#     "resilience", 
#     "Error handling and recovery testing"
# )