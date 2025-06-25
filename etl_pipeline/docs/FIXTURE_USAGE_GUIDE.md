# Fixture Usage Guide

This guide shows how to use the fixtures defined in `conftest.py` in your actual tests.

## Table of Contents
1. [Basic Fixture Usage](#basic-fixture-usage)
2. [Unit Test Examples](#unit-test-examples)
3. [Integration Test Examples](#integration-test-examples)
4. [Performance Test Examples](#performance-test-examples)
5. [Idempotency Test Examples](#idempotency-test-examples)
6. [Mock Fixture Examples](#mock-fixture-examples)
7. [Database Fixture Examples](#database-fixture-examples)

## Basic Fixture Usage

### How Fixtures Work
Fixtures are automatically injected into test functions as parameters:

```python
def test_patient_processing(sample_patient_data):
    """The sample_patient_data fixture is automatically injected."""
    assert len(sample_patient_data) == 5  # 5 sample patients
    assert 'PatNum' in sample_patient_data.columns
```

### Multiple Fixtures
You can use multiple fixtures in a single test:

```python
def test_pipeline_with_data(sample_patient_data, mock_analytics_engine, mock_settings):
    """Use multiple fixtures together."""
    processor = PatientProcessor(mock_analytics_engine, mock_settings)
    result = processor.process(sample_patient_data)
    assert result.success
```

## Unit Test Examples

### 1. Patient Data Processing

```python
# tests/unit/transformers/test_patient_transformer.py
import pytest
import pandas as pd
from etl_pipeline.transformers.patient_transformer import PatientTransformer

def test_patient_data_validation(sample_patient_data):
    """Test patient data validation with sample data."""
    transformer = PatientTransformer()
    
    # Validate the sample data
    result = transformer.validate_data(sample_patient_data)
    
    assert result.is_valid
    assert result.valid_count == 5
    assert result.invalid_count == 0

def test_patient_field_mapping(sample_patient_data):
    """Test field mapping from source to target schema."""
    transformer = PatientTransformer()
    
    # Transform the data
    transformed_data = transformer.transform(sample_patient_data)
    
    # Check that required fields are present
    required_fields = ['patient_id', 'first_name', 'last_name', 'email', 'phone']
    for field in required_fields:
        assert field in transformed_data.columns
    
    # Check data types
    assert transformed_data['patient_id'].dtype == 'int64'
    assert transformed_data['first_name'].dtype == 'object'

def test_patient_data_cleaning(sample_patient_data):
    """Test data cleaning operations."""
    transformer = PatientTransformer()
    
    # Add some dirty data
    dirty_data = sample_patient_data.copy()
    dirty_data.loc[0, 'Email'] = '  JOHN.DOE@EXAMPLE.COM  '  # Extra spaces, uppercase
    
    # Clean the data
    cleaned_data = transformer.clean_data(dirty_data)
    
    # Verify cleaning worked
    assert cleaned_data.loc[0, 'Email'] == 'john.doe@example.com'
```

### 2. Appointment Data Processing

```python
# tests/unit/transformers/test_appointment_transformer.py
import pytest
from etl_pipeline.transformers.appointment_transformer import AppointmentTransformer

def test_appointment_status_mapping(sample_appointment_data):
    """Test appointment status code mapping."""
    transformer = AppointmentTransformer()
    
    # Check status mappings
    status_mappings = {
        1: 'scheduled',
        2: 'completed', 
        5: 'broken_missed'
    }
    
    for source_status, expected_status in status_mappings.items():
        test_data = sample_appointment_data[sample_appointment_data['AptStatus'] == source_status]
        if not test_data.empty:
            transformed = transformer.transform(test_data)
            assert transformed.iloc[0]['status'] == expected_status

def test_appointment_datetime_parsing(sample_appointment_data):
    """Test datetime field parsing."""
    transformer = AppointmentTransformer()
    
    transformed_data = transformer.transform(sample_appointment_data)
    
    # Check that datetime fields are properly parsed
    assert pd.api.types.is_datetime64_any_dtype(transformed_data['appointment_datetime'])
    assert pd.api.types.is_datetime64_any_dtype(transformed_data['created_at'])
```

### 3. Securitylog Data Processing

```python
# tests/unit/transformers/test_securitylog_transformer.py
import pytest
from etl_pipeline.transformers.securitylog_transformer import SecurityLogTransformer

def test_securitylog_permission_mapping(sample_securitylog_data):
    """Test permission type mapping."""
    transformer = SecurityLogTransformer()
    
    transformed_data = transformer.transform(sample_securitylog_data)
    
    # Check permission type mappings
    permission_mappings = {
        1: 'login',
        2: 'patient_access',
        3: 'appointment_access',
        4: 'procedure_access',
        5: 'payment_access'
    }
    
    for source_perm, expected_perm in permission_mappings.items():
        test_data = sample_securitylog_data[sample_securitylog_data['"PermType"'] == source_perm]
        if not test_data.empty:
            transformed = transformer.transform(test_data)
            assert transformed.iloc[0]['permission_type'] == expected_perm

def test_securitylog_error_handling(sample_securitylog_data):
    """Test handling of error records."""
    transformer = SecurityLogTransformer()
    
    # Find records with errors
    error_records = sample_securitylog_data[sample_securitylog_data['"DefNumError"'].notna()]
    
    if not error_records.empty:
        transformed = transformer.transform(error_records)
        assert 'has_error' in transformed.columns
        assert transformed['has_error'].all()
```

## Integration Test Examples

### 1. End-to-End Pipeline Testing

```python
# tests/integration/test_full_pipeline.py
import pytest
import time
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

def test_full_pipeline_with_sample_data(
    sample_patient_data, 
    sample_appointment_data, 
    sample_procedure_data,
    test_replication_database,
    test_analytics_database
):
    """Test complete pipeline with sample data."""
    # Initialize pipeline
    orchestrator = PipelineOrchestrator()
    
    # Run pipeline
    start_time = time.time()
    result = orchestrator.run_pipeline()
    duration = time.time() - start_time
    
    # Verify results
    assert result.success
    assert result.tables_processed > 0
    assert duration < 60  # Should complete within 1 minute
    
    # Verify data was loaded
    assert result.patients_loaded == len(sample_patient_data)
    assert result.appointments_loaded == len(sample_appointment_data)
    assert result.procedures_loaded == len(sample_procedure_data)

def test_pipeline_idempotency(
    sample_patient_data,
    test_replication_database,
    test_analytics_database
):
    """Test that running pipeline twice produces same results."""
    orchestrator = PipelineOrchestrator()
    
    # First run
    result1 = orchestrator.run_pipeline()
    assert result1.success
    
    # Second run (should be idempotent)
    result2 = orchestrator.run_pipeline()
    assert result2.success
    
    # Verify same results
    assert result1.patients_loaded == result2.patients_loaded
    assert result1.appointments_loaded == result2.appointments_loaded
```

### 2. Database Integration Testing

```python
# tests/integration/test_database_integration.py
import pytest
from sqlalchemy import text
from etl_pipeline.core.connections import ConnectionFactory

def test_mysql_replication_with_real_data(
    sample_patient_data,
    test_replication_database
):
    """Test MySQL replication with real test database."""
    # Create replication engine
    engine = ConnectionFactory.create_mysql_connection(
        host=test_replication_database['host'],
        port=test_replication_database['port'],
        database=test_replication_database['database'],
        user=test_replication_database['user'],
        password=test_replication_database['password']
    )
    
    # Load test data
    with engine.connect() as conn:
        # Create table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS patient (
                PatNum INT PRIMARY KEY,
                LName VARCHAR(100),
                FName VARCHAR(100),
                Email VARCHAR(255)
            )
        """))
        
        # Insert test data
        for _, row in sample_patient_data.iterrows():
            conn.execute(text("""
                INSERT INTO patient (PatNum, LName, FName, Email)
                VALUES (:patnum, :lname, :fname, :email)
            """), {
                'patnum': row['PatNum'],
                'lname': row['LName'],
                'fname': row['FName'],
                'email': row['Email']
            })
        
        # Verify data
        result = conn.execute(text("SELECT COUNT(*) FROM patient"))
        count = result.scalar()
        assert count == len(sample_patient_data)

def test_postgres_analytics_with_real_data(
    sample_securitylog_data,
    test_analytics_database
):
    """Test PostgreSQL analytics with real test database."""
    # Create analytics engine
    engine = ConnectionFactory.create_postgres_connection(
        host=test_analytics_database['host'],
        port=test_analytics_database['port'],
        database=test_analytics_database['database'],
        schema=test_analytics_database['schema'],
        user=test_analytics_database['user'],
        password=test_analytics_database['password']
    )
    
    # Load test data
    with engine.connect() as conn:
        # Create table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS securitylog (
                "SecurityLogNum" SERIAL PRIMARY KEY,
                "PermType" SMALLINT,
                "UserNum" BIGINT,
                "LogDateTime" TIMESTAMP,
                "LogText" TEXT,
                "PatNum" BIGINT,
                "CompName" VARCHAR(255),
                "FKey" BIGINT,
                "LogSource" SMALLINT,
                "DefNum" BIGINT,
                "DefNumError" BIGINT,
                "DateTPrevious" TIMESTAMP
            )
        """))
        
        # Insert test data
        for _, row in sample_securitylog_data.iterrows():
            conn.execute(text("""
                INSERT INTO securitylog (
                    "PermType", "UserNum", "LogDateTime", "LogText", 
                    "PatNum", "CompName", "FKey", "LogSource", 
                    "DefNum", "DefNumError", "DateTPrevious"
                ) VALUES (
                    :permtype, :usernum, :logdatetime, :logtext,
                    :patnum, :compname, :fkey, :logsource,
                    :defnum, :defnumerror, :dateprevious
                )
            """), {
                'permtype': row['"PermType"'],
                'usernum': row['"UserNum"'],
                'logdatetime': row['"LogDateTime"'],
                'logtext': row['"LogText"'],
                'patnum': row['"PatNum"'],
                'compname': row['"CompName"'],
                'fkey': row['"FKey"'],
                'logsource': row['"LogSource"'],
                'defnum': row['"DefNum"'],
                'defnumerror': row['"DefNumError"'],
                'dateprevious': row['"DateTPrevious"']
            })
        
        # Verify data
        result = conn.execute(text("SELECT COUNT(*) FROM securitylog"))
        count = result.scalar()
        assert count == len(sample_securitylog_data)
```

## Performance Test Examples

### 1. Large Dataset Performance Testing

```python
# tests/performance/test_large_dataset_performance.py
import pytest
import time
from etl_pipeline.loaders.postgres_loader import PostgresLoader

def test_securitylog_large_dataset_performance(
    large_test_dataset,
    test_analytics_database,
    performance_test_config
):
    """Test performance with large securitylog dataset."""
    # Generate large dataset
    large_data = large_test_dataset(100000)  # 100K records
    
    # Initialize loader
    loader = PostgresLoader(
        host=test_analytics_database['host'],
        port=test_analytics_database['port'],
        database=test_analytics_database['database'],
        user=test_analytics_database['user'],
        password=test_analytics_database['password']
    )
    
    # Measure performance
    start_time = time.time()
    result = loader.load_table('securitylog', large_data)
    duration = time.time() - start_time
    
    # Verify performance requirements
    expected_duration = performance_test_config['medium_dataset']['expected_duration_seconds']
    assert duration < expected_duration, f"Performance test failed: {duration}s > {expected_duration}s"
    
    # Verify data integrity
    assert result.records_loaded == len(large_data)
    assert result.success

def test_securitylog_extreme_performance(
    large_securitylog_dataset,
    test_analytics_database,
    performance_test_config
):
    """Test extreme performance with very large securitylog dataset."""
    # Generate extreme dataset
    extreme_data = large_securitylog_dataset(1000000)  # 1M records
    
    # Initialize loader
    loader = PostgresLoader(
        host=test_analytics_database['host'],
        port=test_analytics_database['port'],
        database=test_analytics_database['database'],
        user=test_analytics_database['user'],
        password=test_analytics_database['password']
    )
    
    # Measure performance
    start_time = time.time()
    result = loader.load_table('securitylog', extreme_data)
    duration = time.time() - start_time
    
    # Verify performance requirements
    expected_duration = performance_test_config['securitylog_performance']['test_scenarios']['large']['expected_seconds']
    assert duration < expected_duration, f"Extreme performance test failed: {duration}s > {expected_duration}s"
    
    # Verify data integrity
    assert result.records_loaded == len(extreme_data)
    assert result.success
```

## Idempotency Test Examples

### 1. Pipeline Idempotency Testing

```python
# tests/integration/test_idempotency.py
import pytest
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

def test_pipeline_idempotent_processing(
    sample_patient_data,
    sample_appointment_data,
    test_replication_database,
    test_analytics_database,
    idempotency_test_data
):
    """Test that pipeline is idempotent (running twice produces same results)."""
    orchestrator = PipelineOrchestrator()
    
    # First run
    result1 = orchestrator.run_pipeline(force_full=False)
    assert result1.success
    
    # Record initial state
    initial_state = {
        'patient_count': result1.patients_loaded,
        'appointment_count': result1.appointments_loaded,
        'processing_time': result1.processing_time
    }
    
    # Second run (should be idempotent)
    result2 = orchestrator.run_pipeline(force_full=False)
    assert result2.success
    
    # Verify idempotency
    assert result2.patients_loaded == initial_state['patient_count']
    assert result2.appointments_loaded == initial_state['appointment_count']
    
    # Verify no duplicate processing
    assert result2.processing_time < result1.processing_time  # Should be faster on second run

def test_incremental_processing_with_changes(
    sample_patient_data,
    test_replication_database,
    test_analytics_database,
    incremental_test_scenarios
):
    """Test incremental processing when source data changes."""
    orchestrator = PipelineOrchestrator()
    
    # Initial run
    result1 = orchestrator.run_pipeline(force_full=False)
    assert result1.success
    
    # Simulate source data changes
    changes = incremental_test_scenarios['multiple_changes']['source_changes']
    
    # Apply changes to source database
    with test_replication_database.connect() as conn:
        for change in changes:
            if change['table'] == 'patient':
                conn.execute(text("""
                    UPDATE patient 
                    SET LName = :new_value 
                    WHERE PatNum = :id
                """), {
                    'new_value': change['new_value'],
                    'id': change['id']
                })
    
    # Second run (should process only changes)
    result2 = orchestrator.run_pipeline(force_full=False)
    assert result2.success
    
    # Verify only changed data was processed
    assert result2.patients_processed == len(changes)
    assert result2.processing_time < result1.processing_time
```

## Mock Fixture Examples

### 1. Using Mock Database Engines

```python
# tests/unit/core/test_mysql_replicator.py
import pytest
from unittest.mock import MagicMock
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator

def test_mysql_replicator_with_mocks(
    mock_source_engine,
    mock_replication_engine,
    sample_patient_data
):
    """Test MySQL replicator with mocked engines."""
    # Configure mocks
    mock_source_engine.execute.return_value.fetchall.return_value = sample_patient_data.to_dict('records')
    mock_replication_engine.execute.return_value = MagicMock()
    
    # Create replicator
    replicator = ExactMySQLReplicator(
        source_engine=mock_source_engine,
        target_engine=mock_replication_engine,
        source_db='opendental',
        target_db='opendental_replication'
    )
    
    # Test replication
    result = replicator.copy_table_data('patient')
    
    # Verify mocks were called correctly
    mock_source_engine.execute.assert_called()
    mock_replication_engine.execute.assert_called()
    assert result is True

def test_connection_factory_with_mocks(mock_connection_factory):
    """Test connection factory with mocked connections."""
    # Test source connection
    source_engine = mock_connection_factory.get_opendental_source_connection()
    assert source_engine is not None
    
    # Test replication connection
    replication_engine = mock_connection_factory.get_mysql_replication_connection()
    assert replication_engine is not None
    
    # Test analytics connection
    analytics_engine = mock_connection_factory.get_postgres_analytics_connection()
    assert analytics_engine is not None
```

### 2. Using Mock Settings

```python
# tests/unit/config/test_settings_integration.py
import pytest
from etl_pipeline.config.settings import Settings

def test_settings_with_mock(mock_settings):
    """Test settings integration with mocked configuration."""
    # Test database config
    db_config = mock_settings.get_database_config('source')
    assert db_config['host'] == 'localhost'
    assert db_config['database'] == 'test_db'
    
    # Test table config
    table_config = mock_settings.get_table_config('patient')
    assert table_config['incremental'] is True
    assert table_config['batch_size'] == 1000
    
    # Test incremental settings
    assert mock_settings.should_use_incremental() is True
    
    # Test table priority
    tables = mock_settings.get_tables_by_priority()
    assert 'patient' in tables
    assert 'appointment' in tables
```

## Database Fixture Examples

### 1. Using Test Database Configuration

```python
# tests/integration/test_database_configuration.py
import pytest
from etl_pipeline.core.connections import ConnectionFactory

def test_database_configuration(test_database_config):
    """Test database configuration fixtures."""
    # Test source database config
    source_config = test_database_config['source']
    assert source_config['testing_strategy'] == 'MOCK_ONLY'
    assert source_config['user'] == 'readonly_user'
    
    # Test replication database config
    replication_config = test_database_config['replication']
    assert replication_config['testing_strategy'] == 'REAL_TEST_DATA'
    assert replication_config['user'] == 'replication_user'
    assert replication_config['port'] == 3305
    
    # Test analytics database config
    analytics_config = test_database_config['analytics']
    assert analytics_config['testing_strategy'] == 'REAL_TEST_DATA'
    assert analytics_config['user'] == 'analytics_user'
    assert analytics_config['schema'] == 'raw'

def test_environment_configuration(test_environment_config):
    """Test environment configuration fixtures."""
    # Test environment settings
    assert test_environment_config['environment'] == 'test'
    assert test_environment_config['log_level'] == 'DEBUG'
    
    # Test user permissions
    readonly_user = test_environment_config['users']['readonly_user']
    assert readonly_user['permissions'] == ['SELECT']
    assert readonly_user['testing_strategy'] == 'MOCK_ONLY'
    
    replication_user = test_environment_config['users']['replication_user']
    assert 'CREATE' in replication_user['permissions']
    assert 'DROP' in replication_user['permissions']
    assert replication_user['testing_strategy'] == 'REAL_TEST_DATA'
```

## Best Practices

### 1. Fixture Naming
- Use descriptive names that indicate the fixture's purpose
- Prefix with `sample_` for small test data
- Prefix with `large_` for performance testing data
- Prefix with `mock_` for mocked objects

### 2. Fixture Scope
- Use `function` scope for most fixtures (default)
- Use `session` scope for expensive setup (databases, large data)
- Use `class` scope for shared state within test classes

### 3. Fixture Dependencies
- Keep fixtures independent when possible
- Use dependency injection for complex scenarios
- Document fixture dependencies clearly

### 4. Performance Considerations
- Use small datasets for unit tests
- Use realistic datasets for integration tests
- Use large datasets only for performance tests
- Clean up resources in fixture teardown

### 5. Test Organization
- Group related tests in classes
- Use descriptive test names
- Include setup and teardown in fixtures
- Document test scenarios and expected outcomes

## Running Tests with Fixtures

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/unit/transformers/test_patient_transformer.py

# Run tests with specific marker
pytest tests/ -m "unit"

# Run tests with verbose output
pytest tests/ -v

# Run tests with coverage
pytest tests/ --cov=etl_pipeline --cov-report=html

# Run performance tests only
pytest tests/performance/ -m "performance"

# Run integration tests only
pytest tests/integration/ -m "integration"
```

This guide provides comprehensive examples of how to use the fixtures in your ETL pipeline tests. The fixtures are designed to be flexible, reusable, and maintainable across different types of testing scenarios. 