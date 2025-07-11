# E2E Testing Plan for Production Database Validation

## Overview

This document outlines the comprehensive E2E (End-to-End) testing strategy for validating the ETL pipeline using production database connections with test data. The plan follows the connection architecture principles outlined in `connection_architecture.md` and ensures safe, thorough validation of the complete data flow.

## Architecture Principles

### Connection Architecture Compliance
- **Settings Injection**: Use `Settings(environment='production')` for E2E tests
- **FileConfigProvider**: Load configuration from `.env_production` for real database connections
- **ConnectionFactory**: Use unified interface with production settings injection
- **Test Data Isolation**: Use unique identifiers to prevent interference with production data

### Safety & Security
- **FAIL FAST**: ETL_ENVIRONMENT must be explicitly set to 'production'
- **Test Data Isolation**: All test data uses unique prefixes (E2E_TEST_*)
- **Complete Cleanup**: Remove all test data after E2E test completion
- **No Production Impact**: Test data doesn't interfere with real production data

## Phase 1: Current State Analysis & Fixes

### 1.1 Diagnose Current E2E Test Issues

**Current Problems:**
- Environment configuration errors in E2E tests
- Missing environment variables for production database connections
- Test data management not properly isolated
- Connection factory not using production settings injection

**Diagnostic Steps:**
```bash
# Test current E2E test environment
pipenv run python -m pytest tests/e2e/ -v --tb=long

# Validate production environment configuration
python -c "from etl_pipeline.config import Settings; s = Settings(environment='production'); print('Production config valid:', s.validate_configs())"
```

### 1.2 Fix Environment Configuration

**Required Changes:**
- Set `ETL_ENVIRONMENT=production` for E2E test environment
- Ensure all required variables present in `.env_production`
- Use `FileConfigProvider` to load production configuration
- Validate `ConnectionFactory` works with production settings

**Configuration Validation:**
```python
# E2E tests should use production environment
settings = Settings(environment='production')  # Uses .env_production
assert settings.validate_configs() is True

# Test all connection types with production settings
source_engine = ConnectionFactory.get_source_connection(settings)
replication_engine = ConnectionFactory.get_replication_connection(settings)
analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
```

### 1.3 Implement Test Data Isolation

**Test Data Strategy:**
```python
# Unique test identifiers for safe isolation
test_identifiers = {
    'patient_lname_prefix': 'E2E_TEST_PATIENT_',
    'appointment_notes_prefix': 'E2E_TEST_APPOINTMENT_',
    'procedure_code_prefix': 'E2E_TEST_PROC_',
    'insurance_carrier_prefix': 'E2E_TEST_INSURANCE_'
}

# Safe data insertion with unique prefixes
def create_test_patient():
    return {
        'LName': f'{test_identifiers["patient_lname_prefix"]}001',
        'FName': 'John',
        'Email': 'john_e2e_001@test.com',
        # ... other fields
    }

# Complete cleanup after tests
def cleanup_test_data():
    for prefix in test_identifiers.values():
        # DELETE FROM table WHERE field LIKE 'E2E_TEST_%'
        pass
```

## Phase 2: Production Database E2E Setup

### 2.1 E2E Test Environment Configuration

**Settings Configuration:**
```python
# E2E tests use production environment with FileConfigProvider
from etl_pipeline.config import Settings
from etl_pipeline.config.providers import FileConfigProvider

# Use production settings for real database connections
settings = Settings(environment='production')  # Loads .env_production
assert settings.validate_configs() is True

# Get production database connections
source_engine = ConnectionFactory.get_source_connection(settings)
replication_engine = ConnectionFactory.get_replication_connection(settings)
analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
```

**Environment Variables Required:**
```bash
# .env_production must contain all required variables
ETL_ENVIRONMENT=production
OPENDENTAL_SOURCE_HOST=192.168.2.10
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=Ben3015
# ... all other production database variables
```

### 2.2 Test Data Management Strategy

**E2E Test Data Manager:**
```python
class E2ETestDataManager:
    def __init__(self):
        # Use production settings for real database connections
        settings = Settings(environment='production')
        self.source_engine = ConnectionFactory.get_source_connection(settings)
        self.replication_engine = ConnectionFactory.get_replication_connection(settings)
        self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        
        # Unique test identifiers for safe isolation
        self.test_identifiers = {
            'patient_lname_prefix': 'E2E_TEST_PATIENT_',
            'appointment_notes_prefix': 'E2E_TEST_APPOINTMENT_',
            'procedure_code_prefix': 'E2E_TEST_PROC_'
        }
        
        # Test data tracking
        self.test_patients = []
        self.test_appointments = []
        self.test_procedures = []
    
    def create_comprehensive_test_data(self):
        """Create test data in production source database."""
        # Insert test patients with unique identifiers
        # Insert test appointments with unique identifiers
        # Insert test procedures with unique identifiers
    
    def cleanup_all_databases(self):
        """Remove all test data from all databases."""
        # DELETE FROM all tables WHERE field LIKE 'E2E_TEST_%'
        pass
```

### 2.3 Complete Data Flow Validation

**Validation Points:**
1. **Source → Replication**: Test MySQL replication with real connections
2. **Replication → Analytics**: Test PostgreSQL loading with real connections
3. **Data transformations**: Validate schema conversions and data type handling
4. **Performance metrics**: Measure real timing and resource usage

**Data Flow Architecture:**
```
Production Source (MySQL) → Replication (MySQL) → Analytics (PostgreSQL)
       ↓                           ↓                      ↓
   Test Data Insert          Real Replication        Real Loading
   (E2E_TEST_*)             (Production Engine)     (Production Engine)
       ↓                           ↓                      ↓
   Pipeline Execution        Data Verification       Data Verification
   (Real Connections)       (Row Counts)           (Data Integrity)
```

## Phase 3: E2E Test Implementation

### 3.1 Core E2E Test Scenarios

**Single Table Pipeline Tests:**
```python
@pytest.mark.e2e
def test_complete_patient_pipeline_e2e(self, e2e_test_data_manager):
    """Test complete E2E pipeline for patient data flow."""
    # Run complete pipeline for patient table
    pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient'])
    
    # Verify pipeline success
    assert pipeline_results['patient']['success'], "Patient pipeline failed"
    
    # Verify data appears in replication
    replication_results = e2e_test_data_manager.verify_data_in_replication()
    assert replication_results['replication_patients'] == len(e2e_test_data_manager.test_patients)
    
    # Verify data appears in analytics
    analytics_results = e2e_test_data_manager.verify_data_in_analytics()
    assert analytics_results['analytics_patients'] == len(e2e_test_data_manager.test_patients)
    
    # Verify data integrity
    integrity_results = e2e_test_data_manager.verify_data_integrity()
    assert integrity_results['patient_integrity']['consistent']
```

**Multi-Table Pipeline Tests:**
```python
@pytest.mark.e2e
def test_complete_multiple_table_pipeline_e2e(self, e2e_test_data_manager):
    """Test complete E2E pipeline for multiple tables simultaneously."""
    # Run complete pipeline for all test tables
    pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient', 'appointment', 'procedure'])
    
    # Verify all pipelines succeeded
    for table, result in pipeline_results.items():
        assert result['success'], f"{table} pipeline failed"
    
    # Verify data appears in all stages
    replication_results = e2e_test_data_manager.verify_data_in_replication()
    analytics_results = e2e_test_data_manager.verify_data_in_analytics()
    
    # Verify complete data integrity
    integrity_results = e2e_test_data_manager.verify_data_integrity()
    assert integrity_results['patient_integrity']['consistent']
```

### 3.2 Test Data Creation & Cleanup

**Test Data Creation:**
```python
def create_comprehensive_test_data(self):
    """Create comprehensive test data for E2E testing."""
    # Create test patients with comprehensive data
    test_patients_data = [
        {
            'LName': f'{self.test_identifiers["patient_lname_prefix"]}001',
            'FName': 'John',
            'Birthdate': '1980-01-01',
            'Email': 'john_e2e_001@test.com',
            'Phone': '555-0101',
            'Address': '123 E2E Test St',
            'City': 'Test City',
            'State': 'TS',
            'Zip': '12345',
            'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        # ... more test patients
    ]
    
    # Insert test data into production source database
    with self.source_engine.begin() as conn:
        for patient_data in test_patients_data:
            result = conn.execute(text("""
                INSERT INTO patient (LName, FName, Birthdate, Email, Phone, Address, City, State, Zip, DateTStamp)
                VALUES (:lname, :fname, :birthdate, :email, :phone, :address, :city, :state, :zip, :datestamp)
            """), patient_data)
            patient_data['PatNum'] = result.lastrowid
            self.test_patients.append(patient_data)
```

**Test Data Cleanup:**
```python
def cleanup_all_databases(self):
    """Remove all test data from all databases."""
    # Clean up source database
    with self.source_engine.begin() as conn:
        # Delete test procedures
        for procedure in self.test_procedures:
            if procedure['ProcNum']:
                conn.execute(text("DELETE FROM procedure WHERE ProcNum = :procnum"), 
                           {'procnum': procedure['ProcNum']})
        
        # Delete test appointments
        for appointment in self.test_appointments:
            if appointment['AptNum']:
                conn.execute(text("DELETE FROM appointment WHERE AptNum = :aptnum"), 
                           {'aptnum': appointment['AptNum']})
        
        # Delete test patients
        for patient in self.test_patients:
            if patient['PatNum']:
                conn.execute(text("DELETE FROM patient WHERE PatNum = :patnum"), 
                           {'patnum': patient['PatNum']})
    
    # Clean up replication database
    with self.replication_engine.begin() as conn:
        # Delete test data from replication
        conn.execute(text("DELETE FROM procedure WHERE ProcCode LIKE :prefix"), 
                   {'prefix': f'{self.test_identifiers["procedure_code_prefix"]}%'})
        conn.execute(text("DELETE FROM appointment WHERE Notes LIKE :prefix"), 
                   {'prefix': f'{self.test_identifiers["appointment_notes_prefix"]}%'})
        conn.execute(text("DELETE FROM patient WHERE LName LIKE :prefix"), 
                   {'prefix': f'{self.test_identifiers["patient_lname_prefix"]}%'})
    
    # Clean up analytics database
    with self.analytics_engine.begin() as conn:
        # Delete test data from analytics
        conn.execute(text('DELETE FROM raw.procedure WHERE "ProcCode" LIKE :prefix'), 
                   {'prefix': f'{self.test_identifiers["procedure_code_prefix"]}%'})
        conn.execute(text('DELETE FROM raw.appointment WHERE "Notes" LIKE :prefix'), 
                   {'prefix': f'{self.test_identifiers["appointment_notes_prefix"]}%'})
        conn.execute(text('DELETE FROM raw.patient WHERE "LName" LIKE :prefix'), 
                   {'prefix': f'{self.test_identifiers["patient_lname_prefix"]}%'})
```

### 3.3 Validation Points

**Row Count Verification:**
```python
def verify_data_in_replication(self):
    """Verify test data appears in replication database."""
    with self.replication_engine.connect() as conn:
        # Count test patients in replication
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM patient 
            WHERE LName LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
        """))
        replication_patients = result.fetchone()[0]
        
        # Count test appointments in replication
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM appointment 
            WHERE Notes LIKE '{self.test_identifiers["appointment_notes_prefix"]}%'
        """))
        replication_appointments = result.fetchone()[0]
        
        return {
            'replication_patients': replication_patients,
            'replication_appointments': replication_appointments
        }
```

**Data Integrity Verification:**
```python
def verify_data_integrity(self):
    """Verify data consistency across all database stages."""
    # Compare specific records across source, replication, and analytics
    with self.source_engine.connect() as source_conn:
        result = source_conn.execute(text(f"""
            SELECT PatNum, LName, FName, Email 
            FROM patient 
            WHERE LName = '{self.test_identifiers["patient_lname_prefix"]}001'
        """))
        source_patient = result.fetchone()
    
    with self.replication_engine.connect() as repl_conn:
        result = repl_conn.execute(text(f"""
            SELECT PatNum, LName, FName, Email 
            FROM patient 
            WHERE LName = '{self.test_identifiers["patient_lname_prefix"]}001'
        """))
        replication_patient = result.fetchone()
    
    with self.analytics_engine.connect() as analytics_conn:
        result = analytics_conn.execute(text(f"""
            SELECT "PatNum", "LName", "FName", "Email" 
            FROM raw.patient 
            WHERE "LName" = '{self.test_identifiers["patient_lname_prefix"]}001'
        """))
        analytics_patient = result.fetchone()
    
    # Verify data consistency
    assert source_patient is not None, "Source patient not found"
    assert replication_patient is not None, "Replication patient not found"
    assert analytics_patient is not None, "Analytics patient not found"
    
    # Compare key fields
    assert source_patient[1] == replication_patient[1] == analytics_patient[1], "LName mismatch"
    assert source_patient[2] == replication_patient[2] == analytics_patient[2], "FName mismatch"
    assert source_patient[3] == replication_patient[3] == analytics_patient[3], "Email mismatch"
    
    return {
        'patient_integrity': {
            'consistent': True,
            'source_record': source_patient,
            'replication_record': replication_patient,
            'analytics_record': analytics_patient
        }
    }
```

## Phase 4: Production Readiness Validation

### 4.1 Performance Benchmarking

**Performance Baselines:**
```python
@pytest.mark.e2e
def test_pipeline_performance_e2e(self, e2e_test_data_manager):
    """Test E2E pipeline performance characteristics."""
    # Run pipeline and measure performance
    start_time = time.time()
    pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient', 'appointment', 'procedure'])
    total_time = time.time() - start_time
    
    # Verify all pipelines succeeded
    for table, result in pipeline_results.items():
        assert result['success'], f"{table} pipeline failed"
    
    # Performance assertions (adjust thresholds as needed)
    assert total_time < 300, f"Pipeline took too long: {total_time:.2f}s"  # 5 minutes max
    
    # Individual table performance
    for table, result in pipeline_results.items():
        assert result['duration'] < 120, f"{table} pipeline took too long: {result['duration']:.2f}s"  # 2 minutes max per table
    
    logger.info(f"Performance test completed: Total time {total_time:.2f}s")
```

**Resource Monitoring:**
- **Memory usage**: Track memory consumption during pipeline execution
- **CPU usage**: Monitor CPU utilization for performance optimization
- **Network I/O**: Measure database connection performance
- **Disk I/O**: Monitor disk usage for large data operations

### 4.2 Error Handling Validation

**Connection Failure Testing:**
```python
@pytest.mark.e2e
def test_connection_failure_handling_e2e(self, e2e_test_data_manager):
    """Test E2E pipeline behavior when database connections fail."""
    # Simulate connection failures and verify proper error handling
    # Test retry logic and recovery procedures
    pass
```

**Data Validation Error Testing:**
```python
@pytest.mark.e2e
def test_data_validation_error_handling_e2e(self, e2e_test_data_manager):
    """Test E2E pipeline behavior with malformed or invalid data."""
    # Insert test data with validation issues and verify proper handling
    pass
```

### 4.3 Security & Safety Validation

**Test Data Isolation Verification:**
```python
@pytest.mark.e2e
def test_data_isolation_e2e(self, e2e_test_data_manager):
    """Verify test data doesn't interfere with production data."""
    # Verify test data is properly isolated with unique identifiers
    # Ensure cleanup procedures work correctly
    pass
```

**Access Control Validation:**
```python
@pytest.mark.e2e
def test_access_control_e2e(self, e2e_test_data_manager):
    """Verify proper database permissions and security."""
    # Test database access controls and security measures
    pass
```

## Phase 5: Production Deployment Confidence

### 5.1 E2E Test Automation

**CI/CD Integration:**
```yaml
# .github/workflows/e2e-tests.yml
name: E2E Tests
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          cd etl_pipeline
          pip install pipenv
          pipenv install --dev
      - name: Run E2E tests
        run: |
          cd etl_pipeline
          pipenv run python -m pytest tests/e2e/ -v --tb=short
        env:
          ETL_ENVIRONMENT: production
```

**Scheduled Validation:**
```bash
# Cron job for nightly E2E validation
0 2 * * * cd /path/to/etl_pipeline && ETL_ENVIRONMENT=production pipenv run python -m pytest tests/e2e/ -v
```

### 5.2 Production Monitoring Integration

**Metrics Collection:**
```python
# Integrate E2E test metrics with production monitoring
def collect_e2e_metrics(pipeline_results):
    """Collect E2E test metrics for monitoring."""
    metrics = {
        'e2e_test_duration': sum(result['duration'] for result in pipeline_results.values()),
        'e2e_test_success': all(result['success'] for result in pipeline_results.values()),
        'e2e_test_tables_processed': len(pipeline_results),
        'e2e_test_timestamp': datetime.now().isoformat()
    }
    
    # Send metrics to monitoring system
    send_metrics_to_monitoring(metrics)
    return metrics
```

**Performance Dashboards:**
- **E2E Test Results**: Dashboard showing test pass/fail rates
- **Performance Trends**: Track E2E test performance over time
- **Resource Usage**: Monitor memory, CPU, and network usage during E2E tests
- **Alert Integration**: Set up alerts for E2E test failures or performance degradation

### 5.3 Documentation & Procedures

**E2E Test Procedures:**
```markdown
# E2E Test Execution Guide

## Prerequisites
- Production database access configured
- ETL_ENVIRONMENT=production set
- All required environment variables present in .env_production

## Running E2E Tests
```bash
cd etl_pipeline
ETL_ENVIRONMENT=production pipenv run python -m pytest tests/e2e/ -v
```

## Interpreting Results
- All tests should pass with production database connections
- Performance should be within established baselines
- No test data should remain in production databases after cleanup
```

**Troubleshooting Guide:**
```markdown
# E2E Test Troubleshooting

## Common Issues

### Environment Configuration Errors
- **Problem**: Missing ETL_ENVIRONMENT variable
- **Solution**: Set ETL_ENVIRONMENT=production before running tests

### Database Connection Failures
- **Problem**: Cannot connect to production databases
- **Solution**: Verify .env_production contains correct connection details

### Test Data Cleanup Issues
- **Problem**: Test data remains in databases after tests
- **Solution**: Check cleanup procedures and unique identifier prefixes
```

## Implementation Timeline

### Week 1: Foundation
- [ ] Fix current E2E test environment configuration
- [ ] Implement test data isolation with unique identifiers
- [ ] Set up production database connections for E2E tests
- [ ] Validate all connection factory methods work with production settings

### Week 2: Core E2E Tests
- [ ] Implement single table E2E tests (patient, appointment, procedure)
- [ ] Implement multi-table E2E tests
- [ ] Add performance validation and data integrity checks
- [ ] Implement comprehensive test data creation and cleanup

### Week 3: Production Validation
- [ ] Run E2E tests against production databases
- [ ] Establish performance baselines
- [ ] Validate error handling and recovery procedures
- [ ] Test security and access control measures

### Week 4: Automation & Monitoring
- [ ] Integrate E2E tests into CI/CD pipeline
- [ ] Set up monitoring and alerting for E2E test results
- [ ] Document procedures and troubleshooting guides
- [ ] Create performance dashboards and metrics collection

## Success Criteria

### ✅ E2E Test Success Metrics
- **All E2E tests pass** with production database connections
- **Performance baselines** established and documented
- **Data integrity** verified across all pipeline stages
- **Error handling** validated for common failure scenarios
- **Test data isolation** confirmed with no interference with production data

### ✅ Production Readiness Indicators
- **E2E tests automated** and integrated into deployment pipeline
- **Performance monitoring** in place with alerting
- **Documentation complete** for E2E test procedures
- **Security validation** completed for production deployment
- **Cleanup procedures** verified to remove all test data

## Architecture Compliance

This E2E testing plan follows the connection architecture principles:

1. **Settings Injection**: Uses `Settings(environment='production')` for E2E tests
2. **FileConfigProvider**: Loads configuration from `.env_production` for real database connections
3. **ConnectionFactory**: Uses unified interface with production settings injection
4. **Test Data Isolation**: Uses unique identifiers to prevent interference with production data
5. **FAIL FAST**: Requires explicit `ETL_ENVIRONMENT=production` setting
6. **Provider Pattern**: Uses `FileConfigProvider` for production configuration loading

The plan ensures thorough validation of the complete ETL pipeline using real production database connections while maintaining safety and security through proper test data isolation and cleanup procedures. 