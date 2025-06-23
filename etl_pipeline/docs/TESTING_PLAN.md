# ETL Pipeline Testing Plan

## Overview

This testing plan outlines the comprehensive testing strategy for the ETL pipeline after refactoring.
 The plan addresses current testing gaps, improves test quality, and ensures robust validation of 
 all pipeline components.

## Current Testing State Analysis (UPDATED - Current Coverage: 41%)

### **✅ EXISTING TEST COVERAGE (Well Tested)**

| Component | Test Files | Status | Coverage | Current Status |
|-----------|------------|--------|----------|----------------|
| **CLI System** | `tests/cli/test_cli.py` | ✅ Comprehensive | High | **74-88% coverage** ✅ |
| **Configuration** | `tests/config/`, `tests/test_config.py` | ✅ Good | High | **80-100% coverage** ✅ |
| **Core Connections** | `tests/core/test_connections.py` | ✅ Good | High | **100% coverage** ✅ |
| **Schema Discovery** | `tests/core/test_schema_discovery.py` | ✅ Good | Medium | **82% coverage** ✅ |
| **Postgres Schema** | `tests/core/test_postgres_schema.py` | ✅ Good | Medium | **85% coverage** ✅ |
| **Logging** | `tests/config/test_logging.py` | ✅ Good | High | **100% coverage** ✅ |
| **Settings** | `tests/config/test_settings.py` | ✅ Good | High | **82% coverage** ✅ |

### **⚠️ PARTIAL TEST COVERAGE (Needs Improvement)**

| Component | Test Files | Status | Issues | Current Coverage |
|-----------|------------|--------|--------|------------------|
| **MySQL Replicator** | `tests/core/test_mysql_replicator.py` | ⚠️ Partial | Limited integration tests | **11% coverage** ❌ |
| **Raw to Public Transformer** | `tests/transformers/test_raw_to_public.py` | ⚠️ Over-mocked | Tests mocks, not real logic | **15% coverage** ❌ |
| **Unified Metrics** | `tests/core/test_metrics.py` | ⚠️ Basic | Limited functionality tests | **59% coverage** ⚠️ |
| **Pipeline Orchestrator** | None | ❌ Missing | No dedicated tests | **32% coverage** ❌ |
| **Table Processor** | None | ❌ Missing | No dedicated tests | **16% coverage** ❌ |
| **Priority Processor** | None | ❌ Missing | No dedicated tests | **16% coverage** ❌ |

### **❌ MISSING TEST COVERAGE (Critical Gaps)**

| Component | Status | Priority | Current Coverage |
|-----------|--------|----------|------------------|
| **Postgres Loader** | ❌ No unit tests | **CRITICAL** | **0% coverage** ❌ |
| **Loaders Directory** | ❌ No unit tests | **CRITICAL** | **0% coverage** ❌ |
| **Monitoring/Metrics** | ❌ No integration tests | **HIGH** | **59% coverage** ❌ |
| **Utils Directory** | ❌ No tests | Medium | **Unknown** ❌ |
| **MCP Directory** | ❌ No tests | Low | **Unknown** ❌ |

## CRITICAL PATH TO PRODUCTION: Full Coverage Plan

### **Phase 1: Critical Component Testing (Week 1) - MUST COMPLETE BEFORE PRODUCTION**

#### **1.1 Postgres Loader (0% → 90% coverage) - CRITICAL**
```python
# tests/loaders/test_postgres_loader.py
class TestPostgresLoader:
    def test_table_loading_success(self)
    def test_table_loading_with_chunks(self)
    def test_schema_creation_and_validation(self)
    def test_data_type_conversion(self)
    def test_incremental_loading_logic(self)
    def test_error_handling_database_errors(self)
    def test_error_handling_connection_errors(self)
    def test_large_table_handling(self)
    def test_transaction_rollback_on_error(self)
    def test_connection_pool_management(self)
    def test_data_validation_before_load(self)
    def test_schema_migration_handling(self)
```

#### **1.2 Pipeline Orchestrator (32% → 90% coverage) - CRITICAL**
```python
# tests/orchestration/test_pipeline_orchestrator.py
class TestPipelineOrchestrator:
    def test_initialization_with_valid_config(self)
    def test_connection_initialization_success(self)
    def test_connection_initialization_failure(self)
    def test_table_processing_by_priority_success(self)
    def test_table_processing_by_priority_failure(self)
    def test_error_handling_and_recovery(self)
    def test_metrics_collection_during_processing(self)
    def test_cleanup_and_disposal(self)
    def test_parallel_processing_limits(self)
    def test_memory_management(self)
    def test_interrupt_handling(self)
    
    # IDEMPOTENCY AND INCREMENTAL LOAD TESTS - CRITICAL
    def test_idempotent_processing_force_full_false_twice(self)
    def test_idempotent_processing_force_full_true_then_false(self)
    def test_incremental_processing_with_source_changes(self)
    def test_incremental_processing_with_no_source_changes(self)
    def test_schema_change_reverts_to_full_load(self)
    def test_incremental_flags_monitoring(self)
    def test_force_full_flag_behavior(self)
    def test_schema_changed_flag_behavior(self)
```

#### **1.3 Table Processor (16% → 90% coverage) - CRITICAL**
```python
# tests/orchestration/test_table_processor.py
class TestTableProcessor:
    def test_table_processing_flow_success(self)
    def test_table_processing_flow_failure(self)
    def test_incremental_processing_logic(self)
    def test_full_processing_logic(self)
    def test_error_handling_and_retry(self)
    def test_metrics_recording_accuracy(self)
    def test_parallel_processing_safety(self)
    def test_data_validation_before_processing(self)
    def test_schema_change_detection(self)
    def test_processing_timeout_handling(self)
    
    # IDEMPOTENCY AND INCREMENTAL LOAD TESTS - CRITICAL
    def test_idempotent_table_processing_force_full_false_twice(self)
    def test_idempotent_table_processing_force_full_true_then_false(self)
    def test_incremental_table_processing_with_source_changes(self)
    def test_incremental_table_processing_with_no_source_changes(self)
    def test_schema_change_detection_and_full_load_revert(self)
    def test_incremental_column_behavior_validation(self)
    def test_force_full_flag_overrides_incremental_logic(self)
    def test_schema_changed_flag_triggers_full_load(self)
    def test_data_consistency_after_multiple_runs(self)
    def test_incremental_processing_with_partial_failures(self)
```

#### **1.4 Idempotency and Incremental Load Testing (NEW SECTION) - CRITICAL**
```python
# tests/orchestration/test_idempotency.py
class TestIdempotencyAndIncrementalLoad:
    """CRITICAL: Tests for idempotency and incremental load behavior"""
    
    def test_force_full_false_twice_should_skip_or_process_deltas(self):
        """✅ Test with force_full=False twice → second run should skip or only process deltas"""
        # 1. Run pipeline with force_full=False
        # 2. Verify data is loaded
        # 3. Run pipeline again with force_full=False
        # 4. Verify second run either skips or only processes deltas
        # 5. Verify no duplicate data is created
        
    def test_source_row_change_triggers_incremental_processing(self):
        """✅ Change a row in source → rerun and see if only that row is processed"""
        # 1. Run initial pipeline with force_full=False
        # 2. Modify a row in source database
        # 3. Run pipeline again with force_full=False
        # 4. Verify only the changed row is processed
        # 5. Verify other rows remain unchanged
        
    def test_schema_change_reverts_to_full_load(self):
        """✅ Break schema → check that it reverts to full load"""
        # 1. Run initial pipeline with force_full=False
        # 2. Modify table schema in source (add/remove column)
        # 3. Run pipeline again with force_full=False
        # 4. Verify schema change is detected
        # 5. Verify pipeline reverts to full load
        # 6. Verify new schema is properly applied
        
    def test_incremental_flags_monitoring(self):
        """✅ Monitor flags like is_incremental, force_full, and schema_changed"""
        # 1. Test is_incremental flag behavior
        # 2. Test force_full flag behavior
        # 3. Test schema_changed flag behavior
        # 4. Verify flag combinations work correctly
        # 5. Verify flags are properly logged and monitored
        
    def test_idempotent_processing_with_no_changes(self):
        """Test that running pipeline multiple times with no changes produces identical results"""
        # 1. Run pipeline with force_full=False
        # 2. Record data state and processing metrics
        # 3. Run pipeline again with force_full=False
        # 4. Verify data state is identical
        # 5. Verify processing metrics show no new data processed
        
    def test_incremental_processing_with_multiple_changes(self):
        """Test incremental processing with multiple source changes"""
        # 1. Run initial pipeline
        # 2. Make multiple changes to source data
        # 3. Run incremental pipeline
        # 4. Verify all changes are captured
        # 5. Verify processing efficiency
        
    def test_force_full_overrides_incremental_logic(self):
        """Test that force_full=True always triggers full processing"""
        # 1. Run pipeline with force_full=True
        # 2. Verify full processing occurs regardless of incremental settings
        # 3. Verify all data is reprocessed
        # 4. Verify incremental flags are ignored
        
    def test_incremental_processing_edge_cases(self):
        """Test edge cases in incremental processing"""
        # 1. Test with empty incremental column
        # 2. Test with NULL values in incremental column
        # 3. Test with deleted rows in source
        # 4. Test with very large incremental datasets
        # 5. Test with concurrent source modifications
        
    def test_data_consistency_validation(self):
        """Test data consistency across multiple pipeline runs"""
        # 1. Run pipeline multiple times
        # 2. Verify data integrity is maintained
        # 3. Verify no data corruption occurs
        # 4. Verify referential integrity is preserved
        # 5. Verify business rules are maintained
```

### **Phase 2: Data Movement Component Testing (Week 2) - MUST COMPLETE BEFORE PRODUCTION**

#### **2.1 MySQL Replicator (11% → 85% coverage) - HIGH**
```python
# tests/core/test_mysql_replicator.py (expand existing)
class TestMySQLReplicator:
    def test_exact_replica_creation_success(self)
    def test_exact_replica_creation_failure(self)
    def test_table_data_copying_accuracy(self)
    def test_replica_verification_success(self)
    def test_replica_verification_failure(self)
    def test_schema_synchronization(self)
    def test_error_recovery_mechanisms(self)
    def test_large_table_replication(self)
    def test_incremental_replication_logic(self)
    def test_connection_failure_handling(self)
    def test_data_integrity_validation(self)
```

#### **2.2 RawToPublicTransformer (15% → 85% coverage) - HIGH**
```python
# tests/transformers/test_raw_to_public.py (refactor existing)
class TestRawToPublicTransformer:
    def test_data_transformation_logic_accuracy(self)
    def test_schema_conversion_correctness(self)
    def test_data_type_conversion_edge_cases(self)
    def test_column_transformations_validation(self)
    def test_table_specific_transformations(self)
    def test_error_handling_transformation_errors(self)
    def test_incremental_transformation_logic(self)
    def test_data_quality_validation(self)
    def test_performance_with_large_datasets(self)
    def test_memory_usage_optimization(self)
```

### **Phase 3: Integration and End-to-End Testing (Week 3) - MUST COMPLETE BEFORE PRODUCTION**

#### **3.1 End-to-End Pipeline Tests**
```python
# tests/integration/test_full_pipeline.py
class TestFullPipeline:
    def test_complete_etl_flow_success(self)
    def test_complete_etl_flow_with_errors(self)
    def test_incremental_processing_flow(self)
    def test_error_recovery_flow(self)
    def test_large_dataset_processing(self)
    def test_schema_change_handling(self)
    def test_concurrent_table_processing(self)
    def test_memory_usage_under_load(self)
    def test_database_connection_limits(self)
    def test_rollback_mechanisms(self)
    
    # IDEMPOTENCY AND INCREMENTAL LOAD INTEGRATION TESTS - CRITICAL
    def test_end_to_end_idempotent_processing(self)
    def test_end_to_end_incremental_processing_with_changes(self)
    def test_end_to_end_schema_change_handling(self)
    def test_end_to_end_force_full_behavior(self)
    def test_end_to_end_data_consistency_across_runs(self)
    def test_end_to_end_incremental_flags_behavior(self)
```

#### **3.2 Configuration Integration Tests**
```python
# tests/integration/test_configuration.py
class TestConfigurationIntegration:
    def test_environment_variable_loading_validation(self)
    def test_config_file_loading_validation(self)
    def test_database_connection_validation(self)
    def test_table_configuration_validation(self)
    def test_error_handling_invalid_configs(self)
    def test_config_reload_mechanisms(self)
```

#### **3.3 Idempotency Integration Tests (NEW SECTION) - CRITICAL**
```python
# tests/integration/test_idempotency_integration.py
class TestIdempotencyIntegration:
    """CRITICAL: End-to-end integration tests for idempotency and incremental behavior"""
    
    def test_full_pipeline_idempotency_with_real_data(self):
        """Test complete pipeline idempotency with real database data"""
        # 1. Set up test databases with real data
        # 2. Run complete pipeline with force_full=False
        # 3. Record data state and processing metrics
        # 4. Run pipeline again with force_full=False
        # 5. Verify data state is identical
        # 6. Verify no duplicate processing occurs
        
    def test_full_pipeline_incremental_with_real_changes(self):
        """Test complete pipeline incremental processing with real source changes"""
        # 1. Set up test databases with real data
        # 2. Run initial pipeline with force_full=False
        # 3. Make real changes to source database
        # 4. Run pipeline again with force_full=False
        # 5. Verify only changed data is processed
        # 6. Verify data consistency is maintained
        
    def test_full_pipeline_schema_change_with_real_schema(self):
        """Test complete pipeline schema change handling with real schema modifications"""
        # 1. Set up test databases with real schema
        # 2. Run initial pipeline with force_full=False
        # 3. Modify real table schema in source
        # 4. Run pipeline again with force_full=False
        # 5. Verify schema change is detected
        # 6. Verify pipeline reverts to full load
        # 7. Verify new schema is properly applied
        
    def test_full_pipeline_force_full_override_behavior(self):
        """Test complete pipeline force_full override behavior"""
        # 1. Set up test databases with real data
        # 2. Run pipeline with force_full=True
        # 3. Verify full processing occurs
        # 4. Run pipeline with force_full=False
        # 5. Verify incremental processing resumes
        # 6. Verify data consistency is maintained
        
    def test_full_pipeline_incremental_flags_monitoring(self):
        """Test complete pipeline incremental flags monitoring and behavior"""
        # 1. Set up test databases with real data
        # 2. Run pipeline and monitor all incremental flags
        # 3. Verify flag behavior matches expected patterns
        # 4. Verify flags are properly logged
        # 5. Verify flag combinations work correctly
        
    def test_full_pipeline_data_consistency_validation(self):
        """Test complete pipeline data consistency across multiple runs"""
        # 1. Set up test databases with real data
        # 2. Run pipeline multiple times with different configurations
        # 3. Verify data integrity is maintained
        # 4. Verify referential integrity is preserved
        # 5. Verify business rules are maintained
        # 6. Verify no data corruption occurs
```

### **Phase 4: Performance and Load Testing (Week 4) - MUST COMPLETE BEFORE PRODUCTION**

#### **4.1 Performance Tests**
```python
# tests/performance/test_performance.py
class TestPerformance:
    def test_small_dataset_performance_benchmarks(self)
    def test_medium_dataset_performance_benchmarks(self)
    def test_large_dataset_performance_benchmarks(self)
    def test_memory_usage_optimization(self)
    def test_connection_pool_performance(self)
    def test_parallel_processing_efficiency(self)
    def test_database_throughput_limits(self)
```

#### **4.2 Load Tests**
```python
# tests/performance/test_load.py
class TestLoad:
    def test_concurrent_table_processing_limits(self)
    def test_database_connection_limits(self)
    def test_memory_usage_under_load(self)
    def test_error_rate_under_load(self)
    def test_system_stability_under_stress(self)
    def test_recovery_time_after_failures(self)
```

## PRODUCTION READINESS CHECKLIST

### **✅ COMPLETED REQUIREMENTS**
- [x] Basic CLI functionality tested (74-88% coverage)
- [x] Configuration system tested (80-100% coverage)
- [x] Core database connections tested (100% coverage)
- [x] Logging system tested (100% coverage)
- [x] Schema discovery tested (82% coverage)

### **❌ CRITICAL REQUIREMENTS (MUST COMPLETE BEFORE PRODUCTION)**

#### **Week 1 Critical Path**
- [ ] **Postgres Loader**: 0% → 90% coverage (CRITICAL)
- [ ] **Pipeline Orchestrator**: 32% → 90% coverage (CRITICAL)
- [ ] **Table Processor**: 16% → 90% coverage (CRITICAL)
- [ ] **Idempotency and Incremental Load Tests**: 0% → 90% coverage (CRITICAL)

#### **Week 2 Critical Path**
- [ ] **MySQL Replicator**: 11% → 85% coverage (HIGH)
- [ ] **RawToPublicTransformer**: 15% → 85% coverage (HIGH)

#### **Week 3 Critical Path**
- [ ] **End-to-End Pipeline Tests**: Create comprehensive integration tests
- [ ] **Configuration Integration Tests**: Validate real-world scenarios
- [ ] **Idempotency Integration Tests**: Validate end-to-end idempotency behavior (CRITICAL)

#### **Week 4 Critical Path**
- [ ] **Performance Tests**: Validate production performance requirements
- [ ] **Load Tests**: Ensure system stability under load

### **🎯 COVERAGE TARGETS FOR PRODUCTION**

| Component | Current | Target | Priority |
|-----------|---------|--------|----------|
| **Overall Coverage** | 41% | **> 85%** | CRITICAL |
| **Postgres Loader** | 0% | **> 90%** | CRITICAL |
| **Pipeline Orchestrator** | 32% | **> 90%** | CRITICAL |
| **Table Processor** | 16% | **> 90%** | CRITICAL |
| **Idempotency & Incremental Tests** | 0% | **> 90%** | CRITICAL |
| **MySQL Replicator** | 11% | **> 85%** | HIGH |
| **RawToPublicTransformer** | 15% | **> 85%** | HIGH |
| **Integration Tests** | 0% | **> 80%** | HIGH |
| **Idempotency Integration Tests** | 0% | **> 90%** | CRITICAL |
| **Performance Tests** | 0% | **> 90%** | MEDIUM |

## Test Infrastructure Setup

### **1. Test Database Configuration**

#### **MySQL Test Database**
```yaml
# tests/fixtures/mysql_test_config.yaml
source:
  host: localhost
  port: 3306
  database: opendental_test
  user: test_user
  password: test_pass
```

#### **PostgreSQL Test Database**
```yaml
# tests/fixtures/postgres_test_config.yaml
analytics:
  host: localhost
  port: 5432
  database: opendental_analytics_test
  user: test_user
  password: test_pass
```

### **2. Test Data Fixtures**

#### **Sample Data Generation**
```python
# tests/fixtures/data_generator.py
class TestDataGenerator:
    def generate_patient_data(self, count: int) -> pd.DataFrame
    def generate_appointment_data(self, count: int) -> pd.DataFrame
    def generate_procedure_data(self, count: int) -> pd.DataFrame
    def generate_invalid_data(self) -> pd.DataFrame
    def generate_large_dataset(self, table: str, count: int) -> pd.DataFrame
```

#### **Database Setup/Teardown**
```python
# tests/conftest.py
@pytest.fixture(scope="session")
def test_databases():
    """Set up test databases for the test session."""
    # Create test databases
    # Load sample data
    # Return connection configs
    yield configs
    # Clean up test databases
```

### **3. Mock Strategies**

#### **Database Mocks**
```python
# tests/mocks/database_mocks.py
class MockDatabaseEngine:
    def __init__(self, mock_data: Dict)
    def connect(self) -> MockConnection
    def dispose(self)
```

#### **File System Mocks**
```python
# tests/mocks/filesystem_mocks.py
class MockFileSystem:
    def create_temp_config(self, content: str) -> str
    def cleanup_temp_files(self)
```

## Test Execution Strategy

### **1. Test Categories and Execution**

#### **Fast Tests (Unit Tests)**
```bash
# Run unit tests only (fast)
pytest tests/ -m "not integration and not slow" -v

# Run specific component tests
pytest tests/orchestration/ -v
pytest tests/loaders/ -v
pytest tests/transformers/ -v
```

#### **Integration Tests**
```bash
# Run integration tests (requires test databases)
pytest tests/integration/ -m integration -v

# Run with specific database setup
pytest tests/integration/ --db-setup=test -v
```

#### **Performance Tests**
```bash
# Run performance tests (slow)
pytest tests/performance/ -m slow -v

# Run with performance profiling
pytest tests/performance/ --profile -v
```

### **2. Continuous Integration**

#### **GitHub Actions Workflow**
```yaml
# .github/workflows/test.yml
name: ETL Pipeline Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: root
          MYSQL_DATABASE: opendental_test
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: opendental_analytics_test
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.11
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest pytest-cov
      - name: Run tests
        run: pytest tests/ -v --cov=etl_pipeline --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v1
```

### **3. Test Reporting**

#### **Coverage Reports**
```bash
# Generate coverage report
pytest tests/ --cov=etl_pipeline --cov-report=html --cov-report=term-missing

# Coverage targets
# - Overall coverage: > 85%
# - Critical components: > 90%
# - New code: > 95%
```

#### **Test Results Dashboard**
```python
# tests/reporting/test_reporter.py
class TestReporter:
    def generate_test_summary(self)
    def generate_coverage_report(self)
    def generate_performance_report(self)
    def generate_test_trends(self)
```

## Quality Assurance

### **1. Test Quality Standards**

#### **Code Quality**
- All tests must have clear, descriptive names
- Tests should be independent and isolated
- Use appropriate assertions and error messages
- Follow DRY principles for test setup

#### **Coverage Requirements**
- **Unit Tests**: > 90% line coverage
- **Integration Tests**: > 80% integration coverage
- **Critical Paths**: 100% coverage
- **Error Handling**: 100% coverage

#### **Performance Requirements**
- **Unit Tests**: < 1 second per test
- **Integration Tests**: < 30 seconds per test
- **End-to-End Tests**: < 5 minutes per test
- **Performance Tests**: < 10 minutes per test

### **2. Test Maintenance**

#### **Test Documentation**
- Document test data requirements
- Document test environment setup
- Document test execution procedures
- Document troubleshooting guides

#### **Test Review Process**
- Code review for all new tests
- Regular test suite reviews
- Performance regression testing
- Coverage trend analysis

## Implementation Timeline

### **Week 1: Critical Component Testing (MUST COMPLETE)**
- [ ] Create PostgresLoader tests (0% → 90% coverage)
- [ ] Create PipelineOrchestrator tests (32% → 90% coverage)
- [ ] Create TableProcessor tests (16% → 90% coverage)
- [ ] Set up test infrastructure

### **Week 2: Data Movement Testing (MUST COMPLETE)**
- [ ] Expand MySQL Replicator tests (11% → 85% coverage)
- [ ] Refactor RawToPublicTransformer tests (15% → 85% coverage)
- [ ] Create test data fixtures

### **Week 3: Integration Testing (MUST COMPLETE)**
- [ ] Create end-to-end pipeline tests
- [ ] Create configuration integration tests
- [ ] Set up test databases

### **Week 4: Performance and Validation (MUST COMPLETE)**
- [ ] Create performance tests
- [ ] Create load tests
- [ ] Set up CI/CD pipeline
- [ ] Validate test coverage

## Success Metrics

### **Coverage Targets**
- **Overall Code Coverage**: > 85% (CRITICAL)
- **Critical Component Coverage**: > 90% (CRITICAL)
- **Integration Test Coverage**: > 80% (HIGH)
- **Error Path Coverage**: 100% (CRITICAL)

### **Performance Targets**
- **Test Execution Time**: < 10 minutes for full suite
- **Test Reliability**: > 99% pass rate
- **Test Maintainability**: < 10% test code per production code

### **Quality Targets**
- **Test Documentation**: 100% documented
- **Test Independence**: 100% isolated tests
- **Test Clarity**: Clear, descriptive test names
- **Test Maintainability**: DRY principles followed

## PRODUCTION DEPLOYMENT GATE

### **🚫 NO PRODUCTION DEPLOYMENT UNTIL:**
1. **Overall coverage reaches > 85%**
2. **All critical components have > 90% coverage**
3. **All integration tests pass**
4. **All performance tests meet requirements**
5. **All load tests validate system stability**
6. **All idempotency and incremental load tests pass (CRITICAL)**
7. **All end-to-end idempotency integration tests pass (CRITICAL)**

### **✅ PRODUCTION READINESS CHECKLIST:**
- [ ] Postgres Loader: > 90% coverage
- [ ] Pipeline Orchestrator: > 90% coverage  
- [ ] Table Processor: > 90% coverage
- [ ] Idempotency & Incremental Load Tests: > 90% coverage (CRITICAL)
- [ ] MySQL Replicator: > 85% coverage
- [ ] RawToPublicTransformer: > 85% coverage
- [ ] End-to-End Tests: > 80% coverage
- [ ] Idempotency Integration Tests: > 90% coverage (CRITICAL)
- [ ] Performance Tests: All pass
- [ ] Load Tests: All pass
- [ ] Documentation: Complete
- [ ] CI/CD Pipeline: Configured and passing

This testing plan provides a comprehensive strategy for ensuring the reliability, performance, and
 maintainability of the refactored ETL pipeline through thorough testing at all levels.
  **Full coverage must be achieved before any production deployment with real data.** 