# ETL Pipeline Testing Plan

## Overview

This testing plan outlines the comprehensive testing strategy for the ETL pipeline after refactoring. The plan addresses current testing gaps, improves test quality, and ensures robust validation of all pipeline components.

## Current Testing State Analysis

### **✅ EXISTING TEST COVERAGE (Well Tested)**

| Component | Test Files | Status | Coverage |
|-----------|------------|--------|----------|
| **CLI System** | `tests/cli/test_cli.py` | ✅ Comprehensive | High |
| **Configuration** | `tests/config/`, `tests/test_config.py` | ✅ Good | High |
| **Core Connections** | `tests/core/test_connections.py` | ✅ Good | High |
| **Schema Discovery** | `tests/core/test_schema_discovery.py` | ✅ Good | Medium |
| **MySQL Replicator** | `tests/core/test_mysql_replicator.py` | ✅ Good | Medium |
| **Postgres Schema** | `tests/core/test_postgres_schema.py` | ✅ Good | Medium |
| **Metrics** | `tests/core/test_metrics.py` | ✅ Basic | Low |

### **⚠️ PARTIAL TEST COVERAGE (Needs Improvement)**

| Component | Test Files | Status | Issues |
|-----------|------------|--------|--------|
| **Pipeline Orchestration** | `tests/core/test_pipeline.py` | ⚠️ Partial | Tests deprecated ETLPipeline class |
| **Table Processor** | None | ❌ Missing | No dedicated tests |
| **Postgres Loader** | `tests/test_config_integration.py` | ⚠️ Partial | Only integration test |
| **Raw to Public Transformer** | `tests/transformers/test_raw_to_public.py` | ⚠️ Over-mocked | Tests mocks, not real logic |

### **❌ MISSING TEST COVERAGE**

| Component | Status | Priority |
|-----------|--------|----------|
| **PipelineOrchestrator** | ❌ No tests | High |
| **TableProcessor** | ❌ No tests | High |
| **PriorityProcessor** | ❌ No tests | Medium |
| **Loaders Directory** | ❌ No unit tests | High |
| **Monitoring/Metrics** | ❌ No integration tests | Medium |
| **Utils Directory** | ❌ No tests | Low |
| **MCP Directory** | ❌ No tests | Low |

## Testing Strategy

### **1. Test Categories**

#### **Unit Tests**
- Test individual components in isolation
- Mock external dependencies (databases, file systems)
- Fast execution (< 1 second per test)
- High coverage of edge cases and error conditions

#### **Integration Tests**
- Test component interactions
- Use test databases with real connections
- Validate data flow between components
- Test configuration and environment setup

#### **End-to-End Tests**
- Test complete ETL pipeline flow
- Use sample data sets
- Validate final output quality
- Test error recovery and rollback

#### **Performance Tests**
- Test with large datasets
- Measure processing times and resource usage
- Identify bottlenecks and optimization opportunities
- Validate scalability

### **2. Test Data Strategy**

#### **Test Databases**
- **MySQL Test DB**: Sample OpenDental data (100-1000 records per table)
- **PostgreSQL Test DB**: Analytics database with test schemas
- **Isolated Environments**: Separate test databases for each test run

#### **Sample Data Sets**
- **Small Dataset**: 10-100 records (fast unit tests)
- **Medium Dataset**: 1000-10000 records (integration tests)
- **Large Dataset**: 100000+ records (performance tests)

#### **Data Quality Tests**
- **Valid Data**: Normal operation scenarios
- **Invalid Data**: Error handling and validation
- **Edge Cases**: Null values, special characters, data types
- **Schema Changes**: Table structure modifications

## Testing Implementation Plan

### **Phase 1: Core Component Testing (Week 1-2)**

#### **1.1 PipelineOrchestrator Tests**
```python
# tests/orchestration/test_pipeline_orchestrator.py
class TestPipelineOrchestrator:
    def test_initialization(self)
    def test_connection_initialization(self)
    def test_table_processing_by_priority(self)
    def test_error_handling_and_recovery(self)
    def test_metrics_collection(self)
    def test_cleanup_and_disposal(self)
```

#### **1.2 TableProcessor Tests**
```python
# tests/orchestration/test_table_processor.py
class TestTableProcessor:
    def test_table_processing_flow(self)
    def test_incremental_processing(self)
    def test_full_processing(self)
    def test_error_handling(self)
    def test_metrics_recording(self)
    def test_parallel_processing(self)
```

#### **1.3 PriorityProcessor Tests**
```python
# tests/orchestration/test_priority_processor.py
class TestPriorityProcessor:
    def test_priority_batch_processing(self)
    def test_priority_assignment(self)
    def test_batch_size_management(self)
    def test_error_propagation(self)
```

### **Phase 2: Data Movement Component Testing (Week 2-3)**

#### **2.1 PostgresLoader Tests**
```python
# tests/loaders/test_postgres_loader.py
class TestPostgresLoader:
    def test_table_loading(self)
    def test_chunked_loading(self)
    def test_schema_creation(self)
    def test_data_validation(self)
    def test_error_handling(self)
    def test_incremental_loading(self)
    def test_large_table_handling(self)
```

#### **2.2 MySQL Replicator Tests (Refactor Existing)**
```python
# tests/core/test_mysql_replicator.py (refactor)
class TestMySQLReplicator:
    def test_exact_replica_creation(self)
    def test_table_data_copying(self)
    def test_replica_verification(self)
    def test_schema_synchronization(self)
    def test_error_recovery(self)
```

#### **2.3 RawToPublicTransformer Tests (Refactor Existing)**
```python
# tests/transformers/test_raw_to_public.py (refactor)
class TestRawToPublicTransformer:
    def test_data_transformation_logic(self)
    def test_schema_conversion(self)
    def test_data_type_conversion(self)
    def test_column_transformations(self)
    def test_table_specific_transformations(self)
    def test_error_handling(self)
    def test_incremental_transformation(self)
```

### **Phase 3: Integration Testing (Week 3-4)**

#### **3.1 End-to-End Pipeline Tests**
```python
# tests/integration/test_full_pipeline.py
class TestFullPipeline:
    def test_complete_etl_flow(self)
    def test_incremental_processing_flow(self)
    def test_error_recovery_flow(self)
    def test_large_dataset_processing(self)
    def test_schema_change_handling(self)
```

#### **3.2 Configuration Integration Tests**
```python
# tests/integration/test_configuration.py
class TestConfigurationIntegration:
    def test_environment_variable_loading(self)
    def test_config_file_loading(self)
    def test_database_connection_validation(self)
    def test_table_configuration_validation(self)
```

#### **3.3 CLI Integration Tests**
```python
# tests/integration/test_cli_integration.py
class TestCLIIntegration:
    def test_cli_with_real_config(self)
    def test_cli_error_handling(self)
    def test_cli_output_formatting(self)
    def test_cli_subprocess_execution(self)
```

### **Phase 4: Performance and Load Testing (Week 4)**

#### **4.1 Performance Tests**
```python
# tests/performance/test_performance.py
class TestPerformance:
    def test_small_dataset_performance(self)
    def test_medium_dataset_performance(self)
    def test_large_dataset_performance(self)
    def test_memory_usage(self)
    def test_connection_pool_performance(self)
```

#### **4.2 Load Tests**
```python
# tests/performance/test_load.py
class TestLoad:
    def test_concurrent_table_processing(self)
    def test_database_connection_limits(self)
    def test_memory_usage_under_load(self)
    def test_error_rate_under_load(self)
```

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
          python-version: 3.9
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
# - Overall coverage: > 80%
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

### **Week 1: Core Component Testing**
- [ ] Create PipelineOrchestrator tests
- [ ] Create TableProcessor tests
- [ ] Create PriorityProcessor tests
- [ ] Set up test infrastructure

### **Week 2: Data Movement Testing**
- [ ] Refactor PostgresLoader tests
- [ ] Refactor MySQL Replicator tests
- [ ] Refactor RawToPublicTransformer tests
- [ ] Create test data fixtures

### **Week 3: Integration Testing**
- [ ] Create end-to-end pipeline tests
- [ ] Create configuration integration tests
- [ ] Create CLI integration tests
- [ ] Set up test databases

### **Week 4: Performance and Validation**
- [ ] Create performance tests
- [ ] Create load tests
- [ ] Set up CI/CD pipeline
- [ ] Validate test coverage

## Success Metrics

### **Coverage Targets**
- **Overall Code Coverage**: > 85%
- **Critical Component Coverage**: > 95%
- **Integration Test Coverage**: > 80%
- **Error Path Coverage**: 100%

### **Performance Targets**
- **Test Execution Time**: < 10 minutes for full suite
- **Test Reliability**: > 99% pass rate
- **Test Maintainability**: < 10% test code per production code

### **Quality Targets**
- **Test Documentation**: 100% documented
- **Test Independence**: 100% isolated tests
- **Test Clarity**: Clear, descriptive test names
- **Test Maintainability**: DRY principles followed

This testing plan provides a comprehensive strategy for ensuring the reliability, performance, and maintainability of the refactored ETL pipeline through thorough testing at all levels. 