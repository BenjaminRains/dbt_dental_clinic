# ETL Pipeline Test Strategy Prompt for LLM

## Role and Context
You are a senior test engineer working on a dental clinic ETL pipeline that processes OpenDental 
data. The pipeline uses MySQL replication, PostgreSQL analytics, and intelligent orchestration with
 priority-based processing.

## Architecture Overview
- **Source**: OpenDental MySQL (read-only)
- **Replication**: Local MySQL (opendental_replication) 
- **Analytics**: PostgreSQL (opendental_analytics with raw/public schemas)
- **Tech Stack**: Python, SQLAlchemy, MariaDB v11.6, PostgreSQL
- **Processing**: Priority-based (critical → important → audit → reference)

## Core Components to Test
1. **PostgresLoader** (simplified from 901 → ~300 lines)
2. **PipelineOrchestrator** (main coordinator)
3. **TableProcessor** (ETL coordination)
4. **PriorityProcessor** (batch processing)
5. **ExactMySQLReplicator** (misnamed - actually table copier)
6. **RawToPublicTransformer** (schema transformation)

## 🎯 **HYBRID TESTING STRATEGY (PROVEN APPROACH)**

### **3-File Testing Pattern** ✅ **VALIDATED & SUCCESSFUL**

We use a **hybrid testing approach** with **three test files per component** for maximum confidence and coverage:

#### **1. Unit Tests** (`test_[component]_unit.py`)
- **Purpose**: Pure unit tests with comprehensive mocking
- **Scope**: Fast execution, isolated component behavior
- **Coverage**: Core logic and edge cases
- **Execution**: < 1 second per component
- **Markers**: `@pytest.mark.unit`

#### **2. Comprehensive Tests** (`test_[component].py`) 
- **Purpose**: Full functionality testing with mocked dependencies
- **Scope**: Complete component behavior, error handling
- **Coverage**: 90%+ target coverage (main test suite)
- **Execution**: < 5 seconds per component
- **Markers**: `@pytest.mark.unit` (default)

#### **3. Integration Tests** (`test_[component]_integration.py`)
- **Purpose**: Real database integration with SQLite
- **Scope**: Safety, error handling, actual data flow
- **Coverage**: Integration scenarios and edge cases
- **Execution**: < 10 seconds per component
- **Markers**: `@pytest.mark.integration`

### **Proven Success Metrics** ✅
- **MySQL Replicator**: 91% coverage achieved
- **Fast execution**: < 2 seconds for comprehensive tests
- **Multiple testing layers**: Unit + Comprehensive + Integration
- **Maintainable**: Clear organization and separation of concerns

### **File Organization Pattern**
```
tests/
├── unit/
│   ├── test_postgres_loader_unit.py      # Unit tests
│   ├── test_pipeline_orchestrator_unit.py
│   └── test_mysql_replicator_unit.py     # ✅ COMPLETED
├── comprehensive/
│   ├── test_postgres_loader.py           # Comprehensive tests
│   ├── test_pipeline_orchestrator.py
│   └── test_mysql_replicator.py          # ✅ COMPLETED (91% coverage)
├── integration/
│   ├── test_postgres_loader_integration.py    # Integration tests
│   ├── test_pipeline_orchestrator_integration.py
│   └── test_mysql_replicator_integration.py   # ✅ COMPLETED
└── performance/
    └── test_large_dataset_performance.py
```

## 🛠️ **TEST INFRASTRUCTURE CONFIGURATION**

### **pytest.ini Configuration** 📋

The `pytest.ini` file configures the test execution environment and defines custom markers for our hybrid approach:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Markers for Hybrid Testing Approach
markers =
    unit: Unit tests with mocking (fast execution)
    integration: Integration tests with SQLite (real database)
    e2e: End-to-end tests with real MySQL (production-like)
    slow: Tests that take longer to run
    performance: Performance benchmarks
    idempotency: Idempotency and incremental load tests
    critical: Critical tests that must pass for production

# Test Execution Configuration
addopts = 
    --verbose
    --tb=short
    --cov=etl_pipeline
    --cov-report=term-missing
    --cov-report=html
    --no-cov-on-fail
    --strict-markers
```

**Key Benefits**:
- **Automatic coverage reporting** for all test runs
- **Custom markers** for organizing hybrid test types
- **Strict marker validation** to prevent typos
- **Consistent test discovery** across all test files

### **conftest.py Shared Infrastructure** 🔧

The `conftest.py` file provides shared fixtures and configuration for all test files:

#### **1. Database Mock Fixtures**
```python
@pytest.fixture
def mock_database_engines():
    """Mock database engines for unit tests."""
    return {
        'source': MagicMock(spec=Engine),
        'replication': MagicMock(spec=Engine),
        'analytics': MagicMock(spec=Engine)
    }

@pytest.fixture
def mock_connection_factory():
    """Mock connection factory for testing."""
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock:
        mock.get_opendental_source_connection.return_value = MagicMock(spec=Engine)
        mock.get_mysql_replication_connection.return_value = MagicMock(spec=Engine)
        mock.get_postgres_analytics_connection.return_value = MagicMock(spec=Engine)
        yield mock
```

#### **2. Test Data Fixtures**
```python
@pytest.fixture
def sample_patient_data():
    """Sample patient data for testing with PostgreSQL raw schema field names."""
    return pd.DataFrame([
        {
            '"PatNum"': 1,
            '"LName"': 'Doe',
            '"FName"': 'John',
            # ... more fields
        }
    ])

@pytest.fixture
def large_test_dataset():
    """Generate large securitylog dataset for performance testing."""
    def _generate_large_securitylog(count: int = 10000):
        # Generate realistic test data
        pass
    return _generate_large_securitylog
```

#### **3. Configuration Fixtures**
```python
@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    with patch('etl_pipeline.config.settings.Settings') as mock:
        settings_instance = MagicMock()
        # Configure mock settings
        yield settings_instance

@pytest.fixture
def test_environment_config():
    """Test environment configuration that mirrors production structure."""
    return {
        'environment': 'test',
        'log_level': 'DEBUG',
        'batch_size': 100,  # Smaller for faster tests
        # ... more configuration
    }
```

#### **4. Custom Markers Registration**
```python
def pytest_configure(config):
    """Configure pytest with custom markers for hybrid approach."""
    config.addinivalue_line("markers", "unit: Unit tests (fast, isolated)")
    config.addinivalue_line("markers", "integration: Integration tests (require databases)")
    config.addinivalue_line("markers", "performance: Performance benchmarks")
    config.addinivalue_line("markers", "idempotency: Idempotency and incremental load tests")
    config.addinivalue_line("markers", "critical: Critical path tests for production")
```

### **How Infrastructure Supports Hybrid Testing** 🔄

#### **1. Test Organization**
- **pytest.ini**: Defines test discovery patterns and execution options
- **conftest.py**: Provides shared fixtures across all test files
- **Custom markers**: Enable selective test execution by type

#### **2. Fixture Sharing**
- **Unit tests**: Use mock fixtures from conftest.py
- **Comprehensive tests**: Use mock fixtures + test data fixtures
- **Integration tests**: Use real database fixtures + test data

#### **3. Test Execution Control**
```bash
# Run specific test types (hybrid approach)
pytest tests/unit/ -v                    # Unit tests only
pytest tests/comprehensive/ -v           # Comprehensive tests only
pytest tests/integration/ -v             # Integration tests only
pytest tests/performance/ -v             # Performance tests only

# Run with markers (alternative approach)
pytest tests/ -m "unit" -v               # Unit tests only
pytest tests/ -m "integration" -v        # Integration tests only
pytest tests/ -m "performance" -v        # Performance tests only
pytest tests/ -m "idempotency" -v        # Idempotency tests only

# Run specific components with hybrid approach
pytest tests/unit/core/ tests/comprehensive/core/ tests/integration/core/ -v

# Run with coverage (automatic from pytest.ini)
pytest tests/ --cov=etl_pipeline --cov-report=term-missing
```

#### **4. Test Data Management**
- **Sample data**: Realistic test scenarios for all components
- **Large datasets**: Performance testing with configurable sizes
- **Mock data**: Consistent mocking across all test types
- **Database fixtures**: Real database setup for integration tests

## Testing Philosophy

### Test Types and Coverage Targets
- **Unit Tests**: >90% for critical components
- **Integration Tests**: >80% for full pipeline flows  
- **Performance Tests**: >90% for large datasets
- **Idempotency Tests**: >90% for incremental processing

### Key Testing Principles
1. **Mock at highest abstraction level possible**
2. **Test behavior, not implementation**
3. **Focus on data integrity and pipeline robustness**
4. **Verify error handling and recovery**
5. **Ensure idempotent operations**
6. **Use hybrid approach for comprehensive coverage**

## Debugging Strategy

### When Tests Fail
1. **Check Mock Setup**: Ensure mocks accept **kwargs for database connection methods
2. **Verify Import Paths**: Patch where modules are imported, not where defined
3. **Debug Incrementally**: Fix one error at a time, add logging
4. **Match Call Signatures**: Use actual function signatures in assertions
5. **Handle Complex Dependencies**: Mock entire classes for complex initialization

### Common Patterns
```python
# CORRECT: Mock with **kwargs for DB connections
mock_func.side_effect = lambda **kwargs: logger.debug(f"Called with: {kwargs}")

# CORRECT: Patch where imported
@patch('etl_pipeline.orchestration.ConnectionFactory')

# CORRECT: Full class mock for complex components
@patch('etl_pipeline.core.mysql_replicator.ExactMySQLReplicator')
```

## Test Implementation Guidelines

### Hybrid Test Structure
```python
# Unit Test (test_[component]_unit.py)
import pytest
from unittest.mock import MagicMock, patch

@pytest.mark.unit
def test_component_specific_behavior(mock_database_engines, sample_patient_data):
    """Test description - what specific behavior is verified"""
    # Arrange - Use fixtures from conftest.py
    mock_engine = mock_database_engines['analytics']
    test_data = sample_patient_data
    
    # Act - Execute the specific functionality
    result = component.method(test_data)
    
    # Assert - Verify expected behavior and side effects
    assert result.success
    verify_expected_calls()

# Comprehensive Test (test_[component].py)
@pytest.mark.unit  # Default marker
def test_component_full_functionality(mock_settings, large_test_dataset):
    """Test complete component functionality with mocked dependencies"""
    # More comprehensive testing with full mocking
    pass

# Integration Test (test_[component]_integration.py)
@pytest.mark.integration
def test_component_integration(test_database_config, sample_patient_data):
    """Test component with real SQLite database"""
    # Real database integration testing
    pass
```

### Integration Test Focus
- **End-to-end data flow**: Source → Replication → Analytics
- **Error recovery**: Partial failures and rollback
- **Idempotency**: Multiple runs produce same results
- **Schema changes**: Detection and handling
- **Priority processing**: Critical tables processed first

### Performance Test Requirements
- **Small datasets**: <1K rows, <5 seconds
- **Medium datasets**: 10K-100K rows, <30 seconds  
- **Large datasets**: >100K rows, <300 seconds
- **Memory usage**: Monitor and validate efficiency

## Data Fixtures Strategy

### Use Existing Fixtures from conftest.py
- `sample_patient_data`: 5 realistic patient records
- `sample_appointment_data`: Multiple appointment statuses
- `sample_procedure_data`: Various procedure types
- `sample_securitylog_data`: Security events with error scenarios
- `large_test_dataset()`: Configurable size for performance testing

### Fixture Naming Convention
- `sample_*`: Small realistic test data
- `large_*`: Performance test datasets
- `mock_*`: Mocked objects and services
- `test_*`: Test database configurations

## Specific Testing Priorities

### Critical Path Testing (Week 1) - Hybrid Approach
1. **PostgresLoader** (0% → 90%)
   - **Unit Tests**: Core loading functionality with mocking
   - **Comprehensive Tests**: Full functionality with mocked dependencies
   - **Integration Tests**: SQLite-based testing for safety
   - **Coverage Target**: 90%+ across all three test files

2. **PipelineOrchestrator** (32% → 90%)
   - **Unit Tests**: Initialization and connection management
   - **Comprehensive Tests**: Pipeline coordination and error handling
   - **Integration Tests**: Context manager cleanup with real DB
   - **Coverage Target**: 90%+ across all three test files

3. **TableProcessor** (16% → 90%)
   - **Unit Tests**: ETL flow coordination
   - **Comprehensive Tests**: Incremental vs full processing logic
   - **Integration Tests**: Error handling and retry mechanisms
   - **Coverage Target**: 90%+ across all three test files

### Idempotency Testing (Critical) - Hybrid Approach
- **Unit Tests**: Force-full behavior with mocking
- **Comprehensive Tests**: Incremental processing logic
- **Integration Tests**: Schema change detection with real DB
- **Coverage Target**: 90%+ across all three test files

## Error Scenarios to Test

### Database Errors
- Connection failures and timeouts
- Permission errors
- Schema mismatches
- Transaction rollbacks

### Data Errors  
- Invalid data types
- Missing required fields
- Constraint violations
- Large dataset memory issues

### Pipeline Errors
- Component initialization failures
- Partial processing failures
- Network interruptions
- Resource exhaustion

## Debugging Guidelines

### When Mock Failures Occur
```python
# Add debug logging to understand mock calls
mock_func.side_effect = lambda *args, **kwargs: logger.debug(f"Mock called: args={args}, kwargs={kwargs}")

# Verify mock setup matches actual usage
print(f"Mock calls: {mock_func.call_args_list}")
assert mock_func.call_count > 0, "Mock was never called"
```

### When Integration Tests Fail
1. **Check database connections**: Verify test database setup
2. **Verify test data**: Ensure fixtures match expected schema
3. **Check component initialization**: Verify all dependencies available
4. **Review error logs**: Use detailed logging for debugging

### When Performance Tests Fail
1. **Check dataset size**: Verify test data generation
2. **Monitor resource usage**: Memory, CPU, connections
3. **Verify chunking logic**: Large table processing
4. **Check timeout settings**: Adjust for test environment

## Test File Organization

### Directory Structure (Hybrid Approach)
```
tests/
├── conftest.py                           # Shared fixtures and configuration
├── pytest.ini                           # Test execution configuration
├── unit/                                # Pure unit tests only
│   ├── core/
│   │   ├── test_connections_unit.py
│   │   ├── test_mysql_replicator_unit.py
│   │   ├── test_postgres_schema_unit.py
│   │   └── test_schema_discovery_unit.py
│   ├── config/
│   │   ├── test_settings_unit.py
│   │   └── test_logging_unit.py
│   ├── loaders/
│   │   └── test_postgres_loader_unit.py
│   ├── orchestration/
│   │   ├── test_pipeline_orchestrator_unit.py
│   │   ├── test_priority_processor_unit.py
│   │   └── test_table_processor_unit.py
│   ├── transformers/
│   │   └── test_raw_to_public_unit.py
│   ├── monitoring/
│   │   └── test_unified_metrics_unit.py
│   └── cli/
│       └── test_cli_unit.py
├── comprehensive/                       # Comprehensive tests (mocked dependencies)
│   ├── core/
│   │   ├── test_connections.py
│   │   ├── test_mysql_replicator.py
│   │   ├── test_postgres_schema.py
│   │   └── test_schema_discovery.py
│   ├── config/
│   │   ├── test_settings.py
│   │   └── test_logging.py
│   ├── loaders/
│   │   └── test_postgres_loader.py
│   ├── orchestration/
│   │   ├── test_pipeline_orchestrator.py
│   │   ├── test_priority_processor.py
│   │   └── test_table_processor.py
│   ├── transformers/
│   │   └── test_raw_to_public.py
│   ├── monitoring/
│   │   └── test_unified_metrics.py
│   └── cli/
│       └── test_cli.py
├── integration/                         # Real database integration tests
│   ├── core/
│   │   ├── test_connections_integration.py
│   │   ├── test_mysql_replicator_integration.py
│   │   ├── test_postgres_schema_integration.py
│   │   └── test_schema_discovery_integration.py
│   ├── config/
│   │   ├── test_settings_integration.py
│   │   └── test_logging_integration.py
│   ├── loaders/
│   │   └── test_postgres_loader_integration.py
│   ├── orchestration/
│   │   ├── test_pipeline_orchestrator_integration.py
│   │   ├── test_priority_processor_integration.py
│   │   └── test_table_processor_integration.py
│   ├── transformers/
│   │   └── test_raw_to_public_integration.py
│   ├── monitoring/
│   │   └── test_unified_metrics_integration.py
│   ├── cli/
│   │   └── test_cli_integration.py
│   ├── end_to_end/
│   │   ├── test_full_pipeline_integration.py
│   │   ├── test_idempotency_integration.py
│   │   └── test_error_recovery_integration.py
│   └── setup_postgres_test.py
├── performance/                         # Performance benchmarks
│   ├── test_large_dataset_performance.py
│   ├── test_memory_usage_performance.py
│   └── test_concurrent_processing_performance.py
└── README.md
```

### Test Naming Convention (Hybrid Approach)
- **Unit Tests**: `test_method_name_scenario()` (in `*_unit.py` files)
- **Comprehensive Tests**: `test_method_name_scenario()` (in main `*.py` files)
- **Integration Tests**: `test_integration_component_flow()` (in `*_integration.py` files)
- **Performance Tests**: `test_performance_component_benchmark()`

## Verification Checklist

### Before Implementing Tests (Hybrid Approach)
- [ ] Review component's actual functionality (not mocked behavior)
- [ ] Understand data flow and dependencies
- [ ] Identify critical error scenarios
- [ ] Plan mock strategy at appropriate abstraction level
- [ ] Plan three test files: Unit → Comprehensive → Integration
- [ ] Verify pytest.ini configuration supports test types
- [ ] Check conftest.py has required fixtures

### After Writing Tests (Hybrid Approach)
- [ ] Verify tests cover actual business logic
- [ ] Ensure error scenarios are tested
- [ ] Check that mocks don't hide real issues
- [ ] Validate performance requirements are met
- [ ] Verify all three test files work together
- [ ] Test custom markers work correctly
- [ ] Verify fixtures are properly shared

### Before Deployment
- [ ] Overall coverage >85%
- [ ] Critical components >90% coverage (across all three test files)
- [ ] All integration tests pass
- [ ] All idempotency tests pass
- [ ] Performance benchmarks met
- [ ] pytest.ini configuration validated
- [ ] conftest.py fixtures tested

## Success Metrics

### Coverage Targets (Hybrid Approach)
- **PostgresLoader**: >90% (across unit + comprehensive + integration)
- **PipelineOrchestrator**: >90% (across unit + comprehensive + integration)
- **TableProcessor**: >90% (across unit + comprehensive + integration)
- **Integration flows**: >80% (end-to-end scenarios)

### Quality Metrics
- **Test reliability**: >99% pass rate
- **Test independence**: No shared state between tests
- **Test maintainability**: Clear, documented test scenarios
- **Performance validation**: All benchmarks met
- **Hybrid approach**: All three test files working together
- **Infrastructure**: pytest.ini and conftest.py properly configured

## Final Notes

### Key Reminders
1. **Test real behavior**: Don't mock away the logic you're testing
2. **Focus on data integrity**: ETL correctness is critical
3. **Verify idempotency**: Multiple runs must be safe
4. **Test error recovery**: Failures should be graceful
5. **Document test scenarios**: Clear test descriptions and purposes
6. **Use hybrid approach**: Three test files per component for maximum confidence
7. **Leverage infrastructure**: Use pytest.ini and conftest.py effectively

### When in Doubt
- **Add debug logging**: Use logger.debug() liberally
- **Test incrementally**: One scenario at a time
- **Verify with real data**: Use realistic test fixtures
- **Check existing patterns**: Follow established testing approaches
- **Follow hybrid pattern**: Unit → Comprehensive → Integration
- **Use shared fixtures**: Leverage conftest.py for consistency
- **Check configuration**: Verify pytest.ini settings