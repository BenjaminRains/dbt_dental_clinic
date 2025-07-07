# ETL Pipeline Test Strategy Prompt for LLM

## Role and Context
You are a senior test engineer working on a dental clinic ETL pipeline that processes OpenDental data. The pipeline uses MySQL replication, PostgreSQL analytics, and intelligent orchestration with priority-based processing.

## Architecture Overview
- **Source**: OpenDental MySQL (read-only)
- **Replication**: Local MySQL (opendental_replication) 
- **Analytics**: PostgreSQL (opendental_analytics with raw schema)
- **Tech Stack**: Python, SQLAlchemy, MariaDB v11.6, PostgreSQL
- **Processing**: Priority-based (critical → important → audit → reference)

## 🎯 **THREE-TIER TESTING STRATEGY**

### **3-File Testing Pattern** ✅ **VALIDATED & SUCCESSFUL**

We use a **three-tier testing approach** with **three test files per component** for maximum confidence and coverage:

#### **1. Unit Tests** (`test_[component]_unit.py`)
- **Purpose**: Pure unit tests with comprehensive mocking
- **Scope**: Fast execution, isolated component behavior, no real connections
- **Coverage**: Core logic and edge cases for all methods
- **Execution**: < 1 second per component
- **Environment**: No production connections, full mocking
- **Markers**: `@pytest.mark.unit`

#### **2. Comprehensive Tests** (`test_[component].py`) 
- **Purpose**: Full functionality testing with mocked dependencies
- **Scope**: Complete component behavior, error handling, all methods
- **Coverage**: 90%+ target coverage (main test suite)
- **Execution**: < 5 seconds per component
- **Environment**: Mocked dependencies, no real connections
- **Markers**: `@pytest.mark.unit` (default)

#### **3. Integration Tests** (`test_[component]_integration.py`)
- **Purpose**: Real database integration with test environment
- **Scope**: Safety, error handling, actual data flow, all methods
- **Coverage**: Integration scenarios and edge cases
- **Execution**: < 10 seconds per component
- **Environment**: Real test databases, no production connections
- **Markers**: `@pytest.mark.integration`

#### **4. End-to-End Tests** (`test_e2e_[component].py`)
- **Purpose**: Production connection testing with test data
- **Scope**: Full pipeline validation with real production connections
- **Coverage**: Complete workflow validation
- **Execution**: < 30 seconds per component
- **Environment**: Production connections with test data only
- **Markers**: `@pytest.mark.e2e`

### **Updated Directory Structure** ✅ **COMPREHENSIVE**
```
tests/
├── unit/                                # Pure unit tests only (mocked)
│   ├── core/
│   ├── config/
│   ├── loaders/
│   ├── orchestration/
│   ├── monitoring/
│   └── scripts/
├── comprehensive/                       # Comprehensive tests (mocked dependencies)
│   ├── core/
│   ├── config/
│   ├── loaders/
│   ├── orchestration/
│   ├── monitoring/
│   └── scripts/
├── integration/                         # Real test database integration tests
│   ├── core/
│   ├── config/
│   ├── loaders/
│   ├── orchestration/
│   ├── monitoring/
│   ├── scripts/
│   └── end_to_end/
└── e2e/                                # Production connection E2E tests
    ├── core/
    ├── loaders/
    ├── orchestration/
    └── full_pipeline/
```

## 📚 **SUPPORTING DOCUMENTATION REFERENCES**

### **Core Architecture Documents**
- **[Connection Architecture](connection_architecture.md)**: Complete connection handling system with explicit environment separation
- **[Data Flow Diagram](DATA_FLOW_DIAGRAM.md)**: Complete pipeline ecosystem with 150 methods across 13 components
- **[ETL Naming Conventions](etl_naming_conventions.md)**: Clear data flow and naming strategy
- **[Fixture Usage Guide](FIXTURE_USAGE_GUIDE.md)**: Comprehensive fixture system for testing

### **Testing Plan**
- **[Testing Plan](TESTING_PLAN.md)**: Comprehensive testing checklist with 127 methods to test

## 🛠️ **TEST INFRASTRUCTURE CONFIGURATION**

### **pytest.ini Configuration** 📋

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Markers for Three-Tier Testing Approach
markers =
    unit: Unit tests with mocking (fast execution)
    integration: Integration tests with test databases (real database)
    e2e: End-to-end tests with production connections (test data)
    slow: Tests that take longer to run
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

## 🎯 **COMPREHENSIVE METHOD TESTING**

### **Complete Method Coverage (127 Methods)**

Based on the [Testing Plan](TESTING_PLAN.md), we need to test **127 methods** across all components:

#### **Phase 1: Core Component Testing (Week 1)**
- **Logging Module**: 15 methods (0% → 95% coverage)
- **Configuration Module**: 22 methods (0% → 95% coverage)
- **Core Connections**: 25 methods (0% → 95% coverage)
- **Core Postgres Schema**: 10 methods (0% → 95% coverage)

#### **Phase 2: Data Movement Testing (Week 2)**
- **MySQL Replicator**: 10 methods (91% → 95% coverage) ✅ **COMPLETED**
- **PostgresLoader**: 10 methods (0% → 95% coverage)
- **Unified Metrics**: 15 methods (0% → 95% coverage)

#### **Phase 3: Orchestration Testing (Week 3)**
- **Pipeline Orchestrator**: 7 methods (32% → 95% coverage)
- **Priority Processor**: 5 methods (0% → 95% coverage)
- **Table Processor**: 8 methods (16% → 95% coverage)
- **Scripts**: 35 methods (0% → 90% coverage)

#### **Phase 4: E2E Testing (Week 4)**
- **Integration Tests**: 0% → 90% coverage
- **E2E Tests**: 0% → 90% coverage

### **Method-Level Testing Requirements**

Each method must be tested in all three tiers:

```python
# Example: Testing PostgresLoader.load_table() method

# Unit Test (test_postgres_loader_unit.py)
@pytest.mark.unit
def test_load_table_method_basic_functionality(mock_analytics_engine, sample_patient_data):
    """Test PostgresLoader.load_table() basic functionality with mocking."""
    # Test core logic without real connections
    pass

# Comprehensive Test (test_postgres_loader.py)
@pytest.mark.unit  # Default marker
def test_load_table_method_full_functionality(mock_settings, large_test_dataset):
    """Test PostgresLoader.load_table() complete functionality with mocked dependencies."""
    # Test full method behavior with all dependencies mocked
    pass

# Integration Test (test_postgres_loader_integration.py)
@pytest.mark.integration
def test_load_table_method_integration(test_analytics_engine, sample_patient_data):
    """Test PostgresLoader.load_table() with real test database."""
    # Test method with real test database connections
    pass
```

## 🔗 **CONNECTION ARCHITECTURE INTEGRATION**

### **Explicit Environment Separation**

Based on [Connection Architecture](connection_architecture.md):

```python
# Production connections (for E2E tests only)
source_engine = ConnectionFactory.get_opendental_source_connection()
replication_engine = ConnectionFactory.get_mysql_replication_connection()
analytics_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Test connections (for integration tests)
test_source_engine = ConnectionFactory.get_opendental_source_test_connection()
test_replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
test_analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection()

# Schema-specific connections
test_raw_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
test_staging_engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
test_intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()
test_marts_engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
```

### **Database Type Enums**

```python
from etl_pipeline.config.settings import DatabaseType, PostgresSchema

# Using enums for type safety
source_config = settings.get_database_config(DatabaseType.SOURCE)
analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
```

## 📊 **DATA FLOW INTEGRATION**

### **Complete Pipeline Ecosystem**

Based on [Data Flow Diagram](DATA_FLOW_DIAGRAM.md):

#### **Nightly ETL Components (123 methods)**
- **PipelineOrchestrator**: 6 methods
- **TableProcessor**: 9 methods (CORE ETL ENGINE)
- **PriorityProcessor**: 5 methods
- **Settings**: 20 methods
- **ConfigReader**: 12 methods
- **ConnectionFactory**: 25 methods
- **PostgresSchema**: 10 methods
- **SimpleMySQLReplicator**: 10 methods
- **PostgresLoader**: 11 methods
- **UnifiedMetricsCollector**: 15 methods

#### **Management Scripts (27 methods)**
- **OpenDentalSchemaAnalyzer**: 12 methods
- **Test Database Setup**: 8 functions
- **PipelineConfigManager**: 15 methods

### **Data Flow Testing**

```python
# Test complete data flow
def test_complete_data_flow():
    """Test complete data flow: Source → Replication → Analytics."""
    # Phase 1: Extract (MySQL → MySQL)
    # Phase 2: Load (MySQL → PostgreSQL)
    # Phase 3: Transform (PostgreSQL → dbt models)
    pass
```

## 🏷️ **NAMING CONVENTIONS INTEGRATION**

### **Consistent Naming Strategy**

Based on [ETL Naming Conventions](etl_naming_conventions.md):

```python
# Environment Variables
OPENDENTAL_SOURCE_HOST=client-opendental-server
MYSQL_REPLICATION_HOST=localhost
POSTGRES_ANALYTICS_HOST=localhost

# Connection Factory Methods
get_opendental_source_connection()           # Source MySQL
get_mysql_replication_connection()           # Replication MySQL
get_opendental_analytics_raw_connection()    # Raw schema
get_opendental_analytics_staging_connection() # Staging schema

# Database Names
source_db = "opendental"
replication_db = "opendental_replication"
analytics_db = "opendental_analytics"
```

### **Schema Structure**

```sql
-- PostgreSQL Database: opendental_analytics
raw.patient              -- ETL pipeline output
staging.stg_opendental__patient  -- dbt staging
intermediate.int_patient_demographics  -- dbt intermediate
marts.dim_patient        -- dbt marts
```

## 🧪 **FIXTURE SYSTEM INTEGRATION**

### **Modular Fixture Architecture**

Based on [Fixture Usage Guide](FIXTURE_USAGE_GUIDE.md):

```python
# Environment and Configuration Fixtures
def test_environment_setup(test_env_vars, test_settings):
    """Test that environment is properly configured."""
    assert test_env_vars['ETL_ENVIRONMENT'] == 'test'
    assert test_settings.environment == 'test'

# Database Connection Fixtures
def test_source_connection(test_source_engine):
    """Test source database connection."""
    with test_source_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

# Test Data Fixtures
def test_patient_data_structure(standard_patient_test_data):
    """Test patient data structure."""
    assert len(standard_patient_test_data) == 3

# Integration Test Fixtures
def test_pipeline_with_populated_databases(populated_test_databases):
    """Test pipeline with pre-populated test databases."""
    source_count = populated_test_databases.get_patient_count('source')
    assert source_count > 0
```

## 🚀 **TEST BUILDING PROMPT TEMPLATE**

### **For Building Individual Test Files**

When building tests, use this template:

```
I need to build [UNIT/COMPREHENSIVE/INTEGRATION/E2E] tests for [COMPONENT_NAME] component.

**Component Details:**
- File: [file_path]
- Methods to test: [list_of_methods]
- Target coverage: [X%]
- Test type: [unit/comprehensive/integration/e2e]

**Requirements:**
1. Follow the three-tier testing approach
2. Use appropriate fixtures from the fixture system
3. Follow connection architecture patterns
4. Implement proper environment separation
5. Test all methods listed above
6. Include error handling scenarios
7. Use realistic test data
8. Follow naming conventions

**Supporting Documentation:**
- Testing Plan: [TESTING_PLAN.md]
- Connection Architecture: [connection_architecture.md]
- Data Flow Diagram: [DATA_FLOW_DIAGRAM.md]
- ETL Naming Conventions: [etl_naming_conventions.md]
- Fixture Usage Guide: [FIXTURE_USAGE_GUIDE.md]

Please create the test file with comprehensive coverage of all methods.
```

## 🎯 **TESTING PHILOSOPHY**

### **Test Types and Coverage Targets**
- **Unit Tests**: >95% for critical components
- **Comprehensive Tests**: >95% for full functionality
- **Integration Tests**: >90% for full pipeline flows  
- **E2E Tests**: >90% for production connection validation
- **Method Coverage**: 100% (all 127 methods)

### **Key Testing Principles**
1. **Mock at highest abstraction level possible**
2. **Test behavior, not implementation**
3. **Focus on data integrity and pipeline robustness**
4. **Verify error handling and recovery**
5. **Ensure idempotent operations**
6. **Use three-tier approach for comprehensive coverage**
7. **Follow explicit environment separation**
8. **Test all methods from methods documentation**

## 🛠️ **DEBUGGING STRATEGY**

### **When Tests Fail**
1. **Check Mock Setup**: Ensure mocks accept **kwargs for database connection methods
2. **Verify Import Paths**: Patch where modules are imported, not where defined
3. **Debug Incrementally**: Fix one error at a time, add logging
4. **Match Call Signatures**: Use actual function signatures in assertions
5. **Handle Complex Dependencies**: Mock entire classes for complex initialization
6. **Check Environment Separation**: Verify test vs production connections
7. **Validate Fixture Usage**: Ensure correct fixture dependencies

### **Common Patterns**
```python
# CORRECT: Mock with **kwargs for DB connections
mock_func.side_effect = lambda **kwargs: logger.debug(f"Called with: {kwargs}")

# CORRECT: Patch where imported
@patch('etl_pipeline.orchestration.ConnectionFactory')

# CORRECT: Full class mock for complex components
@patch('etl_pipeline.core.mysql_replicator.SimpleMySQLReplicator')

# CORRECT: Environment separation
test_engine = ConnectionFactory.get_opendental_source_test_connection()
```

## 📋 **VERIFICATION CHECKLIST**

### **Before Implementing Tests (Three-Tier Approach)**
- [ ] Review component's actual functionality (not mocked behavior)
- [ ] Understand data flow and dependencies from [DATA_FLOW_DIAGRAM.md]
- [ ] Identify critical error scenarios
- [ ] Plan mock strategy at appropriate abstraction level
- [ ] Plan three test files: Unit → Comprehensive → Integration → E2E
- [ ] Verify pytest.ini configuration supports test types
- [ ] Check conftest.py has required fixtures
- [ ] Review connection architecture patterns from [connection_architecture.md]
- [ ] Follow naming conventions from [etl_naming_conventions.md]

### **After Writing Tests (Three-Tier Approach)**
- [ ] Verify tests cover actual business logic
- [ ] Ensure error scenarios are tested
- [ ] Check that mocks don't hide real issues
- [ ] Validate performance requirements are met
- [ ] Verify all three test files work together
- [ ] Test custom markers work correctly
- [ ] Verify fixtures are properly shared
- [ ] Confirm environment separation is maintained
- [ ] Validate method coverage (all 127 methods)

### **Before Deployment**
- [ ] Overall coverage >90%
- [ ] Critical components >95% coverage (across all three test files)
- [ ] All integration tests pass
- [ ] All E2E tests pass
- [ ] All methods from methods documentation tested
- [ ] pytest.ini configuration validated
- [ ] conftest.py fixtures tested
- [ ] Environment separation verified

## 🏆 **SUCCESS METRICS**

### **Coverage Targets (Three-Tier Approach)**
- **Logging Module**: >95% (across unit + comprehensive + integration)
- **Configuration Module**: >95% (across unit + comprehensive + integration)
- **Core Connections**: >95% (across unit + comprehensive + integration)
- **Core Postgres Schema**: >95% (across unit + comprehensive + integration)
- **MySQL Replicator**: >95% (across unit + comprehensive + integration) ✅ **COMPLETED**
- **PostgresLoader**: >95% (across unit + comprehensive + integration)
- **Unified Metrics**: >95% (across unit + comprehensive + integration)
- **Pipeline Orchestrator**: >95% (across unit + comprehensive + integration)
- **Priority Processor**: >95% (across unit + comprehensive + integration)
- **Table Processor**: >95% (across unit + comprehensive + integration)
- **Scripts**: >90% (across unit + comprehensive + integration)
- **Integration flows**: >90% (end-to-end scenarios)
- **E2E flows**: >90% (production connection validation)

### **Quality Metrics**
- **Test reliability**: >99% pass rate
- **Test independence**: No shared state between tests
- **Test maintainability**: Clear, documented test scenarios
- **Method coverage**: 100% (all 127 methods)
- **Three-tier approach**: All three test files working together
- **Infrastructure**: pytest.ini and conftest.py properly configured
- **Environment separation**: Clear production/test boundaries

## 🚀 **IMMEDIATE NEXT STEPS**

### **Start with Logging Module (15 methods)**
1. Create `tests/unit/config/test_logging_unit.py`
2. Create `tests/comprehensive/config/test_logging.py`
3. Create `tests/integration/config/test_logging_integration.py`
4. Test all 15 logging methods with three-tier approach

### **Progress Through Components**
1. **Configuration Module** (22 methods)
2. **Core Connections** (25 methods)
3. **Core Postgres Schema** (10 methods)
4. **PostgresLoader** (10 methods)
5. **Unified Metrics** (15 methods)
6. **Pipeline Orchestrator** (7 methods)
7. **Priority Processor** (5 methods)
8. **Table Processor** (8 methods)
9. **Scripts** (35 methods)
10. **E2E Tests** (production connections)

## 📝 **FINAL NOTES**

### **Key Reminders**
1. **Test real behavior**: Don't mock away the logic you're testing
2. **Focus on data integrity**: ETL correctness is critical
3. **Verify idempotency**: Multiple runs must be safe
4. **Test error recovery**: Failures should be graceful
5. **Document test scenarios**: Clear test descriptions and purposes
6. **Use three-tier approach**: Three test files per component for maximum confidence
7. **Leverage infrastructure**: Use pytest.ini and conftest.py effectively
8. **Follow environment separation**: Clear production/test boundaries
9. **Test all methods**: 100% method coverage from methods documentation
10. **Use supporting documentation**: Reference all architecture documents

### **When in Doubt**
- **Add debug logging**: Use logger.debug() liberally
- **Test incrementally**: One scenario at a time
- **Verify with real data**: Use realistic test fixtures
- **Check existing patterns**: Follow established testing approaches
- **Follow three-tier pattern**: Unit → Comprehensive → Integration → E2E
- **Use shared fixtures**: Leverage conftest.py for consistency
- **Check configuration**: Verify pytest.ini settings
- **Reference documentation**: Use all supporting architecture documents
- **Maintain environment separation**: Never mix production/test connections
- **Test all methods**: Ensure 100% method coverage

This comprehensive test strategy provides a complete framework for building efficient, maintainable tests for the ETL pipeline with full coverage of all 127 methods across the three-tier testing approach.