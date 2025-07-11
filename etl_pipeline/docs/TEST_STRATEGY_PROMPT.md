# ETL Pipeline Test Strategy Prompt for LLM

## Role and Context
You are a senior test engineer working on a dental clinic ETL pipeline that processes OpenDental data. The pipeline uses MySQL replication, PostgreSQL analytics, and intelligent orchestration with priority-based processing.

## 🎯 **THREE-TIER TESTING STRATEGY**

### **Modern Architecture Overview**
The ETL pipeline uses a **modern static configuration approach** with **provider pattern dependency injection**:
- **Static Configuration**: All configuration from `tables.yml` - no database queries during ETL
- **Provider Pattern**: Dependency injection for configuration sources (FileConfigProvider/DictConfigProvider)
- **Settings Injection**: Environment-agnostic connections using Settings objects
- **5-10x Performance**: Faster than dynamic schema discovery approaches
- **Explicit Environment Separation**: Clear production/test environment handling with FAIL FAST
- **Test Isolation**: Complete configuration isolation between production and test environments
- **Type Safety**: Enums for database types and schema names prevent runtime errors
- **FAIL FAST**: System fails immediately if ETL_ENVIRONMENT not explicitly set

### **3-File Testing Pattern**
We use a **three-tier testing approach** with **three test files per component** for maximum confidence and coverage:

#### **1. Unit Tests** (`test_[component]_unit.py`)
- **Purpose**: Pure unit tests with comprehensive mocking and provider pattern
- **Scope**: Fast execution, isolated component behavior, no real connections
- **Coverage**: Core logic and edge cases for all methods
- **Execution**: < 1 second per component
- **Provider Usage**: DictConfigProvider for injected test configuration
- **Settings Injection**: Uses Settings with injected provider for environment-agnostic testing
- **FAIL FAST Testing**: Validates system fails when ETL_ENVIRONMENT not set
- **Markers**: `@pytest.mark.unit`

#### **2. Comprehensive Tests** (`test_[component].py`) 
- **Purpose**: Full functionality testing with mocked dependencies and provider pattern
- **Scope**: Complete component behavior, error handling, all methods
- **Coverage**: 90%+ target coverage (main test suite)
- **Execution**: < 5 seconds per component
- **Provider Usage**: DictConfigProvider for comprehensive test scenarios
- **Settings Injection**: Uses Settings with injected provider for complete test scenarios
- **Environment Separation**: Tests production/test environment handling
- **Markers**: `@pytest.mark.unit` (default)

#### **3. Integration Tests** (`test_[component]_integration.py`)
- **Purpose**: Real database integration with test environment and provider pattern
- **Scope**: Safety, error handling, actual data flow, all methods
- **Coverage**: Integration scenarios and edge cases
- **Execution**: < 10 seconds per component
- **Environment**: Real test databases, no production connections with FileConfigProvider
- **Order Markers**: Proper test execution order for data flow validation
- **Provider Usage**: FileConfigProvider with real test configuration files (.env_test)
- **Settings Injection**: Uses Settings with FileConfigProvider for real test environment
- **Environment Separation**: Uses .env_test file for test environment isolation
- **Markers**: `@pytest.mark.integration`

#### **4. End-to-End Tests** (`test_e2e_[component].py`)
- **Purpose**: Production connection testing with test data and provider pattern
- **Scope**: Full pipeline validation with real production connections
- **Coverage**: Complete workflow validation
- **Execution**: < 30 seconds per component
- **Environment**: Production connections with test data only using FileConfigProvider
- **Provider Usage**: FileConfigProvider with production configuration files (.env_production)
- **Settings Injection**: Uses Settings with FileConfigProvider for production environment
- **Environment Separation**: Uses .env_production file for production environment
- **Markers**: `@pytest.mark.e2e`

### **Directory Structure**
```
tests/
├── unit/                                # Pure unit tests only (mocked)
├── comprehensive/                       # Comprehensive tests (mocked dependencies)
├── integration/                         # Real test database integration tests (ORDER MARKERS)
└── e2e/                                # Production connection E2E tests
```

## 📚 **SUPPORTING DOCUMENTATION REFERENCES**

### **Core Architecture Documents**
- **[Testing Plan](TESTING_PLAN.md)**: Comprehensive testing checklist with provider pattern
- **[Connection Architecture](connection_architecture.md)**: Complete connection handling system with provider pattern
- **[Data Flow Diagram](DATA_FLOW_DIAGRAM.md)**: Complete pipeline ecosystem with 150 methods across 13 components
- **[ETL Pipeline Methods](etl_pipeline_methods.md)**: Complete method documentation for all components
- **[Fixture Usage Guide](FIXTURE_USAGE_GUIDE.md)**: Comprehensive fixture system for testing

**IMPORTANT**: Retrieve detailed information from these documents when building tests. They contain:
- Complete method lists and signatures
- Architecture patterns and best practices
- Connection management strategies with provider pattern
- Data flow diagrams and dependencies
- Fixture definitions and usage patterns

## 🛠️ **TEST INFRASTRUCTURE CONFIGURATION**

### **pytest.ini Configuration**
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
    provider_pattern: Tests using provider pattern dependency injection
    settings_injection: Tests using Settings injection for environment-agnostic connections

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

### **Provider Pattern Testing Examples**
```python
# Unit Test with DictConfigProvider
def test_database_config_with_provider():
    """Test database configuration using provider pattern."""
    from etl_pipeline.config.providers import DictConfigProvider
    from etl_pipeline.config.settings import Settings, DatabaseType
    
    test_provider = DictConfigProvider(
        env={
            'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
        }
    )
    
    settings = Settings(environment='test', provider=test_provider)
    config = settings.get_database_config(DatabaseType.SOURCE)  # Using enums
    assert config['host'] == 'test-host'

# Integration Test with FileConfigProvider
def test_real_config_loading():
    """Test loading configuration from real files."""
    from etl_pipeline.config.settings import Settings, DatabaseType
    
    settings = Settings(environment='test')  # Uses FileConfigProvider with .env_test
    assert settings.validate_configs() is True
    
    config = settings.get_database_config(DatabaseType.SOURCE)
    assert config['host'] is not None

# Settings Injection for Environment-Agnostic Connections
def test_connection_with_settings_injection():
    """Test environment-agnostic connections using Settings injection."""
    from etl_pipeline.config import get_settings
    from etl_pipeline.core import ConnectionFactory
    
    settings = get_settings()  # Environment determined by ETL_ENVIRONMENT
    source_engine = ConnectionFactory.get_source_connection(settings)
    analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
```

## 🎯 **TESTING PHILOSOPHY**

### **Key Testing Principles**
1. **Mock at highest abstraction level possible**
2. **Test behavior, not implementation**
3. **Focus on data integrity and pipeline robustness**
4. **Verify error handling and recovery**
5. **Ensure idempotent operations**
6. **Use three-tier approach for comprehensive coverage**
7. **Follow explicit environment separation with FAIL FAST**
8. **Test all methods from methods documentation**
9. **Use order markers for integration test execution**
10. **Leverage static configuration for predictable testing**
11. **Use provider pattern for dependency injection**
12. **Ensure test isolation with DictConfigProvider for unit tests**
13. **Use enums for type safety in database operations**
14. **Leverage FileConfigProvider for integration/E2E tests**
15. **Maintain configuration isolation between test types**
16. **Use Settings injection for environment-agnostic connections**
17. **Validate FAIL FAST behavior when ETL_ENVIRONMENT not set**

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
3. Follow connection architecture patterns with provider pattern
4. Implement proper environment separation with FAIL FAST
5. Test all methods listed above
6. Include error handling scenarios
7. Use realistic test data
8. Follow naming conventions
9. Use Settings injection for environment-agnostic connections
10. Use DictConfigProvider for unit/comprehensive tests, FileConfigProvider for integration/E2E tests

**Supporting Documentation:**
- Testing Plan: [TESTING_PLAN.md]
- Connection Architecture: [connection_architecture.md]
- Data Flow Diagram: [DATA_FLOW_DIAGRAM.md]
- ETL Pipeline Methods: [etl_pipeline_methods.md]
- Fixture Usage Guide: [FIXTURE_USAGE_GUIDE.md]

Please create the test file with comprehensive coverage of all methods.
```

## 🛠️ **DEBUGGING STRATEGY**

### **When Tests Fail**
1. **Check Mock Setup**: Ensure mocks accept **kwargs for database connection methods
2. **Verify Import Paths**: Patch where modules are imported, not where defined
3. **Debug Incrementally**: Fix one error at a time, add logging
4. **Match Call Signatures**: Use actual function signatures in assertions
5. **Handle Complex Dependencies**: Mock entire classes for complex initialization
6. **Check Environment Separation**: Verify test vs production connections
7. **Validate Fixture Usage**: Ensure correct fixture dependencies
8. **Check Provider Pattern**: Verify DictConfigProvider vs FileConfigProvider usage
9. **Validate Settings Injection**: Ensure environment-agnostic connections
10. **Test FAIL FAST**: Verify system fails when ETL_ENVIRONMENT not set

### **Common Patterns**
```python
# CORRECT: Mock with **kwargs for DB connections
mock_func.side_effect = lambda **kwargs: logger.debug(f"Called with: {kwargs}")

# CORRECT: Patch where imported
@patch('etl_pipeline.orchestration.ConnectionFactory')

# CORRECT: Full class mock for complex components
@patch('etl_pipeline.core.mysql_replicator.SimpleMySQLReplicator')

# CORRECT: Environment separation with provider pattern
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings, DatabaseType

test_provider = DictConfigProvider(env={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'})
test_settings = Settings(environment='test', provider=test_provider)
test_engine = ConnectionFactory.get_source_connection(test_settings)

# CORRECT: Settings injection for environment-agnostic connections
from etl_pipeline.config import get_settings
settings = get_settings()  # Environment determined by ETL_ENVIRONMENT
engine = ConnectionFactory.get_analytics_raw_connection(settings)
```

## 📋 **VERIFICATION CHECKLIST**

### **Before Implementing Tests**
- [ ] Review component's actual functionality (not mocked behavior)
- [ ] Understand data flow and dependencies from [DATA_FLOW_DIAGRAM.md]
- [ ] Identify critical error scenarios
- [ ] Plan mock strategy at appropriate abstraction level
- [ ] Plan three test files: Unit → Comprehensive → Integration → E2E
- [ ] Verify pytest.ini configuration supports test types
- [ ] Check conftest.py has required fixtures
- [ ] Review connection architecture patterns from [connection_architecture.md]
- [ ] Follow naming conventions from [etl_pipeline_methods.md]
- [ ] Plan order markers for integration tests
- [ ] Understand static configuration approach
- [ ] Plan provider pattern usage (DictConfigProvider for unit/comprehensive, FileConfigProvider for integration/E2E)
- [ ] Identify enum usage for type safety (DatabaseType, PostgresSchema)
- [ ] Plan test isolation strategy with provider pattern
- [ ] Plan Settings injection for environment-agnostic connections
- [ ] Plan FAIL FAST testing for ETL_ENVIRONMENT validation

### **After Writing Tests**
- [ ] Verify tests cover actual business logic
- [ ] Ensure error scenarios are tested
- [ ] Check that mocks don't hide real issues
- [ ] Validate performance requirements are met
- [ ] Verify all three test files work together
- [ ] Test custom markers work correctly
- [ ] Verify fixtures are properly shared
- [ ] Confirm environment separation is maintained
- [ ] Validate method coverage (all 150 methods)
- [ ] Verify order markers work correctly
- [ ] Test static configuration integration
- [ ] Verify provider pattern usage
- [ ] Test enum usage for type safety
- [ ] Verify test isolation with provider pattern
- [ ] Confirm no environment pollution in tests
- [ ] Validate dependency injection works correctly
- [ ] Test Settings injection for environment-agnostic connections
- [ ] Verify FAIL FAST behavior when ETL_ENVIRONMENT not set

## 🎯 **SUCCESS METRICS**

### **Coverage Targets**
- **Overall Code Coverage**: > 90% (CRITICAL)
- **Critical Component Coverage**: > 95% (CRITICAL)
- **Integration Test Coverage**: > 90% (HIGH)
- **E2E Test Coverage**: > 90% (HIGH)
- **Error Path Coverage**: 100% (CRITICAL)
- **Method Coverage**: 100% (CRITICAL)

### **Performance Targets**
- **Test Execution Time**: < 15 minutes for full suite
- **Test Reliability**: > 99% pass rate
- **Test Maintainability**: < 10% test code per production code

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
9. **Scripts** (27 methods)
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
8. **Follow environment separation**: Clear production/test boundaries with FAIL FAST
9. **Test all methods**: 100% method coverage from methods documentation (150 methods)
10. **Use supporting documentation**: Reference all architecture documents
11. **Use order markers**: Proper integration test execution order
12. **Leverage static configuration**: Predictable test behavior
13. **Use provider pattern**: DictConfigProvider for unit/comprehensive, FileConfigProvider for integration/E2E
14. **Use Settings injection**: Environment-agnostic connections using Settings objects
15. **Test FAIL FAST**: Validate system fails when ETL_ENVIRONMENT not explicitly set

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
- **Test all methods**: Ensure 100% method coverage (150 methods)
- **Use order markers**: Ensure proper integration test execution
- **Leverage static configuration**: Use predictable test behavior
- **Use provider pattern**: DictConfigProvider for testing, FileConfigProvider for real connections
- **Use Settings injection**: Environment-agnostic connections with Settings objects
- **Test FAIL FAST**: Ensure system fails when ETL_ENVIRONMENT not set

This test strategy provides a complete framework for building efficient, maintainable tests for the ETL pipeline with full coverage of all 150 methods across the three-tier testing approach, using provider pattern dependency injection and Settings injection for environment-agnostic connections.