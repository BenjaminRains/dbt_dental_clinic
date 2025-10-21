# ETL Pipeline Pytest Debugging Guide

> 💡 **See Also**: [FIXTURE_USAGE_GUIDE.md](FIXTURE_USAGE_GUIDE.md) - Comprehensive guide to using the ETL pipeline's fixture system, with examples for all fixture types (environment, configuration, database, test data, CLI, etc.).

## Quick Reference - Common Issues

| Error Pattern | Section | Quick Fix |
|---------------|---------|-----------|
| Tests hanging with ThreadPoolExecutor | [1](#1-threadpoolexecutor-mocking) | Patch `as_completed` at module level |
| "Mock object has no attribute 'url'" | [4.1](#41-sqlalchemy-engine-mocking) | Mock engine.url attribute |
| "cannot access local variable" | [7](#7-variable-scope-issues) | Define mocks early in function |
| F-string syntax error with backslash | [24](#24-f-string-syntax-errors) | Use intermediate variables |
| Test expects different behavior than code | [31](#31-test-expectations-vs-real-behavior) | Read implementation first |
| Context manager errors | [2](#2-context-manager-protocol) | Add `__enter__` and `__exit__` |
| Unexpected keyword argument errors | [12](#12-handling-keyword-arguments) | Use `**kwargs` in side effects |
| Multi-scenario test failures | [10.1](#101-multi-scenario-test-problems) | Split into separate test methods |
| Exit code 2 instead of 0 (file not found) | [10.1.1](#1011-fix-file-dependencies) | Use temporary files |
| Wrong error messages in sequence tests | [10.1.2](#1012-split-multi-scenario-tests) | Reset mocks between scenarios |
| CLI output validation failures | [12.1](#121-cli-output-validation-patterns) | Match actual CLI output format |
| Configuration file not found errors | [12.2](#122-configuration-file-testing-patterns) | Create temporary test files |
| Complex configuration mocking | [12.3](#123-real-configuration-vs-mock-configuration) | Use real config with test data |
| KeyError in SQLAlchemy execute mocks | [3.4](#34-sqlalchemy-execute-parameter-access) | Check args[1] before kwargs |
| Integration tests using mock settings | [14](#14-unit-vs-integration-test-settings-pattern) | Use test_settings_with_file_provider for integration tests |

---

## Core Debugging Principles

### 🎯 **Golden Rules**
1. **Patch where used, not where defined** - Always patch at import location
2. **Test real behavior, not assumptions** - Read implementation before writing tests  
3. **Mock at highest abstraction level possible** - Don't over-mock
4. **Add debug logging** - Use `logger.debug()` to understand execution flow
5. **Fix one issue at a time** - Don't try to fix everything simultaneously
6. **Use appropriate fixtures** - Refer to [FIXTURE_USAGE_GUIDE.md](FIXTURE_USAGE_GUIDE.md) for fixture selection and usage patterns

### 🔧 **Standard Debugging Workflow**
```python
# 1. Add debug logging
logger.debug(f"Mock calls: {mock_func.call_args_list}")

# 2. Check mock setup
mock_func.side_effect = lambda **kwargs: logger.debug(f"Called with: {kwargs}")

# 3. Verify patch location
@patch('module_under_test.imported_function')  # Patch where imported

# 4. Run test incrementally
pytest tests/specific_test.py::test_method -v -s

# 5. Document solution
```

---

## Critical Fixes by Category

## 1. ThreadPoolExecutor Mocking

**Problem**: Tests hang during parallel processing
```python
# ❌ WRONG - as_completed is not a method of executor
mock_executor.as_completed.return_value = mock_futures

# ✅ CORRECT - patch as_completed at module level
with patch('etl_pipeline.orchestration.priority_processor.as_completed') as mock_as_completed:
    mock_as_completed.return_value = mock_futures
```

**Complete Solution**:
```python
with patch('module.ThreadPoolExecutor') as mock_executor_class, \
     patch('module.as_completed') as mock_as_completed:
    
    # Create mock executor with context manager support
    mock_executor = MagicMock()
    mock_executor.__enter__ = MagicMock(return_value=mock_executor)
    mock_executor.__exit__ = MagicMock(return_value=None)
    mock_executor_class.return_value = mock_executor
```

## 2. Context Manager Protocol

**Problem**: "AttributeError: __enter__" when using `with` statements
```python
# ❌ WRONG - missing context manager methods
mock_engine = MagicMock(spec=Engine)
with mock_engine.connect() as conn:  # FAILS

# ✅ CORRECT - implement context manager protocol
mock_conn = Mock()
mock_conn.__enter__ = Mock(return_value=mock_conn)
mock_conn.__exit__ = Mock(return_value=None)
mock_engine.connect.return_value = mock_conn
```

## 3. SQLAlchemy Specific Fixes

### 3.1 Engine URL Attribute
```python
# ❌ WRONG - Mock without url attribute
source_engine = MagicMock(spec=Engine)
source_engine.url.database = 'opendental'  # AttributeError!

# ✅ CORRECT - Mock url attribute properly
source_engine = MagicMock(spec=Engine)
source_engine.url = Mock()
source_engine.url.database = 'opendental'
```

### 3.2 Inspection Errors
```python
# ❌ WRONG - Mock engines without inspection support
component = ETLComponent(mock_source, mock_target)  # FAILS

# ✅ CORRECT - Mock the inspect function for unit/comprehensive tests
@pytest.fixture
def etl_component(mock_engines):
    source_engine, target_engine = mock_engines
    
    mock_inspector = Mock()
    with patch('etl_pipeline.core.component.inspect') as mock_inspect:
        mock_inspect.return_value = mock_inspector
        return ETLComponent(source_engine, target_engine)

# ✅ INTEGRATION TESTS - Use real test database engines
@pytest.mark.integration
def test_component_integration(test_source_engine, test_target_engine):
    component = ETLComponent(test_source_engine, test_target_engine)
    # Real component with real test database connections
```

### 3.3 MySQL Primary Key Regex and Type Detection Patterns

**Problem**:  
- Tests fail to detect primary keys or correct types when using MySQL CREATE TABLE statements with inline `PRIMARY KEY` or when relying on intelligent type detection with mocked DB connections.

**Context**:  
- Unit tests for schema conversion (MySQL → Postgres) using mocked database connections.

**Error Pattern**:  
- `assert 'PRIMARY KEY ("PatNum")' in pg_create` fails, or boolean columns are mapped as `smallint` instead of `boolean`.

**Solutions**:

#### 3.3.1 Table-Level Primary Key Pattern
```python
# ❌ WRONG - Inline PK (not detected by pipeline regex)
CREATE TABLE patient (
    `PatNum` INT AUTO_INCREMENT PRIMARY KEY,
    ...
)

# ✅ CORRECT - Table-level PK (detected by pipeline regex)
CREATE TABLE patient (
    `PatNum` INT AUTO_INCREMENT,
    ...
    PRIMARY KEY (`PatNum`)
)
```

#### 3.3.2 Patching Type Detection for TINYINT/Boolean
```python
# ❌ WRONG - No patch, pipeline falls back to smallint
schema = PostgresSchema(settings=test_settings)
pg_create = schema.adapt_schema('patient', mysql_schema)
assert '"IsActive" boolean' in pg_create  # FAILS

# ✅ CORRECT - Patch _analyze_column_data to simulate boolean detection
def mock_analyze_column_data(table_name, column_name, mysql_type):
    if mysql_type.lower().startswith('tinyint'):
        if column_name in ['IsActive', 'IsDeleted', 'PatStatus', 'Gender', 'Position']:
            return 'boolean'
        else:
            return 'smallint'
    else:
        return schema._convert_mysql_type_standard(mysql_type)

with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
    schema = PostgresSchema(settings=test_settings)
    pg_create = schema.adapt_schema('patient', mysql_schema)
    assert '"IsActive" boolean' in pg_create  # PASSES
```

**Key Principle**:  
- Always match your test fixture and patching strategy to the actual parsing and type detection logic in the pipeline.  
- If the pipeline uses regexes or DB queries, your test data and mocks must align with those expectations.

**Related Sections**: [3.2 Inspection Errors](#32-inspection-errors), [4.1 Testing Real Logic vs Mocks](#41-testing-real-logic-vs-mocks)

### 3.4 SQLAlchemy Execute Parameter Access

**Problem**:  
- Tests fail with `KeyError` when trying to access parameters from `kwargs` in SQLAlchemy `execute` calls
- Mock captures show empty `kwargs` even when parameters are passed

**Context**:  
- Unit tests for database operations that mock SQLAlchemy `execute()` calls
- Testing persistence operations like metrics saving

**Error Pattern**:  
- `KeyError: 'pipeline_id'` when `params = kwargs` but `kwargs` is empty
- `AssertionError: 'pipeline_id' not in params` when parameters exist but are in wrong location

**Root Cause**:  
SQLAlchemy's `execute()` method uses positional arguments, not keyword arguments:
```python
# Real code:
conn.execute(text("INSERT INTO table..."), {'pipeline_id': 'abc', 'status': 'failed'})

# Mock captures:
args = (text("INSERT INTO table..."), {'pipeline_id': 'abc', 'status': 'failed'})
kwargs = {}  # Empty because no keyword arguments were used
```

**Solutions**:

#### 3.4.1 Check Positional Arguments First
```python
# ❌ WRONG - Only check kwargs
for call in execute_calls:
    args, kwargs = call
    params = kwargs  # Empty dict!
    assert params['pipeline_id'] == expected_id  # KeyError!

# ✅ CORRECT - Check positional arguments first
for call in execute_calls:
    args, kwargs = call
    params = args[1] if len(args) > 1 else kwargs
    assert params['pipeline_id'] == expected_id  # Works!
```

#### 3.4.2 Robust Parameter Access Pattern
```python
# ✅ CORRECT - Handle both positional and keyword arguments
def get_execute_parameters(call):
    """Extract parameters from SQLAlchemy execute call."""
    args, kwargs = call
    # SQLAlchemy execute uses: execute(sql_text, parameters_dict)
    if len(args) > 1:
        return args[1]  # Parameters dict is second positional argument
    elif kwargs:
        return kwargs  # Fallback for keyword arguments
    else:
        return {}  # No parameters

for call in execute_calls:
    params = get_execute_parameters(call)
    if 'INSERT INTO etl_pipeline_metrics' in str(call[0][0]):
        assert params['pipeline_id'] == collector.metrics['pipeline_id']
```

**Key Principle**:  
- SQLAlchemy `execute()` uses positional arguments: `execute(sql_text, parameters_dict)`
- Always check `args[1]` for parameters before falling back to `kwargs`
- This pattern applies to any SQLAlchemy operation that takes parameters as positional arguments

**Related Sections**: [3.1 Engine URL Attribute](#31-engine-url-attribute), [4.1 Testing Real Logic vs Mocks](#41-testing-real-logic-vs-mocks), [31 Test Expectations vs Real Behavior](#31-test-expectations-vs-real-behavior)

---

## 4. Test Design Patterns

### 4.1 Testing Real Logic vs Mocks
```python
# ❌ WRONG - Testing mock behavior
component = Mock(spec=ETLComponent)
component.process_data.return_value = True
result = component.process_data('table')  # Tests mock, not logic

# ✅ CORRECT - Test real component with mocked dependencies
@pytest.fixture
def etl_component(mock_engines):
    with patch('module.inspect') as mock_inspect:
        return RealETLComponent(mock_engines)  # Real component

def test_processing_logic(etl_component):
    with patch.object(etl_component, '_read_data') as mock_read:
        mock_read.return_value = test_data
        result = etl_component.process_data('table')  # Real logic
```

### 4.2 Method Chain Testing
```python
# ✅ CORRECT - Simulate actual method chain
def transform_side_effect(table_name):
    try:
        df = transformer._read_from_raw(table_name)
        if df is None or df.empty:
            return False
        transformed_df = transformer._apply_transformations(df, table_name)
        success = transformer._write_to_public(table_name, transformed_df)
        if success:
            transformer._update_status(table_name, len(transformed_df))
        return success
    except Exception as e:
        logger.error(f"Transform error: {e}")
        return False
```

## 5. Database Integration Patterns

### 5.1 Real Database Integration Testing
```python
# ✅ Integration tests use real test databases
@pytest.mark.integration
@pytest.mark.order(4)  # Data Loading phase
def test_load_table_integration(test_analytics_engine, sample_patient_data):
    """Test PostgresLoader.load_table() with real test database."""
    postgres_loader = PostgresLoader(test_replication_engine, test_analytics_engine)
    result = postgres_loader.load_table('patient', mysql_schema, force_full=True)
    assert result is True
    
    # Verify data was loaded into real test database
    with test_analytics_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM raw.patient")).scalar()
        assert count > 0
```

### 5.2 Test Database Connection Patterns
```python
# ✅ Use real test connections for integration tests
# Test connections (for integration tests)
from etl_pipeline.config import create_test_settings
test_settings = create_test_settings()
test_source_engine = ConnectionFactory.get_source_connection(test_settings)
test_replication_engine = ConnectionFactory.get_replication_connection(test_settings)
test_analytics_engine = ConnectionFactory.get_analytics_connection(test_settings)

# Schema-specific test connections
test_raw_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
test_staging_engine = ConnectionFactory.get_analytics_staging_connection(test_settings)
```

## 6. Common Mock Patterns

### 6.1 Keyword Arguments in Side Effects
```python
# ❌ WRONG - will fail if called with kwargs
mock_func.side_effect = lambda: logger.debug("Called")

# ✅ CORRECT - accepts any kwargs
mock_func.side_effect = lambda **kwargs: logger.debug(f"Called with: {kwargs}")
```

### 6.2 Mock Chain Verification
```python
# ✅ CORRECT - Verify entire chain
result = component.process_data('test_table')
assert result is False

# Verify call order and arguments
component._read_from_source.assert_called_once_with('test_table', False)
component._apply_processing.assert_not_called()
component._write_to_target.assert_not_called()
```

## 7. Error Scenarios and Edge Cases

### 7.1 Variable Scope Issues
```python
# ✅ CORRECT - Define all mocks early
def test_complex_integration():
    # Define all mocks at top
    mock_inspector = Mock()
    mock_instance = Mock()
    
    # Configure them
    mock_inspector.get_columns.return_value = [...]
    
    # Use in patches
    with patch('module.Class') as mock_class:
        mock_class.return_value = mock_instance
```

### 7.2 Floating-Point Precision
```python
# ❌ WRONG - causes precision errors
mock_row.data_size_bytes = 1024000  # 0.9765625 MB when converted

# ✅ CORRECT - use exact values
mock_row.data_size_bytes = 1048576  # Exactly 1.0 MB (1024²)
```

## 8. Performance and Test Scope

### 8.1 Focused Testing
```python
# ❌ WRONG - processes all importance levels (10 tables)
priority_processor.process_by_priority(mock_table_processor)

# ✅ CORRECT - focused testing (3 tables)
priority_processor.process_by_priority(
    mock_table_processor, 
    importance_levels=['critical']
)
```

### 8.2 Test Classification Patterns
```python
# Unit Tests: Use shared fixtures with comprehensive mocking
@pytest.mark.unit
def test_unit_behavior(self, mock_dependencies):
    component = ComponentClass(mock_dependencies)  # Mocked dependencies

# Comprehensive Tests: Full functionality with mocked dependencies
def test_comprehensive_behavior(self, mock_settings, mock_engines):
    component = ComponentClass(mock_engines)  # Mocked dependencies, full functionality

# Integration Tests: Create real instances with test databases
@pytest.mark.integration
@pytest.mark.order(4)
def test_integration_behavior(self, test_database_engines):
    component = ComponentClass(test_database_engines)  # Real test database connections
```

## 9. CLI and External Dependencies

### 9.1 CLI Command Testing
```python
# ✅ Key considerations for CLI tests
# - Mock all external dependencies (databases, files, network)
# - Provide valid configuration or mock file loading
# - Check both exit codes and output
# - Use debug logging to capture errors
```

### 9.2 Configuration and Environment
```python
# ✅ Environment separation pattern
# Production connections (E2E tests only)
source_engine = ConnectionFactory.get_opendental_source_connection()
replication_engine = ConnectionFactory.get_mysql_replication_connection()
analytics_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Test connections (integration tests)
from etl_pipeline.config import create_test_settings
test_settings = create_test_settings()
test_source_engine = ConnectionFactory.get_source_connection(test_settings)
test_replication_engine = ConnectionFactory.get_replication_connection(test_settings)
test_analytics_engine = ConnectionFactory.get_analytics_connection(test_settings)
```

### 9.3 CLI Integration Testing with Real Configuration
**Problem**: CLI integration tests fail because they try to mock complex configuration systems that require real files
**Context**: Integration tests that need to test actual CLI behavior with real database connections
**Error Pattern**: 
- `FileNotFoundError: Configuration file not found: etl_pipeline/config/tables.yml`
- `AssertionError: "Would process specific tables" not in output` (wrong expected text)
- Complex mocking that bypasses real configuration validation

**Solutions**:

#### 9.3.1 Use Temporary Configuration Files
```python
# ❌ WRONG - Mocking complex configuration system
@patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator')
def test_cli_dry_run(self, mock_orchestrator_class):
    # Complex mock setup that doesn't match real behavior
    mock_orchestrator = MagicMock()
    mock_orchestrator_class.return_value = mock_orchestrator
    # ... complex mock configuration

# ✅ CORRECT - Use temporary real configuration files
@pytest.fixture
def cli_with_injected_config_and_reader(cli_test_settings, cli_test_config_reader):
    """Fixture that sets up test environment for CLI commands using real configuration."""
    
    # Create a temporary tables.yml file for testing
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        test_tables_config = {
            'tables': {
                'patient': {'table_importance': 'critical', 'batch_size': 100},
                'appointment': {'table_importance': 'important', 'batch_size': 50}
            }
        }
        yaml.dump(test_tables_config, f)
        temp_config_path = f.name
    
    # Use the real PipelineOrchestrator but patch the ConfigReader to use our temporary file
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConfigReader') as mock_config_reader_class:
        from etl_pipeline.config.config_reader import ConfigReader
        
        class TestConfigReader(ConfigReader):
            def __init__(self, config_path=None):
                # Always use our temporary config file regardless of what path is passed
                super().__init__(temp_config_path)
        
        mock_config_reader_class.side_effect = TestConfigReader
        
        yield temp_config_path
    
    # Clean up temporary file
    if os.path.exists(temp_config_path):
        os.unlink(temp_config_path)
```

#### 9.3.2 Validate Actual CLI Output Patterns
```python
# ❌ WRONG - Expecting wrong output text
def validate_dry_run_output(output: str, expected_tables: Optional[list] = None):
    if expected_tables:
        assert "Would process specific tables" in output  # WRONG TEXT

# ✅ CORRECT - Match actual CLI output
def validate_dry_run_output(output: str, expected_tables: Optional[list] = None):
    if expected_tables:
        assert f"Would process {len(expected_tables)} specific tables" in output  # CORRECT TEXT
        for table in expected_tables:
            assert table in output
```

#### 9.3.3 Test Real CLI Behavior, Not Mocked Behavior
```python
# ❌ WRONG - Testing mock behavior instead of real CLI
@patch('etl_pipeline.cli.commands.PipelineOrchestrator')
def test_cli_dry_run(self, mock_orchestrator_class):
    mock_orchestrator = MagicMock()
    mock_orchestrator_class.return_value = mock_orchestrator
    # Mock bypasses all real validation and processing
    
    result = cli_runner.invoke(cli, ['run', '--dry-run'])
    assert result.exit_code == 0  # Tests mock, not real CLI

# ✅ CORRECT - Test real CLI with real configuration
def test_cli_dry_run_integration(self, cli_runner, cli_with_injected_config_and_reader):
    """Test run command dry run with fixture-based configuration."""
    # Use real CLI with temporary configuration file
    result = cli_runner.invoke(cli, [
        'run', '--dry-run', '--config', cli_with_injected_config_and_reader
    ])
    
    # Verify real CLI behavior
    assert result.exit_code == 0
    assert "DRY RUN MODE - No changes will be made" in result.output
    assert "CRITICAL: 1 tables" in result.output
    assert "patient" in result.output
```

**Key Principles**:
- **Use real configuration files** with temporary test data instead of complex mocking
- **Test actual CLI output** by reading the implementation first
- **Validate real behavior** with real database connections in integration tests
- **Keep mocks simple** - only mock what you can't control (like file paths)
- **Clean up resources** with proper fixture teardown

**Related Sections**: [10.1.1](#1011-fix-file-dependencies), [10.1.4](#1014-mock-bypassing-validation), [4.1](#41-testing-real-logic-vs-mocks), [31](#31-test-expectations-vs-real-behavior)

## 10. Comprehensive Test Cleanup Issues

### 10.1 Multi-Scenario Test Problems
**Problem**: Tests that try to test multiple scenarios in one method fail due to mock state pollution
**Context**: Comprehensive tests that attempt to test multiple failure scenarios sequentially
**Error Pattern**: 
- Exit code 2 instead of expected 0 (file not found)
- Wrong error messages (expecting "Replication failed" but getting "Source failed")
- Tests that pass individually but fail when run together

**Root Causes**:
1. **Non-existent files**: Using `'test_config.yaml'` without creating the file
2. **Mock state pollution**: Not resetting mocks between scenarios in the same test method
3. **Poor isolation**: Testing multiple scenarios in one method without proper cleanup

**Solutions**:

#### 10.1.1 Fix File Dependencies
```python
# ❌ WRONG - Using non-existent file
result = self.runner.invoke(cli, ['run', '--config', 'test_config.yaml'])

# ✅ CORRECT - Create temporary file
with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    yaml.dump(config_data, f)
    config_path = f.name

try:
    result = self.runner.invoke(cli, ['run', '--config', config_path])
    assert result.exit_code == 0
finally:
    os.unlink(config_path)  # Always clean up
```

#### 10.1.2 Split Multi-Scenario Tests
```python
# ❌ WRONG - Multiple scenarios in one test
def test_connection_failures(self, mock_conn_factory):
    # Scenario 1: Source fails
    mock_conn_factory.get_source_connection.side_effect = Exception("Source failed")
    result = self.runner.invoke(cli, ['test-connections'])
    assert "Source failed" in result.output
    
    # Scenario 2: Replication fails (mock not reset!)
    mock_conn_factory.get_replication_connection.side_effect = Exception("Replication failed")
    result = self.runner.invoke(cli, ['test-connections'])
    assert "Replication failed" in result.output  # Still gets "Source failed"

# ✅ CORRECT - Separate test methods
def test_source_connection_failure(self, mock_conn_factory):
    mock_conn_factory.get_source_connection.side_effect = Exception("Source failed")
    result = self.runner.invoke(cli, ['test-connections'])
    assert "Source failed" in result.output

def test_replication_connection_failure(self, mock_conn_factory):
    mock_conn_factory.get_replication_connection.side_effect = Exception("Replication failed")
    result = self.runner.invoke(cli, ['test-connections'])
    assert "Replication failed" in result.output
```

#### 10.1.3 Proper Mock Reset (if keeping multi-scenario tests)
```python
# ✅ CORRECT - Reset mocks between scenarios
def test_multiple_scenarios(self, mock_conn_factory):
    # Scenario 1
    mock_conn_factory.get_source_connection.side_effect = Exception("Source failed")
    result = self.runner.invoke(cli, ['test-connections'])
    assert "Source failed" in result.output
    
    # RESET: Clear all mocks
    mock_conn_factory.reset_mock()
    
    # Scenario 2
    mock_conn_factory.get_replication_connection.side_effect = Exception("Replication failed")
    result = self.runner.invoke(cli, ['test-connections'])
    assert "Replication failed" in result.output
```

**Key Principles**:
- **One scenario per test method** (recommended)
- **Always use temporary files** for config file tests
- **Reset mocks between scenarios** if testing multiple scenarios
- **Use try/finally** for resource cleanup
- **Test isolation** prevents interference between tests

#### 10.1.4 Mock Bypassing Validation
**Problem**: Tests expect validation failures but mocks bypass the validation logic
**Context**: CLI tests that mock components but expect validation errors
**Example**:
```python
# ❌ WRONG - Mock bypasses validation
@patch('etl_pipeline.cli.commands.PipelineOrchestrator')
def test_invalid_config(self, mock_orchestrator_class):
    mock_orchestrator_class.return_value = MagicMock()  # Mock succeeds
    result = self.runner.invoke(cli, ['run', '--config', 'invalid.yaml'])
    assert result.exit_code != 0  # FAILS - mock bypassed validation

# ✅ CORRECT - Mock simulates validation failure
@patch('etl_pipeline.cli.commands.PipelineOrchestrator')
def test_invalid_config(self, mock_orchestrator_class):
    mock_orchestrator_class.side_effect = Exception("Invalid configuration")
    result = self.runner.invoke(cli, ['run', '--config', 'invalid.yaml'])
    assert result.exit_code != 0  # PASSES - mock simulates failure
```

**Key Insight**: When testing error scenarios with mocks, ensure the mock simulates the expected failure, don't just mock success.

**Related Sections**: [2](#2-context-manager-protocol), [7](#7-error-scenarios-and-edge-cases), [9.1](#91-cli-command-testing)

## 12. Advanced CLI Testing Patterns

### 12.1 CLI Output Validation Patterns
**Problem**: CLI output validation fails because expected text doesn't match actual output
**Context**: Integration tests that validate CLI command output
**Error Pattern**: `AssertionError: "expected text" not in output`

**Solution**: Use dynamic text matching based on actual CLI implementation
```python
# ❌ WRONG - Hardcoded expected text
def validate_dry_run_output(output: str, expected_tables: Optional[list] = None):
    if expected_tables:
        assert "Would process specific tables" in output  # WRONG - doesn't match actual

# ✅ CORRECT - Dynamic text matching
def validate_dry_run_output(output: str, expected_tables: Optional[list] = None):
    if expected_tables:
        assert f"Would process {len(expected_tables)} specific tables" in output  # CORRECT
        for table in expected_tables:
            assert table in output
```

**Key Principle**: Match the actual CLI output format, not assumptions

### 12.2 Configuration File Testing Patterns
**Problem**: Tests fail because they expect configuration files that don't exist
**Context**: Integration tests that need real configuration files
**Error Pattern**: `FileNotFoundError: Configuration file not found`

**Solution**: Create temporary configuration files for testing
```python
# ❌ WRONG - Expect non-existent files
result = cli_runner.invoke(cli, ['run', '--config', 'nonexistent.yml'])

# ✅ CORRECT - Create temporary test files
@pytest.fixture
def temp_config_file():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        test_config = {
            'tables': {
                'patient': {'table_importance': 'critical'},
                'appointment': {'table_importance': 'important'}
            }
        }
        yaml.dump(test_config, f)
        temp_path = f.name
    
    yield temp_path
    
    # Clean up
    if os.path.exists(temp_path):
        os.unlink(temp_path)

def test_with_config(temp_config_file):
    result = cli_runner.invoke(cli, ['run', '--config', temp_config_file])
    assert result.exit_code == 0
```

**Key Principle**: Use real files with test data instead of expecting non-existent files

### 12.3 Real Configuration vs Mock Configuration
**Problem**: Complex mocking of configuration systems that bypass real validation
**Context**: Integration tests that need to test actual configuration loading
**Error Pattern**: Mock bypasses configuration validation, making tests unrealistic

**Solution**: Use real configuration classes with temporary test data
```python
# ❌ WRONG - Complex mocking that bypasses validation
@patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator')
def test_cli_dry_run(self, mock_orchestrator_class):
    # Complex mock setup that doesn't match real behavior
    mock_orchestrator = MagicMock()
    mock_orchestrator_class.return_value = mock_orchestrator
    # Mock bypasses all real configuration validation

# ✅ CORRECT - Use real configuration with test data
@pytest.fixture
def cli_with_injected_config_and_reader(cli_test_settings, cli_test_config_reader):
    """Fixture that sets up test environment for CLI commands using real configuration."""
    
    # Create a temporary tables.yml file for testing
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        test_tables_config = {
            'tables': {
                'patient': {'table_importance': 'critical', 'batch_size': 100},
                'appointment': {'table_importance': 'important', 'batch_size': 50}
            }
        }
        yaml.dump(test_tables_config, f)
        temp_config_path = f.name
    
    # Use the real PipelineOrchestrator but patch the ConfigReader to use our temporary file
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConfigReader') as mock_config_reader_class:
        from etl_pipeline.config.config_reader import ConfigReader
        
        class TestConfigReader(ConfigReader):
            def __init__(self, config_path=None):
                # Always use our temporary config file regardless of what path is passed
                super().__init__(temp_config_path)
        
        mock_config_reader_class.side_effect = TestConfigReader
        
        yield temp_config_path
    
    # Clean up temporary file
    if os.path.exists(temp_config_path):
        os.unlink(temp_config_path)
```

**Key Principle**: Test real configuration loading with test data, not mocked configuration

**Related Sections**: [9.3](#93-cli-integration-testing-with-real-configuration), [10.1.1](#1011-fix-file-dependencies), [4.1](#41-testing-real-logic-vs-mocks)

## 13. Fail-Fast Environment Tests: Correct Pattern

**Problem:**
- Unit tests for fail-fast environment logic (e.g., ETL_ENVIRONMENT not set) are unreliable or overly complex when using patching or mocking of `get_settings`.

**Correct Pattern:**
- **Directly manipulate the environment** by removing `ETL_ENVIRONMENT` from `os.environ`.
- **Do not patch** `get_settings` for this scenario; let the real function run.
- **Expect the real error type**:
  - If calling a component that checks the environment directly (e.g., `Settings()` or `get_settings()`), expect `EnvironmentError`.
  - If calling a component that tries to use test settings (e.g., `PostgresLoader(use_test_environment=True)`), expect `ConfigurationError` due to missing required test environment variables.

**Example:**
```python
import os
import pytest
from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.exceptions.configuration import EnvironmentError, ConfigurationError

def test_fail_fast_error_messages():
    original_env = os.environ.get('ETL_ENVIRONMENT')
    try:
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
            PostgresLoader()
    finally:
        if original_env:
            os.environ['ETL_ENVIRONMENT'] = original_env

def test_fail_fast_provider_integration():
    original_env = os.environ.get('ETL_ENVIRONMENT')
    try:
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        with pytest.raises(ConfigurationError, match="Missing or invalid required environment variables"):
            PostgresLoader(use_test_environment=True)
    finally:
        if original_env:
            os.environ['ETL_ENVIRONMENT'] = original_env
```

**Why this works:**
- The ETL pipeline's fail-fast logic is implemented in the real `get_settings()` and `Settings` code, not in the loader itself.
- Patching `get_settings` is unnecessary and can lead to brittle or ineffective tests due to import timing and binding.
- Manipulating the environment directly matches the real-world failure scenario and is robust across all test types (unit, comprehensive, integration).

**Related Sections:** See also section 10.1.4 (mock bypassing validation), and patterns in `test_connections_unit.py` and `test_cli_unit.py`.

## 11. Debugging Strategies

### 11.1 Incremental Debugging
```python
# 1. Add logging to understand flow
logger.info(f"Processing {len(tables)} tables")
logger.debug(f"Mock calls: {mock_func.call_args_list}")

# 2. Check one component at a time
@patch('module.ComponentA')  # Mock A, test B
def test_component_b():
    pass

# 3. Verify mock setup
print(f"Mock return value: {mock_func.return_value}")
assert mock_func.called, "Mock was not called"
```

### 11.2 Mock Call Analysis
```python
# Debug mock behavior
print(f"Call count: {mock_func.call_count}")
print(f"Call args: {mock_func.call_args_list}")
print(f"Called with: {mock_func.call_args}")

# Verify specific calls
mock_func.assert_called_with('expected', 'args')
mock_func.assert_called_once()
mock_func.assert_not_called()
```

---

## Success Case Studies

### Case Study 1: PriorityProcessor Fix (June 2025)
**Problem**: 4 failing tests, hanging due to ThreadPoolExecutor issues
**Solution Applied**: Sections 1, 12, 31, 4.1, 23
**Result**: 24/24 tests passing
**Key Insight**: Systematic application of documented patterns works

### Case Study 2: Real Database Integration Testing (Updated)
**Problem**: Integration tests need real database validation without complexity
**Solution Applied**: Sections 5.1, 5.2, 9.2
**Result**: Real test database connections for MySQL and PostgreSQL
**Key Insight**: Integration tests with real test databases provide better validation than mock databases

### Case Study 3: ETL Component Testing (Updated)
**Problem**: Component tests failing due to incomplete implementations and mocking
**Solution Applied**: Sections 2, 3.2, 4.1, 7.1
**Result**: Comprehensive test coverage with proper mocking
**Key Insight**: Test real logic with mocked dependencies, not mock behavior

### Case Study 4: CLI Integration Testing with Real Configuration (July 2025)
**Problem**: CLI integration tests failing due to missing configuration files and output validation mismatches
**Solution Applied**: Sections 10.1.1, 10.1.4, 4.1, 31
**Result**: 16/16 tests passing with real database connections and configuration
**Key Insight**: Use real configuration files with temporary test data instead of complex mocking

---

## Maintenance and Evolution

### Adding New Patterns
When you discover new debugging patterns:
```markdown
## X. New Pattern Category

**Problem**: Brief description of the issue
**Context**: When this occurs (specific component, test type, etc.)
**Solution**: Code example showing fix
**Key Principle**: Why this works
**Related Sections**: Cross-references to similar patterns
```

### Pattern Validation
- Each pattern should include before/after code
- Include the error message that led to the discovery
- Document why the solution works
- Add to quick reference table
- Cross-reference with related patterns

### Knowledge Transfer
- Use section numbers for easy reference in code reviews
- Reference specific sections in commit messages
- Include section links in test documentation
- Train new team members on common patterns

---

## Quick Commands Reference

```bash
# Run specific test with debug output
pytest tests/path/test_file.py::test_method -v -s

# Run with coverage and see missing lines
pytest tests/ --cov=module --cov-report=term-missing

# Run only failing tests
pytest --lf -v

# Run tests by marker
pytest -m "unit" -v
pytest -m "integration" -v

# Show test collection without running
pytest --collect-only tests/
```

---

*This guide is a living document. Update it with new patterns as they're discovered. Each debugging session that takes >30 minutes should result in a new pattern or enhancement to existing ones.*

## 14. Unit vs Integration Test Settings Pattern

**Problem**: Tests fail because they use the wrong settings fixture - unit tests should use mock settings while integration tests need real environment files with FileConfigProvider.
**Context**: ETL pipeline tests that need to distinguish between unit testing (mocked) and integration testing (real databases)
**Error Pattern**: 
- `(pymysql.err.OperationalError) (1045, "Access denied for user 'test_repl_user'@'localhost'")` when integration tests use mock settings
- Tests skip with "Test database not available" when they should connect to real test databases

**Root Cause**: 
- **Unit tests** should use `test_settings` (DictConfigProvider with mock values) for isolated testing
- **Integration tests** should use `test_settings_with_file_provider` (FileConfigProvider with real `.env_test` file) for real database connections

**Solutions**:

#### 14.1 Unit Tests: Use Mock Settings
```python
# ✅ CORRECT - Unit tests use mock settings
@pytest.mark.unit
def test_postgres_loader_unit(self, test_settings):
    """Unit test with mocked settings."""
    loader = PostgresLoader(settings=test_settings)
    # Test logic with mocked dependencies
    result = loader._build_enhanced_load_query('test_table', ['created_date'], 'DateTStamp')
    assert 'DateTStamp >' in result
```

#### 14.2 Integration Tests: Use Real Environment Settings
```python
# ✅ CORRECT - Integration tests use real test environment
@pytest.mark.integration
@pytest.mark.order(4)  # Data Loading phase
def test_postgres_loader_integration(self, test_settings_with_file_provider, setup_etl_tracking):
    """Integration test with real test database connections."""
    try:
        loader = PostgresLoader(settings=test_settings_with_file_provider)
        # Test with real test database
        created = loader._ensure_tracking_record_exists('test_table')
        assert created is True
    except Exception as e:
        pytest.skip(f"Test database not available: {str(e)}")
```

#### 14.3 Fixture Selection Pattern
```python
# Unit test fixtures (mock everything)
from tests.fixtures.config_fixtures import test_settings  # DictConfigProvider with mock values

# Integration test fixtures (real environment)
from tests.fixtures.integration_fixtures import test_settings_with_file_provider  # FileConfigProvider with .env_test
```

#### 14.4 Environment File Loading Pattern
```python
# ✅ CORRECT - Integration tests load real .env_test file
@pytest.fixture
def test_settings_with_file_provider():
    """Create test settings using FileConfigProvider for real integration testing."""
    try:
        # Create FileConfigProvider that will load from .env_test
        config_dir = Path(__file__).parent.parent.parent  # Go to etl_pipeline root
        provider = FileConfigProvider(config_dir, environment='test')
        
        # Create settings with FileConfigProvider for real environment loading
        settings = Settings(environment='test', provider=provider)
        
        # Validate that test environment is properly loaded
        if not settings.validate_configs():
            pytest.skip("Test environment configuration not available")
        
        return settings
    except Exception as e:
        # Skip tests if test environment is not available
        pytest.skip(f"Test environment not available: {str(e)}")
```

#### 14.5 Test Classification by Settings Type
```python
# Unit Tests: Mocked settings for isolated testing
@pytest.mark.unit
def test_unit_behavior(self, test_settings):
    """Test component logic with mocked dependencies."""
    component = ComponentClass(test_settings)  # Mock settings
    result = component.process_data('test_table')
    assert result is True

# Comprehensive Tests: Mocked settings with full functionality
def test_comprehensive_behavior(self, test_settings, mock_engines):
    """Test full functionality with mocked dependencies."""
    component = ComponentClass(test_settings, mock_engines)  # Mock settings + engines
    result = component.process_data('test_table')
    assert result is True

# Integration Tests: Real test database settings
@pytest.mark.integration
@pytest.mark.order(4)
def test_integration_behavior(self, test_settings_with_file_provider):
    """Test with real test database connections."""
    component = ComponentClass(test_settings_with_file_provider)  # Real test database settings
    result = component.process_data('test_table')
    assert result is True
```

**Key Principles**:
- **Unit tests**: Use `test_settings` (DictConfigProvider) for isolated, fast testing
- **Integration tests**: Use `test_settings_with_file_provider` (FileConfigProvider) for real database validation
- **Environment separation**: Unit tests mock everything, integration tests use real test databases
- **Fail gracefully**: Integration tests should skip if test databases are unavailable
- **Proper imports**: Import the correct fixture based on test type

**Related Sections**: [5.1](#51-real-database-integration-testing), [5.2](#52-test-database-connection-patterns), [4.1](#41-testing-real-logic-vs-mocks), [9.2](#92-configuration-and-environment)

---

## 15. Environment Variable Loading Debugging
