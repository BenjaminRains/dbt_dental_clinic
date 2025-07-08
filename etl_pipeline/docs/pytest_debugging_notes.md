# ETL Pipeline Pytest Debugging Guide

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

---

## Core Debugging Principles

### 🎯 **Golden Rules**
1. **Patch where used, not where defined** - Always patch at import location
2. **Test real behavior, not assumptions** - Read implementation before writing tests  
3. **Mock at highest abstraction level possible** - Don't over-mock
4. **Add debug logging** - Use `logger.debug()` to understand execution flow
5. **Fix one issue at a time** - Don't try to fix everything simultaneously

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
test_source_engine = ConnectionFactory.get_opendental_source_test_connection()
test_replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
test_analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection()

# Schema-specific test connections
test_raw_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
test_staging_engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
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
test_source_engine = ConnectionFactory.get_opendental_source_test_connection()
test_replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
test_analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection()
```

## 10. Debugging Strategies

### 10.1 Incremental Debugging
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

### 10.2 Mock Call Analysis
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