## Common Issues and Solutions

### 1. Mock Not Being Called

If a mock isn't being called as expected:
```python
# Add debug logging to mock
mock_func.side_effect = lambda *args, **kwargs: logger.debug(f"Mock called with: {args}, {kwargs}")

# Verify mock calls
assert mock_func.call_count > 0, "Mock was not called"
print(f"Mock calls: {mock_func.call_args_list}")
```

### 2. Database Connection Issues

For database-related tests:
```python
# Log connection details
logger.debug(f"Connecting to database: {db_url}")

# Verify connection
assert engine is not None, "Failed to create engine"
assert engine.connect() is not None, "Failed to connect to database"
```

### 3. Schema Conversion Issues

For schema conversion tests:
```python
# Log input schema
logger.debug(f"Input schema: {mysql_schema}")

# Log intermediate results
logger.debug(f"Extracted columns: {columns}")

# Log final output
logger.debug(f"PostgreSQL schema: {pg_schema}")
```

### 4. SQLAlchemy Inspection Errors

When getting "No inspection system is available" errors:
```python
# Mock engines with proper attributes for SQLAlchemy compatibility
source_engine = Mock(spec=Engine)
source_engine.url = Mock()
source_engine.url.drivername = 'mysql'
source_engine.dialect = Mock()
source_engine.dialect.name = 'mysql'

# Mock inspect function globally
with patch('sqlalchemy.inspect') as mock_inspect:
    mock_inspector = Mock()
    mock_inspector.get_columns.return_value = [...]
    mock_inspect.return_value = mock_inspector
```

### 5. Complex Integration Test Mocking

For tests involving multiple interconnected components:
```python
# Create mocks for all external dependencies first
mock_inspector = Mock()  # Define early to avoid UnboundLocalError

# Mock classes where they're imported, not where they're defined
with patch('module.submodule.ClassName') as mock_class:
    mock_instance = Mock()
    mock_class.return_value = mock_instance
    
# For classes that call inspect() in __init__, mock the class completely
# rather than trying to mock individual inspect calls
```

### 6. Variable Scope Issues in Tests

When getting "cannot access local variable" errors:
```python
# Define mock objects at the top of the test function
def test_complex_integration():
    # Define all mocks early
    mock_inspector = Mock()
    mock_instance = Mock()
    
    # Then configure them
    mock_inspector.get_columns.return_value = [...]
    
    # Use them in patches
    with patch('module.Class') as mock_class:
        mock_class.return_value = mock_instance
```

### 7. Generalized Lessons Learned

- **Patch Where Used, Not Where Defined:**  
  Always patch classes/functions where they are imported in the code under test, not where they are originally defined.

- **Match Actual Call Signatures:**  
  When asserting mock calls, ensure your assertions match the actual function call signatures (omit defaults unless explicitly passed).

- **Add Debug Logging:**  
  Use debug logging and print mock call arguments to quickly diagnose why a mock isn't being called or is called incorrectly.

- **Mock All External Dependencies Early:**  
  Define and configure all mocks at the top of your test function to avoid scope and initialization issues.

- **Handle Complex Dependencies with Full Class Mocks:**  
  For classes with complex initialization (e.g., those calling `inspect()`), mock the entire class rather than individual methods.

- **Check for SQLAlchemy Compatibility:**  
  When mocking engines or inspectors, ensure all required attributes (like `url`, `dialect`) are present for compatibility.

- **Incremental Debugging:**  
  Fix one error at a time, rerun tests, and use error messages to guide your next step.

### 8. High-Level Mocking Strategy

When testing complex systems with many dependencies:
```python
# Instead of mocking all internal dependencies:
@patch('module.connection_factory')
@patch('module.settings')
@patch('module.transformer')
def test_complex_system(mock_transformer, mock_settings, mock_conn_factory):
    # Complex setup with many mocks...

# Mock at the highest level possible:
@patch('module.main_component')
def test_complex_system(mock_main):
    # Mock only the main component's interface
    mock_main.initialize.return_value = True
    mock_main.process.return_value = True
    
    # Test the high-level behavior
    result = run_command()
    assert result.success
    mock_main.process.assert_called_with(expected_args)
```

Key principles:
- Mock at the highest level of abstraction you're testing
- Focus on the component's interface, not its implementation
- Let the component handle its own internal dependencies
- Only mock what you need to verify the test's behavior
- Use context manager mocks when testing with `with` statements

This approach:
- Reduces test complexity
- Makes tests more maintainable
- Focuses on behavior rather than implementation
- Is less likely to break with internal changes

### 9. Testing CLI Commands

When testing CLI commands, keep in mind:

- **Exit Codes:**
  - CLI commands may exit with different codes for success or error. Always check exit codes in your tests.

- **External Dependencies:**
  - CLI commands often rely on external resources (e.g., databases, files, network). If not mocked, these can cause tests to fail unexpectedly.

- **Configuration Files:**
  - Many CLI commands require configuration files. Ensure your test provides a valid config file or mocks the file loading process.

**General Solutions:**
- Mock all external dependencies to isolate the CLI logic from outside failures.
- Provide a valid configuration file or mock file access as needed.
- Assert both the exit code and relevant output to verify expected behavior.
- Use debug logging to capture error messages and aid troubleshooting.

This approach helps ensure CLI tests are robust, reliable, and not dependent on the external environment.

### 10. Handling Keyword Arguments in Mock Side Effects

When mocking functions that are called with keyword arguments:
```python
# INCORRECT - will fail if called with kwargs
mock_func.side_effect = lambda: logger.debug("Mock called")

# CORRECT - accepts any kwargs
mock_func.side_effect = lambda **kwargs: logger.debug("Mock called")
```

Common pitfalls:
- If the real function is called with keyword arguments (e.g., `pool_size`, `max_overflow`), but your mock's side effect doesn't accept them, you'll get an error like:
  ```
  got an unexpected keyword argument 'pool_size'
  ```
- This is especially common when mocking database connection methods that accept configuration parameters

Best practices:
- Always use `**kwargs` in mock side effects when the real function might be called with keyword arguments
- Even if you don't use the arguments in your mock, accepting them prevents the test from failing
- Consider logging the received arguments for debugging:
  ```python
  mock_func.side_effect = lambda **kwargs: logger.debug(f"Mock called with: {kwargs}")
  ```

This approach makes your mocks more robust and prevents unexpected failures when the real function's signature changes or when called with additional parameters.

### 11. Mocking Complex Method Chains

When testing classes with complex method chains (e.g., ETL transformers):

```python
# INCORRECT - Direct mocking of return values
transformer = Mock(spec=MyTransformer)
transformer.method1.return_value = value1
transformer.method2.return_value = value2
transformer.main_method.return_value = True  # This bypasses the chain!

# CORRECT - Using side_effect to simulate the chain
def main_method_side_effect(*args, **kwargs):
    # Simulate the actual method chain
    result1 = transformer.method1(*args, **kwargs)
    if not result1:
        return False
    result2 = transformer.method2(result1)
    return result2

transformer.main_method = Mock(side_effect=main_method_side_effect)
```

Key principles:
- Mock the entire chain behavior, not just return values
- Use `side_effect` to simulate the actual method flow
- Handle error cases and edge conditions in the side effect
- Ensure mock methods are called in the correct order
- Verify both the final result and intermediate calls

Example from ETL transformer:
```python
def transform_table_side_effect(table_name, is_incremental=False):
    # Simulate the actual transformation chain
    df = transformer._read_from_raw(table_name, is_incremental)
    if df is None or df.empty:
        return False
    try:
        transformed_df = transformer._apply_transformations(df, table_name)
        success = transformer._write_to_public(table_name, transformed_df, is_incremental)
        if success:
            transformer._update_transformation_status(table_name, len(transformed_df))
        return success
    except Exception:
        return False
```

Benefits:
- Tests reflect actual code behavior
- Catches issues with method ordering
- Verifies error handling
- Tests complete workflows
- More maintainable tests

### 12. Handling Mock Return Types

When mocking methods that return specific types (e.g., DataFrames):

```python
# INCORRECT - Mock returns Mock object
transformer._read_from_raw = Mock()
result = transformer._read_from_raw()  # Returns Mock, not DataFrame

# CORRECT - Mock returns correct type
test_df = pd.DataFrame({'id': [1], 'name': ['test']})
transformer._read_from_raw = Mock(return_value=test_df)
result = transformer._read_from_raw()  # Returns DataFrame
```

Key principles:
- Always return the correct type from mocks
- Use realistic test data
- Consider edge cases (empty DataFrames, None values)
- Match the real method's return type exactly
- Use type hints to catch type mismatches early

Example:
```python
# Setup mock chain with correct types
test_df = pd.DataFrame({'id': [1], 'name': ['test']})
transformer._read_from_raw.return_value = test_df
transformer._apply_transformations.return_value = test_df
transformer._write_to_public.return_value = True  # Boolean for success/failure
```

Benefits:
- Type checking works correctly
- Tests catch type-related bugs
- More realistic test scenarios
- Better IDE support
- Clearer test failures

### 13. Debugging Mock Chains

When debugging complex mock chains:

```python
# Add debug logging to mock chain
def transform_table_side_effect(table_name, is_incremental=False):
    logger.debug(f"Starting transform for {table_name}")
    df = transformer._read_from_raw(table_name, is_incremental)
    logger.debug(f"Read data: {df.shape if df is not None else 'None'}")
    # ... rest of chain ...

# Print mock calls for debugging
print(f"Mock calls: {transformer._read_from_raw.call_args_list}")
print(f"Mock return values: {transformer._read_from_raw.return_value}")
```

Key principles:
- Add logging at each step of the chain
- Print mock call arguments and return values
- Use `call_args_list` to see all calls
- Check mock call counts
- Verify mock call order

Benefits:
- Easier to diagnose failures
- Clear visibility into mock behavior
- Helps identify chain breaks
- Shows unexpected calls
- Validates mock setup
```

### 14. Error Handling in Mock Chains

When testing error scenarios in complex chains:

```python
# INCORRECT - Error not properly propagated
def transform_table_side_effect(table_name):
    df = transformer._read_from_raw(table_name)  # Exception here
    # Never reaches here, but no error handling

# CORRECT - Proper error handling and logging
def transform_table_side_effect(table_name):
    try:
        df = transformer._read_from_raw(table_name)
        if df is None or df.empty:
            return False
        # ... rest of chain ...
    except Exception as e:
        logger.error(f"Error in transform_table: {str(e)}")
        return False
```

Key principles:
- Wrap entire chain in try-except
- Log errors with context
- Return appropriate error values
- Verify error handling in tests
- Check that subsequent methods aren't called

Benefits:
- Tests verify error handling
- Better debugging information
- More robust tests
- Clearer error messages
- Proper error propagation

### 15. Logging Best Practices in Tests

When adding logging to tests:

```python
# INCORRECT - No logging or print statements
def test_transform_table_error():
    result = transformer.transform_table('test_table')
    assert result is False

# CORRECT - Proper logging
def test_transform_table_error():
    logger.info("Testing error handling in transform_table")
    result = transformer.transform_table('test_table')
    logger.debug(f"Transform result: {result}")
    assert result is False
```

Key principles:
- Use appropriate log levels (debug, info, error)
- Log both inputs and outputs
- Include context in log messages
- Log error details
- Use structured logging when possible

Benefits:
- Easier debugging
- Better test documentation
- Clearer test failures
- More maintainable tests
- Better error tracking

### 16. Mock Chain Verification

When verifying complex mock chains:

```python
# INCORRECT - Only checking final result
def test_transform_table_error():
    result = transformer.transform_table('test_table')
    assert result is False

# CORRECT - Verifying entire chain
def test_transform_table_error():
    result = transformer.transform_table('test_table')
    assert result is False
    transformer._read_from_raw.assert_called_once_with('test_table', False)
    transformer._apply_transformations.assert_not_called()
    transformer._write_to_public.assert_not_called()
    transformer._update_transformation_status.assert_not_called()
```

Key principles:
- Verify each step in the chain
- Check method call arguments
- Verify call order
- Check that methods aren't called when they shouldn't be
- Use appropriate assertion methods

Benefits:
- More thorough testing
- Better error detection
- Clearer test failures
- More maintainable tests
- Better test coverage