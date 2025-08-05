"""
PostgresLoader unit test modules.

This package contains modular unit tests for the PostgresLoader class,
organized by functionality to improve maintainability and test isolation.

Test Modules:
    - test_initialization_unit.py: Basic initialization and configuration tests
    - test_query_logic_unit.py: SQL query building and incremental logic tests
    - test_data_quality_unit.py: Data quality validation and filtering tests
    - test_bulk_operations_unit.py: Bulk insert and streaming operations tests
    - test_loading_strategies_unit.py: Table loading strategy tests
    - test_error_handling_unit.py: Error handling and exception scenarios
    - test_data_conversion_unit.py: Data type conversion and handling tests
    - test_schema_cache_unit.py: Schema cache operations and performance tests
    - test_tracking_status_unit.py: Tracking record and status management tests
    - test_advanced_query_unit.py: Advanced query building and strategy tests
    - test_advanced_loading_unit.py: Parallel loading and data validation tests
"""

__all__ = [
    'test_initialization_unit',
    'test_query_logic_unit', 
    'test_data_quality_unit',
    'test_bulk_operations_unit',
    'test_loading_strategies_unit',
    'test_error_handling_unit',
    'test_data_conversion_unit',
    'test_schema_cache_unit',
    'test_tracking_status_unit',
    'test_advanced_query_unit',
    'test_advanced_loading_unit'
] 