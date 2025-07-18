"""
Unit tests for SimpleMySQLReplicator using provider pattern with DictConfigProvider.

This package contains unit tests for the SimpleMySQLReplicator with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Modules:
    - test_initialization: Tests for SimpleMySQLReplicator initialization
    - test_configuration: Tests for configuration methods and strategy determination
    - test_table_copy_logic: Tests for table copy logic and database operations
    - test_bulk_operations: Tests for bulk table copying operations
    - test_error_handling: Tests for error handling and exception management
    - test_integration_points: Tests for integration with other components

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests FAIL FAST behavior when ETL_ENVIRONMENT not set
    - Ensures type safety with DatabaseType and PostgresSchema enums
    - Tests environment separation (production vs test) with provider pattern
    - Validates incremental copy logic and configuration management

Coverage Areas:
    - SimpleMySQLReplicator initialization with Settings injection
    - Configuration loading from YAML files with provider pattern
    - Copy strategy determination based on table size
    - Incremental copy logic with change data capture
    - Error handling and logging for dental clinic ETL operations
    - Provider pattern configuration isolation and environment separation
    - Settings injection for environment-agnostic connections

ETL Context:
    - Critical for nightly ETL pipeline execution with dental clinic data
    - Supports MariaDB v11.6 source and MySQL replication database
    - Uses provider pattern for clean dependency injection and test isolation
    - Implements Settings injection for environment-agnostic connections
    - Enforces FAIL FAST security to prevent accidental production usage
""" 