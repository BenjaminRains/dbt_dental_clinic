"""
Unit tests for OpenDentalSchemaAnalyzer using provider pattern with DictConfigProvider.

This package contains unit tests for the OpenDentalSchemaAnalyzer with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Modules:
    - test_initialization: Tests for OpenDentalSchemaAnalyzer initialization
    - test_table_discovery: Tests for table discovery and database inspection
    - test_schema_analysis: Tests for schema analysis and column information
    - test_size_analysis: Tests for table size analysis and row counting
    - test_importance_determination: Tests for table importance classification
    - test_extraction_strategy: Tests for extraction strategy determination
    - test_incremental_columns: Tests for incremental column discovery
    - test_dbt_model_discovery: Tests for DBT model discovery
    - test_configuration_generation: Tests for configuration file generation
    - test_schema_hash: Tests for schema hash generation
    - test_error_handling: Tests for error handling and exception management
    - test_batch_processing: Tests for batch processing operations
    - test_progress_monitoring: Tests for progress monitoring and reporting
    - test_incremental_strategy: Tests for incremental strategy determination

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests FAIL FAST behavior when OPENDENTAL_SOURCE_DB not set
    - Ensures type safety and configuration validation
    - Tests environment separation (production vs test) with provider pattern
    - Validates schema analysis and configuration generation

Coverage Areas:
    - OpenDentalSchemaAnalyzer initialization with Settings injection
    - Table discovery with mocked database inspector
    - Schema analysis with mocked table structures
    - Table size analysis with mocked row counts
    - Table importance determination with systematic criteria
    - Extraction strategy determination with mocked data
    - Incremental column discovery with mocked schemas
    - DBT model discovery with mocked project structure
    - Configuration generation with mocked metadata
    - Error handling with mocked failure scenarios
    - FAIL FAST behavior with mocked environment validation

ETL Context:
    - Critical for ETL pipeline configuration generation
    - Tests with mocked dental clinic database schemas
    - Uses Settings injection with DictConfigProvider for unit testing
    - Generates test tables.yml for ETL pipeline configuration
"""