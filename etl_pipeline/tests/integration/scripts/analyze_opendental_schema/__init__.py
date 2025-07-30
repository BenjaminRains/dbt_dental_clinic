"""
Integration tests for OpenDentalSchemaAnalyzer using real production database connections.

This package contains integration tests for the OpenDentalSchemaAnalyzer with real production
database connections to validate actual schema analysis, table discovery, and configuration
generation. All tests use FileConfigProvider for production environment configuration.

Test Modules:
    - test_initialization_integration: Tests for OpenDentalSchemaAnalyzer initialization with real production DB
    - test_table_discovery_integration: Tests for table discovery with real production database
    - test_schema_analysis_integration: Tests for schema analysis with real production database structure
    - test_size_analysis_integration: Tests for table size analysis with real production data
    - test_importance_determination_integration: Tests for table importance classification with real data
    - test_extraction_strategy_integration: Tests for extraction strategy determination with real data
    - test_incremental_strategy_integration: Tests for incremental strategy and column discovery with real data
    - test_dbt_integration_integration: Tests for DBT model discovery with real project structure
    - test_configuration_generation_integration: Tests for configuration file generation with real metadata
    - test_batch_processing_integration: Tests for batch processing operations with real database
    - test_reporting_integration: Tests for analysis reports and summary generation with real data
    - test_data_quality_integration: Tests for data quality validation with real production data

Test Strategy:
    - Integration tests with real production database connections using FileConfigProvider
    - Validates provider pattern dependency injection and Settings injection with real environment
    - Tests FAIL FAST behavior with real production environment validation
    - Ensures type safety and configuration validation with actual production data
    - Tests environment separation (production vs test) with real FileConfigProvider
    - Validates real schema analysis and configuration generation with production metadata

Coverage Areas:
    - OpenDentalSchemaAnalyzer initialization with real production database connection
    - Table discovery with real production database inspector
    - Schema analysis with real production table structures
    - Table size analysis with real production row counts
    - Table importance determination with real production data
    - Extraction strategy determination with real production data
    - Incremental column discovery with real production schemas
    - DBT model discovery with real project structure
    - Configuration generation with real production metadata
    - Error handling with real production database scenarios
    - FAIL FAST behavior with real production environment validation
    - Data quality validation with real production timestamp columns

ETL Context:
    - Critical for production ETL pipeline configuration generation
    - Tests with real production dental clinic database schemas
    - Uses Settings injection with FileConfigProvider for production environment
    - Generates production-ready tables.yml for ETL pipeline configuration
    - Validates actual production database connections and schema analysis
    - Tests real production database operations and error scenarios

Production Test Environment:
    - Uses real production database connections with readonly access
    - Tests real schema analysis with actual production data structures
    - Validates table discovery with real production database
    - Tests configuration generation with real production metadata
    - Uses Settings injection with FileConfigProvider for production environment
    - Tests FAIL FAST behavior with production environment validation
    - Generates real configuration files for production ETL pipeline

Integration Test Categories:
    - Initialization: Real production database connection establishment
    - Table Discovery: Real production table discovery and filtering
    - Schema Analysis: Real production schema information extraction
    - Size Analysis: Real production table size and row count analysis
    - Importance Determination: Real production table classification
    - Extraction Strategy: Real production strategy determination
    - Incremental Strategy: Real production incremental column discovery
    - DBT Integration: Real production DBT model discovery
    - Configuration Generation: Real production configuration file generation
    - Batch Processing: Real production batch operations
    - Reporting: Real production analysis report generation
    - Data Quality: Real production data quality validation
""" 