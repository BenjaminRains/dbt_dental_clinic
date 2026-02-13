"""
Integration tests for OpenDentalSchemaAnalyzer using real clinic database connections.

This package contains integration tests for the OpenDentalSchemaAnalyzer with real clinic
database connections to validate actual schema analysis, table discovery, and configuration
generation. All tests use FileConfigProvider for clinic environment configuration.

Test Modules:
    - test_initialization_integration: Tests for OpenDentalSchemaAnalyzer initialization with real clinic DB
    - test_table_discovery_integration: Tests for table discovery with real clinic database
    - test_schema_analysis_integration: Tests for schema analysis with real clinic database structure
    - test_size_analysis_integration: Tests for table size analysis with real production data
    - test_importance_determination_integration: Tests for table importance classification with real data
    - test_extraction_strategy_integration: Tests for extraction strategy determination with real data
    - test_incremental_strategy_integration: Tests for incremental strategy and column discovery with real data
    - test_dbt_integration_integration: Tests for DBT model discovery with real project structure
    - test_configuration_generation_integration: Tests for configuration file generation with real metadata
    - test_batch_processing_integration: Tests for batch processing operations with real database
    - test_reporting_integration: Tests for analysis reports and summary generation with real data
    - test_data_quality_integration: Tests for data quality validation with real clinic data

Test Strategy:
    - Integration tests with real clinic database connections using FileConfigProvider
    - Validates provider pattern dependency injection and Settings injection with real environment
    - Tests FAIL FAST behavior with real clinic environment validation
    - Ensures type safety and configuration validation with actual clinic data
    - Tests environment separation (clinic vs test) with real FileConfigProvider
    - Validates real schema analysis and configuration generation with clinic metadata

Coverage Areas:
    - OpenDentalSchemaAnalyzer initialization with real clinic database connection
    - Table discovery with real clinic database inspector
    - Schema analysis with real clinic table structures
    - Table size analysis with real clinic row counts
    - Table importance determination with real clinic data
    - Extraction strategy determination with real clinic data
    - Incremental column discovery with real clinic schemas
    - DBT model discovery with real project structure
    - Configuration generation with real clinic metadata
    - Error handling with real clinic database scenarios
    - FAIL FAST behavior with real clinic environment validation
    - Data quality validation with real clinic timestamp columns

ETL Context:
    - Critical for clinic ETL pipeline configuration generation
    - Tests with real clinic dental database schemas
    - Uses Settings injection with FileConfigProvider for clinic environment
    - Generates clinic-ready tables.yml for ETL pipeline configuration
    - Validates actual clinic database connections and schema analysis
    - Tests real clinic database operations and error scenarios

Clinic Test Environment:
    - Uses real clinic database connections with readonly access
    - Tests real schema analysis with actual clinic data structures
    - Validates table discovery with real clinic database
    - Tests configuration generation with real clinic metadata
    - Uses Settings injection with FileConfigProvider for clinic environment
    - Tests FAIL FAST behavior with clinic environment validation
    - Generates real configuration files for clinic ETL pipeline

Integration Test Categories:
    - Initialization: Real clinic database connection establishment
    - Table Discovery: Real clinic table discovery and filtering
    - Schema Analysis: Real clinic schema information extraction
    - Size Analysis: Real clinic table size and row count analysis
    - Importance Determination: Real clinic table classification
    - Extraction Strategy: Real clinic strategy determination
    - Incremental Strategy: Real clinic incremental column discovery
    - DBT Integration: Real clinic DBT model discovery
    - Configuration Generation: Real clinic configuration file generation
    - Batch Processing: Real clinic batch operations
    - Reporting: Real clinic analysis report generation
    - Data Quality: Real clinic data quality validation
""" 