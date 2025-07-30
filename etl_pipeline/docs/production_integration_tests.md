# Production Integration Tests

This document explains how to set up and run production integration tests for the ETL pipeline.

## Overview

The integration tests in `tests/integration/scripts/analyze_opendental_schema/` require access to
 the actual production OpenDental database to validate real schema analysis, incremental strategy
  determination, and data quality validation.

## Prerequisites

1. **Database Access**: You need readonly access to the production OpenDental database
2. **Network Access**: Ability to connect to the production database server
3. **Credentials**: Valid database credentials for production access

## Setup

### 1. Create Production Environment File

Run the setup script to create the `.env_production` file from the template:

```bash
cd etl_pipeline
pipenv run python scripts/setup_production_env.py
```

This will:
- Copy `.env_production.template` to `.env_production`
- Provide instructions for configuration

### 2. Configure Production Environment

Edit the `.env_production` file and configure the following:

```bash
# Required: Set environment to production
ETL_ENVIRONMENT=production

# Production OpenDental Source Database (Read Only)
OPENDENTAL_SOURCE_HOST=your-production-host
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=your_readonly_user
OPENDENTAL_SOURCE_PASSWORD=your_password

# MySQL Replication Database (if needed)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3305
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=replication_user
MYSQL_REPLICATION_PASSWORD=your_password

# PostgreSQL Analytics Database (if needed)
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_SCHEMA=raw
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=your_password
```

### 3. Test Configuration

Test your configuration by running a simple integration test:

```bash
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/test_schema_analysis_integration.py::TestSchemaAnalysisIntegration::test_production_table_schema_analysis -v
```

## Running Production Integration Tests

### Run All Production Integration Tests

```bash
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/ -v -m production
```

### Run Specific Test Categories

```bash
# Schema analysis tests
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/test_schema_analysis_integration.py -v

# Incremental strategy tests
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/test_incremental_strategy_integration.py -v

# Data quality tests
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/test_data_quality_integration.py -v
```

### Run Tests with Specific Markers

```bash
# Run only production tests
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/ -v -m production

# Run only integration tests
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/ -v -m integration

# Run critical ETL tests
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/ -v -m etl_critical
```

## Test Categories

### Schema Analysis Tests
- **Purpose**: Validate real production schema analysis
- **Tests**: Table schema extraction, column information, primary key detection
- **Files**: `test_schema_analysis_integration.py`

### Incremental Strategy Tests
- **Purpose**: Validate incremental column discovery and strategy determination
- **Tests**: Timestamp column discovery, strategy selection, data quality validation
- **Files**: `test_incremental_strategy_integration.py`

### Data Quality Tests
- **Purpose**: Validate data quality analysis on production data
- **Tests**: Data quality metrics, outlier detection, validation rules
- **Files**: `test_data_quality_integration.py`

### Configuration Generation Tests
- **Purpose**: Validate ETL configuration generation from production schema
- **Tests**: Configuration file generation, table configuration, pipeline settings
- **Files**: `test_configuration_generation_integration.py`

## Environment Management

### Fixtures Used

The production integration tests use the following fixtures:

- `production_settings_with_file_provider`: Loads production environment from `.env_production`
- `load_production_environment_file`: Loads environment variables from `.env_production`
- `reset_global_settings`: Ensures clean settings state between tests

### Environment Variables

The tests expect the following environment variables to be set in `.env_production`:

```bash
# Required
ETL_ENVIRONMENT=production

# Production Database Connections
OPENDENTAL_SOURCE_HOST=your-host
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=your-user
OPENDENTAL_SOURCE_PASSWORD=your-password

# Optional (for full integration testing)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3305
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=repl_user
MYSQL_REPLICATION_PASSWORD=your-password

POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_SCHEMA=raw
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=your-password
```

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check database host, port, and credentials
2. **Missing Environment File**: Run the setup script to create `.env_production`
3. **Permission Denied**: Ensure readonly access to production database
4. **Network Issues**: Verify network connectivity to production server

### Debug Information

The tests provide debug output showing:
- Environment variables loaded
- Database connection attempts
- Schema discovery results
- Test execution details

### Skipping Tests

If production database is not available, tests will be skipped with appropriate messages:

```bash
pipenv run python -m pytest tests/integration/scripts/analyze_opendental_schema/ -v -k "not production"
```

## Security Considerations

1. **Never commit `.env_production`**: The file contains sensitive database credentials
2. **Readonly Access**: Use readonly database user for production tests
3. **Network Security**: Ensure secure network access to production database
4. **Credential Management**: Use secure methods to manage database credentials

## File Structure

```
etl_pipeline/
├── .env_production.template          # Template for production environment
├── .env_production                   # Production environment (created by user)
├── scripts/
│   └── setup_production_env.py      # Setup script for production environment
├── tests/
│   └── integration/
│       └── scripts/
│           └── analyze_opendental_schema/
│               ├── test_schema_analysis_integration.py
│               ├── test_incremental_strategy_integration.py
│               ├── test_data_quality_integration.py
│               └── ...
└── docs/
    └── production_integration_tests.md  # This file
``` 