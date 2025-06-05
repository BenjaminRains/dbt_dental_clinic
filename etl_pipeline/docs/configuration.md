# ETL Pipeline Configuration

This document describes the configuration system for the ETL pipeline.

## Overview

The configuration system consists of two main components:

1. `DatabaseConfig`: Handles database connection settings
2. `PipelineConfig`: Manages pipeline-wide settings and configurations

## Configuration Files

### pipeline.yaml

The main configuration file that contains all pipeline settings:

```yaml
general:
  environment: production
  version: 1.0.0

connections:
  source:
    schema: source_db
    charset: utf8mb4
    pool_size: 5
    max_overflow: 10
  staging:
    schema: staging
    charset: utf8mb4
    pool_size: 5
    max_overflow: 10
  target:
    schema: public
    charset: utf8
    pool_size: 5
    max_overflow: 10

stages:
  extract:
    enabled: true
    schedule: "0 * * * *"
    timeout_minutes: 30
  transform:
    enabled: true
    schedule: "5 * * * *"
    timeout_minutes: 45
  load:
    enabled: true
    schedule: "10 * * * *"
    timeout_minutes: 60

monitoring:
  enabled: true
  log_level: INFO
  alerts:
    email:
      enabled: true
      recipients:
        - admin@example.com
    slack:
      enabled: false
      webhook_url: https://hooks.slack.com/services/xxx

performance:
  batch_size: 10000
  max_workers: 4
  timeout_seconds: 3600

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: etl.log
```

### Environment Variables

Database connection settings are managed through environment variables:

#### Source Database (MySQL)
```
OPENDENTAL_SOURCE_HOST=localhost
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=source_db
OPENDENTAL_SOURCE_USER=source_user
OPENDENTAL_SOURCE_PW=source_pass
```

#### Staging Database (MySQL)
```
REPLICATION_MYSQL_HOST=localhost
REPLICATION_MYSQL_PORT=3306
REPLICATION_MYSQL_DB=replication_db
REPLICATION_MYSQL_USER=replication_user
REPLICATION_MYSQL_PASSWORD=replication_pass
```

#### Target Database (PostgreSQL)
```
ANALYTICS_POSTGRES_HOST=localhost
ANALYTICS_POSTGRES_PORT=5432
ANALYTICS_POSTGRES_DB=analytics_db
ANALYTICS_POSTGRES_USER=analytics_user
ANALYTICS_POSTGRES_PASSWORD=analytics_pass
ANALYTICS_POSTGRES_SCHEMA=public
```

## Configuration Classes

### DatabaseConfig

The `DatabaseConfig` class handles database connection settings:

```python
from etl_pipeline.config import DatabaseConfig

# Get source database configuration
source_config = DatabaseConfig.get_source_config()

# Get staging database configuration
staging_config = DatabaseConfig.get_staging_config()

# Get target database configuration
target_config = DatabaseConfig.get_target_config()

# Validate all configurations
is_valid = DatabaseConfig.validate_configs()
```

### PipelineConfig

The `PipelineConfig` class manages pipeline-wide settings:

```python
from etl_pipeline.config import PipelineConfig

# Get configuration instance (singleton)
config = PipelineConfig()

# Load configuration from file
config.load_config('path/to/pipeline.yaml')

# Access configuration sections
general_settings = config.general
connection_settings = config.connections
stage_settings = config.stages

# Get specific configurations
extract_config = config.get_stage_config('extract')
is_enabled = config.is_stage_enabled('extract')
schedule = config.get_stage_schedule('extract')
timeout = config.get_stage_timeout('extract')

# Get connection settings
source_config = config.get_connection_config('source')
monitoring_config = config.get_monitoring_config()
alert_config = config.get_alert_config('email')
logging_config = config.get_logging_config()
```

## Usage in Components

### Connection Factory

```python
from etl_pipeline.core.connections import ConnectionFactory

# Get database connections
source_conn = ConnectionFactory.get_source_connection()
staging_conn = ConnectionFactory.get_staging_connection()
target_conn = ConnectionFactory.get_target_connection()
```

### Postgres Loader

```python
from etl_pipeline.loaders.postgres_loader import PostgresLoader

# Create loader with connections
loader = PostgresLoader(source_conn, target_conn)

# Access schema configurations
target_schema = loader.target_schema
staging_schema = loader.staging_schema

# Access pipeline configuration
pipeline_config = loader.config
```

## Best Practices

1. **Environment Variables**
   - Use `.env` files for local development
   - Use environment variables in production
   - Never commit sensitive information to version control

2. **Configuration Files**
   - Keep configuration files in version control
   - Use different files for different environments
   - Document all configuration options

3. **Validation**
   - Always validate configurations before use
   - Provide meaningful error messages
   - Use type hints and default values

4. **Security**
   - Use read-only users where possible
   - Encrypt sensitive information
   - Follow principle of least privilege

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**
   - Check if `.env` file exists
   - Verify environment variable names
   - Ensure variables are loaded

2. **Invalid Configuration**
   - Check YAML syntax
   - Verify required sections
   - Validate data types

3. **Connection Issues**
   - Verify database credentials
   - Check network connectivity
   - Validate connection strings

### Debugging

1. Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

2. Check configuration loading:
```python
config = PipelineConfig()
print(config._config)
```

3. Verify environment variables:
```python
import os
print(os.environ.get('OPENDENTAL_SOURCE_HOST'))
``` 