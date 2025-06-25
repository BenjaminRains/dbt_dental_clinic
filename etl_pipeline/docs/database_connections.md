# Database Connections Reference

This document explains the database connection naming scheme used in the ETL pipeline configuration.

## Connection Naming Scheme

The pipeline uses specific, descriptive connection names that clearly indicate which database and schema they refer to:

### MySQL Connections

| Connection Name | Database | Purpose | Environment Variables |
|----------------|----------|---------|----------------------|
| `opendental_source` | OpenDental Production | Source data extraction | `OPENDENTAL_SOURCE_*` |
| `opendental_replication` | OpenDental Replication | ETL processing | `MYSQL_REPLICATION_*` |

### PostgreSQL Connections (opendental_analytics database)

| Connection Name | Schema | Purpose | dbt Models |
|----------------|--------|---------|------------|
| `opendental_analytics_raw` | `raw` | Raw ingested data | `models/raw/` |
| `opendental_analytics_staging` | `public_staging` | Staging transformations | `models/staging/` |
| `opendental_analytics_intermediate` | `public_intermediate` | Intermediate business logic | `models/intermediate/` |
| `opendental_analytics_marts` | `public_marts` | Business marts | `models/marts/` |
| `opendental_analytics_public` | `public` | Final transformed data | `models/public/` |

**Note**: All PostgreSQL connections point to the **same database** (`opendental_analytics`) but use different **schemas** within that database.

## Legacy Compatibility

For backward compatibility, the following legacy names are still supported:

| Legacy Name | Maps To | Notes |
|-------------|---------|-------|
| `source` | `opendental_source` | OpenDental production database |
| `staging` | `opendental_replication` | Actually the replication database, not staging |
| `target` | `opendental_analytics_public` | Default analytics schema |

## Usage Examples

### In Code
```python
from etl_pipeline.config.settings import Settings

settings = Settings()

# Get specific connection configs
source_config = settings.get_database_config('opendental_source')
replication_config = settings.get_database_config('opendental_replication')
raw_config = settings.get_database_config('opendental_analytics_raw')
staging_config = settings.get_database_config('opendental_analytics_staging')

# Get connection strings
source_conn_str = settings.get_connection_string('opendental_source')
analytics_public_conn_str = settings.get_connection_string('opendental_analytics_public')
```

### In Pipeline Configuration
```yaml
connections:
  opendental_source:
    type: "mysql"
    pool_size: 5
    connect_timeout: 60
    
  opendental_analytics_public:
    type: "postgresql"
    pool_size: 5
    schema: "public"
```

## Environment Variables

### OpenDental Source Database
```bash
OPENDENTAL_SOURCE_HOST=localhost
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=source_user
OPENDENTAL_SOURCE_PASSWORD=source_pass
```

### OpenDental Replication Database
```bash
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3306
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=replication_user
MYSQL_REPLICATION_PASSWORD=replication_pass
```

### Analytics Database (PostgreSQL)
```bash
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_SCHEMA=public
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=analytics_pass
```

## Database Structure

```
OpenDental Production (opendental_source)
    ↓
OpenDental Replication (opendental_replication)
    ↓
PostgreSQL Database: opendental_analytics
    ├── Schema: raw
    ├── Schema: public_staging
    ├── Schema: public_intermediate
    ├── Schema: public_marts
    └── Schema: public
```

## Migration from Legacy Names

If you're updating existing code, replace:

- `source` → `opendental_source`
- `staging` → `opendental_replication` (note: this was confusing!)
- `target` → `opendental_analytics_public`

The legacy names will continue to work for backward compatibility, but using the new specific names is recommended for clarity. 