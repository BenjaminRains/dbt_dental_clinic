# OpenDental ETL Pipeline

A robust ETL pipeline for extracting data from OpenDental MySQL, replicating it locally, and loading it into a PostgreSQL analytics database.

## Architecture

The pipeline follows a three-database architecture with multiple schemas in the analytics database:

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────────────────────────────┐
│   Source MySQL      │    │   Replication MySQL │    │   Analytics PostgreSQL                       │
│   (OpenDental)      │───▶│   (Local Copy)      │───▶│   (Analytics)                               │
│                     │    │                     │    │                                              │
│ - opendental        │    │ - opendental_repl   │    │ - opendental_analytics                      │
│ - Read-only access  │    │ - Full read/write   │    │ - Schemas:                                  │
│ - Port 3306         │    │ - Port 3305         │    │   • raw (initial load)                      │
│                     │    │                     │    │   • public (base tables)                     │
│                     │    │                     │    │   • public_staging (dbt staging)            │
│                     │    │                     │    │   • public_intermediate (dbt intermediate)  │
│                     │    │                     │    │   • public_marts (dbt marts)                │
└─────────────────────┘    └─────────────────────┘    └─────────────────────────────────────────────┘
```

### Database Roles and Schemas

1. **Source MySQL (OpenDental)**
   - Production OpenDental database
   - Read-only access
   - Used for initial data extraction

2. **Replication MySQL**
   - Local MySQL database
   - Exact copy of source data
   - Used for staging and validation

3. **Analytics PostgreSQL**
   - Analytics data warehouse with multiple schemas:
     - `raw`: Initial landing zone for replicated data
     - `public`: Base tables with minimal transformations
     - `public_staging`: dbt staging models
     - `public_intermediate`: dbt intermediate models
     - `public_marts`: dbt final models for consumption

## Data Flow

1. **Extract Phase**
   - Reads from source OpenDental MySQL
   - Performs exact replication to local MySQL
   - Tracks extraction status and schema changes

2. **Load Phase**
   - Reads from replication MySQL
   - Applies basic type conversions
   - Loads to PostgreSQL `raw` schema
   - Tracks load status

3. **Transform Phase**
   - `raw` → `public`: Basic transformations and standardization
     - Type casting and normalization
     - Basic data cleaning
     - Table structure standardization
   - `public` → `public_staging`: dbt staging models
     - Source table definitions
     - Basic transformations
   - `public_staging` → `public_intermediate`: dbt intermediate models
     - Complex transformations
     - Business logic
   - `public_intermediate` → `public_marts`: dbt final models
     - Final presentation layer
     - Ready for consumption

## Monitoring

The pipeline includes built-in monitoring:

1. **Logging**
   - Detailed logs in `elt_pipeline.log`
   - Connection logs in `connection.log`

2. **Tracking Tables**
   - `etl_extract_status`: Tracks extraction status in replication MySQL
   - `etl_transform_status`: Tracks load status in PostgreSQL

3. **Metrics**
   - Extraction counts
   - Load counts
   - Processing times
   - Error rates

## Development

1. **Code Structure**
   ```
   etl_pipeline/
   ├── core/              # Core pipeline components
   ├── extractors/        # Data extraction logic
   ├── transformers/      # Data transformation logic
   ├── loaders/          # Data loading logic
   ├── monitoring/       # Monitoring and metrics
   ├── utils/            # Utility functions
   ├── tests/            # Test files
   └── docs/             # Documentation
   ```

2. **Adding New Tables**
   - Add table to source database
   - Pipeline will automatically detect and process
   - No configuration needed for standard tables

3. **Custom Transformations**
   - Add transformation logic in `transformers/`
   - Update tracking tables as needed
   - Follow existing patterns for consistency

## Schema Management

1. **Raw Schema**
   - Initial landing zone for replicated data
   - Maintains exact structure from source
   - Minimal transformations
   - Used for data validation and debugging

2. **Public Schema**
   - Base tables for analytics
   - Standardized data types
   - Cleaned and normalized data
   - Foundation for dbt models

3. **dbt Schemas**
   - `public_staging`: Source definitions and basic transformations
   - `public_intermediate`: Complex transformations and business logic
   - `public_marts`: Final presentation layer

## Setup

1. **Environment Configuration**

   Copy the template environment file and update with your settings:
   ```bash
   cp .env.template .env
   ```

   Required environment variables:
   ```bash
   # Source Database (OpenDental Production)
   SOURCE_MYSQL_HOST=client-server
   SOURCE_MYSQL_PORT=3306
   SOURCE_MYSQL_DB=opendental
   SOURCE_MYSQL_USER=readonly_user
   SOURCE_MYSQL_PASSWORD=your_password

   # Replication Database (Local MySQL)
   REPLICATION_MYSQL_HOST=localhost
   REPLICATION_MYSQL_PORT=3305
   REPLICATION_MYSQL_DB=opendental_replication
   REPLICATION_MYSQL_USER=replication_user
   REPLICATION_MYSQL_PASSWORD=your_password

   # Analytics Database (PostgreSQL)
   ANALYTICS_POSTGRES_HOST=localhost
   ANALYTICS_POSTGRES_PORT=5432
   ANALYTICS_POSTGRES_DB=opendental_analytics
   ANALYTICS_POSTGRES_SCHEMA=raw
   ANALYTICS_POSTGRES_USER=analytics_user
   ANALYTICS_POSTGRES_PASSWORD=your_password
   ```

2. **Database Setup**

   Create the replication and analytics databases:
   ```sql
   -- MySQL Replication Database
   CREATE DATABASE opendental_replication;
   GRANT ALL PRIVILEGES ON opendental_replication.* TO 'replication_user'@'localhost';
   FLUSH PRIVILEGES;

   -- PostgreSQL Analytics Database
   CREATE DATABASE opendental_analytics;
   
   -- Create schemas
   CREATE SCHEMA raw;
   CREATE SCHEMA public;
   CREATE SCHEMA public_staging;
   CREATE SCHEMA public_intermediate;
   CREATE SCHEMA public_marts;
   
   -- Grant permissions
   GRANT ALL PRIVILEGES ON DATABASE opendental_analytics TO analytics_user;
   GRANT ALL PRIVILEGES ON SCHEMA raw TO analytics_user;
   GRANT ALL PRIVILEGES ON SCHEMA public TO analytics_user;
   GRANT ALL PRIVILEGES ON SCHEMA public_staging TO analytics_user;
   GRANT ALL PRIVILEGES ON SCHEMA public_intermediate TO analytics_user;
   GRANT ALL PRIVILEGES ON SCHEMA public_marts TO analytics_user;
   ```

3. **Install Dependencies**

   Using pipenv:
   ```bash
   pipenv install
   ```

## Usage

1. **Run Full Pipeline**

   ```bash
   python -m etl_pipeline
   ```

2. **Process Specific Tables**

   ```python
   from etl_pipeline.elt_pipeline import ELTPipeline

   pipeline = ELTPipeline()
   pipeline.run_elt_pipeline('patient')
   pipeline.run_elt_pipeline('appointment')
   ```

3. **Force Full Sync**

   ```python
   pipeline.run_elt_pipeline('patient', force_full=True)
   ```

## Troubleshooting

1. **Connection Issues**
   - Check environment variables
   - Verify database permissions
   - Check network connectivity

2. **Data Issues**
   - Check extraction status in `etl_extract_status`
   - Check load status in `etl_transform_status`
   - Review logs for specific errors

3. **Performance Issues**
   - Monitor connection pool settings
   - Check batch sizes
   - Review database indexes

## Contributing

1. Follow the existing code style
2. Add tests for new features
3. Update documentation
4. Submit pull requests

## License

MIT License - See LICENSE file for details 