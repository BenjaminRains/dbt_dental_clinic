# ELT Pipeline Data Flow and Naming Conventions

## Clear Data Flow Architecture

```
┌─────────────────────┐    ┌─────────────────────┐     ┌────────────────────── ┐
│   Source MySQL      │    │   Replication MySQL │     │   Analytics PostgreSQL│
│   (OpenDental)      │───▶│   (Local Copy)      │───▶│   (Raw Schema)        │
│                     │    │                     │     │                       │
│ - opendental        │    │ - opendental_repl   │     │ - opendental_analytics│
│ - Read-only access  │    │ - Full read/write   │     │ - raw schema          │
│ - Port 3306         │    │ - Port 3305         │     │ - Port 5432           │
└─────────────────────┘    └─────────────────────┘     └────────────────────── ┘
                                    │                           │
                                    │                           │
                                    ▼                           ▼
                            ExactMySQLReplicator         PostgresLoader
                            (Extract Phase)              (Load Phase)
                            - Exact replication          - Type conversion
                            - Schema discovery           - Data extraction
                            - Incremental tracking       - Bulk loading
```

## ETL Pipeline Phases

### Extract Phase (ExactMySQLReplicator)
- **Source**: OpenDental MySQL (`opendental`)
- **Target**: Replication MySQL (`opendental_replication`)
- **Method**: Exact MySQL replication using `ExactMySQLReplicator`
- **Tables**: `patient`, `appointment`, `treatment` (no suffixes)
- **Purpose**: Create local copy for ETL processing

### Load Phase (PostgresLoader)
- **Source**: Replication MySQL (`opendental_replication`)
- **Target**: PostgreSQL raw schema (`opendental_analytics.raw`)
- **Method**: Intelligent type conversion using `PostgresSchema`
- **Transformation**: MySQL → PostgreSQL type mapping with data analysis
- **Purpose**: Convert data types and load to analytics database

### Transform Phase (dbt)
- **Source**: PostgreSQL raw schema (`opendental_analytics.raw`)
- **Target**: PostgreSQL staging/intermediate/marts schemas
- **Method**: dbt models and transformations
- **Transformation**: Business logic, data standardization, analytics preparation
- **Purpose**: Transform raw data into analytics-ready datasets

## Consistent Naming Strategy

### 1. Environment Variables
```bash
# Source Database (OpenDental Production)
OPENDENTAL_SOURCE_HOST=client-opendental-server
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=secure_password

# Replication Database (Local MySQL)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3305
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=replication_user
MYSQL_REPLICATION_PASSWORD=replication_password

# Analytics Database (PostgreSQL) - Single database with multiple schemas
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=analytics_password
```

### 2. Connection Factory Methods

The `ConnectionFactory` provides specific methods for each database and schema:

```python
# Source connections
get_opendental_source_connection()           # Source MySQL (OpenDental)

# Replication connections  
get_mysql_replication_connection()           # Replication MySQL (Local copy)

# Analytics connections (PostgreSQL with specific schemas)
get_opendental_analytics_raw_connection()    # Raw schema (ETL output)
get_opendental_analytics_staging_connection() # Staging schema (dbt staging)
get_opendental_analytics_intermediate_connection() # Intermediate schema (dbt intermediate)
get_opendental_analytics_marts_connection()  # Marts schema (dbt marts)
```

### 3. Pipeline Engine Variables
```python
class ELTPipeline:
    def __init__(self):
        # Clear, descriptive naming
        self.source_engine = None          # Source MySQL (OpenDental)
        self.replication_engine = None     # Replication MySQL (Local copy)
        self.raw_engine = None             # Analytics PostgreSQL (raw schema)
        
        # Database names
        self.source_db = "opendental"
        self.replication_db = "opendental_replication"
        self.analytics_db = "opendental_analytics"
        self.raw_schema = "raw"
```

## PostgreSQL Analytics Database Schema Structure

### Single Database with Multiple Schemas
```sql
-- PostgreSQL Database: opendental_analytics

-- Raw schema (ETL pipeline output - direct from replication)
raw.patient              -- Direct from opendental_replication
raw.appointment          -- Direct from opendental_replication
raw.treatment            -- Direct from opendental_replication

-- Staging schema (dbt models)
staging.stg_opendental__patient
staging.stg_opendental__appointment
staging.stg_opendental__treatment

-- Intermediate schema (dbt models)
intermediate.int_patient_demographics
intermediate.int_appointment_scheduling
intermediate.int_treatment_history

-- Marts schema (dbt models)
marts.dim_patient
marts.fact_appointments
marts.fact_treatments
```

### Schema-Specific Connection Methods

Each schema has its own connection method for clear separation:

```python
# Raw schema (ETL pipeline output)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Staging schema (dbt staging models)
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()

# Intermediate schema (dbt intermediate models)
intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_connection()

# Marts schema (dbt mart models)
marts_engine = ConnectionFactory.get_opendental_analytics_marts_connection()
```

## ETL Pipeline Components

### Extract Phase (ExactMySQLReplicator)
- **Purpose**: Create exact copy of source database
- **Transformation**: None - exact replication
- **Output**: `opendental_replication` database

### Load Phase (PostgresLoader)
- **Purpose**: Move data from MySQL to PostgreSQL with type conversion
- **Transformation**: MySQL → PostgreSQL type mapping using `PostgresSchema`
- **Output**: `opendental_analytics.raw` schema
- **Key Features**:
  - Intelligent type analysis (TINYINT → boolean detection)
  - Incremental loading support
  - Chunked processing for large tables
  - Schema validation and verification

### Transform Phase (dbt)
- **Purpose**: Transform raw data into analytics-ready datasets
- **Transformation**: Business logic, data standardization, analytics preparation
- **Output**: `opendental_analytics.staging`, `intermediate`, `marts` schemas
- **Key Features**:
  - Column name standardization (snake_case)
  - Data type conversions and validations
  - Business logic implementation
  - Analytics-ready data preparation

## Tracking Tables

### MySQL Replication (Extract tracking)
```sql
opendental_replication.etl_extract_status
- table_name
- last_extracted
- rows_extracted
- extraction_status
- schema_hash
```

### PostgreSQL Analytics (Load tracking)
```sql
raw.etl_load_status
- table_name
- last_loaded
- rows_loaded
- load_status
- created_at
- updated_at
```

### PostgreSQL Analytics (dbt tracking)
```sql
staging.etl_transform_status
- table_name
- transform_type
- rows_processed
- transform_time
- status
```

## Configuration Files

### 1. tables.yml (Source Analysis)
- **Purpose**: Generated from OpenDental source database analysis
- **Content**: Table metadata, incremental columns, dependencies, extraction strategy
- **Used by**: ETL pipeline for extraction and loading strategy

### 2. dbt Configuration
- **Purpose**: Business logic and analytics transformations
- **Content**: Staging, intermediate, and mart models
- **Used by**: dbt for business intelligence



## dbt Integration Points

### dbt Sources Configuration
```yaml
# models/staging/sources.yml
sources:
  - name: opendental
    description: "Raw OpenDental data from ETL pipeline"
    schema: raw
    tables:
      - name: patient
        description: "Raw patient data from OpenDental"
      - name: appointment
        description: "Raw appointment data from OpenDental"
      - name: treatment
        description: "Raw treatment data from OpenDental"
```

### dbt Model Naming
```sql
-- Staging models (source system prefix)
stg_opendental__patient.sql
stg_opendental__appointment.sql
stg_opendental__treatment.sql

-- Intermediate models (business concept)
int_patient_demographics.sql
int_appointment_scheduling.sql
int_treatment_history.sql

-- Mart models (business domain)
dim_patient.sql
fact_appointments.sql
fact_treatments.sql
```

## Configuration Updates Needed

### 1. Environment Variables
- Use `OPENDENTAL_SOURCE_*` for source OpenDental database
- Use `MYSQL_REPLICATION_*` for local MySQL database
- Use `POSTGRES_ANALYTICS_*` for PostgreSQL database
- Ensure consistency between `.env` and code

### 2. Connection Factory
- Use `get_opendental_source_connection()` for source OpenDental
- Use `get_mysql_replication_connection()` for local MySQL
- Use schema-specific methods for PostgreSQL analytics:
  - `get_opendental_analytics_raw_connection()` for raw schema
  - `get_opendental_analytics_staging_connection()` for staging schema
  - `get_opendental_analytics_intermediate_connection()` for intermediate schema
  - `get_opendental_analytics_marts_connection()` for marts schema
- Maintain clear separation of concerns

### 3. Pipeline Code
- Use clean table names in raw schema (no _raw suffix)
- Update variable names for clarity
- Align tracking table names with purpose
- Use appropriate schema-specific connections

### 4. Documentation
- Keep README.md updated with current architecture
- Document clear boundaries between ELT phases
- Maintain clear data flow diagrams
- Document schema-specific connection methods

## Connection Usage Examples

### ELT Pipeline Usage
```python
# Extract phase
source_engine = ConnectionFactory.get_opendental_source_connection()
replication_engine = ConnectionFactory.get_mysql_replication_connection()

# Load phase
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Transform phase (dbt)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()
```

### dbt Usage
```python
# Staging models (read from raw schema)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Staging models (write to staging schema)
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()

# Intermediate models
intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_connection()

# Mart models
marts_engine = ConnectionFactory.get_opendental_analytics_marts_connection()
```

### Testing Usage
```python
# Unit tests with mocking
mock_source = ConnectionFactory.get_opendental_source_connection()
mock_replication = ConnectionFactory.get_mysql_replication_connection()
mock_raw = ConnectionFactory.get_opendental_analytics_raw_connection()
mock_staging = ConnectionFactory.get_opendental_analytics_staging_connection()

# Integration tests with real databases
source_engine = ConnectionFactory.get_opendental_source_connection()
replication_engine = ConnectionFactory.get_mysql_replication_connection()
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()
```

## Benefits of This Approach

1. **Clear Separation**: Each database and schema has a distinct role
2. **Consistent Naming**: No confusion about data location
3. **Schema-Specific Connections**: Clear boundaries between different data layers
4. **Configuration-Driven**: Separate configs for different transformation phases
5. **dbt Integration**: Clean handoff from ELT to dbt with proper schema separation
6. **Maintainability**: Easy to understand and modify
7. **Monitoring**: Clear tracking at each stage
8. **Scalability**: Clear boundaries allow independent scaling
9. **Debuggability**: Clear data lineage and tracking
10. **Testability**: Easy to mock specific connections for testing

