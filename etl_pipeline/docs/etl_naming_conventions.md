# ELT Pipeline Data Flow and Naming Conventions

## Clear Data Flow Architecture

```
┌─────────────────────┐    ┌─────────────────────┐     ┌────────────────────── ┐
│   Source MySQL      │    │   Replication MySQL │     │   Analytics PostgreSQL│
│   (OpenDental)      │───▶│   (Local Copy)      │───▶│   (Analytics)         │
│                     │    │                     │     │                       │
│ - opendental        │    │ - opendental_repl   │     │ - opendental_analytics│
│ - Read-only access  │    │ - Full read/write   │     │ - Multiple schemas    │
│ - Port 3306         │    │ - Port 3305         │     │ - Port 5432           │
└─────────────────────┘    └─────────────────────┘     └────────────────────── ┘
```

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
POSTGRES_ANALYTICS_SCHEMA=raw
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
get_postgres_analytics_connection()          # Default: raw schema
get_opendental_analytics_raw_connection()    # Explicit: raw schema
get_opendental_analytics_public_connection() # Explicit: public schema
get_opendental_analytics_staging_connection() # Explicit: public_staging schema
get_opendental_analytics_intermediate_connection() # Explicit: public_intermediate schema
get_opendental_analytics_marts_connection()  # Explicit: public_marts schema
```

### 3. Pipeline Engine Variables
```python
class ELTPipeline:
    def __init__(self):
        # Clear, descriptive naming
        self.source_engine = None          # Source MySQL (OpenDental)
        self.replication_engine = None     # Replication MySQL (Local copy)
        self.analytics_engine = None       # Analytics PostgreSQL (raw schema)
        
        # Database names
        self.source_db = "opendental"
        self.replication_db = "opendental_replication"
        self.analytics_db = "opendental_analytics"
        self.analytics_schema = "raw"
```

## PostgreSQL Analytics Database Schema Structure

### Single Database with Multiple Schemas
```sql
-- PostgreSQL Database: opendental_analytics

-- Raw schema (ELT pipeline output)
raw.patient              -- Direct from replication MySQL
raw.appointment          -- Direct from replication MySQL
raw.treatment            -- Direct from replication MySQL

-- Public schema (general purpose)
public.general_tables    -- Any general purpose tables

-- Staging schema (dbt models)
public_staging.stg_opendental__patient
public_staging.stg_opendental__appointment
public_staging.stg_opendental__treatment

-- Intermediate schema (dbt models)
public_intermediate.int_patient_demographics
public_intermediate.int_appointment_scheduling
public_intermediate.int_treatment_history

-- Marts schema (dbt models)
public_marts.dim_patient
public_marts.fact_appointments
public_marts.fact_treatments
```

### Schema-Specific Connection Methods

Each schema has its own connection method for clear separation:

```python
# Raw schema (ELT pipeline output)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Public schema (general purpose)
public_engine = ConnectionFactory.get_opendental_analytics_public_connection()

# Staging schema (dbt staging models)
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()

# Intermediate schema (dbt intermediate models)
intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_connection()

# Marts schema (dbt mart models)
marts_engine = ConnectionFactory.get_opendental_analytics_marts_connection()
```

## ELT Pipeline Phases

### Extract Phase
- **Source**: OpenDental MySQL (`opendental`)
- **Target**: Replication MySQL (`opendental_replication`)
- **Method**: Exact MySQL replication using `ExactMySQLReplicator`
- **Tables**: `patient`, `appointment`, `treatment` (no suffixes)

### Load Phase
- **Source**: Replication MySQL (`opendental_replication`)
- **Target**: PostgreSQL raw schema (`raw.patient`, `raw.appointment`)
- **Method**: Basic type conversion and data cleaning
- **No business logic**: Only technical transformations for PostgreSQL compatibility

### Transform Phase (Basic)
- **Scope**: Data type conversions, NULL handling, PostgreSQL compatibility
- **No business transformations**: Leave complex logic for dbt
- **Output**: Clean raw tables ready for dbt consumption

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
raw.etl_transform_status
- table_name
- last_transformed
- rows_transformed
- transformation_status
- created_at
- updated_at
```

## dbt Integration Points

### dbt Sources Configuration
```yaml
# models/staging/sources.yml
sources:
  - name: opendental_raw
    description: "Raw OpenDental data from ELT pipeline"
    schema: raw
    tables:
      - name: patient
        description: "Raw patient data"
      - name: appointment
        description: "Raw appointment data"
      - name: treatment
        description: "Raw treatment data"
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
- Document clear boundaries between ELT and dbt
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

# Transform phase (if needed)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()
```

### dbt Usage
```python
# Staging models
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

# Integration tests with real databases
source_engine = ConnectionFactory.get_opendental_source_connection()
replication_engine = ConnectionFactory.get_mysql_replication_connection()
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()
```

## Benefits of This Approach

1. **Clear Separation**: Each database and schema has a distinct role
2. **Consistent Naming**: No confusion about data location
3. **Schema-Specific Connections**: Clear boundaries between different data layers
4. **dbt Integration**: Clean handoff from ELT to dbt with proper schema separation
5. **Maintainability**: Easy to understand and modify
6. **Monitoring**: Clear tracking at each stage
7. **Scalability**: Clear boundaries allow independent scaling
8. **Debuggability**: Clear data lineage and tracking
9. **Testability**: Easy to mock specific connections for testing