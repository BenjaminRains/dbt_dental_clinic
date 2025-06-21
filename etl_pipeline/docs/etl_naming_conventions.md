# ELT Pipeline Data Flow and Naming Conventions

## Clear Data Flow Architecture

```
┌─────────────────────┐    ┌─────────────────────┐     ┌────────────────────── ┐
│   Source MySQL      │    │   Replication MySQL │     │   Analytics PostgreSQL│
│   (OpenDental)      │───▶│   (Local Copy)      │───▶│   (Analytics)         │
│                     │    │                     │     │                       │
│ - opendental        │    │ - opendental_repl   │     │ - opendental_analytics│
│ - Read-only access  │    │ - Full read/write   │     │ - Analytics warehouse │
│ - Port 3306         │    │ - Port 3305         │     │ - Port 5432           │
└─────────────────────┘    └─────────────────────┘     └────────────────────── ┘
```

## Consistent Naming Strategy

### 1. Environment Variables
```bash
# Source Database (OpenDental Production)
SOURCE_MYSQL_HOST=client-opendental-server
SOURCE_MYSQL_PORT=3306
SOURCE_MYSQL_DB=opendental
SOURCE_MYSQL_USER=readonly_user
SOURCE_MYSQL_PASSWORD=secure_password

# Replication Database (Local MySQL)
REPLICATION_MYSQL_HOST=localhost
REPLICATION_MYSQL_PORT=3305
REPLICATION_MYSQL_DB=opendental_replication
REPLICATION_MYSQL_USER=replication_user
REPLICATION_MYSQL_PASSWORD=replication_password

# Analytics Database (PostgreSQL)
ANALYTICS_POSTGRES_HOST=localhost
ANALYTICS_POSTGRES_PORT=5432
ANALYTICS_POSTGRES_DB=opendental_analytics
ANALYTICS_POSTGRES_SCHEMA=raw
ANALYTICS_POSTGRES_USER=analytics_user
ANALYTICS_POSTGRES_PASSWORD=analytics_password
```

### 2. Connection Factory Functions
```python
# New clear naming
get_source_connection()      # Source MySQL (OpenDental)
get_replication_connection() # Replication MySQL (Local copy)
get_analytics_connection()   # Analytics PostgreSQL
```

### 3. Pipeline Engine Variables
```python
class ELTPipeline:
    def __init__(self):
        # Clear, descriptive naming
        self.source_engine = None          # Source MySQL (OpenDental)
        self.replication_engine = None     # Replication MySQL (Local copy)
        self.analytics_engine = None       # Analytics PostgreSQL
        
        # Database names
        self.source_db = "opendental"
        self.replication_db = "opendental_replication"
        self.analytics_db = "opendental_analytics"
        self.analytics_schema = "raw"
```

## Schema and Table Naming in PostgreSQL

### Current Structure
```sql
-- PostgreSQL Database: opendental_analytics

-- Raw schema (ELT pipeline output)
raw.patient              -- Direct from replication MySQL
raw.appointment          -- Direct from replication MySQL
raw.treatment            -- Direct from replication MySQL

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
- Use `REPLICATION_MYSQL_*` for local MySQL database
- Use `ANALYTICS_POSTGRES_*` for PostgreSQL database
- Ensure consistency between `.env` and code

### 2. Connection Factory
- Use `get_replication_connection()` for local MySQL
- Use `get_analytics_connection()` for PostgreSQL
- Maintain clear separation of concerns

### 3. Pipeline Code
- Use clean table names in raw schema (no _raw suffix)
- Update variable names for clarity
- Align tracking table names with purpose

### 4. Documentation
- Keep README.md updated with current architecture
- Document clear boundaries between ELT and dbt
- Maintain clear data flow diagrams

## Benefits of This Approach

1. **Clear Separation**: Each database has a distinct role
2. **Consistent Naming**: No confusion about data location
3. **dbt Integration**: Clean handoff from ELT to dbt
4. **Maintainability**: Easy to understand and modify
5. **Monitoring**: Clear tracking at each stage
6. **Scalability**: Clear boundaries allow independent scaling
7. **Debuggability**: Clear data lineage and tracking