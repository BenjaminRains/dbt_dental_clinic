## Layer Descriptions

### 1. Staging Layer (`models/staging/`)
- **Purpose**: Initial transformation of raw OpenDental data
- **Conventions**:
  - One-to-one mapping with source tables
  - Minimal transformations (cleaning, typing, naming)
  - Prefixed with `stg_`
- **Key Functions**:
  - Column renaming for consistency
  - Data type casting
  - Basic data cleaning
  - Documentation of source columns

Example:
```sql
-- models/staging/opendental/patient/stg_patient.sql
select
    PatNum as patient_id,
    LName as last_name,
    FName as first_name,
    -- additional columns
from {{ source('opendental', 'patient') }}
```

### 2. Intermediate Layer (`models/intermediate/`)
- **Purpose**: Business logic implementation
- **Organization**: Aligned with major business systems
  - System A: Fee Processing & Verification
  - System B: Insurance Processing
  - System C: Payment Allocation
  - System D: AR Analysis
  - System E: Collection Process
- **Naming**: Prefixed with `int_`
- **Key Functions**:
  - Complex transformations
  - Business rule application
  - Data enrichment
  - Relationship modeling

### 3. Marts Layer (`models/marts/`)
- **Purpose**: Business-facing data models
- **Structure**:
  - `core/`: Foundation dimensional models
    - `dimensions/`: Core business entities
    - `facts/`: Core business events/transactions
  - `reporting/`: Business-specific reporting models
- **Key Functions**:
  - Dimensional modeling
  - Metric calculations
  - Report-ready views
  - Business domain aggregations

## Model Materialization Strategy

1. **Staging Models**
   - Materialized as: `view`
   - Schema: `staging`
   - Refresh: On-demand

2. **Intermediate Models**
   - Materialized as: `table`
   - Schema: `intermediate`
   - Refresh: Daily

3. **Marts Models**
   - Materialized as: `table`
   - Schema: `marts`
   - Refresh: Daily

## Testing Strategy

1. **Generic Tests**
   - Unique keys
   - Not null values
   - Referential integrity
   - Accepted values

2. **Custom Tests**
   - Business logic validation
   - Data quality checks
   - Relationship verification

## Documentation Standards

1. **Model Documentation**
   - Purpose of model
   - Business definitions
   - Source references
   - Column descriptions

2. **Business Logic Documentation**
   - Transformation rules
   - Business assumptions
   - Calculation methods

## Usage

### Development
```bash
# Install dependencies
dbt deps

# Run models
dbt run

# Test models
dbt test

# Generate docs
dbt docs generate
```

### Model Selection
```bash
# Run specific models
dbt run --select staging.opendental.patient
dbt run --select marts.core

# Run models and dependencies
dbt run --select +marts.reporting.financial
```

## Related Resources
- [OpenDental Schema Documentation](../docs/opendental_db_schemas/README.md)
- [Data Flow Diagram](../docs/data_flow_diagram.md)
- [DBT Best Practices](https://docs.getdbt.com/guides/best-practices)