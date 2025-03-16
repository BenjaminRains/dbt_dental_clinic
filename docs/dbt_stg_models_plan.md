# Dental Practice DBT Project - Staging Models Plan

## Overview

This document outlines the staging models in our dental practice analytics DBT project. The staging 
layer serves as the foundation of our data model, performing initial cleaning and standardization 
of raw OpenDental source data. All staging models enforce consistent naming conventions, data 
types, and basic data quality rules.

## Data Validation Scope

### Temporal Scope
- **Valid Data Period**: All staging models filter for records from 2023-01-01 onwards
  - This aligns with current business ownership and standardized data entry practices
  - Historical data before 2023 is excluded due to inconsistent data entry practices

### Source Systems
- **Primary Source**: OpenDental Database
- **Database Version**: 24.3.35.0
- **Program Version**: 24.3.36.0
- **MySQL Version**: 5.5
- **Storage Engine**: MyISAM
- **Database Name**: opendental
- **Server Location**: 192.168.2.10
- **Update Frequency**: Daily incremental updates
- **Language/Region**: en-US

### System Configuration
- **Clinics**: Single clinic setup (EasyNoClinics: 1)
- **Patient Volume**: ~32,780 patients
- **Data Integrity**: No database corruption reported (CorruptedDatabase: 0)
- **Primary Keys**: Sequential (RandomPrimaryKeys: 0)

## Staging Models Structure

### Naming Conventions
- All staging models prefixed with `stg_`
- Source-specific prefix: `stg_opendental__`
- File location: `models/staging/opendental/`

### Standard Transformations
All staging models implement these standard transformations:
1. Convert 0 values to NULL for ID/reference fields
2. Standardize boolean fields to true/false
3. Consistent date/timestamp formats
4. Standardized column naming (snake_case)
5. Clear categorization of fields (keys, dates, amounts, etc.)

## Core Staging Models

### 1. `stg_adjustment`
- **Purpose**: Standardize financial adjustments data
- **Key Transformations**:
  - Categorize adjustment directions (positive/negative/zero)
  - Convert zero IDs to NULL
  - Boolean flag for procedure adjustments
- **Primary Key**: adjustment_id
- **Critical Fields**:
  - adjustment_amount
  - adjustment_date
  - patient_id

[Continue with similar sections for other core staging models...]

## Testing Strategy

### Standard Tests for All Staging Models
1. **Primary Key Tests**
   - Uniqueness
   - Not null constraints

2. **Date Range Tests**
   ```yaml
   - name: date_after_2022
     description: Ensures dates are after 2022-01-01
     tests:
       - custom_date_range_check:
           field: adjustment_date
           min_date: '2022-01-01'
   ```

3. **Foreign Key Tests**
   - Relationship validation to other staging models
   - Null checks for optional relationships

4. **Data Type Tests**
   - Amount fields are numeric
   - Date fields are valid dates
   - Boolean fields are true/false

### Model-Specific Tests

#### stg_adjustment
```yaml
models:
  - name: stg_adjustment
    columns:
      - name: adjustment_id
        tests:
          - unique
          - not_null
      - name: adjustment_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: -100000  # Adjust based on business rules
              max_value: 100000
      - name: adjustment_direction
        tests:
          - accepted_values:
              values: ['positive', 'negative', 'zero']
```

[Continue with other model-specific tests...]

## Data Quality Monitoring

### Freshness Checks
```yaml
sources:
  - name: opendental
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
```

### Volume Checks
- Daily record count validation
- Anomaly detection for sudden changes in volume

### Value Distribution Monitoring
- Monitor key metrics distribution
- Alert on significant statistical deviations

## Implementation Approach

### Phase 1: Core Financial Models
1. stg_adjustment
2. stg_payment
3. stg_procedure
4. stg_claim

### Phase 2: Patient/Provider Models
1. stg_patient
2. stg_provider
3. stg_clinic

### Phase 3: Operational Models
1. stg_appointment
2. stg_schedule
3. stg_communication

## Documentation Requirements

Each staging model must include:
1. Column-level descriptions
2. Source-to-target mappings
3. Business rules documentation
4. Data quality thresholds
5. Known limitations or exceptions

## Maintenance and Governance

### Version Control
- All changes tracked in git
- PR review required for changes
- Documentation updates mandatory

### Monitoring
- Daily data quality checks
- Weekly volume trend analysis
- Monthly completeness audit

## Downstream Dependencies

These staging models support:
1. Intermediate models as defined in `dbt_int_models_plan.md`
2. Direct operational reporting needs
3. Data quality monitoring dashboards

For detailed process flows, refer to `mdc_process_flow_diagram.md`.
