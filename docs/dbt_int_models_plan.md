# STILL IN DEVELOPMENT. NOT CURRENTLY IMPLEMENTED

# Dental Practice DBT Project - Intermediate Models Plan

## Overview

This document outlines the intermediate models in our dental practice analytics DBT project. These models transform the staging tables into business-focused entities that align with our core business processes. The intermediate layer serves as a bridge between raw staging data and final analytics models.

## Relationship to Process Flow Systems

Our intermediate models are directly aligned with the seven key systems identified in our business process flow diagram (`mdc_process_flow_diagram.mmd`):

| Process Flow System | Intermediate Models |
|---------------------|---------------------|
| System A: Fee Processing & Verification | `int_procedure_complete` |
| System B: Insurance Processing | `int_insurance_coverage`, `int_claim_details` |
| System C: Payment Allocation & Reconciliation | `int_payment_allocated` |
| System D: AR Analysis | `int_account_aging` |
| System E: Collection Process | (Uses data from AR Analysis) |
| System F: Patient-Clinic Communications | `int_patient_communication` |
| System G: Scheduling & Referrals | `int_appointment_details` |

Additionally, we've created cross-system models that represent end-to-end business flows:
- `int_patient_financial`: Connects systems A, B, and C (procedures, insurance, payments)
- `int_treatment_journey`: Tracks the complete patient journey across all systems

For visualization of how these systems interconnect, please refer to `mdc_process_flow_diagram.md`.

## Model Structure

### Foundation Models

1. **`int_patient_profile`**
   - Base patient demographic information
   - Combines patient, patientlink, and zipcode tables
   - Primary patient attributes used by all downstream models

### System-Specific Models

2. **`int_appointment_details`** (System G)
   - Appointment information enhanced with type and provider details
   - Adds status descriptions for reporting
   - Supports scheduling analysis and operational metrics

3. **`int_procedure_complete`** (System A)
   - Comprehensive procedure data with codes, fees and clinical notes
   - Links procedures to fee schedules and appointments
   - Enables procedure analysis and production reporting

4. **`int_insurance_coverage`** (System B)
   - Patient insurance plan and carrier information
   - Tracks insurance verification status
   - Supports insurance eligibility reporting

5. **`int_claim_details`** (System B)
   - Detailed claim information with procedures and payments
   - Includes claim tracking status and aging
   - Enables insurance claim management and follow-up

6. **`int_payment_allocated`** (System C)
   - Payment data with allocation to procedures
   - Handles different payment split types
   - Supports financial reconciliation and payment analysis

7. **`int_account_aging`** (System D)
   - Aging buckets for accounts receivable (0-30, 31-60, 61-90, 91+)
   - Both patient-level and family-level aging metrics
   - Enables AR analysis and collections prioritization

8. **`int_patient_communication`** (System F)
   - All patient communications consolidated
   - Categorizes by type, mode, and delivery status
   - Supports communication analysis and patient engagement metrics

### Cross-System Models

9. **`int_patient_financial`**
   - Complete financial picture for each patient
   - Aggregates procedures, payments, insurance, and adjustments
   - Enables comprehensive financial analysis

10. **`int_treatment_journey`**
    - End-to-end patient treatment journey
    - Tracks flow from appointment through procedure, claim, and payment
    - Enables process analysis and bottleneck identification

## Implementation Approach

Each intermediate model follows a consistent structure:

1. **CTEs for Source Data**: Each model starts with CTEs that reference staging models
2. **Joining Logic**: CTEs are joined to create comprehensive entities
3. **Business Logic**: Status descriptors and calculated fields are added
4. **Tracking Fields**: Each model includes created_at and updated_at timestamps

## Testing Strategy

Our testing strategy is defined in the `schema.yml` file and includes:

### Data Quality Tests
- **Not Null Tests**: Critical fields like patient_id, procedure_id
- **Unique Tests**: Primary keys for each model
- **Relationship Tests**: Foreign key relationships between models

### Business Logic Tests
- **Accepted Values**: Ensuring status fields contain valid values
- **Freshness Tests**: Verifying data is current
- **Custom SQL Tests**: Special validation for complex business rules

### Example Tests

```yaml
models:
  - name: int_procedure_complete
    columns:
      - name: procedure_id
        tests:
          - unique
          - not_null
      - name: procedure_status
        tests:
          - accepted_values:
              values: ['C', 'EC', 'EO', 'R', 'TP']
  
  - name: int_payment_allocated
    columns:
      - name: payment_id
        tests:
          - relationships:
              to: ref('stg_opendental__payment')
              field: payment_id
      - name: split_amount
        tests:
          - custom_test_positive_amount
```

## Documentation

For each intermediate model, we maintain:

1. **Column Descriptions**: Field definitions in `schema.yml`
2. **Business Context**: Comments in SQL explaining purpose
3. **Lineage Documentation**: Source-to-target mappings
4. **Process Flow**: Relationship to `mdc_process_flow_diagram.mmd`

## Implementation Schedule

The recommended implementation order:

1. Foundation models: `int_patient_profile`, `int_procedure_complete`
2. Financial models: `int_insurance_coverage`, `int_claim_details`, `int_payment_allocated`
3. Operational models: `int_appointment_details`, `int_patient_communication`
4. Analysis models: `int_account_aging`
5. Cross-system models: `int_patient_financial`, `int_treatment_journey`

## Maintenance and Governance

- **Ownership**: Each model has a designated business owner
- **Refresh Frequency**: Most models refresh daily, AR models refresh hourly
- **Version Control**: All model changes tracked in git
- **Change Management**: Model changes require PR approval and documentation updates

## Downstream Usage

These intermediate models serve as the foundation for:

1. **Analytics Models**: Final reporting tables in the mart layer
2. **Dashboards**: Power BI/Tableau dashboards for operational monitoring
3. **Data Science**: Machine learning models for patient behavior and revenue prediction
4. **Operational Reports**: Daily/weekly operational reports for clinic staff

For more detailed information about the business processes these models support, please refer to the `mdc_process_flow_diagram.md` document.