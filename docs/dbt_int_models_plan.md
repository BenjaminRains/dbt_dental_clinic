# Dental Practice DBT Project - Intermediate Models Plan

## Overview

This document outlines the intermediate models in our dental practice analytics DBT project. 
These models transform the staging tables into business-focused entities that align with our 
core business processes. The intermediate layer serves as a bridge between raw staging data 
and final analytics models.

## Current Implementation Status

### Foundation Layer (✅ Implemented)
- `int_patient_profile`: Comprehensive patient information model combining demographics, status,
 relationships, and financial data
  - Includes detailed patient attributes and contact preferences
  - Tracks patient status and important dates
  - Contains financial flags and balance information
  - Fully documented with tests and metadata

### System A: Fee Processing & Verification (✅ Implemented)
- `int_procedure_complete`: Comprehensive procedure model with fee schedules and validation
  - Tracks procedure details, codes, and clinical information
  - Includes fee validation statuses and statistics
  - Supports fee verification workflow
  - Contains detailed tests for fee validation

- `int_adjustments`: Model for tracking and analyzing fee adjustments
  - Links adjustments to procedures and standard fees
  - Calculates adjusted fees and impact
  - Tracks various adjustment types and categories

- `int_fee_model`: Focused on fee processing and verification
  - Tracks applied vs. standard fees
  - Calculates fee variances and relationships
  - Integrates adjustment information
  - Provides fee statistics per procedure code

### System B: Insurance Processing (✅ Implemented)
- `int_claim_details`: Core insurance claim information model
  - Combines claim details with procedures
  - Tracks claim status and type
  - Includes billing and payment amounts
  - Contains detailed validation rules

- `int_claim_payments`: Detailed payment information for insurance claims
  - Preserves complete payment history
  - Tracks payment amounts and dates
  - Includes payment type and status
  - Links to claim procedures

- `int_claim_tracking`: Complete tracking history for insurance claims
  - Records status updates and notes
  - Tracks claim progression
  - Includes timestamp information
  - Maintains audit trail

### System C: Payment Allocation & Reconciliation (✅ Implemented)
- `int_payment_allocated`: Payment allocation model
  - Combines patient and insurance payments
  - Tracks payment splits and allocations
  - Includes detailed payment type information
  - Contains comprehensive validation rules

### Pending Implementation
- System D: AR Analysis
- System E: Collection Process
- System F: Patient-Clinic Communications
- System G: Scheduling & Referrals
- Cross-system models

## Implementation Approach

Each intermediate model follows a consistent structure:
1. **CTEs for Source Data**: Each model starts with CTEs that reference staging models
2. **Joining Logic**: CTEs are joined to create comprehensive entities
3. **Business Logic**: Status descriptors and calculated fields are added
4. **Tracking Fields**: Each model includes created_at and updated_at timestamps

## Testing Strategy

Our testing strategy includes:
- Data Quality Tests (not null, unique, relationships)
- Business Logic Tests (accepted values, custom validations)
- Performance Tests (row counts, data freshness)
- Custom Tests for specific business rules

## Documentation

Each implemented model includes:
- Detailed column descriptions
- Business context and rules
- Test coverage
- Metadata and ownership information
- Refresh frequency and data quality requirements

## Next Steps

1. Implement remaining system-specific models (D-G)
2. Develop cross-system models
3. Enhance existing models with additional business rules
4. Expand test coverage for edge cases
5. Improve documentation and lineage tracking

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