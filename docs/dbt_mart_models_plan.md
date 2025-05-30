# Dental Practice DBT Project - Mart Models Plan

## Overview

This document outlines the strategy for our mart layer models, which will transform our intermediate
 models into business-focused analytics views. The mart layer serves as the final transformation 
 layer before data reaches end users through dashboards and reports.

## List of Mart Models

- mart_financial_performance
- mart_insurance_performance
- mart_procedure_analytics
- mart_scheduling_analytics
- mart_patient_engagement
- mart_patient_financial
- mart_operational_metrics
- mart_quality_metrics
- mart_hygiene_retention
- mart_ar_summary
- mart_ar_aging
- mart_patient_refunds
- mart_revenue_lost
- mart_patient_growth
- mart_patient_retention
- mart_new_patients
- mart_active_patients
- mart_patient_source
- mart_patient_cohort

## Dimensional Model Structure

### Dimension Tables
1. `dim_date`
   - Calendar attributes
   - Fiscal periods
   - Holiday flags
   - Business day indicators

2. `dim_patient`
   - Patient demographics
   - Status information
   - Financial attributes
   - Contact preferences

3. `dim_provider`
   - Provider details
   - Specialties
   - Fee schedules
   - Productivity metrics

4. `dim_procedure`
   - Procedure codes
   - Categories
   - Standard fees
   - Clinical attributes

5. `dim_insurance`
   - Insurance carrier details
   - Plan types
   - Coverage rules
   - Network status

6. `dim_location`
   - Clinic locations
   - Room/chair details
   - Equipment information
   - Capacity metrics

### Fact Tables

1. `fact_procedure`
   - Procedure execution details
   - Actual vs. standard fees
   - Provider assignments
   - Time and duration
   - Links to: dim_date, dim_patient, dim_provider, dim_procedure

2. `fact_payment`
   - Payment transactions
   - Allocation details
   - Payment types
   - Collection status
   - Links to: dim_date, dim_patient, dim_procedure, dim_insurance

3. `fact_claim`
   - Insurance claim details
   - Submission status
   - Payment information
   - Processing metrics
   - Links to: dim_date, dim_patient, dim_procedure, dim_insurance

4. `fact_appointment`
   - Appointment details
   - Status tracking
   - No-show information
   - Duration metrics
   - Links to: dim_date, dim_patient, dim_provider, dim_procedure

5. `fact_communication`
   - Communication events
   - Channel information
   - Response tracking
   - Outcome metrics
   - Links to: dim_date, dim_patient, dim_provider

## Mart Layer Structure

### 1. Financial Analytics Mart
**Purpose**: Comprehensive financial performance analysis
**Key Models**:
- `mart_financial_performance`
  - Revenue by procedure, provider, time period
  - Fee variance analysis
  - Payment collection rates
  - AR aging trends
  - Collection effectiveness
  - Sources: fact_procedure, fact_payment, dim_date, dim_procedure

- `mart_insurance_performance`
  - Claim submission and approval rates
  - Insurance payment trends
  - Denial analysis
  - Processing time metrics
  - Sources: fact_claim, dim_insurance, dim_date

### 2. Clinical Operations Mart
**Purpose**: Clinical efficiency and quality metrics
**Key Models**:
- `mart_procedure_analytics`
  - Procedure volume and mix
  - Provider productivity
  - Chair time utilization
  - Procedure profitability
  - Sources: fact_procedure, dim_procedure, dim_provider, dim_date

- `mart_scheduling_analytics`
  - Appointment utilization
  - No-show and cancellation rates
  - Schedule optimization metrics
  - Provider availability
  - Sources: fact_appointment, dim_provider, dim_date, dim_location

### 3. Patient Experience Mart
**Purpose**: Patient engagement and satisfaction analysis
**Key Models**:
- `mart_patient_engagement`
  - Communication effectiveness
  - Response rates
  - Patient retention
  - Treatment plan acceptance
  - Sources: fact_communication, dim_patient, dim_date

- `mart_patient_financial`
  - Payment behavior
  - Financial risk scoring
  - Payment plan performance
  - Outstanding balance trends
  - Sources: fact_payment, dim_patient, dim_date

### 4. Operational Efficiency Mart
**Purpose**: Overall practice operations analysis
**Key Models**:
- `mart_operational_metrics`
  - System usage patterns
  - User activity analysis
  - Process efficiency metrics
  - Resource utilization
  - Sources: fact_procedure, fact_appointment, dim_location, dim_date

- `mart_quality_metrics`
  - Clinical quality indicators
  - Compliance metrics
  - Documentation completeness
  - Error rates
  - Sources: fact_procedure, fact_claim, dim_procedure, dim_provider

## General Design Principles
- **Date Grain**: Most marts should support daily, monthly, and rolling period aggregations.
- **Breakdowns**: All marts should support breakdowns by provider, location, patient type, and source.
- **Drill-down**: Fact tables at the patient or appointment level to support drill-downs from summary marts.
- **Frontend Alignment**: Design marts so each dashboard card or chart can be powered by a single mart or a simple join.

## Implementation Strategy

### Phase 1: Core Dimensions and Facts
1. Implement core dimension tables
   - dim_date
   - dim_patient
   - dim_provider
   - dim_procedure

2. Implement primary fact tables
   - fact_procedure
   - fact_payment
   - fact_claim

### Phase 2: Financial Analytics
1. Revenue and AR Analysis
2. Insurance Performance
3. Collection Effectiveness

### Phase 3: Clinical Operations
1. Procedure Analytics
2. Scheduling Efficiency
3. Provider Productivity

### Phase 4: Patient Experience
1. Engagement Metrics
2. Financial Behavior
3. Communication Effectiveness

### Phase 5: Operational Efficiency
1. System Usage
2. Process Metrics
3. Quality Indicators

## Testing Strategy

Each model will include:
- Data quality tests
- Business logic validation
- Performance optimization
- Documentation and lineage
- Referential integrity tests
- Slowly changing dimension handling

## Documentation Requirements

Each model will document:
- Business context and purpose
- Key metrics and calculations
- Data sources and transformations
- Usage guidelines
- Refresh frequency
- Dimensional relationships
- SCD type and handling

## Next Steps

1. Begin with Core Dimensions
   - Implement dim_date
   - Create dim_patient
   - Build dim_provider
   - Develop dim_procedure

2. Implement Primary Facts
   - Create fact_procedure
   - Build fact_payment
   - Develop fact_claim

3. Develop Financial Analytics Mart
   - Start with revenue analysis
   - Add AR and collection metrics
   - Include insurance performance

4. Build Remaining Marts
   - Clinical Operations
   - Patient Experience
   - Operational Efficiency

## Success Criteria

- All dimensions properly maintained
- Fact tables correctly linked
- Key metrics properly calculated
- Performance optimized for dashboard use
- Comprehensive test coverage
- Clear documentation and lineage
- Regular refresh schedule maintained
- Business user adoption and feedback 