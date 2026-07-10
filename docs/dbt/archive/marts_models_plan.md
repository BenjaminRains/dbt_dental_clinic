# Dental Practice DBT Project - Mart Models Plan

## Overview

This document outlines the strategy for our mart layer models, which will transform our intermediate
 models into business-focused analytics views. The mart layer serves as the final transformation 
 layer before data reaches end users through dashboards and reports.

## List of Mart Models

### Operational Marts (Daily Grain)
- mart_daily_operations
- mart_daily_financial_summary
- mart_daily_schedule_performance
- mart_provider_daily_productivity

### Financial Analytics Marts
- mart_financial_performance
- mart_insurance_performance
- mart_procedure_profitability
- mart_ar_aging_analysis
- mart_insurance_reimbursement
- mart_patient_financial
- mart_ar_summary
- mart_ar_aging
- mart_patient_refunds
- mart_revenue_lost

### Clinical Operations Marts
- mart_procedure_analytics
- mart_scheduling_analytics
- mart_operational_metrics
- mart_quality_metrics
- mart_hygiene_retention

### Patient Analytics Marts
- mart_patient_engagement
- mart_patient_lifetime_value
- mart_patient_growth
- mart_patient_retention
- mart_new_patients
- mart_active_patients
- mart_patient_source
- mart_patient_cohort

### Executive Reporting Marts (Monthly Grain)
- mart_monthly_performance
- mart_monthly_trends
- mart_provider_productivity

## Dimensional Model Structure

### Dimension Tables

1. `dim_date`
   - Calendar attributes
   - Fiscal periods
   - Holiday flags
   - Business day indicators
   - Time intelligence support:
     - fiscal_quarter
     - rolling_12_months_flag
     - same_period_prior_year
     - business_day_of_month
     - holiday_adjusted_flag

2. `dim_patient`
   - Patient demographics
   - Status information
   - Financial attributes
   - Contact preferences
   - **Enhanced calculated fields for BI**:
     - age_group (0-17, 18-34, 35-54, 55+)
     - patient_tenure_months (time since first_visit_date)
     - financial_risk_category (low, medium, high based on balance aging)
     - engagement_level (active, inactive, churned)
     - communication_preference_score (engagement metric)

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
   - **Enhanced categorization for BI**:
     - procedure_category (preventive, restorative, surgical)
     - complexity_level (simple, moderate, complex)
     - revenue_tier (high, medium, low value)

5. `dim_insurance`
   - Insurance carrier details
   - Plan types
   - Coverage rules
   - Network status
   - **Enhanced performance metrics**:
     - claim_approval_rate (approved_claims / total_claims)
     - average_reimbursement_rate (total_paid / total_billed)
     - payment_velocity_score (tiered payment speed rating)
     - network_status_current (active/inactive based on verification_date)

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
   - **Enhanced measures**:
     - chair_time_efficiency (actual vs scheduled duration)
     - provider_utilization_rate
     - same_day_completion_flag
     - treatment_plan_adherence_score
   - Links to: dim_date, dim_patient, dim_provider, dim_procedure, dim_location, dim_insurance, dim_appointment

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
   - Links to: dim_date, dim_patient, dim_provider, dim_procedure, dim_location

5. `fact_communication`
   - Communication events
   - Channel information
   - Response tracking
   - Outcome metrics
   - Links to: dim_date, dim_patient, dim_provider

## Mart Layer Structure

### 1. Operational Analytics Mart (Daily Grain)
**Purpose**: Real-time operational performance for daily management
**Key Models**:
- `mart_daily_operations`
  - Daily procedure volume and mix
  - Chair utilization rates
  - Same-day completion metrics
  - Operational efficiency indicators
  - Sources: fact_procedure, fact_appointment, dim_date, dim_location

- `mart_daily_financial_summary`
  - Daily production and collection totals
  - Payment method breakdown
  - Collection rate tracking
  - Daily targets vs. actual performance
  - Sources: fact_procedure, fact_payment, dim_date

- `mart_daily_schedule_performance`
  - Appointment adherence rates
  - No-show and cancellation tracking
  - Schedule optimization metrics
  - Chair time utilization
  - Sources: fact_appointment, dim_provider, dim_date, dim_location

- `mart_provider_daily_productivity`
  - Individual provider metrics
  - Procedures per hour
  - Revenue per provider
  - Utilization rates
  - Sources: fact_procedure, fact_appointment, dim_provider, dim_date

### 2. Financial Analytics Mart
**Purpose**: Comprehensive financial performance analysis
**Key Models**:
- `mart_financial_performance`
  - Revenue by procedure, provider, time period
  - Fee variance analysis
  - Payment collection rates
  - AR aging trends
  - Collection effectiveness
  - **Pre-calculated dashboard metrics**:
    - total_production
    - total_collections
    - collection_rate
    - average_procedure_fee
    - procedures_per_day
    - production_vs_prior_month
    - collection_rate_vs_target
  - Sources: fact_procedure, fact_payment, dim_date, dim_procedure

- `mart_insurance_performance`
  - Claim submission and approval rates
  - Insurance payment trends
  - Denial analysis
  - Processing time metrics
  - Reimbursement rate analysis
  - Sources: fact_claim, dim_insurance, dim_date

- `mart_procedure_profitability`
  - Procedure-level profitability analysis
  - Cost vs. revenue analysis
  - Provider efficiency by procedure type
  - Profit margin trends
  - Sources: fact_procedure, dim_procedure, dim_provider, dim_date

- `mart_ar_aging_analysis`
  - Detailed accounts receivable aging
  - Collection probability scoring
  - Patient payment behavior analysis
  - Bad debt risk assessment
  - Sources: fact_payment, dim_patient, dim_date

### 3. Clinical Operations Mart
**Purpose**: Clinical efficiency and quality metrics
**Key Models**:
- `mart_procedure_analytics`
  - Procedure volume and mix
  - Provider productivity
  - Chair time utilization
  - Treatment plan completion rates
  - Sources: fact_procedure, dim_procedure, dim_provider, dim_date

- `mart_scheduling_analytics`
  - Appointment utilization
  - No-show and cancellation rates
  - Schedule optimization metrics
  - Provider availability analysis
  - Sources: fact_appointment, dim_provider, dim_date, dim_location

### 4. Patient Experience Mart
**Purpose**: Patient engagement and satisfaction analysis
**Key Models**:
- `mart_patient_engagement`
  - Communication effectiveness
  - Response rates
  - Patient retention metrics
  - Treatment plan acceptance
  - **Enhanced patient behavior analytics**:
    - appointment_adherence_rate
    - treatment_plan_acceptance_rate
    - payment_compliance_score
    - communication_responsiveness
    - churn_risk_score
  - Sources: fact_communication, fact_appointment, dim_patient, dim_date

- `mart_patient_lifetime_value`
  - Patient value segmentation
  - Revenue per patient analysis
  - Lifetime value projections
  - Patient profitability scoring
  - Sources: fact_procedure, fact_payment, dim_patient, dim_date

- `mart_patient_financial`
  - Payment behavior analysis
  - Financial risk scoring
  - Payment plan performance
  - Outstanding balance trends
  - Sources: fact_payment, dim_patient, dim_date

### 5. Operational Efficiency Mart
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

### 6. Executive Reporting Mart (Monthly Grain)
**Purpose**: High-level strategic analysis and trending
**Key Models**:
- `mart_monthly_performance`
  - Monthly financial summaries
  - Key performance indicator tracking
  - Trend analysis
  - Variance reporting
  - Sources: Various daily marts aggregated

- `mart_monthly_trends`
  - Year-over-year comparisons
  - Seasonal analysis
  - Growth rate calculations
  - Forecasting data points
  - Sources: Various daily and monthly marts

## BI Tool Optimization Guidelines

### Pre-calculated Aggregations
Each mart model should include commonly requested dashboard metrics to optimize BI tool performance:

**Financial Marts**:
- collection_rate = total_collections / total_production
- procedures_per_day = count(procedures) / count(business_days)
- average_procedure_fee = total_fees / count(procedures)
- production_vs_prior_period = (current_production - prior_production) / prior_production

**Operational Marts**:
- chair_utilization_rate = scheduled_time / available_time
- appointment_adherence_rate = kept_appointments / scheduled_appointments
- provider_productivity = procedures_completed / scheduled_procedures

**Patient Marts**:
- lifetime_value_estimate = historical_revenue + projected_future_revenue
- engagement_score = weighted_average(appointment_adherence, payment_compliance, communication_response)

### Dashboard-Ready Dimensions
Structure dimensions to minimize BI tool processing:

**Time-based Groupings**:
- Current period vs. prior period flags
- Rolling period indicators (30-day, 90-day, 12-month)
- Fiscal period alignments

**Categorical Groupings**:
- Pre-defined age groups, risk categories, procedure types
- Hierarchical rollups (procedure → category → department)
- Performance tiers (high/medium/low performers)

## General Design Principles
- **Multi-grain Support**: Marts should support daily operational analysis and monthly strategic reporting
- **Breakdowns**: All marts should support breakdowns by provider, location, patient type, and time periods
- **Drill-down Capability**: Fact tables at granular levels to support detailed analysis from summary views
- **Frontend Alignment**: Design marts so each dashboard component can be powered by a single mart or simple join
- **Performance Optimization**: Include pre-calculated metrics and avoid complex calculations in BI tools

## Implementation Strategy

### Phase 1: Core Foundation
1. Implement enhanced dimension tables
   - dim_date (with time intelligence)
   - dim_patient (with calculated fields)
   - dim_provider
   - dim_procedure (with categorization)
   - dim_insurance (with performance metrics)

2. Implement primary fact tables
   - fact_procedure (with enhanced measures)
   - fact_payment
   - fact_claim
   - fact_appointment

### Phase 2: Operational Analytics (Immediate Business Value)
1. Daily Operations
   - mart_daily_operations
   - mart_daily_financial_summary
   - mart_provider_daily_productivity

2. Performance Monitoring
   - mart_daily_schedule_performance
   - mart_procedure_analytics

### Phase 3: Financial Analytics
1. Revenue Analysis
   - mart_financial_performance
   - mart_procedure_profitability

2. AR Management
   - mart_ar_aging_analysis
   - mart_insurance_performance

### Phase 4: Patient Analytics
1. Engagement Analysis
   - mart_patient_engagement
   - mart_patient_lifetime_value

2. Retention Analysis
   - mart_patient_retention
   - mart_patient_growth

### Phase 5: Strategic Analytics
1. Executive Reporting
   - mart_monthly_performance
   - mart_monthly_trends

2. Advanced Analytics
   - mart_quality_metrics
   - mart_operational_metrics

## Testing Strategy

Each model will include:
- **Data Quality Tests**: Uniqueness, not-null constraints, accepted values
- **Business Logic Validation**: Metric calculations, aggregation accuracy
- **Performance Tests**: Query execution time, resource utilization
- **Referential Integrity**: Foreign key relationships, dimension consistency
- **Historical Accuracy**: SCD handling, temporal data integrity
- **Dashboard Integration**: End-to-end testing with BI tools

## Documentation Requirements

Each model will document:
- **Business Context**: Purpose, key users, decision-making support
- **Technical Specifications**: Grain, calculations, data sources
- **Usage Guidelines**: Recommended filters, groupings, time periods
- **Performance Notes**: Refresh frequency, optimization considerations
- **Relationships**: Dimensional links, fact table joins
- **Change Management**: SCD handling, historical preservation

## Success Criteria

### Technical Success
- All dimensions properly maintained with SCD handling
- Fact tables correctly linked with referential integrity
- Key metrics accurately calculated and validated
- Query performance optimized for dashboard use (<5 second response)
- Comprehensive test coverage (>95% pass rate)
- Automated refresh schedules maintained

### Business Success
- Dashboard load times under 3 seconds
- Self-service analytics adoption by end users
- Reduced ad-hoc reporting requests
- Consistent metric definitions across all reports
- Positive user feedback and adoption metrics
- Business decision impact measurability

## Next Steps

1. **Enhanced Core Dimensions**
   - Implement time intelligence in dim_date
   - Add calculated fields to dim_patient
   - Enhance dim_procedure with categorization
   - Add performance metrics to dim_insurance

2. **Operational Marts (Priority 1)**
   - mart_daily_operations
   - mart_daily_financial_summary
   - mart_provider_daily_productivity

3. **BI Tool Integration**
   - Design dashboard mockups using mart structure
   - Validate performance with realistic data volumes
   - Implement automated testing for dashboard queries

4. **Financial Analytics Expansion**
   - mart_financial_performance with pre-calculated KPIs
   - mart_ar_aging_analysis for revenue cycle management
   - mart_insurance_performance for payer analysis

## Monitoring and Maintenance

### Performance Monitoring
- Track mart refresh times and resource usage
- Monitor BI tool query performance against marts
- Set up alerts for failed refreshes or data quality issues

### Business Validation
- Regular validation of calculated metrics with business users
- Quarterly review of mart usage and optimization opportunities
- Feedback collection from dashboard users

### Evolution Strategy
- Plan for new business requirements and mart extensions
- Version control for mart schema changes
- Migration strategy for breaking changes