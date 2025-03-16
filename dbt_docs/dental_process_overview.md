# Dental Practice Data Model Overview

{% docs dental_practice_process %}

This document outlines the key business processes that drive our dental practice data models. Our 
dbt structure directly mirrors these business processes.

## Process Overview

The dental practice workflow is divided into seven interconnected systems:

1. **Fee Processing & Verification**: Handles procedure fee creation and validation
2. **Insurance Processing**: Manages claim lifecycle
3. **Payment Allocation & Reconciliation**: Processes payments from patients and insurance
4. **AR Analysis**: Analyzes accounts receivable aging
5. **Collection Process**: Manages outstanding balance collection
6. **Patient-Clinic Communications**: Handles all patient communications
7. **Scheduling & Referrals**: Manages appointments and referrals

## Mapping to Data Models

Our dbt project structure directly reflects these business processes:

### Staging Layer
- Raw source data from OpenDental (`stg_opendental__*` models)
- Standardized field names and types
- Basic data quality checks

### Intermediate Layer
Each system has dedicated intermediate models:

- `int_fees__*`: Fee processing models
- `int_insurance__*`: Insurance claim processing models
- `int_payments__*`: Payment allocation models
- `int_ar__*`: Accounts receivable models
- `int_collections__*`: Collection process models
- `int_communications__*`: Patient communication models
- `int_scheduling__*`: Appointment and referral models

### Mart Layer
Business-focused data marts that combine the intermediate models:

- `mart_finance__*`: Financial analytics
- `mart_patient__*`: Patient journey analytics
- `mart_operations__*`: Operational metrics

## Key Business Rules

- Payments can be split across multiple procedures
- Different payment split types follow different accounting rules
- AR is categorized by days outstanding (0-30, 30-60, 60-90, 90+)
- Collection follows a progressive notice sequence
- Claims must be batched efficiently
- Fees must be validated before finalization

## Critical Data Flow Paths

- Patient → Scheduling → Appointment → Procedure → Fee → Claim → Payment
- AR Analysis → Collection Process → Payment Allocation
- Communication flows connect all other systems

For more detailed documentation and diagrams, please reference the full business process 
documentation in the project wiki.

{% enddocs %}