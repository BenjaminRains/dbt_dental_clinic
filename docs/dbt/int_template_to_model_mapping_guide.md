# Template-to-Model Mapping Guide

## Overview

This document maps each of your 41 intermediate models to the appropriate documentation template and implementation approach. It works in conjunction with:
- **`int_model_standardization_strategy.md`** - Defines SQL model file standardization patterns and technical implementation
- **`int_yml_standardization_strategy.md`** - Provides detailed documentation template patterns and standards
- **`int_roadmap_checklist.md`** - Provides the detailed implementation roadmap and quality gates

Together, these documents provide a comprehensive framework for standardizing both SQL files and documentation across all intermediate models.

---

## Template Assignment by Model

### Template 1: Financial/Transaction Models
**Use for:** Models handling monetary transactions, fees, payments, and financial calculations

**Models (13 total):**
- `int_adjustments` - Financial adjustments with complex categorization
- `int_payment_split` - Payment allocation and reconciliation  
- `int_patient_payment_allocated` - Patient payment tracking
- `int_insurance_payment_allocated` - Insurance payment tracking
- `int_ar_analysis` - Financial foundation for AR reporting
- `int_ar_shared_calculations` - Common AR calculations
- `int_ar_aging_snapshot` - AR aging analysis
- `int_ar_balance` - AR balance tracking
- `int_ar_transaction_history` - AR transaction history
- `int_billing_statements` - Collections system statements
- `int_collection_metrics` - Collections performance measurement
- `int_fee_model` - Fee structure and pricing
- `int_patient_financial_journey` - Financial tracking across systems

### Template 2: Clinical/Operational Models  
**Use for:** Models supporting clinical workflows, patient care, and operational processes

**Models (8 total):**
- `int_appointment_details` - Scheduling and appointment management
- `int_appointment_schedule` - Appointment schedule management
- `int_appointment_metrics` - Appointment performance tracking
- `int_procedure_complete` - Procedure completion tracking
- `int_provider_availability` - Provider availability management
- `int_task_management` - Task tracking and management
- `int_treatment_journey` - Patient treatment progress tracking
- `int_user_preferences` - User preference management

### Template 3: Insurance/Benefits Models
**Use for:** Models managing insurance coverage, claims, and benefits processing

**Models (7 total):**
- `int_insurance_coverage` - Insurance coverage with complex data quality
- `int_insurance_employer` - Employer insurance relationships
- `int_insurance_eob_attachments` - EOB document tracking
- `int_claim_details` - Detailed claim information and processing
- `int_claim_payments` - Insurance payment tracking
- `int_claim_snapshot` - Claim status and progress tracking
- `int_claim_tracking` - Claim workflow and status management

### Template 4: System/Audit Models
**Use for:** Models supporting system monitoring, audit trails, and administrative functions

**Models (4 total):**
- `int_patient_profile` - Patient demographics and core information (Foundation)
- `int_provider_profile` - Provider information and capabilities (Foundation)
- `int_opendental_system_logs` - System audit and monitoring (Foundation)
- `int_collection_tasks` - Collection task management (has system workflow aspects)

### Template 5: Metrics/Aggregation Models
**Use for:** Models that aggregate data for performance measurement and KPI calculation

**Models (9 total):**
- `int_communication_metrics` - Communication effectiveness aggregation
- `int_statement_metrics` - Statement effectiveness measurement
- `int_patient_communications_base` - Communication events and tracking
- `int_communication_templates` - Communication template management
- `int_automated_communications` - Automated communication tracking
- `int_automated_communication_flags` - Automated communication triggers
- `int_collection_communication` - Collection communication tracking
- `int_collection_campaigns` - Collection campaign tracking

## Implementation Priority by Template

### Phase 1: Foundation Models (Week 1-2)
**Template 4 (System/Audit):**
1. `int_patient_profile` ✅ **COMPLETED**
2. `int_provider_profile` 
3. `int_opendental_system_logs`

**Template 1 (Financial - Foundation):**
4. `int_ar_analysis`

### Phase 2: Core Transaction Models (Week 3-4)
**Template 1 (Financial):**
1. `int_fee_model`
2. `int_adjustments` 
3. `int_payment_split`
4. `int_patient_payment_allocated`
5. `int_insurance_payment_allocated`

**Template 3 (Insurance):**
6. `int_insurance_coverage`
7. `int_insurance_employer`

### Phase 3: Operational and Clinical (Week 5-6)
**Template 2 (Clinical/Operational):**
1. `int_appointment_details`
2. `int_procedure_complete`
3. `int_provider_availability`
4. `int_appointment_schedule`

**Template 3 (Insurance - continued):**
5. `int_claim_details`
6. `int_claim_payments`
7. `int_claim_snapshot`
8. `int_claim_tracking`
9. `int_insurance_eob_attachments`

### Phase 4: Metrics and Aggregation (Week 7-8)
**Template 5 (Metrics):**
1. `int_communication_metrics`
2. `int_patient_communications_base`
3. `int_communication_templates`
4. `int_automated_communications`
5. `int_automated_communication_flags`

**Template 1 (Financial - continued):**
6. `int_ar_shared_calculations`
7. `int_ar_aging_snapshot`
8. `int_ar_balance`
9. `int_ar_transaction_history`

### Phase 5: Collections and Advanced (Week 9-10)
**Template 1 (Financial - Collections):**
1. `int_billing_statements`
2. `int_collection_metrics`

**Template 5 (Metrics - Collections):**
3. `int_collection_communication`
4. `int_collection_campaigns`
5. `int_statement_metrics`

**Template 4 (System - Collections):**
6. `int_collection_tasks`

**Template 2 (Clinical - Advanced):**
7. `int_appointment_metrics`
8. `int_task_management`
9. `int_user_preferences`
10. `int_treatment_journey`

**Template 1 (Financial - Advanced):**
11. `int_patient_financial_journey`

## Template-Specific Configuration Standards

### Template 1 (Financial) - Standard Config:
```yaml
config:
  materialized: table
  schema: intermediate
  unique_key: [entity]_id
  indexes:
    - {columns: ['[entity]_id'], unique: true}
    - {columns: ['patient_id']}
    - {columns: ['amount']}
    - {columns: ['_updated_at']}
```

### Template 2 (Clinical) - Standard Config:
```yaml
config:
  materialized: incremental
  schema: intermediate
  unique_key: [entity]_id
  incremental_strategy: merge
  indexes:
    - {columns: ['[entity]_id'], unique: true}
    - {columns: ['patient_id']}
    - {columns: ['provider_id']}
    - {columns: ['appointment_date']}
```

### Template 3 (Insurance) - Standard Config:
```yaml
config:
  materialized: table
  schema: intermediate
  unique_key: [entity]_id
  indexes:
    - {columns: ['[entity]_id'], unique: true}
    - {columns: ['patient_id']}
    - {columns: ['claim_id']}
    - {columns: ['insurance_plan_id']}
```

### Template 4 (System) - Standard Config:
```yaml
config:
  materialized: table
  schema: intermediate
  unique_key: [entity]_id
  indexes:
    - {columns: ['[entity]_id'], unique: true}
    - {columns: ['_updated_at']}
    - {columns: ['user_id']}
```

### Template 5 (Metrics) - Standard Config:
```yaml
config:
  materialized: incremental
  schema: intermediate
  unique_key: ['date_month', 'dimension_1', 'dimension_2']
  incremental_strategy: merge
  indexes:
    - {columns: ['date_month']}
    - {columns: ['patient_id']}
    - {columns: ['_updated_at']}
```

## Quality Gate Requirements by Template

### All Templates - Universal Requirements:
- ✅ Standard metadata fields (_extracted_at, _created_at, _updated_at, _transformed_at)
- ✅ snake_case naming throughout
- ✅ Proper OpenDental column quoting and transformation
- ✅ Primary key follows [entity]_id pattern
- ✅ Schema explicitly set to 'intermediate'

### Template-Specific Requirements:

**Template 1 (Financial):**
- Amount field validation (range checks)
- Financial integrity tests (sum validations)
- Payment allocation logic validation
- Account code reference validation

**Template 2 (Clinical):**
- Provider assignment validation
- Appointment scheduling conflict checks
- Patient safety requirement validation
- Clinical workflow compliance

**Template 3 (Insurance):**
- Coverage determination logic validation
- Claims processing status validation
- Benefits calculation accuracy
- EOB attachment integrity

**Template 4 (System):**
- Audit trail completeness
- User attribution accuracy
- Security event tracking
- System integrity validation

**Template 5 (Metrics):**
- Aggregation accuracy validation
- KPI calculation verification
- Grain consistency checks
- Performance threshold monitoring

This mapping ensures each model gets the appropriate template treatment while maintaining consistency across your 7-system architecture.