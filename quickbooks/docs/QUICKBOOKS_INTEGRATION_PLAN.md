# QuickBooks Integration — Architecture Specification

> Stage files are `quickbooks/.env_local`, `quickbooks/.env_clinic`, and `quickbooks/.env_test`, loaded with `QUICKBOOKS_ENVIRONMENT`. `clinic` is the live company. Sections below that say `.env_production` predate that standard.

**ETL Architecture & Data Modeling for MDC and GLIC QuickBooks Sources**

| Attribute | Value |
|-----------|--------|
| **Status** | Planning — Pending Business Owner Response |
| **Version** | 1.1 |
| **Date** | 2026-03-05 |
| **Classification** | Internal — Architecture |

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Business Context](#business-context)
3. [Current State](#current-state)
4. [Key Requirements](#key-requirements)
5. [Information Needed from Business Owner](#information-needed-from-business-owner)
6. [Proposed Architecture](#proposed-architecture-multi-source-with-entity-tagging) (Multi-Source with Entity Tagging)
7. [Implementation Phases](#implementation-phases)
8. [Final System Architecture](#final-system-architecture)
9. [Success Criteria](#success-criteria)
10. [Technical Considerations](#technical-considerations)
11. [Risks & Mitigation](#risks--mitigation)
12. [Appendices & References](#appendices)

---

## Executive Summary

This specification defines the ETL architecture and data modeling approach for integrating QuickBooks (Online or Desktop) as a data source for Merrillville Dental Center (MDC) and Great Lakes Implant Center (GLIC). The goal is not simply to ingest QuickBooks data, but to **build a reconciled financial analytics layer** that combines operational data from OpenDental with accounting data from QuickBooks.

QuickBooks will supply financial data (invoices, payments, expenses, chart of accounts) that complements existing OpenDental clinical and operational data. The integration will enable:

- **Financial reconciliation** — Match OpenDental payments and production to QuickBooks transactions and revenue
- **Unified financial reporting** — Combine operational revenue (OpenDental) with accounting data (QuickBooks) and expenses
- **Expense visibility** — Track practice expenses alongside revenue in the analytics platform
- **Multi-entity separation** — Maintain strict entity boundaries (MDC vs GLIC) in all models and reports
- **Accounts receivable alignment** — Cross-reference AR between OpenDental and QuickBooks and explain discrepancies

---

## Business Context

### Business Value for Practice Owner

Right now, MDC likely has several financial blind spots that you may not even realize exist—problems that only surface when your accountant spends hours manually reconciling your books at month-end, work that costs you significant money. For example, when a patient pays $500 on Monday and it's entered in OpenDental immediately, but your staff doesn't enter it into QuickBooks until Friday's batch entry, you have no way to know there's a $500 payment that hasn't hit your accounting system yet, creating cash flow confusion where OpenDental shows money collected but QuickBooks doesn't. Similarly, when OpenDental shows $25,000 in accounts receivable but QuickBooks shows $23,000, you have no way to know which number is correct or why there's a $2,000 discrepancy—is it timing differences, missing entries, or actual collection issues? Worse yet, if a payment gets entered incorrectly in OpenDental (wrong amount, duplicate entry, or just forgotten entirely), you won't discover it until your accountant finds it weeks later during reconciliation. Additionally, because OpenDental only tracks revenue and QuickBooks tracks expenses, you can't see your true profitability—you know you collected $100K this month, but you have no idea what your net income is because expenses are trapped in QuickBooks, requiring your accountant to manually combine the data. By integrating QuickBooks into our analytics platform, we'll automate the reconciliation process your accountant currently performs manually, giving you daily visibility into exactly which payments are matched between systems, which ones are missing or unmatched (with automatic flags for investigation), timing differences that explain cash flow discrepancies, and a complete real-time P&L that combines OpenDental revenue with QuickBooks expenses—all without waiting for your accountant to compile reports, saving you both time and money while giving you the financial insights you need to run your practice more effectively.

---

## Current State

### Existing Infrastructure (As-Is)
- **Primary Entities**: Merrillville Dental Center (MDC) and Great Lakes Implant Center (GLIC)
- **OpenDental Source**: MySQL databases for both entities (already integrated)
- **Analytics Database**: PostgreSQL `opendental_analytics`
- **ETL Pipeline**: Multi-entity extraction from OpenDental with `clinic_entity_id` tagging
- **dbt Models**: 157+ models across staging/intermediate/marts layers
- **Financial Models**: `fact_payment`, `fact_procedure`, `mart_ar_summary`, `mart_revenue_lost`
- **API**: FastAPI backend serving analytics data
- **Frontend**: React dashboard

### New Addition (QuickBooks)
- **QuickBooks Sources**: Separate QuickBooks instances for MDC and GLIC
- **QuickBooks Type**: TBD (Online vs Desktop - **Information Needed**)
- **Integration Method**: TBD (API vs Database Export vs File Export - **Information Needed**)
- **Data Volume**: TBD (transaction volume, history depth - **Information Needed**)
- **Financial Reporting**: Completely separate per entity (consistent with OpenDental model)

---

## Key Requirements

### Entity Separation
- ✅ **Financial Reporting**: Completely separate (no cross-entity aggregation)
- ✅ **Dashboards**: Separate views per entity, with optional combined views
- ✅ **Data Integrity**: Clear entity boundaries in all models (`clinic_entity_id`)

### Data Integration Challenges
- ⚠️ **Payment Reconciliation**: Match OpenDental payments to QuickBooks transactions
- ⚠️ **Customer Mapping**: Link QuickBooks Customers to OpenDental Patients (name/ID matching)
- ⚠️ **Invoice Matching**: Link QuickBooks Invoices to OpenDental Procedures/Claims
- ⚠️ **Date Alignment**: Handle timing differences between clinical and accounting entries

### Schema Compatibility
- ✅ **Flexible Design**: QuickBooks data structure differs from OpenDental - will use separate staging layer
- ✅ **Entity Tagging**: All QuickBooks tables will include `clinic_entity_id` column
- ⚠️ **Data Mapping**: Business owner must clarify QuickBooks chart of accounts and category structure

---

## Information Needed from Business Owner

### 1. QuickBooks Type and Access Method

**Critical Questions**:
1. **QuickBooks Type**: 
   - [ ] QuickBooks Online (cloud-based, API access)
   - [ ] QuickBooks Desktop (local installation, database access)
   - [ ] QuickBooks Enterprise (on-premise, database access)

2. **Access Method** (depends on type):
   - **For QuickBooks Online**: OAuth 2.0 API access required
     - Do we have API credentials (Client ID, Client Secret)?
     - What is the Company ID (Realm ID) for each entity?
     - What API access level (sandbox vs production)?
   - **For QuickBooks Desktop**: Database/file access required
     - Database file location and format?
     - Can we access .QBW file directly or need exports?
     - ODBC/API access available?

3. **Authentication Details**:
   - API keys/credentials for QuickBooks Online?
   - OAuth token refresh process?
   - Network access (VPN required for Desktop)?

### 2. Data Availability and Volume

**Questions**:
1. **Historical Data Depth**: How far back does QuickBooks data go?
   - [ ] 1 year
   - [ ] 2-3 years
   - [ ] 5+ years
   - [ ] All historical data

2. **Transaction Volume**:
   - Approximate number of invoices per month (MDC)?
   - Approximate number of invoices per month (GLIC)?
   - Approximate number of expense transactions per month?
   - Approximate number of payments received per month?

3. **Data Export Frequency**:
   - [ ] Real-time (API polling every X minutes)
   - [ ] Daily (nightly batch)
   - [ ] Weekly
   - [ ] Manual export (one-time initial load, then periodic)

### 3. Chart of Accounts Structure

**Questions**:
1. **Revenue Accounts**: What QuickBooks accounts track patient revenue?
   - Account names/numbers for patient payments?
   - Account names/numbers for insurance payments?
   - Any other revenue categories?

2. **Expense Categories**: What expense categories are used?
   - Staff/payroll accounts?
   - Supplies/equipment accounts?
   - Facility/rent accounts?
   - Insurance/benefits accounts?

3. **Account Naming Convention**: 
   - Are accounts standardized between MDC and GLIC?
   - Do account names/numbers match OpenDental categories?

### 4. Customer/Patient Matching Strategy

**Questions**:
1. **Customer Identification**: How are customers identified in QuickBooks?
   - [ ] Customer name matches patient name in OpenDental
   - [ ] Customer ID matches patient ID in OpenDental
   - [ ] Separate customer database (need mapping table)
   - [ ] Customer = insurance company (not individual patients)

2. **Customer Data Structure**:
   - Is each OpenDental patient a QuickBooks customer?
   - Or are QuickBooks customers insurance companies/payers?
   - Are there parent/child customer relationships (families)?

3. **Matching Logic**: How should we match QuickBooks customers to OpenDental patients?
   - Exact name match?
   - Name + DOB match?
   - Manual mapping table required?

### 5. Invoice/Procedure Matching

**Questions**:
1. **Invoice Creation**: When are QuickBooks invoices created?
   - [ ] Per procedure (one invoice per OpenDental procedure)
   - [ ] Per visit (one invoice per appointment with multiple procedures)
   - [ ] Per statement cycle (monthly patient statements)
   - [ ] Manual entry (not directly linked to OpenDental)

2. **Invoice Numbering**: How are invoices numbered?
   - Sequential numbers?
   - Linked to OpenDental procedure IDs?
   - Custom format?
   - Can we extract OpenDental procedure ID from invoice memo/notes?

3. **Invoice Matching**: How should we match QuickBooks invoices to OpenDental procedures?
   - Invoice number contains procedure ID?
   - Invoice date matches procedure date?
   - Manual mapping required?

### 6. Payment Reconciliation

**Questions**:
1. **Payment Entry**: How are payments entered in QuickBooks?
   - [ ] Automatically from OpenDental (integration exists)
   - [ ] Manual entry (separate from OpenDental)
   - [ ] Batch import from bank statements
   - [ ] Payment processor integration (Stripe, etc.)

2. **Payment Matching**: Can we match QuickBooks payments to OpenDental payments?
   - Payment amount + date matching?
   - Payment reference number matching?
   - Manual reconciliation required?

3. **Payment Timing**: Are payments entered in QuickBooks on the same day as OpenDental?
   - Same day (real-time sync)?
   - Next day (batch sync)?
   - Monthly (statement reconciliation)?

### 7. Multi-Entity Setup

**Questions**:
1. **QuickBooks Instances**: Do MDC and GLIC have separate QuickBooks companies?
   - [ ] Yes, completely separate QuickBooks companies
   - [ ] No, single QuickBooks company with classes/locations
   - [ ] No, single QuickBooks company with different customer types

2. **Entity Separation**: If single QuickBooks company:
   - How are MDC and GLIC transactions distinguished?
   - Using Classes (Class = MDC or GLIC)?
   - Using Locations (Location = MDC or GLIC)?
   - Using Customer Groups/Types?
   - Using Account prefixes?

3. **If Separate Companies**: 
   - Separate API credentials for each?
   - Separate database files for each (Desktop)?
   - Different chart of accounts structures?

---

## Proposed Architecture: Multi-Source with Entity Tagging

The following design supports [Phase 1](#phase-1--quickbooks-data-foundation) and [Phase 2](#phase-2--source-specific-financial-analytics) and aligns with the [Final System Architecture](#final-system-architecture) once all phases are complete.

### High-Level Data Flow

```
┌─────────────────────┐         ┌─────────────────────┐
│  MDC QuickBooks     │         │  GLIC QuickBooks    │
│  (Source System)    │         │  (Source System)    │
└──────────┬──────────┘         └──────────┬──────────┘
           │                                │
           │ QuickBooks ETL                │ QuickBooks ETL
           │ (entity_id='MDC')             │ (entity_id='GLIC')
           │                                │
           └────────────┬───────────────────┘
                        │
           ┌────────────▼────────────────────┐
           │  PostgreSQL Analytics           │
           │  opendental_analytics            │
           │                                  │
           │  raw schema:                    │
           │  - quickbooks_customer           │
           │  - quickbooks_invoice            │
           │  - quickbooks_payment            │
           │  - quickbooks_expense            │
           │  - quickbooks_account            │
           │  (all with clinic_entity_id)     │
           │                                  │
           │  staging schema:                │
           │  - stg_quickbooks__customer      │
           │  - stg_quickbooks__invoice       │
           │  - stg_quickbooks__payment       │
           │  ...                            │
           │                                  │
           │  intermediate schema:           │
           │  - int_quickbooks_customer_      │
           │      patient_mapping             │
           │  - int_quickbooks_invoice_       │
           │      procedure_mapping           │
           │  - int_quickbooks_payment_       │
           │      reconciliation              │
           │  ...                            │
           │                                  │
           │  marts schema:                  │
           │  - dim_quickbooks_customer       │
           │  - fact_quickbooks_invoice       │
           │  - fact_quickbooks_payment       │
           │  - mart_financial_reconciliation │
           │  - mart_expense_analysis         │
           └──────────────────────────────────┘
                        │
           ┌────────────┴────────────┐
           │                         │
    ┌──────▼──────┐         ┌───────▼──────┐
    │ MDC Dashboard│         │ GLIC Dashboard│
    │ Financial    │         │ Financial    │
    │ Reporting    │         │ Reporting    │
    └─────────────┘         └──────────────┘
```

### Architecture Components

#### 1. **QuickBooks ETL Pipeline** (New Component)

**Location**: `etl_pipeline/extractors/quickbooks_extractor.py`

**Responsibilities**:
- Connect to QuickBooks (API or database)
- Extract core entities (Customers, Invoices, Payments, Expenses, Accounts)
- Tag all records with `clinic_entity_id` ('MDC' or 'GLIC')
- Handle incremental loading (track last sync timestamp)
- Manage API rate limits and authentication refresh

**Key Entities to Extract**:
1. **Customers**: Customer names, IDs, contact info, custom fields
2. **Invoices**: Invoice numbers, dates, amounts, line items, customer links
3. **Payments**: Payment dates, amounts, methods, invoice links, customer links
4. **Expenses**: Expense dates, amounts, categories, vendors, descriptions
5. **Accounts**: Chart of accounts (account names, types, numbers)
6. **Vendors**: Vendor names, IDs, contact info
7. **Items/Services**: Product/service catalog (may map to OpenDental procedures)

**Extraction Strategy**:
- **Initial Load**: Full historical data export
- **Incremental Load**: Track `MetaData.LastUpdatedTime` (QuickBooks Online) or `TimeModified` (Desktop)
- **Rate Limiting**: Respect QuickBooks API rate limits (500 requests per minute for Online)
- **Error Handling**: Retry logic for transient failures, log failed extractions

#### 2. **QuickBooks Loader** (New Component)

**Location**: `etl_pipeline/loaders/quickbooks_loader.py`

**Responsibilities**:
- Load extracted QuickBooks data into PostgreSQL `raw` schema
- Transform JSON/XML responses to relational tables
- Handle nested data (line items, attachments, custom fields)
- Ensure entity tagging is preserved
- Create appropriate indexes

**Raw Schema Tables**:
```sql
raw.quickbooks_customer (
    customer_id VARCHAR(255) PRIMARY KEY,
    clinic_entity_id VARCHAR(10) NOT NULL,  -- 'MDC' or 'GLIC'
    company_name VARCHAR(255),
    display_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    billing_address JSONB,
    shipping_address JSONB,
    balance DECIMAL(18,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    metadata JSONB,  -- Store full QuickBooks response
    _loaded_at TIMESTAMP
);

raw.quickbooks_invoice (
    invoice_id VARCHAR(255) PRIMARY KEY,
    clinic_entity_id VARCHAR(10) NOT NULL,
    invoice_number VARCHAR(100),
    customer_id VARCHAR(255),
    invoice_date DATE,
    due_date DATE,
    total_amount DECIMAL(18,2),
    balance DECIMAL(18,2),
    status VARCHAR(50),
    currency_code VARCHAR(10),
    line_items JSONB,  -- Array of line items
    custom_fields JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    metadata JSONB,
    _loaded_at TIMESTAMP
);

raw.quickbooks_payment (
    payment_id VARCHAR(255) PRIMARY KEY,
    clinic_entity_id VARCHAR(10) NOT NULL,
    payment_number VARCHAR(100),
    customer_id VARCHAR(255),
    payment_date DATE,
    payment_amount DECIMAL(18,2),
    payment_method VARCHAR(100),
    deposit_to_account_id VARCHAR(255),
    invoice_ids TEXT[],  -- Array of linked invoice IDs
    reference_number VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    metadata JSONB,
    _loaded_at TIMESTAMP
);

raw.quickbooks_expense (
    expense_id VARCHAR(255) PRIMARY KEY,
    clinic_entity_id VARCHAR(10) NOT NULL,
    expense_number VARCHAR(100),
    vendor_id VARCHAR(255),
    expense_date DATE,
    expense_amount DECIMAL(18,2),
    account_id VARCHAR(255),  -- Chart of accounts account
    payment_method VARCHAR(100),
    line_items JSONB,  -- Expense line items
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    metadata JSONB,
    _loaded_at TIMESTAMP
);

raw.quickbooks_account (
    account_id VARCHAR(255) PRIMARY KEY,
    clinic_entity_id VARCHAR(10) NOT NULL,
    account_number VARCHAR(50),
    account_name VARCHAR(255),
    account_type VARCHAR(100),  -- Asset, Liability, Equity, Revenue, Expense
    account_subtype VARCHAR(100),
    current_balance DECIMAL(18,2),
    is_active BOOLEAN,
    parent_account_id VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    metadata JSONB,
    _loaded_at TIMESTAMP
);
```

#### 3. **Configuration Management**

**Environment Variables** (`.env_production`):
```bash
# MDC QuickBooks Configuration
MDC_QUICKBOOKS_TYPE=online  # or 'desktop'
MDC_QUICKBOOKS_CLIENT_ID=your_client_id
MDC_QUICKBOOKS_CLIENT_SECRET=your_client_secret
MDC_QUICKBOOKS_REALM_ID=mdc_realm_id  # For Online
MDC_QUICKBOOKS_REFRESH_TOKEN=mdc_refresh_token  # For Online
MDC_QUICKBOOKS_ENTITY=MDC

# GLIC QuickBooks Configuration
GLIC_QUICKBOOKS_TYPE=online  # or 'desktop'
GLIC_QUICKBOOKS_CLIENT_ID=your_client_id
GLIC_QUICKBOOKS_CLIENT_SECRET=your_client_secret
GLIC_QUICKBOOKS_REALM_ID=glic_realm_id  # For Online
GLIC_QUICKBOOKS_REFRESH_TOKEN=glic_refresh_token  # For Online
GLIC_QUICKBOOKS_ENTITY=GLIC

# QuickBooks ETL Settings
QUICKBOOKS_SYNC_FREQUENCY=daily  # or 'hourly', 'weekly'
QUICKBOOKS_INITIAL_LOAD_DATE=2020-01-01  # Start date for historical data
QUICKBOOKS_RATE_LIMIT_PER_MINUTE=450  # Stay under 500/min limit
```

**Table Configuration** (`etl_pipeline/config/quickbooks_tables.yml`):
```yaml
metadata:
  source_type: quickbooks
  entities: [mdc, glic]
  
tables:
  quickbooks_customer:
    extraction_strategy: incremental
    sync_field: MetaData.LastUpdatedTime
    priority: critical
    batch_size: 100
    
  quickbooks_invoice:
    extraction_strategy: incremental
    sync_field: MetaData.LastUpdatedTime
    priority: critical
    batch_size: 50
    
  quickbooks_payment:
    extraction_strategy: incremental
    sync_field: MetaData.LastUpdatedTime
    priority: critical
    batch_size: 50
    
  quickbooks_expense:
    extraction_strategy: incremental
    sync_field: MetaData.LastUpdatedTime
    priority: important
    batch_size: 100
    
  quickbooks_account:
    extraction_strategy: full_table  # Accounts change infrequently
    priority: reference
    batch_size: 1000
```

---

## DBT Modeling Plan

The following model inventory supports the staging, intermediate, and mart layers referenced in [Implementation Phases](#implementation-phases) (Phases 1–4).

### Staging Layer: QuickBooks Data Standardization

**Location**: `dbt_dental_models/models/staging/quickbooks/`

**Models to Create**:
1. `stg_quickbooks__customer.sql`
   - Standardize customer data
   - Extract address components from JSONB
   - Handle name variations
   - Tag with `clinic_entity_id`

2. `stg_quickbooks__invoice.sql`
   - Standardize invoice data
   - Flatten line items (one row per line item)
   - Extract custom fields
   - Calculate totals and balances
   - Tag with `clinic_entity_id`

3. `stg_quickbooks__payment.sql`
   - Standardize payment data
   - Extract payment method details
   - Flatten invoice links (one row per invoice payment)
   - Tag with `clinic_entity_id`

4. `stg_quickbooks__expense.sql`
   - Standardize expense data
   - Flatten line items
   - Extract account/category information
   - Tag with `clinic_entity_id`

5. `stg_quickbooks__account.sql`
   - Standardize chart of accounts
   - Build account hierarchy
   - Tag with `clinic_entity_id`

### Intermediate Layer: Business Logic and Cross-System Mapping

**Location**: `dbt_dental_models/models/intermediate/system_d_quickbooks/`

**Models to Create**:

1. **Customer/Patient Mapping**:
   - `int_quickbooks_customer_patient_mapping.sql`
     - Match QuickBooks customers to OpenDental patients
     - Use name matching, ID matching, or custom mapping table
     - Create `customer_patient_match_score` for confidence level
     - Handle one-to-many relationships (family accounts)

2. **Invoice/Procedure Matching**:
   - `int_quickbooks_invoice_procedure_mapping.sql`
     - Match QuickBooks invoices to OpenDental procedures/claims
     - Use invoice date + amount matching
     - Extract procedure IDs from invoice memo/notes if available
     - Handle multiple procedures per invoice

3. **Payment Reconciliation**:
   - `int_quickbooks_payment_reconciliation.sql`
     - Match QuickBooks payments to OpenDental payments
     - Use date + amount matching
     - Handle partial payments and payment splits
     - Flag unmatched payments (reconciliation gaps)

4. **Financial Summaries**:
   - `int_quickbooks_revenue_summary.sql`
     - Summarize QuickBooks revenue by entity, date, account
     - Compare to OpenDental production amounts
     - Identify discrepancies

   - `int_quickbooks_expense_summary.sql`
     - Summarize expenses by entity, date, category
     - Group by chart of accounts hierarchy
     - Calculate expense ratios

### Mart Layer: Analytics-Ready Models

**Location**: `dbt_dental_models/models/marts/`

**Models to Create**:

1. **Dimensions**:
   - `dim_quickbooks_customer.sql`
     - Customer dimension with patient linkage
     - Entity-specific filtering
     - SCD Type 2 for customer changes

2. **Facts**:
   - `fact_quickbooks_invoice.sql`
     - Invoice fact table with procedure linkage
     - Entity-specific filtering
     - Date dimensions
     - Financial metrics (invoice amount, balance, days outstanding)

   - `fact_quickbooks_payment.sql`
     - Payment fact table with OpenDental payment reconciliation
     - Entity-specific filtering
     - Payment method analysis
     - Reconciliation status flags

   - `fact_quickbooks_expense.sql`
     - Expense fact table
     - Entity-specific filtering
     - Expense category analysis
     - Vendor analysis

3. **Summary Marts**:
   - `mart_financial_reconciliation.sql`
     - Compare OpenDental revenue to QuickBooks revenue
     - Identify reconciliation gaps
     - Entity-specific summaries
     - Daily/monthly reconciliation reports

   - `mart_expense_analysis.sql`
     - Expense trends by category
     - Expense ratios (expense/revenue)
     - Vendor spending analysis
     - Entity-specific summaries

   - `mart_quickbooks_ar_summary.sql`
     - Accounts receivable from QuickBooks
     - Compare to OpenDental AR
     - Aging analysis
     - Entity-specific summaries

   - `mart_comprehensive_financial_reporting.sql`
     - Combined revenue (OpenDental + QuickBooks adjustments)
     - Combined expenses
     - Net income calculations
     - Entity-specific P&L summaries

### Example DBT Model: Customer/Patient Mapping

```sql
-- models/intermediate/system_d_quickbooks/int_quickbooks_customer_patient_mapping.sql

{{ config(
    materialized='table',
    schema='intermediate'
) }}

with quickbooks_customers as (
    select
        customer_id,
        clinic_entity_id,
        display_name,
        company_name,
        email,
        phone,
        billing_address,
        _loaded_at
    from {{ ref('stg_quickbooks__customer') }}
),

opendental_patients as (
    select
        patient_id,
        clinic_entity_id,
        first_name || ' ' || last_name as full_name,
        date_of_birth,
        email,
        phone_home,
        phone_cell,
        _loaded_at
    from {{ ref('stg_opendental__patient') }}
),

-- Name-based matching (exact match)
name_match as (
    select
        qb.customer_id,
        qb.clinic_entity_id as qb_entity_id,
        op.patient_id,
        op.clinic_entity_id as op_entity_id,
        'exact_name' as match_type,
        1.0 as match_confidence
    from quickbooks_customers qb
    inner join opendental_patients op
        on qb.clinic_entity_id = op.clinic_entity_id
        and lower(trim(qb.display_name)) = lower(trim(op.full_name))
),

-- Email-based matching
email_match as (
    select
        qb.customer_id,
        qb.clinic_entity_id as qb_entity_id,
        op.patient_id,
        op.clinic_entity_id as op_entity_id,
        'email' as match_type,
        0.9 as match_confidence
    from quickbooks_customers qb
    inner join opendental_patients op
        on qb.clinic_entity_id = op.clinic_entity_id
        and lower(trim(qb.email)) = lower(trim(op.email))
        and qb.email is not null
        and op.email is not null
),

-- Combined matches
all_matches as (
    select * from name_match
    union all
    select * from email_match
),

-- Deduplicate (prefer exact name match)
ranked_matches as (
    select
        customer_id,
        qb_entity_id,
        patient_id,
        op_entity_id,
        match_type,
        match_confidence,
        row_number() over (
            partition by customer_id, qb_entity_id 
            order by match_confidence desc
        ) as match_rank
    from all_matches
)

select
    customer_id,
    qb_entity_id as clinic_entity_id,
    patient_id,
    match_type,
    match_confidence,
    _loaded_at
from ranked_matches
where match_rank = 1
```

---

## Implementation Phases

The QuickBooks integration will be implemented in five major phases, progressing from data access to fully reconciled financial analytics. Each phase has a clear objective and deliverable before moving to the next. The goal is not simply to ingest QuickBooks data, but to build a **reconciled financial analytics layer** that combines operational data from OpenDental with accounting data from QuickBooks.

---

### Phase 1 — QuickBooks Data Foundation

**Goal:** Establish reliable extraction and ingestion of QuickBooks data into the analytics warehouse while preserving entity separation and accounting source integrity.

**Objectives:**

- Establish authenticated access to QuickBooks (API or database/file)
- Extract core financial entities from QuickBooks
- Land raw QuickBooks data into PostgreSQL
- Create dbt staging models for normalized financial structures
- Preserve clear entity separation between MDC and GLIC

**Deliverables:**

- Working pipeline that loads core QuickBooks financial objects into the warehouse:
  - `raw_quickbooks_invoice`
  - `raw_quickbooks_payment`
  - `raw_quickbooks_customer`
  - `raw_quickbooks_account`
  - `raw_quickbooks_expense`
- All tables must include:
  - `clinic_entity_id`
  - `source_system = 'quickbooks'`

**Phase 1 outcomes:** At the end of Phase 1 we should be able to answer: What invoices exist in QuickBooks? What payments were recorded? What accounts and expense categories exist? What customers or payers exist? No reconciliation logic is implemented yet; the goal is accurate financial data ingestion.

---

### Phase 2 — Source-Specific Financial Analytics

**Goal:** Enable QuickBooks-only financial reporting within the analytics platform. This phase provides immediate business value by making accounting data visible in the analytics environment.

**Objectives:**

- Create dbt models that support:
  - Revenue summaries
  - Expense summaries
  - Account balances
  - Invoice aging
  - Cash flow summaries

**Example models:**

- Staging: `stg_quickbooks_invoice`, `stg_quickbooks_payment`, `stg_quickbooks_account`, `stg_quickbooks_expense`
- Intermediate: `int_quickbooks_revenue`, `int_quickbooks_expenses`, `int_quickbooks_cashflow`
- Marts: `mart_quickbooks_profit_and_loss`, `mart_quickbooks_revenue_summary`, `mart_quickbooks_expense_summary`

**Deliverables:**

- Operational dashboards that answer: What revenue is recorded in QuickBooks? What expenses are recorded? What is the accounting P&L? What is the current QuickBooks AR balance?
- This phase does not attempt to merge QuickBooks and OpenDental data; both systems remain independent sources of truth.

---

### Phase 3 — Cross-System Reconciliation

**Goal:** Identify and explain discrepancies between OpenDental operational data and QuickBooks accounting data. This phase introduces the reconciliation layer, which is the most important analytical component of the integration.

**Objectives:**

- Build models that compare:
  - OpenDental payments vs QuickBooks payments
  - OpenDental production vs QuickBooks revenue
  - OpenDental AR vs QuickBooks AR

**Key reconciliation models:**

- `int_payment_reconciliation`
- `int_revenue_reconciliation`
- `int_ar_reconciliation`

Each reconciliation model should include diagnostic metadata such as: `match_status`, `match_confidence`, `match_method`, `amount_difference`, `date_difference`, `manual_override_flag`.

**Deliverables:**

- Reconciliation dashboards showing: matched transactions, unmatched payments, timing differences, potential accounting errors
- Example outputs: `mart_payment_reconciliation_summary`, `mart_unmatched_payments`, `mart_revenue_discrepancies`
- This phase answers: **Do the clinical system and accounting system agree?**

---

### Phase 4 — Integrated Financial Analytics

**Goal:** Create a fully integrated financial analytics layer combining operational production data with accounting financial data. This phase enables the most valuable reporting for practice management.

**Objectives:**

- Build integrated models combining OpenDental revenue and QuickBooks expenses
- Key analytics models:
  - `mart_practice_profitability`
  - `mart_provider_profitability`
  - `mart_cashflow_analysis`
  - `mart_financial_performance`

These models enable insights such as: true practice profitability, revenue vs expenses, provider production vs net income, financial performance trends.

**Deliverables:**

- Executive-level dashboards showing: real-time P&L, revenue vs expense trends, provider profitability, financial performance by entity
- At this stage the analytics platform becomes a complete financial intelligence system for the practice.

---

### Phase 5 — Monitoring and Automation

**Goal:** Ensure the integration operates reliably in production and automatically detects financial inconsistencies.

**Objectives:**

- Implement:
  - Automated reconciliation checks
  - Anomaly detection
  - Ingestion monitoring
  - API reliability safeguards

**Example monitoring features:**

- Daily reconciliation checks
- Missing payment alerts
- Large discrepancy alerts
- ETL pipeline health monitoring

**Deliverables:**

- Operational safeguards including: alerting when reconciliation fails, automatic discrepancy detection, monitoring dashboards for ETL health

---

## Final System Architecture

After completion of all phases, the system will have the following structure:

```
Operational Source          Accounting Source
     OpenDental                    QuickBooks
          │                             │
          └──────────┬──────────────────┘
                     │
          Data Ingestion Layer
          (Python API extraction)
                     │
                     ▼
          Raw Data Layer
          raw_opendental.*
          raw_quickbooks.*
                     │
                     ▼
          Staging Layer (dbt)
          stg_opendental_*
          stg_quickbooks_*
                     │
                     ▼
          Reconciliation Layer
          int_payment_reconciliation
          int_revenue_reconciliation
                     │
                     ▼
          Analytics Layer
          mart_practice_profitability
          mart_financial_performance
          mart_provider_profitability
```

---

## Success Criteria

The QuickBooks integration will be considered successful when the platform can reliably answer the following business questions:

| # | Business Question |
|---|-------------------|
| 1 | Do OpenDental payments match QuickBooks records? |
| 2 | What revenue has been recorded vs collected? |
| 3 | What is the true profitability of the practice? |
| 4 | Are there accounting discrepancies between systems? |
| 5 | What are the major drivers of revenue and expense trends? |

These criteria should be validated with the business owner and used to prioritize reconciliation logic and dashboard design.

---

## Technical Considerations

### QuickBooks Online API

**API Endpoints** (if using QuickBooks Online):
- `/v3/company/{realmId}/query` - Query entities
- `/v3/company/{realmId}/customers` - Customer operations
- `/v3/company/{realmId}/invoices` - Invoice operations
- `/v3/company/{realmId}/payments` - Payment operations
- `/v3/company/{realmId}/purchases` - Expense operations
- `/v3/company/{realmId}/accounts` - Chart of accounts

**Rate Limits**:
- 500 requests per minute (per company)
- Use exponential backoff for retries
- Implement request queuing for batch operations

**OAuth 2.0 Flow**:
- Refresh tokens expire after 100 days (must refresh)
- Implement automatic token refresh
- Store tokens securely (environment variables or secrets manager)

### QuickBooks Desktop

**Database Access** (if using QuickBooks Desktop):
- Access `.QBW` file via ODBC driver
- Or use QuickBooks SDK/QODBC
- Or export to CSV/Excel for ETL processing
- Local network access required (VPN if remote)

**Alternative**: Use Intuit's QuickBooks Data Service (QDS) for Desktop

### Data Quality Considerations

1. **Date Alignment**: QuickBooks dates may differ from OpenDental dates
   - Use date ranges for matching (±3 days tolerance)
   - Flag date discrepancies for manual review

2. **Amount Matching**: Floating-point precision issues
   - Use rounding for comparison (round to 2 decimal places)
   - Flag small discrepancies (< $0.01) as rounding differences

3. **Missing Data**: QuickBooks may not have all OpenDental transactions
   - Flag unmatched OpenDental payments as "not in QuickBooks"
   - Allow manual mapping table for exceptions

4. **Duplicate Handling**: Multiple QuickBooks entries for same transaction
   - Deduplicate by date + amount + customer
   - Flag duplicates for review

### Technical Setup Checklist (QuickBooks Online)

The following step-by-step checklist describes how to technically set up QuickBooks Online integration for ingestion and modeling. This is the concrete “do this in order” guide for engineering.

#### 1. Create and Configure the Intuit Developer App

1. Log in to the Intuit Developer portal with the engineering account.
2. Create a new **QuickBooks Online** app (or use the shared multi-tenant app if already provisioned).
3. Configure:
   - **Redirect URI**: `https://{API_HOST}/qbo/oauth/callback` (clinic + demo variants as needed)
   - **Environment**: Sandbox first, then Production.
4. Capture the following values for deployment:
   - `QBO_CLIENT_ID`
   - `QBO_CLIENT_SECRET`
   - `QBO_ENVIRONMENT` (`sandbox` or `production`)
   - `QBO_REDIRECT_URI`

These values are stored as environment variables in the API / ETL services (not committed to git).

#### 2. Extend the ETL / API Configuration

Add QBO-specific settings to the existing configuration system (equivalent to current OpenDental connection config):

- Global environment variables (per deployment environment):
  - `QBO_CLIENT_ID`
  - `QBO_CLIENT_SECRET`
  - `QBO_ENVIRONMENT`
  - `QBO_REDIRECT_URI`
- Application config entries:
  - QBO OAuth callback route
  - Token storage backend (PostgreSQL table or secrets manager)

For local development, use a git-ignored `.env` file (e.g. `.env_api_clinic`) and map these into the running container / process.

#### 3. Implement OAuth 2.0 Connection Flow per Clinic

For each clinic (MDC, GLIC, future entities), implement a standard “Connect to QuickBooks” flow in the API:

1. **Authorization URL generation**
   - Backend generates the Intuit authorization URL using `QBO_CLIENT_ID`, `QBO_REDIRECT_URI`, requested scopes, and a CSRF/state token that encodes `clinic_entity_id`.
2. **User redirect**
   - The frontend or admin UI redirects the clinic owner / admin to this URL to log in to QBO and select the company (realm).
3. **Callback handling**
   - Intuit redirects back to `QBO_REDIRECT_URI` with `code` and `realmId`.
   - Backend exchanges the `code` for:
     - `access_token`
     - `refresh_token`
     - `expires_in`
   - Backend stores these in the `qbo_connections` table (see next step), keyed by `clinic_entity_id` and `realm_id`.
4. **Token refresh**
   - Implement a background or on-demand refresh mechanism that:
     - Detects impending expiry
     - Uses the `refresh_token` to obtain a new `access_token`
     - Persists updated tokens and expiry timestamps

No QBO username/password is ever stored; everything is mediated by OAuth tokens.

#### 4. Create Persistence for QBO Connections

Create a table (or equivalent secrets storage) to store per-clinic QBO connection metadata:

- Suggested table: `qbo_connections` (in `public` or a dedicated config schema)
- Columns:
  - `clinic_entity_id` (e.g. `MDC`, `GLIC`)
  - `realm_id` (QuickBooks company ID)
  - `access_token`
  - `refresh_token`
  - `access_token_expires_at`
  - `refresh_token_expires_at`
  - `scopes`
  - `environment` (`sandbox` / `production`)
  - `created_at`
  - `updated_at`

The ETL layer reads from this table to know which QBO companies are active and how to authenticate.

#### 5. Implement QuickBooks Extraction Jobs

Extend the existing ETL pipeline to include QBO extraction tasks:

1. **Job configuration**
   - Add a job type for QBO extraction (e.g. `system_d_quickbooks`).
   - Parameterize by:
     - `clinic_entity_id`
     - `realm_id`
     - Entity type (`invoice`, `payment`, `customer`, `account`, `purchase/expense`)
2. **Incremental extraction**
   - For each entity, track a `last_synced_at` timestamp (per clinic, per entity).
   - Use the QBO `LastUpdatedTime` filter to request only new/changed records since `last_synced_at`.
3. **Landing in raw schema**
   - For each pulled entity, write to:
     - `raw_quickbooks_invoice`
     - `raw_quickbooks_payment`
     - `raw_quickbooks_customer`
     - `raw_quickbooks_account`
     - `raw_quickbooks_expense`
   - Ensure each row includes:
     - `clinic_entity_id`
     - `realm_id`
     - `_loaded_at` (ETL load timestamp)

Schedule these jobs via the existing orchestration mechanism (same scheduler used for OpenDental loads), with separate schedules for MDC and GLIC.

#### 6. Wire Up dbt Models for QBO

Once raw tables are populated, enable the dbt side:

1. **dbt sources**
   - Define `raw_quickbooks_*` as dbt sources in `schema.yml` under the QuickBooks system.
2. **Staging models**
   - Implement the `stg_quickbooks__*` models described earlier for:
     - `invoice`
     - `payment`
     - `customer`
     - `account`
     - `expense`
   - Normalize column names, data types, and add `clinic_entity_id`, `_loaded_at`.
3. **Intermediate and marts**
   - Implement the intermediate and mart models listed in the **Intermediate Layer** and **Mart Layer** sections:
     - `int_quickbooks_*` (revenue, expenses, payment reconciliation, mapping tables)
     - `fact_quickbooks_*` and `mart_*` models for reporting.
4. **dbt orchestration**
   - Extend the existing dbt run commands / orchestration to include the QuickBooks models, typically via:
     - `--select system_d_quickbooks` or tags
   - Ensure dependency graph includes OpenDental where reconciliation is required.

#### 7. Environment-Specific Deployment

For each environment (local, test, clinic, production):

1. Configure environment variables for the QBO app (sandbox vs prod).
2. Run database migrations to create `qbo_connections` and any related config tables.
3. Execute the OAuth flow to connect the appropriate QBO companies (per clinic).
4. Run an initial backfill job to populate historical QBO data into `raw_quickbooks_*`.
5. Execute dbt to build staging/intermediate/marts.
6. Validate with spot checks in QBO UI vs analytics tables.

Once this checklist is complete for an environment, that environment is considered technically “set up” for QBO ingestion and modeling.

### Performance Optimization

1. **Incremental Loading**: Only sync changed records
   - Track `LastUpdatedTime` per entity
   - Query only records modified since last sync

2. **Batch Processing**: Process entities in parallel
   - Run MDC and GLIC extractions simultaneously
   - Use connection pooling for API calls

3. **Indexing**: Optimize query performance
   - Index on `clinic_entity_id` + date columns
   - Index on foreign keys (customer_id, invoice_id)
   - Index on matching keys (invoice_number, payment_date + amount)

---

## Risks & Mitigation

### Risk 1: API Rate Limits
**Mitigation**: Implement request queuing, batch processing, respect rate limits

### Risk 2: OAuth Token Expiration
**Mitigation**: Automatic token refresh, monitoring, alerting

### Risk 3: Data Matching Accuracy
**Mitigation**: Multiple matching strategies, confidence scores, manual mapping table for exceptions

### Risk 4: Entity Separation
**Mitigation**: Strict `clinic_entity_id` tagging, validation tests, entity filtering in all queries

### Risk 5: Schema Changes
**Mitigation**: Store full JSONB metadata, version API responses, handle new fields gracefully

### Risk 6: Historical Data Volume
**Mitigation**: Phased initial load, optimize batch sizes, monitor performance

---

## Success Metrics (Quantitative)

In addition to the [Success Criteria](#success-criteria) above, the following quantitative metrics will be used to validate the integration:

| Metric | Target |
|--------|--------|
| Data completeness | ≥ 95% of QuickBooks transactions extracted |
| Customer/patient matching accuracy | ≥ 90% (with business validation) |
| Payment reconciliation rate | ≥ 95% |
| ETL performance | Daily sync completes within 30 minutes |
| Data quality test pass rate | ≥ 99% |

---

## Next Steps

1. ✅ Architecture specification documented
2. ⏳ **PRIORITY**: Collect answers to [Information Needed from Business Owner](#information-needed-from-business-owner)
3. ⏳ Obtain QuickBooks API credentials or access
4. ⏳ Begin Phase 1 — QuickBooks Data Foundation
5. ⏳ Schedule periodic check-ins with business owner for validation

---

## Appendices

### Appendix A: QuickBooks Online API Authentication Flow

```python
# Example OAuth 2.0 token refresh (pseudo-code)
def refresh_access_token(refresh_token, client_id, client_secret):
    response = requests.post(
        'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data={
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret
        }
    )
    return response.json()['access_token'], response.json()['refresh_token']
```

### Appendix B: Sample QuickBooks Invoice Query

```sql
-- Example: Query invoices from QuickBooks Online API
SELECT * FROM Invoice 
WHERE MetaData.LastUpdatedTime > '2026-01-01T00:00:00'
AND MetaData.LastUpdatedTime < '2026-01-08T23:59:59'
```

### Appendix C: Entity Filtering Example

```sql
-- Example: Entity-specific query in dbt model
SELECT *
FROM {{ ref('fact_quickbooks_invoice') }}
WHERE clinic_entity_id = 'MDC'  -- Or 'GLIC'
AND invoice_date >= '2026-01-01'
```

---

### References

- [QuickBooks Online API Documentation](https://developer.intuit.com/app/developer/qbo/docs/develop)
- [QuickBooks Desktop SDK Documentation](https://developer.intuit.com/app/developer/qbd/docs)
- [QuickBooks OAuth 2.0 Guide](https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization)
- [Intuit Developer Portal](https://developer.intuit.com/)

---

**Document Version**: 1.1  
**Last Updated**: 2026-03-05  
**Status**: Planning — Awaiting Business Owner Input