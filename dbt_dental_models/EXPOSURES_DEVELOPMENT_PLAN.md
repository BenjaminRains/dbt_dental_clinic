# Exposures Development Plan

## Executive Summary

This document outlines the strategic development plan for building out analytical exposures (dashboards and reports) to answer the 6 critical operational questions that drive dental practice success. 

**Current State**: 4 exposures implemented, 17 marts built, 150+ models total  
**Target State**: Complete operational coverage answering all 6 critical questions  
**Estimated Effort**: 40-50 hours total development time  
**Strategic Value**: Operational parity with $500/month SaaS platforms, unlimited customization

**Related Documentation**:
- Technical Architecture: `etl_pipeline/docs/PIPELINE_ARCHITECTURE.md`
- Data Contracts: `etl_pipeline/docs/DATA_CONTRACTS.md`
- Airflow Orchestration: `airflow/README.md`

---

## The 6 Critical Operational Questions

These questions represent the **core operational needs** of a dental practice. Answering them effectively drives revenue, efficiency, and growth.

### 1. Are we fully booked? (Schedule Optimization)
**Business Impact**: Maximize revenue per chair-hour, minimize idle time  
**Key Metrics**: Utilization rate, appointment density, schedule gaps  
**Update Frequency**: Real-time to daily

### 2. Are patients accepting treatment? (Case Acceptance)
**Business Impact**: Treatment plan conversion drives major revenue  
**Key Metrics**: Acceptance rate, pending plans, average plan value  
**Update Frequency**: Daily to weekly

### 3. Are we collecting what we produce? (AR Management)
**Business Impact**: Cash flow, financial health, working capital  
**Key Metrics**: Collection rate, AR aging, DSO, write-offs  
**Update Frequency**: Daily

### 4. Are insurance claims getting paid? (Revenue Cycle)
**Business Impact**: 50-70% of revenue from insurance, payment timing critical  
**Key Metrics**: Claim approval rate, days to payment, aging claims  
**Update Frequency**: Daily

### 5. Are hygiene appointments productive? (Hygiene Optimization)
**Business Impact**: Hygiene is high-margin, preventive care drives retention  
**Key Metrics**: Production per hour, recare conversion, hygiene utilization  
**Update Frequency**: Daily to weekly

### 6. Are we getting new patients? (Growth)
**Business Impact**: Practice growth, replacing natural attrition  
**Key Metrics**: New patient volume, conversion rates, retention  
**Update Frequency**: Weekly to monthly

---

## Current State Analysis

### Implemented Exposures (4)

#### 1. ✅ Executive Dashboard
- **URL**: `http://localhost:3000/`
- **Marts**: `mart_revenue_lost`, `mart_provider_performance`
- **Purpose**: High-level KPI overview
- **Users**: Executive leadership, practice managers
- **Status**: **COMPLETE**

#### 2. ✅ Revenue Analytics Dashboard
- **URL**: `http://localhost:3000/revenue`
- **Marts**: `mart_revenue_lost`
- **Purpose**: Revenue recovery opportunities
- **Users**: Finance team, billing team
- **Status**: **COMPLETE**

#### 3. ✅ Appointment Analytics Dashboard → **Answers Q1**
- **URL**: `http://localhost:3000/appointments`
- **Marts**: `mart_appointment_summary`
- **Purpose**: Scheduling efficiency and capacity planning
- **Users**: Scheduling team, operations, providers
- **Status**: **COMPLETE**
- **Operational Questions Covered**: 
  - ✅ **Q1: Are we fully booked?**
  - Metrics: Utilization rate, schedule density, completion rate, capacity analysis

#### 4. ✅ Patient Management Dashboard
- **URL**: `http://localhost:3000/patients`
- **Marts**: `dim_patient`
- **Purpose**: Patient directory and demographics
- **Users**: Front desk, clinical staff
- **Status**: **COMPLETE**

### Planned Exposures (1)

#### 5. 📋 Accounts Receivable Aging Dashboard
- **URL**: `http://localhost:3000/ar-aging`
- **Marts**: `mart_ar_summary` ✅ (already built!)
- **Purpose**: Collection management and AR aging
- **Users**: Billing team, finance team
- **Status**: **PLANNED - Mart exists, dashboard not built**
- **Maturity**: Low

---

## Gap Analysis by Operational Question

### Q1: Are we fully booked? ✅ **COMPLETE**

**Status**: **100% Coverage - Production Ready**

**Existing Resources**:
- ✅ Mart: `mart_appointment_summary`
- ✅ Intermediate: `int_appointment_schedule`, `int_appointment_metrics`, `int_provider_availability`
- ✅ Exposure: `appointment_analytics_dashboard`

**Key Metrics Available**:
- Time utilization rate (productive hours / scheduled hours)
- Schedule density (appointments per hour)
- Schedule span (first to last appointment)
- Appointment completion rate
- Schedule intensity categorization
- Provider capacity utilization

**Dashboard Features**:
- Summary analytics with date range filtering
- Per-provider performance cards
- Today's schedule view
- Upcoming appointments (7 days)
- Completion/no-show/cancellation tracking

**Gaps**: None - fully operational

**Recommended Actions**: None - maintain and monitor

---

### Q2: Are patients accepting treatment? ⚠️ **50% COMPLETE**

**Status**: **Backend Ready - Frontend Missing**

**Existing Resources**:
- ✅ Intermediate: `int_treatment_plan` (System A: Fee Processing)
- ❌ Mart: **NOT CREATED** - `mart_treatment_plan_summary` needed
- ❌ Exposure: **NOT CREATED**

**What's Missing**:

#### Required: `mart_treatment_plan_summary`
**Effort**: 8-10 hours (mart development)

**Proposed Metrics**:
```yaml
Grain: One row per date + provider + clinic

Volume Metrics:
  - total_treatment_plans
  - accepted_treatment_plans
  - declined_treatment_plans
  - pending_treatment_plans
  - plans_in_progress

Conversion Metrics:
  - acceptance_rate (%)
  - avg_days_to_decision
  - avg_days_to_start_treatment
  
Financial Metrics:
  - total_planned_value ($)
  - accepted_plan_value ($)
  - pending_plan_value ($)
  - avg_plan_value ($)
  
Performance Categories:
  - acceptance_performance (Excellent/Good/Fair/Poor)
  - Based on thresholds: >80% Excellent, >70% Good, >60% Fair, <60% Poor
```

#### Required: Treatment Plan Dashboard
**Effort**: 4-6 hours (frontend development)

**Dashboard Features**:
```
Summary KPIs:
  - Total plans presented (month/quarter)
  - Acceptance rate trending
  - Pending plans requiring follow-up
  - Average plan value

Provider Performance Table:
  - Provider name
  - Plans presented
  - Acceptance rate
  - Average plan value
  - Performance category (color-coded)
  
High-Value Plans Table:
  - Plans >$5,000
  - Status (pending/accepted/declined)
  - Days pending
  - Provider assignment
  - Recommended actions

Charts:
  - Acceptance rate trending (30/60/90 day)
  - Plan value distribution
  - Time to decision analysis
```

**Total Effort**: 12-16 hours  
**Priority**: **HIGH** - Critical clinical and financial metric  
**Dependencies**: None - intermediate models exist

---

### Q3: Are we collecting what we produce? ⚠️ **90% COMPLETE**

**Status**: **Marts Complete - Dashboard Planned**

**Existing Resources**:
- ✅ Mart: `mart_ar_summary` (comprehensive AR aging and collection metrics)
- ✅ Mart: `mart_production_summary` (production and collection rates)
- ✅ Intermediate: System D (AR Analysis), System E (Collection)
- ⚠️ Exposure: `accounts_receivable_aging_dashboard` - **PLANNED** in exposures.yml

**Existing Mart Coverage** (`mart_ar_summary`):
- ✅ AR aging buckets (0-30, 31-60, 61-90, 90+)
- ✅ Aging percentages (`pct_current`, `pct_over_90`)
- ✅ Collection rate (last year)
- ✅ Risk categorization (High/Medium/Moderate/Low)
- ✅ Collection priority scoring (0-100 scale)
- ✅ Payment recency tracking
- ✅ Patient vs insurance breakdown

**What's Missing**:

#### Required: AR Aging Dashboard
**Effort**: 4-6 hours (frontend only - mart exists!)

**Dashboard Features**:
```
Summary KPIs:
  - Total AR outstanding
  - Current (0-30 days) amount and %
  - Over 90 days amount and %
  - Days Sales Outstanding (DSO)
  - Collection rate (trailing 30/90/365 days)

AR Aging Chart:
  - Stacked bar chart by aging bucket
  - Trending over time (30/60/90 day views)
  - Color-coded by risk level

Collection Priority Queue:
  - Patients sorted by collection_priority_score
  - Balance amount
  - Aging risk category
  - Days since last payment
  - Recommended action
  - Provider assignment

Risk Distribution:
  - Pie chart showing risk categories
  - High/Medium/Moderate/Low distribution
  - Drill-down capability

Filters:
  - Date range selector
  - Provider filter
  - Risk category filter
  - Minimum balance threshold
```

**Total Effort**: 4-6 hours  
**Priority**: **MEDIUM-HIGH** - Critical for billing operations  
**Dependencies**: None - mart fully built with excellent coverage

---

### Q4: Are insurance claims getting paid? ⚠️ **40% COMPLETE**

**Status**: **Intermediates Complete - Mart and Dashboard Missing**

**Existing Resources**:
- ✅ Intermediate: `int_claim_details`, `int_claim_payments`, `int_claim_tracking`, `int_claim_snapshot` (System B)
- ✅ Intermediate: `int_insurance_coverage`, `int_insurance_employer`, `int_insurance_eob_attachments`
- ✅ Fact: `fact_claim`
- ❌ Mart: **NOT CREATED** - `mart_insurance_summary` needed
- ❌ Exposure: **NOT CREATED**

**What's Missing**:

#### Required: `mart_insurance_summary`
**Effort**: 12-14 hours (complex mart - multiple intermediates to combine)

**Proposed Metrics**:
```yaml
Grain: One row per date + provider + insurance_carrier

Volume Metrics:
  - total_claims_submitted
  - approved_claims
  - rejected_claims
  - denied_claims
  - pending_claims
  - resubmitted_claims

Performance Metrics:
  - approval_rate (%)
  - rejection_rate (%)
  - avg_days_to_payment
  - avg_days_pending
  
Financial Metrics:
  - total_claim_amount ($)
  - approved_amount ($)
  - rejected_amount ($)
  - pending_amount ($)
  - payment_variance (expected vs actual)
  
Aging Metrics:
  - claims_0_30_days
  - claims_31_60_days
  - claims_over_60_days
  - claims_over_90_days
  
By Carrier Analysis:
  - carrier_approval_rate
  - carrier_avg_payment_days
  - carrier_rejection_reasons
  
Performance Categories:
  - claim_performance (Excellent/Good/Fair/Poor)
  - carrier_reliability (Excellent/Good/Fair/Poor)
```

**Data Sources**:
```sql
-- Build from existing intermediates
SELECT
  date_id,
  provider_id,
  insurance_carrier_id,
  
  -- Aggregate from int_claim_details
  COUNT(DISTINCT claim_id) as total_claims,
  
  -- Status breakdown from int_claim_tracking
  SUM(CASE WHEN claim_status = 'Received' THEN 1 ELSE 0 END) as paid_claims,
  
  -- Financial aggregations from int_claim_payments
  SUM(claim_fee_billed) as total_claim_amount,
  SUM(ins_pay_amount) as approved_amount,
  
  -- Timing from int_claim_details
  AVG(days_to_payment) as avg_days_to_payment
  
FROM {{ ref('int_claim_details') }}
LEFT JOIN {{ ref('int_claim_payments') }} USING (claim_id)
LEFT JOIN {{ ref('int_claim_tracking') }} USING (claim_id)
GROUP BY date_id, provider_id, insurance_carrier_id
```

#### Required: Insurance Claims Dashboard
**Effort**: 6-8 hours (frontend development)

**Dashboard Features**:
```
Summary KPIs:
  - Total claims submitted (month/quarter)
  - Approval rate trending
  - Average days to payment
  - Total pending amount
  - Rejection rate

Claims Status Table:
  - Claim date
  - Patient name
  - Insurance carrier
  - Claim amount
  - Status (color-coded chips)
  - Days pending
  - Action required

Aging Analysis:
  - Claims by aging bucket (0-30, 31-60, 60+ days)
  - Stacked bar chart trending
  - Carrier comparison

Carrier Performance:
  - Approval rate by carrier
  - Days to payment by carrier
  - Rejection reasons by carrier
  - Carrier reliability scoring

Rejection Analysis:
  - Common rejection reasons
  - Rejection trend over time
  - Resubmission success rate
  - Action recommendations

Filters:
  - Date range
  - Provider
  - Insurance carrier
  - Claim status
  - Minimum claim amount
```

**Total Effort**: 18-22 hours  
**Priority**: **HIGH** - Major revenue source (50-70% of collections)  
**Dependencies**: Complex - requires combining multiple intermediate models

---

### Q5: Are hygiene appointments productive? ⚠️ **70% COMPLETE**

**Status**: **Marts Complete - Dashboard Missing**

**Existing Resources**:
- ✅ Mart: `mart_hygiene_retention`
- ✅ Mart: `mart_appointment_summary` (includes hygiene metrics)
- ✅ Intermediate: `int_recall_management` (System I)
- ❌ Exposure: **NOT CREATED** - Dedicated hygiene dashboard

**Existing Mart Coverage**:

**From `mart_hygiene_retention`**:
- Hygiene-specific retention metrics
- Recare scheduling performance
- Patient recall tracking

**From `mart_appointment_summary`** (filterable by hygiene providers):
- `hygiene_appointments` count
- Time utilization for hygienists
- Completion rates
- Production amounts

**What's Missing**:

#### Optional Enhancement: Hygiene-specific mart (if needed)
**Effort**: 6-8 hours (only if current marts insufficient)

**Proposed**: `mart_hygiene_summary` 
```yaml
Grain: One row per date + hygienist

Productivity Metrics:
  - hygiene_production ($)
  - hygiene_hours
  - production_per_hour ($)
  - avg_production_per_appointment ($)
  
Recare Metrics:
  - recare_appointments_scheduled
  - recare_conversion_rate (%)
  - patients_due_for_cleaning
  - patients_overdue (>6 months)
  
Patient Management:
  - unique_patients_seen
  - new_hygiene_patients
  - returning_patients
  - patient_retention_rate (%)
  
Quality Metrics:
  - perio_charting_completion_rate (%)
  - patient_education_delivery_rate (%)
  - fluoride_treatment_rate (%)
  
Performance Categories:
  - hygiene_performance (Excellent/Good/Fair/Poor)
```

#### Required: Hygiene Productivity Dashboard
**Effort**: 3-4 hours (frontend - can use existing marts)

**Dashboard Features**:
```
Summary KPIs:
  - Total hygiene production (month)
  - Production per hour (by hygienist)
  - Recare conversion rate
  - Patients overdue for cleaning

Hygienist Performance Table:
  - Hygienist name
  - Appointments completed
  - Production amount
  - Production per hour
  - Utilization rate
  - Performance category

Recare Management:
  - Patients due this month
  - Patients overdue
  - Recare conversion trending
  - Next appointment scheduled rate

Production Trending:
  - Daily/weekly/monthly hygiene production
  - Per-hygienist comparison
  - Production per hour trending
  
Filters:
  - Date range
  - Hygienist
  - Performance category
```

**Total Effort**: 3-4 hours (using existing marts)  
OR 9-12 hours (if building new mart first)  
**Priority**: **MEDIUM** - Important revenue driver  
**Dependencies**: Low - existing marts likely sufficient

---

### Q6: Are we getting new patients? ⚠️ **70% COMPLETE**

**Status**: **Marts Complete - Dashboard Missing**

**Existing Resources**:
- ✅ Mart: `mart_new_patient`
- ✅ Mart: `mart_patient_retention`
- ✅ Intermediate: System I (Patient Management)
- ❌ Exposure: **NOT CREATED**

**Existing Mart Coverage**:

**From `mart_new_patient`**:
- New patient volume tracking
- First appointment metrics
- New patient sources (if captured)

**From `mart_patient_retention`**:
- Patient retention rates
- Active patient counts
- Churn analysis

**What's Missing**:

#### Required: New Patient Acquisition Dashboard
**Effort**: 3-4 hours (frontend only - marts exist)

**Dashboard Features**:
```
Summary KPIs:
  - New patients (month/quarter/year)
  - New patient growth rate (%)
  - Conversion rate (scheduled → active)
  - Average first appointment value
  - Retention rate (30/90/180 days)

New Patient Volume Chart:
  - Monthly trending
  - Year-over-year comparison
  - Seasonal pattern analysis

Acquisition Sources (if captured):
  - Referral source breakdown
  - Marketing campaign effectiveness
  - Online vs walk-in
  - Doctor referrals

Conversion Funnel:
  - Leads → Scheduled → Showed → Active
  - Drop-off at each stage
  - Time to conversion

Retention Analysis:
  - 30-day retention rate
  - 90-day retention rate
  - 1-year retention rate
  - Reasons for churn (if captured)

Filters:
  - Date range
  - Acquisition source
  - Provider assigned
```

**Total Effort**: 3-4 hours  
**Priority**: **MEDIUM** - Growth metric, executive visibility  
**Dependencies**: None - marts fully built

---

## Development Roadmap

### Phase 1: Quick Wins (Next 2-4 Weeks)

**Goal**: Maximize coverage with minimal effort - build dashboards for existing marts

**Deliverables**:

#### 1. AR Aging Dashboard (Week 1)
- **Effort**: 4-6 hours
- **Answers**: Q3 (Are we collecting?)
- **Value**: CRITICAL - daily operational need
- **Complexity**: Low - mart fully built
- **Users**: Billing team (daily use)

#### 2. New Patient Dashboard (Week 2)
- **Effort**: 3-4 hours
- **Answers**: Q6 (Getting new patients?)
- **Value**: HIGH - growth metric
- **Complexity**: Low - mart fully built
- **Users**: Executive team, marketing

#### 3. Hygiene Dashboard (Week 3)
- **Effort**: 3-4 hours
- **Answers**: Q5 (Hygiene productive?)
- **Value**: HIGH - revenue optimization
- **Complexity**: Low - can use existing marts
- **Users**: Hygiene team, operations

**Phase 1 Results**:
- **Time Investment**: 10-14 hours
- **Coverage**: 4/6 questions answered (67%)
- **Operational Value**: ~80% of daily decision-making needs
- **Cost**: $0 additional (using existing infrastructure)

---

### Phase 2: High-Value New Development (Month 2)

**Goal**: Fill critical gaps requiring new mart development

**Deliverables**:

#### 4. Treatment Plan Mart + Dashboard (Weeks 5-7)
- **Effort**: 12-16 hours total
  - Mart development: 8-10 hours
  - Dashboard: 4-6 hours
- **Answers**: Q2 (Treatment acceptance?)
- **Value**: CRITICAL - major revenue driver
- **Complexity**: Medium - need to build mart from intermediate
- **Users**: Providers, treatment coordinators, finance

**Development Steps**:
1. Build `mart_treatment_plan_summary` (8-10 hours)
   - Source from `int_treatment_plan`
   - Define grain (date + provider + clinic)
   - Calculate acceptance rate, avg plan value
   - Add performance categorization
   - Add dbt tests (contract validation)

2. Build dashboard (4-6 hours)
   - Create API endpoints
   - Build React components
   - Add filters and interactivity
   - Connect to mart

**Phase 2 Results**:
- **Time Investment**: 12-16 hours
- **Coverage**: 5/6 questions answered (83%)
- **Gap Closure**: Answers critical case acceptance question

---

### Phase 3: Complex Insurance Analytics (Month 3)

**Goal**: Complete coverage with most complex development

**Deliverables**:

#### 5. Insurance Claim Mart + Dashboard (Weeks 9-12)
- **Effort**: 18-22 hours total
  - Mart development: 12-14 hours
  - Dashboard: 6-8 hours
- **Answers**: Q4 (Claims getting paid?)
- **Value**: CRITICAL - 50-70% of revenue
- **Complexity**: HIGH - complex joins across System B intermediates
- **Users**: Billing team, finance, insurance coordinator

**Development Steps**:
1. Build `mart_insurance_summary` (12-14 hours)
   - Source from multiple System B intermediates:
     - `int_claim_details` (base claim info)
     - `int_claim_payments` (payment amounts)
     - `int_claim_tracking` (status tracking)
     - `int_claim_snapshot` (point-in-time data)
   - Complex aggregations by carrier and status
   - Calculate approval rates and timing metrics
   - Build aging analysis for claims
   - Add comprehensive dbt tests
   - Handle complex payment matching logic

2. Build dashboard (6-8 hours)
   - Create API endpoints (more complex than others)
   - Build React components with drill-down
   - Add multi-level filtering
   - Implement carrier comparison views
   - Add rejection reason analysis

**Phase 3 Results**:
- **Time Investment**: 18-22 hours
- **Coverage**: 6/6 questions answered (100%)
- **Completion**: Full operational dashboard suite

---

## Total Development Summary

### Effort by Phase

| Phase | Deliverables | Effort | Questions Answered | Coverage |
|-------|-------------|--------|-------------------|----------|
| **Current** | 4 exposures | Completed | 1/6 (Q1) | 17% |
| **Phase 1** | 3 dashboards | 10-14 hours | +3 (Q3, Q5, Q6) | 67% |
| **Phase 2** | 1 mart + dashboard | 12-16 hours | +1 (Q2) | 83% |
| **Phase 3** | 1 mart + dashboard | 18-22 hours | +1 (Q4) | 100% |
| **Total** | 9 exposures | 40-52 hours | 6/6 | 100% |

### Value Delivery Timeline

```
Week 0:  ████░░░░░░ 17% coverage (Current - Q1)
Week 2:  ████████░░ 50% coverage (+ Q3 AR Aging)
Week 3:  ████████░░ 67% coverage (+ Q6 New Patients)
Week 4:  ████████░░ 67% coverage (+ Q5 Hygiene)
Week 7:  ████████░░ 83% coverage (+ Q2 Treatment Plans)
Week 12: ██████████ 100% coverage (+ Q4 Insurance)
```

### Cost-Benefit Analysis

**Development Investment**:
- Total time: 40-52 hours
- At $50/hour opportunity cost: $2,000-2,600
- At $100/hour opportunity cost: $4,000-5,200

**Alternative (Practice By Numbers)**:
- Cost: $500/month = $6,000/year
- 5-year cost: $30,000
- Features: 1000s of KPIs (but how many do you actually use?)

**Your Platform After Full Build**:
- Infrastructure: $100/month = $1,200/year
- 5-year cost: $6,000 + $2,000-5,200 development = $8,000-11,200
- **5-year savings: $19,000-22,000**
- Features: Exactly what you need + unlimited customization

**Break-even**: 4-10 months vs PBN subscription

---

## Technical Considerations

### Frontend Technology Stack

**Current Stack** (from your frontend directory):
- React + TypeScript
- Vite build tool
- Material-UI components
- API integration to FastAPI backend

**Estimated Development Time by Component Type**:
- Simple KPI cards: 0.5-1 hour
- Data table with filtering: 2-3 hours
- Chart/visualization: 1-2 hours
- Dashboard layout: 1-2 hours
- API endpoint: 0.5-1 hour
- Testing and polish: 1-2 hours

**Per Dashboard Average**: 4-6 hours

### Backend/Mart Development

**Current Stack** (from your dbt project):
- dbt with PostgreSQL
- 150+ existing models to reference
- Comprehensive testing framework
- Well-documented intermediate layer

**Estimated Development Time by Mart Complexity**:
- Simple mart (single source): 4-6 hours
- Medium mart (2-3 sources): 8-10 hours
- Complex mart (4+ sources, complex logic): 12-16 hours

### Data Refresh Strategy

**Current Approach** (from exposures.yml):
- Manual refresh (user-initiated dbt runs)
- **Planned**: Automated daily refresh at 6:00 AM via Airflow

**With Airflow** (when implemented):
```
3:00 AM: ETL Pipeline DAG runs
  ↓ Populates PostgreSQL raw schema
6:00 AM: dbt Build DAG runs
  ↓ Transforms raw → marts
7:00 AM: Dashboards show fresh data
```

**Latency**: T+1 day for most metrics (acceptable for daily operations)

---

## Prioritization Framework

### Decision Criteria for Build Order

**1. Backend Readiness** (Does mart exist?)
- ✅ Mart exists → Quick win (4-6 hours)
- ⚠️ Needs enhancement → Medium effort (8-10 hours)
- ❌ New mart → Higher effort (12-16 hours)

**2. Operational Impact** (How often will it be used?)
- **Daily**: High priority (AR aging, claims)
- **Weekly**: Medium priority (treatment plans, hygiene)
- **Monthly**: Lower priority (new patients, retention)

**3. User Count** (How many people need this?)
- **Team-wide** (10+ users): High priority
- **Department** (3-5 users): Medium priority
- **Individual** (1-2 users): Lower priority

**4. Financial Impact** (Revenue/cost effect)
- **Direct revenue**: High priority (treatment plans, claims)
- **Revenue optimization**: Medium priority (hygiene, scheduling)
- **Cost reduction**: Medium priority (collections, efficiency)

### Recommended Priority Order

| Priority | Exposure | Effort | Impact | Frequency | Users | Reason |
|----------|----------|--------|--------|-----------|-------|--------|
| **1** | AR Aging Dashboard | 4-6h | CRITICAL | Daily | 5+ | Backend ready, daily need, billing operations |
| **2** | Treatment Plan Dashboard | 12-16h | CRITICAL | Daily | 8+ | Major revenue driver, clinical operations |
| **3** | Insurance Claims Dashboard | 18-22h | CRITICAL | Daily | 5+ | 50-70% revenue, complex but essential |
| **4** | New Patient Dashboard | 3-4h | HIGH | Weekly | 5+ | Growth metric, executive visibility, quick win |
| **5** | Hygiene Dashboard | 3-4h | MEDIUM | Weekly | 3+ | Revenue optimization, quick win |
| **6** | Enhanced Treatment Acceptance | 4-6h | MEDIUM | Monthly | 3+ | Deep analysis capability |

---

## Competitive Analysis: You vs Practice By Numbers

### Practice By Numbers Strengths

**What PBN Offers**:
- 1000s of KPIs (breadth)
- Professional UI/UX (design team)
- Industry benchmarks (multi-practice data)
- Zero setup time (plug-and-play)
- Professional support team
- Proven at scale (1000+ offices)

**Their Challenges** (at scale):
- Must support multiple OpenDental versions (v18-v23+)
- Must handle schema variance across 1000+ clients
- Must be flexible (least common denominator)
- Limited customization (multi-tenant constraints)
- Generic dashboards (can't be too specific)

**Their Cost**: $500/month = $6,000/year = $30,000 over 5 years

### Your Platform Strengths

**What You Have**:
- 150+ models (depth)
- 17 marts (strong foundation)
- 9 business systems modeled (comprehensive)
- Full data access (no vendor limitations)
- Unlimited customization
- Complete data privacy/control
- Strong intermediate layer

**Your Advantages**:
- Single schema (can be very specific)
- Custom metrics in hours (not months of feature requests)
- Total data ownership
- Career development (learning data engineering)
- Can integrate with ANY system
- No per-seat licensing

**Your Cost**: ~$100/month infrastructure + your time

### Strategic Positioning

**PBN's Value Proposition**: "We do everything so you don't have to"
- Best for: Non-technical practices, want turnkey solution

**Your Value Proposition**: "I control my data and build exactly what I need"
- Best for: Technical capability, specific needs, cost-conscious

**Not Competitive - Complementary Options**:

You could even use **both**:
- PBN: Industry benchmarking, board presentations, standard reports ($500/month)
- Your platform: Daily operations, custom analytics, deep-dive analysis ($100/month)
- Total: $600/month but best of both worlds

OR more likely:

- Your platform: Everything you actually need ($100/month)
- Savings: $400/month = $4,800/year

---

## Development Guidelines

### Mart Development Standards

**Required Elements** (all marts must have):
```yaml
1. Documentation
   - Business context
   - Technical specifications (grain, sources, refresh)
   - Business logic (calculations, rules)
   - Data quality notes
   
2. Tests
   - Row count validation
   - Percentage fields (0-100%)
   - Financial fields (non-negative)
   - Referential integrity
   - Business rule validation
   
3. Metadata
   - owner, contains_pii, business_process
   - refresh_frequency, business_impact
   - primary_consumers, data_quality_requirements
   
4. Performance
   - Indexes on common query columns
   - Materialization strategy
   - Expected query response time
```

### Dashboard Development Standards

**Required Elements** (all dashboards must have):
```yaml
1. Summary KPIs
   - 4-6 key metrics at top
   - Color-coded performance indicators
   - Trend arrows (up/down)
   
2. Main Content
   - Primary table or chart
   - Sortable, filterable
   - Drill-down capability
   
3. Filters
   - Date range selector
   - Provider filter (if applicable)
   - Status/category filter
   
4. Actions
   - Export to CSV
   - Print-ready view
   - Refresh data button
   
5. Performance
   - Load time <3 seconds
   - Responsive design
   - Mobile-friendly
```

### Testing Requirements

**For Each New Mart**:
```yaml
Contract Tests:
  - test_mart_has_required_fields
  - test_percentages_in_valid_range
  - test_financial_fields_non_negative
  - test_grain_uniqueness
  - test_referential_integrity

Business Logic Tests:
  - test_calculations_correct
  - test_categorization_logic
  - test_aggregation_accuracy

Data Quality Tests:
  - test_no_null_in_required_fields
  - test_reasonable_value_ranges
  - test_date_ranges_valid
```

**For Each New Dashboard**:
```yaml
Frontend Tests:
  - test_renders_without_errors
  - test_displays_correct_data
  - test_filters_work_correctly
  - test_charts_render_properly
  - test_responsive_design

Integration Tests:
  - test_api_endpoints_return_data
  - test_data_matches_mart
  - test_error_handling
```

---

## Resource Requirements

### Development Environment

**Required**:
- dbt development environment (already have)
- PostgreSQL access (already have)
- React/TypeScript development setup (already have)
- FastAPI backend (already have)

**Optional**:
- Metabase/Superset for quick prototyping
- Figma for dashboard mockups

### Infrastructure

**Current** (adequate for all planned development):
- PostgreSQL (marts storage)
- FastAPI (API layer)
- React frontend (dashboard layer)
- Airflow (when implemented - scheduled refresh)

**No additional infrastructure needed**

### Skills Required

**For Mart Development**:
- SQL (intermediate to advanced)
- dbt (model development, testing)
- Business logic understanding

**For Dashboard Development**:
- React/TypeScript (intermediate)
- Material-UI (basic)
- Chart libraries (Recharts/similar)
- REST API integration

**You have all required skills** based on existing codebase quality

---

## Success Metrics

### Phase 1 Success (After Quick Wins)

**Quantitative**:
- 4/6 operational questions answered (67%)
- 7 total exposures (vs 4 current)
- Daily dashboard usage >5 users

**Qualitative**:
- Billing team uses AR dashboard daily
- Executive team checks growth metrics weekly
- Hygiene team tracks productivity

### Phase 2 Success (After Treatment Plans)

**Quantitative**:
- 5/6 operational questions answered (83%)
- Treatment plan acceptance rate tracked
- Pending plans managed proactively

**Qualitative**:
- Treatment coordinators use dashboard daily
- Acceptance rate improves (visibility drives improvement)
- Providers see their conversion metrics

### Phase 3 Success (Full Coverage)

**Quantitative**:
- 6/6 operational questions answered (100%)
- All critical operational metrics tracked
- 9-10 total exposures

**Qualitative**:
- Platform replaces $500/month SaaS need
- Team prefers custom dashboards (fit workflows exactly)
- Decision-making speed improves
- Data-driven culture established

### Long-term Success (6-12 Months)

**Platform Maturity**:
- Airflow orchestration live (automated refresh)
- Contract tests passing (data quality validated)
- 10-15 exposures total (grew organically)
- Team trained on self-service analytics

**Business Impact**:
- Measurable improvement in collection rate
- Increased treatment acceptance
- Better schedule utilization
- Faster claim payments
- **ROI**: $5,000-6,000/year savings vs PBN

---

## Risks & Mitigation

### Development Risks

**Risk**: Time estimates too low (scope creep)
- **Mitigation**: Start with MVP, iterate
- **Mitigation**: Use existing patterns from current dashboards
- **Mitigation**: Timebox each dashboard (don't over-engineer)

**Risk**: Data quality issues discovered during development
- **Mitigation**: Contract tests catch early (you just built these!)
- **Mitigation**: Document assumptions clearly
- **Mitigation**: Build with graceful degradation

**Risk**: Maintenance burden grows over time
- **Mitigation**: Good documentation (you're doing this!)
- **Mitigation**: Automated testing (contract tests)
- **Mitigation**: Airflow automation (planned)

### Operational Risks

**Risk**: Team doesn't adopt new dashboards
- **Mitigation**: Involve users in design (ask what they need)
- **Mitigation**: Training and documentation
- **Mitigation**: Make dashboards genuinely useful (not vanity metrics)

**Risk**: Performance degradation as data grows
- **Mitigation**: Proper indexing on marts
- **Mitigation**: Incremental dbt models where appropriate
- **Mitigation**: Query optimization

**Risk**: Complexity becomes unmaintainable
- **Mitigation**: Keep dashboards simple and focused
- **Mitigation**: Don't try to match PBN's 1000 KPIs
- **Mitigation**: Build only what you'll actually use

---

## Next Steps

### Immediate Actions (This Week)

1. **Review this plan** with key stakeholders
   - Validate the 6 questions are correct
   - Confirm dashboard priorities
   - Get input on must-have features

2. **Set up development sprint**
   - Block time for Phase 1 (10-14 hours)
   - Identify user testers for each dashboard
   - Create dashboard mockups/wireframes

3. **Validate existing marts**
   - Run contract tests on `mart_ar_summary`
   - Run contract tests on `mart_new_patient`
   - Confirm data quality for dashboard use

### Week 1: AR Aging Dashboard

**Monday-Tuesday** (4-6 hours):
- Design dashboard layout
- Build API endpoints (`/ar/summary`, `/ar/aging-detail`)
- Create React components
- Test with real data

**Wednesday**:
- User testing with billing team
- Refinements

**Thursday**:
- Deploy to development
- Document usage

**Friday**:
- Deploy to production
- Train users

### Weeks 2-4: Continue Phase 1

Follow same pattern for new patient and hygiene dashboards

### Month 2: Treatment Plan Development

Build out mart first, then dashboard

### Month 3: Insurance Analytics

Complete the suite

---

## Conclusion

You have a **strong analytical foundation** with 17 marts and 150+ models covering 9 business systems. The gap isn't in **analytical capability** - it's in **exposing that capability** through user-friendly dashboards.

**Current Coverage**: 1/6 critical questions (17%)
**With 40 hours development**: 6/6 critical questions (100%)

**You're not competing with PBN on breadth** (their 1000 KPIs) - you're **exceeding them on depth and customization** for your specific needs.

**Key Insight**: Your 17 marts can generate 100+ KPIs easily. The question isn't "how many KPIs can we have?" but "which KPIs do we actually need to make better decisions?"

Focus on the 6 questions that matter, build the exposures that answer them, and you'll have operational parity with PBN at 1/5 the cost with unlimited upside.

---

**Document Version**: 1.0  
**Created**: 2025-10-22  
**Next Review**: After Phase 1 completion  
**Owner**: Data Engineering  
**Status**: Development roadmap defined, ready to execute

