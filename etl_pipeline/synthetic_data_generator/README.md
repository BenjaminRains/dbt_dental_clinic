# 🦷 Dental Practice Synthetic Data Generator

**Complete synthetic data generator for dental analytics dbt models**

Version: 1.0.0  
Author: Benjamin Rains  
Date: January 2025

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Generated Tables](#generated-tables)
- [Business Logic](#business-logic)
- [dbt Cloud Deployment](#dbt-cloud-deployment)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This generator creates **completely fake but realistic** dental practice data for:
- ✅ Testing 150+ dbt models without real PHI
- ✅ Deploying public dbt Cloud documentation
- ✅ Demonstrating data engineering skills in portfolio
- ✅ 100% HIPAA compliant (zero real patient data)

### Key Focus Areas

**Production Dashboard:**
- Daily/MTD/YTD production metrics
- Provider productivity analysis
- Fee schedule validation
- Procedure completion tracking

**AR Analysis Dashboard:**
- Aging bucket calculations (0-30, 31-60, 61-90, 90+ days)
- Collection rate tracking
- Insurance vs patient responsibility
- Payment allocation analytics

---

## ✨ Features

### Data Quality
- **Referential Integrity:** All foreign keys reference valid records
- **Date Consistency:** Logical date sequences (created < updated, procedure < payment)
- **Financial Balancing:** AR = Production - Collections - Adjustments (exactly)
- **Status Consistency:** Completed appointments have procedures, claims, payments

### Business Logic
- **Realistic Patterns:** 75% appointment completion, 60% insurance coverage
- **Fee Validation:** 80% of procedures match standard fees exactly
- **Adjustment Types:** Exact type IDs matching your dbt models (186, 188, 474, etc.)
- **Payment Allocation:** Splits must reference procedure OR adjustment (never both NULL)
- **Transfer Payments:** Matching pairs with unearned_type 0 and 288

### Scale
- **5,000 patients** with realistic age distribution (5% pediatric, 70% adult, 25% senior)
- **15,000 appointments** with provider schedules and operatory assignments
- **20,000 procedures** with proper fee schedule linkage
- **~135,000 total rows** across 50+ tables

---

## 🏗️ Architecture

### Phase-Based Generation

The generator processes data in six sequential phases to maintain referential integrity:

```
Phase 1: Foundation
├── Clinics (5)
├── Providers (12)
├── Operatories (20)
├── Procedure Codes (200 CDT codes)
├── Definitions (500 lookup values)
├── Fee Schedules (3)
└── Fees (600 fee amounts)

Phase 2: Patients
├── Patients (5,000)
├── Patient Notes (5,000 - 1:1 relationship)
├── Patient Links (1,000 family relationships)
└── Zipcodes (100 reference)

Phase 3: Insurance
├── Carriers (15)
├── Employers (50)
├── Insurance Plans (30)
├── Subscribers (2,000)
├── Patient Plans (3,500)
└── Benefits (1,500)

Phase 4: Clinical
├── Appointments (15,000)
├── Appointment Types (15)
├── Procedures (20,000)
├── Procedure Notes (10,000)
└── Recalls (3,000)

Phase 5: Financial
├── Claims (8,000)
├── Claim Procedures (25,000)
├── Claim Payments (5,000)
├── Payments (12,000)
├── Payment Splits (15,000)
└── Adjustments (2,000)

Phase 6: Supporting
├── Communication Logs (8,000)
├── Documents (5,000)
├── Referrals (20)
└── Tasks (1,000)
```

---

## 📦 Installation

### Prerequisites

- Python 3.8+
- PostgreSQL 11.6+ or 13+
- Virtual environment (recommended)

### Setup

```bash
# 1. Clone repository
git clone https://github.com/BenjaminRains/dbt_dental_clinic.git
cd dbt_dental_clinic/etl_pipeline/synthetic_data_generator

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create database
createdb opendental_demo

# 5. Create schema
psql -d opendental_demo -c "CREATE SCHEMA IF NOT EXISTS raw;"

# 6. Run schema creation script (creates all tables)
psql -d opendental_demo -f create_tables.sql
```

### Requirements File

```txt
# requirements.txt
psycopg2-binary==2.9.9
faker==22.0.0
python-dateutil==2.8.2
```

---

## 🚀 Quick Start

### Basic Usage

```bash
# Generate with default settings (5,000 patients)
python main.py

# Generate with custom patient count
python main.py --patients 1000

# Generate with custom database
python main.py \
  --db-host localhost \
  --db-name opendental_demo \
  --db-user postgres \
  --db-password yourpassword

# Generate with custom date range
python main.py --start-date 2023-06-01
```

### Expected Output

```
============================================================
DENTAL PRACTICE SYNTHETIC DATA GENERATOR
============================================================
Target database: opendental_demo
Date range: 2023-01-01 to 2025-01-07
Patients: 5,000
Appointments: 15,000
Procedures: 20,000
============================================================

[Phase 1] Generating Foundation Data...
✓ Generated 10 users
✓ Generated 5 clinics
✓ Generated 12 providers
✓ Generated 20 operatories
✓ Generated 46 procedure codes
✓ Generated 25 definitions
✓ Generated 3 fee schedules
✓ Generated 138 fees

[Phase 2] Generating Patient Data...
✓ Generated 5,000 patients
✓ Generated 5,000 patient notes
✓ Generated 1,000 family links
✓ Generated 100 zipcodes

... (continues through all phases) ...

============================================================
✓ Data generation completed successfully!
============================================================

Generated Record Counts:
----------------------------------------
  adjustment                    :    2,000
  appointment                   :   15,000
  appointmenttype               :       15
  benefit                       :    1,500
  carrier                       :       15
  claim                         :    8,000
  claimpayment                  :    5,000
  claimproc                     :   25,000
  clinic                        :        5
  ... (continues) ...
  TOTAL                         :  135,423
```

### Generation Time

- **Small (1,000 patients):** ~5 minutes
- **Medium (5,000 patients):** ~15-20 minutes
- **Large (10,000 patients):** ~30-40 minutes

---

## 🎨 Using with Existing Frontend

### Complete Isolation Strategy

Your synthetic data in `opendental_demo` stays **completely separate** from production (`opendental_analytics`). Here's how to use it with your existing React frontend and FastAPI backend:

### Architecture

```
PRODUCTION (untouched):
┌─────────────────────────┐
│ opendental_analytics    │ ← Your production data (safe!)
│ - raw, staging, marts   │
└─────────────────────────┘

DEMO (isolated):
┌─────────────────────────┐
│ opendental_demo         │ ← Synthetic data + dbt transforms
│ - raw (synthetic)       │ ✅ You generated this!
│ - staging               │ ← dbt creates these
│ - intermediate          │ ← dbt creates these
│ - marts                 │ ← API reads from here
└───────────┬─────────────┘
            │
            ↓
┌─────────────────────────┐
│ FastAPI (test mode)     │ ← Reads from opendental_demo
└───────────┬─────────────┘
            │
            ↓
┌─────────────────────────┐
│ React Frontend          │ ← Your existing frontend!
└─────────────────────────┘
```

### Step-by-Step Setup

#### 1. Run dbt Against Demo Database

Transform synthetic data within `opendental_demo`:

```powershell
# Navigate to dbt project
cd dbt_dental_models

# Set environment variable to point at demo database
$env:POSTGRES_ANALYTICS_DB = "opendental_demo"

# Run dbt to create staging, intermediate, and marts schemas
dbt run --target local

# Run tests to validate data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # View at http://localhost:8080
```

This creates all transformed schemas **inside** `opendental_demo`:
- `raw` (your synthetic data)
- `staging` (dbt staging models)
- `intermediate` (dbt intermediate models)
- `marts` (dbt mart models - what the API reads)

#### 2. Configure API for Demo Database

Create API environment file for demo:

```powershell
# From project root
cd ..

# Create .env_api_test file
@"
# API Environment Configuration
API_ENVIRONMENT=test

# Demo database connection (points to opendental_demo)
TEST_POSTGRES_ANALYTICS_HOST=localhost
TEST_POSTGRES_ANALYTICS_PORT=5432
TEST_POSTGRES_ANALYTICS_DB=opendental_demo
TEST_POSTGRES_ANALYTICS_USER=postgres
TEST_POSTGRES_ANALYTICS_PASSWORD=yourpassword

# API settings
API_PORT=8000
API_HOST=0.0.0.0
API_DEBUG=true
API_CORS_ORIGINS=http://localhost:5173,http://localhost:3000
"@ | Out-File -FilePath .env_api_test -Encoding UTF8
```

#### 3. Start API Server

```powershell
cd api

# Activate virtual environment if needed
# .\venv\Scripts\activate

# Set environment to test (reads from .env_api_test)
$env:API_ENVIRONMENT = "test"

# Start FastAPI server
python -m uvicorn main:app --reload --port 8000

# API will be available at http://localhost:8000
# API docs at http://localhost:8000/docs
```

#### 4. Start Frontend

```powershell
# Open NEW terminal window
cd frontend

# Install dependencies (first time only)
npm install

# Start development server
npm run dev

# Frontend opens at http://localhost:5173
```

#### 5. View Your Data!

Open browser to `http://localhost:5173` and explore:
- **Dashboard:** KPIs, production metrics, AR summary
- **Patients:** 5,000 synthetic patients with realistic data
- **Appointments:** 10,354 appointments with providers and procedures
- **Revenue:** Financial analytics with proper AR calculations
- **Providers:** 12 providers with productivity metrics

### Benefits

✅ **Complete Isolation** - Production data never touched  
✅ **No ETL Needed** - Data already in PostgreSQL  
✅ **Use Existing Frontend** - No code changes required  
✅ **Full dbt Testing** - Tests your actual transformations  
✅ **Easy Cleanup** - Just drop `opendental_demo` when done  
✅ **Portfolio Ready** - Screenshot dashboards for resume  

### Environment Variables Reference

| Variable | Production | Demo (Test) |
|----------|-----------|-------------|
| `POSTGRES_ANALYTICS_DB` | `opendental_analytics` | `opendental_demo` |
| `API_ENVIRONMENT` | `clinic` | `demo` / `test` |
| Environment File | `.env_api_clinic` | `.env_api_demo` / `.env_api_test` |

### Switching Between Environments

```powershell
# Use DEMO data
$env:API_ENVIRONMENT = "test"
python -m uvicorn main:app --reload

# Use PRODUCTION data (be careful!)
$env:API_ENVIRONMENT = "production"
python -m uvicorn main:app --reload
```

### Cleanup

When done testing:

```sql
-- Drop demo database (keeps production safe)
DROP DATABASE opendental_demo;
```

---

## ⚙️ Configuration

### Method 1: .env_demo File (Recommended)

```bash
# Copy template and configure
cd etl_pipeline/synthetic_data_generator
copy .env_demo.template .env_demo

# Edit .env_demo
DEMO_POSTGRES_HOST=localhost
DEMO_POSTGRES_PORT=5432
DEMO_POSTGRES_DB=opendental_demo
DEMO_POSTGRES_USER=postgres
DEMO_POSTGRES_PASSWORD=yourpassword

# Data generation settings (optional)
DEMO_START_DATE=2023-01-01
DEMO_NUM_PATIENTS=5000
DEMO_NUM_APPOINTMENTS=15000
DEMO_NUM_PROCEDURES=20000

# Then run without arguments (reads from .env_demo)
.\generate.ps1 -Patients 100
```

### Method 2: Command Line Arguments

```bash
# Override any setting via CLI
python main.py \
  --db-host localhost \
  --db-name opendental_demo \
  --db-user postgres \
  --db-password yourpassword \
  --patients 5000 \
  --start-date 2023-01-01
```

### Config Parameters (Reference)

```python
# Default configuration in main.py
class GeneratorConfig:
    # Database (always opendental_demo for safety)
    db_host = "localhost"
    db_port = 5432
    db_name = "opendental_demo"  # FIXED - never changes
    db_user = "postgres"
    db_password = "postgres"
    
    # Data Volumes
    num_patients = 5000
    num_appointments = 15000
    num_procedures = 20000
    
    # Date Range (CRITICAL: dbt filters to >= 2023-01-01)
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    
    # Probabilities
    insurance_coverage_rate = 0.60  # 60% have insurance
    appointment_completion_rate = 0.75  # 75% completed
    standard_fee_match_rate = 0.80  # 80% match standard fees
```

---

## 📊 Generated Tables

### Critical Path (30 Tables) - Required for Core Dashboards

| Table | Rows | Purpose |
|-------|------|---------|
| **Foundation (7)** |||
| clinic | 5 | Clinic locations |
| provider | 12 | Dentists, hygienists |
| operatory | 20 | Dental chairs/rooms |
| procedurecode | 200 | CDT procedure codes |
| definition | 500 | Lookup values |
| feesched | 3 | Fee schedules |
| fee | 600 | Fee amounts |
| **Patients (3)** |||
| patient | 5,000 | Patient demographics |
| patientnote | 5,000 | Patient notes (1:1) |
| zipcode | 100 | Zipcode reference |
| **Insurance (6)** |||
| carrier | 15 | Insurance companies |
| employer | 50 | Employers |
| insplan | 30 | Insurance plans |
| inssub | 2,000 | Subscribers |
| patplan | 3,500 | Patient-plan links |
| benefit | 1,500 | Coverage rules |
| **Clinical (4)** |||
| appointment | 15,000 | Appointments |
| appointmenttype | 15 | Appt categories |
| procedurelog | 20,000 | Procedures |
| recall | 3,000 | Hygiene recalls |
| **Financial (6)** |||
| claim | 8,000 | Insurance claims |
| claimproc | 25,000 | Claim procedures |
| claimpayment | 5,000 | Insurance payments |
| payment | 12,000 | Patient payments |
| paysplit | 15,000 | Payment allocations |
| adjustment | 2,000 | Discounts, writeoffs |
| **Supporting (4)** |||
| commlog | 8,000 | Communications |
| userod | 30 | System users |
| document | 5,000 | Document metadata |
| referral | 20 | Referral sources |

**Total: 30 core tables, ~135,000 rows**

---

## 🧮 Business Logic

### Critical Rules Implemented

#### 1. Financial Balancing

```python
# EXACT formula from int_ar_balance.sql
current_balance = (
    procedure_fee 
    - insurance_payment_amount 
    - patient_payment_amount 
    - total_adjustment_amount
)

# Must equal zero for fully paid procedures
assert current_balance >= -0.01  # Allow rounding tolerance
```

#### 2. Adjustment Type IDs

```python
# MUST match int_adjustments.sql expectations
ADJUSTMENT_TYPES = {
    186: 'senior_discount',
    188: 'insurance_writeoff',
    474: 'provider_discount',
    475: 'provider_discount',
    472: 'employee_discount',
    485: 'employee_discount',
    9: 'cash_discount',
    # ... (see main.py for complete list)
}
```

#### 3. Payment Split Allocation

```python
# From int_payment_split.sql
# MUST have exactly ONE of these (never all NULL):
paysplit = {
    'ProcNum': 123,        # Links to procedure
    'AdjNum': None,        # OR links to adjustment
    'PayPlanChargeNum': None  # OR links to payment plan
}
```

#### 4. Fee Schedule Matching

```python
# From int_procedure_complete.sql
# 80% of procedures match standard fee exactly
if random.random() < 0.80:
    proc_fee = standard_fee  # Exact match
else:
    proc_fee = standard_fee * random.uniform(0.90, 1.10)  # ±10%

# Your dbt model validates:
fee_matches_standard = ABS(proc_fee - standard_fee) < 0.01
```

#### 5. Date Sequencing

```python
# All dates must follow logical sequences
appointment.AptDateTime >= '2023-01-01'  # dbt filter
appointment.DateTimeArrived <= appointment.DateTimeSeated
appointment.DateTimeSeated <= appointment.DateTimeDismissed
claim.DateSent >= claim.DateService
payment.payment_date >= procedure.proc_date
```

#### 6. Aging Buckets

```python
# From int_ar_balance.sql
days_outstanding = CURRENT_DATE - procedure_date

aging_bucket = CASE
    WHEN days_outstanding <= 30 THEN '0-30'
    WHEN days_outstanding <= 60 THEN '31-60'
    WHEN days_outstanding <= 90 THEN '61-90'
    ELSE '90+'
END
```

---

## ☁️ dbt Cloud Deployment

### Deploying to Cloud

#### Step 1: Deploy Database to Cloud

**Option A: Render.com (Recommended)**

```bash
# 1. Sign up at render.com
# 2. Create new PostgreSQL instance
#    - Name: opendental-demo
#    - Region: US (closest to you)
#    - Plan: Free (90-day trial, then $7/mo)

# 3. Get connection details from Render dashboard
#    - Host: xxx.render.com
#    - Database: opendental_demo
#    - User: opendental_demo_user
#    - Password: [generated]
#    - Port: 5432

# 4. Run generator with cloud database
python main.py \
  --db-host xxx.render.com \
  --db-name opendental_demo \
  --db-user opendental_demo_user \
  --db-password [your-password]
```

**Option B: Supabase**

```bash
# 1. Sign up at supabase.com
# 2. Create new project
# 3. Get connection string from Settings > Database
# 4. Run generator with Supabase connection
```

#### Step 2: Configure dbt Cloud

```bash
# 1. Sign up at cloud.getdbt.com (free for 1 developer)

# 2. Create new project:
#    - Project name: Dental Analytics
#    - Repository: github.com/BenjaminRains/dbt_dental_clinic
#    - Subdirectory: dbt_dental_models

# 3. Configure connection:
#    - Database: opendental_demo
#    - Schema: raw
#    - Host: [from Render/Supabase]
#    - Port: 5432
#    - User: [from Render/Supabase]
#    - Password: [from Render/Supabase]

# 4. Test connection (should succeed)

# 5. Create environment:
#    - Name: Production
#    - dbt Version: 1.7.0
#    - Threads: 4
```

#### Step 3: Create dbt Cloud Job

```yaml
# Job configuration in dbt Cloud UI
name: "Generate Documentation"
commands:
  - dbt deps
  - dbt seed
  - dbt run --full-refresh
  - dbt test
  - dbt docs generate

schedule: "Daily at 6 AM UTC"  # Or manual trigger
```

#### Step 4: Run Job & Get Public URL

```bash
# 1. Click "Run Now" in dbt Cloud
# 2. Wait for completion (~10-15 minutes for 150 models)
# 3. Click "View Documentation" 
# 4. Get public URL (share on resume/portfolio)

# Example URL:
# https://cloud.getdbt.com/accounts/12345/jobs/67890/docs/
```

#### Step 5: Update Resume/LinkedIn

```markdown
**Dental Analytics Data Pipeline**
- Built end-to-end analytics platform processing 135K+ records
- 150+ dbt models with full test coverage and documentation
- Public dbt Docs: [your-url-here]
- Tech stack: PostgreSQL, dbt, Python, Jinja, SQL
```

---

## ✅ Validation

### Automated Checks

The generator includes validation after each phase:

```python
# Financial validation
def validate_ar_balance(procedure_id):
    """Ensures AR balance equation holds"""
    production = procedure.fee
    collections = insurance_paid + patient_paid
    adjustments = sum(adjustments)
    
    ar_balance = production - collections - adjustments
    assert ar_balance >= -0.01, "AR balance negative!"

# Referential integrity
def validate_foreign_keys():
    """Ensures all FKs reference valid records"""
    for paysplit in paysplits:
        assert paysplit.payment_id in payments
        assert paysplit.patient_id in patients
        assert (paysplit.procedure_id in procedures OR 
                paysplit.adjustment_id in adjustments)

# Date consistency
def validate_dates():
    """Ensures logical date sequences"""
    for appt in appointments:
        if appt.status == 'COMPLETED':
            assert appt.date_arrived <= appt.date_seated
            assert appt.date_seated <= appt.date_dismissed
```

### Manual Validation Queries

```sql
-- 1. Check financial balancing
SELECT 
    COUNT(*) as total_procedures,
    SUM(CASE WHEN ar_balance < -0.01 THEN 1 ELSE 0 END) as negative_balances
FROM (
    SELECT 
        p."ProcNum",
        p."ProcFee" - 
        COALESCE(SUM(cp."InsPayAmt"), 0) - 
        COALESCE(SUM(ps."SplitAmt"), 0) - 
        COALESCE(SUM(a."AdjAmt"), 0) as ar_balance
    FROM raw.procedurelog p
    LEFT JOIN raw.claimproc cp ON p."ProcNum" = cp."ProcNum"
    LEFT JOIN raw.paysplit ps ON p."ProcNum" = ps."ProcNum"
    LEFT JOIN raw.adjustment a ON p."ProcNum" = a."ProcNum"
    GROUP BY p."ProcNum", p."ProcFee"
) balances;
-- Expected: negative_balances = 0

-- 2. Check referential integrity
SELECT COUNT(*) FROM raw.paysplit ps
LEFT JOIN raw.payment p ON ps."PayNum" = p."PayNum"
WHERE p."PayNum" IS NULL;
-- Expected: 0

-- 3. Check date ranges
SELECT 
    MIN("ProcDate") as earliest_procedure,
    MAX("ProcDate") as latest_procedure
FROM raw.procedurelog;
-- Expected: earliest >= 2023-01-01

-- 4. Run dbt tests
dbt test
-- Expected: All tests pass
```

---

## 🐛 Troubleshooting

### Common Issues

#### Issue: "Database connection failed"

```bash
# Solution: Check PostgreSQL is running
pg_isready -h localhost

# Check credentials
psql -h localhost -U postgres -d opendental_demo

# Check firewall (if remote database)
telnet xxx.render.com 5432
```

#### Issue: "Foreign key constraint violation"

```bash
# Solution: Ensure generation order is correct
# Foundation → Patients → Insurance → Clinical → Financial

# Drop and recreate DEMO database if needed (safe - no production data!)
dropdb opendental_demo
createdb opendental_demo
psql -d opendental_demo -f create_tables.sql
```

#### Issue: "dbt models fail on synthetic data"

```bash
# Solution: Check specific model error
dbt run --models stg_opendental__procedurelog

# Common causes:
# 1. Date filter mismatch (must be >= 2023-01-01)
# 2. Missing adjustment type IDs
# 3. Invalid definition categories
# 4. Paysplit allocation rules violated

# Debug with:
dbt run --models +model_name --debug
```

#### Issue: "AR balances don't match"

```sql
-- Debug query
SELECT 
    p."ProcNum",
    p."ProcFee" as production,
    SUM(cp."InsPayAmt") as insurance_paid,
    SUM(ps."SplitAmt") as patient_paid,
    SUM(a."AdjAmt") as adjustments,
    p."ProcFee" - 
    COALESCE(SUM(cp."InsPayAmt"), 0) - 
    COALESCE(SUM(ps."SplitAmt"), 0) - 
    COALESCE(SUM(a."AdjAmt"), 0) as balance
FROM raw.procedurelog p
LEFT JOIN raw.claimproc cp ON p."ProcNum" = cp."ProcNum"
LEFT JOIN raw.paysplit ps ON p."ProcNum" = ps."ProcNum"
LEFT JOIN raw.adjustment a ON p."ProcNum" = a."ProcNum"
WHERE p."ProcNum" = [problem_proc_id]
GROUP BY p."ProcNum", p."ProcFee";
```

---

## 📚 Additional Resources

### Documentation
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Cloud Setup](https://docs.getdbt.com/docs/cloud/about-cloud-setup)
- [OpenDental Schema](https://www.opendental.com/manual/)

### Support
- GitHub Issues: https://github.com/BenjaminRains/dbt_dental_clinic/issues

---

## 📝 License

MIT License - Free to use for portfolio and educational purposes.

---

## 🚀 Getting Started

To use the synthetic data generator:

1. **Install dependencies** - Follow the [Installation](#-installation) section
2. **Generate data** - Run `python main.py` or use the PowerShell wrapper
3. **Run dbt transforms** - Execute `dbt run` against the demo database
4. **Validate data quality** - Run `dbt test` to verify all checks pass
5. **Deploy to cloud** (optional) - Follow the [dbt Cloud Deployment](#-dbt-cloud-deployment) guide

**Ready to showcase your professional data engineering skills!** 🚀