# 🚀 Quick Start Guide

## Prerequisites

```bash
# 1. Python 3.8+
python --version

# 2. PostgreSQL 11.6+ or 13+
psql --version

# 3. Virtual environment (recommended)
python -m venv venv
```

## Installation (2 minutes)

```bash
# 1. Activate ETL environment (installs all dependencies including synthetic data generator packages)
etl-init
# Choose 'test' environment when prompted

# 2. Configure credentials (optional but recommended)
cd etl_pipeline/synthetic_data_generator
copy .env_demo.template .env_demo
# Edit .env_demo and set your password

# That's it! The required packages (faker, psycopg2-binary, python-dateutil) 
# are already in the ETL Pipfile and will be installed automatically.
```

**Note**: The synthetic data generator dependencies are included in the ETL environment's Pipfile, so no separate installation is needed!

## Database Setup (10 minutes)
w
**Option A: If opendental_demo already exists (recreate schema)**

If the database exists but the schema is outdated, use the automated script:

```powershell
cd etl_pipeline/synthetic_data_generator
.\recreate_schema.ps1 -ProductionDbPassword "your_prod_password" -DemoDbPassword "your_demo_password"
```

This will safely drop and recreate the `raw` schema with the latest production structure.

**Option B: Create new database**

```bash
# 1. Create database
createdb opendental_demo

# 2. Create schema
psql -d opendental_demo -c "CREATE SCHEMA IF NOT EXISTS raw;"

# 3. Extract your production schema (STRUCTURE ONLY - NO DATA)
pg_dump -h localhost -U postgres -d opendental_analytics \
  --schema-only --schema=raw \
  --no-owner --no-acl \
  -f schema/create_tables.sql

# 4. Apply schema to demo database
psql -d opendental_demo -f schema/create_tables.sql
```

## Test Generators (Optional but Recommended)

Before generating full datasets, test that all generators work correctly:

```powershell
# Test all generators with small dataset (100 patients)
python test_generators.py --patients 100 --db-password "yourpassword"

# Test specific generator
python test_generators.py --generator foundation --db-password "yourpassword"
python test_generators.py --generator patients --db-password "yourpassword"

# Test with custom database settings
python test_generators.py --db-host localhost --db-name opendental_demo --db-user postgres --db-password "yourpassword"
```

The test script will:
- Verify database connection
- Test each generator independently
- Check that required tables are created
- Verify record counts match expectations
- Print a summary of all tests

**Expected output:**
```
✓ FoundationGenerator: PASS
✓ PatientGenerator: PASS
✓ InsuranceGenerator: PASS
✓ ClinicalGenerator: PASS
✓ FinancialGenerator: PASS
✓ SupportingGenerator: PASS
```

## Generate Data (5-20 minutes)

### Method 1: Safe Wrapper Script with .env_demo (Easiest)

```powershell
# Navigate to synthetic data generator directory
cd etl_pipeline/synthetic_data_generator

# If you set up .env_demo, just run without any arguments!

# Small test dataset (100 patients) - ~2 minutes
.\generate.ps1 -Patients 100

# Medium dataset (1,000 patients) - ~5 minutes
.\generate.ps1 -Patients 1000

# Full dataset (5,000 patients) - ~20 minutes
.\generate.ps1 -Patients 5000

# Override any .env_demo setting via CLI
.\generate.ps1 -Patients 10000 -StartDate "2024-01-01"
```

### Method 1b: Safe Wrapper Script without .env_demo

```powershell
# If you didn't create .env_demo, pass credentials manually

# Small test dataset
.\generate.ps1 -Patients 100 -DbPassword "yourpassword"

# Full configuration
.\generate.ps1 `
  -Patients 5000 `
  -DbHost "localhost" `
  -DbUser "postgres" `
  -DbPassword "yourpassword" `
  -StartDate "2023-01-01"
```

### Method 2: Direct Python (Alternative)

```bash
# IMPORTANT: Always specify --db-name opendental_demo (separate from clinic/test databases)

# Small test dataset
python main.py \
  --patients 100 \
  --db-name opendental_demo \
  --db-user postgres \
  --db-password yourpassword

# Full dataset with all options
python main.py \
  --patients 5000 \
  --db-host localhost \
  --db-name opendental_demo \
  --db-user postgres \
  --db-password yourpassword \
  --start-date 2023-01-01
```

**⚠️ Critical Safety Notes**:
- Make sure the ETL environment is active (`etl-init`, choose either clinic or test)
- **Always target `opendental_demo` database** (never clinic/test!)
- The wrapper script (`generate.ps1`) enforces this automatically
- The ETL environment choice only affects which packages are installed, not the target database
- The generator uses CLI arguments to override any environment database settings

## Validate with dbt (10 minutes)

```bash
# 1. Navigate to dbt project
cd ../../dbt_dental_models

# 2. Update profiles.yml to point to opendental_demo
# Edit profiles.yml:
#   dbname: opendental_demo
#   schema: raw

# 3. Test staging models
dbt run --select stg_opendental__*

# 4. Test intermediate models
dbt run --select int_*

# 5. Test marts
dbt run --select fct_* dim_*

# 6. Run all tests
dbt test

# 7. Generate documentation
dbt docs generate
dbt docs serve
```

## Expected Output

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
✓ Generated 100 zipcodes
✓ Generated 5,000 patients in 2,000 families
✓ Generated 3,500 patient notes
✓ Generated 1,000 family relationship links

[Phase 3] Generating Insurance Data...
✓ Generated 15 insurance carriers
✓ Generated 50 employers
✓ Generated 45 insurance plans
✓ Generated 3,000 insurance subscribers
✓ Generated 3,500 patient plan relationships
✓ Generated 315 benefit rules

[Phase 4] Generating Clinical Data...
✓ Generated 4 recall types
✓ Generated 15 appointment types
✓ Generated 15,000 appointments
✓ Generated 20,000 procedures
✓ Generated 3,200 recall records
✓ Generated 5 recall triggers

[Phase 5] Generating Financial Data...
✓ Generated 8,000 claims with 25,000 claim procedures
✓ Generated 5,000 insurance check payments
✓ Generated 12,000 patient payments with 15,000 payment splits
✓ Generated 2,000 adjustments

[Financial Validation] Checking AR balance equation...
  Total procedures checked: 20,000
  Fully balanced: 18,000 (90.0%)
  With AR balance: 2,000 (10.0%)
  Max imbalance: $125.50
✓ Financial validation complete

[Phase 6] Generating Supporting Data...
✓ Generated 10 referrals
✓ Generated 2,000 referral attachments
✓ Generated 15,000 communication logs
✓ Generated 10,000 documents

============================================================
✓ Data generation completed successfully!
============================================================

Generated Record Counts:
----------------------------------------
  adjustment                    :    2,000
  appointment                   :   15,000
  appointmenttype               :       15
  benefit                       :      315
  carrier                       :       15
  claim                         :    8,000
  claimpayment                  :    5,000
  claimproc                     :   25,000
  clinic                        :        5
  commlog                       :   15,000
  definition                    :       25
  document                      :   10,000
  employer                      :       50
  fee                           :      138
  feesched                      :        3
  insplan                       :       45
  inssub                        :    3,000
  operatory                     :       20
  patient                       :    5,000
  patientlink                   :    1,000
  patientnote                   :    3,500
  patplan                       :    3,500
  payment                       :   12,000
  paysplit                      :   15,000
  procedurecode                 :       46
  procedurelog                  :   20,000
  provider                      :       12
  recall                        :    3,200
  recalltrigger                 :        5
  recalltype                    :        4
  refattach                     :    2,000
  referral                      :       10
  userod                        :       10
  zipcode                       :      100
  TOTAL                         :  135,423
```

## Troubleshooting

### "Database connection failed"
```bash
# Check PostgreSQL is running
pg_isready -h localhost

# Test connection manually
psql -h localhost -U postgres -d opendental_demo -c "SELECT 1;"
```

### "Table does not exist"
```bash
# Verify schema was created
psql -d opendental_demo -c "\dt raw.*"

# If empty, re-run schema creation
psql -d opendental_demo -f schema/create_tables.sql
```

### "Foreign key constraint violation"
```bash
# Drop and recreate database
dropdb opendental_demo
createdb opendental_demo
psql -d opendental_demo -c "CREATE SCHEMA IF NOT EXISTS raw;"
psql -d opendental_demo -f schema/create_tables.sql

# Re-run generator
python main.py --patients 100
```

### "dbt models fail"
```bash
# Check which models fail
dbt run --select stg_opendental__* --debug

# Common issues:
# 1. Date filter: Ensure data >= 2023-01-01
# 2. Adjustment types: Must match expected IDs (186, 188, 474, etc.)
# 3. Payment splits: Must have ProcNum OR AdjNum (not both NULL)

# Validate with queries:
psql -d opendental_demo -c "
    SELECT MIN(\"ProcDate\") FROM raw.procedurelog;
"
```

## Next Steps

1. ✅ **Test locally** - Verify all dbt models run
2. ✅ **Cloud deployment** - Deploy to Render.com or Supabase
3. ✅ **dbt Cloud setup** - Configure dbt Cloud project
4. ✅ **Documentation** - Generate public dbt docs
5. ✅ **Portfolio** - Add to resume/LinkedIn

## Support

- **Documentation**: See README.md for full details
- **Issues**: Check TROUBLESHOOTING.md
- **dbt Help**: `dbt run --debug` for detailed logs

---

**Happy generating!** 🎉

