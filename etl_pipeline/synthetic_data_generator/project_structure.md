# 📁 Project Structure & Implementation Guide

## 🎯 Quick Start Summary

**Goal:** Generate synthetic dental data → Test dbt models → Deploy to dbt Cloud

**Timeline:** 4 weeks (Option B)
- Week 1: Setup + Foundation data
- Week 2-3: Clinical + Financial data  
- Week 4: Validation + Testing
- Week 5: Cloud deployment

**Focus:** Production Dashboard + AR Analysis

---

## 📂 Complete Project Structure

```
etl_pipeline/
└── synthetic_data_generator/
    ├── README.md                    # ✅ Complete documentation
    ├── requirements.txt             # ✅ Python dependencies
    ├── config.example.py            # ✅ Configuration template
    ├── config.py                    # ⚠️  Create from example (gitignored)
    ├── .env.example                 # Environment variables template
    ├── .env                         # ⚠️  Your secrets (gitignored)
    ├── .gitignore                   # Git ignore rules
    │
    ├── main.py                      # ✅ Main generator orchestrator
    │
    ├── generators/                  # Generator modules
    │   ├── __init__.py
    │   ├── foundation_generator.py  # ✅ Clinics, providers, codes, fees
    │   ├── patient_generator.py     # 📝 Patients, notes, families
    │   ├── insurance_generator.py   # 📝 Carriers, plans, benefits
    │   ├── clinical_generator.py    # 📝 Appointments, procedures
    │   ├── financial_generator.py   # 📝 Claims, payments, adjustments
    │   └── supporting_generator.py  # 📝 Communications, tasks
    │
    ├── validators/                  # Data quality validators
    │   ├── __init__.py
    │   ├── integrity_validator.py   # Foreign key checks
    │   ├── financial_validator.py   # AR balance validation
    │   └── business_validator.py    # Business rule checks
    │
    ├── schema/                      # Database schema
    │   ├── create_tables.sql        # 📝 DDL for all tables
    │   └── grant_permissions.sql    # Database permissions
    │
    ├── tests/                       # Unit tests (optional)
    │   ├── __init__.py
    │   ├── test_generators.py
    │   └── test_validators.py
    │
    └── docs/                        # Additional documentation
        ├── BUSINESS_LOGIC.md        # Business rules reference
        ├── TABLE_SPECIFICATIONS.md  # Detailed table specs
        └── TROUBLESHOOTING.md       # Common issues & solutions
```

---

## 🚀 Implementation Steps

### Step 1: Setup Environment (15 minutes)

```bash
# 1. Navigate to project directory
cd etl_pipeline/synthetic_data_generator

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create configuration
cp config.example.py config.py
# Edit config.py with your database credentials

# 5. Create environment file
cp .env.example .env
# Edit .env with your secrets
```

### Step 2: Create Database (5 minutes)

```bash
# 1. Create database
createdb opendental_demo

# 2. Create schema
psql -d opendental_demo -c "CREATE SCHEMA IF NOT EXISTS raw;"

# 3. Create tables (you'll need to create this SQL file)
psql -d opendental_demo -f schema/create_tables.sql
```

### Step 3: Generate Data (Week 1-3)

```bash
# Week 1: Test with small dataset
python main.py --patients 100

# Validate dbt models work
cd ../../dbt_dental_models
dbt run --select stg_opendental__*
dbt test

# Week 2-3: Generate full dataset
cd ../etl_pipeline/synthetic_data_generator
python main.py --patients 5000
```

### Step 4: Validate (Week 4)

```bash
# Run all dbt models
cd ../../dbt_dental_models
dbt run --full-refresh

# Run all tests
dbt test

# Generate documentation locally
dbt docs generate
dbt docs serve
```

### Step 5: Deploy to Cloud (Week 5)

See README.md "dbt Cloud Deployment" section for detailed steps.

---

## 📝 Files You Need to Create

### 1. Additional Generator Modules

You have `foundation_generator.py` ✅. You need to create:

```python
# generators/patient_generator.py
"""
Generates:
- patients (5,000)
- patientnote (5,000 - 1:1 relationship)
- patientlink (1,000 family relationships)
- zipcode (100 reference data)
"""

# generators/insurance_generator.py
"""
Generates:
- carrier (15)
- employer (50)
- insplan (30)
- inssub (2,000)
- patplan (3,500)
- benefit (1,500)
"""

# generators/clinical_generator.py
"""
Generates:
- appointment (15,000)
- appointmenttype (15)
- procedurelog (20,000)
- recall (3,000)
- recalltype (10)
- recalltrigger (30)
"""

# generators/financial_generator.py
"""
Generates:
- claim (8,000)
- claimproc (25,000)
- claimpayment (5,000)
- payment (12,000)
- paysplit (15,000)
- adjustment (2,000)

CRITICAL: This module must implement exact financial balancing logic!
"""

# generators/supporting_generator.py
"""
Generates:
- commlog (8,000)
- document (5,000)
- referral (20)
- refattach (1,000)
- task (1,000) - optional
"""
```

### 2. Database Schema File

```sql
-- schema/create_tables.sql
-- This file should contain CREATE TABLE statements for all 50+ tables
-- Extract from your production database using:
pg_dump -h localhost -U postgres -d opendental_analytics \
  --schema-only --schema=raw \
  --no-owner --no-acl \
  -f schema/create_tables.sql
```

### 3. Environment Files

```bash
# .env.example
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=opendental_demo
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
START_DATE=2023-01-01
NUM_PATIENTS=5000

# .gitignore
venv/
__pycache__/
*.pyc
.env
config.py
*.log
.vscode/
.idea/
```

---

## 🔑 Key Implementation Patterns

### Pattern 1: ID Generation

```python
# In each generator module
class PatientGenerator:
    def __init__(self, config, id_gen, data_store):
        self.config = config
        self.id_gen = id_gen  # Shared ID generator
        self.data_store = data_store  # Shared data store
    
    def generate(self, db):
        patients = []
        for i in range(self.config.num_patients):
            patient_id = self.id_gen.next_id('patient')
            patients.append((
                patient_id,
                # ... other fields
            ))
        
        # Store for other generators to reference
        self.data_store['patients'] = [p[0] for p in patients]
```

### Pattern 2: Foreign Key References

```python
# Reference data from previous generators
def _generate_appointments(self):
    appointments = []
    
    for patient_id in self.data_store['patients']:
        provider_id = random.choice(self.data_store['providers'])
        clinic_id = random.choice(self.data_store['clinics'])
        operatory_id = random.choice(self.data_store['operatories'])
        
        appointments.append((
            self.id_gen.next_id('appointment'),
            patient_id,  # Valid FK
            provider_id,  # Valid FK
            clinic_id,  # Valid FK
            operatory_id,  # Valid FK
            # ... other fields
        ))
```

### Pattern 3: Financial Balancing

```python
# CRITICAL: Ensure AR balance equation holds
def _generate_payment_for_procedure(self, procedure):
    """
    Generate payment that exactly balances the procedure
    
    Formula: procedure_fee = insurance_payment + patient_payment + adjustments
    """
    proc_fee = procedure['ProcFee']
    insurance_paid = procedure.get('insurance_payment', 0)
    adjustments = procedure.get('adjustments', 0)
    
    # Patient must pay remainder
    patient_payment = proc_fee - insurance_paid - adjustments
    
    # Validate
    assert patient_payment >= 0, "Patient payment cannot be negative!"
    
    return patient_payment
```

### Pattern 4: Date Sequencing

```python
# Ensure logical date progression
def _generate_appointment_with_procedure(self, patient_id, appt_date):
    """Generate appointment with logical timestamp flow"""
    
    # Appointment datetime (business hours)
    appt_datetime = random_business_datetime(appt_date)
    
    # Patient flow (must be sequential)
    arrived = appt_datetime + timedelta(minutes=random.randint(-5, 10))
    seated = arrived + timedelta(minutes=random.randint(5, 20))
    dismissed = seated + timedelta(minutes=random.randint(30, 90))
    
    # Procedure on same date
    procedure = {
        'ProcDate': appt_date,  # Same day
        'DateEntryC': appt_datetime,  # Created during appointment
    }
    
    # Payment comes later
    payment_date = appt_date + timedelta(days=random.randint(1, 60))
    
    return appointment, procedure, payment_date
```

---

## ✅ Validation Checklist

After generating data, verify:

- [ ] All 50+ tables have records
- [ ] No foreign key violations
- [ ] All dates >= 2023-01-01
- [ ] Financial balancing holds (AR = Production - Collections - Adjustments)
- [ ] Paysplits reference procedure OR adjustment (never both NULL)
- [ ] Adjustment type IDs match expected values (186, 188, 474, etc.)
- [ ] Definition categories exist (1, 4, 5, 6)
- [ ] Fee schedule linkage works (80% exact match)
- [ ] dbt staging models compile
- [ ] dbt intermediate models run successfully
- [ ] dbt marts produce correct results
- [ ] All dbt tests pass

---

## 🆘 Next Actions

### Immediate (This Week)
1. ✅ Review main.py and foundation_generator.py
2. 📝 Create remaining generator modules (patient, insurance, clinical, financial, supporting)
3. 📝 Extract database schema (create_tables.sql)
4. 🧪 Test with small dataset (100 patients)

### Week 1-2
5. 🏗️ Generate foundation data
6. 🏗️ Generate patient data
7. 🏗️ Generate insurance data
8. ✅ Validate staging models work

### Week 2-3
9. 🏗️ Generate clinical data (appointments, procedures)
10. 🏗️ Generate financial data (claims, payments) - **Most complex!**
11. ✅ Validate intermediate models work

### Week 4
12. 🧪 Full validation suite
13. 🧪 Test all dbt models (150+)
14. 📊 Generate local dbt docs

### Week 5
15. ☁️ Deploy database to cloud (Render/Supabase)
16. ☁️ Configure dbt Cloud
17. 🎉 Get public documentation URL
18. 📝 Update resume/portfolio

---

## 💡 Tips for Success

1. **Start Small**: Test with 100 patients before scaling to 5,000
2. **Validate Early**: Check dbt models after each phase
3. **Financial First**: Get payment balancing working perfectly before adding complexity
4. **Use Debugging**: Add print statements to track which records are being generated
5. **Check Logs**: Monitor for foreign key errors and date inconsistencies
6. **Commit Often**: Git commit after each working generator module
7. **Document Issues**: Keep notes on problems you encounter and solutions

---

## 📞 Support

If you encounter issues:
1. Check the TROUBLESHOOTING.md guide (in docs/)
2. Review validation queries in README.md
3. Check dbt logs: `dbt run --debug`
4. Examine database directly: `psql -d opendental_demo`

---

**You have everything you need to build this!** 🚀

The main.py and foundation_generator.py provide the complete pattern. Just follow the same structure for the remaining generators, focusing on financial balancing logic.

**Good luck with your portfolio project!** 🎯