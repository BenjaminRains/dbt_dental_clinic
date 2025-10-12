# 🎯 **Synthetic Data Generator Plan**
## For dbt Cloud Deployment & Professional Portfolio

---

## **1. PROJECT OVERVIEW**

### Goals
- Generate realistic but **completely fake** dental practice data
- Maintain referential integrity across 90+ core tables
- Support full dbt model execution (150+ models)
- Enable dbt Cloud deployment for professional showcase
- Ensure 100% HIPAA compliance (zero real PHI)

### Success Criteria
- ✅ All 150 dbt models compile and run successfully
- ✅ Data volumes match production scale (thousands of records)
- ✅ Realistic business scenarios (appointments, claims, payments)
- ✅ No real patient information anywhere in the system
- ✅ Deployable to dbt Cloud with public visibility

---

## **2. TECHNICAL ARCHITECTURE**

### Components
```
┌─────────────────────────────────────────────────────────────┐
│  Synthetic Data Generator (Python)                          │
│  - Faker library for realistic fake data                    │
│  - Custom dental domain logic                               │
│  - Relationship management                                  │
│  - Data validation                                          │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL Database (NEW - Synthetic Only)                 │
│  Database: opendental_demo                                  │
│  Schema: raw (matching your production structure)           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  DBT Models (Existing - No Changes)                         │
│  - Connect to opendental_demo instead of analytics          │
│  - All 150+ models run against synthetic data              │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│  dbt Cloud Deployment                                       │
│  - Publicly accessible project                              │
│  - Generated documentation with lineage                     │
│  - Test results and data quality metrics                   │
└─────────────────────────────────────────────────────────────┘
```

---

## **3. DATABASE STRUCTURE**

### New Database Setup
- **Database Name:** `opendental_demo`
- **Schema:** `raw` (matches your current structure)
- **Location Options:**
  - **Option A:** Local PostgreSQL (easiest for initial testing)
  - **Option B:** Cloud PostgreSQL (AWS RDS, DigitalOcean, Render, Supabase)
  - **Option C:** ElephantSQL free tier (20MB limit - too small)
  
**Recommendation:** Start with **Local**, then move to **Render.com** or **Supabase** free tier for dbt Cloud access

### Schema Replication
- Copy exact table structure from your production database
- Include all indexes, constraints, foreign keys
- Match column types and defaults exactly
- Add `_loaded_at` metadata columns

---

## **4. DATA GENERATION STRATEGY**

### Phase 1: Foundation Tables (Configuration & Reference Data)
**Priority 1 - Core Configuration**
- `clinic` - 3-5 clinic locations
- `provider` - 8-12 providers (dentists, hygienists)
- `procedurecode` - ~200 common dental procedures (from CDT codes)
- `definition` - Lookup values (appointment types, payment types, etc.)
- `feesched` - 2-3 fee schedules
- `fee` - Fee amounts for each procedure/schedule combination

**Priority 2 - Insurance Configuration**
- `carrier` - 10-15 insurance companies
- `insplan` - 20-30 insurance plans
- `benefit` - Coverage benefits for each plan

### Phase 2: Patient & Family Data
**Core Patient Tables**
- `patient` - **5,000 patients**
  - Fake names (Faker library)
  - Random DOB (realistic age distribution)
  - Fake addresses, phones, emails
  - Family groupings (guarantor relationships)
- `patientnote` - Notes for each patient
- `patientlink` - Family relationships

**Demographics Strategy:**
- Age distribution: 5% pediatric, 70% adult, 25% senior
- Gender distribution: 50/50
- Family sizes: 1-5 members per family

### Phase 3: Clinical & Financial Data
**Transaction Tables (Time-Series Data)**

Generate data for **24-month period** (2023-01-01 to 2024-12-31)

**Appointment Data**
- `appointment` - **15,000 appointments**
  - 75% completed, 15% scheduled (future), 10% broken/cancelled
  - Realistic scheduling patterns (Monday-Friday, business hours)
  - Appointment types: Hygiene, Restorative, Emergency, etc.
- `histappointment` - Historical changes to appointments

**Procedure Data**
- `procedurelog` - **20,000 procedures**
  - Linked to completed appointments
  - Realistic procedure mix (exams, cleanings, fillings, crowns, etc.)
  - Fee amounts from fee schedule
  - Status: Completed (TP), Existing Prior, Condition, etc.

**Insurance Claims**
- `claim` - **8,000 claims**
  - Linked to procedures with insurance coverage
  - Claim statuses: Sent, Received, Pending, etc.
- `claimproc` - **25,000 claim procedures**
  - Individual procedure claims
  - Insurance estimates, payments, write-offs
- `claimpayment` - **5,000 insurance payments**

**Patient Payments**
- `payment` - **12,000 payments**
  - Cash, check, credit card payments
  - Realistic payment patterns
- `paysplit` - **15,000 payment splits**
  - Split payments across procedures

**Adjustments & Discounts**
- `adjustment` - **2,000 adjustments**
  - Discounts, write-offs, corrections

### Phase 4: Supporting Data
- `commlog` - **8,000 communication logs** (calls, texts, emails)
- `recall` - **3,000 recall appointments** (hygiene due dates)
- `document` - **5,000 documents** (metadata only, no actual files)
- `sheet` - **2,000 forms** (medical history, consents)
- `task` - **1,000 tasks** (office workflow)

---

## **5. DATA GENERATION LOGIC**

### Realistic Business Rules

**Patient Journey Simulation**
```
New Patient Flow:
1. Create patient record
2. Initial appointment (comprehensive exam + X-rays)
3. Treatment plan (multiple procedures)
4. Follow-up appointments
5. Hygiene recall cycle (every 6 months)
6. Occasional emergency visits
```

**Financial Flow Simulation**
```
Procedure → Insurance Claim → Insurance Payment → Patient Balance → Patient Payment
- 60% of procedures have insurance coverage
- Insurance pays 50-80% depending on benefit
- Patient pays remaining balance
- Some procedures are cash-only (cosmetic, etc.)
```

**Realistic Patterns**
- Seasonal variations (fewer appointments in summer/holidays)
- Provider productivity patterns (different providers have different schedules)
- No-show rates (~10% of appointments)
- Same-day emergency appointments
- Multi-procedure appointments (cleaning + exam)

### Data Quality Features
- **Referential Integrity:** All foreign keys valid
- **Date Consistency:** Created < Updated, Appointment Date < Payment Date
- **Financial Balancing:** Production = Collections + Adjustments + AR
- **Status Consistency:** Completed appointments have procedures, claims, payments

---

## **6. VOLUME TARGETS**

### Estimated Row Counts (2-Year Dataset)

| Category | Table | Row Count |
|----------|-------|-----------|
| **Configuration** | clinic | 5 |
| | provider | 12 |
| | procedurecode | 200 |
| | definition | 500 |
| | fee | 600 |
| **Insurance** | carrier | 15 |
| | insplan | 30 |
| | inssub | 2,000 |
| | patplan | 3,500 |
| **Patients** | patient | 5,000 |
| | patientnote | 5,000 |
| **Clinical** | appointment | 15,000 |
| | procedurelog | 20,000 |
| **Financial** | claim | 8,000 |
| | claimproc | 25,000 |
| | claimpayment | 5,000 |
| | payment | 12,000 |
| | paysplit | 15,000 |
| | adjustment | 2,000 |
| **Communication** | commlog | 8,000 |
| | recall | 3,000 |
| **Other** | document | 5,000 |
| | sheet | 2,000 |
| | task | 1,000 |
| **TOTAL** | **~90 tables** | **~135,000 rows** |

**Database Size Estimate:** 200-500 MB (compared to your 3.7GB production)

---

## **7. TECHNOLOGY STACK**

### Python Libraries
```python
# Core Data Generation
faker                 # Realistic fake data (names, addresses, etc.)
numpy                 # Random distributions
pandas                # Data manipulation
python-dateutil       # Date handling

# Database
psycopg2-binary      # PostgreSQL connection
sqlalchemy           # ORM and connection pooling

# Dental Domain
custom_dental_lib    # Custom dental-specific data (CDT codes, etc.)

# Validation
great_expectations   # Data quality validation (optional)
```

### Project Structure
```
etl_pipeline/
├── synthetic_data_generator/
│   ├── __init__.py
│   ├── config/
│   │   ├── tables_config.yml      # Table generation order & rules
│   │   ├── dental_codes.json      # CDT procedure codes
│   │   └── insurance_plans.json   # Insurance plan templates
│   ├── generators/
│   │   ├── base_generator.py      # Base class for all generators
│   │   ├── config_generator.py    # Clinics, providers, codes
│   │   ├── patient_generator.py   # Patient demographics
│   │   ├── clinical_generator.py  # Appointments, procedures
│   │   ├── financial_generator.py # Claims, payments
│   │   └── supporting_generator.py # Communications, tasks
│   ├── validators/
│   │   ├── integrity_validator.py # Referential integrity checks
│   │   └── business_validator.py  # Business rule validation
│   ├── database/
│   │   ├── connection.py          # Database connection management
│   │   ├── schema_builder.py      # Create tables from production schema
│   │   └── loader.py              # Bulk insert data
│   └── cli.py                     # Command-line interface
```

---

## **8. DEPLOYMENT OPTIONS**

### Local Development (Phase 1)
- PostgreSQL on Windows (already installed)
- Database: `opendental_demo`
- Test dbt models locally
- Validate data quality

### Cloud Deployment (Phase 2)

**Option A: Render.com** (RECOMMENDED)
- ✅ Free PostgreSQL tier (90 days, then $7/month)
- ✅ Easy setup, no credit card for trial
- ✅ Publicly accessible for dbt Cloud
- ✅ 1GB storage (plenty for synthetic data)

**Option B: Supabase**
- ✅ Free tier with 500MB database
- ✅ Built on PostgreSQL
- ✅ Good for demos
- ⚠️ May have connection limits

**Option C: AWS RDS Free Tier**
- ✅ 12 months free (with new AWS account)
- ✅ 20GB storage
- ⚠️ Requires credit card
- ⚠️ More complex setup

**Option D: Railway.app**
- ✅ $5/month credit free
- ✅ Easy PostgreSQL deployment
- ✅ Good developer experience

---

## **9. HIPAA COMPLIANCE VERIFICATION**

### Zero PHI Guarantee
- ✅ **All names:** Generated by Faker (no real people)
- ✅ **All addresses:** Fake addresses
- ✅ **All phone/email:** Fake contact info
- ✅ **All SSN/IDs:** Random generated (invalid format)
- ✅ **All dates:** Synthetic date ranges
- ✅ **All medical data:** Generic/random conditions

### Validation Steps
1. **No real data export:** Generator never touches production database
2. **Schema-only extraction:** Use information_schema to get table structure
3. **Synthetic-only data:** All data generated from scratch
4. **Public safe:** Entire database can be public without HIPAA risk

---

## **10. IMPLEMENTATION PHASES**

### Phase 1: Setup & Foundation (Week 1)
**Tasks:**
1. Set up local `opendental_demo` database
2. Extract schema structure from production (no data)
3. Build base generator framework
4. Create configuration data generators (clinics, providers, codes)
5. Generate and load foundation data

**Deliverables:**
- Empty database with correct schema
- Foundation tables populated
- Base generator classes working

### Phase 2: Patient & Clinical Data (Week 2)
**Tasks:**
1. Build patient generator (5,000 patients with families)
2. Build appointment generator (15,000 appointments)
3. Build procedure generator (20,000 procedures)
4. Implement realistic business logic (appointment patterns, procedure mix)
5. Validate referential integrity

**Deliverables:**
- Patient demographics complete
- Clinical workflows simulated
- Appointment and procedure data loaded

### Phase 3: Financial Data (Week 3)
**Tasks:**
1. Build insurance claim generator
2. Build payment generator
3. Implement financial balancing logic
4. Generate adjustments and write-offs
5. Validate financial integrity (balances, AR aging)

**Deliverables:**
- Complete financial transaction data
- Balanced books (production = collections + AR)
- Realistic claim processing

### Phase 4: Supporting & Validation (Week 4)
**Tasks:**
1. Generate communication logs
2. Generate documents, sheets, tasks
3. Run comprehensive data quality validation
4. Test all 150 dbt models against synthetic data
5. Fix any data issues

**Deliverables:**
- All supporting tables populated
- All dbt models passing
- Data quality report

### Phase 5: Cloud Deployment (Week 5)
**Tasks:**
1. Set up cloud PostgreSQL instance (Render.com)
2. Deploy synthetic database to cloud
3. Configure dbt Cloud project
4. Connect dbt Cloud to cloud database
5. Run dbt Cloud and generate documentation
6. Test public access

**Deliverables:**
- Live cloud database
- dbt Cloud project configured
- Public documentation accessible

---

## **11. TESTING STRATEGY**

### Data Quality Tests
- **Referential Integrity:** All foreign keys valid
- **Date Logic:** Created < Updated, logical date sequences
- **Financial Balance:** Production = Collections + AR
- **Status Consistency:** Completed appointments have procedures
- **Volume Tests:** Row counts within expected ranges

### dbt Model Tests
- **Compilation:** All 150 models compile
- **Execution:** All models run without errors
- **Tests:** All dbt tests pass
- **Documentation:** Lineage graphs generate correctly
- **Performance:** Models run in reasonable time

### Business Logic Tests
- **Patient Journey:** New patients have initial exams
- **Recall Cycles:** Hygiene patients have 6-month recalls
- **Insurance Claims:** Procedures with insurance have claims
- **Payment Flow:** Procedures have payments or AR balance

---

## **12. RISK MITIGATION**

### Risk: Data doesn't match production patterns
**Mitigation:** 
- Analyze your production data patterns first (aggregate statistics only)
- Calibrate generator based on real distributions
- Iterative refinement based on dbt model results

### Risk: dbt models fail on synthetic data
**Mitigation:**
- Start with staging models, then intermediate, then marts
- Fix data issues progressively
- Maintain flexibility to adjust generator logic

### Risk: Cloud database costs
**Mitigation:**
- Use free tiers (Render, Supabase)
- Optimize data volume (can reduce if needed)
- Option to shut down between demos

### Risk: Synthetic data looks "fake"
**Mitigation:**
- Use realistic distributions (not uniform random)
- Implement proper business logic
- Include edge cases and variations
- Add realistic noise and imperfections

---

## **13. SUCCESS METRICS**

### Technical Metrics
- ✅ 150/150 dbt models passing (PASS=150, ERROR=0)
- ✅ 100% referential integrity
- ✅ <5 minutes generation time
- ✅ <500MB database size

### Business Metrics
- ✅ Realistic patient distribution (age, families)
- ✅ Realistic appointment patterns (scheduling, no-shows)
- ✅ Realistic financial balances (AR aging, collection rates)
- ✅ Realistic procedure mix (preventive vs restorative)

### Portfolio Metrics
- ✅ Public dbt Cloud documentation live
- ✅ Lineage graphs showing 150+ models
- ✅ Test results visible
- ✅ Professional presentation

---

## **14. ESTIMATED TIMELINE**

**Total Time:** 4-5 weeks (part-time work)

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1: Setup & Foundation | 1 week | 15-20 hours |
| Phase 2: Patient & Clinical | 1 week | 20-25 hours |
| Phase 3: Financial Data | 1 week | 20-25 hours |
| Phase 4: Supporting & Validation | 1 week | 15-20 hours |
| Phase 5: Cloud Deployment | 3-5 days | 10-15 hours |
| **TOTAL** | **5 weeks** | **80-105 hours** |

**Accelerated Option:** Focus on core tables only (20-30 tables instead of 90) - 2-3 weeks

---

## **15. DECISION POINTS (Your Approval Required)**

### Questions for You:

1. **Timeline:** Is 4-5 weeks reasonable, or do you need this faster?
   
2. **Scope:** Do you want all 90+ tables, or can we start with core 20-30 tables?
   - See `docs/dbt/dbt_documentation_strategy.md` for minimum required tables
   - **Recommendation:** Core 30 tables (Tier 1-4) to support all 150 dbt models
   
3. **Cloud Provider:** Which cloud PostgreSQL provider do you prefer?
   - Render.com (my recommendation)
   - Supabase
   - Railway.app
   - Other?

4. **Data Volume:** Are 5,000 patients / 15,000 appointments sufficient, or do you want more?

5. **Existing Schema:** Do you have a way to export your production schema (structure only, no data)?

6. **Budget:** Any budget for cloud hosting ($0-$10/month)?

7. **Documentation Strategy:** Review `docs/dbt/dbt_documentation_strategy.md` for dbt Cloud deployment plan

---

## **16. DELIVERABLES**

Once complete, you'll have:

1. ✅ **Synthetic Data Generator** - Reusable Python tool
2. ✅ **Cloud Demo Database** - Publicly accessible PostgreSQL
3. ✅ **dbt Cloud Project** - Live documentation and lineage (see `docs/dbt/dbt_documentation_strategy.md`)
4. ✅ **GitHub Repository** - Code for generator (can be in same repo)
5. ✅ **Documentation** - How to regenerate/customize data
6. ✅ **Portfolio Asset** - Professional showcase of your skills
   - Public dbt documentation URL
   - Interactive lineage graph (150+ models)
   - Complete data dictionary
   - Test results and data quality metrics

---

## **YOUR FEEDBACK NEEDED** 🎯

Please review this plan and provide:
- ✅ **Approval** - Ready to start implementing
- 🔧 **Modifications** - What changes would you like?
- ❓ **Questions** - What needs clarification?
- 💡 **Suggestions** - Any additions or alternatives?

---

## **NEXT STEPS**

Once you approve (with any modifications):

1. **Immediate:** Create project structure and database setup
2. **Week 1:** Build foundation data generators
3. **Week 2-3:** Build clinical and financial data generators
4. **Week 4:** Validation and dbt testing
5. **Week 5:** Cloud deployment and dbt Cloud setup

Ready to build a professional showcase of your dental analytics platform! 🚀

