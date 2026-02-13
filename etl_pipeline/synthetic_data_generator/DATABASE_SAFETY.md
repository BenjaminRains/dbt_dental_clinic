# 🔒 Database Safety Guide

## The Three-Database System

Your environment has **three separate databases** with different purposes:

| Database | Purpose | Access Via | Risk Level |
|----------|---------|------------|------------|
| **Clinic** | Real patient data (HIPAA) | ETL environment (clinic) | 🔴 CRITICAL - Never modify |
| **Test** | Testing/development | ETL environment (test) | 🟡 CAUTION - Used for testing |
| **opendental_demo** | Synthetic data only | Synthetic generator | 🟢 SAFE - Fake data only |

---

## ⚠️ The Challenge

When you run `etl-init`:
- Choose **clinic** → Sets environment vars for clinic database
- Choose **test** → Sets environment vars for test database

**BUT** the synthetic data generator needs to write to `opendental_demo` (neither of those!)

## ✅ The Solution

### Option 1: Safe Wrapper Script + .env_demo (Recommended)

**Setup once:**
```powershell
cd etl_pipeline/synthetic_data_generator
copy .env_demo.template .env_demo
# Edit .env_demo and set DEMO_POSTGRES_PASSWORD
```

**Use forever:**
```powershell
# The wrapper ensures you ALWAYS write to opendental_demo
.\generate.ps1 -Patients 100
```

**Why this is safe:**
- ✅ Database name is hardcoded to `opendental_demo` in the script
- ✅ Credentials stored securely in .env_demo (gitignored)
- ✅ Shows confirmation before running
- ✅ Cannot accidentally write to clinic/test
- ✅ Clear output showing target database
- ✅ CLI args override .env_demo settings if needed

### Option 2: Direct Python (Manual)

Always specify the database explicitly:

```bash
python main.py \
  --db-name opendental_demo \
  --db-user postgres \
  --db-password yourpassword \
  --patients 100
```

**Requirements:**
- ⚠️ Must specify `--db-name opendental_demo` every time
- ⚠️ Easy to forget and use wrong database
- ⚠️ No safety confirmation

---

## 🛡️ Safety Checklist

Before running the generator:

### ✅ Environment Setup
- [ ] Run `etl-init` (choose either clinic or test for package installation)
- [ ] Navigate to `cd etl_pipeline/synthetic_data_generator`
- [ ] Confirm `opendental_demo` database exists: `psql -l | grep opendental_demo`

### ✅ Database Verification
- [ ] Database name is **opendental_demo** (not opendental_analytics!)
- [ ] Schema file created from production (structure only, no data)
- [ ] Schema applied to demo database

### ✅ Running Generator
- [ ] Use `generate.ps1` wrapper script (safest)
- [ ] OR specify `--db-name opendental_demo` explicitly
- [ ] Verify output shows: "Target database: opendental_demo"

---

## 🚨 Common Mistakes to Avoid

### ❌ DANGER: Don't do this!
```bash
# BAD - Uses whatever database is in environment variables
python main.py --patients 5000
```
**Risk:** Could write to clinic or test database!

### ❌ DANGER: Don't do this!
```bash
# BAD - Typo in database name
python main.py --db-name opendental_analytics --patients 5000
```
**Risk:** Writing to analytics database instead of demo!

### ✅ SAFE: Do this instead
```powershell
# GOOD - Wrapper script enforces correct database
.\generate.ps1 -Patients 5000 -DbPassword "yourpassword"
```

---

## 📊 Environment Variable Matrix

| Environment Choice | ETL_ENVIRONMENT | Database Vars Set | Synthetic Generator Uses |
|-------------------|-----------------|-------------------|--------------------------|
| `etl-init` → production | production | OPENDENTAL_SOURCE_DB, etc. | ❌ Ignores (uses CLI args) |
| `etl-init` → test | test | TEST_OPENDENTAL_SOURCE_DB, etc. | ❌ Ignores (uses CLI args) |
| Direct CLI args | N/A | None | ✅ Uses CLI args directly |
| Wrapper script | N/A | None | ✅ Hardcoded to opendental_demo |

**Key Insight:** The ETL environment is only needed for **package installation** (faker, etc.). The generator's database connection is **completely separate** and controlled via CLI arguments.

---

## 🔍 How to Verify Safety

### Before Running
```powershell
# Check what database environment vars are set
$env:OPENDENTAL_SOURCE_DB          # Should be production DB
$env:TEST_OPENDENTAL_SOURCE_DB     # Should be test DB
# Neither of these affects the generator!
```

### During Running
Watch for this in the output:
```
Target database: opendental_demo  # ✅ Correct!
Host: localhost
```

### After Running
```sql
-- Verify data is in the right place
\c opendental_demo
SELECT COUNT(*) FROM raw.patient;  -- Should show your generated patients

\c opendental_analytics
SELECT COUNT(*) FROM raw.patient;  -- Should show REAL patient count (unchanged!)
```

---

## 💡 Best Practice Workflow

### First Time Setup
```powershell
# 1. Activate ETL environment (for packages only)
etl-init
# Choose: test (doesn't matter which, just need packages)

# 2. Navigate to generator and configure
cd etl_pipeline/synthetic_data_generator
copy .env_demo.template .env_demo
notepad .env_demo  # Set DEMO_POSTGRES_PASSWORD
```

### Every Time You Generate
```powershell
# 1. Ensure ETL environment is active
etl-init  # If not already active

# 2. Navigate to generator
cd etl_pipeline/synthetic_data_generator

# 3. Run with just patient count (credentials from .env_demo)
.\generate.ps1 -Patients 100
# Shows: "📄 Loading configuration from .env_demo"
# Confirms target: opendental_demo ✅
# Prompts: Continue? (yes/no)
# You type: yes
# Generates data safely!

# 4. Verify
psql -d opendental_demo -c "SELECT COUNT(*) FROM raw.patient;"
```

---

## 🆘 Emergency: What If I Made a Mistake?

### If you accidentally wrote to the wrong database:

```sql
-- 1. Check what happened
\c <database_name>
SELECT MAX("SecDateEntry") FROM raw.patient;
-- If this is TODAY and matches your generation time, you might have a problem

-- 2. For test database (recoverable)
-- Drop and recreate from backup or re-run ETL

-- 3. For production database (CRITICAL)
-- DO NOT MODIFY FURTHER
-- Contact DBA immediately
-- Restore from backup
```

**Prevention:** Always use `generate.ps1` wrapper script! 🔒

---

## 📝 Summary

1. ✅ **Always use the wrapper script**: `.\generate.ps1`
2. ✅ **Verify target database**: Should always show `opendental_demo`
3. ✅ **ETL environment is for packages only**: clinic/test choice doesn't affect target DB
4. ✅ **CLI args override everything**: `--db-name` is your safety net
5. ✅ **When in doubt, check twice**: Better safe than sorry with production data!

---

**Remember:** The synthetic data generator is a **one-way tool** - it writes data, never reads from production. Keep it that way! 🛡️

