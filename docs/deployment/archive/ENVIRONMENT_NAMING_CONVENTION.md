# Environment Naming Convention — historical plan (ARCHIVED)

**Status:** Superseded — archived 2026-07-20.  
**Day-to-day naming + env inventory:** [../ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md) §2.2.  
**Setup cheat sheet:** [../ENVIRONMENT_SETUP.md](../ENVIRONMENT_SETUP.md).

This file was a pre-clinic planning doc to rename confusing `production` (demo API) → `demo` / `clinic`. That rename is **done** in code and in `ENVIRONMENT_FILES.md`. Do not extend this checklist; update `ENVIRONMENT_FILES.md` instead.

Below is the original planning text kept for history.

---

# Environment Naming Convention & Refactoring Plan

**Status:** Planning *(historical — see archive banner above)*  
**Priority:** CRITICAL (Before clinic deployment)  
**Goal:** Establish consistent, clear terminology for all environments

---

## 🎯 Proposed Naming Convention

### Three-Tier Environment Model

```
┌─────────────────────────────────────────────────────────────┐
│                    DEVELOPMENT                              │
│  Local Development (localhost)                             │
│  - Where code is written and tested                         │
│  - Connects to localhost databases                         │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
        ┌───────────────────┴───────────────────┐
        │                                       │
        ▼                                       ▼
┌──────────────────────┐          ┌──────────────────────┐
│   DEPLOYMENT PATH 1   │          │   DEPLOYMENT PATH 2   │
│   Portfolio/Demo      │          │   Clinic (MDC/GLIC)   │
│   (Public)            │          │   (Private, IP-restricted)│
└──────────────────────┘          └──────────────────────┘
```

---

## 📋 Environment Definitions

### 1. **Local Development** (`local` or `dev`)

**Purpose:** Where development happens  
**Location:** Developer's machine (localhost)  
**Access:** Local only  
**Database:** Can connect to any database (local, remote, demo, or analytics)

**Characteristics:**
- ✅ Development and testing
- ✅ Can access demo or analytics databases
- ✅ No public access
- ✅ Flexible configuration

**Examples:**
- `http://localhost:8000` (API)
- `http://localhost:3000` (Frontend)
- Local PostgreSQL instances

---

### 2. **Portfolio/Demo Deployment** (`demo` or `portfolio`)

**Purpose:** Public portfolio site with synthetic data  
**Location:** AWS (N. Virginia)  
**Access:** Public internet  
**Data:** Synthetic/anonymized data only (no PHI)

**Infrastructure:**
- **Frontend**: `dbtdentalclinic.com` (S3 + CloudFront)
- **API**: `api.dbtdentalclinic.com` (EC2 + ALB)
- **Database**: `opendental_demo` (PostgreSQL on demo EC2 instance)
- **API Key**: `DEMO_API_KEY` (public, safe to expose)

**Characteristics:**
- ✅ Publicly accessible
- ✅ Synthetic data only
- ✅ No PHI
- ✅ Safe for portfolio/demonstration

**Current Confusion:**
- ❌ Currently called "production" in code
- ❌ Uses `API_ENVIRONMENT=production`
- ❌ File: `.env_api_production`
- ✅ **Should be**: `API_ENVIRONMENT=demo` or `portfolio`

---

### 3. **Clinic Deployment** (`clinic` / `production` / `prod`)

**Purpose:** Real clinic infrastructure for MDC/GLIC - **This is the production environment**  
**Location:** AWS (N. Virginia)  
**Access:** IP-restricted (clinic offices only)  
**Data:** Real patient data (PHI-protected)

**Infrastructure:**
- **Frontend**: `clinic.dbtdentalclinic.com` (S3 + CloudFront + WAF)
- **API**: `api-clinic.dbtdentalclinic.com` (EC2 + ALB)
- **Database**: `opendental_analytics` with schema-based multi-tenancy:
  - `mdc` schema (MDC clinic data)
  - `glic` schema (GLIC clinic data)
- **API Key**: `CLINIC_API_KEY` (private, IP-restricted)

**Characteristics:**
- ⚠️ IP-restricted access
- ⚠️ Real PHI data
- ⚠️ HIPAA-protected
- ✅ Multi-tenant (MDC, GLIC)
- ✅ **This is the production environment** - the only live system with real patient data

**Naming Notes:**
- **Primary name:** `clinic` (used in code/config for clarity and specificity)
- **Also valid:** `production` or `prod` (semantically correct since this is the production environment)
- **Rationale:** `clinic` is preferred because it's more specific and clearly indicates this is clinic-specific infrastructure, not generic "production". However, using "prod" or "production" for this environment is semantically correct and acceptable, especially in infrastructure naming (e.g., S3 buckets, CloudFront distributions).

**Current Status:**
- ❌ Not yet deployed
- ❌ Infrastructure needs to be built
- ✅ **Will use**: `API_ENVIRONMENT=clinic` (or `production`/`prod` if preferred)

---

### 4. **Testing Environment** (`test`)

**Purpose:** Local testing environment  
**Location:** Developer's machine (localhost)  
**Access:** Local only  
**Database:** Test database (separate from development)

**Characteristics:**
- ✅ Local testing
- ✅ Isolated test database
- ✅ No public access
- ✅ Used for automated tests

**Note:** Test environment remains local (localhost), separate from deployment paths.

---

## 🔄 Current vs. Proposed Terminology

### Current (Confusing)

| Current Term | Actual Meaning | Database | Access |
|-------------|----------------|----------|--------|
| `production` | Demo/Portfolio API | `opendental_demo` | Public |
| `local` | Local development | `opendental_analytics` | Localhost |
| `test` | Testing | Test database | Localhost |
| ❌ **Missing** | Clinic deployment | `opendental_analytics` | IP-restricted |

### Proposed (Clear)

| Proposed Term | Meaning | Database | Access | Domain |
|--------------|---------|----------|--------|--------|
| `local` | Local development | Any (flexible) | Localhost | `localhost:8000` |
| `demo` | Portfolio/Demo site | `opendental_demo` | Public | `api.dbtdentalclinic.com` |
| `clinic` | Clinic deployment | `opendental_analytics` (mdc/glic) | IP-restricted | `api-clinic.dbtdentalclinic.com` |
| `test` | Testing (local) | Test database | Localhost | `localhost:8000` |

---

## 📝 Naming Convention

**Standard Environment Names:**

```
local      → Local development (localhost)
demo       → Portfolio/Demo deployment (public, AWS) - NOT production
clinic     → Clinic deployment (private, IP-restricted, AWS) - THIS IS PRODUCTION
test       → Testing environment (localhost)
```

**Also Acceptable:**
- `production` or `prod` → Can be used as alias for `clinic` (semantically correct)
- Especially appropriate for AWS resource naming (e.g., S3 buckets, CloudFront distributions)

**Rationale:**
1. **Clear Intent**: `demo` clearly means demo/portfolio deployment (NOT production)
2. **Clear Intent**: `clinic` clearly means clinic deployment with real PHI (THIS IS PRODUCTION)
3. **Production Terminology**: The clinic deployment is the only production environment. Using "prod" or "production" for clinic is semantically correct and acceptable.
4. **Self-Documenting**: Environment names clearly indicate purpose
5. **Test Remains Local**: Testing environment stays on localhost, separate from deployment paths

---

## 🔧 Refactoring Plan

### Phase 1: Add New Environment Support

**Update `api/config.py`:**
```python
class Environment(Enum):
    """Supported API environments."""
    LOCAL = "local"      # Local development (localhost)
    DEMO = "demo"        # Portfolio/Demo deployment (public, AWS)
    CLINIC = "clinic"    # Clinic deployment (private, IP-restricted, AWS)
    TEST = "test"        # Testing environment (localhost)

# NOTE: PRODUCTION is REMOVED - use DEMO instead
# This is a breaking change - all references to "production" must be updated to "demo"
```

**Update Environment Mappings:**
```python
ENV_MAPPINGS = {
    Environment.LOCAL.value: {
        DatabaseType.ANALYTICS.value: {
            'host': 'POSTGRES_ANALYTICS_HOST',
            'port': 'POSTGRES_ANALYTICS_PORT',
            'database': 'POSTGRES_ANALYTICS_DB',
            'user': 'POSTGRES_ANALYTICS_USER',
            'password': 'POSTGRES_ANALYTICS_PASSWORD'
        }
    },
    Environment.DEMO.value: {
        DatabaseType.ANALYTICS.value: {
            'host': 'DEMO_POSTGRES_HOST',
            'port': 'DEMO_POSTGRES_PORT',
            'database': 'DEMO_POSTGRES_DB',
            'user': 'DEMO_POSTGRES_USER',
            'password': 'DEMO_POSTGRES_PASSWORD'
        }
    },
    Environment.CLINIC.value: {
        DatabaseType.ANALYTICS.value: {
            'host': 'POSTGRES_ANALYTICS_HOST',  # Same RDS, different schemas
            'port': 'POSTGRES_ANALYTICS_PORT',
            'database': 'POSTGRES_ANALYTICS_DB',
            'user': 'POSTGRES_ANALYTICS_USER',
            'password': 'POSTGRES_ANALYTICS_PASSWORD'
        }
    },
    Environment.TEST.value: {
        DatabaseType.ANALYTICS.value: {
            'host': 'TEST_POSTGRES_ANALYTICS_HOST',
            'port': 'TEST_POSTGRES_ANALYTICS_PORT',
            'database': 'TEST_POSTGRES_ANALYTICS_DB',
            'user': 'TEST_POSTGRES_ANALYTICS_USER',
            'password': 'TEST_POSTGRES_ANALYTICS_PASSWORD'
        }
    }
}
```

### Phase 2: Update Environment Files

**Rename Files:**
- `.env_api_production` → `.env_api_demo` (portfolio/demo)
- Create `.env_api_clinic` (clinic deployment)
- Keep `.env_api_local` (local development)
- Keep `.env_api_test` (testing)

**File Structure:**
```
api/
├── .env_api_local      # Local development
├── .env_api_demo       # Portfolio/Demo deployment (public)
├── .env_api_clinic     # Clinic deployment (private, IP-restricted)
└── .env_api_test       # Testing
```

### Phase 3: Update Deployment Credentials

**Add to `deployment_credentials.json`:**

```json
{
  "environments": {
    "local": {
      "description": "Local development environment",
      "location": "Developer machine (localhost)",
      "access": "Local only",
      "database": "Flexible (can connect to any)"
    },
    "demo": {
      "description": "Portfolio/Demo deployment (public)",
      "location": "AWS N. Virginia",
      "access": "Public internet",
      "frontend": "dbtdentalclinic.com",
      "api": "api.dbtdentalclinic.com",
      "database": "opendental_demo",
      "data_type": "Synthetic/anonymized (no PHI)"
    },
    "clinic": {
      "description": "Clinic deployment (private, IP-restricted)",
      "location": "AWS N. Virginia",
      "access": "IP-restricted (clinic offices only)",
      "frontend": "clinic.dbtdentalclinic.com",
      "api": "api-clinic.dbtdentalclinic.com",
      "database": "opendental_analytics",
      "schemas": ["mdc", "glic"],
      "data_type": "Real patient data (PHI-protected)"
    },
    "test": {
      "description": "Testing environment (local)",
      "location": "Developer machine (localhost)",
      "access": "Local only",
      "database": "Test database (separate from development)"
    }
  }
}
```

### Phase 4: Update Documentation

**Update all references:**
- `TODO.md` - Use new terminology
- `api/README.md` - Update environment descriptions
- `docs/deployment/*.md` - Update all deployment docs
- Code comments - Update inline documentation

---

## 📊 Environment Comparison Matrix

| Aspect | Local Dev | Demo/Portfolio | Clinic Deployment | Test |
|--------|-----------|----------------|-------------------|------|
| **Environment Name** | `local` | `demo` | `clinic` | `test` |
| **Location** | Localhost | AWS (N. Virginia) | AWS (N. Virginia) | Localhost |
| **Frontend Domain** | `localhost:3000` | `dbtdentalclinic.com` | `clinic.dbtdentalclinic.com` | `localhost:3000` |
| **API Domain** | `localhost:8000` | `api.dbtdentalclinic.com` | `api-clinic.dbtdentalclinic.com` | `localhost:8000` |
| **Database** | Flexible | `opendental_demo` | `opendental_analytics` | Test database |
| **Schemas** | N/A | N/A | `mdc`, `glic` | N/A |
| **Data Type** | Any | Synthetic | Real PHI | Test data |
| **Access** | Local only | Public | IP-restricted | Local only |
| **API Key** | N/A | `DEMO_API_KEY` | `CLINIC_API_KEY` | N/A |
| **CORS Origins** | `localhost:3000` | `dbtdentalclinic.com` | `clinic.dbtdentalclinic.com` | `localhost:3000` |
| **Security** | Development | Public (safe) | HIPAA-protected | Testing |

---

## ✅ Implementation Checklist

### Before Clinic Deployment

- [ ] **Update `api/config.py`**
  - [ ] Add `DEMO` and `CLINIC` to Environment enum
  - [ ] **Remove `PRODUCTION` from Environment enum** (breaking change - no backward compatibility)
  - [ ] Update ENV_MAPPINGS (remove `production` mappings)
  - [ ] Add validation to reject `production` with clear error message directing to use `demo`

- [ ] **Create Environment Files**
  - [ ] Rename `.env_api_production` → `.env_api_demo`
  - [ ] Create `.env_api_clinic` (template)
  - [ ] Update `.env_api_local` if needed

- [ ] **Update `deployment_credentials.json`**
  - [ ] Add `environments` section with all four environments (`local`, `demo`, `clinic`, `test`)
  - [ ] Document infrastructure for each
  - [ ] Add clinic deployment section (to be filled as we build)

- [ ] **Update Documentation**
  - [ ] Update `TODO.md` terminology
  - [ ] Update `api/README.md`
  - [ ] Update deployment docs

- [ ] **Update Code References**
  - [ ] Update `api/main.py` environment detection
  - [ ] Update deployment scripts
  - [ ] Update environment_manager.ps1

### During Clinic Deployment

- [ ] **Document Infrastructure in `deployment_credentials.json`**
  - [ ] Add clinic frontend section (S3, CloudFront, Route 53)
  - [ ] Add clinic API section (ALB rule, SSL, Route 53)
  - [ ] Add clinic database schemas (mdc, glic)
  - [ ] Document API keys and secrets

---

## 🎯 Key Principles

1. **Local Dev**: Where development happens (flexible, localhost)
2. **Demo/Portfolio**: Public site with synthetic data (safe, public, AWS) - NOT production
3. **Clinic**: Private deployment with real PHI (restricted, secure, AWS) - **This IS production**
4. **Test**: Testing environment (local, separate from development)
5. **Documentation**: Update `deployment_credentials.json` as we build infrastructure
6. **Consistency**: Use same terminology across all code, docs, and config
7. **Production Terminology**: The clinic deployment is the only production environment. Using "prod" or "production" for clinic infrastructure is semantically correct and acceptable, especially for AWS resource naming (e.g., `clinic-frontend-prod` bucket name).

---

## 📚 Reference

- **Current State**: `api/config.py` - Environment enum and mappings
- **Deployment Credentials**: `deployment_credentials.json` - Infrastructure documentation
- **Implementation Plan**: `CLINIC_DEPLOYMENT_IMPLEMENTATION_PLAN.md` - Step-by-step guide

---

**Next Steps:**
1. **BREAKING CHANGE**: Update `api/config.py` - Remove `PRODUCTION`, add `DEMO` and `CLINIC`
2. **Find and replace all `production` references** - Update to `demo` immediately (no backward compatibility)
3. **Rename `.env_api_production` → `.env_api_demo`** (required - file will not work otherwise)
4. **Update all code references** - `api/main.py`, scripts, deployment files
5. **Update `deployment_credentials.json`** structure with all four environments
6. **Test that `production` is rejected** - Verify breaking change works correctly
7. **Proceed with clinic deployment** using new terminology (`demo` and `clinic`)

**⚠️ IMPORTANT: This is a breaking change. All references to `production` must be updated immediately. No backward compatibility will be provided.**
