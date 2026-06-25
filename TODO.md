# TODO List

This file contains all pending TODO items organized by codebase area.

## 🎯 PRIORITY GUIDE (Updated June 2026)

**CRITICAL (Do First):**
- **Airflow** — Phase A nearly complete: native Windows Airflow up, both DAGs loaded (paused); unpause `etl_pipeline` + smoke test, then Phase B clinic cutover
- **Frontend evolution** — shift clinic UX from metric dashboards to role-based operational decision platform ([proposal](docs/frontend/FRONTEND_EVOLUTION_PROPOSAL.md))
- **Frontend split** — portfolio vs clinic two-app refactor ([plan](docs/frontend/FRONTEND_SPLIT_PLAN.md))
- Client server deployment — **Phases 1–3.5 largely complete**; remaining: **additional tenant schemas + multi-tenant infra** (Phase 4)
- QuickBooks Online integration
- BI tool connectors (Power BI / Tableau)
- Snowflake integration (dual-warehouse / portfolio)

**HIGH (Important but not blocking):**
- ETL standardization refactor (stable but could benefit)
- ETL performance optimization (stable but could be faster)

**MEDIUM (Nice to have):**
- dbt test fixes
- dbt model fixes
- Frontend enhancements

**LOW (Back seat - defer):**
- **AWS cost optimization** — account ~$105/month; within expectations (see cost section below)
- **Event-driven analytics layer** — Kafka replay from existing marts; portfolio/educational only ([proposal](docs/streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md))
- **ML on analytics warehouse** — churn / no-show / collections; batch scoring first ([proposal](docs/ml/ML_ANALYTICS_PROPOSAL.md))
- Dashboard development
- Additional modeling work
- Non-critical refactoring

---

## 🚨 CRITICAL PRIORITY - Airflow Orchestration

### Airflow Orchestration — Deploy and Validate

**Status:** Phase A nearly complete — UI live, both DAGs loaded (paused); unpause + smoke test remaining  
**Priority:** **CRITICAL**  
**Goal:** Run ETL + dbt on a schedule without manual `etl-run` / `run_dbt_on_ec2.ps1` triggers

**Canonical plan:** [`airflow/ORCHESTRATION_ROADMAP.md`](airflow/ORCHESTRATION_ROADMAP.md) (updated 2026-06-19)

**Current maturity:**
- **DAG code:** ✅ `schema_analysis`, `etl_pipeline`, integrated `dbt_build` task group
- **Docker sandbox:** ✅ `Dockerfile.airflow`, `docker-compose.yml` (optional; not clinic nightly path)
- **Orchestration host:** ✅ **Option A** — native Airflow on VPN-connected dev laptop ($0 AWS)
- **Phase A bootstrap:** ✅ `.venv-airflow`, SQLite metadata, Fernet, Variables, Windows POSIX patches, two-terminal start
- **UI verification:** ✅ http://localhost:8080 — `etl_pipeline` + `schema_analysis` visible; **both paused** (expected default)
- **Validation:** ⏳ Unpause `etl_pipeline`; manual trigger outside business hours; `pytest airflow/tests/ -v`

**Decisions** (see roadmap):
1. ~~Where should the 9 PM clinic run execute?~~ → **Dev laptop + WireGuard VPN, native Airflow**
2. First validation: Phase A native test env, then Phase B clinic via `.env_clinic`
3. Nightly full `dbt build` vs split slow marts — _TBD_ (schema + ETL ~32 min; dbt dominates total time)

**Required actions** (see roadmap phases):
- [x] **Phase A (bootstrap):** Native venv, init script, Windows patches, scheduler + webserver running
- [ ] **Phase A (smoke):** Unpause `etl_pipeline`; trigger manually after 9 PM Central; confirm validation → ETL → dbt
- [ ] **Phase A:** `pytest airflow/tests/ -v`
- [ ] **Phase B:** Clinic on laptop — Variables incl. `publish_environment=clinic`; tunnel before publish
- [ ] **Phase C:** Enable schedule; alerts; monitor 2–3 full runs (schema → ETL → dbt → publish → notify)
- [ ] **Phase D (later):** Selectors, Cosmos, split slow dbt models; re-evaluate EC2 if laptop unreliable
- [ ] **Phase D (later):** Wire `dbt source freshness` before `dbt build` in Airflow (YAML configured; not run today — see [`airflow/DBT_DAG_PLAN.md`](airflow/DBT_DAG_PLAN.md))

**Key files:**
- `airflow/ORCHESTRATION_ROADMAP.md` — gaps, phased plan, open decisions
- `scripts/utils/init-airflow-native.ps1`, `scripts/utils/start-airflow-native.ps1` — Windows native bootstrap
- `scripts/utils/run_airflow.py`, `scripts/utils/windows_posix_stubs/` — Windows dev shims
- `airflow/dags/etl_pipeline_dag.py` — ETL + dbt (integrated)
- `airflow/DEPLOYMENT_STRATEGY.md`, `airflow/NIGHTLY_RUN.md`, `airflow/DAGS_STATUS.md`
- `docker-compose.yml`, `Dockerfile.airflow`

**Related:** Fix EC2 dbt Database Connection Credentials (blocks `dbt_target=clinic` on RDS)

---

## 🚨 CRITICAL PRIORITY - Frontend Evolution (Operational Decision Platform)

### Role-Based Clinic UX — From Dashboards to Decision Support

**Status:** Draft for discussion — ready to implement Phase 1  
**Priority:** **CRITICAL** (next after Airflow)  
**Goal:** Transform the clinic frontend from a collection of metric pages into an operational decision platform that answers "What needs attention today?" for each staff role

**Plan:** [docs/frontend/FRONTEND_EVOLUTION_PROPOSAL.md](docs/frontend/FRONTEND_EVOLUTION_PROPOSAL.md)

**Current gaps:**
- `ClinicHome.tsx` is a placeholder; users land on a stub instead of actionable content
- Flat metric-domain sidebar (Dashboard, Revenue, AR, …) — no role-based workflows
- Queues (AR priority, revenue recovery) buried inside metric pages, not first-class
- No exception framing, narrative briefs, or insurance-specific views

**Phase 1 — Role-Based Home Pages (4–6 weeks):**
- [ ] Role picker or default role (localStorage; no full auth required initially)
- [ ] **Practice Manager Home** — exception insights + top work queues (collections, revenue recovery, scheduling)
- [ ] **Owner Home** — financial snapshot, AR trend, top risks, referral summary
- [ ] **Front Desk Home** — today's appointments, week gaps, reactivation short-list
- [ ] Shared components: `InsightCard`, `WorkQueue`, `MorningBrief`
- [ ] Navigation restructure: sidebar groups **Home · Queues · Reports**
- [ ] Extract queue tables from AR/Revenue into shared `WorkQueue`
- [ ] Refactor extract: `useApiQuery`, `DateRangeFilter`, `DataTable`, `TabPanel`

**Phase 2 — Operational Work Queues (4–6 weeks):**
- [ ] Promote `/ar/priority-queue` → `/queues/collections`
- [ ] Promote revenue recovery with `recommended_actions` prominently
- [ ] Scheduling queue from appointments today/upcoming
- [ ] New endpoints as needed: treatment acceptance, reactivation, insurance follow-up

**Phase 3+ — Exception framing, narrative briefs, guided investigation** (see proposal)

**Reuse existing API:** `dashboardApi`, `arApi`, `revenueApi`, `appointmentApi`, `hygieneApi`, `treatmentAcceptanceApi`

**Related:** [docs/frontend/OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md](docs/frontend/OFFICE_KNOWLEDGE_ANALYTICS_PROPOSAL.md) (insurance knowledge track)

**Success criteria:** Practice Manager opens clinic site and sees prioritized items without visiting `/dashboard` or `/ar-aging`.

---

## 🚨 CRITICAL PRIORITY - Frontend Split (Portfolio vs Clinic)

### Two Deployable Apps — Shared Packages, Separate Products

**Status:** Proposed — phased refactor  
**Priority:** **CRITICAL** (next after Frontend Evolution Phase 1 kickoff, or in parallel once packages land)  
**Goal:** Split the unified React app into `apps/portfolio` and `apps/clinic` with shared `packages/analytics-ui`; keep existing S3/CloudFront/ALB until it hurts

**Plan:** [docs/frontend/FRONTEND_SPLIT_PLAN.md](docs/frontend/FRONTEND_SPLIT_PLAN.md)

**Why:** Demo/portfolio and clinic are different products (audience, auth, data, network) sharing one `App.tsx`, `Layout.tsx`, and `isClinicSite()` — does not scale as clinic adds roles, PHI, and WAF restrictions.

**Phases:**
- [ ] **Phase 0** — Clinic-only routes on clinic host; split SPA deploy keys per `--target`
- [ ] **Phase 1** — `packages/analytics-ui` + `analytics-api` (npm workspaces); move shared dashboard pages and charts
- [ ] **Phase 2** — `apps/portfolio` → `mdc deploy frontend --target demo`; no AuthProvider or clinic routes
- [ ] **Phase 3** — `apps/clinic` → `mdc deploy frontend --target clinic`; delete unified `App.tsx` / `isClinicSite()`
- [ ] **Phase 4** — `mdc frontend dev --app portfolio|clinic`, deploy preflight, status command
- [ ] **Phase 5** — Hardening: remove dead code, grep-clean `isClinicSite` / `VITE_IS_DEMO`

**Non-goals:** Splitting ALB/RDS/Route53; rewriting dashboard pages or FastAPI backend; changing API hostnames.

**Exit criteria (end state):** `dbtdentalclinic.com` serves portfolio only; `clinic.dbtdentalclinic.com` serves clinic only; deploying demo cannot upload clinic bundle.

---

## 🚨 CRITICAL PRIORITY - Client Deployment & Integration

### Client Server Deployment - Secure Frontend Access

**Status:** **Phases 1–3.5 largely complete** (2026-06-17) — **Phase 4 (multi-tenant schemas + infra) next**  
**Priority:** **CRITICAL** (remaining work only)  
**Goal:** Hosted clinic site serves real analytics; MDC + GLIC isolated in separate tenant schemas on shared RDS

**Rollup:** [docs/deployment/CLINIC_DEPLOYMENT_STATUS.md](docs/deployment/CLINIC_DEPLOYMENT_STATUS.md)

#### Completed

| Phase | Focus | Status | Notes |
|-------|--------|--------|--------|
| **1** | Frontend infra (S3, CloudFront, WAF, DNS, SSL) | ✅ Complete | `clinic.dbtdentalclinic.com`; WAF **clinic-frontend-prod-waf** (IP allowlist) |
| **2** | Demo/clinic API isolation | ✅ Complete | Separate EC2 + target groups; `api-clinic.dbtdentalclinic.com`; IAM/log separation |
| **3** | Clinic frontend build & deploy | ✅ Complete | `mdc deploy frontend --target clinic` / `clinic-frontend-deploy`; SPA loads from allowlisted IPs |
| **3.5** | Analytics data on RDS (single-tenant) | ✅ Complete | `mdc publish analytics --env clinic`; `marts`/`int`/`staging` on RDS; dashboard KPIs return 200 |

**Current live stack (N. Virginia):**
- **Frontend:** S3 + CloudFront → `https://clinic.dbtdentalclinic.com`
- **API:** Clinic EC2 via ALB host routing → `https://api-clinic.dbtdentalclinic.com`
- **Database:** RDS `dental-clinic-analytics` / `opendental_analytics` — **single-tenant** schemas (`raw`, `staging`, `int`, `marts`)

#### Remaining — Phase 4: Additional schemas & multi-tenant infra

**Status:** ⏸ Not started — do after single-tenant go-live is stable  
**Plan:** [docs/deployment/CLINIC_DEPLOYMENT_PHASE4_ACTION_PLAN.md](docs/deployment/CLINIC_DEPLOYMENT_PHASE4_ACTION_PLAN.md)

- [ ] **Database — per-tenant schema layers**
  - [ ] Create `mdc_raw`, `mdc_staging`, `mdc_intermediate`, `mdc_marts` (and `glic_*` mirror)
  - [ ] Set schema-level permissions for `analytics_user`
  - [ ] ETL tracking tables in each tenant raw schema
  - [ ] Cross-clinic patient identity layer (patients seen at MDC + GLIC)

- [ ] **ETL — schema routing**
  - [ ] Route MDC OpenDental source → `mdc_raw`; GLIC → `glic_raw` (`POSTGRES_ANALYTICS_SCHEMA`)
  - [ ] Verify tenant isolation (no cross-schema leakage)

- [ ] **dbt — multi-tenant targets**
  - [ ] Add `mdc` / `glic` targets in `profiles.yml`; tenant-aware `generate_schema_name`
  - [ ] Point sources at tenant raw schemas; test `dbt run --target mdc` and `--target glic`

- [ ] **API — multi-tenant routing**
  - [ ] Clinic ID / tenant middleware; schema selection on DB connections
  - [ ] Per-clinic API keys (if required); endpoint scoping

- [ ] **Publish & ops**
  - [ ] Extend `publish_analytics_to_rds.ps1` (or per-tenant publish) for multi-schema layout
  - [ ] Automate nightly publish via Airflow (after Airflow dbt DAG exists)

#### Optional follow-ons (lower urgency)

- [ ] Portal login / role-based access (see [FRONTEND_EVOLUTION_PROPOSAL.md](docs/frontend/FRONTEND_EVOLUTION_PROPOSAL.md))
- [ ] Documentation & handoff (deployment guide, clinic staff user guide, support runbook)
- [ ] CORS / API key parity verification on each frontend redeploy

**Key files:**
- `docs/deployment/CLINIC_DEPLOYMENT_PHASE1_ACTION_PLAN.md` — Phase 1 (done)
- `docs/deployment/CLINIC_DEPLOYMENT_PHASE2_ACTION_PLAN.md` — Phase 2 (done)
- `docs/deployment/CLINIC_DEPLOYMENT_PHASE3_ACTION_PLAN.md` — Phase 3 (done)
- `docs/deployment/CLINIC_DEPLOYMENT_PHASE3_5_ANALYTICS_DATA.md` — publish path (done)
- `docs/deployment/CLINIC_DEPLOYMENT_PHASE4_ACTION_PLAN.md` — **current focus**
- `docs/deployment/CLINIC_INFRASTRUCTURE_PLAN.md` — architecture reference
- `scripts/publish/publish_analytics_to_rds.ps1` — local → RDS analytics publish
- `deployment_credentials.json.template` — S3, CloudFront, WAF, API endpoints

**Estimated effort (Phase 4):** 2–4 weeks

**Business impact:** **MEDIUM** for go-live (single-tenant works today); **HIGH** for GLIC onboarding and true multi-clinic isolation

---

### QuickBooks Online Integration

**Status:** New - Planning  
**Priority:** **CRITICAL**  
**Goal:** Integrate QuickBooks Online data into analytics platform

**Business Context:**
- ✅ Business owner has approved QuickBooks Online access
- 🎯 **High value opportunity** - Financial data integration
- Potential to enhance AR, payment, and financial analytics

**Required Actions:**
- [ ] **Phase 1: Research & Planning**
  - [ ] Review QuickBooks Online API documentation
  - [ ] Identify key data entities to integrate (invoices, payments, customers, chart of accounts, etc.)
  - [ ] Map QuickBooks data to existing dental practice data model
  - [ ] Design integration architecture (ETL vs API direct access)
  - [ ] Determine authentication method (OAuth 2.0)

- [ ] **Phase 2: Authentication Setup**
  - [ ] Set up QuickBooks Online developer account
  - [ ] Create OAuth 2.0 application
  - [ ] Implement OAuth flow for clinic connection
  - [ ] Set up token refresh mechanism
  - [ ] Store credentials securely (AWS Secrets Manager or similar)

- [ ] **Phase 3: Data Extraction**
  - [ ] Create QuickBooks API client/service
  - [ ] Implement data extraction for key entities:
    - [ ] Customers (map to patients)
    - [ ] Invoices (map to procedures/claims)
    - [ ] Payments (map to payments)
    - [ ] Chart of Accounts (financial structure)
    - [ ] Journal Entries (accounting transactions)
  - [ ] Handle pagination and rate limiting
  - [ ] Implement incremental sync (last modified dates)

- [ ] **Phase 4: ETL Integration**
  - [ ] Add QuickBooks source to ETL pipeline
  - [ ] Create raw ingestion tables for QuickBooks data
  - [ ] Implement data transformation/mapping layer
  - [ ] Create staging models for QuickBooks data
  - [ ] Integrate with existing financial models (AR, payments, etc.)

- [ ] **Phase 5: Analytics Integration**
  - [ ] Create dbt models for QuickBooks data
  - [ ] Build reconciliation models (QuickBooks vs OpenDental)
  - [ ] Enhance financial dashboards with QuickBooks data
  - [ ] Create financial reporting combining both sources

- [ ] **Phase 6: Testing & Validation**
  - [ ] Test with clinic's QuickBooks account
  - [ ] Validate data accuracy and reconciliation
  - [ ] Performance testing (API rate limits)
  - [ ] Error handling and retry logic

**Key Considerations:**
- QuickBooks API rate limits (500 requests per minute per company)
- Data mapping complexity (QuickBooks vs OpenDental terminology)
- Reconciliation between two financial systems
- Incremental sync strategy (avoid full reloads)

**Estimated Effort:** 4-6 weeks

**Business Impact:** **HIGH** - Enables comprehensive financial analytics and reconciliation

---

### BI Tool Connectors - Power BI & Tableau

**Status:** New - Planning  
**Priority:** **CRITICAL**  
**Goal:** Enable clinic to connect Power BI and/or Tableau to analytics database

**Business Context:**
- Clinic needs to consume dashboards and datasets
- BI tools provide advanced analytics capabilities
- Self-service analytics for clinic staff

**Required Actions:**
- [ ] **Phase 1: Database Access Setup**
  - [ ] Determine access method (direct connection vs API)
  - [ ] Set up read-only database user for BI tools
  - [ ] Configure network access (VPN, IP whitelisting, or secure tunnel)
  - [ ] Set up connection pooling if needed
  - [ ] Document connection strings and credentials

- [ ] **Phase 2: Power BI Connector**
  - [ ] Create Power BI data source configuration
  - [ ] Document PostgreSQL connection setup
  - [ ] Create sample Power BI reports/templates
  - [ ] Document recommended data models (star schema from marts)
  - [ ] Create Power BI data refresh schedule documentation
  - [ ] Test with clinic's Power BI account

- [ ] **Phase 3: Tableau Connector**
  - [ ] Create Tableau data source configuration
  - [ ] Document PostgreSQL connection setup
  - [ ] Create sample Tableau workbooks/dashboards
  - [ ] Document recommended data models
  - [ ] Create Tableau data refresh schedule documentation
  - [ ] Test with clinic's Tableau account

- [ ] **Phase 4: Data Model Documentation**
  - [ ] Document available schemas (raw, staging, intermediate, marts)
  - [ ] Create data dictionary for key marts tables
  - [ ] Document recommended tables for BI tools (focus on marts layer)
  - [ ] Create sample queries for common use cases
  - [ ] Document refresh cadence (ETL schedule)

- [ ] **Phase 5: Security & Governance**
  - [ ] Set up row-level security if needed (by clinic, provider, etc.)
  - [ ] Document data access policies
  - [ ] Set up monitoring for BI tool connections
  - [ ] Create user guide for clinic analysts

- [ ] **Phase 6: Alternative: API-Based Access**
  - [ ] If direct DB access not preferred, create BI-friendly API endpoints
  - [ ] Implement bulk data export endpoints
  - [ ] Create API documentation for BI tools
  - [ ] Consider GraphQL or REST API for flexible queries

**Key Files to Create:**
- `docs/bi_tools/POWER_BI_SETUP.md`
- `docs/bi_tools/TABLEAU_SETUP.md`
- `docs/bi_tools/DATA_MODEL_GUIDE.md`
- `docs/bi_tools/SAMPLE_QUERIES.md`

**Key Considerations:**
- Direct database access vs API (security vs flexibility trade-off)
- Network security (VPN, IP whitelisting, or secure tunnel)
- Read-only access with proper permissions
- Performance (query optimization for BI tools)
- Data refresh cadence alignment with ETL schedule

**Estimated Effort:** 2-3 weeks (depending on clinic's BI tool preference)

**Business Impact:** **HIGH** - Enables clinic self-service analytics

---

### Snowflake Integration

**Status:** New — Planning  
**Priority:** **CRITICAL** (portfolio / dual-warehouse; parallel to BI connectors)  
**Goal:** Add Snowflake as a second analytics warehouse for portfolio demonstration and dual-warehouse dbt patterns

**Business context:**
- Demonstrates modern stack breadth (PostgreSQL + Snowflake) for portfolio and hiring narrative
- Optional path for BI tools that prefer Snowflake over direct Postgres
- Not required for current clinic go-live (RDS Postgres remains source of truth)

**Required actions:**
- [ ] **Phase 1: Account & connectivity**
  - [ ] Snowflake trial or dev account; document connection params
  - [ ] Network / key-pair or OAuth auth pattern; secrets storage
  - [ ] Verify dbt `profiles.yml` Snowflake target

- [ ] **Phase 2: Ingestion pattern**
  - [ ] Stage design (internal / external) for mart exports
  - [ ] `COPY INTO` or Snowpipe from Postgres export / S3 parquet
  - [ ] Document refresh cadence aligned with local dbt run

- [ ] **Phase 3: dbt dual-warehouse**
  - [ ] Adapter config for Postgres (clinic) vs Snowflake (portfolio/demo)
  - [ ] Run subset of marts on Snowflake; compare row counts / tests
  - [ ] Document when to use which warehouse

- [ ] **Phase 4: BI / portfolio**
  - [ ] Optional: Power BI or Tableau against Snowflake semantic layer
  - [ ] Portfolio page or doc callout for dual-warehouse architecture

**Key considerations:**
- Cost control on Snowflake credits (dev/trial sizing)
- PHI: clinic production data stays on RDS; Snowflake limited to synthetic/demo unless explicitly approved
- Keep publish-to-RDS path as clinic production workflow

**Estimated effort:** 2–4 weeks (can run in parallel with BI connector work)

**Business impact:** **MEDIUM** for clinic operations; **HIGH** for portfolio / career positioning

---

## dbt (Data Transformation)

**Priority Note:** dbt modeling and test fixes are **MEDIUM/LOW** priority - should take back seat to client deployment and integrations

### Review dbt build failures and warnings (post–1.10 upgrade)

**Status:** Logged — needs triage  
**Priority:** **MEDIUM**  
**Date observed:** 2026-06-19  
**Command:** `mdc dbt invoke --env local -- build --target local` (dbt **1.10.22**)

**Last build summary:** `PASS=2485` · `WARN=216` · `ERROR=52` · `SKIP=2270` · `TOTAL=5023`

**Action items:**
- [ ] Triage **52 failing tests** — group by model/test type; separate data issues vs config/threshold issues
- [ ] Review **216 warnings** — e.g. `warn_procedures_missing_descriptions` on `int_procedure_complete` (1,645 rows); decide fix vs `severity` / threshold / accept
- [ ] Clear **parse deprecations** (re-run with `--no-partial-parse --show-all-deprecations`):
  - `DuplicateYAMLKeysDeprecation` (~22–23 files — duplicate `meta` / `tests` / `config` in schema YAML)
  - `MissingArgumentsPropertyInGenericTestDeprecation` (~2,268 — nest generic test args under `arguments:` or finish migration; flag `require_generic_test_arguments_property: false` in `dbt_project.yml` is temporary)
- [ ] Re-run full build after fixes; confirm nightly Airflow `dbt_build` path still green

**Related:** `docs/DEPENDENCY_ALERTS.md` (dbt 1.10 upgrade), `dbt_dental_models/dbt_project.yml` (`flags`), `_mart_treatment_plan_summary.yml.planned` (unimplemented mart)

---


**Status:** Identified - Needs Implementation  
**Priority:** **MEDIUM** (Model fixes, not blocking but should be addressed)  
**Goal:** Fix incremental model filters to use `_loaded_at` with appropriate source timestamps instead of business dates or wrong target columns  

📄 **See:** `docs/refactor/DBT_INCREMENTAL_MODEL_LOADED_AT_REFACTOR_STATUS.md` — Status, remaining 9 models, fix specs, and testing plan

**Overview:**
Phases 1–2 complete (18 models fixed). Phase 3 (9 MEDIUM models) and validation remain.

**Action Items:**
- [ ] **Phase 3: Review MEDIUM Priority Models (9 models)**
  - [ ] Review `int_collection_tasks` and `int_collection_communication` (unknown target column `model_created_at`)
  - [ ] Determine correct source timestamp for models with no target column
  - [ ] Add incremental filters to models missing them (7 models identified):
    - **Staging (4):** `stg_opendental__tasknote` (use `"DateTimeNote"`), `stg_opendental__document` (use `"DateTStamp"`), `stg_opendental__timeadjust` (use `"TimeEntry"`), `stg_opendental__toothinitial` (use `"SecDateTEdit"`)
    - **Intermediate (3):** `int_insurance_payment_allocated`, `int_patient_payment_allocated`, `int_payment_split` (filter on upstream `p._loaded_at` / `ps._loaded_at` from payment/paysplit refs)
   - [ ] Test all fixes

- [ ] **Phase 4: Validation**
  - [ ] Run full dbt build to verify all fixes
  - [ ] Check zero-row models now return data
  - [ ] Verify incremental loads work correctly
  - [ ] Monitor for any regressions

**Reference:**
- `docs/refactor/DBT_INCREMENTAL_MODEL_LOADED_AT_REFACTOR_STATUS.md` - Status, remaining models, and testing plan
- `dbt_dental_models/REFACTOR_PLAN.json` - JSON export of model analysis (if present)

---

### Fix EC2 dbt Database Connection Credentials

**Status:** Ready for Deployment and Testing  
**Priority:** **HIGH** (Blocks dbt runs on EC2)  
**Goal:** Deploy dbt credentials/SSL config to EC2 and verify RDS connection

**Credentials Source:**
- `deployment_credentials.json` → `backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value`

**Action Items:**
- [ ] **Deploy Files to EC2**
  - [ ] Deploy `setup_ec2_dbt_env.sh` to `/opt/dbt_dental_clinic/scripts/` on EC2
  - [ ] Deploy `.env_ec2` to `/opt/dbt_dental_clinic/.env` on EC2 using `deploy_ec2_env.ps1`
  - [ ] Make setup script executable: `chmod +x /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh`
  - [ ] Optionally add to `~/.bashrc` for persistence: `echo 'source /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh' >> ~/.bashrc`
  
- [ ] **Test Connection** (pending)
  - [ ] Verify `setup_ec2_dbt_env.sh` successfully loads credentials from `deployment_credentials.json`
  - [ ] Verify dbt can connect to RDS with credentials
  - [ ] Test `dbt run --target clinic --select fact_claim` (AWS production)
  - [ ] Verify SSL connection is working

**Deployment Instructions:**
```powershell
# 1. Deploy .env file to EC2
.\scripts\deploy_ec2_env.ps1

# 2. Deploy setup script to EC2 (using deploy_dbt_file.ps1 or manual)
.\scripts\deploy_dbt_file.ps1 scripts/setup_ec2_dbt_env.sh

# 3. On EC2 instance, make script executable and test:
# aws ssm start-session --target <EC2_INSTANCE_ID>
# chmod +x /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh
# source /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh
# cd /opt/dbt_dental_clinic/dbt_dental_models
# dbt run --target clinic --select fact_claim
```

**Key Files:**
- `scripts/setup_ec2_dbt_env.sh`, `scripts/deploy_ec2_env.ps1`, `scripts/run_dbt_on_ec2.ps1`
- `dbt_dental_models/profiles.yml`

---

### Test aws-ssm-init POSTGRES_ANALYTICS_* for Port Forwarding

**Status:** Pending verification  
**Priority:** MEDIUM  
**Goal:** Verify `aws-ssm-init` sets correct `POSTGRES_ANALYTICS_*` for local port forwarding (localhost:5433)

- [ ] Test `aws-ssm-init` sets correct `POSTGRES_ANALYTICS_*` for port forwarding

**Reference:** `scripts/environment_manager.ps1` — `Initialize-AWSSSMEnvironment`

---

### Set Up dbt Cloud Account for Documentation Deployment

**Status:** Planned - Future Enhancement  
**Priority:** **LOW** (Portfolio/documentation enhancement, not blocking)  
**Goal:** Create dbt Cloud account and set up public documentation for professional portfolio

**Reference:** `docs/dbt/dbt_documentation_strategy.md` - Option 1: dbt Cloud (RECOMMENDED)

**Setup Steps:**
- [ ] **Create dbt Cloud Account** (free for 1 developer)
  - [ ] Go to https://cloud.getdbt.com
  - [ ] Sign up with GitHub account
  - [ ] Create new project

- [ ] **Connect Git Repository**
  - [ ] Connect to GitHub repo: `BenjaminRains/dbt_dental_clinic`
  - [ ] Select `dbt_dental_models` as project subdirectory
  - [ ] Configure branch (main)

- [ ] **Configure Database Connection**
  - [ ] **For Demo:** Connect to `opendental_demo` (synthetic data - can be public)
    - Database: `opendental_demo`
    - Schema: `raw`
    - Host: `<your-cloud-postgres-host>`
    - Port: 5432
    - User: `demo_user`
    - Password: `<secure-password>`
  - [ ] **For Production:** Connect to `opendental_analytics` (keep private, optional)

- [ ] **Set Up dbt Cloud Environment**
  - [ ] Configure environment settings via UI
  - [ ] Use `demo` target for public documentation
  - [ ] Use `clinic` target for private/production (if configured)

- [ ] **Create Documentation Job**
  - [ ] Create job: "Generate Documentation"
  - [ ] Commands:
    ```bash
    dbt deps
    dbt seed
    dbt run --target demo
    dbt test --target demo
    dbt docs generate
    ```
  - [ ] Schedule: Daily or manual trigger

- [ ] **Access Public Documentation**
  - [ ] Get public documentation URL from dbt Cloud
  - [ ] Share URL on resume/portfolio
  - [ ] Example format: `https://cloud.getdbt.com/accounts/123/jobs/456/docs/`

**Benefits:**
- ✅ Professional presentation
- ✅ Hosted by dbt (no maintenance)
- ✅ Public shareable link
- ✅ Auto-updates when code is pushed
- ✅ Shows lineage, tests, freshness
- ✅ Industry-standard tool

**Reference:**
- `docs/dbt/dbt_documentation_strategy.md` - Full documentation deployment strategy
- dbt Cloud documentation: https://docs.getdbt.com/docs/dbt-cloud

---

### Snowflake Integration (Career/Portfolio)

**Status:** Planned  
**Priority:** **LOW** (Portfolio/career—adds Snowflake + cloud storage experience)  
**Goal:** Add Snowflake as second warehouse: file ingestion via stages + COPY INTO (Phase 1), then dbt dual-target (Phase 2). Data volume from ETL log: ~446 tables, ~20–25M rows, ~25–50 GB full warehouse.

**Reference:** `docs/career/platform_projects/snowflake_integration_plan.md`

---

### Event-Driven Analytics Layer (Kafka Replay)

**Status:** Proposed — not started  
**Priority:** **LOW** (Portfolio/educational; does not replace nightly batch ETL)  
**Goal:** Add a simulated event-driven path downstream of existing `marts` — replay real clinic workflows to Kafka, land append-only events in PostgreSQL `streaming` schema, transform with dbt, optional FastAPI/React panel. Demo data only on public infra; clinic replay localhost-only.

**Plan:** [docs/streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md](docs/streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md)

**Related:** Log-based CDC (Debezium) deferred — see [docs/etl/ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](docs/etl/ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md) §5.1 Option D

**Phases:**
- [ ] **Phase 1:** `sql/init/02_streaming_schema.sql` + Docker Compose `streaming` profile (Kafka, Kafka UI)
- [ ] **Phase 2:** Pydantic event schemas in `streaming/schemas/`
- [ ] **Phase 3:** Event generator replaying from `marts.fact_*` / `mart_*` (`--env demo` first)
- [ ] **Phase 4:** Python consumers → `streaming.*_events` landing tables
- [ ] **Phase 5:** dbt models under `dbt_dental_models/models/streaming/` (`tag:streaming`)
- [ ] **Phase 6:** DLQ + ingest validation
- [ ] **Phase 7:** Kafka UI monitoring; optional `mdc streaming` CLI + FastAPI/React replay panel

**Non-goals:** Replacing Airflow nightly DAG; PHI on public Kafka; production real-time ingestion for the clinic.

---

### Optimize mart_patient_retention Performance (dbt run time)

**Status:** Pending  
**Priority:** **MEDIUM** (Full dbt run ~52 min; one model dominates)  
**Goal:** Reduce `mart_patient_retention` build time so full pipeline runs in minutes instead of ~52 minutes.

**Current situation:**
- Full `dbt run`: 157 models in ~52 min; **mart_patient_retention** alone is ~52 min (~99% of total).
- Model does four full scans of fact tables (fact_appointment, fact_payment, fact_claim, fact_communication) with large aggregations by patient_id, then joins to dim_patient, dim_date, dim_provider.

**Action items:**
- [ ] Pre-aggregate patient-level metrics in intermediate models (e.g. int_patient_appointment_summary, int_patient_financial_summary); make those incremental or tables; have mart join pre-aggregated data only
- [ ] Consider making mart_patient_retention incremental by snapshot date (e.g. only (re)build today’s rows) or run it on a separate schedule (e.g. nightly)
- [ ] After changes: `dbt run --select mart_patient_retention` and re-run `list_slow_models.ps1` / EXPLAIN (ANALYZE, BUFFERS) to confirm improvement

**Reference:** `docs/dbt/DBT_PERFORMANCE_INVESTIGATION.md`

---

### Review Explicit Column Selection Findings Document

**Status:** Pending Review  
**Priority:** **MEDIUM** (Architectural discussion, not blocking)  
**Goal:** Review findings document and decide on refactoring approach for `select *` usage in dbt models

**Overview:**
A comprehensive modeling review has been completed that identified widespread use of `select *` in staging models (72 instances) and intermediate models (42 instances). The document outlines risks, recommendations, and implementation strategies for migrating to explicit column selection.

**Key Findings:**
- **72 staging models** use `select * from {{ source('opendental', ...) }}` (HIGH PRIORITY)
- **42 intermediate models** use `select * from {{ ref('stg_...') }}` (MEDIUM PRIORITY)
- Risk: OpenDental schema changes could break models unexpectedly
- Recommendation: Incremental refactor starting with Tier 1 critical tables

**Action Items:**
- [ ] Review `docs/dbt/explicit_column_selection_findings.md` document
- [ ] Discuss scope and timeline with team
- [ ] Decide on refactoring approach (incremental vs big bang vs new models only)
- [ ] Prioritize which models to refactor first (Tier 1: patient, appointment, procedurelog, payment, claim, provider)
- [ ] Create implementation plan if proceeding with refactor
- [ ] Update coding standards for new models

**Reference:** `docs/dbt/explicit_column_selection_findings.md`

---

### Fix dbt Test Failures - Demo Database

**Status:** Pending  
**Priority:** **MEDIUM** (Demo database, not blocking production)  
**Goal:** Fix 58 errors and 48 warnings from dbt tests on demo database  
**Reference:** `docs/dbt/dbt_test_failures_demo_analysis.md`

**Test Results:** PASS=1150 WARN=48 ERROR=58 SKIP=0 (Total: 1256 tests)

**Error Categories:**
- **Category 1:** Accepted Values Tests (3-4 failures) - Data quality issues
- **Category 2:** Database Syntax Errors (12 errors) - Test definition issues
- **Category 3:** Missing Columns (19 errors) - Schema mismatch
- **Category 4:** Not Null Tests (9 failures) - Data completeness issues
- **Category 5:** Range/Validation Tests (6-7 failures) - Business logic issues

**Recommended Fix Order:**
1. Fix test syntax errors (Category 2) - Quick wins
2. Fix missing column tests (Category 3) - Add columns or update tests
3. Investigate accepted_values failures (Category 1) - Data quality
4. Review not_null failures (Category 4) - Decide if NULLs acceptable
5. Review range/validation failures (Category 5) - May be expected for demo data

**Action Items:**
- [ ] Fix test syntax errors (duplicate column names, array_length function calls)
- [ ] Resolve missing column tests (add columns or update/remove tests)
- [ ] Investigate and fix accepted_values failures
- [ ] Review and address not_null failures
- [ ] Review range/validation failures (adjust thresholds or disable if appropriate for demo)

**Note:** Many failures may be expected for synthetic/demo data. Some tests may need to be disabled or adjusted for demo environment.

---

### Appointment Workflow Improvement - Type Classification

**Status:** Investigation Complete, Implementation Pending  
**Priority:** **LOW** (Long-term workflow improvement)  
**Goal:** Address root cause of appointment data quality issues related to unclassified appointments

**Problem Identified:**
- **68.7% of broken appointments without procedures** have `appointment_type_id = 0` (None/Unknown)
- **87.5% of past scheduled appointments** have `appointment_type_id = 0` (None/Unknown)
- Strong correlation between unclassified appointments (`appointment_type_id = 0`) and data quality issues
- Suggests workflow issue where appointments aren't properly categorized when created

**Action Items:**
- [ ] Review appointment creation workflow in OpenDental system (operational; see type classification investigation doc)
- [ ] Set up alerts if unclassified appointment rate increases (use monitoring query in DAG or run manually)

**Expected Impact:**
- Reduce data quality issues in broken appointments
- Improve appointment categorization accuracy
- Better workflow compliance

**Reference Documents:**
- **Type classification investigation:** `dbt_dental_models/validation/staging/appointment/APPOINTMENT_TYPE_CLASSIFICATION_INVESTIGATION.md`
- **Monitoring query (DBeaver/analytics DB):** `dbt_dental_models/validation/staging/appointment/monitor_unclassified_appointment_rate.sql`
- **Investigation Findings:** `dbt_dental_models/validation/staging/appointment/BROKEN_APPOINTMENTS_FINDINGS.md`
- **Investigation SQL:** `dbt_dental_models/validation/staging/appointment/investigate_appointment_data_quality.sql`
- **Model Documentation:** `dbt_dental_models/models/staging/opendental/_stg_opendental__appointment.yml`

---

### Fix Synthetic Data Dashboard Metrics

**Status:** Code Complete, Verification Pending  
**Priority:** **LOW** (Demo dashboard only, not clinic-facing)  
**Goal:** Verify fixes for zero values in executive dashboard (Revenue Lost, Recovery Potential, Collection Rate)

**Next Steps:**
- [ ] Run dbt models to verify fix: `dbt run --select mart_production_summary mart_provider_performance --target demo`
- [ ] Run diagnostic queries (see `docs/frontend/COLLECTION_RATE_FIX_DETAILS.md`) to verify payments exist
- [ ] Verify collection rate > 0% in dashboard after dbt run
- [ ] If still 0%, investigate date matching issue (payments may be on different dates than appointments)

**Reference:** `docs/frontend/SYNTHETIC_DATA_DASHBOARD_FIX_PLAN.md`

---

## ETL Pipeline

**Current Status:** ✅ **STABLE** - ETL is working reliably  
**Priority Note:** ETL refactoring and optimization are **HIGH** priority (not critical) - can be done after client deployment is complete

### Complete postgres_loader Refactor

**Status:** Implementation Complete, Verification Pending  
**Priority:** **HIGH** (ETL is stable, refactor can wait)  
**Goal:** Verify refactored `postgres_loader.py` in test and clinic environments

**Approach:** Gradual rollout with monitoring at each stage

#### 6.1 Test Environment Verification (Basic Functionality Check)
- [ ] Run ETL pipeline on **test environment** to verify basic functionality
- [ ] Verify test tables load successfully (patient, appointment, procedurelog)
- [ ] Check row counts match expected test data
- [ ] Review logs for any errors or warnings

#### 6.2 Clinic Verification - Small Tables First (Primary Verification)
- [ ] Run pipeline for **small/medium tables only** (low risk, fast to verify)
- [ ] **Verify using compare_databases.py script** (automated comparison)
- [ ] **Verify tracking information** for each table
- [ ] **Manual verification** (spot check)

#### 6.3 Clinic Verification - Medium Tables (Gradual Expansion)
- [ ] After small tables verify correctly, expand to **medium tables**
- [ ] **Verify using compare_databases.py** for all medium tables
- [ ] Monitor during execution
- [ ] Compare results with previous runs for these tables

#### 6.4 Clinic Verification - Full Pipeline (Final Step)
- [ ] After medium tables verify correctly, run **full pipeline** on clinic
- [ ] **Comprehensive verification using compare_databases.py**
- [ ] **Compare ETL tracking tables**
- [ ] Monitor during execution

#### 6.5 Compare Results with Previous Runs
- [ ] Save baseline comparison before refactor
- [ ] Compare after refactor (using compare_databases.py or SQL queries)
- [ ] Check performance metrics

#### 6.6 Verify HYBRID FIX Works Correctly
- [ ] Check HYBRID FIX Triggers (expected: 0-5 per run is normal)
- [ ] Verify Change Detection in UPSERT (check PostgreSQL stats)

#### 6.7 Monitor for Regressions
- [ ] Daily monitoring (first week)
- [ ] Weekly review (first month)

**Reference Documentation:**
- **Method Deprecation Analysis:** [etl_pipeline/docs/METHOD_DEPRECATION_ANALYSIS.md](etl_pipeline/docs/METHOD_DEPRECATION_ANALYSIS.md)
- **Database Comparison Script:** `etl_pipeline/scripts/compare_databases.py`
- **HYBRID FIX Monitoring:** `etl_pipeline/docs/monitoring_hybrid_fix.md`
- **Performance Review:** `etl_pipeline/docs/PERFORMANCE_REVIEW_20251222.md`
- **Pipeline Architecture:** `etl_pipeline/docs/PIPELINE_ARCHITECTURE.md`

---

### ETL Raw Ingestion Contract & Pipeline Health Metrics

**Status:** Investigation Complete, Implementation Pending  
**Priority:** **HIGH** (ETL is stable, standardization can wait)  
**Goal:** Fix unconditional UPSERT churn and establish reusable patterns for all raw ingestion tables

**Investigation Summary:**
Root cause identified: Unconditional UPSERT updates without change detection in `PostgresLoader._build_upsert_sql()` causing no-op updates and high `n_tup_upd` counts.

**Next Steps:**

### 1. Implement Raw Ingestion Contract
- [ ] Update `etl_pipeline/config/tables.yml` to support contract definitions
- [ ] Document contract requirements in `etl_pipeline/docs/DATA_CONTRACTS.md`

### 2. Create Pipeline Health Metrics
- [ ] Create `dbt_dental_models/models/monitoring/raw_table_churn_metrics.sql`
- [ ] Add churn monitoring to `etl_pipeline/monitoring/pipeline_health.py`
- [ ] Set up alert thresholds (Critical > 2.0, Warning > 1.0, Info > 0.5)
- [ ] Document in `etl_pipeline/docs/PIPELINE_MONITORING.md`

**Reference:** `docs/etl_commlog_churn_analysis.md`

---

### ETL Pipeline Performance Optimization

**Status:** Investigation Complete, Implementation Pending  
**Priority:** **HIGH** (ETL is stable, optimization can wait)  
**Goal:** Optimize ETL pipeline bulk insert performance from 200-250 rows/sec to 1,000-5,000+ rows/sec, reducing full sync time from 12+ hours to 1-2 hours

**Note:** ETL is currently stable and working. This optimization is valuable but not blocking client deployment.

**Reference:** `etl_pipeline/docs/PERFORMANCE_REVIEW_20251222.md`

---

### Settings Access Patterns - Naming Improvements

**Status:** ✅ Documentation Complete, Implementation Pending  
**Priority:** **MEDIUM** (Code works correctly, naming clarity improvement)  
**Goal:** Improve naming conventions for Settings access patterns to make them clearer for new developers

**Description:**
The ETL pipeline uses three Settings access patterns intentionally for test/clinic isolation. However, the current function names (`get_settings()`, `create_settings()`) don't clearly communicate their purpose, which can be challenging for new developers.

**Key Findings:**
- Current names don't clearly indicate singleton vs new instance behavior
- Unclear when to use `get_settings()` vs `Settings(...)` vs `create_settings()`
- Three patterns exist intentionally but naming doesn't reflect their purpose

**Action Items:**
- [ ] Review `etl_pipeline/CODE_REVIEW.md` section 3.3 "Settings Global Instance Management" for detailed analysis
- [ ] Review `etl_pipeline/docs/SETTINGS_ACCESS_PATTERNS.md` for comprehensive pattern documentation and naming improvement options
- [ ] Decide on naming improvement approach:
  - Option 1: Add descriptive aliases (recommended - backward compatible)
  - Option 2: Rename existing functions (breaking change)
  - Option 3: Use class methods (more OOP style)
- [ ] Implement chosen naming improvements
- [ ] Update codebase to use new naming conventions
- [ ] Update documentation to reflect new naming

**Related Files:**
- **Code Review:** `etl_pipeline/CODE_REVIEW.md` - Section 3.3 for analysis
- **Pattern Documentation:** `etl_pipeline/docs/SETTINGS_ACCESS_PATTERNS.md` - Complete guide with naming improvement recommendations

**Recommendation:**
See `etl_pipeline/docs/SETTINGS_ACCESS_PATTERNS.md` "Naming Convention Improvements" section for three implementation options with detailed code examples and trade-offs.

---

**Current Performance:**
- Large tables: 130-180 rows/second
- Medium tables: 200-250 rows/second
- Full sync: 12+ hours

**Target Performance:**
- Large tables: 1,000-5,000 rows/second (5-30x improvement)
- Medium tables: 1,000-5,000 rows/second (4-25x improvement)
- Full sync: 1-2 hours (6-12x improvement)

**Critical Bottlenecks Identified:**
- Payment table: 40-50 seconds per 10,000-row batch (200-250 rows/sec)
- Securitylog table: 6.55 hours to load 4.2M rows (178 rows/sec)

## Priority 1: Critical (Immediate Action Required)

### 1.1 Investigate PostgreSQL Insert Performance
- [ ] Check if indexes are being maintained during bulk inserts
- [ ] Verify foreign key constraints and their impact on insert performance
- [ ] Review triggers on target tables (payment, securitylog, paysplit, etc.)
- [ ] Compare UPSERT vs INSERT performance for full syncs
- [ ] Check PostgreSQL `pg_stat_statements` for slow queries during inserts
- [ ] Review connection pooling and transaction settings
- [ ] Measure network latency between ETL server and PostgreSQL

**Expected Impact:** 5-20x performance improvement

### 1.2 Optimize Bulk Insert Strategy for Full Syncs
- [ ] For full syncs, use `TRUNCATE` + `INSERT` instead of `UPSERT` (already implemented, verify usage)
- [ ] Consider using PostgreSQL `COPY` command for bulk loads instead of `executemany`
- [ ] Disable indexes during load, rebuild after (for large tables)
- [ ] Increase batch sizes for full syncs (50K-100K rows instead of 10K)
- [ ] Review `bulk_insert_optimized()` method for optimization opportunities
- [ ] Test `COPY FROM STDIN` vs current `executemany` approach

**Expected Impact:** 2-5x performance improvement

## Priority 2: High (Should Address Soon)

### 2.1 Fix Duplicate Schema Detection
- [ ] Review schema detection caching logic in `PostgresSchema` class
- [ ] Ensure schema cache is checked before running detection
- [ ] Cache results for entire ETL run duration
- [ ] Fix payment table column detection running twice

**Expected Impact:** 5-10 seconds saved per table

### 2.2 Parallelize Medium Table Processing
- [ ] Process medium tables in parallel (2-4 workers)
- [ ] Balance parallelization with database connection limits
- [ ] Monitor resource usage during parallel processing
- [ ] Update `priority_processor.py` to support parallel medium table processing

**Expected Impact:** 30-50% reduction in total time for medium tables

### 2.3 Fix Metrics Collection Error
- [ ] Review `UnifiedMetricsCollector.record_performance_metric()` signature
- [ ] Fix method call to match expected parameters (remove `duration` keyword or update method)
- [ ] Ensure all performance metrics are properly recorded
- [ ] Update all call sites to use correct method signature

**Expected Impact:** Better visibility into performance trends

## Priority 3: Medium (Nice to Have)

### 3.1 PostgreSQL Configuration Review
- [ ] Review PostgreSQL `postgresql.conf` settings
- [ ] Configure `wal_buffers` appropriately (requires server restart)
- [ ] Review other WAL and checkpoint settings
- [ ] Document required PostgreSQL configuration for optimal performance
- [ ] Fix warnings about `wal_buffers` requiring restart (currently just warnings)

**Expected Impact:** 10-20% performance improvement

### 3.2 Add Performance Monitoring
- [ ] Add detailed timing logs for each phase (extract, transform, load)
- [ ] Track rows/second metrics per table
- [ ] Create performance dashboard/report
- [ ] Set up alerts for performance degradation
- [ ] Add batch-level timing to identify slow batches

**Expected Impact:** Better visibility and early detection of issues

**Success Metrics:**
- [ ] Payment table: <5 minutes (currently 34+ minutes)
- [ ] Securitylog table: <1 hour (currently 6.55 hours)
- [ ] Full sync: <2 hours (currently 12+ hours)
- [ ] All large tables: >1,000 rows/second
- [ ] All medium tables: >1,000 rows/second

---

### Refactor Load Status Method Names

**Status:** Planned  
**Priority:** **MEDIUM** (Code quality improvement, not urgent)  
**Goal:** Improve code clarity by renaming internal methods to be self-documenting

**Overview:**
Rename two internal methods in `PostgresLoader`:
- `_update_load_status_hybrid()` → `_update_load_status_with_primary_value()`
- `_update_load_status()` → `_update_load_status_timestamp_only()`

**To complete:**
- [ ] Create branch `refactor/rename-load-status-methods` from `main`
- [ ] Work through the checklist in the refactor doc (PostgresLoader, table_processor, integration + unit tests, verification)
- [ ] Run full test suite and optional ETL smoke run
- [ ] Merge to `main` only after all tests pass on the branch

**Reference:** `docs/refactor/REFACTOR_LOAD_STATUS_METHOD_NAMES.md` — full checklist, line references, and verification steps

---

### Refactor SimpleMySQLReplicator → MySQLReplicator

**Status:** Planned  
**Priority:** **MEDIUM** (Code quality, not urgent)  
**Goal:** Rename class and remove all "simple" naming from the replicator and its dependencies.

**To complete:** Rename class to `MySQLReplicator`, module to `mysql_replicator.py`, test packages and test files, update all imports/patches/markers/result keys and docstrings. Do on a dedicated branch; run full test suite before merging.

**Reference:** `docs/refactor/REFACTOR_SIMPLE_MYSQL_REPLICATOR_TO_MYSQL_REPLICATOR.md` — scope, renames, and verification

---

## Frontend

### KPI Definitions — Hover Tooltips (Phase 4)

**Status:** Planned  
**Priority:** **LOW** (Enhancement, not blocking)  
**Goal:** Add hover-over tooltips to dashboard KPIs linking to `/kpi-definitions`

- [ ] **Phase 4:** Future Enhancement - Hover Tooltips

**Related Files:** `frontend/src/pages/KPIDefinitions.tsx`, `frontend/src/components/layout/Layout.tsx`

---

### Validation Workflow Component for Portfolio

**Status:** Planning - Feature Plan Complete  
**Priority:** **LOW** (Portfolio enhancement, not blocking)  
**Goal:** Add a "Validation Workflow" component to the React portfolio that demonstrates how mart models are validated through source reconciliation, business rule testing, and operationalized dbt tests

**Overview:**
A feature plan has been created that outlines a portfolio component showcasing the validation methodology used for mart models. This demonstrates the difference between "building models" and "shipping trusted analytics" - a key differentiator for data engineering roles.

**Key Features:**
- Overview section explaining validation methodology
- Workflow diagram showing data flow and validation lanes
- Case study using `fact_claim` as the flagship example
- Three "proof" validations (row count reconciliation, financial balance, sentinel/exception handling)
- Business rules → tests operationalization taxonomy
- Real-world impact section

**Implementation Plan:**
- [ ] **Phase 1:** Add "Validation Workflow" tile under Project Components
- [ ] **Phase 2:** Create `/validation` page with content structure
- [ ] **Phase 3:** Add Mermaid diagram (workflow visualization)
- [ ] **Phase 4:** Add 3 proof validations (detailed examples)
- [ ] **Phase 5:** Add "rules→tests" taxonomy section
- [ ] **Phase 6:** Add links to repo documentation
- [ ] **Phase 7:** (Optional) Add synthetic "results" widget

**Priority:** **LOW** - Portfolio enhancement, not blocking core functionality  
**Estimated Effort:** 12-16 hours

**Related Files:**
- **Feature Plan:** `docs/frontend/validation_workflow_component.md` ✅
- **Validation Documentation:** `dbt_dental_models/validation/README.md`
- **Fact Claim Validation Plan:** `dbt_dental_models/validation/marts/fact_claim/fact_claim_validation_plan.md`
- **Business Rules Mapping:** `dbt_dental_models/validation/marts/fact_claim/BUSINESS_RULES_TO_DBT_TESTS.md`

---

### ML on Analytics Warehouse

**Status:** Proposal drafted — not scheduled  
**Priority:** **LOW** (defer until Tier 1 marts stable and Airflow nightly validated)  
**Goal:** Supervised models for churn, no-show, or collections using dbt feature marts + batch scoring

- [ ] Pick first target (churn vs no-show vs new-patient value)
- [ ] Phase 1: point-in-time `ml_*` training mart on `opendental_demo`
- [ ] Evaluate vs rule-based scores (`churn_risk_score`, etc.)

**Docs:** [docs/ml/ML_ANALYTICS_PROPOSAL.md](docs/ml/ML_ANALYTICS_PROPOSAL.md) · [docs/ml/ML_SERVING_ARCHITECTURE.md](docs/ml/ML_SERVING_ARCHITECTURE.md)

---

## API

- `docs/AGENT_ACCESSIBILITY_AND_CLI_DESIGN.md` documents agent accessibility goals and CLI design decisions.
- Plan `agent_commands.py` module to provide curated, agent-safe command entrypoints aligned with that design
- See also ops.py

---

## Deployment / Infrastructure

### Portfolio Site Reconfiguration

**Status:** In Progress  
**Priority:** **LOW** (Demo site, not clinic-facing)  
**Goal:** Complete separation of portfolio site (demo data) from production database (opendental_analytics)

**Note:** This is for the demo/portfolio site. Client deployment is separate and higher priority.

**Architecture:**
- **Portfolio Site:** `dbtdentalclinic.com` → Demo API (`api.dbtdentalclinic.com`) → Demo Database (`opendental_demo` on demo EC2)
- **Production Database:** `opendental_analytics` (RDS) → Localhost only, never accessed by public API

**Current Status:**
- Frontend deployment (Phase 6) - In Progress
- Final verification (Phase 7.3 - End-to-End Testing) - Pending

**Important Notes:**
⚠️ **CRITICAL:** 
- The production database (`opendental_analytics`) must NEVER be accessible from the public API
- Always verify database name is `opendental_demo` before deploying
- Test with synthetic data before going live

**Related Documentation:**
- **Reconfiguration Summary:** `docs/deployment/PORTFOLIO_SITE_RECONFIGURATION.md`
- **Frontend split (portfolio vs clinic apps):** [docs/frontend/FRONTEND_SPLIT_PLAN.md](docs/frontend/FRONTEND_SPLIT_PLAN.md)
- **Demo EC2 Setup:** `docs/deployment/DEMO_DATABASE_EC2_SETUP_CHECKLIST.md`
- **Demo Database Sync:** `docs/deployment/DEMO_DATABASE_SYNC_CHECKLIST.md`
- **API Deployment Strategy:** `docs/api/API_DEPLOYMENT_STRATEGY.md`

---

### Frontend split — see CRITICAL section above

Tracking checklist and phase detail: [docs/frontend/FRONTEND_SPLIT_PLAN.md](docs/frontend/FRONTEND_SPLIT_PLAN.md) · Frontend evolution: [docs/frontend/FRONTEND_EVOLUTION_PROPOSAL.md](docs/frontend/FRONTEND_EVOLUTION_PROPOSAL.md)

---

### AWS Cost Optimization & Savings Opportunities

**Status:** Monitoring — defer active optimization  
**Priority:** **LOW** (Account spend ~$105/month is within expectations; optimize opportunistically, not urgently)  
**Goal:** Track AWS spend and apply savings when low-effort; avoid disrupting clinic deployment or Airflow work

**Current Situation (AWS Cost Explorer, June 18 2026):**
- **Month-to-date (Jun 1–18):** $60.18 (4% lower than same period last month)
- **June 2026 forecast:** $105.69/month (2% lower than May)
- **May 2026 total:** $108.26/month
- **May 2026 same period (May 1–18):** $62.88
- **Trend:** Stable ~$100–108/month since Jan 2026 (EC2, VPC, ELB, RDS, EC2-Other)
- **Alerts:** 1 budget over threshold; 1 cost anomaly MTD ($0.05 impact)

**Alignment with documented expectations:**
- **Clinic API stack** (`api/README.md`): ~$51/month (EC2 + ALB + RDS + transfer)
- **Demo/portfolio stack** (`deployment-docs/README copy.md`): <$20/month (t3.micro demo workload)
- **Account total ~$105/month** = clinic + demo + shared VPC/ELB/RDS — consistent with per-stack docs, not the outdated $128/month figure

**Top Cost Drivers (June 2026 forecast ~$106):**
- EC2 Compute: ~29% (~$30/month) — multiple instances, 24/7
- RDS PostgreSQL: ~28% (~$29/month)
- VPC Public IPs: ~21% (~$22/month)
- ELB + EC2-Other: remainder

**Optimization Strategy (when time permits):**
1. **Right-Sizing:** Review instance sizes → potential $5–10/month savings
2. **Cost Explorer recommendations:** RDS RI (~$7/mo), clinic EC2 Graviton (~$3/mo), EBS cleanup (~$2/mo), RDS downsizing (~$13/mo)

**Remaining:**
- [ ] Review instance sizes for right-sizing (opportunistic)
- [ ] **RDS (dental-clinic-analytics) Optimization**
  - [ ] Review Cost Explorer recommendation for `dental-clinic-analytics` RDS instance (~$13.14/month potential savings).
  - [ ] Confirm instance utilization and determine if instance class/storage can be downsized or if hours can be reduced.
  - [ ] Decide whether to keep On-Demand, move to Reserved Instances, or adjust instance family/size.
- [ ] **RDS Reserved Instances Evaluation**
  - [ ] Review RDS Reserved Instance recommendation (~$6.92/month savings) and validate that long-term usage justifies a 1-year or 3-year commitment.
  - [ ] If appropriate, purchase a small RI that matches the `dental-clinic-analytics` workload.
- [ ] **Clinic API EC2 Instance Optimization**
  - [ ] Review Cost Explorer recommendation for clinic API EC2 instance (`t3.small`, ~$2.92/month savings potential).
  - [ ] Evaluate feasibility of migrating to Graviton (`t4g.*`) instance type without impacting API performance.
  - [ ] If migration is safe, update launch configuration / instance type and re-run smoke tests for the clinic API + S3/CloudFront frontend.
- [ ] **EBS Volume Cleanup**
  - [ ] Review EBS volume recommendation (~$2.40/month savings) for idle/unused storage.
  - [ ] Confirm whether the identified volume is an old snapshot or unused disk related to earlier deployments.
  - [ ] If safe, delete or downsize the volume, ensuring any critical snapshots are retained elsewhere.

**Reference Documents:**
- `api/README.md` — per-stack cost breakdown (~$51/month clinic API)
- `docs/deployment/COST_OPTIMIZATION_SUMMARY.md` - Complete action plan
- `docs/deployment/COST_ANALYSIS.md` - Detailed cost breakdown
- `docs/deployment/EC2_SCHEDULING_MANUAL_SETUP.md` - EC2 scheduling implementation
- `docs/deployment/EC2_SCHEDULING_IMPLEMENTATION_PLAN.md` - Planning document
- `docs/deployment/EC2_SCHEDULING_APPROACH_COMPARISON.md` - Approach comparison
- `docs/deployment/OHIO_REGION_AUDIT_GUIDE.md` - Ohio deletion procedures

**Notes:**
- These savings are for the **clinic S3/CloudFront deployment and associated analytics stack**, not the demo portfolio environment alone.
- Re-run AWS Cost Explorer "Savings opportunities" after changes to verify recommendations are cleared.
