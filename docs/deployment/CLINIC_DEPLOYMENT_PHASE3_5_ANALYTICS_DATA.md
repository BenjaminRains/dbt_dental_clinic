# Client Server Deployment - Phase 3.5: Analytics Data on RDS

**Date:** 2026-06-16  
**Priority:** **CRITICAL — blocks clinic go-live**  
**Goal:** Populate `opendental_analytics` on RDS so the clinic API can serve dashboard data to `clinic.dbtdentalclinic.com`

---

## Overview

Phases 1–3 delivered **infrastructure and the SPA**. Phase 3.5 delivers **data** on RDS for the hosted clinic API.

**Maintainer workflow (recommended):**

1. **Local:** ETL → `localhost` `opendental_analytics.raw` (office LAN; unchanged)
2. **Local:** dbt → `staging`, `int`, `marts` on localhost (unchanged)
3. **Publish:** copy `marts`, `int`, `staging` from localhost → **RDS** (new step; nightly after dbt)

The clinic API (`api-clinic.dbtdentalclinic.com`) reads **RDS only**. It does not see your local Postgres.

Alternative (not used today): point ETL directly at RDS or run dbt on clinic EC2. See [Step 3–4 (legacy direct path)](#step-3-legacy--etl-directly-to-rds-optional).

The API queries PostgreSQL using hardcoded dbt schema names:

- `marts.*` — dashboard facts and marts (e.g. `mart_ar_summary`, `fact_appointment`)
- `int.*` — intermediate models (e.g. `int_recall_management`, `int_procedure_complete`)
- `staging.*` — one revenue route joins `staging.stg_opendental__appointmenttype`

**Publish includes all three schemas** so hosted dashboards match local.

**This phase uses the existing single-tenant schema layout** (`raw`, `staging`, `int`, `marts`). **Phase 4** (multi-tenant `mdc_*` / `glic_*` schemas) is a separate follow-on.

---

## Progress (living note)

**As of 2026-06-17**

| Step | Status | Notes |
|------|--------|--------|
| API `/health` + `/health/db` | Done | RDS connected |
| Local ETL + dbt | Done | User workflow; data on localhost |
| RDS `marts` populated | Done | `publish_analytics_to_rds.ps1` — int=45, marts=28, staging=47 |
| API code on EC2 (schema + deps) | Done | `deploy_api_code_clinic.ps1`; uses `marts.*` not `raw_marts.*` |
| Dashboard E2E | Done | `/reports/dashboard/kpis` → 200 on EC2 (2026-06-17) |

---

## Phase 3.5 Checklist (publish path)

### Step 1: Confirm API → RDS connectivity

**Time:** ~5 minutes

- [x] **1.1** `curl.exe https://api-clinic.dbtdentalclinic.com/health`
- [x] **1.2** `curl.exe https://api-clinic.dbtdentalclinic.com/health/db` → 200
- [ ] **1.3** CORS allows `https://clinic.dbtdentalclinic.com` (if browser fails after data is live)

---

### Step 2: Confirm local dbt output is fresh

**Time:** ~5 minutes

On **localhost** (DBeaver or psql):

```sql
SELECT table_schema, COUNT(*) AS tables
FROM information_schema.tables
WHERE table_schema IN ('marts', 'int', 'staging') AND table_type = 'BASE TABLE'
GROUP BY 1 ORDER BY 1;

SELECT COUNT(*) FROM marts.mart_revenue_lost;
```

Expect non-zero marts tables and row counts. If empty, run local ETL + dbt first.

---

### Step 3: Publish local analytics → RDS

**Time:** ~5–30 minutes (depends on dump size)

**Prerequisites:**

- PostgreSQL client tools (`pg_dump`, `pg_restore`) on PATH
- `etl_pipeline/.env_clinic` — local `POSTGRES_ANALYTICS_*`
- `api/.env_api_clinic` — RDS credentials
- Tunnel: `mdc tunnel clinic-db` (keep open in another terminal)

**Run:**

```powershell
mdc publish analytics --env clinic
```

Or:

```powershell
pwsh -File scripts/publish/publish_analytics_to_rds.ps1
```

**What it does:**

1. `pg_dump` schemas `marts`, `int`, `staging` from local `opendental_analytics`
2. `pg_restore` to RDS via tunnel (`127.0.0.1:5433`) with `--clean --if-exists`
3. Verifies table counts and `marts.mart_revenue_lost` row count on RDS

**Dry run (preflight only):**

```powershell
mdc publish analytics --env clinic --dry-run
```

**Nightly (manual today; Airflow later):** After local ETL + dbt succeed, run publish. Do not publish if local dbt failed.

---

### Step 4: Verify API endpoints

```powershell
curl.exe "https://api-clinic.dbtdentalclinic.com/reports/dashboard/kpis" -H "X-API-Key: YOUR_CLINIC_API_KEY"
curl.exe "https://api-clinic.dbtdentalclinic.com/ar/kpi-summary" -H "X-API-Key: YOUR_CLINIC_API_KEY"
```

Expect **200** with JSON (not `UndefinedTable`).

If the API still references `raw_marts.*` or returns import errors, EC2 is running stale code. Deploy current API from repo root:

```powershell
pwsh -File scripts/deployment/deploy_api_code_clinic.ps1 -Clinic
```

Clinic EC2 runs **Python 3.9** — avoid `X | None` type hints in route signatures (use `Optional[X]`).

**Logs:** CloudWatch log group `api-clinic` (console), or on the instance: `/var/log/dental-clinic-api-clinic.log` and `journalctl -u dental-clinic-api-clinic`.

---

### Step 5: Browser E2E

From allowlisted IP: open `https://clinic.dbtdentalclinic.com/dashboard` — KPI cards populate.

---

## Step 3 (legacy) — ETL directly to RDS (optional)

Not the current maintainer workflow. Use only if you stop using local analytics as source of truth.

<details>
<summary>Legacy direct ETL + dbt on EC2 steps</summary>

**Time:** Variable (first full load: hours)

- [ ] **3.1** Confirm ETL env points at clinic OpenDental source and RDS:

  - `ETL_ENVIRONMENT=clinic` or equivalent
  - `POSTGRES_ANALYTICS_*` → RDS `opendental_analytics`
  - `POSTGRES_ANALYTICS_SCHEMA=raw` (single-tenant default)

- [ ] **3.2** Initialize ETL tracking tables if missing:

  ```powershell
  $env:POSTGRES_ANALYTICS_SCHEMA = "raw"
  python etl_pipeline/scripts/initialize_etl_tracking_tables.py
  ```

- [ ] **3.3** Run ETL:

  ```powershell
  mdc etl run --env clinic --profile full
  ```

  Or from maintainer workflow documented in `etl_pipeline/README.md`.

- [ ] **3.4** Verify load on RDS:

  ```sql
  SELECT COUNT(*) FROM raw.patient;
  SELECT COUNT(*) FROM raw.appointment;
  SELECT MAX(updated_at) FROM raw.etl_load_status;  -- if column exists
  ```

**Reference:** `etl_pipeline/`, `docs/ENVIRONMENT_FILES.md`

---

### Step 4: Run dbt on clinic EC2 (`--target clinic`)

**Time:** ~30–90 minutes (full project)

- [ ] **4.1** Ensure dbt works on clinic instance:

  ```powershell
  .\scripts\fix_ec2_dbt_setup.ps1 -Clinic
  ```

- [ ] **4.2** Full project build to RDS:

  ```powershell
  .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic -RefreshProject
  ```

  Equivalent: `dbt run --target clinic` on `/opt/dbt_dental_clinic/dbt_dental_models`.

- [ ] **4.3** (Optional) Run critical tests:

  ```powershell
  .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic test --select mart_ar_summary
  ```

- [ ] **4.4** Verify on RDS:

  ```sql
  SELECT COUNT(*) FROM marts.mart_ar_summary;
  SELECT COUNT(*) FROM marts.fact_appointment;
  ```

**Reference:** `dbt_dental_models/profiles.yml`, `scripts/ec2/run_dbt_on_ec2.ps1`

</details>

---

## Verification checklist

- [ ] `/health` and `/health/db` return 200
- [ ] Local `marts` / `int` / `staging` populated (ETL + dbt on localhost)
- [ ] `mdc publish analytics --env clinic` completes successfully
- [ ] RDS `marts` has dbt models including `mart_revenue_lost`
- [ ] Protected API routes return 200 with valid `X-API-Key`
- [ ] Clinic frontend dashboards display data from allowlisted IP

---

## Troubleshooting

### Frontend: "Network error. Please check your connection."

Usually **no HTTP response** or API **500** (missing tables). After API health is OK, run publish if RDS `marts` is empty. See [CLINIC_DEPLOYMENT_STATUS.md](./CLINIC_DEPLOYMENT_STATUS.md).

### API: 500 with relation does not exist

RDS missing published schemas. Re-run `mdc publish analytics --env clinic` after local dbt succeeds.

### Publish: tunnel not reachable

Start `mdc tunnel clinic-db` in a separate terminal (port 5433).

### Publish: pg_dump not found

Install PostgreSQL client tools or add `C:\Program Files\PostgreSQL\17\bin` to PATH.

### dbt on EC2 (legacy path only)

See [DBT_EC2_TROUBLESHOOTING.md](./DBT_EC2_TROUBLESHOOTING.md).

---

## Relationship to Phase 4

| Phase 3.5 (now) | Phase 4 (later) |
|-----------------|-----------------|
| Single-tenant `raw`, `staging`, `int`, `marts` | Per-clinic `mdc_raw`, `mdc_marts`, `glic_*`, … |
| API unchanged (hardcoded `marts.*`) | API schema routing per tenant |
| MDC data only (or combined in one schema) | MDC + GLIC isolation |
| No cross-clinic patient linking | `shared` patient identity for dual-clinic patients (Phase 4 Step 9) |

**Do not start Phase 4 until Phase 3.5 E2E passes** for at least one clinic on the legacy schema layout.

---

## Summary

| Step | Action | Owner |
|------|--------|--------|
| 1 | Local ETL + dbt (unchanged) | Dev |
| 2 | `mdc publish analytics --env clinic` | Dev |
| 3 | curl API dashboard routes | Dev |
| 4 | Browser E2E from allowlisted IP | Dev/Clinic |

**Outcome:** Clinic dashboards at `clinic.dbtdentalclinic.com` display analytics published from local marts to RDS.
