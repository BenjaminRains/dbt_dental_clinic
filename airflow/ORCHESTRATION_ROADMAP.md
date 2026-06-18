# Airflow Orchestration Roadmap

**Status:** Planning — deployment and validation in progress  
**Last updated:** 2026-06-17  
**Goal:** Move beyond manual ETL pulls (`etl-run` / `mdc etl run`) and manual dbt runs (`run_dbt_on_ec2.ps1`) to scheduled, unattended nightly orchestration.

**Source discussion:** [Airflow wiring review](30475cd5-d66b-42f7-84a3-3ae3466c2e9b) (2026-06-17)

---

## Executive summary

The **DAG code is largely complete**. What remains is **deployment, environment wiring, end-to-end validation, and production cutover** — not greenfield DAG authoring.

| Component | Status | Notes |
|-----------|--------|-------|
| `schema_analysis` DAG | ✅ Implemented | Weekly schema → `tables.yml` |
| `etl_pipeline` DAG | ✅ Implemented | Nightly 9 PM Central, business-hours guard, incremental ETL |
| dbt orchestration | ✅ **Integrated in `etl_pipeline` DAG** | `should_run_dbt` → `dbt_deps` → `dbt_build` task group (not a separate DAG) |
| Docker runtime | ✅ Scaffolded | `Dockerfile.airflow`, `docker-compose.yml`, `requirements-airflow.txt` |
| Production deployment | ❌ Not wired | No EC2 deploy script; not in `deployment_credentials`; not in `docs/deployment/` |
| End-to-end validation | ❓ Unknown | Manual workflows still in daily use |

---

## Documentation index

Planning lives primarily under `airflow/`. There is **no separate Airflow plan under `docs/deployment/`** (clinic infra covers frontend/API/RDS only).

| Document | Purpose |
|----------|---------|
| **This file** | Roadmap, gaps, phased plan, open decisions |
| [`README.md`](README.md) | DAG overview, variables, local setup |
| [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md) | Docker Compose locally → same image on EC2 → MWAA later |
| [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) | What one run means: guard → ETL → report → dbt on success |
| [`DAGS_STATUS.md`](DAGS_STATUS.md) | Quick status reference |
| [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md) | **Future** dbt enhancements (selectors, Cosmos) — initial dbt is already in `etl_pipeline` |
| [`docs/ENVIRONMENT_FILES.md`](../docs/ENVIRONMENT_FILES.md) §4.4 | Root `.env` → Docker/Airflow env substitution |

---

## Architecture (current code)

```
schema_analysis (weekly, on-demand)
       │
       └──► tables.yml ──► etl_pipeline (nightly 9 PM Central)
                                    │
                                    ├── validation
                                    ├── ETL (large → medium → small → tiny)
                                    ├── verify / report / notify
                                    └── dbt_build (only if pipeline_success)
                                            ├── dbt_deps
                                            └── dbt_build --target {dbt_target}
```

dbt is **not** a third DAG. It is a task group inside `airflow/dags/etl_pipeline_dag.py` (see `should_run_dbt`, `dbt_build` group).

---

## Manual workflow → Airflow mapping

| Today (manual) | After Airflow | Notes |
|----------------|---------------|-------|
| `etl-run` / `mdc etl run --env clinic` | `etl_pipeline` DAG @ 9 PM Central | Keep manual scripts as **break-glass** for single-table reruns |
| `run_dbt_on_ec2.ps1 -Clinic RefreshProject` | `dbt_build` task group after ETL success | Requires EC2 dbt/RDS credentials (see TODO: Fix EC2 dbt Database Connection Credentials) |
| Ad-hoc schema analysis | `schema_analysis` DAG (weekly) or manual trigger | Before OpenDental upgrades |

---

## Environment gaps (biggest blocker)

The ETL pipeline loads **`etl_pipeline/.env_<stage>`** via `FileConfigProvider` / `settings_v2.py`. OS environment variables **override** file values when set (see `settings_v2` precedence).

Docker Compose today:

| Gap | Detail |
|-----|--------|
| `.env_clinic` / `.env_test` | Not mounted or generated inside Airflow containers |
| Partial root `.env` | Injects some `OPENDENTAL_*` / `POSTGRES_*` but not the full ETL contract |
| `MYSQL_REPLICATION_*` | Not set on Airflow containers (required for 3-stage ETL: source → replication → analytics) |
| `ETL_ENVIRONMENT` | Not set on Airflow containers (DAG uses Airflow Variable `etl_environment` for orchestration; ETL settings still need stage file or matching env vars) |
| dbt RDS credentials | `POSTGRES_ANALYTICS_*` + Airflow Variable `dbt_target=clinic` — tied to EC2 dbt credential deploy |

**Resolution options (pick one per environment):**

1. **Mount stage env file** — deploy `etl_pipeline/.env_clinic` (or `.env_test`) on the host; file is visible inside the mounted `etl_pipeline/` volume.
2. **Inject full env var set** — set all `OPENDENTAL_*`, `MYSQL_REPLICATION_*`, `POSTGRES_ANALYTICS_*` in `docker-compose.yml` or container env (OS wins over file).

For dbt, ensure `POSTGRES_ANALYTICS_*` reach the container and set Airflow Variable `dbt_target` (`local` for Compose dev, `clinic` for RDS).

---

## Orchestration host (open decision)

The nightly run must execute on a host that can reach **client OpenDental MySQL** (VPN/tunnel) without running during business hours (6 AM–8:59 PM Central — enforced by `guard_business_hours`).

| Option | Pros | Cons |
|--------|------|------|
| **A. Dev machine over VPN** | Fastest to validate | Not production-grade; machine must be on at 9 PM |
| **B. Dedicated EC2 orchestrator** | Recommended for unattended runs | New infra + deploy work |
| **C. Clinic API EC2 (`i-0b7013339cf648e0f`)** | Reuses existing instance | Mixes API + heavy ETL on `t3.small`; resource contention |

`docs/deployment/CLINIC_INFRASTRUCTURE_PLAN.md` does **not** yet assign an orchestration host.

---

## Phased implementation plan

### Phase A — Local proof (1–2 days)

- [ ] Copy `.env.template` → `.env`; copy `etl_pipeline/.env_test.template` → `etl_pipeline/.env_test` (or `.env_clinic` for clinic-shaped test)
- [ ] `docker-compose build airflow-webserver airflow-scheduler`
- [ ] `docker-compose --profile init run --rm airflow-init`
- [ ] `docker-compose up -d postgres mysql airflow-webserver airflow-scheduler`
- [ ] Set Airflow Variables: `etl_environment=test`, `dbt_target=local`, `project_root=/opt/airflow/dbt_dental_clinic`
- [ ] Enable `etl_pipeline`; trigger manually **outside** business hours (or temporarily adjust guard for testing)
- [ ] Confirm: validation → ETL → `dbt_build` completes
- [ ] `pytest airflow/tests/ -v`
- [ ] Fix any env/mount gaps discovered in `docker-compose.yml`

### Phase B — Clinic orchestrator (2–3 days)

- [ ] **Decide orchestration host** (see open decisions below)
- [ ] Deploy same Docker image + Compose (or equivalent) on chosen host
- [ ] Wire `etl_pipeline/.env_clinic` + RDS as `POSTGRES_ANALYTICS_*`
- [ ] Deploy dbt credentials (`.env_ec2` / `setup_ec2_dbt_env.sh` — see TODO: Fix EC2 dbt Database Connection Credentials)
- [ ] Set Variables: `etl_environment=clinic`, `dbt_target=clinic`, `project_root=/opt/dbt_dental_clinic` (or mounted path)
- [ ] First manual trigger against clinic; verify raw load + dbt marts

### Phase C — Production cutover (1 day)

- [ ] Enable nightly schedule (already `0 21 * * *` with `America/Chicago`)
- [ ] Configure Slack (`slack_webhook_url` Variable) and/or SMTP alerts
- [ ] Restrict Airflow UI (port 8080) — VPN / security group only
- [ ] Monitor 2–3 consecutive nightly runs
- [ ] Document break-glass procedures for manual ETL/dbt reruns

### Phase D — Hardening (later)

- [ ] Layered dbt selectors (see [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md))
- [ ] Astronomer Cosmos for model-level tasks and retries
- [ ] Split `mart_patient_retention` to separate schedule (~52 min of full `dbt build`; see TODO: Optimize mart_patient_retention)
- [ ] EC2 deploy script + `deployment_credentials` entry for Airflow
- [ ] Cross-link from `docs/deployment/` when clinic orchestration host is chosen

---

## Open decisions

Record answers here as they are made.

| # | Question | Options | Decision |
|---|----------|---------|----------|
| 1 | **Where should the 9 PM clinic run execute?** | Dedicated EC2 · Dev machine over VPN · Existing API EC2 | _TBD_ |
| 2 | **First validation target?** | Local/test env in Compose · Go straight to clinic RDS | _TBD_ |
| 3 | **dbt scope nightly?** | Full `dbt build` (~52 min; dominated by `mart_patient_retention`) · Split slow marts to weekly DAG/selector | _TBD_ |

---

## Remaining code/config cleanup

These are small but prevent confusion:

- [ ] Update placeholder email in DAG `default_args` (`data-team@example.com`)
- [ ] Confirm `project_root` Variable works on EC2 without code changes (already supported)
- [ ] Align `scripts/utils/init-airflow.sh` with root `.env` (script references `config/.env`)
- [ ] Add orchestration host to clinic infra plan once decided

---

## Related TODO items

- **Fix EC2 dbt Database Connection Credentials** — blocks dbt on EC2/RDS from Airflow or manual runs
- **Optimize mart_patient_retention Performance** — affects nightly dbt duration if using full `dbt build`
- **Client deployment Phase 3** — schema-based multi-tenancy may affect ETL routing later

---

## Quick reference: Airflow Variables

| Variable | Local dev | Clinic production |
|----------|-----------|-------------------|
| `project_root` | `/opt/airflow/dbt_dental_clinic` | `/opt/dbt_dental_clinic` (or host path) |
| `etl_environment` | `test` | `clinic` |
| `dbt_target` | `local` | `clinic` |
| `slack_webhook_url` | optional | recommended |

See [`README.md`](README.md) and [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) for schedule and run semantics.
