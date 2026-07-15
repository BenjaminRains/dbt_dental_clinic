# Airflow Orchestration Roadmap

**Status:** Phase A nearly complete — UI live, both DAGs loaded (paused); smoke test + Phase B next  
**Last updated:** 2026-06-19  
**Goal:** Move beyond manual ETL pulls (`etl-run` / `mdc etl run`) and manual dbt runs (`run_dbt_on_ec2.ps1`) to scheduled, unattended nightly orchestration.

**Source discussion:** [Airflow wiring review](30475cd5-d66b-42f7-84a3-3ae3466c2e9b) (2026-06-17)

---

## Executive summary

The **DAG code is largely complete**. What remains is **deployment, environment wiring, end-to-end validation, and production cutover** — not greenfield DAG authoring.

| Component | Status | Notes |
|-----------|--------|-------|
| `schema_analysis` DAG | ✅ Loaded (paused) | **Optional** — change reports + notifications; not on the nightly path |
| `etl_pipeline` DAG | ✅ Loaded (paused) | Nightly 9 PM Central; **schema refresh first**, then validation + incremental ETL |
| dbt orchestration | ✅ **Integrated in `etl_pipeline` DAG** | `should_run_dbt` → `dbt_deps` → `dbt_build` task group (not a separate DAG) |
| Native Windows bootstrap | ✅ Done | `init-airflow-native.ps1`, `start-airflow-native.ps1`, POSIX stubs; two-terminal start |
| Docker sandbox | ✅ Scaffolded | Optional — `Dockerfile.airflow`, `docker-compose.yml`; **not** used for clinic nightly |
| Production deployment | 🔄 Option A (dev laptop) | **Native Airflow** (Python venv) + WireGuard VPN; no EC2 orchestrator |
| End-to-end validation | ⏳ Next | Unpause `etl_pipeline`; manual trigger outside business hours (6 AM–8:59 PM Central blocked) |

---

## Documentation index

Planning lives primarily under `airflow/`. There is **no separate Airflow plan under `docs/deployment/`** (clinic infra covers frontend/API/RDS only).

| Document | Purpose |
|----------|---------|
| **This file** | Roadmap, gaps, phased plan, open decisions |
| [`README.md`](README.md) | DAG overview, variables, local setup |
| [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md) | Native Option A (clinic) vs optional Docker sandbox |
| [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) | What one run means: guard → schema refresh → ETL → dbt → publish |
| [`DAGS_STATUS.md`](DAGS_STATUS.md) | Quick status reference |
| [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md) | **Future** dbt enhancements (selectors, Cosmos) — initial dbt is already in `etl_pipeline` |
| [`AIRFLOW_UPGRADE_PLAN.md`](AIRFLOW_UPGRADE_PLAN.md) | Upgrade 2.11.1 → **3.1.7** (Task SDK / api-server) |
| [`docs/deployment/ENVIRONMENT_FILES.md`](../docs/deployment/ENVIRONMENT_FILES.md) §4.4 | Native Airflow vs optional Compose sandbox |

---

## Architecture (current code)

```
etl_pipeline (nightly 9 PM Central)
       │
       ├── guard_business_hours
       ├── refresh_schema_configuration   ← analyze_opendental_schema.py (every night, before ETL)
       ├── validation (etl_environment)
       ├── ETL (large → medium → small → tiny)
       ├── verify / report
       └── on ETL success:
               ├── dbt_build (dbt_target via mdc)
               ├── publish_analytics (publish_environment via mdc)
               └── notify

schema_analysis DAG (optional, manual trigger)
       └── Same analyzer + change reports / Slack — not required for nightly runs
```

dbt is **not** a third DAG. It is a task group inside `airflow/dags/etl_pipeline_dag.py` (see `should_run_dbt`, `dbt_build` group).

---

## Manual workflow → Airflow mapping

| Today (manual) | After Airflow | Notes |
|----------------|---------------|-------|
| `etl-run` / `mdc etl run --env clinic` | `etl_pipeline` DAG @ 9 PM Central | Keep manual scripts as **break-glass** for single-table reruns |
| `run_dbt_on_ec2.ps1 -Clinic RefreshProject` | `dbt_build` task group (`dbt_target=local` via mdc) | Build on laptop Postgres |
| `mdc publish analytics --env clinic` | `publish_analytics` task (`publish_environment=clinic`) | Requires SSM tunnel on :5433 |
| `analyze_opendental_schema.py` / fresh `tables.yml` | `refresh_schema_configuration` in `etl_pipeline` (first step after guard) | Runs **every nightly** before ETL |
| Ad-hoc schema analysis + change report | `schema_analysis` DAG (manual trigger) | Optional — e.g. before OpenDental upgrades |

---

## Environment contract (Option A) — **native Airflow, same files as `mdc`**

**Decided:** 2026-06-19 — No duplicate env files, no Compose-injected ETL overrides.

### Why not Docker for clinic runs?

Inside a container, `localhost` in `etl_pipeline/.env_clinic` points at the **container**, not your laptop. Fixing that requires either duplicate files (`host.docker.internal`) or Compose overrides — both rejected.

**Simplest path:** run Airflow **on the host** (Python venv), same as `mdc etl run` / `mdc dbt run`. One stage file per environment; no root `.env` ETL contract.

### What loads what

| Concern | Source | Notes |
|---------|--------|-------|
| **ETL** | `etl_pipeline/.env_<stage>` | DAG sets `ETL_ENVIRONMENT` from Airflow Variable → same file `mdc` uses |
| **dbt** | `mdc dbt invoke --env {dbt_target}` in DAG | `dbt_target=local` → same as `mdc dbt run --env local` |
| **Publish** | `mdc publish analytics --env {publish_environment}` in DAG | `publish_environment=clinic` — no manual publish step |
| **Airflow metadata** | Local Postgres DB (e.g. database `airflow` on your existing instance) | Not the analytics warehouse |
| **Airflow Variables** | `etl_environment`, `dbt_target`, `publish_environment`, `project_root` | See table below |
| **Root `/.env`** | **Not used** for Option A clinic orchestration | Only needed if you use optional Docker sandbox |
| **SSM tunnel** | Auto-started by `mdc publish analytics --ensure-tunnel` (DAG default) | Manual `mdc tunnel clinic-db` still works if port already open |

### Airflow Variables (Option A clinic nightly)

| Variable | Value |
|----------|-------|
| `project_root` | Repo root, e.g. `C:\Users\rains\dbt_dental_clinic` |
| `etl_environment` | `clinic` |
| `dbt_target` | `local` |
| `publish_environment` | `clinic` |
| `publish_tunnel_port` | `5433` (optional) |
| `publish_ensure_tunnel` | `true` (default — auto-start SSM tunnel for publish) |
| `slack_webhook_url` | optional |

Leave `publish_environment` **unset** for Phase A smoke tests (skips publish). Do **not** set `dbt_target=clinic` until dbt can build directly against RDS safely.

### SSM tunnel (publish prerequisite)

**Default (Airflow + unattended publish):** `publish_ensure_tunnel=true` (default) runs `mdc publish analytics --ensure-tunnel`, which starts a **background** SSM session when `127.0.0.1:5433` is closed and stops it after publish completes.

**Manual (interactive):** You can still run `mdc tunnel clinic-db` in a dedicated terminal before the nightly run. If the port is already open, publish reuses it and does not stop your session.

`mdc publish analytics` (inside the DAG) connects to RDS via **`127.0.0.1:5433`**.

**Optional manual preflight:**

```powershell
mdc tunnel clinic-db
Test-NetConnection 127.0.0.1 -Port 5433
```

The tunnel uses **AWS SSM** through the clinic API EC2 instance into the private RDS VPC — separate from **WireGuard** (which is only needed for OpenDental ETL). You need both on nightly runs: **VPN for ETL**, **SSM tunnel for publish** (auto or manual).

**If publish fails:** check Airflow task logs for `Failed to start clinic RDS tunnel`; verify AWS credentials, Session Manager plugin, and `deployment_credentials.json` instance IDs. Fallback: start `mdc tunnel clinic-db` manually and set Airflow Variable `publish_ensure_tunnel=false`.

### Pre-flight (same as manual)

```powershell
mdc etl validate --env clinic --profile full   # VPN on
mdc dbt validate --env local
```

If those pass, Airflow tasks using the same files will see the same connections.

### Optional: Docker Compose sandbox

`docker-compose.yml` can still spin up Airflow + Compose postgres/mysql for **isolated experiments**. It is **not** the Option A clinic path — do not rely on it for nightly clinic runs. ETL/dbt connection vars are **not** injected into Airflow containers (avoids overriding mounted stage files).

---

## Orchestration host — **Decision: Option A (dev machine over VPN)**

**Decided:** 2026-06-19 — Run Airflow **natively on the developer laptop** (Python venv), with **WireGuard VPN** to reach clinic OpenDental (`192.168.2.x`). Same env files as `mdc`; **not** Docker Compose for clinic nightly runs.

The nightly run must execute on a host that can reach **client OpenDental MySQL** without running during business hours (6 AM–8:59 PM Central — enforced by `guard_business_hours`).

### Why Option A

| Factor | Detail |
|--------|--------|
| Network | OpenDental is on clinic LAN; laptop + VPN already works for manual ETL |
| Cost | No new EC2 or site-to-site VPN (~$20–45+/mo avoided) |
| Schema refresh | ~**7 min** (446 tables; `schema_analysis` logs, Mar–Jun 2026) |
| Incremental ETL | ~**25 min** when runs are every couple of days (observed) |
| Full nightly window | 9 PM → ~**7 min schema** + ~**25 min ETL** + dbt (see open decision #3) + publish → ~**32 min** before dbt; full `dbt build` adds ~52 min |

### Operational requirements (Option A)

- [ ] **Machine on at 9 PM Central** — disable sleep during the run window, or use “Never sleep when plugged in”
- [ ] **WireGuard connected** before scheduler fires (manual, or OS startup task / scheduled connect at 8:55 PM)
- [ ] **SSM tunnel** — auto via `publish_ensure_tunnel=true` (default); optional manual `mdc tunnel clinic-db`
- [ ] **Native Airflow** — venv with `requirements-airflow-native.txt`; scheduler + webserver via `start-airflow-native.ps1` (Windows) or host processes (not Docker)
- [ ] **`etl_pipeline/.env_clinic`** — unchanged; same file as `mdc etl run --env clinic`
- [ ] **Airflow Variable `project_root`** — repo root on disk (not `/opt/airflow/...`)
- [ ] **dbt + RDS** — DAG runs `mdc publish analytics` when `publish_environment=clinic` (tunnel required)

### Deferred options (revisit if laptop becomes unreliable)

| Option | When to reconsider |
|--------|-------------------|
| **B. Dedicated EC2** | Need 24/7 unattended runs without laptop; willing to add site-to-site VPN |
| **C. Clinic API EC2** | Not recommended — resource contention on `t3.small` |
| **D. On-prem MDC server** | Clinic IT prefers orchestration on LAN; no AWS VPN |

---

## Phased implementation plan

### Phase A — Local proof (1–2 days)

- [x] Create venv; `pip install -r requirements-airflow-native.txt` (Airflow **3.1.7** pinned)
- [x] `airflow db init` / `db migrate` (metadata DB — SQLite `airflow/airflow.db` on dev laptop)
- [x] Set Airflow Variables: `etl_environment=test`, `dbt_target=local`, `project_root=<repo root>`
- [x] Windows native bootstrap: POSIX stubs, `run_airflow.py`, scheduler + dag-processor + api-server
- [x] Confirm DAGs load in UI — `etl_pipeline` + `schema_analysis` visible at http://localhost:8080
- [ ] **Unpause `etl_pipeline`** in UI (leave `schema_analysis` paused unless testing change reports)
- [ ] Confirm `etl_pipeline/.env_test` exists; `mdc etl validate --env test --profile full`
- [ ] Trigger `etl_pipeline` manually **outside** business hours (after 9 PM Central)
- [ ] Confirm: validation → ETL → Layer 0 → `dbt_build` completes
- [ ] `pytest airflow/tests/ -v`

**Start commands (Windows, from repo root):**

```powershell
# Terminal 1 — scheduler
mdc airflow start --scheduler

# Terminal 2 — dag-processor (required in Airflow 3)
mdc airflow start --dag-processor

# Terminal 3 — api-server (UI; --webserver is an alias)
mdc airflow start --api-server
```

Both DAGs load **paused** by default — toggle `etl_pipeline` on before testing. Do not use `airflow standalone` on Windows; use the three-terminal pattern above.

### Phase B — Clinic on dev laptop (Option A) (2–3 days)

- [x] **Decide orchestration host** → Option A (dev machine over VPN)
- [x] **Env model** → native Airflow; single `etl_pipeline/.env_clinic` (no duplicates, no Compose overrides)
- [ ] WireGuard auto-connect or pre-run checklist (VPN up before 9 PM)
- [ ] Set Airflow Variables: `etl_environment=clinic`, `dbt_target=local`, `project_root=<repo root>`
- [ ] `mdc etl validate --env clinic --profile full` with VPN on
- [ ] First manual DAG trigger outside business hours; confirm schema refresh (~7 min) → ETL (~25 min) → dbt → publish → notify
- [ ] SSM tunnel running before publish task (`mdc tunnel clinic-db` → port 5433 open)

### Phase C — Production cutover on laptop (1 day)

- [ ] Enable nightly schedule (already `0 21 * * *` with `America/Chicago`)
- [ ] Configure Slack (`slack_webhook_url` Variable) and/or SMTP alerts on failure
- [ ] Airflow UI (localhost:8080) — no public exposure; laptop-only
- [ ] Monitor 2–3 consecutive nightly runs (VPN + **tunnel terminal open** + native Airflow up)
- [ ] Document break-glass: manual `mdc etl run`, `mdc dbt run`, `mdc publish analytics`
- [ ] (Later) Long-lived tunnel Scheduled Task for debugging only — publish auto-starts tunnel by default

### Phase D — Hardening (later)

- [ ] **Wire dbt source freshness** — run `dbt source freshness` before `dbt build` in the `dbt_build` task group; YAML thresholds already exist on raw sources (`_loaded_at`). Closes the late-data gap that `verify-loads` does not enforce. See [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md) § Wire dbt source freshness.
- [ ] Layered dbt selectors (see [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md))
- [ ] Astronomer Cosmos for model-level tasks and retries
- [ ] Split `mart_patient_retention` to separate schedule (~52 min of full `dbt build`; see TODO: Optimize mart_patient_retention)
- [ ] Re-evaluate EC2/on-prem orchestrator if laptop schedule proves unreliable
- [ ] EC2 deploy script + `deployment_credentials` entry for Airflow (only if moving off Option A)

---

## Open decisions

Record answers here as they are made.

| # | Question | Options | Decision |
|---|----------|---------|----------|
| 1 | **Where should the 9 PM clinic run execute?** | Dedicated EC2 · Dev machine over VPN · Existing API EC2 | **Option A — dev laptop + VPN** (2026-06-19) |
| 2 | **First validation target?** | Native venv + `etl_environment=test` · Go straight to clinic | **Phase A: native test env · Phase B: clinic via `.env_clinic`** |
| 3 | **dbt scope nightly?** | Full `dbt build` (~52 min; dominated by `mart_patient_retention`) · Split slow marts to weekly DAG/selector | _TBD_ — schema ~7 min + ETL ~25 min observed; dbt dominates total if full build |

---

## Remaining code/config cleanup

These are small but prevent confusion:

- [ ] Update placeholder email in DAG `default_args` (`data-team@example.com`)
- [ ] Confirm `project_root` Variable works on EC2 without code changes (already supported)
- [x] Windows native start scripts (`init-airflow-native.ps1`, `start-airflow-native.ps1`, POSIX stubs)
- [x] Align `scripts/airflow/init-airflow.sh` with root `.env` (was `config/.env`)
- [x] Add Option A orchestration note to clinic infra / deployment docs (laptop + VPN, native Airflow)

---

## Related TODO items

- **Fix EC2 dbt Database Connection Credentials** — blocks dbt on EC2/RDS from Airflow or manual runs
- **Optimize mart_patient_retention Performance** — affects nightly dbt duration if using full `dbt build`
- **Client deployment Phase 3** — schema-based multi-tenancy may affect ETL routing later

---

## Quick reference: Airflow Variables

| Variable | Phase A (test) | Option A clinic (native) |
|----------|----------------|---------------------------|
| `project_root` | `<repo root>` | same |
| `etl_environment` | `test` | `clinic` |
| `dbt_target` | `local` | `local` |
| `publish_environment` | *(unset)* | `clinic` |
| `slack_webhook_url` | optional | recommended |

See [`README.md`](README.md) and [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) for schedule and run semantics.
