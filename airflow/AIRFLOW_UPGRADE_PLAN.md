# Airflow Upgrade Plan: 2.7.3 → 2.11.1+

**Status:** Completed on main (2026-06-25)  
**Last updated:** 2026-06-20  
**Driver:** Dependabot alerts #215, #217, #218, #224, #227 on `requirements-airflow-native.txt`  
**Target:** **Apache Airflow 2.11.1** (last 2.x security line; Python 3.11 supported)

Related: [`docs/DEPENDENCY_ALERTS.md`](../docs/DEPENDENCY_ALERTS.md) (full alert matrix), [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md), [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

---

## Why upgrade

| Alert | CVE / issue | Fixed in | Risk in our deployment |
|-------|-------------|----------|------------------------|
| #218 | XCom pickle deserialization (CVE-2023-50943) | 2.8.1+ | Medium — DAGs use JSON XComs, but platform is still vulnerable |
| #224 | LogTemplate code injection (CVE-2024-56373) | 2.11.1 | Low–medium — trusted DAG authors; webserver RCE if abused |
| #215 | DAG author code execution in scheduler | Various 2.8–2.11 fixes | Low — internal DAG authors only |
| #217 | Execution with unnecessary privileges / audit exposure | 2.11.1 | Low — restrict UI to trusted users |
| #227 | example_xcom RCE (CVE-2025-54550) | 3.2.0 docs only | **Very low** — `load_examples=False` in `init-airflow-native.ps1` |

**2.7.3** addressed CVE-2023-47037 but is **below** the 2.8.1 XCom fix and **below** the 2.11.1 LogTemplate / dependency hardening line.

---

## Scope

**In scope**

- Native venv (`.venv-airflow`) used by `init-airflow-native.ps1` / `start-airflow-native.ps1`
- `requirements-airflow-native.txt` constraints and pins
- Airflow metadata DB migration (`airflow db migrate`)
- DAG smoke test on Windows (SequentialExecutor) and validation checklist for clinic nightly

**Out of scope (this pass)**

- Airflow 3.x (larger UI/API break; revisit after 2.11.1 is stable)
- Docker image (`Dockerfile.airflow`) — update separately if sandbox is still used
- EC2 / managed Airflow (deferred per deployment strategy)

---

## Version targets

| Component | Current | Target |
|-----------|---------|--------|
| `apache-airflow` | 2.7.3 | **2.11.1** |
| Constraints file | `constraints-2.7.3/constraints-3.11.txt` | `constraints-2.11.1/constraints-3.11.txt` |
| Python | 3.11.x | 3.11.x (unchanged) |
| `apache-airflow-providers-slack` | `>=8.0` (resolved by constraints) | Pin per 2.11.1 constraints |
| `dbt-core` / `dbt-postgres` (DAG subprocess) | `>=1.6,<2` | Keep range; verify `dbt build` in DAG after upgrade |

Check [Airflow release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html) for 2.8 → 2.11 breaking changes.

---

## DAG compatibility assessment

Our DAGs use stable 2.x APIs:

- `PythonOperator`, `BranchPythonOperator`, `ShortCircuitOperator`, `TaskGroup`
- `Variable`, `TriggerRule`, `AirflowException`, `AirflowSkipException`
- `SlackWebhookHook` from `apache-airflow-providers-slack`
- No deprecated `SubDagOperator`, no FAB-only custom plugins

**Expected:** DAG Python code should load without changes. Validate imports and one full manual `etl_pipeline` run after upgrade.

**Windows-specific:** Keep `SequentialExecutor`, `airflow_win_patch`, and POSIX stubs from `init-airflow-native.ps1` — retest subprocess paths (`mdc`, `dbt`, schema refresh).

---

## Upgrade procedure

### Phase 0 — Prep (no downtime)

1. Finish current Phase A smoke test on **2.7.3** (baseline logs for comparison).
2. Export Airflow metadata backup:
   - SQLite: copy `airflow/airflow.db`
   - Postgres: `pg_dump` of `airflow` database
3. Note current Variables (`project_root`, `etl_environment`, `dbt_target`, etc.) from UI or CLI.
4. Create branch: `airflow-2.11.1-upgrade`.

### Phase 1 — Bump requirements

Edit `requirements-airflow-native.txt`:

```text
-c https://raw.githubusercontent.com/apache/airflow/constraints-2.11.1/constraints-3.11.txt

apache-airflow==2.11.1
```

Update comments referencing `2.7.3`. Mirror version in:

- `scripts/airflow/init-airflow-native.ps1` (step label / docs only)
- `Dockerfile.airflow` (if Docker sandbox stays in use)

### Phase 2 — Rebuild venv

From repo root:

```powershell
# Optional: rename old venv for rollback
Rename-Item .venv-airflow .venv-airflow-2.7.3-backup

.\scripts\airflow\init-airflow-native.ps1
```

`init-airflow-native.ps1` already sets:

- `AIRFLOW__CORE__LOAD_EXAMPLES=False`
- `AIRFLOW__CORE__EXECUTOR=SequentialExecutor`
- `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago`

After install:

```powershell
.\.venv-airflow\Scripts\activate
airflow version
airflow db migrate
airflow dags list
```

### Phase 3 — Config review

Compare generated defaults with prior `airflow.cfg` / env overrides:

| Setting | Recommendation |
|---------|----------------|
| `core.load_examples` | `False` (already env-forced) |
| `core.use_historical_filename_templates` | Leave **default False** (2.11.1 security default; fixes #224) |
| `core.enable_xcom_pickling` | `False` (default; 2.8.1+ enforces safely) |
| `database.sql_alchemy_conn` | Same metadata URL as before |
| `webserver` / `api` | Note any new auth flags in 2.9+ |

### Phase 4 — Validation checklist

| # | Test | Pass criteria |
|---|------|----------------|
| 1 | `airflow dags list` | `etl_pipeline`, `schema_analysis` present, no import errors |
| 2 | `airflow dags show etl_pipeline` | Renders task graph |
| 3 | Manual trigger `etl_pipeline` (outside business hours, `etl_environment=test`) | Through `validation.validate_configuration` at minimum |
| 4 | Full manual run (optional) | ETL + dbt + publish on test env |
| 5 | Slack notification task | Message received or graceful skip if conn missing |
| 6 | Scheduler + webserver | Both start via `start-airflow-native.ps1` |
| 7 | Rollback drill | Restore `.venv-airflow-2.7.3-backup` + metadata backup |

### Phase 5 — Production cutover

1. Run one clinic (`etl_environment=clinic`) manual trigger outside business hours.
2. Monitor 2–3 scheduled nights before treating upgrade as complete.
3. Dismiss Dependabot Airflow alerts #215, #217, #218, #224 with link to this doc.
4. Dismiss #227 as “example DAG only; load_examples=False”.

---

## Rollback

1. Stop scheduler and webserver.
2. Restore venv: `Rename-Item .venv-airflow-2.7.3-backup .venv-airflow`
3. Restore metadata DB from Phase 0 backup (if migrate was run).
4. Restart with `start-airflow-native.ps1`.

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Constraint conflicts with `dbt-core` in same venv | DAG runs dbt via subprocess; if pip conflicts arise, pin dbt in a separate venv or loosen DAG venv to Airflow-only packages |
| Metadata migration failure | Backup before `airflow db migrate`; test migrate on copy first |
| Provider API changes (Slack) | Run notification task in Phase 4 |
| Windows subprocess regressions | Retest `mdc_runner`, schema refresh, `dbt deps` / `dbt build` paths |
| Long upgrade delays security exposure | Prioritize Phase 1–3 in a single maintenance window after Phase A baseline |

---

## Future: Airflow 3.x

Defer until 2.11.1 is stable in clinic nightly. Airflow 3 removes/changes FAB UI, DAG authoring patterns, and example DAG packaging. Re-evaluate when:

- 2.11.x approaches EOL, or
- A CVE requires 3.x (e.g. full example_xcom doc fix is 3.2.0-only — not blocking for us).

---

## File checklist

| File | Action on upgrade |
|------|-------------------|
| `requirements-airflow-native.txt` | Pin 2.11.1 + constraints URL |
| `requirements-airflow.txt` | Update comment if Docker base image version changes |
| `scripts/airflow/init-airflow-native.ps1` | Version strings in messages |
| `scripts/airflow/start-airflow-native.ps1` | Retest after upgrade |
| `airflow/ORCHESTRATION_ROADMAP.md` | Mark security upgrade complete |
| `docs/DEPENDENCY_ALERTS.md` | Update Airflow row status |
