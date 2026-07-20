# Environment handling review — phase history (ARCHIVED)

**Status:** Complete — archived 2026-07-20.  
**Day-to-day reference:** [../ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md).  
**Daily commands:** [../../../tools/mdc_cli/README.md](../../../tools/mdc_cli/README.md).

Historical tracker for the environment modernization effort (Phases 0–6 all **Done**). Do not extend with new work; update `ENVIRONMENT_FILES.md` instead.

The old root-level `ENVIRONMENT_HANDLING_REVIEW.md` and `ENVIRONMENT_HANDLING_REVIEW_PHASE5_PROPOSAL.md` were never committed to this repo; this file replaces them.

---

## Summary

| Phase | Focus | Status |
|-------|--------|--------|
| **0** | Precedence: OS env wins; API on EC2 uses systemd `api/.env` only | Done |
| **1** | Component-scoped env files; track `docs/deployment/ENVIRONMENT_FILES.md` | Done |
| **2–4** | Typed settings (`api/settings.py`, ETL `settings_v2.py`); `mdc` CLI with isolated child env | Done |
| **5** | Retire shell `*-init` / `environment_manager.ps1` | Done (archived under `scripts/archive/`) |
| **6** | Credential dedup — one authority per connection role | Done |

Current **`mdc` version:** see `tools/mdc_cli/pyproject.toml` (0.9.x at time of writing).

---

## Phase 0 — Precedence and production loading

- **API:** `api/settings.py` skips on-disk `api/.env_api_*` when analytics host + creds are already in OS env (systemd on EC2).
- **Deploy:** `mdc deploy api --env clinic` writes `/opt/dbt_dental_clinic/api/.env` from `api/.env_api_clinic`; Python does not re-read the source file on the instance.
- **ETL / ad-hoc scripts:** `override=False` when loading dotenv so process env wins.

See [ENVIRONMENT_FILES.md §3.2](../ENVIRONMENT_FILES.md#32-precedence-rule-target-state) and [api/settings.py](../../../api/settings.py).

---

## Phases 1–4 — Typed loaders and `mdc`

- **Single vocabulary:** `local`, `demo`, `clinic`, `test` — components declare support; no root `/.env_local` / `/.env_clinic`.
- **Loaders:** API → `api/.env_api_<stage>`; ETL → `etl_pipeline/.env_<stage>`; dbt → `mdc_cli/dbt_env.py` + `dbt_dental_models/.env_*` / `deployment_credentials.json`.
- **CLI:** `pip install -e tools/mdc_cli`; `mdc status`, `mdc * validate`, `mdc * run` inject validated env into child processes without mutating the parent shell.
- **Clinic RDS password:** RDS master user secret (`rds!db-...` in Secrets Manager) for publish/freshness; `mdc secrets pull clinic` syncs the live password into `api/.env_api_clinic` for EC2 deploy.

See [ENVIRONMENT_FILES.md §4](../ENVIRONMENT_FILES.md#4-who-loads-what-by-component) and [CLINIC_ANALYTICS_WORKFLOW.md](../CLINIC_ANALYTICS_WORKFLOW.md).

---

## Phase 5 — Retire shell env manager

- **`scripts/environment_manager.ps1`** moved to **`scripts/archive/environment_manager.ps1`** (Phase 5.5).
- **`load_project.ps1`** loads `mdc` aliases only; `-Legacy` can still dot-source the archive with a deprecation warning.
- No new work should depend on `dbt-init`, `etl-init`, or `api-init` patterns.

---

## Phase 6 — Credential dedup (done)

**Goal:** One source of truth per connection role; stop duplicating `POSTGRES_ANALYTICS_*` across ETL, dbt, and API clinic files.

| Role | Authority |
|------|-----------|
| Local warehouse Postgres | `dbt_dental_models/.env_local` |
| Clinic RDS metadata | `deployment_credentials.json` → `clinic_database.postgresql` |
| Clinic RDS password (live) | RDS master user secret (`rds!db-...`) via `overlay_clinic_rds_credentials` |
| Clinic RDS deploy file | `api/.env_api_clinic` → EC2 `api/.env` via `mdc deploy api --env clinic` |
| OpenDental source + replication | `etl_pipeline/.env_clinic` only (no `POSTGRES_ANALYTICS_*`) |
| Retired | `dbt_dental_models/.env_clinic` (deprecated fallback) |

**Implementation:**

- `mdc_cli/postgres_env.py` — shared clinic/local warehouse Postgres resolution
- `mdc_cli/etl_env.py` — `compose_etl_env_dict()` overlays analytics authority for `mdc etl …`
- `mdc etl … --tunnel-db` — clinic RDS via SSM tunnel (same as dbt/api)
- `mdc etl exec` — run ad-hoc scripts (e.g. `initialize_etl_tracking_tables.py`) with composed env
- `mdc status` / `mdc secrets pull clinic` — warn on deprecated `POSTGRES_ANALYTICS_*` in `etl_pipeline/.env_clinic`
- ETL `connections.py` — honors `POSTGRES_ANALYTICS_SSLMODE` / `PGSSLMODE`
- `mdc publish analytics` — local source defaults to `dbt_dental_models/.env_local`

See [ENVIRONMENT_FILES.md §3.6](../ENVIRONMENT_FILES.md#36-phase-6--postgres-authority-matrix).

---

## Related docs

- [ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md) — operator reference (inventory, loaders, templates)
- [CLINIC_ANALYTICS_WORKFLOW.md](../CLINIC_ANALYTICS_WORKFLOW.md) — local dbt → publish to RDS
- [api/API_ENV_FILE_EXPLANATION.md](../../api/API_ENV_FILE_EXPLANATION.md) — API file loading
- [scripts/archive/environment_manager.ps1](../../../scripts/archive/environment_manager.ps1) — legacy (reference only)
