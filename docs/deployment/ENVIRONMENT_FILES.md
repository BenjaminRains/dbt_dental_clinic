# Environment Files — Project-Wide Reference

This document is the **single source of truth** for how `.env` and related environment files are managed across the dbt_dental_clinic codebase. Use it to onboard, audit, or refactor environment configuration.

**Daily commands:** [`tools/mdc_cli/README.md`](../../tools/mdc_cli/README.md).  
**Phase history (archived):** [archive/ENVIRONMENT_HANDLING_REVIEW.md](archive/ENVIRONMENT_HANDLING_REVIEW.md) — Phases 0–6 complete.

---

## Developer workflow (`mdc`)

Configuration is loaded by **typed Python settings**; **`mdc`** orchestrates runs with **isolated child-process env** (no shell `*-init`, no dot-sourcing `environment_manager.ps1`).

```powershell
pip install -e tools/mdc_cli    # once per machine
.\load_project.ps1              # optional aliases (status, api-run, etl-validate, …)
mdc status                      # paths, validation, venv discovery
mdc api run --env local
mdc etl validate --env local --profile load
mdc dbt run --env local
mdc frontend dev
mdc tunnel clinic-db
mdc deploy api --env clinic
```

| Component | Config authority | `mdc` examples |
|-----------|----------------|----------------|
| API | `api/settings.py` | `mdc api test-config --env local`, `mdc api run --env clinic --tunnel-db` |
| ETL | `etl_pipeline/.../settings_v2.py` | `mdc etl validate --env clinic --profile full`, `mdc etl run --env clinic` |
| dbt | `mdc_cli/dbt_env.py` + `profiles.yml` | `mdc dbt validate --env local`, `mdc dbt run --env clinic` |
| Frontend | Vite env files + `mdc_cli/credentials.py` for deploy | `mdc frontend dev`, `mdc deploy frontend --target demo` |
| Consult audio | `consult_audio_pipe/.env` | `mdc consult-audio validate`, `mdc consult-audio install` |

**Legacy:** `scripts/archive/environment_manager.ps1` is archived (Phase 5.5). `.\load_project.ps1 -Legacy` loads it with a deprecation warning only.

**PowerShell alias defaults (ETL):** `etl-validate` → `local` + `load`; `etl-run`, `etl-test`, `etl-status` → `clinic` + `full` (use `etl-status -Env local` for local warehouse). See `tools/mdc_cli/README.md`.

---

## Root `.env` (project root) — what uses it

The **project root `.env`** (from `.env.template`) is used **only** by **Docker Compose** when running the optional Airflow sandbox — **not** by native Option A clinic orchestration:

| Consumer | How it uses root `.env` |
|----------|-------------------------|
| **Docker Compose** | Variable substitution in `docker-compose.yml`; postgres/mysql/Airflow-init container env |
| **Native Airflow (Option A)** | **Does not use** root `.env` — ETL reads `etl_pipeline/.env_<stage>`; dbt via `mdc` / `dbt_dental_models/.env_*` |

**Not used by:** API (`api/settings.py` uses `api/.env_api_*` when OS env is empty), ETL (`etl_pipeline` uses `etl_pipeline/.env_*` via `settings_v2`), dbt (`mdc_cli/dbt_env.py` uses `dbt_dental_models/.env_*` or `deployment_credentials.json`), or ad-hoc Python dotenv from repo root. On EC2, `/opt/dbt_dental_clinic/.env` is the **deployed** EC2 env (from `/.env_ec2`), not this repo’s root `.env`.

---

## No env files at project root for local/clinic/test

**`/.env_local`**, **`/.env_clinic`**, and **`/.env_test`** at project root are **not used**. There is no fallback to root. Use only:

- **dbt:** `dbt_dental_models/.env_local`, `dbt_dental_models/.env_clinic`; demo/clinic JSON via `mdc_cli/dbt_env.py` + `deployment_credentials.json`.
- **ETL:** `etl_pipeline/.env_local`, `etl_pipeline/.env_clinic`, `etl_pipeline/.env_test` (loaded by ETL `settings_v2` when `mdc etl … --env <stage>` runs).

Root env files that **are** used: `/.env` (Docker Compose sandbox only), `/.env_ec2` (EC2 deploy source). Native clinic Airflow uses `etl_pipeline/.env_clinic` only. See §4.4.

---

## 1. Quick reference: where each env file lives

Every env file has a **unique name** = path from repo root. The same basename (e.g. `.env`) can appear in different directories; always use the full path when referring to a file.

| **Component** | **Unique path (env file)** | **Template** | **Loaded by** |
|---------------|----------------------------|-------------|---------------|
| **Root / Docker sandbox** | `/.env` | `/.env.template` | `docker-compose.yml` only (not native Option A) |
| **API** | `api/.env_api_local` | `api/.env_api_local.template` | `api/settings.py` when `API_ENVIRONMENT=local` (OS env wins; Phase 0) |
| **API** | `api/.env_api_demo` | (none committed) | `api/settings.py` when `API_ENVIRONMENT=demo` |
| **API** | `api/.env_api_clinic` | `api/.env_api_clinic.template` | `api/settings.py` when `API_ENVIRONMENT=clinic` |
| **API** | `api/.env_api_test` | `api/.env_api_test.template` | `api/settings.py` when `API_ENVIRONMENT=test` |
| **API on EC2** | `api/.env` (on EC2: `/opt/dbt_dental_clinic/api/.env`) | Contents from `api/.env_api_demo` or `api/.env_api_clinic` | systemd `EnvironmentFile`; **not** by Python |
| **ETL pipeline** | `etl_pipeline/.env_local` | `etl_pipeline/.env_local.template` | `settings_v2` via `mdc etl … --env local` |
| **ETL pipeline** | `etl_pipeline/.env_clinic` | `etl_pipeline/.env_clinic.template` | `settings_v2` via `mdc etl … --env clinic` |
| **ETL pipeline** | `etl_pipeline/.env_test` | `etl_pipeline/.env_test.template` | `settings_v2` via `mdc etl … --env test` |
| **dbt local** | `dbt_dental_models/.env_local` | `dbt_dental_models/.env_local.template` | `mdc_cli/dbt_env.py` via `mdc dbt … --env local` |
| **dbt clinic** | `dbt_dental_models/.env_clinic` (optional) | `etl_pipeline/.env_clinic.template` | `mdc_cli/dbt_env.py` + `deployment_credentials.json` |
| **dbt demo** | (no file) | — | `mdc_cli/dbt_env.py` from `deployment_credentials.json` |
| **dbt snowflake** | `dbt_dental_models/.env_snowflake` | `dbt_dental_models/.env_snowflake.template` | `mdc_cli/dbt_env.py` via `mdc dbt … --env snowflake` (portfolio mini warehouse) |
| **Frontend** | `frontend/.env` | (see frontend README) | Vite (dev); build-time for production |
| **Frontend** | `frontend/.env.local` | — | Vite; `mdc frontend dev` writes API URL/key for local |
| **Frontend** | `frontend/.env.production` | — | Vite production build (e.g. demo/clinic) |
| **Consult audio pipe** | `consult_audio_pipe/.env` | `consult_audio_pipe/.env.template` | `mdc consult-audio` child env (OS wins over file) |
| **Synthetic data generator** | `etl_pipeline/synthetic_data_generator/.env_demo` | `etl_pipeline/synthetic_data_generator/.env_demo.template` | Synthetic data scripts |
| **EC2 (generic)** | `/.env_ec2` | — | Deploy copies to `/opt/dbt_dental_clinic/.env` on EC2 |

---

## 2. Naming and placement rules

### 2.1 Unique names: path from repo root

Each env file has a **unique name** = its path from the repo root (e.g. `/.env`, `frontend/.env`, `api/.env_api_local`). The same **basename** can appear in different directories because different tools expect it:

| Basename | Unique paths | Used by |
|----------|--------------|---------|
| `.env` | `/.env`, `frontend/.env`, `consult_audio_pipe/.env`, `api/.env` (EC2 only) | Docker Compose (root); Vite (frontend); python-dotenv (consult_audio_pipe); systemd (api on EC2) |
| `.env.local` | `frontend/.env.local` | Vite |
| `.env.production` | `frontend/.env.production` | Vite |

When documenting or scripting, always use the **full path** (e.g. `frontend/.env`) so the file is unambiguous.

### 2.2 Global environment set (naming)

Use a single vocabulary so stages stay consistent across API, ETL, dbt, and deploy. Components **declare which they support**; they don’t invent new names. Frontend uses Vite’s scheme (`.env`, `.env.local`, `.env.production`) and maps to this via build/deploy targets.

**Deployment model:** local development on the laptop; two AWS deploy paths (demo vs clinic); plus a portfolio-only Snowflake warehouse (synthetic).

```
                    LOCAL / TEST (localhost)
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
     DEPLOY PATH 1                     DEPLOY PATH 2
     demo / portfolio                  clinic (PHI)
     (public, synthetic)               (IP-restricted)
              │
              ▼
     snowflake (optional)
     portfolio mini WH
     (synthetic export only)
```

| **Name** | **Meaning** | **API** | **ETL** | **dbt** |
|----------|-------------|---------|---------|---------|
| `local` | Local dev (localhost) | ✅ | ✅ | ✅ |
| `demo` | Portfolio/demo (synthetic data, public) — **not** production | ✅ | — | ✅ (from JSON) |
| `clinic` | Real clinic (PHI, IP‑restricted) — **this is production** | ✅ | ✅ | ✅ |
| `test` | Test DBs (CI/integration, localhost) | ✅ | ✅ | — |
| `snowflake` | Portfolio Snowflake mini warehouse (synthetic only) | — | — | ✅ (from `.env_snowflake`) |

**`prod` / `production`:** Not a separate `mdc` stage. Prefer `clinic` in code and env drivers (`API_ENVIRONMENT=clinic`). `prod` / `production` are acceptable only as **AWS resource** naming aliases for clinic infra (e.g. S3 bucket suffixes). Never use `production` to mean the public demo API — that was renamed to `demo` (see [archive/ENVIRONMENT_NAMING_CONVENTION.md](archive/ENVIRONMENT_NAMING_CONVENTION.md)).

| Aspect | `local` | `demo` | `clinic` | `test` | `snowflake` |
|--------|---------|--------|----------|--------|-------------|
| Location | Localhost | AWS | AWS | Localhost | Snowflake cloud |
| Frontend | `localhost:3000` | `dbtdentalclinic.com` | `clinic.dbtdentalclinic.com` | `localhost:3000` | — (portfolio tile later) |
| API | `localhost:8000` | `api.dbtdentalclinic.com` | `api-clinic.dbtdentalclinic.com` | `localhost:8000` | — |
| Database | Flexible / local PG | `opendental_demo` | `opendental_analytics` (schemas e.g. `mdc`, `glic`) | Test DBs | `OPENDENTAL_SF` |
| Data | Any | Synthetic | Real PHI | Test | Synthetic (from demo PG) |
| Access | Local | Public | IP-restricted | Local | Key-pair / Snowsight |

### 2.3 Conventions by component

- **Root**: `/.env` (Docker Compose sandbox only); `/.env.template` committed; `/.env_ec2` source for EC2 deploy.
- **API**: `api/.env_api_<env>` — e.g. `api/.env_api_local`, `api/.env_api_demo`, `api/.env_api_clinic`, `api/.env_api_test`. On EC2, systemd uses `api/.env` (contents from one of the `api/.env_api_*` files).
- **ETL**: `etl_pipeline/.env_<env>` — e.g. `etl_pipeline/.env_local`, `etl_pipeline/.env_clinic`, `etl_pipeline/.env_test`. **No project-root fallback** (ETL loader reads only from `etl_pipeline/`).
- **dbt:** `dbt_dental_models/.env_local`, `dbt_dental_models/.env_clinic`, `dbt_dental_models/.env_snowflake`; demo/clinic from `deployment_credentials.json` via `mdc_cli/dbt_env.py`. No project-root fallback.
- **Frontend**: `frontend/.env`, `frontend/.env.local`, `frontend/.env.production` (Vite; see frontend README).
- **Consult audio pipe**: `consult_audio_pipe/.env`; template `consult_audio_pipe/.env.template`.
- **Templates**: Only `*.template` (or `.env.template`) are committed. Actual `.env*` (except templates) are gitignored.

### 2.4 “Used by” header in each env file

Every `.env` and `.env_*` file should have a comment at the top stating **which services/components use it**, for example:

```text
# Used by: DOCKER COMPOSE, AIRFLOW (variable substitution + container env for postgres, mysql, dbt)
```

This makes it obvious at a glance which file to edit. Templates and most env files in the repo already include a `# Used by: ...` line. For **gitignored** files that you create or that tools generate (e.g. `/.env`, `frontend/.env`, `consult_audio_pipe/.env`), add the header manually:

| **File** | **Used by** |
|----------|--------------|
| `/.env` | DOCKER COMPOSE (variable substitution), AIRFLOW, POSTGRES, MYSQL, DBT containers |
| `dbt_dental_models/.env_local` | `mdc dbt … --env local`; publish local side; Phase 6 ETL analytics overlay |
| `dbt_dental_models/.env_snowflake` | `mdc dbt … --env snowflake`; `scripts/snowflake/*.py` (portfolio mini warehouse) |
| `frontend/.env` | VITE (frontend default env) |
| `consult_audio_pipe/.env` | CONSULT AUDIO PIPE (LLM analysis pipeline) |

### 2.6 .gitignore

From project `.gitignore`:

- `.env` is ignored.
- `.env_*` are ignored.
- **Exceptions (not ignored):** `!.env_*.template`, `!.env.template`, `!**/.env.template`.
- So: **commit only template files**; never commit real secrets or live `.env` / `.env_*` files.

---

## 3. Design principles and scalability

### 3.1 What’s already scalable

- **Component-scoped env files** (API vs ETL vs frontend vs consult_audio) — different runtimes want different env shapes; one “.env to rule them all” would be brittle.
- **Explicit drivers** (`API_ENVIRONMENT`, `ETL_ENVIRONMENT`) — you can add staging/prod without rewriting everything.
- **Templates committed, secrets gitignored** — correct baseline for a repo others might clone.
- **This doc as single source of truth** — helps onboarding and “why does this service not start?” debugging.

### 3.2 Precedence rule (target state)

To avoid “which value wins?” and “works locally, breaks on EC2,” every component should follow the same precedence and have **one official loader** in production:

1. **Process environment** (e.g. `os.environ`, systemd `EnvironmentFile`) **always wins**.
2. **Component env file** (e.g. `api/.env_api_clinic`) — loaded only if not already set (or as sole source when process env is empty).
3. **Safe defaults** — only for optional vars; required vars should fail fast with a clear error.

**Production rule:** In production (e.g. API on EC2), the **only** source of env should be the process (systemd injects `api/.env` into the process). The component’s Python code should **not** load a file in prod unless explicitly desired (e.g. optional override). That way there is no dual-loading ambiguity.

### 3.3 Canonical location and fallback

- **Canonical location per component** = the component’s directory (e.g. `etl_pipeline/.env_*`, `api/.env_api_*`, `dbt_dental_models/.env_*`). Prefer that; avoid “shadow configs” at repo root.
- **Root fallback** (e.g. ETL/dbt reading `/.env_local`, `/.env_clinic`) is convenient but at scale someone edits the wrong file and nothing changes.
- **If fallback is kept:** (1) **Noisy** — log a WARNING with the path actually used (e.g. “Loaded config from `/.env_local` (fallback); canonical is `etl_pipeline/.env_local`”). (2) **Optional** — e.g. behind a flag or env var like `ETL_ALLOW_ROOT_ENV_FALLBACK=1`.

### 3.4 One config module per component (target state)

Each component should have a single config entrypoint that:

- Loads **once** (no scattered `load_dotenv` calls).
- Logs a **one-line summary** at startup: environment name + path loaded from (e.g. `API_ENVIRONMENT=clinic, loaded from api/.env_api_clinic`).
- **Validates required vars** and fails fast with a good error (no silent defaults for required credentials).
- Optionally: a **lightweight schema** — e.g. `required_vars = [...]` per component, or Pydantic/TypedSettings.

### 3.6 Phase 6 — Postgres authority matrix

| Connection role | Authority | Consumed by |
|-----------------|-----------|-------------|
| Local warehouse Postgres | `dbt_dental_models/.env_local` | `mdc dbt --env local`, `mdc etl --env local`, `mdc publish analytics` (local side) |
| Clinic RDS host/db/user | `deployment_credentials.json` → `clinic_database.postgresql` | `mdc dbt --env clinic`, `mdc etl --env clinic` (composed) |
| Clinic RDS password (live) | Secrets Manager `rds!db-...` (fallback: `api/.env_api_clinic`) | `overlay_clinic_rds_credentials` in etl/dbt/freshness/publish |
| Clinic API deploy | `api/.env_api_clinic` → EC2 `api/.env` | `mdc deploy api --env clinic`, `mdc secrets pull clinic` |
| OpenDental source + MySQL replication | `etl_pipeline/.env_clinic` | `mdc etl --env clinic` (source/repl only — **no** `POSTGRES_ANALYTICS_*`) |
| Snowflake portfolio warehouse | `dbt_dental_models/.env_snowflake` (`SNOWFLAKE_*`; key-pair preferred) | `mdc dbt --env snowflake`; `scripts/snowflake/*.py` |
| Demo Postgres (synthetic export source) | `etl_pipeline/synthetic_data_generator/.env_demo` (`DEMO_POSTGRES_*`) | Synthetic generator; Snowflake export script |

**Do not** put Snowflake secrets under `scripts/snowflake/` or repo root. **Do not** duplicate `DEMO_POSTGRES_*` into `.env_snowflake` — keep demo Postgres authority on the generator file.

**Do not** duplicate `POSTGRES_ANALYTICS_*` in `etl_pipeline/.env_clinic`. Use:

```powershell
mdc etl run --env clinic --tunnel-db --profile full
mdc etl exec --env clinic --tunnel-db -- pipenv run python scripts/initialize_etl_tracking_tables.py clinic
```

`mdc status` warns when deprecated analytics keys remain in ETL env files.

---

### 3.5 Planned improvements (checklist)

| **Improvement** | **Status** | **Notes** |
|-----------------|------------|------------|
| Document precedence (process > file > defaults) in code + doc | **Done (Phase 0–1)** | API, ETL `providers.py`, ad-hoc ETL scripts use `override=False`; see [archive/ENVIRONMENT_HANDLING_REVIEW.md](archive/ENVIRONMENT_HANDLING_REVIEW.md). |
| One official loader in production (e.g. API on EC2: systemd only) | **Done (Phase 0)** | `api/settings.py` skips `.env_api_*` when OS env is populated; deploy writes `api/.env` only. |
| Canonical location only, or noisy + optional fallback | Todo | ETL/dbt today fall back to root; add WARNING and/or flag. |
| **Phase 6 credential dedup** | **Done** | `postgres_env.py`, `etl_env.py`, `mdc etl --tunnel-db`, `mdc etl exec`; see §3.6. |
| Global env set (local, demo, clinic, test, snowflake) | **Done** | §2.2 naming + comparison matrix; historical rename plan in [archive/ENVIRONMENT_NAMING_CONVENTION.md](archive/ENVIRONMENT_NAMING_CONVENTION.md). |
| Commit sanitized templates for API + frontend | **Partial** | **Done:** `api/.env_api_local.template`, `api/.env_api_clinic.template`, `dbt_dental_models/.env_local.template`, `dbt_dental_models/.env_snowflake.template`. **Todo:** `api/.env_api_demo.template`, `frontend/.env.template`. |
| Config module: load once, log source, validate required | **Done (Phase 2–4)** | `api/settings.py`, ETL `settings_v2.py`; `mdc status` / `mdc * validate` |
| Track `docs/deployment/ENVIRONMENT_FILES.md` in git | **Done (Phase 1)** | Whitelisted in `.gitignore`; README link resolves. |
| `dbt_dental_models/profiles.yml.template` | **Done** | Uses `env_var()`; copy to local `profiles.yml`. |

---

## 4. Who loads what (by component)

### 4.1 API (`api/`)

- **Driver:** `API_ENVIRONMENT` (set by `mdc api … --env <stage>` in the child process, or explicitly in the shell).
- **Loader:** `api/settings.py` → `load_api_settings()` loads `api/.env_api_<environment>` **only when** the stage’s analytics host var is not already in the OS environment (Phase 0). `api/config.py` delegates to settings. On EC2, systemd `EnvironmentFile=api/.env` populates the process env first, so a stale `api/.env_api_clinic` on disk is ignored.
- **On EC2:** systemd uses `EnvironmentFile=/opt/dbt_dental_clinic/api/.env`. Deploy with `mdc deploy api --env clinic` (wraps `deploy_api_file.ps1 -ClinicEnv`). Clinic unit: `dental-clinic-api-clinic` (`api/dental-clinic-api-clinic.service`).
- **Local:** `mdc api test-config --env local` or alias `api-test`. Run with `mdc api run --env local` / `api-run`. Clinic + tunnel: `mdc tunnel clinic-db` then `mdc api run --env clinic --tunnel-db`.

See [archive/ENVIRONMENT_HANDLING_REVIEW.md](archive/ENVIRONMENT_HANDLING_REVIEW.md) (Phase 0) and `api/settings.py`.

### 4.2 ETL pipeline (`etl_pipeline/`)

- **Driver:** `ETL_ENVIRONMENT` (`local`, `clinic`, or `test`) — set per run by `mdc etl … --env <stage>` in the child env (not by shell `etl-init`).
- **Loader:** `etl_pipeline/etl_pipeline/config/settings_v2.py` (via `FileConfigProvider`) loads `etl_pipeline/.env_<environment>` with `override=False` (OS env wins; Phase 0).
- **Validate:** `mdc etl validate --env local --profile load` or alias `etl-validate` (defaults to local).
- **Run:** `mdc etl run --env clinic --profile full` or alias `etl-run` (defaults to clinic + full).
- **Status:** `mdc etl status --env clinic` or alias `etl-status` (defaults to clinic + full; use `-Env local` for local).
- **Creation:** `etl_pipeline/scripts/setup_environments.py` can create `.env_clinic` and `.env_test` from templates.

#### ETL profiles (`load` vs `full`)

`ETL_PROFILE` selects **which database connections pydantic validates and exposes** for a run. It does not change which `.env_<stage>` file is loaded — only which connection roles are required.

| Profile | Validates | Typical use |
|---------|-----------|-------------|
| **`load`** | MySQL replication + PostgreSQL analytics only | Local warehouse work: confirm repl/analytics connectivity without OpenDental source creds |
| **`full`** | OpenDental source + replication + analytics | Full pipeline: extract from source → replication → analytics (`mdc etl run` default) |

**Resolution** (`settings_v2.resolve_etl_profile`):

| Input | Result |
|-------|--------|
| `mdc etl … --profile load\|full` | Explicit profile wins |
| `ETL_PROFILE` in child/shell env | Used when no explicit `--profile` |
| Neither set | `local` → `load`; `clinic` / `test` → `full` |

**`mdc` defaults** (see `tools/mdc_cli/mdc_cli/paths.py`):

| Command | Default stage | Default profile |
|---------|---------------|-----------------|
| `mdc etl validate` / alias `etl-validate` | `local` | `load` |
| `mdc etl run` / `etl-run` | `clinic` | `full` |
| `mdc etl status` / `etl-status` | `clinic` | `full` (use `-Env local` for local warehouse) |

Validate uses `settings_v2.load_etl_connection_settings(..., profile=…)` directly. Run/status inject a flat child env (including `ETL_PROFILE`) via `load_etl_env_dict`.

**`Settings.profile`** (`etl_pipeline/etl_pipeline/config/settings.py`):

- When using `FileConfigProvider` / typed `settings_v2` models: returns the resolved profile string (`"load"` or `"full"`) from `ETLConnectionSettings`.
- Otherwise: falls back to the raw `ETL_PROFILE` environment variable (may be unset).

**`Settings.get_source_connection_config()` when profile is `load`:**

- OpenDental source is **optional** at validation time (`OPENDENTAL_SOURCE_*` may be absent from `.env_local`).
- Calling `get_source_connection_config()` (or `get_database_config(DatabaseType.SOURCE)`) still requires a configured source. If `ETLConnectionSettings.source` is `None`, `connection_config_dict` raises `ValueError` with a message to add `OPENDENTAL_SOURCE_*` to `.env_<stage>` or switch to profile `full`.
- **Replication and analytics getters always work** under `load` when those vars are present.

**Practical guidance:**

- Use **`mdc etl validate --env local --profile load`** to check local MySQL replication + Postgres analytics without source credentials.
- Use **`mdc etl run --env clinic --profile full`** (default) for end-to-end extraction; source vars must be present.
- Do not call `get_source_connection_config()` in code paths that should run under `load` unless source creds are intentionally configured.

**Library note:** `FileConfigProvider` currently calls `load_etl_connection_settings(..., profile="full")` when constructing `Settings` from disk. The `mdc` child env still sets `ETL_PROFILE` for subprocesses; honoring that in `FileConfigProvider` is a follow-up. For tests, pass `profile="load"` to `DictConfigProvider` or use `load_etl_connection_settings_from_env(..., profile="load")`.

#### Same variables in `.env_local` and `.env_clinic` (confusion / cross-contam risk)

Development for clinic infra is often done locally before deploy, so **`.env_local` and `.env_clinic` intentionally share the same variable names** (e.g. `OPENDENTAL_SOURCE_HOST`, `POSTGRES_ANALYTICS_*`). Only **one** file is loaded per run: the loader reads `ETL_ENVIRONMENT` from the process, then loads `etl_pipeline/.env_<that>`. There is no merging of the two files.

**Possible confusion / cross-contamination:**

| Risk | What happens | Mitigation |
|------|----------------|------------|
| **Edit the wrong file** | You change `etl_pipeline/.env_clinic` but meant `.env_local` (or vice versa). Same variable names make it easy to not notice. | Clear “Used by” headers; keep clinic header with a **PHI / deploy** warning so the file’s purpose is obvious. |
| **Clinic file with local-only values** | `.env_clinic` is used on EC2 or in a real clinic context but still has `POSTGRES_ANALYTICS_HOST=localhost` (e.g. from copy-paste). Pipeline would then talk to localhost instead of RDS. | Before deploying or running clinic ETL in a non-local context, confirm `.env_clinic` has the correct hosts/credentials. Optional: loader could log a WARNING when `ETL_ENVIRONMENT=clinic` and `POSTGRES_ANALYTICS_HOST=localhost`. |
| **Think parent shell has wrong stage** | Parent shell may still have `ETL_ENVIRONMENT=clinic` from an old session, but **`mdc` scrubs stage prefixes** before injecting child env from the file for the `--env` you passed. | Prefer `mdc etl … --env <stage>`; confirm stage in the run banner and ETL startup log (“Loading environment from …”). |

**Recommendation:** Keep the shared variable set (same code paths, simpler), but (1) ensure both files have a clear “Used by” and a **warning in `.env_clinic`** that it may be used for real PHI / deploy, and (2) before any clinic deploy or non-local clinic run, spot-check that `etl_pipeline/.env_clinic` has the intended targets (not localhost unless that’s deliberate).

**Do not rely on stale shell `ETL_ENVIRONMENT`:** Use `mdc etl validate|run|status --env <stage>` (or aliases) so the correct `.env_*` file is loaded into the **child** process. Required vars are validated by pydantic before run (`mdc etl validate`).

### 4.3 dbt (`dbt_dental_models/`)

- **Driver:** `mdc dbt … --env <stage>` (`local`, `demo`, `clinic`, `snowflake`). Child env sets `DBT_TARGET` and connection vars.
- **Loader:** `tools/mdc_cli/mdc_cli/dbt_env.py` — mirrors legacy `dbt-init` rules:
  - **local:** `dbt_dental_models/.env_local` (from `dbt_dental_models/.env_local.template`)
  - **clinic:** `deployment_credentials.json` (`clinic_database.postgresql`, or nested `backend_api.clinic_database_reference.rds.*`); optional fallback `dbt_dental_models/.env_clinic` (deprecated)
  - **demo:** `deployment_credentials.json` only (no `.env_demo` file)
  - **snowflake:** `dbt_dental_models/.env_snowflake` (from `.env_snowflake.template`) — portfolio payments/collections mini warehouse; **synthetic only**
  - Password values in JSON are normalized (handles accidental full Secrets Manager JSON blobs in the password field)
- **Snowflake auth (key-pair preferred):**
  - Required in `.env_snowflake`: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`
  - Private key file lives under gitignored `dbt_dental_models/.snowflake/` (e.g. `rsa_key.p8`) — **not** an env file; path only goes in `.env_snowflake`
  - Register public key in Snowsight (`ALTER USER … SET RSA_PUBLIC_KEY=…`); see [docs/snowflake/SETUP.md](../snowflake/SETUP.md)
  - `SNOWFLAKE_PASSWORD` is optional fallback only (often fails when Snowsight uses passkey login)
  - `profiles.yml` snowflake target uses `private_key_path` / optional passphrase via `env_var()`
- **Snowflake export source:** demo Postgres `DEMO_POSTGRES_*` from `etl_pipeline/synthetic_data_generator/.env_demo` (do not duplicate into `.env_snowflake`)
- **Validate:** `mdc dbt validate --env local` (or `--env snowflake`)
- **Run:** `mdc dbt run --env clinic -- --select …` (passthrough after `--`); Snowflake: `mdc dbt invoke --env snowflake -- build --select tag:snowflake`
- **Clinic from laptop:** RDS is private — see [CLINIC_ANALYTICS_WORKFLOW.md](CLINIC_ANALYTICS_WORKFLOW.md) (local dbt + tunnel + publish, or EC2 dbt)
- **Profiles:** `dbt_dental_models/profiles.yml` (from `profiles.yml.template`); `DBT_PROFILES_DIR` set in child env by `mdc`.

### 4.4 Docker / Airflow

**Option A (clinic nightly):** Native Airflow on the host — **no root `.env` ETL contract**. ETL reads `etl_pipeline/.env_<stage>`; dbt via `mdc dbt` or `dbt_dental_models/.env_local`. See [`airflow/ORCHESTRATION_ROADMAP.md`](../../airflow/ORCHESTRATION_ROADMAP.md) § Environment contract.

**Optional Docker sandbox:** `docker-compose.yml` runs Airflow in containers for experiments. Root `.env` supplies Airflow metadata DB credentials and Fernet key only. ETL/dbt vars are **not** injected into Airflow containers (avoids overriding stage files).

- **Root `.env`:** Copy `.env.template` → `.env` for Compose postgres/mysql/Airflow-init. Never commit `.env`.

### 4.5 Frontend (`frontend/`)

- Vite reads `.env`, `.env.local`, `.env.production` (and mode-specific files). See `frontend/README.md`.
- Variables must be prefixed with `VITE_` (e.g. `VITE_API_URL`, `VITE_API_KEY`, `VITE_IS_DEMO`).
- **Local dev:** `mdc frontend dev` writes `frontend/.env.local` and runs Vite (alias `frontend-dev`).
- **Deploy:** `mdc deploy frontend --target demo|clinic` (S3 + CloudFront); credentials from `deployment_credentials.json` / `.frontend-deploy.json` via `mdc_cli/credentials.py`.

### 4.6 Consult audio pipe (`consult_audio_pipe/`)

- Single `.env` in `consult_audio_pipe/` from `consult_audio_pipe/.env.template` (API keys).
- **Setup:** `mdc consult-audio install` (venv + requirements).
- **Validate:** `mdc consult-audio validate` (keys, ffmpeg warning).
- **Run:** `mdc consult-audio pipeline run` — child env from `.env`; OS env wins over file.

### 4.7 Synthetic data generator

- Uses `etl_pipeline/synthetic_data_generator/.env_demo` (from `.env_demo.template`) for demo/synthetic data generation.
- Also the **authority for `DEMO_POSTGRES_*`** when exporting synthetic tables to Snowflake (`scripts/snowflake/export_demo_to_snowflake.py`). Do not invent a second demo Postgres env file for Snowflake.

### 4.8 EC2 deployment

#### Clinic API env (`.env` on EC2)

| Step | Detail |
|------|--------|
| **Source (you maintain)** | Local gitignored file `api/.env_api_clinic` (see `api/README.md` for variable list) |
| **Deploy** | `mdc deploy api --env clinic` from repo root (after `pip install -e tools/mdc_cli` and `.\load_project.ps1`) |
| **Mechanism** | `scripts/deployment/deploy_api_file.ps1 -FilePath api\.env_api_clinic -ClinicEnv` — reads local file, base64 over SSM, writes **`/opt/dbt_dental_clinic/api/.env`** |
| **Not generated at deploy** | Credentials are **not** pulled from `deployment_credentials.json` during deploy; that file holds AWS resource IDs and reference values only |
| **Post-deploy** | Restarts systemd unit, curls `/health/db` on the instance (unless `-SkipHealthCheck`) |

#### AWS naming vs systemd (common confusion)

| Name | Example | Meaning |
|------|---------|---------|
| EC2 **Name tag** | `dental-clinic-api-clinic` | AWS console / `deployment_credentials.json` → `backend_api.clinic_api.ec2.name` |
| **systemd unit** | `dental-clinic-api-clinic` | `systemctl restart dental-clinic-api-clinic`; unit file `api/dental-clinic-api-clinic.service` |
| Target group | `dental-clinic-api-clinic-tg` | ALB routing for `api-clinic.dbtdentalclinic.com` |
| IAM role | `dental-clinic-api-clinic-role` | Clinic EC2 instance profile |

Set `backend_api.clinic_api.ec2.systemd_service` to `dental-clinic-api-clinic` in `deployment_credentials.json`. Deploy scripts map legacy value `dental-clinic-api` to `dental-clinic-api-clinic` automatically.

#### SSM shell on clinic API EC2

After `.\load_project.ps1`:

```powershell
ssm-connect-clinic-api    # alias -> mdc ssm connect clinic-api
# or: mdc tunnel clinic-db
```

Requires `deployment_credentials.json` → `backend_api.clinic_api.ec2.instance_id` and AWS CLI + Session Manager plugin.

#### Manual deploy (equivalent)

```powershell
.\scripts\deployment\deploy_api_file.ps1 -FilePath "api\.env_api_clinic" -ClinicEnv
```

This backs up the previous `.env`, retires any stale `api/.env_api_clinic` on the instance, restarts the API, and verifies `/health/db` (unless `-SkipHealthCheck`). See [archive/ENVIRONMENT_HANDLING_REVIEW.md](archive/ENVIRONMENT_HANDLING_REVIEW.md) (Phase 0).

- **Root on EC2:** `deployment_credentials.json.template` references `environment_file: ".env_ec2"` and `environment_file_path: "/opt/dbt_dental_clinic/.env"` for dbt on EC2; that is separate from `api/.env`.

---

## 5. Environment variable drivers (summary)

| **Variable** | **Used by** | **Valid values** | **Effect** |
|--------------|-------------|-------------------|------------|
| `API_ENVIRONMENT` | API | `local`, `demo`, `clinic`, `test` | Which `api/.env_api_*` `settings.py` may load (if OS env empty) |
| `ETL_ENVIRONMENT` | ETL pipeline | `local`, `clinic`, `test` | Which `etl_pipeline/.env_*` is loaded in the **mdc child** process |
| `ETL_PROFILE` | ETL pipeline | `load`, `full` | Which connections to validate (see §4.2 ETL profiles); default `load` for `local`, `full` for `clinic`/`test` |
| `DBT_TARGET` | dbt | `local`, `demo`, `clinic`, `snowflake` | Set by `mdc_cli/dbt_env.py` for dbt subprocess; selects `profiles.yml` target |
| `SNOWFLAKE_*` | dbt snowflake + export scripts | see `.env_snowflake.template` | Account/user/role/warehouse/db; auth via `SNOWFLAKE_PRIVATE_KEY_PATH` (preferred) |
| (none for frontend) | Frontend | — | File name (e.g. `.env.production`) and build-time env vars |

---

## 6. Templates (committed) — checklist

Use these to create local/env-specific files; never commit the filled-in files. Paths below are the unique env file names.

- [ ] **Root:** `/.env.template` → copy to `/.env` only if using Docker Compose sandbox.
- [ ] **ETL:** `etl_pipeline/.env_local.template`, `etl_pipeline/.env_clinic.template`, `etl_pipeline/.env_test.template` → copy to `etl_pipeline/.env_local`, `.env_clinic`, `.env_test` as needed.
- [ ] **dbt local:** `dbt_dental_models/.env_local.template` → copy to `dbt_dental_models/.env_local`.
- [ ] **dbt snowflake:** `dbt_dental_models/.env_snowflake.template` → copy to `dbt_dental_models/.env_snowflake`; set `SNOWFLAKE_PRIVATE_KEY_PATH` to a key under `dbt_dental_models/.snowflake/` (see [docs/snowflake/SETUP.md](../snowflake/SETUP.md)).
- [ ] **API local:** `api/.env_api_local.template` → copy to `api/.env_api_local`.
- [ ] **API clinic:** `api/.env_api_clinic.template` → copy to `api/.env_api_clinic`, then `mdc secrets pull clinic` for the RDS password.
- [ ] **API test:** `api/.env_api_test.template` → copy to `api/.env_api_test` and fill in test DB credentials.
- [ ] **Consult audio:** `consult_audio_pipe/.env.template` → copy to `consult_audio_pipe/.env`.
- [ ] **Synthetic data:** `etl_pipeline/synthetic_data_generator/.env_demo.template` → copy to `etl_pipeline/synthetic_data_generator/.env_demo`.
- **API demo / Frontend:** No committed templates yet for `api/.env_api_demo` or `frontend/.env`; create from component READMEs or copy from team docs.

---

## 7. Inventory script

Run the optional inventory script to see which expected env files exist (without printing secrets):

```powershell
.\scripts\utils\list_env_files.ps1
```

This lists known env file paths and reports Present / Missing. See script header for scope.

---

## 8. Related docs

- **Daily CLI:** [tools/mdc_cli/README.md](../../tools/mdc_cli/README.md), [scripts/README.md](../../scripts/README.md)
- **Setup cheat sheet (incl. Snowflake):** [ENVIRONMENT_SETUP.md](ENVIRONMENT_SETUP.md)
- **Environment modernization (phase history, archived):** [archive/ENVIRONMENT_HANDLING_REVIEW.md](archive/ENVIRONMENT_HANDLING_REVIEW.md)
- **Env naming history (`production` → `demo`/`clinic`, archived):** [archive/ENVIRONMENT_NAMING_CONVENTION.md](archive/ENVIRONMENT_NAMING_CONVENTION.md)
- **Snowflake portfolio warehouse:** [docs/snowflake/SETUP.md](../snowflake/SETUP.md), [SNOWFLAKE_INTEGRATION_PLAN.md](../snowflake/SNOWFLAKE_INTEGRATION_PLAN.md)
- **API env in detail:** [docs/api/API_ENV_FILE_EXPLANATION.md](../api/API_ENV_FILE_EXPLANATION.md)
- **EC2 env file rename/setup:** [EC2_ENV_FILE_RENAME_GUIDE.md](EC2_ENV_FILE_RENAME_GUIDE.md), [EC2_ENV_FILE_RENAME_COMMANDS.md](EC2_ENV_FILE_RENAME_COMMANDS.md)
- **Deployment credentials (env file refs):** `deployment_credentials.json.template`
- **Frontend env:** `frontend/README.md`
- **ETL setup:** `etl_pipeline/scripts/setup_environments.py`, `etl_pipeline/README.md`

---

## 9. Keeping this doc updated

- **Unique names:** Always refer to env files by path from repo root (e.g. `frontend/.env`, not just `.env`) so each file has a single, unambiguous name.
- When adding a **new env file** (or new component): add a row to the table in §1 with the unique path, document loader in §4, and add template to §6 if committed.
- When **renaming or removing** an env file: update this doc, `.gitignore`, and any loader or deploy script (e.g. `mdc_cli/dbt_env.py`, `deploy_api_file.ps1`, `generate_api_key.ps1`).
- When changing **load order or driver** (e.g. `API_ENVIRONMENT` / `ETL_ENVIRONMENT`): update §4 and §5.
- When implementing **scalability improvements** (§3): tick items in the §3.5 checklist and update code/docs accordingly.
