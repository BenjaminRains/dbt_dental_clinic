# Environment Files — Project-Wide Reference

This document is the **single source of truth** for how `.env` and related environment files are managed across the dbt_dental_clinic codebase. Use it to onboard, audit, or refactor environment configuration.

See also **[ENVIRONMENT_HANDLING_REVIEW.md](../ENVIRONMENT_HANDLING_REVIEW.md)** for the Phase 0/1 precedence fixes and the `pydantic-settings` modernization roadmap.

---

## Root `.env` (project root) — what uses it

The **project root `.env`** (created from `.env.template`) is used **only** by **Docker Compose** when you run the stack locally or on a host where Compose is used:

| Consumer | How it uses root `.env` |
|----------|-------------------------|
| **Docker Compose** | (1) **Variable substitution** — Compose reads `.env` from the project directory and substitutes `${VAR}` in `docker-compose.yml` (e.g. `${POSTGRES_USER}`, `${AIRFLOW_FERNET_KEY}`). (2) **Container env** — The **dbt**, **postgres**, and **mysql** services have `env_file: - .env`, so those containers get their environment from the root `.env`. |
| **Airflow containers** | Airflow services (webserver, scheduler) get values via the `environment:` section in `docker-compose.yml`; those values are substituted from the same `.env` when you run `docker-compose up`. |

**Not used by:** API (`api/config.py` uses `api/.env_api_*`), ETL (`etl_pipeline` uses `etl_pipeline/.env_*`), dbt env manager (uses `.env_local` / `.env_clinic` / `deployment_credentials.json`), or any Python code that loads dotenv from the repo. On EC2, the file at `/opt/dbt_dental_clinic/.env` is the **deployed** env (from `.env_ec2`), not this repo’s root `.env`.

---

## No env files at project root for local/clinic/test

**`/.env_local`**, **`/.env_clinic`**, and **`/.env_test`** at project root are **not used**. There is no fallback to root. Use only:

- **dbt:** `dbt_dental_models/.env_local`, `dbt_dental_models/.env_clinic` (loaded by `dbt-init`).
- **ETL:** `etl_pipeline/.env_local`, `etl_pipeline/.env_clinic`, `etl_pipeline/.env_test` (loaded by ETL config and scripts).

Root env files that **are** used: `/.env` (Docker/Airflow), `/.env_ec2` (EC2 deploy source). See §1 table.

---

## 1. Quick reference: where each env file lives

Every env file has a **unique name** = path from repo root. The same basename (e.g. `.env`) can appear in different directories; always use the full path when referring to a file.

| **Component** | **Unique path (env file)** | **Template** | **Loaded by** |
|---------------|----------------------------|-------------|---------------|
| **Root / Docker / Airflow** | `/.env` | `/.env.template` | `docker-compose.yml`, Airflow containers |
| **API** | `api/.env_api_local` | (none committed) | `api/config.py` when `API_ENVIRONMENT=local` |
| **API** | `api/.env_api_demo` | (none committed) | `api/config.py` when `API_ENVIRONMENT=demo` |
| **API** | `api/.env_api_clinic` | (none committed) | `api/config.py` when `API_ENVIRONMENT=clinic` |
| **API** | `api/.env_api_test` | `api/.env_api_test.template` | `api/config.py` when `API_ENVIRONMENT=test` |
| **API on EC2** | `api/.env` (on EC2: `/opt/dbt_dental_clinic/api/.env`) | Contents from `api/.env_api_demo` or `api/.env_api_clinic` | systemd `EnvironmentFile`; **not** by Python |
| **ETL pipeline** | `etl_pipeline/.env_local` | `etl_pipeline/.env_local.template` | ETL config when `ETL_ENVIRONMENT=local` (no root fallback) |
| **ETL pipeline** | `etl_pipeline/.env_clinic` | `etl_pipeline/.env_clinic.template` | ETL config when `ETL_ENVIRONMENT=clinic` (no root fallback) |
| **ETL pipeline** | `etl_pipeline/.env_test` | `etl_pipeline/.env_test.template` | ETL config when `ETL_ENVIRONMENT=test` |
| **dbt (env manager)** | `dbt_dental_models/.env_local` | `etl_pipeline/.env_local.template` | `scripts/environment_manager.ps1` (dbt-init -Target local) |
| **dbt (env manager)** | `dbt_dental_models/.env_clinic` | `etl_pipeline/.env_clinic.template` | `scripts/environment_manager.ps1` (dbt-init -Target clinic) |
| **dbt demo** | (no file) | — | `environment_manager.ps1` from `deployment_credentials.json` |
| **Frontend** | `frontend/.env` | (see frontend README) | Vite (dev); build-time for production |
| **Frontend** | `frontend/.env.local` | — | Vite; created by env manager / setup scripts |
| **Frontend** | `frontend/.env.production` | — | Vite production build (e.g. demo/clinic) |
| **Consult audio pipe** | `consult_audio_pipe/.env` | `consult_audio_pipe/.env.template` | App (e.g. python-dotenv) |
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

### 2.2 Global environment set

Use a single vocabulary so adding staging/prod later doesn’t fragment names:

| **Name** | **Meaning** | **API** | **ETL** | **dbt** |
|----------|-------------|---------|---------|---------|
| `local` | Local dev (localhost) | ✅ | ✅ | ✅ |
| `demo` | Portfolio/demo (synthetic data, public) | ✅ | — | ✅ (from JSON) |
| `clinic` | Real clinic (PHI, IP‑restricted) | ✅ | ✅ | ✅ |
| `test` | Test DBs (CI/integration) | ✅ | ✅ | — |
| `prod` | Future: production / multi-tenant | — | — | — |

Components **declare which they support**; they don’t invent new names. Frontend uses Vite’s scheme (`.env`, `.env.local`, `.env.production`) and maps to this via build scripts.

### 2.3 Conventions by component

- **Root**: `/.env` (Docker/Airflow); `/.env.template` committed; `/.env_ec2` source for EC2 deploy.
- **API**: `api/.env_api_<env>` — e.g. `api/.env_api_local`, `api/.env_api_demo`, `api/.env_api_clinic`, `api/.env_api_test`. On EC2, systemd uses `api/.env` (contents from one of the `api/.env_api_*` files).
- **ETL**: `etl_pipeline/.env_<env>` — e.g. `etl_pipeline/.env_local`, `etl_pipeline/.env_clinic`, `etl_pipeline/.env_test`. **No project-root fallback** (ETL loader reads only from `etl_pipeline/`).
- **dbt (env manager)**: `dbt_dental_models/.env_local`, `dbt_dental_models/.env_clinic` only (no project-root fallback). Demo: no file, uses `deployment_credentials.json`.
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

### 3.5 Planned improvements (checklist)

| **Improvement** | **Status** | **Notes** |
|-----------------|------------|------------|
| Document precedence (process > file > defaults) in code + doc | **Done (Phase 0–1)** | API, ETL `providers.py`, ad-hoc ETL scripts use `override=False`; see `ENVIRONMENT_HANDLING_REVIEW.md`. |
| One official loader in production (e.g. API on EC2: systemd only) | **Done (Phase 0)** | `config.py` skips `.env_api_*` when OS env is populated; deploy script writes `api/.env` only. |
| Canonical location only, or noisy + optional fallback | Todo | ETL/dbt today fall back to root; add WARNING and/or flag. |
| Global env set (local, demo, clinic, test, prod) | Doc only | §2.2; components to declare “unsupported” instead of new names. |
| Commit sanitized templates for API + frontend | Todo | Add `api/.env_api_local.template`, `api/.env_api_demo.template`, `api/.env_api_clinic.template`, `frontend/.env.template` (variable names + comments, no secrets). High ROI for collaborators. |
| Config module: load once, log source, validate required | Partial | API and ETL already validate; pydantic-settings migration planned (Phase 2). |
| Track `docs/ENVIRONMENT_FILES.md` in git | **Done (Phase 1)** | Whitelisted in `.gitignore`; README link resolves. |
| `dbt_dental_models/profiles.yml.template` | **Done** | Uses `env_var()`; copy to local `profiles.yml`. |

---

## 4. Who loads what (by component)

### 4.1 API (`api/`)

- **Driver:** `API_ENVIRONMENT` (must be set before or when the API starts).
- **Loader:** `api/config.py` → `_load_environment_file()` loads `api/.env_api_<environment>` **only when** the stage’s analytics host var is not already in the OS environment (Phase 0). On EC2, systemd `EnvironmentFile=api/.env` populates the process env first, so a stale `api/.env_api_clinic` on disk is ignored.
- **On EC2:** systemd uses `EnvironmentFile=/opt/dbt_dental_clinic/api/.env`. Deploy with `scripts/deployment/deploy_api_file.ps1 -ClinicEnv` (writes `api/.env` and retires stale `api/.env_api_clinic`). See `api/dental-clinic-api.service`.
- **Local:** Use `scripts/environment_manager.ps1` → `api-init`, then choose local/demo/clinic/test; it loads the matching `api/.env_api_*` into the current PowerShell process, and `api-run` starts the API with that environment.

See **[ENVIRONMENT_HANDLING_REVIEW.md](../ENVIRONMENT_HANDLING_REVIEW.md)** (Phase 0) and `api/config.py` for API loading details.

### 4.2 ETL pipeline (`etl_pipeline/`)

- **Driver:** `ETL_ENVIRONMENT` (must be `local`, `clinic`, or `test`).
- **Loader:** `etl_pipeline/etl_pipeline/config/providers.py` → `FileConfigProvider._load_environment_file()` loads `etl_pipeline/.env_<environment>` with `override=False` (OS env wins; Phase 0).
- **Creation:** `etl_pipeline/scripts/setup_environments.py` can create `.env_clinic` and `.env_test` from `etl_pipeline/.env_clinic.template` and `etl_pipeline/.env_test.template`.

**Note:** The ETL script in `environment_manager.ps1` (around line 587) once pointed to `etlPath\docs\env_*.template`; the real templates are under `etl_pipeline/` (e.g. `etl_pipeline/.env_clinic.template`), not `etl_pipeline/docs/`.

#### Same variables in `.env_local` and `.env_clinic` (confusion / cross-contam risk)

Development for clinic infra is often done locally before deploy, so **`.env_local` and `.env_clinic` intentionally share the same variable names** (e.g. `OPENDENTAL_SOURCE_HOST`, `POSTGRES_ANALYTICS_*`). Only **one** file is loaded per run: the loader reads `ETL_ENVIRONMENT` from the process, then loads `etl_pipeline/.env_<that>`. There is no merging of the two files.

**Possible confusion / cross-contamination:**

| Risk | What happens | Mitigation |
|------|----------------|------------|
| **Edit the wrong file** | You change `etl_pipeline/.env_clinic` but meant `.env_local` (or vice versa). Same variable names make it easy to not notice. | Clear “Used by” headers; keep clinic header with a **PHI / deploy** warning so the file’s purpose is obvious. |
| **Clinic file with local-only values** | `.env_clinic` is used on EC2 or in a real clinic context but still has `POSTGRES_ANALYTICS_HOST=localhost` (e.g. from copy-paste). Pipeline would then talk to localhost instead of RDS. | Before deploying or running clinic ETL in a non-local context, confirm `.env_clinic` has the correct hosts/credentials. Optional: loader could log a WARNING when `ETL_ENVIRONMENT=clinic` and `POSTGRES_ANALYTICS_HOST=localhost`. |
| **Think you’re on local but ETL_ENVIRONMENT=clinic** | Process has `ETL_ENVIRONMENT=clinic` (e.g. from a previous `etl-init`), so the loader loads `.env_clinic`. You assume you’re on “local” and run a destructive script. | Always set env via `etl-init` (or explicitly) so the correct file is loaded; startup log already logs “Loading environment from &lt;path&gt;” so you can confirm which file was used. |

**Recommendation:** Keep the shared variable set (same code paths, simpler), but (1) ensure both files have a clear “Used by” and a **warning in `.env_clinic`** that it may be used for real PHI / deploy, and (2) before any clinic deploy or non-local clinic run, spot-check that `etl_pipeline/.env_clinic` has the intended targets (not localhost unless that’s deliberate).

**Environment is always set via -init:** Use `scripts/environment_manager.ps1`: **dbt-init**, **etl-init**, or **api-init**. Those commands **prompt for environment (or target) first**; if you choose **cancel**, nothing is installed or activated and no env vars are set. Only after you select an environment do they install dependencies, activate the shell/venv, and load the env file. Child processes (Python ETL, API, dbt) then inherit the correct env. Do not rely on setting driver vars manually; run the appropriate init first.

### 4.3 dbt (environment manager only)

- **Driver:** `scripts/environment_manager.ps1` — `dbt-init -Target local|demo|clinic`.
- **Local:** Loads from `dbt_dental_models/.env_local` only (no project-root fallback).
- **Clinic:** Loads from `deployment_credentials.json` first; fallback `dbt_dental_models/.env_clinic` only (no project-root fallback).
- **Demo:** Loads from `deployment_credentials.json` only (no `.env_demo` file).

dbt itself uses `profiles.yml` and `DBT_PROFILES_DIR`; the env manager only sets **environment variables** (e.g. for connection params) in the shell.

### 4.4 Docker / Airflow

- **Root `.env`:** `docker-compose.yml` uses `env_file: - .env` for postgres, mysql, dbt services. Variables like `POSTGRES_*`, `AIRFLOW_FERNET_KEY`, etc. come from this file.
- **Creation:** Copy `.env.template` to `.env` and fill in values. Never commit `.env`.

### 4.5 Frontend (`frontend/`)

- Vite reads `.env`, `.env.local`, `.env.production` (and mode-specific files). See `frontend/README.md`.
- Variables must be prefixed with `VITE_` to be exposed to the client (e.g. `VITE_API_URL`, `VITE_API_KEY`, `VITE_IS_DEMO`).
- `scripts/environment_manager.ps1` can create/update `frontend/.env.local` for local dev; demo/clinic builds use env vars or `.env.production` as documented in the frontend README.

### 4.6 Consult audio pipe (`consult_audio_pipe/`)

- Single `.env` in `consult_audio_pipe/`, with `consult_audio_pipe/.env.template` as the committed template (API keys, optional local LLM URL).

### 4.7 Synthetic data generator

- Uses `etl_pipeline/synthetic_data_generator/.env_demo` (from `.env_demo.template`) for demo/synthetic data generation.

### 4.8 EC2 deployment

- **API on EC2:** systemd expects one file: `/opt/dbt_dental_clinic/api/.env`. Deploy clinic env with:
  ```powershell
  .\scripts\deployment\deploy_api_file.ps1 -FilePath "api\.env_api_clinic" -ClinicEnv
  ```
  This writes `api/.env` (single source of truth), retires any stale `api/.env_api_clinic` on the instance, and restarts the API service. See `ENVIRONMENT_HANDLING_REVIEW.md` (Phase 0).
- **Root on EC2:** `deployment_credentials.json.template` references `environment_file: ".env_ec2"` and `environment_file_path: "/opt/dbt_dental_clinic/.env"` for context; actual setup is done by your deployment scripts.

---

## 5. Environment variable drivers (summary)

| **Variable** | **Used by** | **Valid values** | **Effect** |
|--------------|-------------|-------------------|------------|
| `API_ENVIRONMENT` | API | `local`, `demo`, `clinic`, `test` | Which `api/.env_api_*` is loaded by `config.py` |
| `ETL_ENVIRONMENT` | ETL pipeline | `local`, `clinic`, `test` | Which `etl_pipeline/.env_*` is loaded |
| `DBT_TARGET` | dbt (profiles) | e.g. `local`, `clinic`, `demo` | Which target in `profiles.yml` is used; env manager loads vars by Target |
| (none for frontend) | Frontend | — | File name (e.g. `.env.production`) and build-time env vars |

---

## 6. Templates (committed) — checklist

Use these to create local/env-specific files; never commit the filled-in files. Paths below are the unique env file names.

- [ ] **Root:** `/.env.template` → copy to `/.env` for Docker/Airflow.
- [ ] **ETL:** `etl_pipeline/.env_local.template`, `etl_pipeline/.env_clinic.template`, `etl_pipeline/.env_test.template` → copy to `etl_pipeline/.env_local`, `.env_clinic`, `.env_test` as needed.
- [ ] **API test:** `api/.env_api_test.template` → copy to `api/.env_api_test` and fill in test DB credentials.
- [ ] **Consult audio:** `consult_audio_pipe/.env.template` → copy to `consult_audio_pipe/.env`.
- [ ] **Synthetic data:** `etl_pipeline/synthetic_data_generator/.env_demo.template` → copy to `etl_pipeline/synthetic_data_generator/.env_demo`.
- **API local/demo/clinic / Frontend:** No committed templates for `api/.env_api_local`, `api/.env_api_demo`, `api/.env_api_clinic`, or `frontend/.env`; create from READMEs or env manager prompts.

---

## 7. Inventory script

Run the optional inventory script to see which expected env files exist (without printing secrets):

```powershell
.\scripts\list_env_files.ps1
```

This lists known env file paths and reports Present / Missing. See script header for scope.

---

## 8. Related docs

- **API env in detail:** [docs/api/API_ENV_FILE_EXPLANATION.md](api/API_ENV_FILE_EXPLANATION.md)
- **EC2 env file rename/setup:** [docs/deployment/EC2_ENV_FILE_RENAME_GUIDE.md](deployment/EC2_ENV_FILE_RENAME_GUIDE.md), [docs/deployment/EC2_ENV_FILE_RENAME_COMMANDS.md](deployment/EC2_ENV_FILE_RENAME_COMMANDS.md)
- **Deployment credentials (env file refs):** `deployment_credentials.json.template`
- **Frontend env:** `frontend/README.md`
- **ETL setup:** `etl_pipeline/scripts/setup_environments.py`, `etl_pipeline/README.md`

---

## 9. Keeping this doc updated

- **Unique names:** Always refer to env files by path from repo root (e.g. `frontend/.env`, not just `.env`) so each file has a single, unambiguous name.
- When adding a **new env file** (or new component): add a row to the table in §1 with the unique path, document loader in §4, and add template to §6 if committed.
- When **renaming or removing** an env file: update this doc, `.gitignore`, and any script that references the path (e.g. `environment_manager.ps1`, `deploy_api_file.ps1`, `generate_api_key.ps1`).
- When changing **load order or driver** (e.g. `API_ENVIRONMENT` / `ETL_ENVIRONMENT`): update §4 and §5.
- When implementing **scalability improvements** (§3): tick items in the §3.5 checklist and update code/docs accordingly.
