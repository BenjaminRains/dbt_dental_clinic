# Migration: environment_manager.ps1 → Docker-Based Local Development (ARCHIVED)

**Status:** Archived 2026-10-19. Superseded by the **`mdc` CLI** (isolated child-process env per stage). Do not extend this checklist.  
**Day-to-day reference:** [../ENVIRONMENT_FILES.md](../ENVIRONMENT_FILES.md).  
**Daily commands:** [../../../tools/mdc_cli/README.md](../../../tools/mdc_cli/README.md).  
**Legacy script:** `scripts/archive/environment_manager.ps1` (stub at `scripts/environment_manager.ps1`).

Docker Compose remains available for the **Airflow sandbox** only (`/.env` + root `docker-compose.yml`). A full local-stack replacement of `mdc` was never adopted and is not planned.

---

**Original goal (historical):** Replace the monolithic PowerShell environment manager with Docker Compose (and optional devcontainer) so local dev is reproducible, envs (local/demo/clinic/test) are separated by config, and the script can be deprecated or reduced to thin wrappers.

**Out of scope (remain host/script or manual):** AWS SSM port-forwarding, EC2 connect, frontend/API deployment to S3/CloudFront, and production RDS access. Those stay as separate workflows (script or docs).

---

## Current State Summary

### What the script does (3,164 lines)

| Area | Functions | Env sources | Notes |
|------|------------|-------------|--------|
| **dbt** | `Initialize-DBTEnvironment`, `Stop-DBTEnvironment`, `Invoke-DBT`, etc. | `.env_local` / `.env_clinic` (dbt_dental_models), `deployment_credentials.json` (demo) | Pipenv in `dbt_dental_models`, mutual exclusion with ETL/API/consult-audio |
| **ETL** | `Initialize-ETLEnvironment`, `Stop-ETLEnvironment`, `Invoke-ETL`, etc. | `.env_local`, `.env_clinic`, `.env_test` (etl_pipeline), `deployment_credentials.json` (demo) | Pipenv in `etl_pipeline`, demo = synthetic data only |
| **API** | `Initialize-APIEnvironment`, `Stop-APIEnvironment`, `Start-APIServer`, etc. | `.env_api_local`, `.env_api_demo`, `.env_api_clinic`, `.env_api_test` (api/) | venv in api/, runs uvicorn |
| **Consult audio** | `Initialize-ConsultAudioEnvironment`, `Stop-ConsultAudioEnvironment` | `consult_audio_pipe/.env` | venv in consult_audio_pipe |
| **Frontend** | `Start-FrontendDev`, `Deploy-Frontend`, `Deploy-ClinicFrontend`, `Get-FrontendStatus` | `.env.local` (generated), `deployment_credentials.json` | npm run dev; deploy = S3/CloudFront |
| **AWS SSM** | `Initialize-AWSSSMEnvironment`, `Connect-SSM*`, `Start-SSMPortForward*` | `deployment_credentials.json` | Port forwarding, EC2 sessions; host-only |
| **Status/help** | `Get-EnvironmentStatus`, `Show-*Help`, `prompt` override | — | UX only |

### Existing Docker assets

- **docker-compose.yml** (repo root): Airflow (webserver, scheduler), dbt service, postgres, mysql. Uses root `.env`. dbt service expects Pipfile at root (may not match `dbt_dental_models`).
- **api/Dockerfile**: Python 3.11, uvicorn; no compose service yet for local API.
- **Dockerfile** (root): dbt + Pipfile (legacy layout).
- **Dockerfile.airflow**: Airflow + ETL/dbt mounts.

### Nomenclature (keep consistent)

- **local** = localhost dev (API :8000, frontend :3000, local DBs).
- **demo** = portfolio/demo (opendental_demo, synthetic data).
- **clinic** = production clinic (opendental_analytics, PHI).
- **test** = test DBs (test_opendental, test_opendental_analytics), no PHI.

---

## Phase 1: Audit and Env-File Standardization

**Objective:** Single source of truth for env vars per environment; no behavior change yet.

### 1.1 Inventory env vars per workflow

- [ ] **dbt**  
  List every variable set by the script for local/demo/clinic (e.g. `DBT_PROFILES_DIR`, `DBT_TARGET`, `POSTGRES_ANALYTICS_*`, `DEMO_POSTGRES_*`).  
  Doc: `docs/deployment/ENV_VARS_DBT.md` (or section in this doc).

- [ ] **ETL**  
  List vars from `.env_local`, `.env_clinic`, `.env_test`, and from `deployment_credentials.json` for demo.  
  Doc: `docs/deployment/ENV_VARS_ETL.md`.

- [ ] **API**  
  List vars from `.env_api_local`, `.env_api_demo`, `.env_api_clinic`, `.env_api_test`.  
  Doc: `docs/deployment/ENV_VARS_API.md`.

- [ ] **Frontend**  
  List `VITE_*` and any other vars used by `frontend-dev` and deploy.  
  Doc: `docs/deployment/ENV_VARS_FRONTEND.md`.

### 1.2 Standardize .env layout for Docker

- [ ] **Root `.env`**  
  Only shared, non-secret defaults (e.g. `POSTGRES_USER`, `POSTGRES_DATABASE`, ports).  
  Keep secrets out; use `env_file` or compose overrides for secrets.

- [ ] **Per-environment override files** (no secrets in repo):  
  - `.env.local` – local dev (DB host = postgres/mysql service names or localhost).  
  - `.env.demo` – demo (when using local demo stack; secrets from `deployment_credentials.json` or env).  
  - `.env.clinic` – clinic (often RDS; secrets from deployment_credentials or AWS).  
  - `.env.test` – test DBs only.

- [ ] **Templates**  
  Ensure `.env*.template` exist for every `.env*` that may contain secrets; document in README.

### 1.3 Document which script commands must be replicated

- [ ] List “day-one” commands: e.g. start DBs, run API, run dbt, run ETL, run frontend dev.
- [ ] List “occasional” commands: e.g. dbt run, etl validate, api-test.
- [ ] List “out of scope for Docker”: SSM init, port-forward, deploy-frontend, deploy-clinic-frontend (keep in script or runbooks).

---

## Phase 2: Docker Compose – Local Stack Only

**Objective:** One `docker compose up` (or a small set of commands) brings up local databases + API + (optionally) dbt/ETL runners. Frontend can stay host-run initially.

### 2.1 Compose project and env strategy

- [ ] **Single compose file for local**  
  - File: `docker-compose.yml` (extend existing) or `docker-compose.local.yml` (override).  
  - Use `env_file: [.env, .env.local]` and `environment:` for service-specific overrides.  
  - Do not bundle demo/clinic RDS in this file; those are “external” or separate override files later.

- [ ] **Services to add/align**  
  - **postgres** (existing): local analytics DB; expose port only for local dev (e.g. 5432).  
  - **mysql** (existing): OpenDental source; expose port only if needed (e.g. 3306).  
  - **api**: Add service using `api/Dockerfile`; `env_file: api/.env_api_local` or equivalent; `depends_on: postgres`; port 8000.  
  - **dbt**: Fix context to `dbt_dental_models` (or a Dockerfile that lives there); mount project; `depends_on: postgres` (and mysql if needed); command overridable (e.g. `dbt run --target local`).  
  - **etl**: New service; Dockerfile in `etl_pipeline/`; `env_file: etl_pipeline/.env_local`; `depends_on: postgres, mysql`; no default long-running command (use `docker compose run etl ...` for CLI).

- [ ] **Volumes**  
  - Named volumes for postgres/mysql data (existing).  
  - No host mounts for secrets; inject via env_file or env.

### 2.2 Dockerfiles

- [ ] **dbt**  
  - Prefer `dbt_dental_models/Dockerfile` (or root Dockerfile with correct context) so `Pipfile`/`dbt_project.yml` live in one place.  
  - Install deps with pipenv or pip; set `DBT_PROFILES_DIR` and `DBT_TARGET=local` in compose.

- [ ] **ETL**  
  - Add `etl_pipeline/Dockerfile` (Python, Pipfile or requirements); default command can be `sleep infinity` or leave empty; use `docker compose run etl python -m etl_pipeline.cli.main ...` for etl/validate/run.

- [ ] **API**  
  - Use existing `api/Dockerfile`; ensure it reads env (e.g. `POSTGRES_ANALYTICS_*`) from environment.

### 2.3 Profiles or override files (optional)

- [ ] If you want “start only DBs” vs “DBs + API”: use **profiles** (e.g. `docker compose --profile api up`) or separate override files (`docker-compose.override.yml` for API + dbt + etl).

### 2.4 Frontend (Phase 2)

- [ ] **Keep frontend on host** for now: `npm run dev` in `frontend/`, with `VITE_API_URL=http://localhost:8000` and `.env.local` generated or documented.  
- [ ] Optional: add a `frontend` service later that runs `npm run dev` and exposes 3000; ensure CORS and API URL point to `http://api:8000` or `http://localhost:8000` as appropriate.

### 2.5 Validation

- [ ] From repo root: `docker compose up -d postgres mysql` then `docker compose up -d api` (or `docker compose --profile api up -d`).  
- [ ] Verify API health: `curl http://localhost:8000/health` (or equivalent).  
- [ ] Run dbt: `docker compose run --rm dbt dbt run --target local`.  
- [ ] Run ETL: `docker compose run --rm etl python -m etl_pipeline.cli.main status` (and validate if applicable).  
- [ ] Document in README: “Local development with Docker” and “Without Docker (legacy script)” so both paths are valid during migration.

---

## Phase 3: Demo / Clinic / Test Separation in Compose

**Objective:** Same compose stack, different env files and (for clinic) optional external RDS; no PHI in repo.

### 3.1 Override files per environment

- [ ] **Local** (default): already done in Phase 2; uses `.env` + `.env.local`; postgres/mysql in Docker.

- [ ] **Test**  
  - `docker-compose.test.yml`: same services, different env_file (e.g. `.env.test`) and/or different DB names (e.g. `test_opendental_analytics`).  
  - Use separate volumes for test DBs if you run test DBs in containers (e.g. `postgres_test`, `mysql_test`) or point to host/port-forwarded test instances via env.

- [ ] **Demo**  
  - For **local demo** (synthetic data in Docker): `docker-compose.demo.yml` that uses `opendental_demo` DB, env_file `.env.demo`, and optionally a seed/synthetic data container or init script.  
  - For **remote demo** (RDS opendental_demo): do not add RDS to compose; use script/docs to start SSM port-forward, then point API/dbt/ETL at localhost with forwarded port via `.env.demo` (host-run or a single “runner” container that uses host network or extra host).

- [ ] **Clinic**  
  - Compose does not start clinic RDS.  
  - Provide `.env.clinic` (or deployment_credentials-driven) for use when running API/dbt/ETL **on host** against RDS (e.g. after port-forward or VPN).  
  - Optional: a “clinic” profile that only sets env_file to `.env.clinic` and expects `POSTGRES_ANALYTICS_HOST` to be set (e.g. RDS endpoint or localhost with port-forward).

### 3.2 Document run matrix

- [ ] Table: Environment × What runs where (Docker vs host) × How to run.  
  Example:

  | Environment | Postgres/MySQL | API | dbt | ETL | Frontend |
  |-------------|----------------|-----|-----|-----|----------|
  | local       | Docker         | Docker | Docker run | Docker run | Host (npm) |
  | test        | Docker (test_* DBs) or host | Docker | Docker run | Docker run | Host |
  | demo (local) | Docker (opendental_demo) | Docker | Docker run | Docker run | Host |
  | demo (remote) | RDS (port-forward) | Host or EC2 | Host | Host | Host |
  | clinic      | RDS (port-forward) | Host or EC2 | Host | Host | Host |

### 3.3 Validation

- [ ] Switch env: `docker compose -f docker-compose.yml -f docker-compose.test.yml up -d` (or equivalent) and run one dbt and one ETL command.  
- [ ] Ensure no cross-env leakage (e.g. test never points at clinic DB).

---

## Phase 4: Optional Devcontainer

**Objective:** Contributors open repo in container; same tooling everywhere; script optional.

### 4.1 Add .devcontainer

- [ ] **.devcontainer/devcontainer.json**  
  - Use Docker Compose as the dev container source: `"dockerComposeFile": "docker-compose.yml"`, define which service is the “main” dev container (e.g. a new `dev` service that has Python, Node, dbt, ETL CLI, AWS CLI).  
  - Or: single Dockerfile that installs Python, Node, dbt, ETL deps, and use `"runArgs": ["--network", "host"]` or compose network so the dev container can reach `postgres`, `mysql`, `api` from the same compose project.

- [ ] **.devcontainer/docker-compose.yml** (if separate from root)  
  - Include postgres, mysql, api (and optionally dbt/etl as services); dev container depends_on them.  
  - Mount repo into dev container; set env_file to `.env.local`.

### 4.2 Post-create / post-start

- [ ] `pip install` (or pipenv install) for dbt and ETL in dev container.  
- [ ] `npm install` for frontend if dev container is used for frontend too.  
- [ ] Optional: `dbt deps`, `dbt compile`.

### 4.3 Docs

- [ ] README section: “Development with Dev Container” and “Development with Docker Compose only” and “Legacy: environment_manager.ps1”.

---

## Phase 5: Deprecate or Slim Down environment_manager.ps1

**Objective:** No one depends on the script for local DB + API + dbt + ETL; script only for deploy and SSM if desired.

### 5.1 Redirect local dev to Docker

- [ ] README and CONTRIBUTING: “To run the stack locally, use Docker Compose (see …). The PowerShell script is deprecated for local env setup.”
- [ ] Optional: add a minimal `scripts/start-local.ps1` that runs `docker compose up -d` and prints URLs/next steps (thin wrapper).

### 5.2 Keep in script (or move to dedicated scripts)

- [ ] **AWS SSM**: `aws-ssm-init`, `ssm-port-forward-*`, `ssm-connect-api` – document that these are for remote demo/clinic access and stay host-run.
- [ ] **Deploy**: `Deploy-Frontend`, `Deploy-ClinicFrontend`, `Deploy-DBTDocs` – keep as script or move to `scripts/deploy-*.ps1` and call from script for backward compatibility.
- [ ] **Status**: `Get-EnvironmentStatus` can be replaced by `docker compose ps` and a one-liner for “which env file is active”; or keep a tiny status helper.

### 5.3 Remove or archive

- [ ] **Initialize/Stop for dbt, ETL, API, ConsultAudio** – remove once Docker path is default; or keep as no-op that prints “Use Docker Compose; see README.”
- [ ] **Command wrappers** (Invoke-DBT, Invoke-ETL, Start-APIServer, etc.) – remove or replace with wrappers that call `docker compose run ...` so aliases still work for those who source the script.

### 5.4 Cleanup

- [ ] Delete or archive large blocks of the script; leave a short “deprecated” notice and links to Docker/README.  
- [ ] Update any CI that relied on the script to use `docker compose` instead.

---

## Phase 6: Consult Audio and Edge Cases

### 6.1 Consult audio pipeline

- [ ] If used rarely: leave on host with its own venv; document in README.  
- [ ] If used often: add `consult_audio_pipe/Dockerfile` and a compose service (or devcontainer step); env from `consult_audio_pipe/.env`.

### 6.2 Multiple envs at once

- [ ] If you need “local API + demo DB” at once: use two compose projects (different project name: `docker compose -p demo up`) or different ports (e.g. API demo on 8001, local on 8000) and document.

### 6.3 Secrets and deployment_credentials.json

- [ ] Never put `deployment_credentials.json` or real secrets in images or in repo.  
- [ ] Compose: use `env_file` pointing to a file that is gitignored, or inject at runtime (e.g. AWS Secrets Manager, or copy from secure location before `docker compose up`).  
- [ ] Document: “For demo/clinic, copy credentials to .env.demo / .env.clinic and do not commit.”

---

## Success Criteria

- [ ] One-command local stack: `docker compose up -d` (or with one profile) brings up postgres, mysql, API; dbt and ETL runnable via `docker compose run`.
- [ ] Env separation: local / test / demo (local) / clinic clearly separated by env_file and docs; no PHI in repo.
- [ ] README describes both “Docker” and “Legacy script” paths; script is clearly deprecated for env activation.
- [ ] SSM and deployment remain documented and scripted outside the “env activation” concern.

---

## File and Doc Checklist

| Deliverable | Phase |
|-------------|--------|
| `docs/deployment/ENV_VARS_*.md` (or single doc) | 1 |
| Root `.env` + `.env.local` / `.env.test` / `.env.demo` / `.env.clinic` (templates) | 1, 3 |
| `docker-compose.yml` (or `.local`) with postgres, mysql, api, dbt, etl | 2 |
| `dbt_dental_models/Dockerfile` or corrected root dbt Dockerfile | 2 |
| `etl_pipeline/Dockerfile` | 2 |
| `docker-compose.test.yml` / `docker-compose.demo.yml` (optional) | 3 |
| `.devcontainer/devcontainer.json` (and optional compose) | 4 |
| README “Local development” and “Deprecation of environment_manager.ps1” | 5 |
| Slimmed or deprecated `scripts/environment_manager.ps1` | 5 |

---

## References

- Existing: `docker-compose.yml`, `api/Dockerfile`, `Dockerfile`, `Dockerfile.airflow`
- Env files: `etl_pipeline/.env_*`, `api/.env_api_*`, `dbt_dental_models/.env_*`, `deployment_credentials.json`
- Nomenclature: script header (local / demo / clinic); `docs/deployment/CLINIC_DEPLOYMENT_PHASE2_ACTION_PLAN.md`
