# mdc CLI (Phase 4)

Monorepo developer CLI for API, ETL, and dbt workflows. Configuration is loaded only
via existing pydantic loaders in `api/settings.py` and `etl_pipeline/.../settings_v2.py`.

**Env file inventory:** [docs/deployment/ENVIRONMENT_FILES.md](../../docs/deployment/ENVIRONMENT_FILES.md)

## Install (editable)

From the repository root:

```powershell
pip install -e tools/mdc_cli
.\load_project.ps1
```

Then:

```powershell
mdc --help
mdc status
status
api-run
```

Use `.\load_project.ps1` for optional PowerShell aliases (`status`, `api-run`, etc.).

## Commands

### Validation

- `mdc status` — config paths, validation overview, venv discovery, **clinic credential sync vs Secrets Manager**, and **analytics data freshness** (use `--no-secrets-check` / `--no-freshness` to skip sections)
- `mdc api health --env <stage>` — pydantic settings load (no HTTP)
- `mdc api test-config --env <stage>`
- `mdc etl validate --env <stage> [--profile load|full]`
- `mdc dbt validate --env <stage>`

### Runtime (stateless, isolated child env)

- `mdc api run --env <stage>` — uvicorn; `--reload` default on `local` only; `--tunnel-db` for clinic + `mdc tunnel clinic-db`
- `mdc etl run|status|test-connections|exec --env <stage> [--profile full] [--tunnel-db] -- [args]`
- `mdc dbt run|test|docs --env <stage> [--tunnel-db] [--tunnel-port 5433] -- [args]`
- `mdc dbt invoke --env <stage> -- deps` — arbitrary dbt subcommands
- `mdc publish analytics --env clinic` — local marts → clinic RDS (`--ensure-tunnel` for unattended/Airflow; live Secrets Manager password; see [docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md](../../docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md))
- `mdc secrets pull clinic` — read RDS master secret (`rds!db-...`), sync password into `api/.env_api_clinic` (see [CLINIC_ANALYTICS_WORKFLOW.md](../../docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md))

### Frontend (workspace apps)

Two deployable apps under `frontend/` (`@mdc/portfolio`, `@mdc/clinic`). See [frontend/README.md](../../frontend/README.md) and [docs/frontend/FRONTEND_SPLIT_PLAN.md](../../docs/frontend/FRONTEND_SPLIT_PLAN.md).

- `mdc frontend dev [--app portfolio|clinic]` — Vite for one app (writes `frontend/apps/<app>/.env.local`)
- `mdc frontend status` — workspace paths, S3/CloudFront/API key resolution, last local deploy hint
- `mdc deploy frontend --target demo|portfolio|clinic` — typecheck + build matching workspace, SPA fallbacks, CloudFront invalidation (`portfolio` is an alias for the `demo` stage / `@mdc/portfolio`)
- `mdc deploy dbt-docs [--env local] [--skip-generate]` — upload `dbt_dental_models/target` to `s3://…/dbt-docs/`

Nomenclature: use **portfolio/clinic** for Vite apps (`--app`); use **demo/clinic** for env stages; frontend deploy accepts both `demo` and `portfolio`.
### Consult audio (Phase 5.4)

- `mdc consult-audio install` — create `consult_audio_pipe/venv` and install requirements
- `mdc consult-audio validate` — venv, `.env` API keys, ffmpeg warning
- `mdc consult-audio pipeline run|status|validate|cleanup` — `consult_audio_pipe.pipeline`
- `mdc consult-audio run -- <cmd>` — arbitrary venv subprocess (cwd `consult_audio_pipe/`)
- `mdc consult-audio analyze` / `tokens` — `scripts/llm_analysis_integration.py` helpers

### Infrastructure wrappers

- `mdc tunnel clinic-db|demo-db|rds` — SSM port forward (Python; no PowerShell bridge)
- `mdc ssm status` — AWS CLI, plugin, instance IDs from `deployment_credentials.json`
- `mdc ssm connect api|clinic-api|demo-db` — interactive SSM shell
- `mdc deploy api --env clinic` — copies `api/.env_api_clinic` to EC2 `api/.env`, restarts **`dental-clinic-api-clinic`** systemd unit (via `deploy_api_file.ps1`), `/health/db` check

### PowerShell aliases (Phase 4.5)

`scripts/mdc_aliases.ps1` (default via `load_project.ps1`) mirrors the current `mdc` surface:

| Area | Aliases |
|------|---------|
| Status / secrets | `status`, `env-status`, `status-clinic-rds`, `secrets-pull-clinic` |
| API | `api-run`, `api-test`, `api-health` |
| ETL | `etl-run`, `etl-validate`, `etl-test`, `etl-status`, `etl-schema`, `etl-exec` |
| dbt | `dbt` (run/test/docs/deps), `dbt-validate` |
| Frontend / deploy | `frontend-dev`, `frontend-status`, `demo-frontend-deploy`, `clinic-frontend-deploy`, `dbt-docs-deploy`, `deploy-api`, `publish-analytics` |
| Tunnels / SSM | `tunnel-clinic-db`, `tunnel-demo-db`, `tunnel-rds`, `tunnel-close`, `ssm-status`, `ssm-connect-api`, `ssm-connect-clinic-api`, `ssm-connect-demo-db` |
| Consult audio / Airflow | `consult-audio-install`, `consult-audio-validate`, `consult-audio-run`, `airflow-init`, `airflow-start-*`, `airflow-logs` |

**ETL alias defaults** (when stage/profile omitted):

| Alias | Default stage | Default profile |
|-------|---------------|-----------------|
| `etl-validate` | `local` | `load` for local, `full` otherwise |
| `etl-run`, `etl-test`, `etl-schema`, `etl-exec` | `clinic` | `full` |
| `etl-status` | `clinic` | `full` (use `etl-status -Env local` for local warehouse) |

Stages: `local`, `clinic`, `test`, `demo` (API/dbt). Use `clinic` for live clinic context.

Clinic portal local stack: `api-run` (needs `CLINIC_PORTAL_SESSION_SECRET` or `CLINIC_API_KEY`) then `frontend-dev --app clinic`.

## CI

GitHub Actions workflow `.github/workflows/mdc_cli.yml` runs on PRs and pushes to `main`
when `tools/mdc_cli` or API/ETL settings loaders change:

```bash
pip install -e "./tools/mdc_cli[dev]"
pytest   # from tools/mdc_cli
mdc status --env local
```

No `load_project.ps1`, AWS credentials, or runtime `.env` files required — validation rows
may show `fail` when env files are absent; the command must exit 0.
