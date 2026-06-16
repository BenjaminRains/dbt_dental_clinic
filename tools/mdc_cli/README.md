# mdc CLI (Phase 4)

Monorepo developer CLI for API, ETL, and dbt workflows. Configuration is loaded only
via existing pydantic loaders in `api/settings.py` and `etl_pipeline/.../settings_v2.py`.

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

Use `.\load_project.ps1 -Legacy` only for unmigrated monolith menus.

## Commands

### Validation

- `mdc status` — config paths, validation overview, venv discovery
- `mdc api test-config --env <stage>`
- `mdc etl validate --env <stage> [--profile load|full]`
- `mdc dbt validate --env <stage>`

### Runtime (stateless, isolated child env)

- `mdc api run --env <stage>` — uvicorn; `--reload` default on `local` only
- `mdc etl run|status|test-connections --env <stage> [--profile full] -- [args]`
- `mdc dbt run|test|docs --env <stage> -- [args]`
- `mdc dbt invoke --env <stage> -- deps` — arbitrary dbt subcommands

### Frontend (Phase 5.3)

- `mdc frontend dev` — local Vite dev server (writes `frontend/.env.local`)
- `mdc frontend status` — demo/clinic S3/CloudFront/API key resolution
- `mdc deploy frontend --target demo|clinic` — build + S3 sync + CloudFront invalidation
- `mdc deploy dbt-docs [--env local] [--skip-generate]` — upload `dbt_dental_models/target` to `s3://…/dbt-docs/`

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
- `mdc deploy api --env clinic` — copies `api/.env_api_clinic` to EC2 `api/.env`, restarts **`dental-clinic-api`** systemd unit, `/health/db` check

### PowerShell aliases (Phase 4.5)

`scripts/mdc_aliases.ps1` (default via `load_project.ps1`): `status`, `api-run`, `api-test`,
`etl-run`, `etl-validate`, `etl-test`, `etl-status`, `env-status`, `ssm-connect-clinic-api`,
`ssm-connect-api`, `ssm-connect-demo-db`.

Stages: `local`, `clinic`, `test`, `demo` (API/dbt). Use `clinic` for live clinic context.

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
