# mdc CLI (Phase 4)

Monorepo developer CLI for API, ETL, and dbt workflows. Configuration is loaded only
via existing pydantic loaders in `api/settings.py` and `etl_pipeline/.../settings_v2.py`.

## Install (editable)

From the repository root:

```powershell
pip install -e tools/mdc_cli
```

Then:

```powershell
mdc --help
mdc status
mdc status --env local
```

Or without installing:

```powershell
python -m mdc_cli --help
```

## Commands

### Validation (Phase 4.2)

- `mdc status` — config paths, validation overview, venv discovery
- `mdc api test-config --env <stage>` — validate API pydantic settings
- `mdc api health --env <stage>` — config health (settings load)
- `mdc etl validate --env <stage> [--profile load|full]` — validate ETL settings
- `mdc dbt validate --env <stage>` — validate dbt connection env (local/clinic/demo)

### Runtime (Phase 4.3)

Stateless runs inject validated env into an **isolated child process** (parent shell vars do not leak):

- `mdc api run --env <stage> [--host H] [--port P] [--reload/--no-reload]` — uvicorn (`--reload` default on `local` only)
- `mdc etl run --env <stage> [--profile full] -- [etl cli args]`
- `mdc etl test-connections --env <stage> [--profile full] -- [args]`
- `mdc dbt run|test|docs --env <stage> -- [dbt args]`
- `mdc dbt invoke --env <stage> -- deps` — arbitrary dbt subcommands

PowerShell aliases `api-run`, `etl-run`, `etl-test`, and `dbt` delegate to `mdc` (Phase 4.4).

Stages are dev/test targets only: `local`, `clinic`, `test`, and `demo` (API/dbt).
Use `clinic` for the live clinic deployment context — not a separate `production` stage name.

## Stub

- `mdc tunnel *` — Phase 4.5+ wrappers to existing deployment scripts
