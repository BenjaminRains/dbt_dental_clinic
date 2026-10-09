# Repository maintenance

Housekeeping for this laptop checkout. GitHub security alerts, Dependabot, and Bugbot stay as they are. This note is the local cleanup cadence.

## Weekly

Log prune is `prune_logs.ps1`. Retention is by file age. `tables.yml` backups under `etl_pipeline/logs/schema_analysis/backups/` keep 90 days **and** the newest 20 files. `tables.yml` is gitignored, so those copies are the local history. Do not shorten that rule.

```powershell
.\scripts\maintenance\prune_logs.ps1 -DryRun
.\scripts\maintenance\prune_logs.ps1                 # Recycle Bin
.\scripts\maintenance\prune_logs.ps1 -Permanent      # scheduled run
```

Task Scheduler job **MDC Log Prune** runs the permanent form weekly. The 2026-10-08 run used the Recycle Bin (866 files, about 352 MB). Empty the bin once you are sure nothing from that run is needed.

This script only sees logs in this checkout. API or dbt logs on the clinic EC2 host need the same age rule on that machine.

## Local config backup

GitHub is the offsite copy of tracked code. Untracked config is copied by `scripts/utils/backup_dbt_dental_clinic.ps1` into dated folders under `C:\backup\dbt_dental_clinic_config` (newest 5 kept):

- `etl_pipeline/etl_pipeline/config/tables.yml`
- `deployment_credentials.json`
- root `.env` and `.env_ec2`
- component `.env` files (`api`, `etl_pipeline`, `dbt_dental_models`, consult-audio, frontend)

The script does not copy virtualenvs, logs, or `.git`. It does not delete the old full-tree folder `C:\backup\dbt_dental_clinic`. That copy is about 9.5 GB and includes secrets and virtualenvs. Remove it manually after a dated config snapshot exists and you do not need the full tree.

`.env_ec2` is the EC2 deploy source. It is not a duplicate of root `.env`.

Confirm separately, outside git: RDS automated backups for the analytics database, and the clinic's own OpenDental MySQL backups.

## Monthly

- Push branches you intend to keep.
- Merge or close Dependabot PRs. Clear the Bugbot and GitHub security inbox.
- New files at the repo root should be entry points (`README`, `SECURITY`, compose, `load_project.ps1`). Everything else goes in an existing folder.

## Quarterly

- Delete archived scripts that have sat unused for two quarters (`scripts/archive/README.md`).
- Banner or move superseded plans into the existing `docs/*/archive/` folders.
- Confirm RDS and OpenDental backups still match what you expect.

## Left in place on purpose

| Item | Why |
|------|-----|
| `scripts/ec2/fix_ec2_*.ps1` | `fix_ec2_dbt_setup.ps1` still calls the path and permissions scripts |
| `etl_pipeline/scripts/investigate_*.py` | PK-level phantom checks in `docs/etl/ETL_SYNC_SEMANTICS.md` |
| `method_tracker.py`, `analyze_connection_usage.py`, `test_root_connections.py` | Imported or documented as current tools |
| `scripts/utils/monitor_workflow.ps1` | Reusable sampler. Output dir `workload_monitor_logs/` is gitignored |
| `databricks_dental_lakehouse/` | Parked future project. See its README |
| `archive/analysis`, `archive/stakeholders` | Historical. Not inputs to the pipeline |
