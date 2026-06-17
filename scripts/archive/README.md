# Archived scripts

Legacy PowerShell orchestration removed from the default developer path in **Phase 5.5**.

| File | Replacement |
|------|-------------|
| `environment_manager.ps1` | `mdc` CLI (`pip install -e tools/mdc_cli`) + `load_project.ps1` aliases |
| `mdc_run_ps_function.ps1` | Removed — `mdc` implements commands in Python |
| `mdc_run_ssm_tunnel.ps1` | `mdc tunnel` / `mdc_cli/ssm.py` |

Do not dot-source these for daily work. Kept for reference and emergency rollback only.
