# Load Project — thin mdc aliases (Phase 5.5 default).
# Run from project root: .\load_project.ps1
# One-time: pip install -e tools/mdc_cli
#
# Loads scripts/mdc_aliases.ps1 and prints the current mdc quickstart
# (api/etl/dbt/frontend/deploy/publish/tunnel/ssm/airflow/consult-audio).
# Full tree: mdc --help  |  docs: tools/mdc_cli/README.md

param(
    [switch]$Legacy
)

$scriptPath = Join-Path (Get-Location) "scripts"

if ($Legacy) {
    Write-Host "Deprecated: -Legacy loads the archived environment manager (Phase 5.5)." -ForegroundColor Yellow
    Write-Host "Daily workflow uses mdc only. See tools/mdc_cli/README.md" -ForegroundColor Yellow
    $archived = Join-Path $scriptPath "archive\environment_manager.ps1"
    if (Test-Path $archived) {
        Write-Host "Loading archive for reference: $archived" -ForegroundColor DarkGray
        . $archived
    } else {
        Write-Host "Archive not found: $archived" -ForegroundColor Red
    }
    return
}

$aliasesPath = Join-Path $scriptPath "mdc_aliases.ps1"
if (-not (Test-Path $aliasesPath)) {
    Write-Host "mdc aliases not found: $aliasesPath" -ForegroundColor Red
    Write-Host "Run this script from the project root (dbt_dental_clinic)." -ForegroundColor Yellow
    return
}

. $aliasesPath
Write-Host "mdc aliases loaded. Ensure mdc is installed: pip install -e tools/mdc_cli" -ForegroundColor Green
Write-Host "Tip: frontend-dev --app clinic  |  api-run  |  mdc --help" -ForegroundColor DarkGray
