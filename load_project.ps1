# Load Project (Environment Manager)
# Default: thin mdc aliases (Phase 4.5/4.6). Use -Legacy for full environment_manager.ps1.
# Run from project root: .\load_project.ps1

param(
    [switch]$Legacy
)

$scriptPath = Join-Path (Get-Location) "scripts"

if ($Legacy) {
    $envManager = Join-Path $scriptPath "environment_manager.ps1"
    if (-not (Test-Path $envManager)) {
        Write-Host "Environment manager not found: $envManager" -ForegroundColor Red
        Write-Host "Run this script from the project root (dbt_dental_clinic)." -ForegroundColor Yellow
        return
    }
    Write-Host "Loading full environment manager (deploy, SSM, frontend, legacy *-init)..." -ForegroundColor Cyan
    . $envManager
    Write-Host "Environment manager loaded (-Legacy)." -ForegroundColor Green
    return
}

$aliasesPath = Join-Path $scriptPath "mdc_aliases.ps1"
if (-not (Test-Path $aliasesPath)) {
    Write-Host "mdc aliases not found: $aliasesPath" -ForegroundColor Red
    Write-Host "Run this script from the project root (dbt_dental_clinic)." -ForegroundColor Yellow
    return
}

. $aliasesPath
Write-Host "mdc aliases loaded (default). Use .\load_project.ps1 -Legacy for deploy/SSM/frontend." -ForegroundColor Green
