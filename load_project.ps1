# Load Project (Environment Manager)
# Dot-sources scripts\environment_manager.ps1 to register project commands and aliases.
# Run from project root: .\load_project.ps1

Write-Host "Loading environment manager..." -ForegroundColor Cyan

$scriptPath = Join-Path (Get-Location) "scripts\environment_manager.ps1"
if (-not (Test-Path $scriptPath)) {
    Write-Host "Environment manager not found: $scriptPath" -ForegroundColor Red
    Write-Host "Run this script from the project root (dbt_dental_clinic)." -ForegroundColor Yellow
    return
}

. $scriptPath

Write-Host "Environment manager loaded." -ForegroundColor Green
