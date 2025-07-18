# Project-Specific PowerShell Profile
# This profile loads the environment manager for the current project

# Get the current project directory
$projectDir = Get-Location

# Check if this is a dbt_dental_clinic project
if (Test-Path "$projectDir\scripts\environment_manager.ps1") {
    Write-Host "🔄 Loading dbt_dental_clinic environment manager..." -ForegroundColor Cyan
    
    # Load the environment manager
    . .\scripts\environment_manager.ps1
    
    Write-Host "✅ Environment manager loaded!" -ForegroundColor Green
    Write-Host "Available commands:" -ForegroundColor Yellow
    Write-Host "  dbt-init       - Initialize dbt environment" -ForegroundColor Cyan
    Write-Host "  etl-init       - Initialize ETL environment (interactive)" -ForegroundColor Magenta
    Write-Host "  etl-env-status - Show ETL environment details" -ForegroundColor Yellow
    Write-Host "  env-status     - Check environment status" -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Host "ℹ️  No environment manager found in this project" -ForegroundColor Gray
} 