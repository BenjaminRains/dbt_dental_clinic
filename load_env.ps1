# Load Local Environment Manager
# This script loads the project-specific environment manager

Write-Host "🔄 Loading local environment manager..." -ForegroundColor Cyan

# Load the environment manager script
. .\scripts\environment_manager.ps1

Write-Host "✅ Environment manager loaded!" -ForegroundColor Green
Write-Host "Available commands:" -ForegroundColor Yellow
Write-Host "  dbt-init       - Initialize dbt environment" -ForegroundColor Cyan
Write-Host "  etl-init       - Initialize ETL environment (interactive)" -ForegroundColor Magenta
Write-Host "  etl-env-status - Show ETL environment details" -ForegroundColor Yellow
Write-Host "  env-status     - Check environment status" -ForegroundColor Yellow
Write-Host "" 