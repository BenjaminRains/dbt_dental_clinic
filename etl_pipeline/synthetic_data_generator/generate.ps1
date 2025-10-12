#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Safe wrapper for synthetic data generator
    
.DESCRIPTION
    Ensures the generator always connects to opendental_demo database,
    preventing accidental writes to production or test databases.
    
.PARAMETER Patients
    Number of patients to generate (default: 100)
    
.PARAMETER DbHost
    Database host (default: localhost)
    
.PARAMETER DbUser
    Database user (default: postgres)
    
.PARAMETER DbPassword
    Database password (required)
    
.PARAMETER StartDate
    Start date for data generation (default: 2023-01-01)

.EXAMPLE
    .\generate.ps1 -Patients 100 -DbPassword "mypassword"
    
.EXAMPLE
    .\generate.ps1 -Patients 5000 -DbUser "analytics_user" -DbPassword "mypassword"
#>

param(
    [Parameter(Mandatory=$false)]
    [int]$Patients,
    
    [Parameter(Mandatory=$false)]
    [string]$DbHost,
    
    [Parameter(Mandatory=$false)]
    [string]$DbUser,
    
    [Parameter(Mandatory=$false)]
    [string]$DbPassword,
    
    [Parameter(Mandatory=$false)]
    [string]$StartDate
)

# Load .env_demo if exists (provides defaults)
$envFile = Join-Path $PSScriptRoot ".env_demo"
if (Test-Path $envFile) {
    Write-Host "📄 Loading configuration from .env_demo" -ForegroundColor Gray
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.*)$' -and $_ -notmatch '^\s*#') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            Set-Variable -Name $name -Value $value -Scope Script
        }
    }
}

# Apply defaults from .env_demo or fallback to hardcoded defaults
if (-not $Patients) { $Patients = if ($DEMO_NUM_PATIENTS) { [int]$DEMO_NUM_PATIENTS } else { 100 } }
if (-not $DbHost) { $DbHost = if ($DEMO_POSTGRES_HOST) { $DEMO_POSTGRES_HOST } else { "localhost" } }
if (-not $DbUser) { $DbUser = if ($DEMO_POSTGRES_USER) { $DEMO_POSTGRES_USER } else { "postgres" } }
if (-not $DbPassword) { $DbPassword = if ($DEMO_POSTGRES_PASSWORD) { $DEMO_POSTGRES_PASSWORD } else { $null } }
if (-not $StartDate) { $StartDate = if ($DEMO_START_DATE) { $DEMO_START_DATE } else { "2023-01-01" } }

# Password is required (either from .env_demo or CLI)
if (-not $DbPassword) {
    Write-Host "`n❌ Error: Database password required!" -ForegroundColor Red
    Write-Host "Provide via:" -ForegroundColor Yellow
    Write-Host "  1. CLI: .\generate.ps1 -DbPassword 'yourpassword'" -ForegroundColor Gray
    Write-Host "  2. .env_demo: Copy .env_demo.template to .env_demo and set DEMO_POSTGRES_PASSWORD" -ForegroundColor Gray
    exit 1
}

# Safety check
Write-Host "`n🔒 Safety Check: Synthetic Data Generator" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan

# Fixed database name (never changes)
$dbName = "opendental_demo"

Write-Host "`nTarget Database: $dbName" -ForegroundColor Yellow
Write-Host "Host: $DbHost" -ForegroundColor Gray
Write-Host "User: $DbUser" -ForegroundColor Gray
Write-Host "Patients: $Patients" -ForegroundColor Gray
Write-Host "Start Date: $StartDate" -ForegroundColor Gray

# Confirmation prompt
Write-Host "`n⚠️  This will generate synthetic data in '$dbName' database." -ForegroundColor Yellow
$confirm = Read-Host "Continue? (yes/no)"

if ($confirm -ne "yes") {
    Write-Host "`n❌ Generation cancelled" -ForegroundColor Red
    exit 1
}

Write-Host "`n🚀 Starting synthetic data generation..." -ForegroundColor Green
Write-Host "═══════════════════════════════════════════`n" -ForegroundColor Cyan

# Run generator with explicit database connection
python main.py `
    --patients $Patients `
    --db-host $DbHost `
    --db-name $dbName `
    --db-user $DbUser `
    --db-password $DbPassword `
    --start-date $StartDate

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Generation completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`n❌ Generation failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

