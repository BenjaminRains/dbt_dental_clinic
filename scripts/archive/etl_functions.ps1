# ARCHIVED (Phase 5.5) — not loaded by default. Daily workflow: pip install -e tools/mdc_cli; mdc ...
# Kept for reference and emergency rollback. Load via scripts/archive/ only if needed.
#
# Legacy ETL PowerShell helpers (Pipenv shell activation + manual etl_pipeline/.env parsing).
# Superseded by:
#   - pydantic-settings: etl_pipeline/.env_<stage> via etl_pipeline/etl_pipeline/config/settings_v2.py
#   - stateless mdc CLI: .\load_project.ps1 → scripts/mdc_aliases.ps1 (etl-run, etl-validate, etl-test, etl-status)
#
# ETL Pipeline Functions
# This file contains functions for managing the ETL pipeline environment

function Initialize-ETLEnvironment {
    param (
        [string]$ProjectPath = $PWD.Path
    )

    Write-Host "`n╔════════════════════════════════╗"
    Write-Host "║      ETL Pipeline Environment  ║"
    Write-Host "╚════════════════════════════════╝`n"

    Write-Host "🔄 Initializing ETL environment for: $(Split-Path $ProjectPath -Leaf)"
    Write-Host "📂 Project path: $ProjectPath`n"

    # Check if Pipenv is installed
    if (-not (Get-Command pipenv -ErrorAction SilentlyContinue)) {
        Write-Host "❌ Pipenv is not installed. Please install it first." -ForegroundColor Red
        return $false
    }

    # Check if Pipfile exists in the root directory
    $pipfilePath = Join-Path $ProjectPath "Pipfile"
    if (-not (Test-Path $pipfilePath)) {
        Write-Host "❌ Pipfile not found at: $pipfilePath" -ForegroundColor Red
        return $false
    }

    Write-Host "✅ ETL Pipenv environment detected"
    Write-Host "📦 Installing ETL dependencies..."
    
    try {
        # Suppress pipenv verbosity and courtesy notices
        $env:PIPENV_VERBOSITY = -1
        $env:PIPENV_IGNORE_VIRTUALENVS = 1
        
        # Install dependencies and the package in development mode
        pipenv install --dev 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install dependencies"
        }
        
        # Install the package in development mode
        Write-Host "📦 Installing ETL package in development mode..."
        pipenv run pip install -e .
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install ETL package"
        }
        Write-Host "✅ Dependencies installed successfully"

        # Load environment variables
        Write-Host "🔧 Loading ETL .env variables..."
        $envPath = Join-Path $ProjectPath "etl_pipeline/.env"
        if (Test-Path $envPath) {
            Get-Content $envPath | ForEach-Object {
                if ($_ -match '^([^=]+)=(.*)$') {
                    $name = $matches[1]
                    $value = $matches[2]
                    [Environment]::SetEnvironmentVariable($name, $value, 'Process')
                }
            }
            Write-Host "✅ .env variables loaded successfully"
        }
        else {
            Write-Host "⚠️  No .env file found at: $envPath" -ForegroundColor Yellow
        }

        # Activate the virtual environment using pipenv shell
        Write-Host "🔧 Activating virtual environment..."
        pipenv shell
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to activate virtual environment"
        }
        Write-Host "✅ Virtual environment activated"

        Write-Host "`n🎉 ETL environment ready!`n"

        # Display available commands in a cleaner format
        Show-ETLCommands

        return $true
    }
    catch {
        Write-Host "❌ Failed to initialize ETL environment: $_" -ForegroundColor Red
        return $false
    }
}

function Show-ETLCommands {
    Write-Host "╔════════════════════════════════════════════════════════════════╗"
    Write-Host "║                        ETL Commands                             ║"
    Write-Host "╚════════════════════════════════════════════════════════════════╝`n"

    Write-Host "🔧 Core Operations:" -ForegroundColor White
    Write-Host "  etl run --full                     Run complete pipeline"
    Write-Host "  etl run --tables patient,appt      Run specific tables"
    Write-Host "  etl status                         Show pipeline status"
    Write-Host "  etl test-connections               Test database connections"
    Write-Host ""

    Write-Host "🏥 Dental Clinic:" -ForegroundColor White
    Write-Host "  etl-patient-sync                   Sync patient data"
    Write-Host "  etl-appointment-metrics            Daily appointment metrics"
    Write-Host "  etl-compliance-check               HIPAA compliance check"
    Write-Host ""

    Write-Host "🔍 Validation & Testing:" -ForegroundColor White
    Write-Host "  etl-validate                       Validate all data"
    Write-Host "  etl-validate -Table patient        Validate specific table"
    Write-Host "  etl-test                          Test connections"
    Write-Host ""

    Write-Host "💡 Quick Tips:" -ForegroundColor Yellow
    Write-Host "  • Use 'etl --help' for detailed options"
    Write-Host "  • Use 'etl-deactivate' to exit environment"
    Write-Host "  • Use 'dbt-init' to switch to dbt environment"
    Write-Host ""
}

function Deactivate-ETLEnvironment {
    if ($env:PIPENV_ACTIVE) {
        deactivate
        Write-Host "✅ ETL environment deactivated"
    }
    else {
        Write-Host "⚠️  No active ETL environment to deactivate" -ForegroundColor Yellow
    }
}

# Core ETL operation functions
function Test-ETLConnections {
    Write-Host "🔍 Testing ETL connections..." -ForegroundColor Magenta
    etl test-connections
}

function Run-ETLPipeline {
    param(
        [string]$Tables,
        [switch]$Full,
        [switch]$Force,
        [string]$Config = "etl_pipeline/config/pipeline.yaml"
    )

    Write-Host "🚀 Running ETL pipeline..." -ForegroundColor Magenta
    
    # Build the command arguments
    $args = @("run", "-c", $Config)
    
    if ($Full) {
        Write-Host "   📊 Running full pipeline" -ForegroundColor Cyan
        $args += "--full"
    }
    elseif ($Tables) {
        Write-Host "   📊 Running pipeline for tables: $Tables" -ForegroundColor Cyan
        $args += "--tables", $Tables
    }
    else {
        Write-Host "❌ Please specify either -Full or -Tables" -ForegroundColor Red
        Write-Host "💡 Example: Run-ETLPipeline -Full" -ForegroundColor Yellow
        Write-Host "💡 Example: Run-ETLPipeline -Tables 'patient,appointment'" -ForegroundColor Yellow
        return
    }
    
    if ($Force) {
        Write-Host "   ⚡ Force mode enabled" -ForegroundColor Yellow
        $args += "--force"
    }
    
    try {
        # Use pipenv run to ensure we're in the correct environment
        pipenv run python -m etl_pipeline.cli.entry @args
    }
    catch {
        Write-Host "❌ Pipeline run failed: $_" -ForegroundColor Red
        Write-Host "💡 Run 'etl run --help' for more information" -ForegroundColor Yellow
    }
}

function Setup-ETLDatabases {
    Write-Host "🔧 Setting up ETL databases..." -ForegroundColor Magenta
    etl setup
}

# Quick action functions with improved naming
function Start-ETLPatientSync {
    param(
        [switch]$IncrementalOnly,
        [string]$ClinicId
    )
    
    $args = @("patient-sync")
    if ($IncrementalOnly) { $args += "--incremental-only" }
    if ($ClinicId) { $args += "--clinic-id", $ClinicId }
    
    Write-Host "🔄 Syncing patient data..." -ForegroundColor Magenta
    etl @args
}

function Get-ETLAppointmentMetrics {
    param(
        [string]$Date = (Get-Date).ToString("yyyy-MM-dd"),
        [string]$Format = "summary"
    )
    
    Write-Host "📊 Generating appointment metrics for $Date..." -ForegroundColor Magenta
    etl appointment-metrics --date $Date --format $Format
}

function Test-ETLCompliance {
    param(
        [switch]$GenerateReport,
        [string]$Table
    )
    
    $args = @("compliance-check")
    if ($GenerateReport) { $args += "--generate-report" }
    if ($Table) { $args += "--table", $Table }
    
    Write-Host "🔒 Running HIPAA compliance check..." -ForegroundColor Magenta
    etl @args
}

function Test-ETLValidation {
    param(
        [Parameter(Mandatory=$false)]
        [string]$Table,
        
        [Parameter(Mandatory=$false)]
        [string]$Config = "etl_pipeline/config/pipeline.yaml",
        
        [Parameter(Mandatory=$false)]
        [switch]$Verbose
    )
    
    Write-Host "🔍 Running data validation..." -ForegroundColor Magenta
    
    # Build the command arguments
    $args = @("validate-data")
    
    if ($Config) {
        $args += "--config", $Config
    }
    
    if ($Table) {
        Write-Host "   📊 Validating table: $Table" -ForegroundColor Cyan
        $args += "--table", $Table
    } else {
        Write-Host "   📊 Validating all tables" -ForegroundColor Cyan
    }
    
    if ($Verbose) {
        $args += "--verbose"
        Write-Host "   🔍 Verbose mode enabled" -ForegroundColor Gray
    }
    
    try {
        # Use python -m to ensure proper module loading
        python -m etl_pipeline.cli.entry @args
    }
    catch {
        Write-Host "❌ Validation failed: $_" -ForegroundColor Red
        Write-Host "💡 Run 'etl validate-data --help' for more information" -ForegroundColor Yellow
    }
}

function Get-ETLStatus {
    param(
        [ValidateSet("table", "json", "summary")]
        [string]$Format = "table",
        
        [string]$Table = "",
        
        [switch]$Watch,
        
        [string]$Output = "",
        
        [switch]$IncludeDentalIntelligence,
        
        [string]$TimeRange = "24h"
    )
    
    Write-Host "📊 Getting ETL pipeline status..." -ForegroundColor Green
    
    # Build the command arguments
    $args = @("status", "--format", $Format, "--time-range", $TimeRange)
    
    # Add optional parameters if specified
    if ($Table) { 
        Write-Host "   📋 Table: $Table" -ForegroundColor Cyan
        $args += "--table", $Table
    }
    
    if ($Watch) { 
        Write-Host "   👀 Watching status in real-time..." -ForegroundColor Cyan
        $args += "--watch"
    }
    
    if ($Output) { 
        Write-Host "   📄 Output: $Output" -ForegroundColor Cyan
        $args += "--output", $Output
    }
    
    if ($IncludeDentalIntelligence) {
        Write-Host "   🦷 Including dental practice intelligence..." -ForegroundColor Cyan
        $args += "--include-dental-intelligence"
    }
    
    try {
        # Execute the command using etl directly (environment already active)
        etl @args
    }
    catch {
        Write-Host "❌ Failed to get ETL status: $_" -ForegroundColor Red
        return $false
    }
    
    return $true
}

# Aliases for improved usability
Set-Alias -Name etl-patient-sync -Value Start-ETLPatientSync
Set-Alias -Name etl-appointment-metrics -Value Get-ETLAppointmentMetrics
Set-Alias -Name etl-compliance-check -Value Test-ETLCompliance
Set-Alias -Name etl-validate -Value Test-ETLValidation
Set-Alias -Name etl-test -Value Test-ETLConnections
Set-Alias -Name etl-status -Value Get-ETLStatus
Set-Alias -Name etl-help -Value Show-ETLCommands
