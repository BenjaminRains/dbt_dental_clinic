# ETL Pipeline PowerShell Functions
# This file contains core ETL functions that are sourced by the main PowerShell profile

# Core ETL environment functions
function Initialize-ETLEnvironment {
    if ($script:IsETLInitialized) {
        Write-Host "✅ ETL environment already initialized" -ForegroundColor Green
        return
    }

    Write-Host "🚀 Initializing ETL environment..." -ForegroundColor Magenta
    
    # Check if we're in the correct directory
    if (-not (Test-Path "etl_pipeline")) {
        Write-Host "❌ Not in ETL project directory. Please navigate to the project root." -ForegroundColor Red
        return
    }

    # Check if Pipenv is installed
    if (-not (Get-Command pipenv -ErrorAction SilentlyContinue)) {
        Write-Host "❌ Pipenv not found. Please install it first: pip install pipenv" -ForegroundColor Red
        return
    }

    # Install dependencies if needed
    if (-not (Test-Path "Pipfile.lock")) {
        Write-Host "📦 Installing dependencies..." -ForegroundColor Yellow
        pipenv install
    }

    # Create necessary directories
    $directories = @(
        "etl_pipeline/logs",
        "etl_pipeline/config",
        "etl_pipeline/data"
    )

    foreach ($dir in $directories) {
        if (-not (Test-Path $dir)) {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            Write-Host "📁 Created directory: $dir" -ForegroundColor Green
        }
    }

    # Set environment variables
    $env:ETL_ENV = "development"
    $env:PYTHONPATH = $PWD.Path

    # Mark as initialized
    $script:IsETLInitialized = $true
    Write-Host "✅ ETL environment initialized successfully" -ForegroundColor Green
    Write-Host "📝 Available commands: etl-help" -ForegroundColor Cyan
}

function Deactivate-ETLEnvironment {
    if (-not $script:IsETLInitialized) {
        Write-Host "ℹ️  ETL environment not initialized" -ForegroundColor Yellow
        return
    }

    # Clear environment variables
    Remove-Item Env:\ETL_ENV -ErrorAction SilentlyContinue
    Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue

    # Mark as deinitialized
    $script:IsETLInitialized = $false
    Write-Host "✅ ETL environment deactivated" -ForegroundColor Green
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
        [switch]$Force
    )

    if ($Full) {
        Write-Host "🚀 Running full ETL pipeline..." -ForegroundColor Magenta
        etl run --full
    } elseif ($Tables) {
        Write-Host "🚀 Running ETL pipeline for tables: $Tables" -ForegroundColor Magenta
        etl run --tables $Tables
    } else {
        Write-Host "❌ Please specify either --Full or --Tables" -ForegroundColor Red
    }
}

function Setup-ETLDatabases {
    Write-Host "🔧 Setting up ETL databases..." -ForegroundColor Magenta
    etl setup
}

# Quick action functions
function Quick-ETLStatus {
    Write-Host "📊 Quick ETL status check..." -ForegroundColor Magenta
    etl status --format summary
}

function Quick-ETLValidate {
    param([string]$Table)
    Write-Host "🔍 Quick validation for table: $Table" -ForegroundColor Magenta
    etl validate --table $Table
}

function Quick-ETLRun {
    param([string]$Table)
    Write-Host "🚀 Quick run for table: $Table" -ForegroundColor Magenta
    etl run --tables $Table
}

# Dental clinic specific functions
function Sync-PatientData {
    param(
        [switch]$IncrementalOnly,
        [string]$ClinicId
    )
    
    $args = @()
    if ($IncrementalOnly) { $args += "--incremental-only" }
    if ($ClinicId) { $args += "--clinic-id", $ClinicId }
    
    Write-Host "🔄 Syncing patient data..." -ForegroundColor Magenta
    etl patient-sync $args
}

function Get-AppointmentMetrics {
    param(
        [string]$Date = (Get-Date).ToString("yyyy-MM-dd"),
        [string]$Format = "summary"
    )
    
    Write-Host "📊 Generating appointment metrics for $Date..." -ForegroundColor Magenta
    etl appointment-metrics --date $Date --format $Format
}

function Test-HIPAACompliance {
    param(
        [switch]$GenerateReport,
        [string]$Table
    )
    
    $args = @()
    if ($GenerateReport) { $args += "--generate-report" }
    if ($Table) { $args += "--table", $Table }
    
    Write-Host "🔒 Running HIPAA compliance check..." -ForegroundColor Magenta
    etl compliance-check $args
}

# Export functions
Export-ModuleMember -Function * 