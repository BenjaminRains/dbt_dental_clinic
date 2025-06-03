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

    # Check if Pipfile exists
    $pipfilePath = Join-Path $ProjectPath "etl_pipeline/Pipfile"
    if (-not (Test-Path $pipfilePath)) {
        Write-Host "❌ Pipfile not found at: $pipfilePath" -ForegroundColor Red
        return $false
    }

    Write-Host "✅ ETL Pipenv environment detected"
    Write-Host "📦 Installing ETL dependencies..."

    # Change to the directory containing the Pipfile
    Push-Location (Split-Path $pipfilePath)
    
    try {
        # Install dependencies
        pipenv install --dev
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install dependencies"
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

        # Activate the virtual environment
        $venvPath = pipenv --venv
        if ($venvPath) {
            $activateScript = Join-Path $venvPath "Scripts/Activate.ps1"
            if (Test-Path $activateScript) {
                . $activateScript
                Write-Host "✅ Virtual environment activated"
            }
        }

        Write-Host "`n🎉 ETL environment ready!`n"

        # Display available commands
        Write-Host "🔄 ETL Pipeline Commands:"
        Write-Host "  etl                       - Main ETL CLI (run 'etl' for help)"
        Write-Host "  etl-help                  - Show detailed command help"
        Write-Host "  etl-status                - Get pipeline status"
        Write-Host "  etl-validate [table]      - Validate data quality"
        Write-Host "  etl-performance           - Analyze performance"
        Write-Host "  etl-config [action]       - Manage configuration`n"

        Write-Host "🏥 Dental Clinic Specific:"
        Write-Host "  etl-patient-sync          - Sync patient data"
        Write-Host "  etl-appointment-metrics   - Daily appointment metrics"
        Write-Host "  etl-compliance-check      - HIPAA compliance check`n"

        Write-Host "⚡ Quick Actions:"
        Write-Host "  etl-quick-status          - Quick status check"
        Write-Host "  etl-quick-validate        - Quick patient validation"
        Write-Host "  etl-quick-run             - Quick run core tables`n"

        Write-Host "🔧 Basic Operations:"
        Write-Host "  etl-test-connections      - Test database connections"
        Write-Host "  etl-run                   - Run ETL pipeline"
        Write-Host "  etl-setup                 - Set up databases"
        Write-Host "  etl-deactivate            - Deactivate ETL environment"

        return $true
    }
    catch {
        Write-Host "❌ Error initializing ETL environment: $_" -ForegroundColor Red
        return $false
    }
    finally {
        Pop-Location
    }
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
        [switch]$Force
    )

    if ($Full) {
        Write-Host "🚀 Running full ETL pipeline..." -ForegroundColor Magenta
        etl run --full
    }
    elseif ($Tables) {
        Write-Host "🚀 Running ETL pipeline for tables: $Tables" -ForegroundColor Magenta
        etl run --tables $Tables
    }
    else {
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

# Remove the Export-ModuleMember line since we're dot-sourcing this file
# Export-ModuleMember -Function Initialize-ETLEnvironment, Deactivate-ETLEnvironment, Test-ETLConnections, Run-ETLPipeline, Setup-ETLDatabases, Quick-ETLStatus, Quick-ETLValidate, Quick-ETLRun, Sync-PatientData, Get-AppointmentMetrics, Test-HIPAACompliance 