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
        # Install dependencies and the package in development mode
        pipenv install --dev
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
        Write-Host "  etl validate-data         - Validate data quality (use --help for options)"
        Write-Host "  etl-performance           - Analyze performance"
        Write-Host "  etl-config [action]       - Manage configuration`n"

        Write-Host "🏥 Dental Clinic Specific:"
        Write-Host "  etl patient-sync          - Sync patient data"
        Write-Host "  etl appointment-metrics   - Daily appointment metrics"
        Write-Host "  etl compliance-check      - HIPAA compliance check`n"

        Write-Host "⚡ Quick Actions (with aliases):"
        Write-Host "  etl-s                     - Quick status check (alias for Quick-ETLStatus)"
        Write-Host "  etl-v                     - Quick data validation (alias for Quick-ETLValidate)"
        Write-Host "  etl-r                     - Quick run core tables (alias for Quick-ETLRun)`n"

        Write-Host "🔧 Basic Operations (with aliases):"
        Write-Host "  etl-t                     - Test database connections (alias for Test-ETLConnections)"
        Write-Host "  etl-p                     - Run ETL pipeline (alias for Run-ETLPipeline)"
        Write-Host "  etl-d                     - Deactivate ETL environment (alias for Deactivate-ETLEnvironment)"

        return $true
    }
    catch {
        Write-Host "❌ Error initializing ETL environment: $_" -ForegroundColor Red
        return $false
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
        Write-Host "❌ Please specify either --Full or --Tables" -ForegroundColor Red
        return
    }
    
    if ($Force) {
        Write-Host "   ⚡ Force mode enabled" -ForegroundColor Yellow
        $args += "--force"
    }
    
    try {
        Write-Host "   🚀 Running command: pipenv run python -m etl_pipeline.cli.entry $($args -join ' ')" -ForegroundColor Gray
        # Use pipenv run to ensure we're in the correct environment
        pipenv run python -m etl_pipeline.cli.entry @args
    }
    catch {
        Write-Host "❌ Pipeline run failed: $_" -ForegroundColor Red
        Write-Host "💡 Tip: Run 'pipenv run python -m etl_pipeline.cli.entry run --help' for more information" -ForegroundColor Yellow
    }
}

function Setup-ETLDatabases {
    Write-Host "🔧 Setting up ETL databases..." -ForegroundColor Magenta
    etl setup
}

# Quick action functions
function Quick-ETLStatus {
    param(
        [switch]$IncludeDental,
        [string]$Format = "summary"
    )
    
    if ($IncludeDental) {
        Write-Host "📊 Quick ETL status check with dental intelligence..." -ForegroundColor Magenta
        etl status --format $Format --include-dental-intelligence
    } else {
        Write-Host "📊 Quick ETL status check..." -ForegroundColor Magenta
        etl status --format $Format
    }
}

function Quick-ETLValidate {
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
        Write-Host "   🚀 Running command: etl $($args -join ' ')" -ForegroundColor Gray
        # Use python -m to ensure proper module loading
        python -m etl_pipeline.cli.entry @args
    }
    catch {
        Write-Host "❌ Validation failed: $_" -ForegroundColor Red
        Write-Host "💡 Tip: Run 'python -m etl_pipeline.cli.entry validate-data --help' for more information" -ForegroundColor Yellow
    }
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

# New enhanced health check function
function Test-ComprehensiveHealth {
    param(
        [switch]$IncludeDentalIntelligence,
        [string]$OutputFile,
        [string]$Format = "summary"
    )
    
    Write-Host "🏥 Running comprehensive health check..." -ForegroundColor Magenta
    
    if ($IncludeDentalIntelligence) {
        Write-Host "   🧠 Including dental practice intelligence..." -ForegroundColor Cyan
        $args = @("status", "--format", $Format, "--include-dental-intelligence")
    } else {
        Write-Host "   🔧 Basic technical health check..." -ForegroundColor Cyan
        $args = @("status", "--format", $Format)
    }
    
    if ($OutputFile) {
        $args += "--output", $OutputFile
        Write-Host "   📄 Results will be saved to: $OutputFile" -ForegroundColor Gray
    }
    
    etl @args
}

function Get-ETLStatus {
    <#
    .SYNOPSIS
    Get the current status of the ETL pipeline.
    
    .DESCRIPTION
    Retrieves and displays the current status of the ETL pipeline, including table statuses,
    last run times, and any errors or warnings.
    
    .PARAMETER Format
    Output format: table, json, or summary (default: table)
    
    .PARAMETER Table
    Show status for specific table only
    
    .PARAMETER Watch
    Watch status in real-time (press Ctrl+C to stop)
    
    .PARAMETER Output
    Output file for status report
    
    .PARAMETER IncludeDentalIntelligence
    Include dental practice specific metrics and insights
    
    .PARAMETER TimeRange
    Time range for status check (e.g., 1h, 24h, 7d, 30d)
    
    .EXAMPLE
    Get-ETLStatus -Format summary
    Get-ETLStatus -Table patient -Format json
    Get-ETLStatus -Watch -TimeRange 24h
    #>
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
    
    # Get the config path
    $configPath = Join-Path $PSScriptRoot ".." "config" "pipeline.yaml"
    if (-not (Test-Path $configPath)) {
        Write-Host "❌ Pipeline config file not found at: $configPath" -ForegroundColor Red
        return $false
    }
    
    # Build the command string
    $cmd = "pipenv run python -m etl_pipeline.cli.main status --config `"$configPath`" --format `"$Format`" --time-range `"$TimeRange`""
    
    # Add optional parameters if specified
    if ($Table) { 
        Write-Host "   📋 Table: $Table" -ForegroundColor Cyan
        $cmd += " --table `"$Table`""
    }
    
    if ($Watch) { 
        Write-Host "   👀 Watching status in real-time..." -ForegroundColor Cyan
        $cmd += " --watch"
    }
    
    if ($Output) { 
        Write-Host "   📄 Output: $Output" -ForegroundColor Cyan
        $cmd += " --output `"$Output`""
    }
    
    if ($IncludeDentalIntelligence) {
        Write-Host "   🦷 Including dental practice intelligence..." -ForegroundColor Cyan
        $cmd += " --include-dental-intelligence"
    }
    
    try {
        # Set PYTHONPATH to include the project root
        $projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
        $env:PYTHONPATH = $projectRoot
        
        # Execute the command
        Write-Host "   🔧 Running command: $cmd" -ForegroundColor Gray
        Invoke-Expression $cmd
    }
    catch {
        Write-Host "❌ Failed to get ETL status: $_" -ForegroundColor Red
        return $false
    }
    finally {
        # Clean up environment variable
        Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue
    }
    
    return $true
}

# Alias for backward compatibility
Set-Alias -Name etl-status -Value Get-ETLStatus

# Remove the Export-ModuleMember line since we're dot-sourcing this file
# Export-ModuleMember -Function Initialize-ETLEnvironment, Deactivate-ETLEnvironment, Test-ETLConnections, Run-ETLPipeline, Setup-ETLDatabases, Quick-ETLStatus, Quick-ETLValidate, Quick-ETLRun, Sync-PatientData, Get-AppointmentMetrics, Test-HIPAACompliance 