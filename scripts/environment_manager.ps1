# Simplified Data Engineering Environment Manager
# Focused on dbt and ETL functionality for dental clinic pipelines

# Environment state tracking
$script:IsDBTActive = $false
$script:IsETLActive = $false
$script:ActiveProject = $null
$script:VenvPath = $null

# =============================================================================
# DBT ENVIRONMENT
# =============================================================================

function Initialize-DBTEnvironment {
    param([string]$ProjectPath = (Get-Location))

    if ($script:IsDBTActive) {
        Write-Host "❌ dbt environment already active. Use 'dbt-deactivate' first." -ForegroundColor Yellow
        return
    }

    if ($script:IsETLActive) {
        Write-Host "❌ ETL environment is currently active. Run 'etl-deactivate' first before activating dbt." -ForegroundColor Red
        return
    }

    $projectName = Split-Path -Leaf $ProjectPath
    Write-Host "`n🏗️  Initializing dbt environment: $projectName" -ForegroundColor Cyan

    # Verify dbt project
    if (-not (Test-Path "$ProjectPath\dbt_project.yml")) {
        Write-Host "❌ No dbt_project.yml found" -ForegroundColor Red
        return
    }

    # Set up dbt pipenv environment (always from project root)
    if (Test-Path "$ProjectPath\Pipfile") {
        Push-Location $ProjectPath
        try {
            Write-Host "📦 Installing dbt dependencies..." -ForegroundColor Yellow
            pipenv install
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "🔧 Activating dbt pipenv shell..." -ForegroundColor Yellow
                
                # Get virtual environment path
                $script:VenvPath = (pipenv --venv 2>$null).Trim()
                
                if ($script:VenvPath) {
                    # Set environment variables to simulate pipenv shell
                    $env:VIRTUAL_ENV = $script:VenvPath
                    $env:PIPENV_ACTIVE = 1
                    
                    # Update PATH to include virtual environment
                    $venvScripts = Join-Path $script:VenvPath "Scripts"
                    if (Test-Path $venvScripts) {
                        $env:PATH = "$venvScripts;$env:PATH"
                        Write-Host "✅ dbt pipenv shell activated: $(Split-Path $script:VenvPath -Leaf)" -ForegroundColor Green
                    } else {
                        Write-Host "⚠️ Virtual environment found but Scripts directory missing" -ForegroundColor Yellow
                    }
                } else {
                    Write-Host "❌ Failed to get virtual environment path" -ForegroundColor Red
                    Pop-Location
                    return
                }
            } else {
                Write-Host "❌ Failed to install dbt dependencies" -ForegroundColor Red
                Pop-Location
                return
            }
        } catch {
            Write-Host "❌ Failed to set up dbt pipenv environment: $_" -ForegroundColor Red
            Pop-Location
            return
        }
        Pop-Location
    } else {
        Write-Host "❌ No Pipfile found in project root - dbt environment unavailable" -ForegroundColor Red
        return
    }

    # Load environment variables
    @(".env", ".dbt-env") | ForEach-Object {
        $envFile = "$ProjectPath\$_"
        if (Test-Path $envFile) {
            Get-Content $envFile | ForEach-Object {
                if ($_ -match '^([^#][^=]+)=(.*)$') {
                    [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
                }
            }
        }
    }

    $script:IsDBTActive = $true
    $script:ActiveProject = $projectName

    Write-Host "`n✅ dbt environment ready!" -ForegroundColor Green
    Write-Host "Commands: dbt, notebook, format, lint, test" -ForegroundColor Cyan
    Write-Host "To switch to ETL: run 'dbt-deactivate' first, then 'etl-init'`n" -ForegroundColor Gray
}

function Stop-DBTEnvironment {
    if (-not $script:IsDBTActive) {
        Write-Host "❌ dbt environment not active" -ForegroundColor Yellow
        return
    }

    Write-Host "🔄 Deactivating dbt pipenv shell..." -ForegroundColor Yellow

    # Clean up pipenv shell environment
    if ($script:VenvPath) {
        $env:VIRTUAL_ENV = $null
        $env:PIPENV_ACTIVE = $null
        
        # Remove virtual environment from PATH
        $venvScripts = Join-Path $script:VenvPath "Scripts"
        if ($env:PATH -like "*$venvScripts*") {
            $env:PATH = $env:PATH.Replace("$venvScripts;", "").Replace(";$venvScripts", "").Replace($venvScripts, "")
        }
        Write-Host "✅ dbt pipenv shell deactivated" -ForegroundColor Green
    }

    $script:IsDBTActive = $false
    $script:ActiveProject = $null
    $script:VenvPath = $null

    Write-Host "✅ dbt environment deactivated - ETL environment can now be activated" -ForegroundColor Green
}

# =============================================================================
# ETL ENVIRONMENT  
# =============================================================================

function Initialize-ETLEnvironment {
    param([string]$ProjectPath = (Get-Location))

    if ($script:IsETLActive) {
        Write-Host "❌ ETL environment already active. Use 'etl-deactivate' first." -ForegroundColor Yellow
        return
    }

    if ($script:IsDBTActive) {
        Write-Host "❌ dbt environment is currently active. Run 'dbt-deactivate' first before activating ETL." -ForegroundColor Red
        return
    }

    $projectName = Split-Path -Leaf $ProjectPath
    Write-Host "`n🔄 Initializing ETL environment: $projectName" -ForegroundColor Magenta

    # Check for ETL project structure (always in etl_pipeline subdirectory)
    $etlPath = "$ProjectPath\etl_pipeline"
    
    if (-not (Test-Path "$etlPath\Pipfile")) {
        Write-Host "❌ No Pipfile found in etl_pipeline directory" -ForegroundColor Red
        return
    }

    # Set up ETL pipenv environment (always from etl_pipeline subdirectory)
    Push-Location $etlPath
    try {
        Write-Host "📦 Installing ETL dependencies..." -ForegroundColor Yellow
        pipenv install
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "🔧 Activating ETL pipenv shell..." -ForegroundColor Yellow
            
            # Get virtual environment path
            $script:VenvPath = (pipenv --venv 2>$null).Trim()
            
            if ($script:VenvPath) {
                # Set environment variables to simulate pipenv shell
                $env:VIRTUAL_ENV = $script:VenvPath
                $env:PIPENV_ACTIVE = 1
                
                # Update PATH to include virtual environment
                $venvScripts = Join-Path $script:VenvPath "Scripts"
                if (Test-Path $venvScripts) {
                    $env:PATH = "$venvScripts;$env:PATH"
                    Write-Host "✅ ETL pipenv shell activated: $(Split-Path $script:VenvPath -Leaf)" -ForegroundColor Green
                } else {
                    Write-Host "⚠️ Virtual environment found but Scripts directory missing" -ForegroundColor Yellow
                }
            } else {
                Write-Host "❌ Failed to get virtual environment path" -ForegroundColor Red
                Pop-Location
                return
            }
        } else {
            Write-Host "❌ Failed to install ETL dependencies" -ForegroundColor Red
            Pop-Location
            return
        }
    } catch {
        Write-Host "❌ Failed to set up ETL pipenv environment: $_" -ForegroundColor Red
        Pop-Location
        return
    }
    Pop-Location

    # Load environment variables
    if (Test-Path "$ProjectPath\.env") {
        Get-Content "$ProjectPath\.env" | ForEach-Object {
            if ($_ -match '^([^#][^=]+)=(.*)$') {
                [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
            }
        }
    }

    $script:IsETLActive = $true
    $script:ActiveProject = $projectName

    Write-Host "`n✅ ETL environment ready!" -ForegroundColor Green
    Write-Host "Commands: etl, etl-status, etl-validate, etl-run, etl-test" -ForegroundColor Cyan
    Write-Host "To switch to dbt: run 'etl-deactivate' first, then 'dbt-init'`n" -ForegroundColor Gray
}

function Stop-ETLEnvironment {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active" -ForegroundColor Yellow
        return
    }

    Write-Host "🔄 Deactivating ETL pipenv shell..." -ForegroundColor Yellow

    # Clean up pipenv shell environment
    if ($script:VenvPath) {
        $env:VIRTUAL_ENV = $null
        $env:PIPENV_ACTIVE = $null
        
        # Remove virtual environment from PATH
        $venvScripts = Join-Path $script:VenvPath "Scripts"
        if ($env:PATH -like "*$venvScripts*") {
            $env:PATH = $env:PATH.Replace("$venvScripts;", "").Replace(";$venvScripts", "").Replace($venvScripts, "")
        }
        Write-Host "✅ ETL pipenv shell deactivated" -ForegroundColor Green
    }

    $script:IsETLActive = $false
    $script:ActiveProject = $null
    $script:VenvPath = $null

    Write-Host "✅ ETL environment deactivated - dbt environment can now be activated" -ForegroundColor Green
}

# =============================================================================
# COMMAND WRAPPERS - FIXED: Avoid infinite recursion
# =============================================================================

# DBT Commands - FIXED: Use pipenv run to avoid recursion
function Invoke-DBT {
    if (-not $script:IsDBTActive) {
        Write-Host "❌ dbt environment not active. Run 'dbt-init' first." -ForegroundColor Red
        return
    }
    Write-Host "🚀 dbt $($args -join ' ')" -ForegroundColor Cyan
    # FIXED: Use pipenv run to avoid infinite recursion with dbt alias
    pipenv run dbt $args
}

function Start-Notebook {
    if (-not $script:IsDBTActive) {
        Write-Host "❌ dbt environment not active. Run 'dbt-init' first." -ForegroundColor Red
        return
    }
    jupyter notebook
}

function Format-Code {
    if (-not $script:IsDBTActive) {
        Write-Host "❌ dbt environment not active. Run 'dbt-init' first." -ForegroundColor Red
        return
    }
    black . && isort .
}

function Test-DBT {
    if (-not $script:IsDBTActive) {
        Write-Host "❌ dbt environment not active. Run 'dbt-init' first." -ForegroundColor Red
        return
    }
    pytest $args
}

# ETL Commands - FIXED: Use python directly instead of pipenv run
function Invoke-ETL {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active. Run 'etl-init' first." -ForegroundColor Red
        return
    }
    
    if (-not $args -or $args.Count -eq 0) {
        Show-ETLHelp
        return
    }
    
    Write-Host "🔄 etl $($args -join ' ')" -ForegroundColor Magenta
    # FIXED: Use python directly since we're already in pipenv environment
    python -m etl_pipeline.cli.main $args
}

function Get-ETLStatus {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active. Run 'etl-init' first." -ForegroundColor Red
        return
    }
    # FIXED: Use python directly since we're already in pipenv environment
    python -m etl_pipeline.cli.main status $args
}

function Test-ETLValidation {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active. Run 'etl-init' first." -ForegroundColor Red
        return
    }
    # FIXED: Use python directly since we're already in pipenv environment
    python -m etl_pipeline.cli.main validate $args
}

function Start-ETLPipeline {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active. Run 'etl-init' first." -ForegroundColor Red
        return
    }
    # FIXED: Use python directly since we're already in pipenv environment
    python -m etl_pipeline.cli.main run $args
}

function Test-ETLConnections {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active. Run 'etl-init' first." -ForegroundColor Red
        return
    }
    
    if (Test-Path "test_connections.py") {
        # FIXED: Use python directly since we're already in pipenv environment
        python test_connections.py
    } else {
        # FIXED: Use python directly since we're already in pipenv environment
        python -m etl_pipeline.cli.main test-connections
    }
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

function Show-ETLHelp {
    Write-Host "`n╔════════════════════════════════════════════════════════════════╗"
    Write-Host "║                        ETL Commands                             ║"
    Write-Host "╚════════════════════════════════════════════════════════════════╝`n"

    Write-Host "🔧 Core Operations:" -ForegroundColor White
    Write-Host "  etl run --full                     Run complete pipeline"
    Write-Host "  etl run --tables patient,appt      Run specific tables"
    Write-Host "  etl status                         Show pipeline status"
    Write-Host "  etl test-connections               Test database connections"
    Write-Host ""

    Write-Host "🏥 Dental Clinic:" -ForegroundColor White
    Write-Host "  etl patient-sync                   Sync patient data"
    Write-Host "  etl appointment-metrics            Daily appointment metrics"
    Write-Host "  etl compliance-check               HIPAA compliance check"
    Write-Host ""

    Write-Host "🔍 Validation & Testing:" -ForegroundColor White
    Write-Host "  etl validate                       Validate all data"
    Write-Host "  etl validate --table patient       Validate specific table"
    Write-Host "  etl-test                          Test connections"
    Write-Host ""

    Write-Host "💡 Quick Tips:" -ForegroundColor Yellow
    Write-Host "  • Use 'etl --help' for detailed options"
    Write-Host "  • Use 'etl-deactivate' to exit environment"
    Write-Host "  • Use 'dbt-init' to switch to dbt environment"
    Write-Host ""
}

function Get-EnvironmentStatus {
    Write-Host "`n📊 Environment Status:" -ForegroundColor White
    
    if ($script:IsDBTActive) {
        Write-Host "  dbt: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
    } else {
        Write-Host "  dbt: ⭕ Inactive" -ForegroundColor Gray
    }
    
    if ($script:IsETLActive) {
        Write-Host "  ETL: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
    } else {
        Write-Host "  ETL: ⭕ Inactive" -ForegroundColor Gray
    }
    
    Write-Host ""
}

# =============================================================================
# PROMPT CUSTOMIZATION
# =============================================================================

function global:prompt {
    $envTag = ""
    $envColor = "White"
    
    if ($script:IsDBTActive -and $script:IsETLActive) {
        # This should never happen with our mutual exclusion, but just in case
        $envTag = "[ERROR:BOTH] "
        $envColor = "Red"
    } elseif ($script:IsDBTActive) {
        $envTag = "[dbt:$script:ActiveProject] "
        $envColor = "Green"
    } elseif ($script:IsETLActive) {
        $envTag = "[etl:$script:ActiveProject] "
        $envColor = "Magenta"
    }
    
    if ($envTag) {
        Write-Host $envTag -NoNewline -ForegroundColor $envColor
    }
    
    return "$($(Get-Location).Path)> "
}

# =============================================================================
# ALIASES
# =============================================================================

# Environment Management
Set-Alias -Name dbt-init -Value Initialize-DBTEnvironment
Set-Alias -Name dbt-deactivate -Value Stop-DBTEnvironment
Set-Alias -Name etl-init -Value Initialize-ETLEnvironment
Set-Alias -Name etl-deactivate -Value Stop-ETLEnvironment

# DBT Commands
Set-Alias -Name dbt -Value Invoke-DBT
Set-Alias -Name notebook -Value Start-Notebook
Set-Alias -Name format -Value Format-Code
Set-Alias -Name lint -Value Format-Code
Set-Alias -Name test -Value Test-DBT

# ETL Commands
Set-Alias -Name etl -Value Invoke-ETL
Set-Alias -Name etl-status -Value Get-ETLStatus
Set-Alias -Name etl-validate -Value Test-ETLValidation
Set-Alias -Name etl-run -Value Start-ETLPipeline
Set-Alias -Name etl-test -Value Test-ETLConnections

# Utility
Set-Alias -Name env-status -Value Get-EnvironmentStatus

# =============================================================================
# STARTUP MESSAGE
# =============================================================================

Write-Host "`n╔══════════════════════════════════════════════════════════╗" -ForegroundColor DarkBlue
Write-Host "║          Data Engineering Environment Manager           ║" -ForegroundColor Blue
Write-Host "║            Dental Clinic ETL & dbt Pipeline             ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor DarkBlue

Write-Host "`n🚀 Quick Start:" -ForegroundColor White
Write-Host "  dbt-init       - Initialize dbt environment" -ForegroundColor Cyan
Write-Host "  etl-init       - Initialize ETL environment" -ForegroundColor Magenta
Write-Host "  env-status     - Check environment status" -ForegroundColor Yellow

# Auto-detect project type
$cwd = Get-Location
if (Test-Path "$cwd\dbt_project.yml") {
    Write-Host "`n🏗️  dbt project detected (Pipfile in root). Run 'dbt-init' to start." -ForegroundColor Green
}
if (Test-Path "$cwd\etl_pipeline\Pipfile") {
    Write-Host "🔄 ETL pipeline detected (Pipfile in etl_pipeline/). Run 'etl-init' to start." -ForegroundColor Magenta
}

Write-Host ""

# Load enhanced ETL functions for dental clinic operations
$etlFunctionsPath = Join-Path $PSScriptRoot "etl_functions.ps1"
if (Test-Path $etlFunctionsPath) {
    . $etlFunctionsPath
    Write-Host "🦷 Enhanced dental clinic ETL functions loaded" -ForegroundColor Green
} 