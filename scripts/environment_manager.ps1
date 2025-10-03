# Simplified Data Engineering Environment Manager
# Focused on dbt and ETL functionality for dental clinic pipelines

# Environment state tracking
$script:IsDBTActive = $false
$script:IsETLActive = $false
$script:IsAPIActive = $false
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

    if ($script:IsAPIActive) {
        Write-Host "❌ API environment is currently active. Run 'api-deactivate' first before activating dbt." -ForegroundColor Red
        return
    }

    $projectName = Split-Path -Leaf $ProjectPath
    Write-Host "`n🏗️  Initializing dbt environment: $projectName" -ForegroundColor Cyan

    # Verify dbt project - check both current directory and dbt_dental_clinic_prod subdirectory
    $dbtProjectPath = $ProjectPath
    if (Test-Path "$ProjectPath\dbt_dental_clinic_prod\dbt_project.yml") {
        $dbtProjectPath = "$ProjectPath\dbt_dental_clinic_prod"
        Write-Host "📁 Found dbt project in: dbt_dental_clinic_prod/" -ForegroundColor Green
    } elseif (Test-Path "$ProjectPath\dbt_project.yml") {
        Write-Host "📁 Found dbt project in: current directory" -ForegroundColor Green
    } else {
        Write-Host "❌ No dbt_project.yml found in current directory or dbt_dental_clinic_prod/" -ForegroundColor Red
        return
    }

    # Set up dbt pipenv environment (use the dbt project directory)
    if (Test-Path "$dbtProjectPath\Pipfile") {
        Push-Location $dbtProjectPath
        try {
            Write-Host "📦 Installing dbt dependencies..." -ForegroundColor Yellow
            
            # Suppress pipenv verbosity and courtesy notices
            $env:PIPENV_VERBOSITY = -1
            $env:PIPENV_IGNORE_VIRTUALENVS = 1
            
            # Suppress pipenv output since we handle activation manually
            pipenv install 2>$null | Out-Null
            
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

    # Set DBT_PROFILES_DIR to point to the dbt project directory
    [Environment]::SetEnvironmentVariable('DBT_PROFILES_DIR', $dbtProjectPath, 'Process')
    Write-Host "🔧 Set DBT_PROFILES_DIR to: $dbtProjectPath" -ForegroundColor Green

    # Load environment variables from both project root and dbt project directory
    @(".env_production", ".dbt-env") | ForEach-Object {
        # Try dbt project directory first, then project root
        $envFile = "$dbtProjectPath\$_"
        if (-not (Test-Path $envFile)) {
            $envFile = "$ProjectPath\$_"
        }
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

    # Clean up dbt environment variables
    [Environment]::SetEnvironmentVariable('DBT_PROFILES_DIR', $null, 'Process')
    Write-Host "🔧 Cleared DBT_PROFILES_DIR" -ForegroundColor Green

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

    if ($script:IsAPIActive) {
        Write-Host "❌ API environment is currently active. Run 'api-deactivate' first before activating ETL." -ForegroundColor Red
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
        
        # Suppress pipenv verbosity and courtesy notices
        $env:PIPENV_VERBOSITY = -1
        $env:PIPENV_IGNORE_VIRTUALENVS = 1
        
        # Suppress pipenv output since we handle activation manually
        pipenv install 2>$null | Out-Null
        
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

    # Interactive Environment Selection
    Write-Host "`n🔧 ETL Environment Selection" -ForegroundColor Cyan
    Write-Host "Which environment would you like to use?" -ForegroundColor White
    Write-Host "  Type 'production' for Production (.env_production)" -ForegroundColor Yellow
    Write-Host "  Type 'test' for Test (.env_test)" -ForegroundColor Yellow
    Write-Host "  Type 'cancel' to abort" -ForegroundColor Red
    
    do {
        $choice = Read-Host "`nEnter environment (production/test/cancel)"
        $choice = $choice.ToLower().Trim()
        
        switch ($choice) {
            "production" { 
                $envFile = ".env_production"
                $envName = "Production"
                break
            }
            "test" { 
                $envFile = ".env_test"
                $envName = "Test"
                break
            }
            "cancel" { 
                Write-Host "❌ Environment setup cancelled" -ForegroundColor Red
                return
            }
            default { 
                Write-Host "❌ Invalid choice. Please enter 'production', 'test', or 'cancel'." -ForegroundColor Red
            }
        }
    } while ($choice -notin @("production", "test", "cancel"))

    # Load only the selected environment file
    $etlPath = "$ProjectPath\etl_pipeline"
    $envPath = "$etlPath\$envFile"
    
    if (Test-Path $envPath) {
        Write-Host "📄 Loading $envName environment from: $envFile" -ForegroundColor Green
        Get-Content $envPath | ForEach-Object {
            if ($_ -match '^([^#][^=]+)=(.*)$' -and $_ -notmatch '^\s*#') {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
                Write-Host "  Loaded: $name" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "❌ Environment file not found: $envPath" -ForegroundColor Red
        Write-Host "Please create $envFile from the template" -ForegroundColor Yellow
        Write-Host "Template location: $etlPath\docs\env_$($envFile.Replace('.env_', '')).template" -ForegroundColor Yellow
        return
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
# API ENVIRONMENT  
# =============================================================================

function Initialize-APIEnvironment {
    param([string]$ProjectPath = (Get-Location))

    if ($script:IsAPIActive) {
        Write-Host "❌ API environment already active. Use 'api-deactivate' first." -ForegroundColor Yellow
        return
    }

    if ($script:IsDBTActive) {
        Write-Host "❌ dbt environment is currently active. Run 'dbt-deactivate' first before activating API." -ForegroundColor Red
        return
    }

    if ($script:IsETLActive) {
        Write-Host "❌ ETL environment is currently active. Run 'etl-deactivate' first before activating API." -ForegroundColor Red
        return
    }

    $projectName = Split-Path -Leaf $ProjectPath
    Write-Host "`n🌐 Initializing API environment: $projectName" -ForegroundColor Blue

    # Check for API project structure
    $apiPath = "$ProjectPath\api"
    
    if (-not (Test-Path "$apiPath\main.py")) {
        Write-Host "❌ No main.py found in api directory" -ForegroundColor Red
        return
    }

    # Set up API virtual environment
    if (Test-Path "$apiPath\requirements.txt") {
        Push-Location $apiPath
        try {
            Write-Host "📦 Setting up API virtual environment..." -ForegroundColor Yellow
            
            # Create virtual environment if it doesn't exist
            if (-not (Test-Path "venv")) {
                Write-Host "🔧 Creating API virtual environment..." -ForegroundColor Yellow
                python -m venv venv
            }
            
            # Activate virtual environment
            $venvScripts = Join-Path (Get-Location) "venv\Scripts"
            if (Test-Path $venvScripts) {
                $script:VenvPath = Join-Path (Get-Location) "venv"
                $env:VIRTUAL_ENV = $script:VenvPath
                $env:PATH = "$venvScripts;$env:PATH"
                Write-Host "✅ API virtual environment activated" -ForegroundColor Green
                
                # Install dependencies
                Write-Host "📦 Installing API dependencies..." -ForegroundColor Yellow
                pip install -r requirements.txt 2>$null | Out-Null
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "✅ API dependencies installed successfully" -ForegroundColor Green
                } else {
                    Write-Host "❌ Failed to install API dependencies" -ForegroundColor Red
                    Pop-Location
                    return
                }
            } else {
                Write-Host "❌ Failed to activate API virtual environment" -ForegroundColor Red
                Pop-Location
                return
            }
        } catch {
            Write-Host "❌ Failed to set up API virtual environment: $_" -ForegroundColor Red
            Pop-Location
            return
        }
        Pop-Location
    } else {
        Write-Host "⚠️ No requirements.txt found in api directory - skipping dependency installation" -ForegroundColor Yellow
    }

    # Interactive Environment Selection
    Write-Host "`n🔧 API Environment Selection" -ForegroundColor Cyan
    Write-Host "Which environment would you like to use?" -ForegroundColor White
    Write-Host "  Type 'production' for Production (.env_api_production)" -ForegroundColor Yellow
    Write-Host "  Type 'test' for Test (.env_api_test)" -ForegroundColor Yellow
    Write-Host "  Type 'cancel' to abort" -ForegroundColor Red
    
    do {
        $choice = Read-Host "`nEnter environment (production/test/cancel)"
        $choice = $choice.ToLower().Trim()
        
        switch ($choice) {
            "production" { 
                $envFile = ".env_api_production"
                $envName = "Production"
                break
            }
            "test" { 
                $envFile = ".env_api_test"
                $envName = "Test"
                break
            }
            "cancel" { 
                Write-Host "❌ API environment setup cancelled" -ForegroundColor Red
                return
            }
            default { 
                Write-Host "❌ Invalid choice. Please enter 'production', 'test', or 'cancel'." -ForegroundColor Red
            }
        }
    } while ($choice -notin @("production", "test", "cancel"))

    # Load the selected API environment file
    $envPath = "$ProjectPath\$envFile"
    
    if (Test-Path $envPath) {
        Write-Host "📄 Loading $envName API environment from: $envFile" -ForegroundColor Green
        Get-Content $envPath | ForEach-Object {
            if ($_ -match '^([^#][^=]+)=(.*)$' -and $_ -notmatch '^\s*#') {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
                Write-Host "  Loaded: $name" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "❌ API environment file not found: $envPath" -ForegroundColor Red
        Write-Host "Please create $envFile in the project root" -ForegroundColor Yellow
        return
    }

    $script:IsAPIActive = $true
    $script:ActiveProject = $projectName

    Write-Host "`n✅ API environment ready!" -ForegroundColor Green
    Write-Host "Commands: api, api-test, api-docs, api-run" -ForegroundColor Cyan
    Write-Host "To switch to other environments: run 'api-deactivate' first`n" -ForegroundColor Gray
}

function Stop-APIEnvironment {
    if (-not $script:IsAPIActive) {
        Write-Host "❌ API environment not active" -ForegroundColor Yellow
        return
    }

    Write-Host "🔄 Deactivating API environment..." -ForegroundColor Yellow

    # Clean up API virtual environment
    if ($script:VenvPath) {
        $env:VIRTUAL_ENV = $null
        
        # Remove virtual environment from PATH
        $venvScripts = Join-Path $script:VenvPath "Scripts"
        if ($env:PATH -like "*$venvScripts*") {
            $env:PATH = $env:PATH.Replace("$venvScripts;", "").Replace(";$venvScripts", "").Replace($venvScripts, "")
        }
        Write-Host "✅ API virtual environment deactivated" -ForegroundColor Green
    }

    # Clean up API environment variables
    $apiEnvVars = @(
        'API_ENVIRONMENT',
        'TEST_POSTGRES_ANALYTICS_HOST',
        'TEST_POSTGRES_ANALYTICS_PORT', 
        'TEST_POSTGRES_ANALYTICS_DB',
        'TEST_POSTGRES_ANALYTICS_USER',
        'TEST_POSTGRES_ANALYTICS_PASSWORD',
        'POSTGRES_ANALYTICS_HOST',
        'POSTGRES_ANALYTICS_PORT',
        'POSTGRES_ANALYTICS_DB', 
        'POSTGRES_ANALYTICS_USER',
        'POSTGRES_ANALYTICS_PASSWORD',
        'API_CORS_ORIGINS',
        'API_DEBUG',
        'API_LOG_LEVEL',
        'API_PORT',
        'API_HOST',
        'API_SECRET_KEY',
        'API_ACCESS_TOKEN_EXPIRE_MINUTES'
    )

    foreach ($var in $apiEnvVars) {
        [Environment]::SetEnvironmentVariable($var, $null, 'Process')
    }

    $script:IsAPIActive = $false
    $script:ActiveProject = $null
    $script:VenvPath = $null

    Write-Host "✅ API environment deactivated - other environments can now be activated" -ForegroundColor Green
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
    # Also change to dbt project directory before running commands
    $currentLocation = Get-Location
    if (Test-Path "dbt_dental_clinic_prod") {
        Push-Location "dbt_dental_clinic_prod"
        try {
            pipenv run dbt $args
        } finally {
            Pop-Location
        }
    } else {
        pipenv run dbt $args
    }
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

# API Commands
function Invoke-API {
    if (-not $script:IsAPIActive) {
        Write-Host "❌ API environment not active. Run 'api-init' first." -ForegroundColor Red
        return
    }
    
    if (-not $args -or $args.Count -eq 0) {
        Show-APIHelp
        return
    }
    
    Write-Host "🌐 api $($args -join ' ')" -ForegroundColor Blue
    
    # Change to api directory
    $currentLocation = Get-Location
    if (Test-Path "api") {
        Push-Location "api"
        try {
            python -m uvicorn main:app $args
        } finally {
            Pop-Location
        }
    } else {
        Write-Host "❌ API directory not found" -ForegroundColor Red
    }
}

function Test-APIConfig {
    if (-not $script:IsAPIActive) {
        Write-Host "❌ API environment not active. Run 'api-init' first." -ForegroundColor Red
        return
    }
    
    Write-Host "🧪 Testing API configuration..." -ForegroundColor Blue
    
    # Change to api directory
    $currentLocation = Get-Location
    if (Test-Path "api") {
        Push-Location "api"
        try {
            # Use python from virtual environment if available
            python test_config.py
        } finally {
            Pop-Location
        }
    } else {
        Write-Host "❌ API directory not found" -ForegroundColor Red
    }
}

function Start-APIDocs {
    if (-not $script:IsAPIActive) {
        Write-Host "❌ API environment not active. Run 'api-init' first." -ForegroundColor Red
        return
    }
    
    Write-Host "📚 Opening API documentation..." -ForegroundColor Blue
    Start-Process "http://localhost:8000/docs"
}

function Start-APIServer {
    if (-not $script:IsAPIActive) {
        Write-Host "❌ API environment not active. Run 'api-init' first." -ForegroundColor Red
        return
    }
    
    $port = $env:API_PORT
    $apiHost = $env:API_HOST
    
    if (-not $port) { $port = "8000" }
    if (-not $apiHost) { $apiHost = "0.0.0.0" }
    
    Write-Host "🚀 Starting API server on $apiHost`:$port..." -ForegroundColor Blue
    
    # Check if we're already in the api directory or need to change to it
    $currentLocation = Get-Location
    $isInApiDir = (Test-Path "main.py") -and (Test-Path "config.py")
    
    if ($isInApiDir) {
        # We're already in the api directory
        Write-Host "📁 Running from api directory: $currentLocation" -ForegroundColor Gray
        try {
            # Use python from virtual environment if available
            python -m uvicorn main:app --host $apiHost --port $port --reload
        } catch {
            Write-Host "❌ Failed to start API server: $_" -ForegroundColor Red
        }
    } elseif (Test-Path "api\main.py") {
        # We're in the project root, change to api directory
        Push-Location "api"
        try {
            Write-Host "📁 Changed to api directory" -ForegroundColor Gray
            # Use python from virtual environment if available
            python -m uvicorn main:app --host $apiHost --port $port --reload
        } finally {
            Pop-Location
        }
    } else {
        Write-Host "❌ API directory not found. Expected to find main.py in current directory or api/ subdirectory." -ForegroundColor Red
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
    Write-Host "  • Use 'api-init' to switch to API environment"
    Write-Host ""
}

function Show-APIHelp {
    Write-Host "`n╔════════════════════════════════════════════════════════════════╗"
    Write-Host "║                        API Commands                             ║"
    Write-Host "╚════════════════════════════════════════════════════════════════╝`n"

    Write-Host "🚀 Core Operations:" -ForegroundColor White
    Write-Host "  api-run                           Start API server"
    Write-Host "  api-test                          Test API configuration"
    Write-Host "  api-docs                          Open API documentation"
    Write-Host "  api --reload                      Start with auto-reload"
    Write-Host ""

    Write-Host "🏥 Dental Clinic Endpoints:" -ForegroundColor White
    Write-Host "  GET /patients/                    List all patients"
    Write-Host "  GET /patients/{id}                Get patient by ID"
    Write-Host "  GET /reports/revenue/trends       Revenue analytics"
    Write-Host "  GET /reports/providers/performance Provider metrics"
    Write-Host ""

    Write-Host "🔍 Development:" -ForegroundColor White
    Write-Host "  api-test                          Test environment config"
    Write-Host "  api-docs                          Interactive API docs"
    Write-Host "  api --host 0.0.0.0 --port 8000   Custom host/port"
    Write-Host ""

    Write-Host "💡 Quick Tips:" -ForegroundColor Yellow
    Write-Host "  • API docs available at http://localhost:8000/docs"
    Write-Host "  • Use 'api-deactivate' to exit environment"
    Write-Host "  • Use 'etl-init' to switch to ETL environment"
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
        $environment = $env:ETL_ENVIRONMENT
        if ($environment) {
            Write-Host "  ETL Environment: $environment" -ForegroundColor Cyan
        }
    } else {
        Write-Host "  ETL: ⭕ Inactive" -ForegroundColor Gray
    }
    
    if ($script:IsAPIActive) {
        Write-Host "  API: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
        $environment = $env:API_ENVIRONMENT
        if ($environment) {
            Write-Host "  API Environment: $environment" -ForegroundColor Cyan
        }
    } else {
        Write-Host "  API: ⭕ Inactive" -ForegroundColor Gray
    }
    
    Write-Host ""
}

function Get-ETLEnvironmentStatus {
    if (-not $script:IsETLActive) {
        Write-Host "❌ ETL environment not active. Run 'etl-init' first." -ForegroundColor Red
        return
    }
    
    $environment = $env:ETL_ENVIRONMENT
    Write-Host "`n📊 ETL Environment Status:" -ForegroundColor White
    Write-Host "  Environment: $environment" -ForegroundColor Green
    Write-Host "  Active: ✅" -ForegroundColor Green
    Write-Host "  Project: $script:ActiveProject" -ForegroundColor Cyan
    
    # Show some key environment variables
    Write-Host "`n🔧 Key Environment Variables:" -ForegroundColor White
    if ($environment -eq "production") {
        Write-Host "  OPENDENTAL_SOURCE_DB: $($env:OPENDENTAL_SOURCE_DB)" -ForegroundColor Gray
        Write-Host "  OPENDENTAL_SOURCE_HOST: $($env:OPENDENTAL_SOURCE_HOST)" -ForegroundColor Gray
    } elseif ($environment -eq "test") {
        Write-Host "  TEST_OPENDENTAL_SOURCE_DB: $($env:TEST_OPENDENTAL_SOURCE_DB)" -ForegroundColor Gray
        Write-Host "  TEST_OPENDENTAL_SOURCE_HOST: $($env:TEST_OPENDENTAL_SOURCE_HOST)" -ForegroundColor Gray
    }
    Write-Host ""
}

function Get-APIEnvironmentStatus {
    if (-not $script:IsAPIActive) {
        Write-Host "❌ API environment not active. Run 'api-init' first." -ForegroundColor Red
        return
    }
    
    $environment = $env:API_ENVIRONMENT
    Write-Host "`n📊 API Environment Status:" -ForegroundColor White
    Write-Host "  Environment: $environment" -ForegroundColor Green
    Write-Host "  Active: ✅" -ForegroundColor Green
    Write-Host "  Project: $script:ActiveProject" -ForegroundColor Cyan
    
    # Show some key environment variables
    Write-Host "`n🔧 Key Environment Variables:" -ForegroundColor White
    if ($environment -eq "production") {
        Write-Host "  POSTGRES_ANALYTICS_DB: $($env:POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
        Write-Host "  POSTGRES_ANALYTICS_HOST: $($env:POSTGRES_ANALYTICS_HOST)" -ForegroundColor Gray
    } elseif ($environment -eq "test") {
        Write-Host "  TEST_POSTGRES_ANALYTICS_DB: $($env:TEST_POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
        Write-Host "  TEST_POSTGRES_ANALYTICS_HOST: $($env:TEST_POSTGRES_ANALYTICS_HOST)" -ForegroundColor Gray
    }
    Write-Host "  API_PORT: $($env:API_PORT)" -ForegroundColor Gray
    Write-Host "  API_DEBUG: $($env:API_DEBUG)" -ForegroundColor Gray
    Write-Host ""
}

# =============================================================================
# PROMPT CUSTOMIZATION
# =============================================================================

function global:prompt {
    $envTag = ""
    $envColor = "White"
    
    if (($script:IsDBTActive -and $script:IsETLActive) -or 
        ($script:IsDBTActive -and $script:IsAPIActive) -or 
        ($script:IsETLActive -and $script:IsAPIActive)) {
        # This should never happen with our mutual exclusion, but just in case
        $envTag = "[ERROR:MULTIPLE] "
        $envColor = "Red"
    } elseif ($script:IsDBTActive) {
        $envTag = "[dbt:$script:ActiveProject] "
        $envColor = "Green"
    } elseif ($script:IsETLActive) {
        $envTag = "[etl:$script:ActiveProject] "
        $envColor = "Magenta"
    } elseif ($script:IsAPIActive) {
        $envTag = "[api:$script:ActiveProject] "
        $envColor = "Blue"
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
Set-Alias -Name dbt-init -Value Initialize-DBTEnvironment -Scope Global
Set-Alias -Name dbt-deactivate -Value Stop-DBTEnvironment -Scope Global
Set-Alias -Name etl-init -Value Initialize-ETLEnvironment -Scope Global
Set-Alias -Name etl-deactivate -Value Stop-ETLEnvironment -Scope Global
Set-Alias -Name api-init -Value Initialize-APIEnvironment -Scope Global
Set-Alias -Name api-deactivate -Value Stop-APIEnvironment -Scope Global

# DBT Commands
Set-Alias -Name dbt -Value Invoke-DBT -Scope Global
Set-Alias -Name notebook -Value Start-Notebook -Scope Global
Set-Alias -Name format -Value Format-Code -Scope Global
Set-Alias -Name lint -Value Format-Code -Scope Global
Set-Alias -Name test -Value Test-DBT -Scope Global

# ETL Commands
Set-Alias -Name etl -Value Invoke-ETL -Scope Global
Set-Alias -Name etl-status -Value Get-ETLStatus -Scope Global
Set-Alias -Name etl-validate -Value Test-ETLValidation -Scope Global
Set-Alias -Name etl-run -Value Start-ETLPipeline -Scope Global
Set-Alias -Name etl-test -Value Test-ETLConnections -Scope Global
Set-Alias -Name etl-env-status -Value Get-ETLEnvironmentStatus -Scope Global

# API Commands
Set-Alias -Name api -Value Invoke-API -Scope Global
Set-Alias -Name api-test -Value Test-APIConfig -Scope Global
Set-Alias -Name api-docs -Value Start-APIDocs -Scope Global
Set-Alias -Name api-run -Value Start-APIServer -Scope Global
Set-Alias -Name api-env-status -Value Get-APIEnvironmentStatus -Scope Global

# Utility
Set-Alias -Name env-status -Value Get-EnvironmentStatus -Scope Global

# =============================================================================
# STARTUP MESSAGE
# =============================================================================

Write-Host "`n╔══════════════════════════════════════════════════════════╗" -ForegroundColor DarkBlue
Write-Host "║          Data Engineering Environment Manager           ║" -ForegroundColor Blue
Write-Host "║            Dental Clinic ETL & dbt Pipeline             ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor DarkBlue

Write-Host "`n🚀 Quick Start:" -ForegroundColor White
Write-Host "  dbt-init       - Initialize dbt environment" -ForegroundColor Cyan
Write-Host "  etl-init       - Initialize ETL environment (interactive)" -ForegroundColor Magenta
Write-Host "  api-init       - Initialize API environment (interactive)" -ForegroundColor Blue
Write-Host "  env-status     - Check environment status" -ForegroundColor Yellow

# Auto-detect project type
$cwd = Get-Location
if ((Test-Path "$cwd\dbt_project.yml") -or (Test-Path "$cwd\dbt_dental_clinic_prod\dbt_project.yml")) {
    Write-Host "`n🏗️  dbt project detected. Run 'dbt-init' to start." -ForegroundColor Green
}
if (Test-Path "$cwd\etl_pipeline\Pipfile") {
    Write-Host "🔄 ETL pipeline detected (Pipfile in etl_pipeline/). Run 'etl-init' to start." -ForegroundColor Magenta
}
if (Test-Path "$cwd\api\main.py") {
    Write-Host "🌐 API server detected (main.py in api/). Run 'api-init' to start (creates venv & installs deps)." -ForegroundColor Blue
}

Write-Host ""

# Load enhanced ETL functions for dental clinic operations
$etlFunctionsPath = Join-Path $PSScriptRoot "etl_functions.ps1"
if (Test-Path $etlFunctionsPath) {
    . $etlFunctionsPath
    Write-Host "🦷 Enhanced dental clinic ETL functions loaded" -ForegroundColor Green
}

# Export functions to global scope
Write-Host "🔧 Loading functions into global scope..." -ForegroundColor Yellow
Get-Command -Type Function | Where-Object {$_.Name -like "*ETL*" -or $_.Name -like "*DBT*"} | ForEach-Object {
    Set-Item -Path "function:global:$($_.Name)" -Value $_.Definition
}
Write-Host "✅ Functions loaded successfully!" -ForegroundColor Green 