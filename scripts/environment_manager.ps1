# Simplified Data Engineering Environment Manager
# Focused on dbt and ETL functionality for dental clinic pipelines

# Environment state tracking
$script:IsDBTActive = $false
$script:IsETLActive = $false
$script:IsAPIActive = $false
$script:ActiveProject = $null
$script:VenvPath = $null

# AWS SSM state tracking
$script:APIInstanceId = $null
$script:DemoDBInstanceId = $null
$script:RDSEndpoint = $null
$script:DemoDBHost = $null
$script:DemoDBPort = $null

# =============================================================================
# DBT ENVIRONMENT
# =============================================================================

function Initialize-DBTEnvironment {
    param(
        [string]$ProjectPath = (Get-Location),
        [ValidateSet('dev', 'demo')]
        [string]$Target = 'dev'
    )

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
    Write-Host "`n🏗️  Initializing dbt environment: $projectName (target: $Target)" -ForegroundColor Cyan

    # Verify dbt project - check both current directory and dbt_dental_clinic_prod subdirectory
    $dbtProjectPath = $ProjectPath
    if (Test-Path "$ProjectPath\dbt_dental_models\dbt_project.yml") {
        $dbtProjectPath = "$ProjectPath\dbt_dental_models"
        Write-Host "📁 Found dbt project in: dbt_dental_models/" -ForegroundColor Green
    } elseif (Test-Path "$ProjectPath\dbt_project.yml") {
        Write-Host "📁 Found dbt project in: current directory" -ForegroundColor Green
    } else {
        Write-Host "❌ No dbt_project.yml found in current directory or dbt_dental_models/" -ForegroundColor Red
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

    # Set DBT_TARGET to the chosen target
    [Environment]::SetEnvironmentVariable('DBT_TARGET', $Target, 'Process')
    Write-Host "🎯 Set DBT_TARGET to: $Target" -ForegroundColor Green

    # Load environment variables based on target
    if ($Target -eq 'demo') {
        # Demo target: Load from deployment_credentials.json
        Write-Host "📋 Loading demo database credentials from deployment_credentials.json..." -ForegroundColor Yellow
        $credentialsPath = "$ProjectPath\deployment_credentials.json"
        if (Test-Path $credentialsPath) {
            try {
                $credentials = Get-Content $credentialsPath | ConvertFrom-Json
                if ($credentials.demo_database.postgresql) {
                    $demo = $credentials.demo_database
                    
                    # Check if we're running locally (need port forwarding) or on EC2 (direct connection)
                    # For local development, use localhost with forwarded port
                    # For EC2, use private IP directly
                    $isLocal = $true  # Assume local unless we detect EC2 environment
                    $demoHost = "localhost"
                    $demoPort = "5434"  # Default forwarded port for demo DB
                    
                    # Check if we're on EC2 (has AWS metadata service)
                    try {
                        $ec2Metadata = Invoke-WebRequest -Uri "http://169.254.169.254/latest/meta-data/instance-id" -TimeoutSec 1 -ErrorAction Stop
                        if ($ec2Metadata.StatusCode -eq 200) {
                            $isLocal = $false
                            $demoHost = $demo.ec2.private_ip
                            $demoPort = $demo.postgresql.port.ToString()
                            Write-Host "🌐 Detected EC2 environment - using direct connection" -ForegroundColor Cyan
                        }
                    } catch {
                        # Not on EC2, use port forwarding
                        Write-Host "💻 Detected local environment - requires port forwarding" -ForegroundColor Cyan
                        Write-Host "⚠️  IMPORTANT: Start port forwarding first!" -ForegroundColor Yellow
                        Write-Host "   Run: aws-ssm-init" -ForegroundColor Gray
                        Write-Host "   Then: ssm-port-forward-demo-db" -ForegroundColor Gray
                        Write-Host "   (Keep that terminal open in the background)" -ForegroundColor Gray
                        Write-Host ""
                    }
                    
                    [Environment]::SetEnvironmentVariable('DEMO_POSTGRES_HOST', $demoHost, 'Process')
                    [Environment]::SetEnvironmentVariable('DEMO_POSTGRES_PORT', $demoPort, 'Process')
                    [Environment]::SetEnvironmentVariable('DEMO_POSTGRES_DB', $demo.postgresql.database, 'Process')
                    [Environment]::SetEnvironmentVariable('DEMO_POSTGRES_USER', $demo.postgresql.user, 'Process')
                    [Environment]::SetEnvironmentVariable('DEMO_POSTGRES_PASSWORD', $demo.postgresql.password, 'Process')
                    [Environment]::SetEnvironmentVariable('DEMO_POSTGRES_SCHEMA', 'raw', 'Process')
                    Write-Host "✅ Demo database credentials loaded:" -ForegroundColor Green
                    Write-Host "   Host: $demoHost" -ForegroundColor Gray
                    Write-Host "   Port: $demoPort" -ForegroundColor Gray
                    Write-Host "   Database: $($demo.postgresql.database)" -ForegroundColor Gray
                    Write-Host "   User: $($demo.postgresql.user)" -ForegroundColor Gray
                    Write-Host "   Target: demo" -ForegroundColor Gray
                } else {
                    Write-Host "⚠️ Demo database credentials not found in deployment_credentials.json" -ForegroundColor Yellow
                }
            } catch {
                Write-Host "❌ Failed to load demo credentials: $_" -ForegroundColor Red
            }
        } else {
            Write-Host "⚠️ deployment_credentials.json not found. Demo credentials not loaded." -ForegroundColor Yellow
        }
    } else {
        # Dev target: Load from .env_production or existing environment variables
        Write-Host "📋 Loading dev database credentials from environment files..." -ForegroundColor Yellow
        @(".env_production", ".dbt-env") | ForEach-Object {
            # Try dbt project directory first, then project root
            $envFile = "$dbtProjectPath\$_"
            if (-not (Test-Path $envFile)) {
                $envFile = "$ProjectPath\$_"
            }
            if (Test-Path $envFile) {
                Write-Host "   Loading from: $_" -ForegroundColor Gray
                Get-Content $envFile | ForEach-Object {
                    if ($_ -match '^([^#][^=]+)=(.*)$') {
                        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
                    }
                }
            }
        }
        
        # Verify required variables are set
        $requiredVars = @('POSTGRES_ANALYTICS_HOST', 'POSTGRES_ANALYTICS_PORT', 'POSTGRES_ANALYTICS_DB', 'POSTGRES_ANALYTICS_USER', 'POSTGRES_ANALYTICS_PASSWORD')
        $missingVars = @()
        foreach ($var in $requiredVars) {
            if (-not [Environment]::GetEnvironmentVariable($var, 'Process')) {
                $missingVars += $var
            }
        }
        if ($missingVars.Count -gt 0) {
            Write-Host "⚠️ Missing required environment variables: $($missingVars -join ', ')" -ForegroundColor Yellow
            Write-Host "   Set these variables or add them to .env_production" -ForegroundColor Gray
        } else {
            Write-Host "✅ Dev database credentials loaded" -ForegroundColor Green
        }
    }

    $script:IsDBTActive = $true
    $script:ActiveProject = $projectName

    Write-Host "`n✅ dbt environment ready! (target: $Target)" -ForegroundColor Green
    Write-Host "Commands: dbt, notebook, format, lint, test" -ForegroundColor Cyan
    Write-Host "To switch targets: run 'dbt-deactivate', then 'dbt-init -Target dev' or 'dbt-init -Target demo'" -ForegroundColor Gray
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
    [Environment]::SetEnvironmentVariable('DBT_TARGET', $null, 'Process')
    Write-Host "🔧 Cleared DBT_PROFILES_DIR and DBT_TARGET" -ForegroundColor Green

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
    Write-Host "  Type 'local' for Local Development (.env_api_local) - opendental_analytics on localhost (PHI)" -ForegroundColor Yellow
    Write-Host "  Type 'production' for Demo API (.env_api_production) - opendental_demo (synthetic data)" -ForegroundColor Yellow
    Write-Host "    - EC2 deployment: api.dbtdentalclinic.com" -ForegroundColor Gray
    Write-Host "    - Local testing: Change POSTGRES_ANALYTICS_HOST to localhost" -ForegroundColor Gray
    Write-Host "  Type 'test' for Test (.env_api_test)" -ForegroundColor Yellow
    Write-Host "  Type 'cancel' to abort" -ForegroundColor Red
    
    do {
        $choice = Read-Host "`nEnter environment (local/production/test/cancel)"
        $choice = $choice.ToLower().Trim()
        
        switch ($choice) {
            "local" { 
                $envFile = ".env_api_local"
                $envName = "Local"
                break
            }
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
                Write-Host "❌ Invalid choice. Please enter 'local', 'production', 'test', or 'cancel'." -ForegroundColor Red
            }
        }
    } while ($choice -notin @("local", "production", "test", "cancel"))

    # Load the selected API environment file
    $apiPath = "$ProjectPath\api"
    $envPath = "$apiPath\$envFile"
    
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
        Write-Host "Please create $envFile in the api/ directory" -ForegroundColor Yellow
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
        'DEMO_POSTGRES_HOST',
        'DEMO_POSTGRES_PORT',
        'DEMO_POSTGRES_DB',
        'DEMO_POSTGRES_USER',
        'DEMO_POSTGRES_PASSWORD',
        'DEMO_API_KEY',
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
    
    # Check if target is specified in args, otherwise use DBT_TARGET env var
    $target = [Environment]::GetEnvironmentVariable('DBT_TARGET', 'Process')
    $targetSpecified = $false
    $newArgs = @()
    foreach ($arg in $args) {
        if ($arg -match '^--target=(.+)$' -or $arg -eq '--target') {
            $targetSpecified = $true
            $newArgs += $arg
        } elseif ($arg -eq '-t' -or $arg -eq '--target') {
            $targetSpecified = $true
            $newArgs += $arg
        } else {
            $newArgs += $arg
        }
    }
    
    # If no target specified in args and DBT_TARGET is set, add it
    if (-not $targetSpecified -and $target) {
        $newArgs += '--target', $target
        Write-Host "🎯 Using target: $target (from dbt-init)" -ForegroundColor Gray
    }
    
    Write-Host "🚀 dbt $($newArgs -join ' ')" -ForegroundColor Cyan
    # FIXED: Use pipenv run to avoid infinite recursion with dbt alias
    # Also change to dbt project directory before running commands
    $currentLocation = Get-Location
    if (Test-Path "dbt_dental_models") {
        Push-Location "dbt_dental_models"
        try {
            pipenv run dbt $newArgs
        } finally {
            Pop-Location
        }
    } else {
        pipenv run dbt $newArgs
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
    
    # Set API_ENVIRONMENT=local for local development with opendental_analytics database
    if (-not $env:API_ENVIRONMENT) {
        $env:API_ENVIRONMENT = "local"
        Write-Host "🔧 Set API_ENVIRONMENT=local (for opendental_analytics database)" -ForegroundColor Cyan
    } else {
        Write-Host "🔧 Using API_ENVIRONMENT=$env:API_ENVIRONMENT" -ForegroundColor Cyan
    }
    
    Write-Host "🚀 Starting API server on $apiHost`:$port..." -ForegroundColor Blue
    Write-Host "   Database: opendental_analytics (local development)" -ForegroundColor Gray
    
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
# FRONTEND COMMANDS
# =============================================================================

function Start-FrontendDev {
    $projectPath = Get-Location
    $frontendPath = "$projectPath\frontend"
    
    if (-not (Test-Path $frontendPath)) {
        Write-Host "❌ Frontend directory not found: $frontendPath" -ForegroundColor Red
        return
    }
    
    if (-not (Test-Path "$frontendPath\package.json")) {
        Write-Host "❌ No package.json found in frontend directory" -ForegroundColor Red
        return
    }
    
    Write-Host "🚀 Starting frontend development server (LOCAL environment)..." -ForegroundColor Cyan
    
    # Setup local environment configuration
    $envLocalFile = "$frontendPath\.env.local"
    $apiKeyFile = "$projectPath\.ssh\dbt-dental-clinic-api-key.pem"
    
    # Read API key from .pem file for local development
    $apiKey = $null
    if (Test-Path $apiKeyFile) {
        try {
            $apiKey = (Get-Content $apiKeyFile -Raw).Trim()
            # Remove PEM headers/footers if present
            $apiKey = $apiKey -replace '-----BEGIN[^-]+-----', '' -replace '-----END[^-]+-----', '' -replace '\s', ''
        } catch {
            Write-Host "⚠️  Could not read API key file: $_" -ForegroundColor Yellow
        }
    }
    
    # Create or update .env.local with local development settings
    $envLocalContent = @"
# Frontend Local Development Environment
# Auto-generated by frontend-dev command
# This file is for LOCAL development only (opendental_analytics on localhost)

# API Configuration for Local Development
VITE_API_URL=http://localhost:8000
"@
    
    if ($apiKey) {
        $envLocalContent += "`nVITE_API_KEY=$apiKey"
        Write-Host "✅ API key loaded from .ssh/dbt-dental-clinic-api-key.pem" -ForegroundColor Green
    } else {
        Write-Host "⚠️  API key file not found at: $apiKeyFile" -ForegroundColor Yellow
        Write-Host "   Frontend will not be able to authenticate with the API." -ForegroundColor Yellow
        Write-Host "   Ensure the API key file exists or run 'frontend\setup_api_key.ps1' manually." -ForegroundColor Yellow
    }
    
    # Write .env.local file
    try {
        Set-Content -Path $envLocalFile -Value $envLocalContent -Force
        Write-Host "✅ Created/updated .env.local for local development" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Could not write .env.local file: $_" -ForegroundColor Yellow
    }
    
    Push-Location $frontendPath
    try {
        # Check if node_modules exists, if not run npm install
        if (-not (Test-Path "node_modules")) {
            Write-Host "`n📦 Installing frontend dependencies..." -ForegroundColor Yellow
            npm install
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
                Pop-Location
                return
            }
        }
        
        Write-Host "`n🔧 Starting Vite dev server..." -ForegroundColor Green
        Write-Host "   Frontend will be available at http://localhost:3000" -ForegroundColor Gray
        Write-Host "   API URL: http://localhost:8000 (local development)" -ForegroundColor Gray
        Write-Host "   Database: opendental_analytics (local)" -ForegroundColor Gray
        if ($apiKey) {
            Write-Host "   ✅ API key configured - API requests should work" -ForegroundColor Green
        } else {
            Write-Host "   ⚠️  API key not configured - API requests will fail with 401" -ForegroundColor Yellow
        }
        Write-Host ""
        npm run dev
    } catch {
        Write-Host "❌ Failed to start frontend dev server: $_" -ForegroundColor Red
    } finally {
        Pop-Location
    }
}

function Deploy-Frontend {
    $projectPath = Get-Location
    $frontendPath = "$projectPath\frontend"
    
    if (-not (Test-Path $frontendPath)) {
        Write-Host "❌ Frontend directory not found: $frontendPath" -ForegroundColor Red
        return
    }
    
    if (-not (Test-Path "$frontendPath\package.json")) {
        Write-Host "❌ No package.json found in frontend directory" -ForegroundColor Red
        return
    }
    
    Write-Host "🚀 Deploying frontend to AWS..." -ForegroundColor Cyan
    
    # Validate prerequisites
    Write-Host "`n🔍 Validating prerequisites..." -ForegroundColor Yellow
    
    # Check AWS CLI
    try {
        $awsVersion = aws --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ AWS CLI not found. Please install AWS CLI first." -ForegroundColor Red
            return
        }
        Write-Host "✅ AWS CLI found: $awsVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ AWS CLI not found. Please install AWS CLI first." -ForegroundColor Red
        return
    }
    
    # Check AWS credentials
    try {
        $awsIdentity = aws sts get-caller-identity 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
            return
        }
        Write-Host "✅ AWS credentials configured" -ForegroundColor Green
    } catch {
        Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
        return
    }
    
    # Check Node.js and npm
    try {
        $nodeVersion = node --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Node.js not found. Please install Node.js first." -ForegroundColor Red
            return
        }
        Write-Host "✅ Node.js found: $nodeVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ Node.js not found. Please install Node.js first." -ForegroundColor Red
        return
    }
    
    try {
        $npmVersion = npm --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ npm not found. Please install npm first." -ForegroundColor Red
            return
        }
        Write-Host "✅ npm found: $npmVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ npm not found. Please install npm first." -ForegroundColor Red
        return
    }
    
    # Get configuration from environment variables or deployment_credentials.json
    $bucketName = $env:FRONTEND_BUCKET_NAME
    $distributionId = $env:FRONTEND_DIST_ID
    $domain = $env:FRONTEND_DOMAIN
    
    # Try to load from deployment_credentials.json if env vars not set
    if (-not $bucketName -or -not $distributionId) {
        $credentialsPath = "$projectPath\deployment_credentials.json"
        if (Test-Path $credentialsPath) {
            try {
                $credentials = Get-Content $credentialsPath | ConvertFrom-Json
                if (-not $bucketName) {
                    $bucketName = $credentials.frontend.s3_buckets.frontend.bucket_name
                }
                if (-not $distributionId) {
                    $distributionId = $credentials.frontend.cloudfront.distribution_id
                }
                if (-not $domain) {
                    $domain = "https://$($credentials.frontend.domain)"
                }
                Write-Host "✅ Loaded configuration from deployment_credentials.json" -ForegroundColor Green
            } catch {
                Write-Host "⚠️ Failed to parse deployment_credentials.json: $_" -ForegroundColor Yellow
            }
        }
    }
    
    # Validate configuration
    if (-not $bucketName) {
        Write-Host "❌ FRONTEND_BUCKET_NAME not set. Set environment variable or ensure deployment_credentials.json exists." -ForegroundColor Red
        return
    }
    
    if (-not $distributionId) {
        Write-Host "❌ FRONTEND_DIST_ID not set. Set environment variable or ensure deployment_credentials.json exists." -ForegroundColor Red
        return
    }
    
    Write-Host "`n📋 Deployment Configuration:" -ForegroundColor Cyan
    Write-Host "  Bucket: $bucketName" -ForegroundColor White
    Write-Host "  CloudFront Distribution: $distributionId" -ForegroundColor White
    if ($domain) {
        Write-Host "  Domain: $domain" -ForegroundColor White
    }
    
    # Validate S3 bucket exists
    Write-Host "`n🔍 Validating S3 bucket..." -ForegroundColor Yellow
    try {
        $bucketCheck = aws s3api head-bucket --bucket $bucketName 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ S3 bucket '$bucketName' not found or not accessible" -ForegroundColor Red
            return
        }
        Write-Host "✅ S3 bucket validated" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to validate S3 bucket: $_" -ForegroundColor Red
        return
    }
    
    # Validate CloudFront distribution
    Write-Host "🔍 Validating CloudFront distribution..." -ForegroundColor Yellow
    try {
        $distCheck = aws cloudfront get-distribution --id $distributionId 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ CloudFront distribution '$distributionId' not found or not accessible" -ForegroundColor Red
            return
        }
        Write-Host "✅ CloudFront distribution validated" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to validate CloudFront distribution: $_" -ForegroundColor Red
        return
    }
    
    # Load DEMO_API_KEY from .env_api_production for production build
    Write-Host "`n🔑 Loading production API configuration..." -ForegroundColor Yellow
    $apiProductionEnvFile = "$projectPath\api\.env_api_production"
    $demoApiKey = $null
    
    if (Test-Path $apiProductionEnvFile) {
        Get-Content $apiProductionEnvFile | ForEach-Object {
            if ($_ -match '^DEMO_API_KEY\s*=\s*(.+)$' -and $_ -notmatch '^\s*#') {
                $demoApiKey = $matches[1].Trim()
            }
        }
    }
    
    # Also check environment variable (in case it's already set)
    if (-not $demoApiKey) {
        $demoApiKey = $env:DEMO_API_KEY
    }
    
    if (-not $demoApiKey) {
        Write-Host "❌ DEMO_API_KEY not found in .env_api_production or environment variables" -ForegroundColor Red
        Write-Host "   Production build requires DEMO_API_KEY for API authentication" -ForegroundColor Yellow
        return
    }
    
    Write-Host "✅ DEMO_API_KEY loaded for production build" -ForegroundColor Green
    
    # Build frontend
    Push-Location $frontendPath
    try {
        Write-Host "`n📦 Building frontend with production configuration..." -ForegroundColor Yellow
        Write-Host "   API URL: https://api.dbtdentalclinic.com" -ForegroundColor Gray
        Write-Host "   Database: opendental_demo (production)" -ForegroundColor Gray
        
        # Install dependencies if needed
        if (-not (Test-Path "node_modules")) {
            Write-Host "📦 Installing dependencies..." -ForegroundColor Yellow
            npm install
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
                Pop-Location
                return
            }
        }
        
        # Set production environment variables for Vite build
        $env:VITE_API_URL = "https://api.dbtdentalclinic.com"
        $env:VITE_API_KEY = $demoApiKey
        
        Write-Host "🔧 Building with production environment variables..." -ForegroundColor Cyan
        
        # Build
        npm run build
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Frontend build failed" -ForegroundColor Red
            Pop-Location
            return
        }
        
        Write-Host "✅ Frontend build completed" -ForegroundColor Green
        
        # Upload to S3
        Write-Host "`n☁️ Uploading to S3..." -ForegroundColor Yellow
        
        # Verify we're in the frontend directory and construct absolute path to dist
        $currentDir = (Get-Location).Path
        $distPath = Join-Path $currentDir "dist"
        
        if (-not (Test-Path $distPath)) {
            Write-Host "❌ Build directory 'dist' not found at: $distPath" -ForegroundColor Red
            Write-Host "  Current directory contents:" -ForegroundColor Yellow
            Get-ChildItem | Select-Object Name, PSIsContainer | Format-Table
            Pop-Location
            return
        }
        
        if (-not (Test-Path $distPath -PathType Container)) {
            Write-Host "❌ Path exists but is not a directory: $distPath" -ForegroundColor Red
            Pop-Location
            return
        }
        
        # Change to dist directory using absolute path
        Push-Location $distPath
        try {
            $distContents = Get-ChildItem
            if ($distContents.Count -eq 0) {
                Write-Host "⚠️ Warning: dist directory is empty - nothing to upload" -ForegroundColor Yellow
                Pop-Location
                Pop-Location
                return
            }
            
            # Upload static assets with long cache (immutable)
            Write-Host "  Uploading static assets..." -ForegroundColor Gray
            $syncOutput = & aws s3 sync . "s3://$bucketName/" --delete --cache-control "public, max-age=31536000, immutable" --exclude "*.html" --exclude "*.json" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to upload static assets" -ForegroundColor Red
                Write-Host "  Error: $syncOutput" -ForegroundColor Red
                Pop-Location
                Pop-Location
                return
            }
            
            # Upload HTML and JSON files with no-cache
            Write-Host "  Uploading HTML and JSON files..." -ForegroundColor Gray
            $syncOutput = & aws s3 sync . "s3://$bucketName/" --cache-control "no-cache, no-store, must-revalidate" --exclude "*" --include "*.html" --include "*.json" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to upload HTML files" -ForegroundColor Red
                Write-Host "  Error: $syncOutput" -ForegroundColor Red
                Pop-Location
                Pop-Location
                return
            }
            
            Write-Host "✅ Files uploaded to S3" -ForegroundColor Green
        } finally {
            Pop-Location
        }
        
        # Invalidate CloudFront cache
        Write-Host "`n🔄 Invalidating CloudFront cache..." -ForegroundColor Yellow
        $invalidation = aws cloudfront create-invalidation --distribution-id $distributionId --paths "/*" 2>&1 | ConvertFrom-Json
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ CloudFront cache invalidation created: $($invalidation.Invalidation.Id)" -ForegroundColor Green
        } else {
            Write-Host "⚠️ Failed to create CloudFront invalidation (deployment may still be successful)" -ForegroundColor Yellow
        }
        
        Write-Host "`n✅ Frontend deployment completed!" -ForegroundColor Green
        if ($domain) {
            Write-Host "🌐 Frontend available at: $domain" -ForegroundColor Cyan
        }
        
    } catch {
        Write-Host "❌ Deployment failed: $_" -ForegroundColor Red
    } finally {
        Pop-Location
    }
}

function Get-FrontendStatus {
    $projectPath = Get-Location
    
    Write-Host "`n📊 Frontend Deployment Status:" -ForegroundColor White
    
    # Check configuration
    $bucketName = $env:FRONTEND_BUCKET_NAME
    $distributionId = $env:FRONTEND_DIST_ID
    $domain = $env:FRONTEND_DOMAIN
    
    # Try to load from deployment_credentials.json
    $credentialsPath = "$projectPath\deployment_credentials.json"
    if (Test-Path $credentialsPath) {
        try {
            $credentials = Get-Content $credentialsPath | ConvertFrom-Json
            if (-not $bucketName) {
                $bucketName = $credentials.frontend.s3_buckets.frontend.bucket_name
            }
            if (-not $distributionId) {
                $distributionId = $credentials.frontend.cloudfront.distribution_id
            }
            if (-not $domain) {
                $domain = "https://$($credentials.frontend.domain)"
            }
        } catch {
            Write-Host "⚠️ Failed to parse deployment_credentials.json" -ForegroundColor Yellow
        }
    }
    
    Write-Host "`n🔧 Configuration:" -ForegroundColor White
    if ($bucketName) {
        Write-Host "  S3 Bucket: $bucketName" -ForegroundColor Green
    } else {
        Write-Host "  S3 Bucket: ❌ Not configured" -ForegroundColor Red
    }
    
    if ($distributionId) {
        Write-Host "  CloudFront Distribution: $distributionId" -ForegroundColor Green
    } else {
        Write-Host "  CloudFront Distribution: ❌ Not configured" -ForegroundColor Red
    }
    
    if ($domain) {
        Write-Host "  Domain: $domain" -ForegroundColor Green
    } else {
        Write-Host "  Domain: ❌ Not configured" -ForegroundColor Red
    }
    
    # Check prerequisites
    Write-Host "`n🔍 Prerequisites:" -ForegroundColor White
    
    # AWS CLI
    try {
        $awsVersion = aws --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  AWS CLI: ✅ $awsVersion" -ForegroundColor Green
        } else {
            Write-Host "  AWS CLI: ❌ Not found" -ForegroundColor Red
        }
    } catch {
        Write-Host "  AWS CLI: ❌ Not found" -ForegroundColor Red
    }
    
    # Node.js
    try {
        $nodeVersion = node --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  Node.js: ✅ $nodeVersion" -ForegroundColor Green
        } else {
            Write-Host "  Node.js: ❌ Not found" -ForegroundColor Red
        }
    } catch {
        Write-Host "  Node.js: ❌ Not found" -ForegroundColor Red
    }
    
    # npm
    try {
        $npmVersion = npm --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  npm: ✅ $npmVersion" -ForegroundColor Green
        } else {
            Write-Host "  npm: ❌ Not found" -ForegroundColor Red
        }
    } catch {
        Write-Host "  npm: ❌ Not found" -ForegroundColor Red
    }
    
    Write-Host ""
}

function Deploy-ClinicFrontend {
    <#
    .SYNOPSIS
    Deploy clinic production frontend to AWS S3/CloudFront with IP restrictions.
    
    .DESCRIPTION
    Builds and deploys the clinic production frontend to clinic.dbtdentalclinic.com.
    This frontend connects to the production API (api-clinic.dbtdentalclinic.com) 
    which accesses opendental_analytics database with real PHI.
    
    .EXAMPLE
    Deploy-ClinicFrontend
    #>
    $projectPath = Get-Location
    $frontendPath = "$projectPath\frontend"
    
    if (-not (Test-Path $frontendPath)) {
        Write-Host "❌ Frontend directory not found: $frontendPath" -ForegroundColor Red
        return
    }
    
    Write-Host "`n🏥 Deploying Clinic Production Frontend" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    # Check AWS credentials
    try {
        $awsIdentity = aws sts get-caller-identity 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
            return
        }
        Write-Host "✅ AWS credentials configured" -ForegroundColor Green
    } catch {
        Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
        return
    }
    
    # Check Node.js and npm
    try {
        $nodeVersion = node --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Node.js not found. Please install Node.js first." -ForegroundColor Red
            return
        }
        Write-Host "✅ Node.js found: $nodeVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ Node.js not found. Please install Node.js first." -ForegroundColor Red
        return
    }
    
    try {
        $npmVersion = npm --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ npm not found. Please install npm first." -ForegroundColor Red
            return
        }
        Write-Host "✅ npm found: $npmVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ npm not found. Please install npm first." -ForegroundColor Red
        return
    }
    
    # Get configuration from environment variables or deployment_credentials.json
    $bucketName = $env:CLINIC_FRONTEND_BUCKET_NAME
    $distributionId = $env:CLINIC_FRONTEND_DIST_ID
    $domain = $env:CLINIC_FRONTEND_DOMAIN
    $apiUrl = $env:CLINIC_API_URL
    $apiKey = $env:CLINIC_API_KEY
    
    # Try to load from deployment_credentials.json if env vars not set
    if (-not $bucketName -or -not $distributionId) {
        $credentialsPath = "$projectPath\deployment_credentials.json"
        if (Test-Path $credentialsPath) {
            try {
                $credentials = Get-Content $credentialsPath | ConvertFrom-Json
                if (-not $bucketName) {
                    $bucketName = $credentials.frontend.s3_buckets.clinic_frontend.bucket_name
                }
                if (-not $distributionId) {
                    $distributionId = $credentials.frontend.cloudfront.clinic_distribution_id
                }
                if (-not $domain) {
                    $domain = "https://clinic.$($credentials.frontend.domain)"
                }
                if (-not $apiUrl) {
                    $apiUrl = "https://api-clinic.$($credentials.frontend.domain)"
                }
                Write-Host "✅ Loaded configuration from deployment_credentials.json" -ForegroundColor Green
            } catch {
                Write-Host "⚠️ Failed to parse deployment_credentials.json: $_" -ForegroundColor Yellow
            }
        }
    }
    
    # Validate configuration
    if (-not $bucketName) {
        Write-Host "❌ CLINIC_FRONTEND_BUCKET_NAME not set. Set environment variable or add to deployment_credentials.json." -ForegroundColor Red
        return
    }
    
    if (-not $distributionId) {
        Write-Host "❌ CLINIC_FRONTEND_DIST_ID not set. Set environment variable or add to deployment_credentials.json." -ForegroundColor Red
        return
    }
    
    if (-not $apiUrl) {
        Write-Host "❌ CLINIC_API_URL not set. Set environment variable or add to deployment_credentials.json." -ForegroundColor Red
        return
    }
    
    if (-not $apiKey) {
        Write-Host "❌ CLINIC_API_KEY not set. This is required for clinic API authentication." -ForegroundColor Red
        Write-Host "   Set environment variable CLINIC_API_KEY with the clinic API key." -ForegroundColor Yellow
        return
    }
    
    Write-Host "`n📋 Deployment Configuration:" -ForegroundColor Cyan
    Write-Host "  Bucket: $bucketName" -ForegroundColor White
    Write-Host "  CloudFront Distribution: $distributionId" -ForegroundColor White
    Write-Host "  API URL: $apiUrl" -ForegroundColor White
    if ($domain) {
        Write-Host "  Domain: $domain" -ForegroundColor White
    }
    
    # Validate S3 bucket exists
    Write-Host "`n🔍 Validating S3 bucket..." -ForegroundColor Yellow
    try {
        $bucketCheck = aws s3api head-bucket --bucket $bucketName 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ S3 bucket '$bucketName' not found or not accessible" -ForegroundColor Red
            Write-Host "   Create the bucket first: aws s3 mb s3://$bucketName --region us-east-1" -ForegroundColor Yellow
            return
        }
        Write-Host "✅ S3 bucket validated" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to validate S3 bucket: $_" -ForegroundColor Red
        return
    }
    
    # Validate CloudFront distribution
    Write-Host "🔍 Validating CloudFront distribution..." -ForegroundColor Yellow
    try {
        $distCheck = aws cloudfront get-distribution --id $distributionId 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ CloudFront distribution '$distributionId' not found or not accessible" -ForegroundColor Red
            return
        }
        Write-Host "✅ CloudFront distribution validated" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to validate CloudFront distribution: $_" -ForegroundColor Red
        return
    }
    
    Write-Host "✅ CLINIC_API_KEY loaded for clinic build" -ForegroundColor Green
    
    # Build frontend
    Push-Location $frontendPath
    try {
        Write-Host "`n📦 Building clinic frontend with production configuration..." -ForegroundColor Yellow
        Write-Host "   API URL: $apiUrl" -ForegroundColor Gray
        Write-Host "   Database: opendental_analytics (production - real PHI)" -ForegroundColor Gray
        Write-Host "   Access: IP-restricted (clinic only)" -ForegroundColor Gray
        
        # Install dependencies if needed
        if (-not (Test-Path "node_modules")) {
            Write-Host "📦 Installing dependencies..." -ForegroundColor Yellow
            npm install
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
                Pop-Location
                return
            }
        }
        
        # Set production environment variables for Vite build
        $env:VITE_API_URL = $apiUrl
        $env:VITE_API_KEY = $apiKey
        $env:VITE_IS_DEMO = "false"
        
        Write-Host "🔧 Building with clinic environment variables..." -ForegroundColor Cyan
        
        # Build
        npm run build
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Frontend build failed" -ForegroundColor Red
            Pop-Location
            return
        }
        
        Write-Host "✅ Frontend build completed" -ForegroundColor Green
        
        # Upload to S3
        Write-Host "`n☁️ Uploading to S3..." -ForegroundColor Yellow
        
        # Verify we're in the frontend directory and construct absolute path to dist
        $currentDir = (Get-Location).Path
        $distPath = Join-Path $currentDir "dist"
        
        if (-not (Test-Path $distPath)) {
            Write-Host "❌ Build directory 'dist' not found at: $distPath" -ForegroundColor Red
            Write-Host "  Current directory contents:" -ForegroundColor Yellow
            Get-ChildItem | Select-Object Name, PSIsContainer | Format-Table
            Pop-Location
            return
        }
        
        if (-not (Test-Path $distPath -PathType Container)) {
            Write-Host "❌ Path exists but is not a directory: $distPath" -ForegroundColor Red
            Pop-Location
            return
        }
        
        # Change to dist directory using absolute path
        Push-Location $distPath
        try {
            $distContents = Get-ChildItem
            if ($distContents.Count -eq 0) {
                Write-Host "⚠️ Warning: dist directory is empty - nothing to upload" -ForegroundColor Yellow
                Pop-Location
                Pop-Location
                return
            }
            
            # Upload static assets with long cache (immutable)
            Write-Host "  Uploading static assets..." -ForegroundColor Gray
            $syncOutput = & aws s3 sync . "s3://$bucketName/" --delete --cache-control "public, max-age=31536000, immutable" --exclude "*.html" --exclude "*.json" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to upload static assets" -ForegroundColor Red
                Write-Host "  Error: $syncOutput" -ForegroundColor Red
                Pop-Location
                Pop-Location
                return
            }
            
            # Upload HTML and JSON files with no-cache
            Write-Host "  Uploading HTML and JSON files..." -ForegroundColor Gray
            $syncOutput = & aws s3 sync . "s3://$bucketName/" --cache-control "no-cache, no-store, must-revalidate" --exclude "*" --include "*.html" --include "*.json" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to upload HTML files" -ForegroundColor Red
                Write-Host "  Error: $syncOutput" -ForegroundColor Red
                Pop-Location
                Pop-Location
                return
            }
            
            Write-Host "✅ Files uploaded to S3" -ForegroundColor Green
        } finally {
            Pop-Location
        }
        
        # Invalidate CloudFront cache
        Write-Host "`n🔄 Invalidating CloudFront cache..." -ForegroundColor Yellow
        $invalidation = aws cloudfront create-invalidation --distribution-id $distributionId --paths "/*" 2>&1 | ConvertFrom-Json
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ CloudFront cache invalidation created: $($invalidation.Invalidation.Id)" -ForegroundColor Green
        } else {
            Write-Host "⚠️ Failed to create CloudFront invalidation (deployment may still be successful)" -ForegroundColor Yellow
        }
        
        Write-Host "`n✅ Clinic frontend deployment completed!" -ForegroundColor Green
        Write-Host "⚠️  IMPORTANT: This frontend is IP-restricted. Verify WAF rules are configured correctly." -ForegroundColor Yellow
        if ($domain) {
            Write-Host "🌐 Clinic frontend available at: $domain (clinic IPs only)" -ForegroundColor Cyan
        }
        
    } catch {
        Write-Host "❌ Deployment failed: $_" -ForegroundColor Red
    } finally {
        Pop-Location
    }
}

function Deploy-DBTDocs {
    $projectPath = Get-Location
    $dbtProjectPath = "$projectPath\dbt_dental_models"
    
    if (-not (Test-Path $dbtProjectPath)) {
        Write-Host "❌ dbt_dental_models directory not found: $dbtProjectPath" -ForegroundColor Red
        return
    }
    
    # Check if dbt docs have been generated
    $targetPath = "$dbtProjectPath\target"
    if (-not (Test-Path $targetPath) -or -not (Test-Path "$targetPath\index.html")) {
        Write-Host "⚠️  dbt docs not found. Generating docs now..." -ForegroundColor Yellow
        Write-Host "   This requires dbt to be initialized." -ForegroundColor Gray
        
        # Check if dbt is available
        $dbtAvailable = $false
        try {
            $dbtVersion = dbt --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                $dbtAvailable = $true
                Write-Host "✅ dbt is available" -ForegroundColor Green
            }
        } catch {
            Write-Host "❌ dbt command not found" -ForegroundColor Red
        }
        
        if (-not $dbtAvailable) {
            Write-Host "`n❌ dbt is not initialized or not in PATH." -ForegroundColor Red
            Write-Host "   Please run 'dbt-init' first, then 'dbt docs generate' in the dbt_dental_models directory." -ForegroundColor Yellow
            Write-Host "   Or manually run: cd dbt_dental_models && dbt docs generate" -ForegroundColor Yellow
            return
        }
        
        # Try to generate docs
        Push-Location $dbtProjectPath
        try {
            Write-Host "`n📚 Generating dbt docs..." -ForegroundColor Cyan
            dbt docs generate
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to generate dbt docs" -ForegroundColor Red
                Pop-Location
                return
            }
            Write-Host "✅ dbt docs generated successfully" -ForegroundColor Green
        } catch {
            Write-Host "❌ Error generating dbt docs: $_" -ForegroundColor Red
            Pop-Location
            return
        } finally {
            Pop-Location
        }
        
        # Verify docs were generated
        if (-not (Test-Path "$targetPath\index.html")) {
            Write-Host "❌ dbt docs generation completed but index.html not found" -ForegroundColor Red
            return
        }
    } else {
        Write-Host "✅ Found existing dbt docs in: $targetPath" -ForegroundColor Green
    }
    
    Write-Host "🚀 Deploying dbt docs to AWS..." -ForegroundColor Cyan
    
    # Validate prerequisites
    Write-Host "`n🔍 Validating prerequisites..." -ForegroundColor Yellow
    
    # Check AWS CLI
    try {
        $awsVersion = aws --version 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ AWS CLI not found. Please install AWS CLI first." -ForegroundColor Red
            return
        }
        Write-Host "✅ AWS CLI found: $awsVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ AWS CLI not found. Please install AWS CLI first." -ForegroundColor Red
        return
    }
    
    # Check AWS credentials
    try {
        $awsIdentity = aws sts get-caller-identity 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
            return
        }
        Write-Host "✅ AWS credentials configured" -ForegroundColor Green
    } catch {
        Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
        return
    }
    
    # Get configuration from environment variables or deployment_credentials.json
    $bucketName = $env:FRONTEND_BUCKET_NAME
    $distributionId = $env:FRONTEND_DIST_ID
    $domain = $env:FRONTEND_DOMAIN
    
    # Try to load from deployment_credentials.json if env vars not set
    if (-not $bucketName -or -not $distributionId) {
        $credentialsPath = "$projectPath\deployment_credentials.json"
        if (Test-Path $credentialsPath) {
            try {
                $credentials = Get-Content $credentialsPath | ConvertFrom-Json
                if (-not $bucketName) {
                    $bucketName = $credentials.frontend.s3_buckets.frontend.bucket_name
                }
                if (-not $distributionId) {
                    $distributionId = $credentials.frontend.cloudfront.distribution_id
                }
                if (-not $domain) {
                    $domain = "https://$($credentials.frontend.domain)"
                }
                Write-Host "✅ Loaded configuration from deployment_credentials.json" -ForegroundColor Green
            } catch {
                Write-Host "⚠️ Failed to parse deployment_credentials.json: $_" -ForegroundColor Yellow
            }
        }
    }
    
    # Validate configuration
    if (-not $bucketName) {
        Write-Host "❌ FRONTEND_BUCKET_NAME not set. Set environment variable or ensure deployment_credentials.json exists." -ForegroundColor Red
        return
    }
    
    if (-not $distributionId) {
        Write-Host "❌ FRONTEND_DIST_ID not set. Set environment variable or ensure deployment_credentials.json exists." -ForegroundColor Red
        return
    }
    
    Write-Host "`n📋 Deployment Configuration:" -ForegroundColor Cyan
    Write-Host "  Bucket: $bucketName" -ForegroundColor White
    Write-Host "  CloudFront Distribution: $distributionId" -ForegroundColor White
    Write-Host "  Target Path: s3://$bucketName/dbt-docs/" -ForegroundColor White
    if ($domain) {
        Write-Host "  Domain: $domain/dbt-docs/" -ForegroundColor White
    }
    
    # Validate S3 bucket exists
    Write-Host "`n🔍 Validating S3 bucket..." -ForegroundColor Yellow
    try {
        $bucketCheck = aws s3api head-bucket --bucket $bucketName 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ S3 bucket '$bucketName' not found or not accessible" -ForegroundColor Red
            return
        }
        Write-Host "✅ S3 bucket validated" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to validate S3 bucket: $_" -ForegroundColor Red
        return
    }
    
    # Upload dbt docs to S3
    Write-Host "`n☁️ Uploading dbt docs to S3..." -ForegroundColor Yellow
    
    # Get absolute path - Resolve-Path returns PathInfo, get .Path property
    $resolvedPathInfo = Resolve-Path $targetPath -ErrorAction Stop
    $resolvedPath = $resolvedPathInfo.Path
    Write-Host "  Source path: $resolvedPath" -ForegroundColor Gray
    
    # Verify the path exists and is a directory
    if (-not (Test-Path $resolvedPath -PathType Container)) {
        Write-Host "❌ Resolved path is not a directory: $resolvedPath" -ForegroundColor Red
        return
    }
    
    try {
        # Change to the target directory to avoid path issues
        Push-Location $resolvedPath
        try {
            # First upload non-JSON files with longer cache
            Write-Host "  Uploading HTML and asset files..." -ForegroundColor Gray
            & aws s3 sync . "s3://$bucketName/dbt-docs/" --delete `
                --cache-control "public, max-age=3600" `
                --exclude "*.json"
        
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to upload dbt docs files" -ForegroundColor Red
                Write-Host "   Error details: Check AWS CLI output above" -ForegroundColor Yellow
                Pop-Location
                return
            }
            
            # Upload JSON files with shorter cache (they change when models change)
            Write-Host "  Uploading JSON files..." -ForegroundColor Gray
            & aws s3 sync . "s3://$bucketName/dbt-docs/" `
                --cache-control "public, max-age=300" `
                --exclude "*" `
                --include "*.json"
            
            if ($LASTEXITCODE -ne 0) {
                Write-Host "❌ Failed to upload dbt docs JSON files" -ForegroundColor Red
                Write-Host "   Error details: Check AWS CLI output above" -ForegroundColor Yellow
                Pop-Location
                return
            }
            
            Write-Host "✅ dbt docs uploaded to S3" -ForegroundColor Green
        } finally {
            Pop-Location
        }
    } catch {
        Write-Host "❌ Failed to upload dbt docs: $_" -ForegroundColor Red
        Write-Host "   Error: $($_.Exception.Message)" -ForegroundColor Yellow
        return
    }
    
    # Invalidate CloudFront cache
    Write-Host "`n🔄 Invalidating CloudFront cache..." -ForegroundColor Yellow
    try {
        $invalidation = aws cloudfront create-invalidation --distribution-id $distributionId --paths "/dbt-docs/*" 2>&1 | ConvertFrom-Json
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ CloudFront cache invalidation created: $($invalidation.Invalidation.Id)" -ForegroundColor Green
        } else {
            Write-Host "⚠️ Failed to create CloudFront invalidation (deployment may still be successful)" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "⚠️ Failed to create CloudFront invalidation: $_" -ForegroundColor Yellow
    }
    
    Write-Host "`n✅ dbt docs deployment completed!" -ForegroundColor Green
    if ($domain) {
        Write-Host "📚 dbt docs available at: $domain/dbt-docs/" -ForegroundColor Cyan
    } else {
        Write-Host "📚 dbt docs available at: https://$bucketName.s3-website.amazonaws.com/dbt-docs/" -ForegroundColor Cyan
    }
}

# =============================================================================
# AWS SSM ENVIRONMENT
# =============================================================================

function Initialize-AWSSSMEnvironment {
    param([string]$ProjectPath = (Get-Location))

    Write-Host "`n☁️  Initializing AWS SSM Environment" -ForegroundColor Cyan

    # Check if Session Manager Plugin is installed
    Write-Host "🔍 Checking Session Manager Plugin..." -ForegroundColor Yellow
    $ssmPluginPath = Get-Command session-manager-plugin -ErrorAction SilentlyContinue
    
    # If not found in PATH, check common installation locations
    if (-not $ssmPluginPath) {
        $commonPaths = @(
            "$env:ProgramFiles\Amazon\SessionManagerPlugin\bin\session-manager-plugin.exe",
            "${env:ProgramFiles(x86)}\Amazon\SessionManagerPlugin\bin\session-manager-plugin.exe",
            "$env:LOCALAPPDATA\Programs\Amazon\SessionManagerPlugin\bin\session-manager-plugin.exe",
            "$env:USERPROFILE\AppData\Local\Programs\Amazon\SessionManagerPlugin\bin\session-manager-plugin.exe"
        )
        
        $foundPath = $null
        foreach ($path in $commonPaths) {
            if (Test-Path $path) {
                $foundPath = $path
                break
            }
        }
        
        if ($foundPath) {
            # Add the directory to PATH for this session
            $pluginDir = Split-Path $foundPath -Parent
            if ($env:Path -notlike "*$pluginDir*") {
                $env:Path = "$pluginDir;$env:Path"
                Write-Host "✅ Session Manager Plugin found at: $foundPath" -ForegroundColor Green
                Write-Host "   Added to PATH for this session" -ForegroundColor Gray
            } else {
                Write-Host "✅ Session Manager Plugin found at: $foundPath" -ForegroundColor Green
            }
            $ssmPluginPath = Get-Command session-manager-plugin -ErrorAction SilentlyContinue
        } else {
            Write-Host "❌ Session Manager Plugin not found" -ForegroundColor Red
            Write-Host "`n📦 Installation Instructions:" -ForegroundColor Yellow
            Write-Host "  1. Install via winget (recommended):" -ForegroundColor White
            Write-Host "     winget install Amazon.SessionManagerPlugin" -ForegroundColor Gray
            Write-Host ""
            Write-Host "  2. Or download manually:" -ForegroundColor White
            Write-Host "     https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html" -ForegroundColor Gray
            Write-Host ""
            Write-Host "  3. After installation, run 'aws-ssm-init' again" -ForegroundColor White
            Write-Host ""
            Write-Host "⚠️  Environment variables will still be loaded, but SSM commands won't work until plugin is installed." -ForegroundColor Yellow
        }
    } else {
        Write-Host "✅ Session Manager Plugin found: $($ssmPluginPath.Source)" -ForegroundColor Green
    }

    # Load credentials from deployment_credentials.json
    $credentialsPath = "$ProjectPath\deployment_credentials.json"
    if (-not (Test-Path $credentialsPath)) {
        Write-Host "❌ deployment_credentials.json not found at: $credentialsPath" -ForegroundColor Red
        Write-Host "   Environment variables will use defaults" -ForegroundColor Yellow
    } else {
        try {
            $credentials = Get-Content $credentialsPath | ConvertFrom-Json
            Write-Host "✅ Loaded credentials from deployment_credentials.json" -ForegroundColor Green

            # Set AWS region
            if ($credentials.aws_account.region) {
                $env:AWS_DEFAULT_REGION = $credentials.aws_account.region
                Write-Host "  AWS_DEFAULT_REGION: $($env:AWS_DEFAULT_REGION)" -ForegroundColor Gray
            }

            # Store instance IDs for helper functions
            if ($credentials.backend_api.ec2.instance_id) {
                $script:APIInstanceId = $credentials.backend_api.ec2.instance_id
                Write-Host "  API EC2 Instance: $script:APIInstanceId" -ForegroundColor Gray
            }

            if ($credentials.demo_database.ec2.instance_id) {
                $script:DemoDBInstanceId = $credentials.demo_database.ec2.instance_id
                Write-Host "  Demo DB EC2 Instance: $script:DemoDBInstanceId" -ForegroundColor Gray
            }

            # Load RDS endpoint for port forwarding
            if ($credentials.backend_api.production_database_reference.rds.endpoint) {
                $script:RDSEndpoint = $credentials.backend_api.production_database_reference.rds.endpoint
                Write-Host "  RDS Endpoint: $script:RDSEndpoint" -ForegroundColor Gray
            }

            # Load demo database connection info
            if ($credentials.demo_database.database_connection) {
                $script:DemoDBHost = $credentials.demo_database.database_connection.host
                $script:DemoDBPort = $credentials.demo_database.database_connection.port
                Write-Host "  Demo DB Host: $script:DemoDBHost" -ForegroundColor Gray
            }

        } catch {
            Write-Host "⚠️  Failed to parse deployment_credentials.json: $_" -ForegroundColor Yellow
            Write-Host "   Using default values" -ForegroundColor Yellow
        }
    }

    # Set default AWS region if not set
    if (-not $env:AWS_DEFAULT_REGION) {
        $env:AWS_DEFAULT_REGION = "us-east-1"
        Write-Host "  AWS_DEFAULT_REGION: $($env:AWS_DEFAULT_REGION) (default)" -ForegroundColor Gray
    }

    # PATH was already refreshed above, just confirm
    if ($ssmPluginPath) {
        Write-Host "✅ PATH configured for Session Manager Plugin" -ForegroundColor Green
    }

    # Set database environment variables for local development (via SSH tunnel)
    # These are used when connecting via port forwarding
    if (-not $env:POSTGRES_HOST) {
        $env:POSTGRES_HOST = "localhost"
    }
    if (-not $env:POSTGRES_PORT) {
        $env:POSTGRES_PORT = "5433"  # Default local forwarded port for RDS
    }
    if (-not $env:POSTGRES_DB) {
        $env:POSTGRES_DB = "opendental_analytics"
    }
    if (-not $env:POSTGRES_USER) {
        $env:POSTGRES_USER = "analytics_user"
    }
    # Password should be set manually or from credentials

    # Demo database variables (for direct connection or port forwarding)
    if (-not $env:DEMO_POSTGRES_HOST) {
        $env:DEMO_POSTGRES_HOST = "localhost"
    }
    if (-not $env:DEMO_POSTGRES_PORT) {
        $env:DEMO_POSTGRES_PORT = "5434"  # Default local forwarded port for demo DB
    }
    if (-not $env:DEMO_POSTGRES_DB) {
        $env:DEMO_POSTGRES_DB = "opendental_demo"
    }
    if (-not $env:DEMO_POSTGRES_USER) {
        $env:DEMO_POSTGRES_USER = "opendental_demo_user"
    }

    Write-Host "`n✅ AWS SSM Environment ready!" -ForegroundColor Green
    Write-Host "Commands:" -ForegroundColor Cyan
    Write-Host "  ssm-connect-api        - Connect to API EC2 instance" -ForegroundColor White
    Write-Host "  ssm-connect-demo-db    - Connect to Demo DB EC2 instance" -ForegroundColor White
    Write-Host "  ssm-port-forward-rds   - Start port forwarding to RDS (production analytics DB)" -ForegroundColor White
    Write-Host "  ssm-port-forward-demo-db - Start port forwarding to demo database" -ForegroundColor White
    Write-Host "  ssm-status             - Check SSM plugin status" -ForegroundColor White
    Write-Host ""
}

function Connect-SSMAPI {
    if (-not $script:APIInstanceId) {
        Write-Host "❌ API EC2 instance ID not loaded. Run 'aws-ssm-init' first." -ForegroundColor Red
        return
    }
    
    Write-Host "🔌 Connecting to API EC2 instance: $script:APIInstanceId" -ForegroundColor Cyan
    aws ssm start-session --target $script:APIInstanceId
}

function Connect-SSMDemoDB {
    if (-not $script:DemoDBInstanceId) {
        Write-Host "❌ Demo DB EC2 instance ID not loaded. Run 'aws-ssm-init' first." -ForegroundColor Red
        return
    }
    
    Write-Host "🔌 Connecting to Demo DB EC2 instance: $script:DemoDBInstanceId" -ForegroundColor Cyan
    aws ssm start-session --target $script:DemoDBInstanceId
}

function Start-SSMPortForwardRDS {
    if (-not $script:APIInstanceId) {
        Write-Host "❌ API EC2 instance ID not loaded. Run 'aws-ssm-init' first." -ForegroundColor Red
        return
    }
    
    if (-not $script:RDSEndpoint) {
        Write-Host "❌ RDS endpoint not loaded. Run 'aws-ssm-init' first." -ForegroundColor Red
        return
    }

    $localPort = $env:POSTGRES_PORT
    if (-not $localPort) {
        $localPort = "5433"
    }

    Write-Host "🔌 Starting port forwarding to RDS..." -ForegroundColor Cyan
    Write-Host "  Local port: $localPort" -ForegroundColor Gray
    Write-Host "  Remote: $script:RDSEndpoint:5432" -ForegroundColor Gray
    Write-Host "  Via: $script:APIInstanceId" -ForegroundColor Gray
    Write-Host ""
    Write-Host "💡 Keep this terminal open. Use Ctrl+C to stop forwarding." -ForegroundColor Yellow
    Write-Host ""

    $params = @{
        host = @($script:RDSEndpoint)
        portNumber = @("5432")
        localPortNumber = @($localPort)
    } | ConvertTo-Json -Compress

    aws ssm start-session --target $script:APIInstanceId --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters $params
}

function Start-SSMPortForwardDemoDB {
    if (-not $script:APIInstanceId) {
        Write-Host "❌ API EC2 instance ID not loaded. Run 'aws-ssm-init' first." -ForegroundColor Red
        return
    }
    
    if (-not $script:DemoDBHost) {
        Write-Host "❌ Demo DB host not loaded. Run 'aws-ssm-init' first." -ForegroundColor Red
        return
    }

    $localPort = $env:DEMO_POSTGRES_PORT
    if (-not $localPort) {
        $localPort = "5434"
    }

    Write-Host "🔌 Starting port forwarding to Demo Database..." -ForegroundColor Cyan
    Write-Host "  Local port: $localPort" -ForegroundColor Gray
    Write-Host "  Remote: $script:DemoDBHost:5432" -ForegroundColor Gray
    Write-Host "  Via: $script:APIInstanceId" -ForegroundColor Gray
    Write-Host ""
    Write-Host "💡 Keep this terminal open. Use Ctrl+C to stop forwarding." -ForegroundColor Yellow
    Write-Host ""

    $params = @{
        host = @($script:DemoDBHost)
        portNumber = @("5432")
        localPortNumber = @($localPort)
    } | ConvertTo-Json -Compress

    aws ssm start-session --target $script:APIInstanceId --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters $params
}

function Get-SSMStatus {
    Write-Host "`n📊 SSM Environment Status" -ForegroundColor White
    Write-Host ""

    # Check Session Manager Plugin
    $ssmPluginPath = Get-Command session-manager-plugin -ErrorAction SilentlyContinue
    if ($ssmPluginPath) {
        Write-Host "  Session Manager Plugin: ✅ Installed" -ForegroundColor Green
        Write-Host "    Location: $($ssmPluginPath.Source)" -ForegroundColor Gray
    } else {
        Write-Host "  Session Manager Plugin: ❌ Not found" -ForegroundColor Red
        Write-Host "    Run 'winget install Amazon.SessionManagerPlugin' to install" -ForegroundColor Yellow
    }

    # Check AWS CLI
    $awsCliPath = Get-Command aws -ErrorAction SilentlyContinue
    if ($awsCliPath) {
        $awsVersion = aws --version 2>&1
        Write-Host "  AWS CLI: ✅ Installed" -ForegroundColor Green
        Write-Host "    Version: $awsVersion" -ForegroundColor Gray
    } else {
        Write-Host "  AWS CLI: ❌ Not found" -ForegroundColor Red
    }

    # Check AWS credentials
    try {
        $identity = aws sts get-caller-identity 2>&1 | ConvertFrom-Json
        if ($identity) {
            Write-Host "  AWS Credentials: ✅ Configured" -ForegroundColor Green
            Write-Host "    Account: $($identity.Account)" -ForegroundColor Gray
            Write-Host "    User/Role: $($identity.Arn)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "  AWS Credentials: ❌ Not configured or invalid" -ForegroundColor Red
    }

    # Show loaded instance IDs
    Write-Host ""
    Write-Host "  Loaded Resources:" -ForegroundColor White
    if ($script:APIInstanceId) {
        Write-Host "    API EC2: ✅ $script:APIInstanceId" -ForegroundColor Green
    } else {
        Write-Host "    API EC2: ⭕ Not loaded" -ForegroundColor Gray
    }
    
    if ($script:DemoDBInstanceId) {
        Write-Host "    Demo DB EC2: ✅ $script:DemoDBInstanceId" -ForegroundColor Green
    } else {
        Write-Host "    Demo DB EC2: ⭕ Not loaded" -ForegroundColor Gray
    }

    if ($script:RDSEndpoint) {
        Write-Host "    RDS Endpoint: ✅ $script:RDSEndpoint" -ForegroundColor Green
    } else {
        Write-Host "    RDS Endpoint: ⭕ Not loaded" -ForegroundColor Gray
    }

    # Show environment variables
    Write-Host ""
    Write-Host "  Environment Variables:" -ForegroundColor White
    Write-Host "    AWS_DEFAULT_REGION: $($env:AWS_DEFAULT_REGION)" -ForegroundColor Gray
    Write-Host "    POSTGRES_HOST: $($env:POSTGRES_HOST)" -ForegroundColor Gray
    Write-Host "    POSTGRES_PORT: $($env:POSTGRES_PORT)" -ForegroundColor Gray
    Write-Host "    DEMO_POSTGRES_HOST: $($env:DEMO_POSTGRES_HOST)" -ForegroundColor Gray
    Write-Host "    DEMO_POSTGRES_PORT: $($env:DEMO_POSTGRES_PORT)" -ForegroundColor Gray

    Write-Host ""
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
        Write-Host "  DEMO_POSTGRES_DB: $($env:DEMO_POSTGRES_DB)" -ForegroundColor Gray
        Write-Host "  DEMO_POSTGRES_HOST: $($env:DEMO_POSTGRES_HOST)" -ForegroundColor Gray
        Write-Host "  DEMO_API_KEY: $(if ($env:DEMO_API_KEY) { '***configured***' } else { 'not set' })" -ForegroundColor Gray
    } elseif ($environment -eq "local") {
        Write-Host "  POSTGRES_ANALYTICS_DB: $($env:POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
        Write-Host "  POSTGRES_ANALYTICS_HOST: $($env:POSTGRES_ANALYTICS_HOST)" -ForegroundColor Gray
        Write-Host "  API_KEY: Loaded from .ssh/dbt-dental-clinic-api-key.pem" -ForegroundColor Gray
    } elseif ($environment -eq "test") {
        Write-Host "  TEST_POSTGRES_ANALYTICS_DB: $($env:TEST_POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
        Write-Host "  TEST_POSTGRES_ANALYTICS_HOST: $($env:TEST_POSTGRES_ANALYTICS_HOST)" -ForegroundColor Gray
        Write-Host "  API_KEY: Loaded from .ssh/dbt-dental-clinic-api-key.pem" -ForegroundColor Gray
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
Set-Alias -Name aws-ssm-init -Value Initialize-AWSSSMEnvironment -Scope Global

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

# Frontend Commands
Set-Alias -Name frontend-dev -Value Start-FrontendDev -Scope Global
Set-Alias -Name frontend-deploy -Value Deploy-Frontend -Scope Global
Set-Alias -Name frontend-status -Value Get-FrontendStatus -Scope Global
Set-Alias -Name clinic-frontend-deploy -Value Deploy-ClinicFrontend -Scope Global

# dbt Docs Commands
Set-Alias -Name dbt-docs-deploy -Value Deploy-DBTDocs -Scope Global

# AWS SSM Commands
Set-Alias -Name ssm-connect-api -Value Connect-SSMAPI -Scope Global
Set-Alias -Name ssm-connect-demo-db -Value Connect-SSMDemoDB -Scope Global
Set-Alias -Name ssm-port-forward-rds -Value Start-SSMPortForwardRDS -Scope Global
Set-Alias -Name ssm-port-forward-demo-db -Value Start-SSMPortForwardDemoDB -Scope Global
Set-Alias -Name ssm-status -Value Get-SSMStatus -Scope Global

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
Write-Host "  dbt-init              - Initialize dbt environment (dev target - local production)" -ForegroundColor Cyan
Write-Host "  dbt-init -Target demo - Initialize dbt environment (demo target - demo EC2 database)" -ForegroundColor Cyan
Write-Host "  etl-init       - Initialize ETL environment (interactive)" -ForegroundColor Magenta
Write-Host "  api-init       - Initialize API environment (interactive)" -ForegroundColor Blue
Write-Host "  aws-ssm-init   - Initialize AWS SSM environment (loads credentials, checks plugin)" -ForegroundColor DarkCyan
Write-Host "  frontend-dev   - Start frontend dev server (localhost:3000 → localhost:8000 API)" -ForegroundColor Green
Write-Host "  frontend-deploy - Deploy demo frontend to AWS S3/CloudFront (public)" -ForegroundColor Green
Write-Host "  clinic-frontend-deploy - Deploy clinic production frontend (IP-restricted)" -ForegroundColor Green
Write-Host "  dbt-docs-deploy - Deploy dbt documentation to AWS S3/CloudFront" -ForegroundColor Cyan
Write-Host "  env-status     - Check environment status" -ForegroundColor Yellow

# Auto-detect project type
$cwd = Get-Location
if ((Test-Path "$cwd\dbt_project.yml") -or (Test-Path "$cwd\dbt_dental_models\dbt_project.yml")) {
    Write-Host "`n🏗️  dbt project detected. Run 'dbt-init' to start." -ForegroundColor Green
}
if (Test-Path "$cwd\etl_pipeline\Pipfile") {
    Write-Host "🔄 ETL pipeline detected (Pipfile in etl_pipeline/). Run 'etl-init' to start." -ForegroundColor Magenta
}
if (Test-Path "$cwd\api\main.py") {
    Write-Host "🌐 API server detected (main.py in api/). Run 'api-init' to start (creates venv & installs deps)." -ForegroundColor Blue
}
if (Test-Path "$cwd\frontend\package.json") {
    Write-Host "🎨 Frontend detected (package.json in frontend/). Run 'frontend-dev' for localhost development (localhost:3000) or 'frontend-deploy' to deploy to AWS." -ForegroundColor Green
}

Write-Host ""

# Export functions to global scope
Write-Host "🔧 Loading functions into global scope..." -ForegroundColor Yellow
Get-Command -Type Function | Where-Object {$_.Name -like "*ETL*" -or $_.Name -like "*DBT*" -or $_.Name -like "*API*"} | ForEach-Object {
    Set-Item -Path "function:global:$($_.Name)" -Value $_.Definition
}
Write-Host "✅ Functions loaded successfully!" -ForegroundColor Green 