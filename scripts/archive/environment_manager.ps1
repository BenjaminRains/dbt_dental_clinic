# ARCHIVED (Phase 5.5) — not loaded by default. Daily workflow: pip install -e tools/mdc_cli; mdc ...
# Kept for reference and emergency rollback. Load via scripts/archive/ only if needed.
#
# Simplified Data Engineering Environment Manager
# Focused on dbt and ETL functionality for dental clinic pipelines
#
# Nomenclature (consistent across script):
#   local  = localhost development (localhost:8000 API, local DB, frontend-dev → localhost:3000)
#   demo   = portfolio/demo (dbtdentalclinic.com, api.dbtdentalclinic.com, opendental_demo)
#   clinic = production clinic (clinic.dbtdentalclinic.com, api-clinic.dbtdentalclinic.com, opendental_analytics, IP-restricted)

# Project root (set when script loads so Deploy-ClinicFrontend etc. find api/.env and credentials from any cwd)
$script:EnvManagerScriptRoot = $PSScriptRoot
$script:ScriptsRoot = Split-Path $PSScriptRoot -Parent
$script:ProjectRoot = Split-Path $script:ScriptsRoot -Parent

function Sync-EnvironmentManagerScript {
    param([switch]$Quiet)

    $scriptFile = Join-Path $script:EnvManagerScriptRoot "environment_manager.ps1"
    if (-not (Test-Path $scriptFile)) { return $false }

    $ticks = (Get-Item $scriptFile).LastWriteTimeUtc.Ticks
    if ($null -ne $script:EnvManagerFileTicks -and $script:EnvManagerFileTicks -eq $ticks) {
        return $false
    }

    if (-not $Quiet) {
        Write-Host "🔄 Reloading scripts/environment_manager.ps1 (file changed on disk)..." -ForegroundColor DarkCyan
    }

    $savedState = @{
        IsDBTActive = $script:IsDBTActive
        IsETLActive = $script:IsETLActive
        IsAPIActive = $script:IsAPIActive
        APIStage = $script:APIStage
        IsConsultAudioActive = $script:IsConsultAudioActive
        ActiveProject = $script:ActiveProject
        VenvPath = $script:VenvPath
        APIInstanceId = $script:APIInstanceId
        ClinicAPIInstanceId = $script:ClinicAPIInstanceId
        DemoDBInstanceId = $script:DemoDBInstanceId
        RDSEndpoint = $script:RDSEndpoint
        DemoDBHost = $script:DemoDBHost
        DemoDBPort = $script:DemoDBPort
    }

    $env:ENV_MANAGER_QUIET_RELOAD = "1"
    try {
        . $scriptFile
    } finally {
        Remove-Item Env:ENV_MANAGER_QUIET_RELOAD -ErrorAction SilentlyContinue
    }

    foreach ($key in $savedState.Keys) {
        Set-Variable -Scope Script -Name $key -Value $savedState[$key]
    }

    return $true
}

function Reload-EnvironmentManager {
    $script:EnvManagerFileTicks = $null
    Sync-EnvironmentManagerScript | Out-Null
    Write-Host "✅ Environment manager reloaded from disk." -ForegroundColor Green
}

. (Join-Path $script:ScriptsRoot "mdc_invoke.ps1")

# Environment state tracking
$script:IsDBTActive = $false
$script:IsETLActive = $false
$script:IsAPIActive = $false
$script:APIStage = $null
$script:IsConsultAudioActive = $false
$script:ActiveProject = $null
$script:VenvPath = $null

# AWS SSM state tracking
$script:APIInstanceId = $null      # dental-clinic-api-demo (api.dbtdentalclinic.com)
$script:ClinicAPIInstanceId = $null # dental-clinic-api-clinic (api-clinic.dbtdentalclinic.com)
$script:DemoDBInstanceId = $null   # dental-clinic-demo-db
$script:RDSEndpoint = $null
$script:DemoDBHost = $null
$script:DemoDBPort = $null

# =============================================================================
# DBT ENVIRONMENT
# =============================================================================

function Initialize-DBTEnvironment {
    param(
        [string]$ProjectPath = (Get-Location),
        [ValidateSet('local', 'demo', 'clinic')]
        [string]$Target = 'local'
    )

    Write-Host "`n⚠️  dbt-init is deprecated (Phase 4.5). dbt no longer requires shell activation." -ForegroundColor Yellow
    Write-Host '   Use mdc with explicit --env (stateless; no DBT_TARGET in your shell).' -ForegroundColor Gray
    Write-Host ""
    Write-Host "   mdc dbt validate --env $Target" -ForegroundColor Cyan
    Write-Host "   mdc dbt run --env $Target" -ForegroundColor Cyan
    Write-Host '   dbt run   (PowerShell alias -> mdc dbt run)' -ForegroundColor DarkGray
    Write-Host ""

    if (-not $PSBoundParameters.ContainsKey('Target')) {
        Write-Host '🔧 dbt target (default: local)' -ForegroundColor Cyan
        $targetChoice = Read-Host "Enter target (local/clinic/demo) [local]"
        if ([string]::IsNullOrWhiteSpace($targetChoice)) { $targetChoice = 'local' }
        $targetChoice = $targetChoice.ToLower().Trim()
        if ($targetChoice -notin @('local', 'clinic', 'demo')) {
            Write-Host "❌ Invalid target. Use local, clinic, or demo." -ForegroundColor Red
            return
        }
        $Target = $targetChoice
    }

    Invoke-MDC @('dbt', 'validate', '--env', $Target)
}

function Stop-DBTEnvironment {
    Write-Host 'dbt-deactivate is a no-op (Phase 4.6). mdc runs are stateless - no shell env to clear.' -ForegroundColor DarkGray
}

# ETL ENVIRONMENT  
# =============================================================================

function Initialize-ETLEnvironment {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = ""
    )

    Write-Host "`n⚠️  etl-init is deprecated (Phase 4.6). ETL no longer requires shell activation." -ForegroundColor Yellow
    Write-Host '   Use mdc with explicit --env and --profile (stateless runs).' -ForegroundColor Gray
    Write-Host '   Example: mdc etl run --env clinic --profile full' -ForegroundColor Cyan
    Write-Host ""

    if (-not $Env) {
        Write-Host 'ETL stage (default: local)' -ForegroundColor Cyan
        $stageChoice = Read-Host "Enter stage (local/clinic/test) [local]"
        if ([string]::IsNullOrWhiteSpace($stageChoice)) { $stageChoice = 'local' }
        $stageChoice = $stageChoice.ToLower().Trim()
        if ($stageChoice -notin @('local', 'clinic', 'test')) {
            Write-Host "❌ Invalid stage. Use local, clinic, or test." -ForegroundColor Red
            return
        }
        $Env = $stageChoice
    }

    $resolvedProfile = if ($Profile) { $Profile } elseif ($Env -eq 'local') { 'load' } else { 'full' }
    Invoke-MDC @('etl', 'validate', '--env', $Env, '--profile', $resolvedProfile)
}

function Stop-ETLEnvironment {
    Write-Host 'etl-deactivate is a no-op (Phase 4.6). mdc runs are stateless - no shell env to clear.' -ForegroundColor DarkGray
}

# API ENVIRONMENT  
# =============================================================================

function Initialize-APIEnvironment {
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = ""
    )

    if (Sync-EnvironmentManagerScript -Quiet) {
        Initialize-APIEnvironment -Env $Env
        return
    }

    Write-Host "`n⚠️  api-init is deprecated (Phase 4.6). API no longer requires shell activation." -ForegroundColor Yellow
    Write-Host '   Use: mdc api run --env local   or   api-run' -ForegroundColor Cyan
    Write-Host ""

    if (-not $Env) {
        Write-Host 'API stage (default: local)' -ForegroundColor Cyan
        $stageChoice = Read-Host "Enter stage (local/demo/clinic/test) [local]"
        if ([string]::IsNullOrWhiteSpace($stageChoice)) { $stageChoice = 'local' }
        $stageChoice = $stageChoice.ToLower().Trim()
        if ($stageChoice -notin @('local', 'demo', 'clinic', 'test')) {
            Write-Host "❌ Invalid stage." -ForegroundColor Red
            return
        }
        $Env = $stageChoice
    }

    Invoke-MDC @('api', 'test-config', '--env', $Env)
}

function Stop-APIEnvironment {
    Write-Host 'api-deactivate is a no-op (Phase 4.6). mdc runs are stateless - no shell env to clear.' -ForegroundColor DarkGray
}

# CONSULT AUDIO PIPELINE ENVIRONMENT
# =============================================================================

function Initialize-ConsultAudioEnvironment {
    param([string]$ProjectPath = (Get-Location))

    if ($script:IsConsultAudioActive) {
        Write-Host "❌ Consult audio pipeline environment already active. Use 'consult-audio-deactivate' first." -ForegroundColor Yellow
        return
    }

    if ($script:IsDBTActive) {
        Write-Host "❌ dbt environment is currently active. Run 'dbt-deactivate' first before activating consult audio pipeline." -ForegroundColor Red
        return
    }

    if ($script:IsETLActive) {
        Write-Host "❌ ETL environment is currently active. Run 'etl-deactivate' first before activating consult audio pipeline." -ForegroundColor Red
        return
    }

    if ($script:IsAPIActive) {
        Write-Host "❌ API environment is currently active. Run 'api-deactivate' first before activating consult audio pipeline." -ForegroundColor Red
        return
    }

    $projectName = Split-Path -Leaf $ProjectPath
    Write-Host "`n🎙️  Initializing Consult Audio Pipeline environment: $projectName" -ForegroundColor DarkCyan

    $consultAudioPath = "$ProjectPath\consult_audio_pipe"
    if (-not (Test-Path "$consultAudioPath\requirements.txt")) {
        Write-Host "❌ No requirements.txt found in consult_audio_pipe directory" -ForegroundColor Red
        return
    }

    Push-Location $consultAudioPath
    try {
        Write-Host "📦 Setting up Consult Audio Pipeline virtual environment..." -ForegroundColor Yellow

        if (-not (Test-Path "venv")) {
            Write-Host "🔧 Creating virtual environment..." -ForegroundColor Yellow
            python -m venv venv
        }

        $venvScripts = Join-Path (Get-Location) "venv\Scripts"
        if (Test-Path $venvScripts) {
            $script:VenvPath = Join-Path (Get-Location) "venv"
            $env:VIRTUAL_ENV = $script:VenvPath
            $env:PATH = "$venvScripts;$env:PATH"
            Write-Host "✅ Consult Audio Pipeline virtual environment activated" -ForegroundColor Green

            Write-Host "📦 Installing dependencies..." -ForegroundColor Yellow
            pip install -r requirements.txt 2>$null | Out-Null

            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ Consult Audio Pipeline dependencies installed successfully" -ForegroundColor Green
            } else {
                Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
                Pop-Location
                return
            }
        } else {
            Write-Host "❌ Failed to activate virtual environment" -ForegroundColor Red
            Pop-Location
            return
        }
    } catch {
        Write-Host "❌ Failed to set up Consult Audio Pipeline environment: $_" -ForegroundColor Red
        Pop-Location
        return
    }
    Pop-Location

    # Load .env from consult_audio_pipe if present (OPENAI_API_KEY, ANTHROPIC_API_KEY, etc.)
    $envPath = "$consultAudioPath\.env"
    if (Test-Path $envPath) {
        Write-Host "📄 Loading environment from consult_audio_pipe/.env" -ForegroundColor Green
        Get-Content $envPath | ForEach-Object {
            if ($_ -match '^([^#][^=]+)=(.*)$' -and $_ -notmatch '^\s*#') {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
                Write-Host "  Loaded: $name" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "⚠️ No .env found in consult_audio_pipe/ (optional). Copy .env.template to .env for API keys." -ForegroundColor Yellow
    }

    $script:IsConsultAudioActive = $true
    $script:ActiveProject = $projectName

    Write-Host "`n✅ Consult Audio Pipeline environment ready!" -ForegroundColor Green
    Write-Host "Commands: run pipeline scripts from consult_audio_pipe/ (e.g. python -m consult_audio_pipe.scripts.llm_analysis_integration)" -ForegroundColor Cyan
    Write-Host "To switch to other environments: run 'consult-audio-deactivate' first`n" -ForegroundColor Gray
}

function Stop-ConsultAudioEnvironment {
    if (-not $script:IsConsultAudioActive) {
        Write-Host "❌ Consult audio pipeline environment not active" -ForegroundColor Yellow
        return
    }

    Write-Host "🔄 Deactivating Consult Audio Pipeline environment..." -ForegroundColor Yellow

    if ($script:VenvPath) {
        $env:VIRTUAL_ENV = $null

        $venvScripts = Join-Path $script:VenvPath "Scripts"
        if ($env:PATH -like "*$venvScripts*") {
            $env:PATH = $env:PATH.Replace("$venvScripts;", "").Replace(";$venvScripts", "").Replace($venvScripts, "")
        }
        Write-Host "✅ Consult Audio Pipeline virtual environment deactivated" -ForegroundColor Green
    }

    $script:IsConsultAudioActive = $false
    $script:ActiveProject = $null
    $script:VenvPath = $null

    Write-Host "✅ Consult audio pipeline environment deactivated - other environments can now be activated" -ForegroundColor Green
}

# =============================================================================
# COMMAND WRAPPERS - FIXED: Avoid infinite recursion
# =============================================================================

# DBT Commands — Phase 4.4: delegate to mdc (no dbt-init required for run/test/docs/deps)
function Invoke-DBT {
    Write-Host "💡 dbt uses mdc (no dbt-init required). Example: mdc dbt run --env local" -ForegroundColor DarkGray

    if (-not $args -or $args.Count -eq 0) {
        Write-Host "Usage: dbt run|test|build|deps|docs ... (delegates to mdc dbt)" -ForegroundColor Yellow
        return
    }

    $target = [Environment]::GetEnvironmentVariable('DBT_TARGET', 'Process')
    $validTargets = @('local', 'demo', 'clinic')
    if (-not $target -or $target -notin $validTargets) {
        $target = 'local'
    }

    foreach ($arg in $args) {
        if ($arg -match '^--target=(.+)$') {
            $target = $Matches[1]
            break
        }
    }

    $sub = $args[0]
    $rest = @()
    if ($args.Count -gt 1) {
        $rest = $args[1..($args.Count - 1)]
    }

    switch ($sub) {
        'run' {
            $mdcArgs = @('dbt', 'run', '--env', $target) + $rest
            Invoke-MDC @mdcArgs
            return
        }
        'test' {
            $mdcArgs = @('dbt', 'test', '--env', $target) + $rest
            Invoke-MDC @mdcArgs
            return
        }
        'docs' {
            if ($rest.Count -gt 0 -and $rest[0] -eq 'serve') {
                $mdcArgs = @('dbt', 'docs', '--env', $target, '--serve') + $rest[1..($rest.Count - 1)]
            } else {
                $extra = $rest
                if ($rest.Count -gt 0 -and $rest[0] -eq 'generate') {
                    $extra = $rest[1..($rest.Count - 1)]
                }
                $mdcArgs = @('dbt', 'docs', '--env', $target) + $extra
            }
            Invoke-MDC @mdcArgs
            return
        }
        'deps' {
            $maxAttempts = 6
            $sleepSeconds = @(3, 5, 8, 12, 15)
            for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
                if ($attempt -gt 1) {
                    $delay = $sleepSeconds[$attempt - 2]
                    Write-Host "🔄 dbt deps attempt $attempt/$maxAttempts (waiting ${delay}s)..." -ForegroundColor Yellow
                    Start-Sleep -Seconds $delay
                }
                $mdcArgs = @('dbt', 'invoke', '--env', $target, '--', 'deps') + $rest
                Invoke-MDC @mdcArgs
                if ($LASTEXITCODE -eq 0) { break }
                if ($attempt -eq $maxAttempts) {
                    Write-Host "❌ dbt deps failed after $maxAttempts attempts." -ForegroundColor Red
                }
            }
            return
        }
        default {
            $mdcArgs = @('dbt', 'invoke', '--env', $target, '--') + $args
            Invoke-MDC @mdcArgs
        }
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
    black .
    if ($LASTEXITCODE -eq 0) { isort . }
}

function Test-DBT {
    if (-not $script:IsDBTActive) {
        Write-Host "❌ dbt environment not active. Run 'dbt-init' first." -ForegroundColor Red
        return
    }
    pytest $args
}

# ETL Commands — Phase 4.5: primary paths delegate to mdc
function Invoke-ETL {
    if (-not $args -or $args.Count -eq 0) {
        Show-ETLHelp
        return
    }

    $sub = $args[0]
    $rest = @()
    if ($args.Count -gt 1) {
        $rest = $args[1..($args.Count - 1)]
    }

    switch ($sub) {
        'status' {
            Get-ETLStatus @rest
            return
        }
        'validate' {
            Test-ETLValidation @rest
            return
        }
        'run' {
            Start-ETLPipeline @rest
            return
        }
        'test-connections' {
            Test-ETLConnections @rest
            return
        }
        default {
            $stage = if ($env:ETL_ENVIRONMENT) { $env:ETL_ENVIRONMENT } else { 'clinic' }
            $mdcArgs = @('etl', 'invoke', '--env', $stage, '--') + $args
            Invoke-MDC @mdcArgs
        }
    }
}

function Get-ETLStatus {
    param(
        [string]$Env = "",
        [string]$Profile = ""
    )

    Write-Host "💡 etl-status uses mdc (no etl-init required). Prefer: mdc etl status --env clinic" -ForegroundColor DarkGray

    $stage = if ($Env) { $Env } elseif ($env:ETL_ENVIRONMENT) { $env:ETL_ENVIRONMENT } else { "clinic" }
    $mdcArgs = @("etl", "status", "--env", $stage)
    if ($Profile) {
        $mdcArgs += @("--profile", $Profile)
    }
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
}

function Test-ETLValidation {
    param(
        [string]$Env = "",
        [string]$Profile = ""
    )

    Write-Host "💡 etl-validate uses mdc (no etl-init required). Prefer: mdc etl validate --env local --profile load" -ForegroundColor DarkGray

    $stage = if ($Env) { $Env } elseif ($env:ETL_ENVIRONMENT) { $env:ETL_ENVIRONMENT } else { "local" }
    $resolvedProfile = if ($Profile) { $Profile } elseif ($stage -eq "local") { "load" } else { "full" }
    if ($stage -eq "demo") {
        Write-Host "❌ ETL validation is not available in demo mode." -ForegroundColor Red
        return
    }
    Invoke-MDC @("etl", "validate", "--env", $stage, "--profile", $resolvedProfile)
}

function Start-ETLPipeline {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full")]
        [string]$Profile = "full"
    )

    Write-Host "💡 etl-run uses mdc (no etl-init required). Prefer: mdc etl run --env clinic --profile full" -ForegroundColor DarkGray

    $stage = if ($Env) { $Env } elseif ($env:ETL_ENVIRONMENT) { $env:ETL_ENVIRONMENT } else { "clinic" }
    if ($stage -eq "demo") {
        Write-Host "❌ ETL pipeline operations are not available in demo mode." -ForegroundColor Red
        Write-Host "   Use clinic or test: mdc etl run --env clinic --profile full" -ForegroundColor Yellow
        return
    }

    $mdcArgs = @("etl", "run", "--env", $stage, "--profile", $Profile)
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
}

function Test-ETLConnections {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full")]
        [string]$Profile = "full"
    )

    Write-Host "💡 etl-test uses mdc (no etl-init required). Prefer: mdc etl test-connections --env clinic" -ForegroundColor DarkGray

    $stage = if ($Env) { $Env } elseif ($env:ETL_ENVIRONMENT) { $env:ETL_ENVIRONMENT } else { "clinic" }
    if ($stage -eq "demo") {
        Write-Host "❌ ETL connection testing is not available in demo mode." -ForegroundColor Red
        return
    }

    $mdcArgs = @("etl", "test-connections", "--env", $stage, "--profile", $Profile)
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
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
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = ""
    )

    $stage = if ($Env) { $Env } elseif ($env:API_ENVIRONMENT) { $env:API_ENVIRONMENT } else { "local" }
    Write-Host "💡 api-test uses mdc (no api-init required). Prefer: mdc api test-config --env $stage" -ForegroundColor DarkGray
    Invoke-MDC @("api", "test-config", "--env", $stage)
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
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = "",
        [string]$BindHost = "",
        [int]$Port = 0
    )

    Write-Host "💡 api-run uses mdc (no api-init required). Prefer: mdc api run --env local" -ForegroundColor DarkGray

    $stage = if ($Env) { $Env } elseif ($env:API_ENVIRONMENT) { $env:API_ENVIRONMENT } else { "local" }
    $mdcArgs = @("api", "run", "--env", $stage)
    if ($BindHost) { $mdcArgs += @("--host", $BindHost) }
    if ($Port -gt 0) { $mdcArgs += @("--port", "$Port") }
    Invoke-MDC @mdcArgs
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

function Merge-DemoFrontendFromCredentialsFile {
    <#
    .SYNOPSIS
        Fills missing demo frontend S3/CloudFront settings from deployment_credentials.json.
        Emits success only when both bucket and distribution id resolve; otherwise a targeted warning.
    #>
    param(
        [Parameter(Mandatory)][string] $ProjectPath,
        [string] $BucketName,
        [string] $DistributionId,
        [string] $Domain
    )
    function Local:Normalize([string]$s) {
        if ([string]::IsNullOrWhiteSpace($s)) { return $null }
        return $s.Trim()
    }
    $bucketName = Normalize $BucketName
    $distributionId = Normalize $DistributionId
    $domain = Normalize $Domain

    $needFile = [string]::IsNullOrWhiteSpace($bucketName) -or [string]::IsNullOrWhiteSpace($distributionId)
    if (-not $needFile) {
        return [pscustomobject]@{ BucketName = $bucketName; DistributionId = $distributionId; Domain = $domain }
    }

    $credentialsPath = Join-Path $ProjectPath "deployment_credentials.json"
    if (-not (Test-Path $credentialsPath)) {
        return [pscustomobject]@{ BucketName = $bucketName; DistributionId = $distributionId; Domain = $domain }
    }
    try {
        $credentials = Get-Content $credentialsPath -Raw | ConvertFrom-Json
        if ([string]::IsNullOrWhiteSpace($bucketName)) {
            $bn = $credentials.frontend.s3_buckets.frontend.bucket_name
            if (-not [string]::IsNullOrWhiteSpace($bn)) { $bucketName = $bn.Trim() }
        }
        if ([string]::IsNullOrWhiteSpace($distributionId)) {
            $did = $credentials.frontend.cloudfront.distribution_id
            if (-not [string]::IsNullOrWhiteSpace($did)) { $distributionId = $did.Trim() }
        }
        if ([string]::IsNullOrWhiteSpace($domain)) {
            $fd = $credentials.frontend.domain
            if (-not [string]::IsNullOrWhiteSpace($fd)) { $domain = "https://$($fd.Trim())" }
        }
        $missing = @()
        if ([string]::IsNullOrWhiteSpace($bucketName)) {
            $missing += 'frontend.s3_buckets.frontend.bucket_name (or FRONTEND_BUCKET_NAME)'
        }
        if ([string]::IsNullOrWhiteSpace($distributionId)) {
            $missing += 'frontend.cloudfront.distribution_id (or FRONTEND_DIST_ID)'
        }
        if ($missing.Count -eq 0) {
            Write-Host "✅ Loaded demo frontend configuration from deployment_credentials.json" -ForegroundColor Green
        } else {
            Write-Host "⚠️ deployment_credentials.json was read but demo frontend values are still missing:" -ForegroundColor Yellow
            foreach ($m in $missing) { Write-Host "   - $m" -ForegroundColor Gray }
        }
    } catch {
        Write-Host "⚠️ Failed to parse deployment_credentials.json: $_" -ForegroundColor Yellow
    }
    return [pscustomobject]@{ BucketName = $bucketName; DistributionId = $distributionId; Domain = $domain }
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
    # Nomenclature: demo = portfolio/demo frontend (dbtdentalclinic.com, api.dbtdentalclinic.com)
    $merged = Merge-DemoFrontendFromCredentialsFile -ProjectPath $projectPath -BucketName $env:FRONTEND_BUCKET_NAME -DistributionId $env:FRONTEND_DIST_ID -Domain $env:FRONTEND_DOMAIN
    $bucketName = $merged.BucketName
    $distributionId = $merged.DistributionId
    $domain = $merged.Domain
    
    # Validate configuration
    if ([string]::IsNullOrWhiteSpace($bucketName)) {
        Write-Host "❌ FRONTEND_BUCKET_NAME not set. Set environment variable or ensure deployment_credentials.json frontend.s3_buckets.frontend.bucket_name exists." -ForegroundColor Red
        return
    }
    
    if ([string]::IsNullOrWhiteSpace($distributionId)) {
        Write-Host "❌ FRONTEND_DIST_ID not set. Set environment variable or ensure deployment_credentials.json frontend.cloudfront.distribution_id exists." -ForegroundColor Red
        return
    }
    
    Write-Host "`n📋 Demo Frontend Deployment Configuration:" -ForegroundColor Cyan
    Write-Host "  Target: demo (portfolio site at dbtdentalclinic.com)" -ForegroundColor Gray
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
    
    # Load DEMO_API_KEY from .env_api_demo for demo build (api.dbtdentalclinic.com)
    Write-Host "`n🔑 Loading demo API configuration (api.dbtdentalclinic.com)..." -ForegroundColor Yellow
    $apiDemoEnvFile = "$projectPath\api\.env_api_demo"
    $demoApiKey = $null
    
    if (Test-Path $apiDemoEnvFile) {
        Get-Content $apiDemoEnvFile | ForEach-Object {
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
        Write-Host "❌ DEMO_API_KEY not found in .env_api_demo or environment variables" -ForegroundColor Red
        Write-Host "   Demo build requires DEMO_API_KEY for API authentication" -ForegroundColor Yellow
        return
    }
    
    Write-Host "✅ DEMO_API_KEY loaded for demo build" -ForegroundColor Green
    
    # Build frontend
    Push-Location $frontendPath
    try {
        Write-Host "`n📦 Building demo frontend (portfolio site)..." -ForegroundColor Yellow
        Write-Host "   API URL: https://api.dbtdentalclinic.com (demo API)" -ForegroundColor Gray
        Write-Host "   Domain: dbtdentalclinic.com" -ForegroundColor Gray
        Write-Host "   Database: opendental_demo (synthetic data)" -ForegroundColor Gray
        
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
        
        # Set demo environment variables for Vite build (portfolio at /, demo API)
        $env:VITE_API_URL = "https://api.dbtdentalclinic.com"
        $env:VITE_API_KEY = $demoApiKey
        $env:VITE_IS_DEMO = "true"
        
        Write-Host "🔧 Building with demo environment variables (portfolio landing at /)..." -ForegroundColor Cyan
        
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
        
        Write-Host "`n✅ Demo frontend deployment completed!" -ForegroundColor Green
        if ($domain) {
            Write-Host "🌐 Demo frontend (portfolio) available at: $domain" -ForegroundColor Cyan
        }
        
    } catch {
        Write-Host "❌ Deployment failed: $_" -ForegroundColor Red
    } finally {
        Pop-Location
    }
}

function Get-FrontendStatus {
    $projectPath = Get-Location
    $projectRoot = if ($script:ProjectRoot) { $script:ProjectRoot } else { $projectPath }
    
    Write-Host "`n📊 Frontend Deployment Status:" -ForegroundColor White
    Write-Host "  Nomenclature: local = localhost dev | demo = portfolio (dbtdentalclinic.com) | clinic = production (clinic.dbtdentalclinic.com)" -ForegroundColor Gray
    
    # Check configuration from deployment_credentials.json
    $credentialsPath = "$projectPath\deployment_credentials.json"
    $credentials = $null
    if (Test-Path $credentialsPath) {
        try {
            $credentials = Get-Content $credentialsPath | ConvertFrom-Json
        } catch {
            Write-Host "⚠️ Failed to parse deployment_credentials.json" -ForegroundColor Yellow
        }
    }
    
    # Demo frontend (portfolio site at dbtdentalclinic.com)
    $bucketName = $env:FRONTEND_BUCKET_NAME
    $distributionId = $env:FRONTEND_DIST_ID
    $domain = $env:FRONTEND_DOMAIN
    if ($credentials) {
        if (-not $bucketName) { $bucketName = $credentials.frontend.s3_buckets.frontend.bucket_name }
        if (-not $distributionId) { $distributionId = $credentials.frontend.cloudfront.distribution_id }
        if (-not $domain) { $domain = "https://$($credentials.frontend.domain)" }
    }
    
    Write-Host "`n🔧 Demo frontend (portfolio - dbtdentalclinic.com):" -ForegroundColor Cyan
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
    
    # Clinic frontend (clinic.dbtdentalclinic.com, IP-restricted)
    $clinicBucket = $env:CLINIC_FRONTEND_BUCKET_NAME
    $clinicDistId = $env:CLINIC_FRONTEND_DIST_ID
    $clinicDomain = $env:CLINIC_FRONTEND_DOMAIN
    if ($credentials -and $credentials.clinic_frontend) {
        if (-not $clinicBucket -and $credentials.clinic_frontend.s3_buckets.clinic_frontend) { $clinicBucket = $credentials.clinic_frontend.s3_buckets.clinic_frontend.bucket_name }
        if (-not $clinicDistId -and $credentials.clinic_frontend.cloudfront) { $clinicDistId = $credentials.clinic_frontend.cloudfront.distribution_id }
        if (-not $clinicDomain -and $credentials.clinic_frontend.domain) { $clinicDomain = "https://$($credentials.clinic_frontend.domain)" }
    } elseif ($credentials -and $credentials.frontend -and $credentials.frontend.s3_buckets.clinic_frontend) {
        if (-not $clinicBucket) { $clinicBucket = $credentials.frontend.s3_buckets.clinic_frontend.bucket_name }
        if (-not $clinicDistId -and $credentials.frontend.cloudfront.clinic_distribution_id) { $clinicDistId = $credentials.frontend.cloudfront.clinic_distribution_id }
        if (-not $clinicDomain -and $credentials.frontend.domain) { $clinicDomain = "https://clinic.$($credentials.frontend.domain)" }
    }
    
    Write-Host "`n🔧 Clinic frontend (clinic.dbtdentalclinic.com, IP-restricted):" -ForegroundColor Cyan
    if ($clinicBucket) {
        Write-Host "  S3 Bucket: $clinicBucket" -ForegroundColor Green
    } else {
        Write-Host "  S3 Bucket: ❌ Not configured" -ForegroundColor Red
    }
    if ($clinicDistId) {
        Write-Host "  CloudFront Distribution: $clinicDistId" -ForegroundColor Green
    } else {
        Write-Host "  CloudFront Distribution: ❌ Not configured" -ForegroundColor Red
    }
    if ($clinicDomain) {
        Write-Host "  Domain: $clinicDomain" -ForegroundColor Green
    } else {
        Write-Host "  Domain: ❌ Not configured" -ForegroundColor Red
    }
    
    # Clinic frontend build targets (Vite: same as clinic-frontend-deploy)
    $clinicApiUrlStatus = $env:CLINIC_API_URL
    if ($credentials -and $credentials.backend_api -and $credentials.backend_api.clinic_api -and $credentials.backend_api.clinic_api.api_url) {
        if (-not $clinicApiUrlStatus) { $clinicApiUrlStatus = $credentials.backend_api.clinic_api.api_url }
    }
    if (-not $clinicApiUrlStatus) { $clinicApiUrlStatus = "https://api-clinic.dbtdentalclinic.com" }
    $clinicKeyFromEnv = [bool]$env:CLINIC_API_KEY
    $clinicKeyFromFile = $false
    if (-not $clinicKeyFromEnv) {
        foreach ($p in @((Join-Path $projectRoot "api\.env_api_clinic"), (Join-Path $projectPath "api\.env_api_clinic"))) {
            if (-not (Test-Path -LiteralPath $p)) { continue }
            try {
                foreach ($line in (Get-Content -LiteralPath $p -Encoding UTF8 -ErrorAction Stop)) {
                    $t = $line.Trim()
                    if (-not $t -or $t.StartsWith('#')) { continue }
                    if ($t -match '^CLINIC_API_KEY\s*=\s*(.+)$') { $clinicKeyFromFile = $true; break }
                }
            } catch { }
            if ($clinicKeyFromFile) { break }
        }
    }
    Write-Host "  clinic-frontend-deploy API (Vite):" -ForegroundColor Gray
    Write-Host "    CLINIC_API_URL → $clinicApiUrlStatus" -ForegroundColor $(if ($clinicApiUrlStatus) { 'Green' } else { 'Red' })
    if ($clinicKeyFromEnv) {
        Write-Host "    CLINIC_API_KEY: ✅ from environment" -ForegroundColor Green
    } elseif ($clinicKeyFromFile) {
        Write-Host "    CLINIC_API_KEY: ✅ from api\.env_api_clinic" -ForegroundColor Green
    } elseif ($credentials -and $credentials.backend_api -and $credentials.backend_api.clinic_api -and $credentials.backend_api.clinic_api.api_key -and $credentials.backend_api.clinic_api.api_key.key) {
        Write-Host "    CLINIC_API_KEY: ✅ from deployment_credentials.json" -ForegroundColor Green
    } else {
        Write-Host "    CLINIC_API_KEY: ❌ not set (env, api\.env_api_clinic, or credentials)" -ForegroundColor Red
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
    Deploy clinic frontend to AWS S3/CloudFront with IP restrictions.

    .DESCRIPTION
    Builds and deploys the clinic frontend to clinic.dbtdentalclinic.com.
    Vite bakes VITE_API_URL and VITE_API_KEY at build time; they must target the clinic API.

    Configuration (see docs/deployment/CLINIC_DEPLOYMENT_PHASE3_ACTION_PLAN.md):
    - CLINIC_API_URL — optional; defaults to https://api-clinic.dbtdentalclinic.com, or backend_api.clinic_api.api_url in deployment_credentials.json
    - CLINIC_API_KEY — required; env var, api\.env_api_clinic (CLINIC_API_KEY=...), or backend_api.clinic_api.api_key.key in deployment_credentials.json
    - CLINIC_FRONTEND_BUCKET_NAME, CLINIC_FRONTEND_DIST_ID, CLINIC_FRONTEND_DOMAIN — optional if set in deployment_credentials.json (clinic_frontend.*)

    The clinic API server uses api\.env_api_clinic on EC2; use the same CLINIC_API_KEY value here as on that server.

    .EXAMPLE
    Deploy-ClinicFrontend
    #>
    $projectPath = Get-Location
    # Resolve project root: use script location (set when script loaded) so env file is always found
    $projectRoot = if ($script:ProjectRoot) { $script:ProjectRoot } else { $projectPath }
    $frontendPath = Join-Path $projectRoot "frontend"
    
    if (-not (Test-Path $frontendPath)) {
        Write-Host "❌ Frontend directory not found: $frontendPath" -ForegroundColor Red
        return
    }
    
    Write-Host "`n🏥 Deploying Clinic Frontend" -ForegroundColor Cyan
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
    
    # Load CLINIC_API_KEY from api/.env_api_clinic if not set (same pattern as DEMO_API_KEY from .env_api_demo)
    # Use script-stored project root so env file is found regardless of current directory
    if (-not $apiKey) {
        $candidates = @(
            (Join-Path $projectRoot "api\.env_api_clinic"),
            (Join-Path $projectPath "api\.env_api_clinic")
        )
        foreach ($apiClinicEnvFile in $candidates) {
            if (-not (Test-Path -LiteralPath $apiClinicEnvFile)) { continue }
            try {
                $lines = Get-Content -LiteralPath $apiClinicEnvFile -Encoding UTF8 -ErrorAction Stop
            } catch {
                continue
            }
            foreach ($line in $lines) {
                $line = $line.Trim()
                if (-not $line -or $line.StartsWith('#')) { continue }
                # Match CLINIC_API_KEY=value (with optional spaces; strip trailing # comment)
                if ($line -match '^CLINIC_API_KEY\s*=\s*(.+)$') {
                    $val = $matches[1].Trim()
                    if ($val -match '^([^#]+)') { $val = $matches[1].Trim() }
                    if ($val) { $apiKey = $val; break }
                }
            }
            if ($apiKey) { break }
        }
    }
    
    # Try to load from deployment_credentials.json if env vars not set
    # Nomenclature: clinic = production clinic (clinic.dbtdentalclinic.com, api-clinic.dbtdentalclinic.com)
    # Include -not $apiUrl so backend_api.clinic_api.api_url from credentials applies when CLINIC_API_URL is unset
    if (-not $bucketName -or -not $distributionId -or -not $domain -or -not $apiKey -or -not $apiUrl) {
        $credentialsPath = Join-Path $projectRoot "deployment_credentials.json"
        if (Test-Path $credentialsPath) {
            try {
                $credentials = Get-Content $credentialsPath | ConvertFrom-Json
                # Prefer top-level clinic_frontend (per deployment_credentials.json.template)
                if ($credentials.clinic_frontend) {
                    if (-not $bucketName) {
                        $bucketName = $credentials.clinic_frontend.s3_buckets.clinic_frontend.bucket_name
                    }
                    if (-not $distributionId) {
                        $distributionId = $credentials.clinic_frontend.cloudfront.distribution_id
                    }
                    if (-not $domain) {
                        $domain = "https://$($credentials.clinic_frontend.domain)"
                    }
                } else {
                    # Fallback: nested under frontend (backward compatibility)
                    if (-not $bucketName) {
                        $bucketName = $credentials.frontend.s3_buckets.clinic_frontend.bucket_name
                    }
                    if (-not $distributionId) {
                        $distributionId = $credentials.frontend.cloudfront.clinic_distribution_id
                    }
                    if (-not $domain) {
                        $domain = "https://clinic.$($credentials.frontend.domain)"
                    }
                }
                if (-not $apiUrl) {
                    if ($credentials.backend_api -and $credentials.backend_api.clinic_api -and $credentials.backend_api.clinic_api.api_url) {
                        $apiUrl = $credentials.backend_api.clinic_api.api_url
                    } elseif ($credentials.frontend.domain) {
                        $apiUrl = "https://api-clinic.$($credentials.frontend.domain)"
                    }
                }
                if (-not $apiKey -and $credentials.backend_api -and $credentials.backend_api.clinic_api -and $credentials.backend_api.clinic_api.api_key) {
                    # Single, canonical format: backend_api.clinic_api.api_key.key MUST be present
                    $clinicApiKey = $credentials.backend_api.clinic_api.api_key
                    if (-not ($clinicApiKey.PSObject.Properties.Name -contains 'key') -or -not $clinicApiKey.key) {
                        Write-Host "❌ backend_api.clinic_api.api_key must be an object with a 'key' property (see deployment_credentials.json template)." -ForegroundColor Red
                        return
                    }
                    $apiKey = $clinicApiKey.key
                }
                Write-Host "✅ Loaded clinic configuration from deployment_credentials.json" -ForegroundColor Green
            } catch {
                Write-Host "⚠️ Failed to parse deployment_credentials.json: $_" -ForegroundColor Yellow
            }
        }
    }

    # Default clinic API URL (Phase 3: production clinic API hostname)
    if (-not $apiUrl) {
        $apiUrl = "https://api-clinic.dbtdentalclinic.com"
        Write-Host "ℹ️ CLINIC_API_URL not set; using default $apiUrl" -ForegroundColor DarkGray
    }
    
    # Validate configuration
    if (-not $bucketName) {
        Write-Host "❌ CLINIC_FRONTEND_BUCKET_NAME not set. Set env var or add clinic_frontend.s3_buckets.clinic_frontend.bucket_name to deployment_credentials.json." -ForegroundColor Red
        return
    }
    
    if (-not $distributionId) {
        Write-Host "❌ CLINIC_FRONTEND_DIST_ID not set. Set env var or add clinic_frontend.cloudfront.distribution_id to deployment_credentials.json." -ForegroundColor Red
        return
    }
    
    if (-not $apiKey) {
        Write-Host "❌ CLINIC_API_KEY not set. This is required for clinic API authentication." -ForegroundColor Red
        Write-Host "   Option 1: Run scripts\utils\generate_api_key.ps1 -Clinic to create and save a clinic API key." -ForegroundColor Yellow
        Write-Host "   Option 2: Set environment variable CLINIC_API_KEY, or add to api\.env_api_clinic as CLINIC_API_KEY=<key>." -ForegroundColor Yellow
        Write-Host "   Option 3: Add backend_api.clinic_api.api_key to deployment_credentials.json." -ForegroundColor Yellow
        return
    }
    
    Write-Host "`n📋 Clinic Frontend Deployment Configuration:" -ForegroundColor Cyan
    Write-Host "  Target: clinic (clinic.dbtdentalclinic.com, IP-restricted)" -ForegroundColor Gray
    Write-Host "  Bucket: $bucketName" -ForegroundColor White
    Write-Host "  CloudFront Distribution: $distributionId" -ForegroundColor White
    Write-Host "  API URL: $apiUrl (api-clinic.dbtdentalclinic.com)" -ForegroundColor White
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
    
    Write-Host "✅ CLINIC_API_KEY loaded for clinic build (api-clinic.dbtdentalclinic.com)" -ForegroundColor Green
    
    # Build frontend
    Push-Location $frontendPath
    try {
        Write-Host "`n📦 Building clinic frontend (clinic.dbtdentalclinic.com)..." -ForegroundColor Yellow
        Write-Host "   API URL: $apiUrl (api-clinic.dbtdentalclinic.com)" -ForegroundColor Gray
        Write-Host "   Domain: clinic.dbtdentalclinic.com (IP-restricted)" -ForegroundColor Gray
        Write-Host "   Database: opendental_analytics (real PHI)" -ForegroundColor Gray
        
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
        
        # Set clinic environment variables for Vite build
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
        Write-Host "⚠️  IMPORTANT: This frontend is IP-restricted (WAF). Verify clinic-office-ips and clinic-dev-ips are configured." -ForegroundColor Yellow
        if ($domain) {
            Write-Host "🌐 Clinic frontend available at: $domain (clinic + dev IPs only)" -ForegroundColor Cyan
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
    
    # Get configuration from environment variables or deployment_credentials.json (same demo frontend bucket as the site)
    $merged = Merge-DemoFrontendFromCredentialsFile -ProjectPath $projectPath -BucketName $env:FRONTEND_BUCKET_NAME -DistributionId $env:FRONTEND_DIST_ID -Domain $env:FRONTEND_DOMAIN
    $bucketName = $merged.BucketName
    $distributionId = $merged.DistributionId
    $domain = $merged.Domain
    
    # Validate configuration
    if ([string]::IsNullOrWhiteSpace($bucketName)) {
        Write-Host "❌ FRONTEND_BUCKET_NAME not set. Set environment variable or ensure deployment_credentials.json frontend.s3_buckets.frontend.bucket_name exists." -ForegroundColor Red
        return
    }
    
    if ([string]::IsNullOrWhiteSpace($distributionId)) {
        Write-Host "❌ FRONTEND_DIST_ID not set. Set environment variable or ensure deployment_credentials.json frontend.cloudfront.distribution_id exists." -ForegroundColor Red
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
# EC2 instance names (match AWS console Name tag):
#   dental-clinic-api-demo  = Demo API (api.dbtdentalclinic.com)
#   dental-clinic-api-clinic = Clinic API (api-clinic.dbtdentalclinic.com)
#   dental-clinic-demo-db   = Demo DB EC2 (opendental_demo host)

# Load instance IDs from deployment_credentials.json on first use (not during aws-ssm-init).
# Returns $true if the required resource is available; $false otherwise.
. (Join-Path $script:ScriptsRoot 'ssm_tunnels.ps1')

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

    # Ensure instance IDs are loaded from deployment_credentials.json (if available)
    $null = Ensure-SSMInstanceIdsLoaded -ProjectPath (Get-Location)

    # Show cached instance IDs (populated from deployment_credentials.json / connect commands)
    Write-Host ""
    Write-Host "  Cached instance IDs (from deployment_credentials.json when you run a connect/port-forward command):" -ForegroundColor White
    if ($script:APIInstanceId) {
        Write-Host "    dental-clinic-api-demo ($script:APIInstanceId):  ✅ Cached" -ForegroundColor Green
    } else {
        Write-Host "    dental-clinic-api-demo:  ⭕ Not loaded" -ForegroundColor Gray
    }
    if ($script:ClinicAPIInstanceId) {
        Write-Host "    dental-clinic-api-clinic ($script:ClinicAPIInstanceId): ✅ Cached" -ForegroundColor Green
    } else {
        Write-Host "    dental-clinic-api-clinic: ⭕ Not loaded" -ForegroundColor Gray
    }

    if ($script:DemoDBInstanceId) {
        Write-Host "    dental-clinic-demo-db ($script:DemoDBInstanceId):   ✅ Cached" -ForegroundColor Green
    } else {
        Write-Host "    dental-clinic-demo-db:   ⭕ Not loaded" -ForegroundColor Gray
    }

    if ($script:RDSEndpoint) {
        Write-Host "    RDS Endpoint ($script:RDSEndpoint): ✅ Cached" -ForegroundColor Green
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
    Write-Host "`n📊 Config status (mdc — stateless, no *-init required):" -ForegroundColor White
    Invoke-MDC @("status")

    Write-Host "`n📊 Legacy shell activation (optional *-init / *-deactivate):" -ForegroundColor DarkGray
    if ($script:IsDBTActive) {
        Write-Host "  dbt: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
    } else {
        Write-Host "  dbt: ⭕ Inactive" -ForegroundColor Gray
    }

    if ($script:IsETLActive) {
        Write-Host "  ETL: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
        if ($env:ETL_ENVIRONMENT) {
            Write-Host "  ETL Environment: $($env:ETL_ENVIRONMENT)" -ForegroundColor Cyan
        }
    } else {
        Write-Host "  ETL: ⭕ Inactive" -ForegroundColor Gray
    }

    if ($script:IsAPIActive) {
        Write-Host "  API: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
        if ($env:API_ENVIRONMENT) {
            Write-Host "  API Environment: $($env:API_ENVIRONMENT)" -ForegroundColor Cyan
        }
    } else {
        Write-Host "  API: ⭕ Inactive" -ForegroundColor Gray
    }

    if ($script:IsConsultAudioActive) {
        Write-Host "  Consult Audio: ✅ Active ($script:ActiveProject)" -ForegroundColor Green
    } else {
        Write-Host "  Consult Audio: ⭕ Inactive" -ForegroundColor Gray
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
    if ($environment -eq "clinic") {
        Write-Host "  OPENDENTAL_SOURCE_DB: $($env:OPENDENTAL_SOURCE_DB)" -ForegroundColor Gray
        Write-Host "  OPENDENTAL_SOURCE_HOST: $($env:OPENDENTAL_SOURCE_HOST)" -ForegroundColor Gray
        Write-Host "  POSTGRES_ANALYTICS_DB: $($env:POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
    } elseif ($environment -eq "test") {
        Write-Host "  TEST_OPENDENTAL_SOURCE_DB: $($env:TEST_OPENDENTAL_SOURCE_DB)" -ForegroundColor Gray
        Write-Host "  TEST_OPENDENTAL_SOURCE_HOST: $($env:TEST_OPENDENTAL_SOURCE_HOST)" -ForegroundColor Gray
        Write-Host "  TEST_MYSQL_REPLICATION_DB: $($env:TEST_MYSQL_REPLICATION_DB)" -ForegroundColor Gray
        Write-Host "  TEST_POSTGRES_ANALYTICS_DB: $($env:TEST_POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
        Write-Host "  ℹ️  Test databases are separate from clinic/demo (setup via setup_test_databases.py)" -ForegroundColor Cyan
    } elseif ($environment -eq "demo") {
        Write-Host "  DEMO_POSTGRES_DB: $($env:DEMO_POSTGRES_DB)" -ForegroundColor Gray
        Write-Host "  DEMO_POSTGRES_HOST: $($env:DEMO_POSTGRES_HOST)" -ForegroundColor Gray
        Write-Host "  ⚠️  Demo mode: Synthetic data generator only (ETL commands disabled)" -ForegroundColor Yellow
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
    if ($environment -eq "clinic") {
        Write-Host "  Run context: clinic config in this shell — uvicorn runs locally, not on EC2" -ForegroundColor Yellow
        Write-Host "  Remote clinic server: aws-ssm-init + ssm-connect-clinic-api" -ForegroundColor Gray
    } elseif ($environment -eq "demo") {
        Write-Host "  Run context: demo config in this shell — uvicorn runs locally" -ForegroundColor Cyan
    }
    
    # Show some key environment variables
    Write-Host "`n🔧 Key Environment Variables:" -ForegroundColor White
    if ($environment -eq "demo") {
        Write-Host "  DEMO_POSTGRES_DB: $($env:DEMO_POSTGRES_DB)" -ForegroundColor Gray
        Write-Host "  DEMO_POSTGRES_HOST: $($env:DEMO_POSTGRES_HOST)" -ForegroundColor Gray
        Write-Host "  DEMO_API_KEY: $(if ($env:DEMO_API_KEY) { '***configured***' } else { 'not set' })" -ForegroundColor Gray
    } elseif ($environment -eq "clinic") {
        Write-Host "  POSTGRES_ANALYTICS_DB: $($env:POSTGRES_ANALYTICS_DB)" -ForegroundColor Gray
        Write-Host "  POSTGRES_ANALYTICS_HOST: $($env:POSTGRES_ANALYTICS_HOST)" -ForegroundColor Gray
        Write-Host "  CLINIC_API_KEY: $(if ($env:CLINIC_API_KEY) { '***configured***' } else { 'not set' })" -ForegroundColor Gray
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
    if (Get-Command Sync-EnvironmentManagerScript -ErrorAction SilentlyContinue) {
        Sync-EnvironmentManagerScript -Quiet | Out-Null
    }

    $envTag = ""
    $envColor = "White"
    
    if (($script:IsDBTActive -and $script:IsETLActive) -or 
        ($script:IsDBTActive -and $script:IsAPIActive) -or 
        ($script:IsETLActive -and $script:IsAPIActive) -or
        ($script:IsConsultAudioActive -and ($script:IsDBTActive -or $script:IsETLActive -or $script:IsAPIActive))) {
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
        $stage = if ($env:API_ENVIRONMENT) { $env:API_ENVIRONMENT } elseif ($script:APIStage) { $script:APIStage } else { "?" }
        $envTag = "[api:$stage`:$script:ActiveProject] "
        $envColor = switch ($stage) {
            "clinic" { "Yellow" }
            "demo"   { "Cyan" }
            "test"   { "DarkYellow" }
            default  { "Blue" }
        }
    } elseif ($script:IsConsultAudioActive) {
        $envTag = "[consult-audio:$script:ActiveProject] "
        $envColor = "DarkCyan"
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
Set-Alias -Name consult-audio-init -Value Initialize-ConsultAudioEnvironment -Scope Global
Set-Alias -Name consult-audio-deactivate -Value Stop-ConsultAudioEnvironment -Scope Global
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
Set-Alias -Name demo-frontend-deploy -Value Deploy-Frontend -Scope Global
Set-Alias -Name frontend-status -Value Get-FrontendStatus -Scope Global
Set-Alias -Name clinic-frontend-deploy -Value Deploy-ClinicFrontend -Scope Global

# dbt Docs Commands
Set-Alias -Name dbt-docs-deploy -Value Deploy-DBTDocs -Scope Global

# AWS SSM Commands
Set-Alias -Name ssm-connect-api -Value Connect-SSMAPI -Scope Global
Set-Alias -Name ssm-connect-clinic-api -Value Connect-SSMClinicAPI -Scope Global
Set-Alias -Name ssm-connect-demo-db -Value Connect-SSMDemoDB -Scope Global
Set-Alias -Name ssm-port-forward-rds -Value Start-SSMPortForwardRDS -Scope Global
Set-Alias -Name ssm-port-forward-rds-clinic -Value Start-SSMPortForwardRDSClinic -Scope Global
Set-Alias -Name ssm-port-forward-demo-db -Value Start-SSMPortForwardDemoDB -Scope Global
Set-Alias -Name ssm-status -Value Get-SSMStatus -Scope Global

# Utility
Set-Alias -Name env-status -Value Get-EnvironmentStatus -Scope Global
Set-Alias -Name env-reload -Value Reload-EnvironmentManager -Scope Global

# =============================================================================
# STARTUP MESSAGE
# =============================================================================

if (-not $env:ENV_MANAGER_QUIET_RELOAD) {
Write-Host "`n╔══════════════════════════════════════════════════════════╗" -ForegroundColor DarkBlue
Write-Host "║          Data Engineering Environment Manager           ║" -ForegroundColor Blue
Write-Host "║            Dental Clinic ETL & dbt Pipeline             ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor DarkBlue

Write-Host "`nLegacy manager (-Legacy): deploy, SSM, frontend, consult-audio." -ForegroundColor White
Write-Host '  Daily dev: use load_project.ps1 (default) or mdc directly - no *-init required.' -ForegroundColor Gray
Write-Host '  *-init commands here run mdc validate only (Phase 4.6 deprecation).' -ForegroundColor DarkGray
Write-Host '  dbt-init / etl-init / api-init  - validate config for a stage' -ForegroundColor Cyan
Write-Host "  consult-audio-init - Initialize Consult Audio Pipeline (venv + deps in consult_audio_pipe/)" -ForegroundColor DarkCyan
Write-Host "  frontend-dev   - Start frontend (local: localhost:3000 → localhost:8000)" -ForegroundColor Green
Write-Host "  demo-frontend-deploy - Deploy demo frontend → dbtdentalclinic.com (public)" -ForegroundColor Green
Write-Host "  clinic-frontend-deploy - Deploy clinic frontend → clinic.dbtdentalclinic.com (IP-restricted)" -ForegroundColor Green
Write-Host "    Vite: CLINIC_API_URL (default https://api-clinic.dbtdentalclinic.com), CLINIC_API_KEY or api\.env_api_clinic" -ForegroundColor DarkGray
Write-Host "  dbt-docs-deploy - Deploy dbt docs to S3/CloudFront" -ForegroundColor Cyan
Write-Host "  env-status     - Check environment status" -ForegroundColor Yellow

Write-Host "`n☁️  AWS (EC2: run aws-ssm-init, then instance IDs load when you run a connect/port-forward command):" -ForegroundColor DarkCyan
Write-Host '  aws-ssm-init             - Set up SSM (region, PATH, RDS); instance IDs load on first use' -ForegroundColor DarkCyan
Write-Host "  ssm-connect-api          - Shell on dental-clinic-api-demo (api.dbtdentalclinic.com)" -ForegroundColor Gray
Write-Host "  ssm-connect-clinic-api   - Shell on dental-clinic-api-clinic (api-clinic.dbtdentalclinic.com)" -ForegroundColor Gray
Write-Host "  ssm-connect-demo-db      - Shell on dental-clinic-demo-db (demo database host)" -ForegroundColor Gray
Write-Host "  ssm-port-forward-rds     - Forward RDS to localhost (via dental-clinic-api-demo)" -ForegroundColor Gray
Write-Host "  ssm-port-forward-rds-clinic - Forward RDS to localhost (via dental-clinic-api-clinic)" -ForegroundColor Gray
Write-Host "  ssm-port-forward-demo-db - Forward demo DB to localhost" -ForegroundColor Gray
Write-Host "  ssm-status               - Show plugin status and cached instance IDs" -ForegroundColor Gray

# Auto-detect project type
$cwd = Get-Location
if ((Test-Path "$cwd\dbt_project.yml") -or (Test-Path "$cwd\dbt_dental_models\dbt_project.yml")) {
    Write-Host "`ndbt project detected. Use: mdc dbt run --env local" -ForegroundColor DarkGray
}
if (Test-Path "$cwd\etl_pipeline\Pipfile") {
    Write-Host "ETL pipeline detected. Use: mdc etl run --env clinic --profile full" -ForegroundColor DarkGray
}
if (Test-Path "$cwd\api\main.py") {
    Write-Host "API detected. Use: mdc api run --env local" -ForegroundColor DarkGray
}
if (Test-Path "$cwd\consult_audio_pipe\requirements.txt") {
    Write-Host "🎙️ Consult Audio Pipeline detected (consult_audio_pipe/). Run 'consult-audio-init' to start." -ForegroundColor DarkCyan
}
if (Test-Path "$cwd\frontend\package.json") {
    Write-Host "🎨 Frontend detected. Run 'frontend-dev' (local), 'demo-frontend-deploy' (demo → dbtdentalclinic.com), or 'clinic-frontend-deploy' (clinic → clinic.dbtdentalclinic.com)." -ForegroundColor Green
}

Write-Host ""
}

# Record load time so Sync-EnvironmentManagerScript can detect edits
$script:EnvManagerScriptFile = Join-Path $script:EnvManagerScriptRoot "environment_manager.ps1"
$script:EnvManagerFileTicks = (Get-Item $script:EnvManagerScriptFile).LastWriteTimeUtc.Ticks

# Export functions to global scope (only on full startup, not quiet reload)
if (-not $env:ENV_MANAGER_QUIET_RELOAD) {
Write-Host ""
Get-Command -Type Function | Where-Object {$_.Name -like "*ETL*" -or $_.Name -like "*DBT*" -or $_.Name -like "*API*" -or $_.Name -like "*ConsultAudio*"} | ForEach-Object {
    Set-Item -Path "function:global:$($_.Name)" -Value $_.Definition
}
Write-Host "✅ Functions loaded successfully!" -ForegroundColor Green
} 