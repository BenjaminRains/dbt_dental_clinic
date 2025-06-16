# Microsoft.PowerShell_profile.ps1
# Enhanced Data Engineering Environment Manager with ETL CLI Integration

# Load ETL functions from project directory
$projectPath = "C:\Users\rains\dbt_dental_clinic"
$etlFunctionsPath = Join-Path $projectPath "etl_pipeline\cli\etl_functions.ps1"
if (Test-Path $etlFunctionsPath) {
    . $etlFunctionsPath
    Write-Host "✅ ETL functions loaded successfully from project" -ForegroundColor Green
} else {
    Write-Host "ℹ️  ETL functions not found in project directory (this is optional)" -ForegroundColor Cyan
}

# Enhanced dbt environment tracking with clear visual feedback
$script:IsInitialized = $false
$script:IsETLInitialized = $false
$script:OriginalEnv = $null
$script:ETLOriginalEnv = $null
$script:ActiveProject = $null
$script:VenvPath = $null
$script:ETLVenvPath = $null

# =============================================================================
# DBT ENVIRONMENT FUNCTIONS
# =============================================================================

# =============================================================================
# ETL ENVIRONMENT FUNCTIONS
# =============================================================================

# ETL initialization command
function etl-init {
    param(
        [string]$ProjectPath = $PWD.Path
    )
    
    if ($script:IsETLInitialized) {
        Write-Host "⚠️  ETL environment already initialized" -ForegroundColor Yellow
        return
    }
    
    $result = Initialize-ETLEnvironment -ProjectPath $ProjectPath
    if ($result) {
        $script:IsETLInitialized = $true
        $script:ETLOriginalEnv = $env:PATH
        $script:ETLVenvPath = pipenv --venv
        
        # Set up ETL command aliases
        Set-Alias -Name etl -Value python -ArgumentList "-m etl_pipeline.cli.entry"
        Set-Alias -Name etl-v -Value Quick-ETLValidate
        Set-Alias -Name etl-s -Value Quick-ETLStatus
        Set-Alias -Name etl-r -Value Quick-ETLRun
        Set-Alias -Name etl-t -Value Test-ETLConnections
        Set-Alias -Name etl-p -Value Run-ETLPipeline
        Set-Alias -Name etl-d -Value Deactivate-ETLEnvironment
    }
}

# ETL deactivation command
function etl-deactivate {
    if (-not $script:IsETLInitialized) {
        Write-Host "⚠️  ETL environment not initialized" -ForegroundColor Yellow
        return
    }
    
    Deactivate-ETLEnvironment
    $script:IsETLInitialized = $false
    $script:ETLOriginalEnv = $null
    $script:ETLVenvPath = $null
    
    # Remove ETL command aliases
    Remove-Item Alias:etl -ErrorAction SilentlyContinue
    Remove-Item Alias:etl-v -ErrorAction SilentlyContinue
    Remove-Item Alias:etl-s -ErrorAction SilentlyContinue
    Remove-Item Alias:etl-r -ErrorAction SilentlyContinue
    Remove-Item Alias:etl-t -ErrorAction SilentlyContinue
    Remove-Item Alias:etl-p -ErrorAction SilentlyContinue
    Remove-Item Alias:etl-d -ErrorAction SilentlyContinue
}

# =============================================================================
# ENHANCED ETL CLI COMMANDS
# ============================================================================= 