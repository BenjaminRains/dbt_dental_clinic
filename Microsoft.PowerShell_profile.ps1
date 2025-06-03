# Microsoft.PowerShell_profile.ps1
# Enhanced Data Engineering Environment Manager with ETL CLI Integration

# Load ETL functions
$projectPath = "C:\Users\rains\dbt_dental_clinic"
$etlFunctionsPath = Join-Path $projectPath "etl_pipeline\cli\etl_functions.ps1"
if (Test-Path $etlFunctionsPath) {
    . $etlFunctionsPath
    Write-Host "✅ ETL functions loaded successfully" -ForegroundColor Green
} else {
    Write-Host "⚠️  ETL functions file not found at: $etlFunctionsPath" -ForegroundColor Yellow
}

# Enhanced dbt environment tracking with clear visual feedback
$script:IsInitialized = $false
$script:IsETLInitialized = $false
$script:OriginalEnv = $null
$script:ETLOriginalEnv = $null
$script:ActiveProject = $null 