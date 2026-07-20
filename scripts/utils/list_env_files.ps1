# List Environment Files — Inventory
#
# Reports which expected .env / .env_* files exist in the repo (Present/Missing).
# Does NOT print file contents or secrets. Use for onboarding and audits.
#
# Usage: .\scripts\utils\list_env_files.ps1
#        .\scripts\utils\list_env_files.ps1 -ProjectRoot "C:\path\to\dbt_dental_clinic"

param(
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}
Push-Location $ProjectRoot

# Expected env files: path relative to project root, short description
# Deprecated=$true: file should not exist (Phase 6 credential dedup)
$expected = @(
    # Root / Docker / Airflow
    @{ Path = ".env";            Desc = "Root (Docker/Airflow sandbox only)"; Template = ".env.template"; Deprecated = $false },
    @{ Path = ".env.template";   Desc = "Root template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = ".env_ec2";       Desc = "EC2 root env source (deploy)"; Template = $null; Deprecated = $false },
    # API
    @{ Path = "api\.env_api_local";  Desc = "API local";  Template = "api\.env_api_local.template"; Deprecated = $false },
    @{ Path = "api\.env_api_demo";   Desc = "API demo";   Template = $null; Deprecated = $false },
    @{ Path = "api\.env_api_clinic"; Desc = "API clinic (deploy file; sync password via mdc secrets pull clinic)"; Template = "api\.env_api_clinic.template"; Deprecated = $false },
    @{ Path = "api\.env_api_test";   Desc = "API test";   Template = "api\.env_api_test.template"; Deprecated = $false },
    @{ Path = "api\.env_api_local.template";  Desc = "API local template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = "api\.env_api_clinic.template"; Desc = "API clinic template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = "api\.env_api_test.template"; Desc = "API test template (committed)"; Template = $null; Deprecated = $false },
    # ETL pipeline
    @{ Path = "etl_pipeline\.env_local";   Desc = "ETL local";   Template = "etl_pipeline\.env_local.template"; Deprecated = $false },
    @{ Path = "etl_pipeline\.env_clinic";   Desc = "ETL clinic (source + replication; Phase 6: no analytics vars)"; Template = "etl_pipeline\.env_clinic.template"; Deprecated = $false },
    @{ Path = "etl_pipeline\.env_test";     Desc = "ETL test";   Template = "etl_pipeline\.env_test.template"; Deprecated = $false },
    @{ Path = "etl_pipeline\.env_local.template";    Desc = "ETL local template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = "etl_pipeline\.env_clinic.template";   Desc = "ETL clinic template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = "etl_pipeline\.env_test.template";     Desc = "ETL test template (committed)"; Template = $null; Deprecated = $false },
    # dbt
    @{ Path = "dbt_dental_models\.env_local"; Desc = "dbt local warehouse (canonical local POSTGRES_ANALYTICS_*)"; Template = "dbt_dental_models\.env_local.template"; Deprecated = $false },
    @{ Path = "dbt_dental_models\.env_local.template"; Desc = "dbt local template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = "dbt_dental_models\.env_snowflake"; Desc = "dbt Snowflake portfolio mini warehouse (SNOWFLAKE_*; key-pair via SNOWFLAKE_PRIVATE_KEY_PATH; synthetic only)"; Template = "dbt_dental_models\.env_snowflake.template"; Deprecated = $false },
    @{ Path = "dbt_dental_models\.env_snowflake.template"; Desc = "dbt Snowflake template (committed)"; Template = $null; Deprecated = $false },
    @{ Path = "dbt_dental_models\.snowflake\rsa_key.p8"; Desc = "Snowflake private key file (gitignored; path referenced by .env_snowflake)"; Template = $null; Deprecated = $false },
    @{ Path = "dbt_dental_models\.env_clinic"; Desc = "DEPRECATED — use deployment_credentials.json + Secrets Manager"; Template = $null; Deprecated = $true },
    # Frontend
    @{ Path = "frontend\.env";        Desc = "Frontend default"; Template = $null; Deprecated = $false },
    @{ Path = "frontend\.env.local";  Desc = "Frontend local dev"; Template = $null; Deprecated = $false },
    @{ Path = "frontend\.env.production"; Desc = "Frontend production build"; Template = $null; Deprecated = $false },
    # Consult audio pipe
    @{ Path = "consult_audio_pipe\.env";        Desc = "Consult audio pipe"; Template = "consult_audio_pipe\.env.template"; Deprecated = $false },
    @{ Path = "consult_audio_pipe\.env.template"; Desc = "Consult audio template (committed)"; Template = $null; Deprecated = $false },
    # Synthetic data generator
    @{ Path = "etl_pipeline\synthetic_data_generator\.env_demo";        Desc = "Synthetic data demo"; Template = "etl_pipeline\synthetic_data_generator\.env_demo.template"; Deprecated = $false },
    @{ Path = "etl_pipeline\synthetic_data_generator\.env_demo.template"; Desc = "Synthetic data demo template (committed)"; Template = $null; Deprecated = $false }
)

Write-Host "`nEnvironment file inventory (project root: $ProjectRoot)" -ForegroundColor Cyan
Write-Host "Legend: [Present] / [Missing] / [Deprecated absent OK] / [Deprecated present - delete]`n" -ForegroundColor Gray

$present = 0
$missing = 0
$deprecatedAbsent = 0
$deprecatedPresent = 0

foreach ($e in $expected) {
    $fullPath = Join-Path $ProjectRoot $e.Path
    $exists = Test-Path -LiteralPath $fullPath
    $isDeprecated = $e.Deprecated -eq $true

    if ($isDeprecated -and -not $exists) {
        Write-Host "  [Deprecated absent OK] $($e.Path)" -ForegroundColor DarkGray
        Write-Host "            $($e.Desc)" -ForegroundColor DarkGray
        $deprecatedAbsent++
        continue
    }

    if ($isDeprecated -and $exists) {
        Write-Host "  [Deprecated present - delete] $($e.Path)" -ForegroundColor Magenta
        Write-Host "            $($e.Desc)" -ForegroundColor DarkGray
        $deprecatedPresent++
        continue
    }

    if ($exists) {
        Write-Host "  [Present] $($e.Path)" -ForegroundColor Green
        Write-Host "            $($e.Desc)" -ForegroundColor DarkGray
        $present++
    } else {
        Write-Host "  [Missing] $($e.Path)" -ForegroundColor Yellow
        Write-Host "            $($e.Desc)" -ForegroundColor DarkGray
        if ($e.Template) {
            Write-Host "            Template: $($e.Template)" -ForegroundColor Gray
        }
        $missing++
    }
}

Write-Host "`nTotal: $present present, $missing missing, $deprecatedAbsent deprecated absent (OK), $deprecatedPresent deprecated present (remove)." -ForegroundColor Cyan
Write-Host "See docs/deployment/ENVIRONMENT_FILES.md for usage details." -ForegroundColor Gray
Write-Host ""

Pop-Location
