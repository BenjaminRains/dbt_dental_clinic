# List Environment Files — Inventory
#
# Reports which expected .env / .env_* files exist in the repo (Present/Missing).
# Does NOT print file contents or secrets. Use for onboarding and audits.
#
# Usage: .\scripts\list_env_files.ps1
#        .\scripts\list_env_files.ps1 -ProjectRoot "C:\path\to\dbt_dental_clinic"

param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"
Push-Location $ProjectRoot

# Expected env files: path relative to project root, short description
$expected = @(
    # Root / Docker / Airflow
    @{ Path = ".env";            Desc = "Root (Docker/Airflow)"; Template = ".env.template" },
    @{ Path = ".env.template";   Desc = "Root template (committed)"; Template = $null },
    @{ Path = ".env_ec2";       Desc = "EC2 root env source (deploy)"; Template = $null },
    # API
    @{ Path = "api\.env_api_local";  Desc = "API local";  Template = $null },
    @{ Path = "api\.env_api_demo";   Desc = "API demo";   Template = $null },
    @{ Path = "api\.env_api_clinic"; Desc = "API clinic"; Template = $null },
    @{ Path = "api\.env_api_test";   Desc = "API test";   Template = "api\.env_api_test.template" },
    @{ Path = "api\.env_api_test.template"; Desc = "API test template (committed)"; Template = $null },
    # ETL pipeline
    @{ Path = "etl_pipeline\.env_local";   Desc = "ETL local";   Template = "etl_pipeline\.env_local.template" },
    @{ Path = "etl_pipeline\.env_clinic";   Desc = "ETL clinic"; Template = "etl_pipeline\.env_clinic.template" },
    @{ Path = "etl_pipeline\.env_test";     Desc = "ETL test";   Template = "etl_pipeline\.env_test.template" },
    @{ Path = "etl_pipeline\.env_local.template";    Desc = "ETL local template (committed)"; Template = $null },
    @{ Path = "etl_pipeline\.env_clinic.template";   Desc = "ETL clinic template (committed)"; Template = $null },
    @{ Path = "etl_pipeline\.env_test.template";     Desc = "ETL test template (committed)"; Template = $null },
    # dbt (env manager uses dbt_dental_models/ only; no project-root fallback)
    @{ Path = "dbt_dental_models\.env_local"; Desc = "dbt local (dbt_dental_models)"; Template = "etl_pipeline\.env_local.template" },
    @{ Path = "dbt_dental_models\.env_clinic"; Desc = "dbt clinic (dbt_dental_models)"; Template = "etl_pipeline\.env_clinic.template" },
    # Frontend
    @{ Path = "frontend\.env";        Desc = "Frontend default"; Template = $null },
    @{ Path = "frontend\.env.local";  Desc = "Frontend local dev"; Template = $null },
    @{ Path = "frontend\.env.production"; Desc = "Frontend production build"; Template = $null },
    # Consult audio pipe
    @{ Path = "consult_audio_pipe\.env";        Desc = "Consult audio pipe"; Template = $null },
    @{ Path = "consult_audio_pipe\.env.template"; Desc = "Consult audio template (committed)"; Template = $null },
    # Synthetic data generator
    @{ Path = "etl_pipeline\synthetic_data_generator\.env_demo";        Desc = "Synthetic data demo"; Template = $null },
    @{ Path = "etl_pipeline\synthetic_data_generator\.env_demo.template"; Desc = "Synthetic data demo template (committed)"; Template = $null }
)

Write-Host "`nEnvironment file inventory (project root: $ProjectRoot)" -ForegroundColor Cyan
Write-Host "Legend: [Present] / [Missing] — templates are expected to be committed; others are optional per environment.`n" -ForegroundColor Gray

$present = 0
$missing = 0
foreach ($e in $expected) {
    $fullPath = Join-Path $ProjectRoot $e.Path
    $exists = Test-Path -LiteralPath $fullPath
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

Write-Host "`nTotal: $present present, $missing missing." -ForegroundColor Cyan
Write-Host "See docs/ENVIRONMENT_FILES.md for how each file is used and which are required for your workflow.`n" -ForegroundColor Gray

Pop-Location
