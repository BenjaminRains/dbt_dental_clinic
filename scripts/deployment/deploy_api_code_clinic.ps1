# Deploy production API Python files to clinic EC2 (batch).
param(
    [switch]$Clinic,
    [string]$InstanceId = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$deployScript = Join-Path $repoRoot "scripts\deployment\deploy_api_file.ps1"
Push-Location $repoRoot
try {
$files = @(
    "api\main.py", "api\config.py", "api\settings.py", "api\database.py", "api\deps.py", "api\cors_runtime.py",
    "api\api_types.py",
    "api\models\__init__.py", "api\models\patient.py", "api\models\provider.py", "api\models\appointment.py",
    "api\models\ar.py", "api\models\revenue.py", "api\models\hygiene.py", "api\models\treatment_acceptance.py",
    "api\models\dbt_metadata.py",
    "api\routers\reports.py", "api\routers\revenue.py", "api\routers\ar.py", "api\routers\hygiene.py",
    "api\routers\provider.py", "api\routers\patient.py", "api\routers\appointment.py",
    "api\routers\treatment_acceptance.py", "api\routers\dbt_metadata.py",
    "api\services\revenue_service.py", "api\services\ar_service.py", "api\services\hygiene_service.py",
    "api\services\provider_service.py", "api\services\patient_service.py", "api\services\appointment_service.py",
    "api\services\treatment_acceptance_service.py",
    "api\auth\__init__.py", "api\auth\api_key.py",
    "api\middleware\__init__.py", "api\middleware\rate_limit.py", "api\middleware\security_headers.py",
    "api\middleware\request_logger.py"
)

$n = $files.Count
for ($i = 0; $i -lt $n; $i++) {
    $f = $files[$i]
    $path = Join-Path $repoRoot $f
    if (-not (Test-Path $path)) {
        Write-Host "SKIP missing: $f" -ForegroundColor Yellow
        continue
    }
    $isLast = ($i -eq ($n - 1))
    Write-Host "`n=== Deploy $($i+1)/$n : $f ===" -ForegroundColor Cyan
    if ($Clinic) {
        if ($isLast) {
            & $deployScript -FilePath $f -Clinic
        } else {
            & $deployScript -FilePath $f -Clinic -NoRestart
        }
    } elseif ($InstanceId) {
        if ($isLast) {
            & $deployScript -FilePath $f -InstanceId $InstanceId
        } else {
            & $deployScript -FilePath $f -InstanceId $InstanceId -NoRestart
        }
    } else {
        if ($isLast) {
            & $deployScript -FilePath $f
        } else {
            & $deployScript -FilePath $f -NoRestart
        }
    }
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
Write-Host "`nAll API files deployed." -ForegroundColor Green
} finally {
    Pop-Location
}
