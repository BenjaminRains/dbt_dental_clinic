# Start native Airflow on Windows.
# Loads airflow/.env.native (Fernet key, etc.) and UTF-8 mode for DAG files.
#
# Usage (from repo root):
#   .\scripts\utils\start-airflow-native.ps1 -SchedulerOnly   # terminal 1
#   .\scripts\utils\start-airflow-native.ps1 -WebserverOnly     # terminal 2
#
# Windows notes:
# - PYTHONPATH includes scripts/utils/windows_posix_stubs (pwd/grp for python-daemon)
# - .venv-airflow\Scripts must be on PATH for `airflow standalone` child processes
# - Two-terminal mode (-SchedulerOnly / -WebserverOnly) is more reliable than standalone

param(
    [switch]$SchedulerOnly,
    [switch]$WebserverOnly
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot

$AirflowHome = Join-Path $RepoRoot "airflow"
$LocalEnvFile = Join-Path $AirflowHome ".env.native"
$ScriptsDir = Join-Path $RepoRoot ".venv-airflow\Scripts"
$Python = Join-Path $ScriptsDir "python.exe"
$RunAirflow = Join-Path $RepoRoot "scripts\utils\run_airflow.py"
$WrapperDir = Join-Path $RepoRoot "scripts\utils\windows_airflow_wrapper"
$StubsDir = Join-Path $RepoRoot "scripts\utils\windows_posix_stubs"

if (-not (Test-Path $Python)) {
    Write-Error "Run .\scripts\utils\init-airflow-native.ps1 first."
}

$env:PYTHONPATH = if ($env:PYTHONPATH) { "$StubsDir;$env:PYTHONPATH" } else { $StubsDir }
# mdc + pipenv (user Python Scripts) — DAG tasks subprocess mdc etl/dbt/publish
$UserPyScripts = Join-Path $env:LOCALAPPDATA "Programs\Python\Python311\Scripts"
$PathPrefix = "$WrapperDir;$ScriptsDir"
if (Test-Path $UserPyScripts) {
    $PathPrefix = "$UserPyScripts;$PathPrefix"
}
$env:Path = "$PathPrefix;$env:Path"
$env:AIRFLOW_HOME = $AirflowHome
$env:PYTHONUTF8 = "1"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:AIRFLOW__CORE__DEFAULT_TIMEZONE = "America/Chicago"
$env:AIRFLOW__CORE__EXECUTOR = "SequentialExecutor"
$DbPath = (Join-Path $AirflowHome "airflow.db") -replace '\\', '/'
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "sqlite:///$DbPath"

if (Test-Path $LocalEnvFile) {
    Get-Content $LocalEnvFile | ForEach-Object {
        if ($_ -match '^\s*([^#=]+)=(.*)$') {
            Set-Item -Path "env:$($matches[1].Trim())" -Value $matches[2].Trim()
        }
    }
} else {
    Write-Warning "Missing $LocalEnvFile - Fernet key may be unset."
}

if ($SchedulerOnly) {
    Write-Host "Starting Airflow scheduler (AIRFLOW_HOME=$AirflowHome)" -ForegroundColor Cyan
    & $Python $RunAirflow scheduler --skip-serve-logs
    exit $LASTEXITCODE
}

if ($WebserverOnly) {
    Write-Host "Starting Airflow webserver at http://localhost:8080" -ForegroundColor Cyan
    Write-Host "Login: admin / (see airflow\.env.native)"
    & $Python $RunAirflow webserver --debug
    exit $LASTEXITCODE
}

Write-Host "Starting Airflow standalone (UI: http://localhost:8080)" -ForegroundColor Cyan
Write-Host "AIRFLOW_HOME=$AirflowHome"
Write-Host "Login: admin / (see airflow\.env.native)"
Write-Host ""
Write-Host "Recommended on Windows - use two terminals instead:" -ForegroundColor Yellow
Write-Host "  .\scripts\utils\start-airflow-native.ps1 -SchedulerOnly"
Write-Host "  .\scripts\utils\start-airflow-native.ps1 -WebserverOnly"
Write-Host ""

& $Python $RunAirflow standalone
