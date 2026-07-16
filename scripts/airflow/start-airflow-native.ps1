# Start native Airflow 3 on Windows.
# Loads airflow/.env.native (Fernet key, etc.) and UTF-8 mode for DAG files.
#
# Usage (from repo root) — three terminals recommended:
#   .\scripts\airflow\start-airflow-native.ps1 -SchedulerOnly      # terminal 1
#   .\scripts\airflow\start-airflow-native.ps1 -DagProcessorOnly   # terminal 2
#   .\scripts\airflow\start-airflow-native.ps1 -ApiServerOnly      # terminal 3
#
# -WebserverOnly is an alias for -ApiServerOnly (Airflow 2 name).
#
# Windows notes:
# - PYTHONPATH includes scripts/airflow/windows_posix_stubs (pwd/grp for python-daemon)
# - .venv-airflow\Scripts must be on PATH for child processes
# - Three-terminal mode is more reliable than standalone on Windows

param(
    [switch]$SchedulerOnly,
    [switch]$DagProcessorOnly,
    [switch]$ApiServerOnly,
    [switch]$WebserverOnly  # alias for ApiServerOnly
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot

$AirflowHome = Join-Path $RepoRoot "airflow"
$LocalEnvFile = Join-Path $AirflowHome ".env.native"
$ScriptsDir = Join-Path $RepoRoot ".venv-airflow\Scripts"
$Python = Join-Path $ScriptsDir "python.exe"
$RunAirflow = Join-Path $RepoRoot "scripts\airflow\run_airflow.py"
$WrapperDir = Join-Path $RepoRoot "scripts\airflow\windows_airflow_wrapper"
$StubsDir = Join-Path $RepoRoot "scripts\airflow\windows_posix_stubs"

if (-not (Test-Path $Python)) {
    Write-Error "Run .\scripts\airflow\init-airflow-native.ps1 first."
}

$env:PYTHONPATH = if ($env:PYTHONPATH) { "$StubsDir;$env:PYTHONPATH" } else { $StubsDir }
# DAG helpers (`from lib.mdc_runner`) — BundleDagBag also adds this; keep for CLI/tests
$env:PYTHONPATH = "$(Join-Path $AirflowHome 'dags');$env:PYTHONPATH"
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
$env:AIRFLOW__CORE__EXECUTOR = "LocalExecutor"
$env:AIRFLOW__CORE__AUTH_MANAGER = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
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

# Airflow 3.2+: local webserver_config may still import removed airflow.www FAB paths
$WebserverConfig = Join-Path $AirflowHome "webserver_config.py"
if (Test-Path $WebserverConfig) {
    $cfg = Get-Content $WebserverConfig -Raw
    if ($cfg -match 'airflow\.www\.fab_security') {
        $cfg = $cfg -replace 'from airflow\.www\.fab_security\.manager import AUTH_DB', 'from flask_appbuilder.const import AUTH_DB'
        $cfg = $cfg -replace 'from airflow\.www\.fab_security\.manager import AUTH_LDAP', 'from flask_appbuilder.const import AUTH_LDAP'
        $cfg = $cfg -replace 'from airflow\.www\.fab_security\.manager import AUTH_OAUTH', 'from flask_appbuilder.const import AUTH_OAUTH'
        $cfg = $cfg -replace 'from airflow\.www\.fab_security\.manager import AUTH_OID', 'from flask_appbuilder.const import AUTH_OID'
        $cfg = $cfg -replace 'from airflow\.www\.fab_security\.manager import AUTH_REMOTE_USER', 'from flask_appbuilder.const import AUTH_REMOTE_USER'
        Set-Content -Path $WebserverConfig -Value $cfg -Encoding UTF8
        Write-Host "Patched $WebserverConfig for Flask-AppBuilder AUTH_* imports (Airflow 3.2+)."
    }
}

if ($SchedulerOnly) {
    Write-Host "Starting Airflow scheduler (AIRFLOW_HOME=$AirflowHome)" -ForegroundColor Cyan
    & $Python $RunAirflow scheduler --skip-serve-logs
    exit $LASTEXITCODE
}

if ($DagProcessorOnly) {
    Write-Host "Starting Airflow dag-processor (AIRFLOW_HOME=$AirflowHome)" -ForegroundColor Cyan
    & $Python $RunAirflow dag-processor
    exit $LASTEXITCODE
}

if ($ApiServerOnly -or $WebserverOnly) {
    Write-Host "Starting Airflow api-server at http://localhost:8080" -ForegroundColor Cyan
    Write-Host "Login: admin / (see airflow\.env.native)"
    & $Python $RunAirflow api-server --port 8080
    exit $LASTEXITCODE
}

Write-Host "Starting Airflow standalone (UI: http://localhost:8080)" -ForegroundColor Cyan
Write-Host "AIRFLOW_HOME=$AirflowHome"
Write-Host "Login: admin / (see airflow\.env.native)"
Write-Host ""
Write-Host "Recommended on Windows - use three terminals instead:" -ForegroundColor Yellow
Write-Host "  .\scripts\airflow\start-airflow-native.ps1 -SchedulerOnly"
Write-Host "  .\scripts\airflow\start-airflow-native.ps1 -DagProcessorOnly"
Write-Host "  .\scripts\airflow\start-airflow-native.ps1 -ApiServerOnly"
Write-Host ""

& $Python $RunAirflow standalone
