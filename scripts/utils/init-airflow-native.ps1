# Initialize native Airflow for Phase A (test env) on Windows.
# Does NOT trigger a DAG run.
#
# Usage (from repo root):
#   .\scripts\utils\init-airflow-native.ps1
#
# Optional env overrides:
#   AIRFLOW_ADMIN_PASSWORD   default: random 16-char
#   AIRFLOW_METADATA_URL     default: SQLite under airflow/airflow.db
#     Postgres example (requires CREATEDB or pre-created DB):
#       postgresql+psycopg2://user:pass@localhost:5432/airflow

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot

$VenvDir = Join-Path $RepoRoot ".venv-airflow"
$AirflowHome = Join-Path $RepoRoot "airflow"
$Python = Join-Path $VenvDir "Scripts\python.exe"
$Airflow = Join-Path $VenvDir "Scripts\airflow.exe"

function Write-Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

Write-Step "Create venv (.venv-airflow)"
if (-not (Test-Path $VenvDir)) {
    python -m venv $VenvDir
}

Write-Step "Install Airflow 2.7.3 + DAG deps"
& $Python -m pip install --upgrade pip | Out-Null
& $Python -m pip install -r (Join-Path $RepoRoot "requirements-airflow-native.txt")

Write-Step "Install Windows POSIX patch (all Python subprocesses in venv)"
$SitePackages = Join-Path $VenvDir "Lib\site-packages"
$PatchSrc = Join-Path $StubsDir "airflow_win_patch.py"
Copy-Item -Force $PatchSrc (Join-Path $SitePackages "airflow_win_patch.py")
Set-Content -Path (Join-Path $SitePackages "airflow_win_bootstrap.pth") -Value "import airflow_win_patch" -Encoding ascii

Write-Step "Configure AIRFLOW_HOME"
$env:AIRFLOW_HOME = $AirflowHome
$env:PYTHONUTF8 = "1"
$StubsDir = Join-Path $RepoRoot "scripts\utils\windows_posix_stubs"
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$StubsDir;$env:PYTHONPATH" } else { $StubsDir }
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:AIRFLOW__CORE__DEFAULT_TIMEZONE = "America/Chicago"
# SequentialExecutor: safest on Windows for local smoke tests
$env:AIRFLOW__CORE__EXECUTOR = "SequentialExecutor"
$env:AIRFLOW__CORE__DAGS_FOLDER = (Join-Path $AirflowHome "dags")

if ($env:AIRFLOW_METADATA_URL) {
    $env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = $env:AIRFLOW_METADATA_URL
} else {
    $DbPath = (Join-Path $AirflowHome "airflow.db") -replace '\\', '/'
    $env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "sqlite:///$DbPath"
}

function Get-IssuedFernetKey {
    $candidates = @(
        (Join-Path $AirflowHome ".env.native"),
        (Join-Path $RepoRoot ".env")
    )
    foreach ($path in $candidates) {
        if (-not (Test-Path $path)) { continue }
        foreach ($line in Get-Content $path) {
            if ($line -match '^\s*AIRFLOW_FERNET_KEY=(.+)$') {
                $key = $matches[1].Trim()
                if ($key -and -not $key.EndsWith('=')) { $key += '=' }
                return $key
            }
            if ($line -match '^\s*AIRFLOW__CORE__FERNET_KEY=(.+)$') {
                $key = $matches[1].Trim()
                if ($key -and -not $key.EndsWith('=')) { $key += '=' }
                return $key
            }
        }
    }
    return $null
}

$LocalEnvFile = Join-Path $AirflowHome ".env.native"
if (-not (Test-Path $LocalEnvFile)) {
    $fernet = Get-IssuedFernetKey
    if (-not $fernet) {
        $fernet = & $Python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
        Write-Host "Generated new Fernet key (no AIRFLOW_FERNET_KEY found in repo env files)."
    } else {
        Write-Host "Using Fernet key from airflow\.env.native (or root .env)."
    }
    $adminPass = if ($env:AIRFLOW_ADMIN_PASSWORD) { $env:AIRFLOW_ADMIN_PASSWORD } else {
        -join ((48..57) + (65..90) + (97..122) | Get-Random -Count 16 | ForEach-Object { [char]$_ })
    }
    @(
        "# Native Airflow local config (Phase A). Not used by mdc/ETL.",
        "AIRFLOW__CORE__FERNET_KEY=$fernet",
        "AIRFLOW_ADMIN_USERNAME=admin",
        "AIRFLOW_ADMIN_PASSWORD=$adminPass",
        "AIRFLOW_ADMIN_EMAIL=rains.bp@gmail.com"
    ) | Set-Content -Path $LocalEnvFile -Encoding UTF8
    Write-Host "Wrote $LocalEnvFile (admin password stored there)."
}

Get-Content $LocalEnvFile | ForEach-Object {
    if ($_ -match '^\s*([^#=]+)=(.*)$') {
        $key = $matches[1].Trim()
        $val = $matches[2].Trim()
        Set-Item -Path "env:$key" -Value $val
    }
}

New-Item -ItemType Directory -Force -Path (Join-Path $AirflowHome "dags") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $AirflowHome "logs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $AirflowHome "plugins") | Out-Null

Write-Step "Initialize metadata DB (airflow db init)"
& $Airflow db init

Write-Step "Create admin user (skip if exists)"
$adminUser = $env:AIRFLOW_ADMIN_USERNAME
if (-not $adminUser) { $adminUser = "admin" }
$adminPass = $env:AIRFLOW_ADMIN_PASSWORD
$adminEmail = if ($env:AIRFLOW_ADMIN_EMAIL) { $env:AIRFLOW_ADMIN_EMAIL } else { "rains.bp@gmail.com" }

& $Airflow users create `
    --username $adminUser `
    --password $adminPass `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email $adminEmail 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Admin user may already exist (ok)."
}

Write-Step "Set Phase A Airflow Variables"
& $Airflow variables set project_root $RepoRoot
& $Airflow variables set etl_environment test
& $Airflow variables set dbt_target local
# publish_environment intentionally unset for Phase A

Write-Step "Verify DAGs parse"
& $Airflow dags list

Write-Step "Run DAG unit tests"
& $Python -m pytest (Join-Path $RepoRoot "airflow\tests") -v

Write-Host "`nDone. Native Airflow ready (no DAG run triggered)." -ForegroundColor Green
Write-Host "  AIRFLOW_HOME: $AirflowHome"
Write-Host "  Admin login:  $adminUser / (see airflow\.env.native)"
Write-Host "  Start UI:     .\scripts\utils\start-airflow-native.ps1"
Write-Host "  Full DAG run: after 9 PM Central (business-hours guard)"
