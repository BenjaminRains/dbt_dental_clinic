# Publish local dbt analytics schemas to clinic RDS (Phase 3.5b).
#
# Workflow: ETL + dbt run locally (localhost opendental_analytics). This script copies
# marts (+ int, staging for API joins) to RDS for api-clinic.dbtdentalclinic.com.
#
# Prerequisites:
#   - Local Postgres has fresh dbt output (marts, int, staging)
#   - pg_dump / pg_restore on PATH (PostgreSQL client tools)
#   - RDS reachable: mdc tunnel clinic-db (default) OR -UseDirectRds
#   - etl_pipeline/.env_clinic (local POSTGRES_ANALYTICS_*)
#   - api/.env_api_clinic (RDS credentials; tunnel uses 127.0.0.1:TunnelPort)
#
# Usage (from repo root):
#   mdc tunnel clinic-db                    # separate terminal, keep open
#   pwsh -File scripts/publish/publish_analytics_to_rds.ps1
#   mdc publish analytics --env clinic      # wrapper (after pip install -e tools/mdc_cli)
#
param(
    [string]$ProjectRoot = "",
    [string]$LocalEnvFile = "",
    [string]$RemoteEnvFile = "",
    [string[]]$Schemas = @("marts", "int", "staging"),
    [int]$TunnelPort = 5433,
    [switch]$UseDirectRds,
    [switch]$DryRun,
    [switch]$SkipVerify
)

$ErrorActionPreference = "Stop"

function Read-DotEnvMap {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Env file not found: $Path"
    }
    $map = @{}
    foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
        $t = $line.Trim()
        if (-not $t -or $t.StartsWith("#")) { continue }
        $eq = $t.IndexOf("=")
        if ($eq -lt 1) { continue }
        $key = $t.Substring(0, $eq).Trim()
        $val = $t.Substring($eq + 1).Trim()
        if ($val.StartsWith('"') -and $val.EndsWith('"')) { $val = $val.Substring(1, $val.Length - 2) }
        if ($val.StartsWith("'") -and $val.EndsWith("'")) { $val = $val.Substring(1, $val.Length - 2) }
        $hash = $val.IndexOf("#")
        if ($hash -ge 0) { $val = $val.Substring(0, $hash).Trim() }
        if ($key) { $map[$key] = $val }
    }
    return $map
}

function Get-PgExecutable {
    param([Parameter(Mandatory = $true)][string]$Name)
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $fallback = Join-Path ${env:ProgramFiles} "PostgreSQL\17\bin\$Name.exe"
    if (Test-Path -LiteralPath $fallback) { return $fallback }
    throw "Missing $Name. Install PostgreSQL client tools or add pg_dump to PATH."
}

function Invoke-PsqlCount {
    param(
        [string]$PgExeDir,
        [hashtable]$Conn,
        [string]$Sql
    )
    $env:PGPASSWORD = $Conn.Password
    $env:PGSSLMODE = $Conn.SslMode
    $out = & (Join-Path $PgExeDir "psql.exe") `
        -h $Conn.Host -p $Conn.Port -U $Conn.User -d $Conn.Database `
        -t -A -c $Sql 2>&1
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $out" }
    return ($out | Out-String).Trim()
}

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}

if (-not $LocalEnvFile) {
    $LocalEnvFile = Join-Path $ProjectRoot "etl_pipeline\.env_clinic"
    if (-not (Test-Path -LiteralPath $LocalEnvFile)) {
        $LocalEnvFile = Join-Path $ProjectRoot "dbt_dental_models\.env_local"
    }
}
if (-not $RemoteEnvFile) {
    $RemoteEnvFile = Join-Path $ProjectRoot "api\.env_api_clinic"
}

Write-Host "`nPublish local analytics -> clinic RDS" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray
Write-Host "Local env:  $LocalEnvFile" -ForegroundColor Gray
Write-Host "Remote env: $RemoteEnvFile" -ForegroundColor Gray
Write-Host "Schemas:    $($Schemas -join ', ')" -ForegroundColor Gray

$local = Read-DotEnvMap -Path $LocalEnvFile
$remote = Read-DotEnvMap -Path $RemoteEnvFile

$localConn = @{
    Host     = if ($local.POSTGRES_ANALYTICS_HOST) { $local.POSTGRES_ANALYTICS_HOST } else { "localhost" }
    Port     = if ($local.POSTGRES_ANALYTICS_PORT) { $local.POSTGRES_ANALYTICS_PORT } else { "5432" }
    Database = if ($local.POSTGRES_ANALYTICS_DB) { $local.POSTGRES_ANALYTICS_DB } else { "opendental_analytics" }
    User     = if ($local.POSTGRES_ANALYTICS_USER) { $local.POSTGRES_ANALYTICS_USER } else { "analytics_user" }
    Password = $local.POSTGRES_ANALYTICS_PASSWORD
    SslMode  = if ($local.POSTGRES_ANALYTICS_SSLMODE) { $local.POSTGRES_ANALYTICS_SSLMODE } else { "prefer" }
}

$rdsConn = @{
    Host     = if ($UseDirectRds) { $remote.POSTGRES_ANALYTICS_HOST } else { "127.0.0.1" }
    Port     = if ($UseDirectRds) { $remote.POSTGRES_ANALYTICS_PORT } else { "$TunnelPort" }
    Database = if ($remote.POSTGRES_ANALYTICS_DB) { $remote.POSTGRES_ANALYTICS_DB } else { "opendental_analytics" }
    User     = if ($remote.POSTGRES_ANALYTICS_USER) { $remote.POSTGRES_ANALYTICS_USER } else { "analytics_user" }
    Password = $remote.POSTGRES_ANALYTICS_PASSWORD
    SslMode  = if ($remote.POSTGRES_ANALYTICS_SSLMODE) { $remote.POSTGRES_ANALYTICS_SSLMODE } else { "require" }
}

if (-not $localConn.Password) { throw "POSTGRES_ANALYTICS_PASSWORD missing in local env file." }
if (-not $rdsConn.Password) { throw "POSTGRES_ANALYTICS_PASSWORD missing in remote env file." }
if (-not $UseDirectRds) {
    Write-Host "RDS via tunnel: $($rdsConn.Host):$($rdsConn.Port) (run: mdc tunnel clinic-db)" -ForegroundColor Yellow
}

$pgDump = Get-PgExecutable -Name "pg_dump"
$pgRestore = Get-PgExecutable -Name "pg_restore"
$pgDir = Split-Path -Parent $pgDump

Write-Host "`nPreflight: local schema table counts..." -ForegroundColor Yellow
$schemaList = ($Schemas | ForEach-Object { "'$_'" }) -join ","
$countSql = @"
SELECT table_schema, COUNT(*)::text
FROM information_schema.tables
WHERE table_schema IN ($schemaList) AND table_type = 'BASE TABLE'
GROUP BY 1 ORDER BY 1;
"@
$localCounts = Invoke-PsqlCount -PgExeDir $pgDir -Conn $localConn -Sql $countSql
Write-Host $localCounts -ForegroundColor Gray
if (-not $localCounts -or $localCounts -notmatch "marts") {
    throw "Local marts schema missing or empty. Run dbt locally before publish."
}

if (-not $UseDirectRds) {
    $tcp = Test-NetConnection -ComputerName $rdsConn.Host -Port ([int]$rdsConn.Port) -WarningAction SilentlyContinue
    if (-not $tcp.TcpTestSucceeded) {
        throw "Tunnel not reachable on $($rdsConn.Host):$($rdsConn.Port). Start: mdc tunnel clinic-db"
    }
}

Write-Host "Preflight: RDS connection..." -ForegroundColor Yellow
$null = Invoke-PsqlCount -PgExeDir $pgDir -Conn $rdsConn -Sql "SELECT 1"

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dumpFile = Join-Path $env:TEMP "mdc_analytics_publish_$stamp.dump"
Write-Host "`nDumping local schemas -> $dumpFile" -ForegroundColor Yellow

$dumpArgs = @(
    "-h", $localConn.Host,
    "-p", $localConn.Port,
    "-U", $localConn.User,
    "-d", $localConn.Database,
    "-Fc",
    "--no-owner",
    "--no-acl",
    "-f", $dumpFile
)
foreach ($schema in $Schemas) {
    $dumpArgs += @("-n", $schema)
}

if ($DryRun) {
    Write-Host "DryRun: would run pg_dump with schemas: $($Schemas -join ', ')" -ForegroundColor Green
    exit 0
}

$env:PGPASSWORD = $localConn.Password
$env:PGSSLMODE = $localConn.SslMode
& $pgDump @dumpArgs
if ($LASTEXITCODE -ne 0) { throw "pg_dump failed (exit $LASTEXITCODE)" }
$sizeMb = [math]::Round((Get-Item -LiteralPath $dumpFile).Length / 1MB, 2)
Write-Host "Dump size: $sizeMb MB" -ForegroundColor Gray

Write-Host "`nRestoring to RDS (clean existing objects in selected schemas)..." -ForegroundColor Yellow
$env:PGPASSWORD = $rdsConn.Password
$env:PGSSLMODE = $rdsConn.SslMode
& $pgRestore `
    -h $rdsConn.Host `
    -p $rdsConn.Port `
    -U $rdsConn.User `
    -d $rdsConn.Database `
    --no-owner `
    --no-acl `
    --clean `
    --if-exists `
    -v `
    $dumpFile
# pg_restore often returns 1 for benign warnings (e.g. missing extensions)
if ($LASTEXITCODE -gt 1) { throw "pg_restore failed (exit $LASTEXITCODE)" }

if (-not $SkipVerify) {
    Write-Host "`nVerify RDS schema table counts..." -ForegroundColor Yellow
    $rdsCounts = Invoke-PsqlCount -PgExeDir $pgDir -Conn $rdsConn -Sql $countSql
    Write-Host $rdsCounts -ForegroundColor Gray
    $martCheck = Invoke-PsqlCount -PgExeDir $pgDir -Conn $rdsConn -Sql "SELECT COUNT(*)::text FROM marts.mart_revenue_lost"
    Write-Host "marts.mart_revenue_lost rows: $martCheck" -ForegroundColor Gray
}

Remove-Item -LiteralPath $dumpFile -Force -ErrorAction SilentlyContinue

Write-Host "`nPublish complete." -ForegroundColor Green
Write-Host "Next: curl https://api-clinic.dbtdentalclinic.com/reports/dashboard/kpis -H `"X-API-Key: ...`"" -ForegroundColor Gray
