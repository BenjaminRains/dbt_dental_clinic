# Post-deploy smoke test for clinic API on EC2 (via SSM, localhost on instance).
#
# Checks: systemd active, GET /health/db, GET /reports/dashboard/kpis (uses CLINIC_API_KEY from api/.env).
# Run after deploy_api_code_clinic.ps1, mdc deploy api --env clinic, or publish to RDS.
#
# Usage (from repo root):
#   pwsh -File scripts/verification/verify_clinic_api_smoke.ps1
#   pwsh -File scripts/verification/verify_clinic_api_smoke.ps1 -SkipDashboard
#
# Future: add -PublicUrl to curl https://api-clinic.dbtdentalclinic.com from your machine
# (requires allowlisted IP; on-instance checks below work without that).

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = "",
    [string]$ServiceName = "",
    [int]$ApiPort = 8000,
    [int]$MaxWaitSeconds = 60,
    [switch]$SkipDashboard
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}

$credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
if (-not (Test-Path -LiteralPath $credentialsFile)) {
    Write-Host "deployment_credentials.json not found: $credentialsFile" -ForegroundColor Red
    exit 1
}

$credentials = Get-Content -LiteralPath $credentialsFile -Raw | ConvertFrom-Json
$clinicEc2 = $credentials.backend_api.clinic_api.ec2
if (-not $clinicEc2) {
    Write-Host "backend_api.clinic_api.ec2 not found in deployment_credentials.json" -ForegroundColor Red
    exit 1
}

$idToUse = if ($InstanceId) { $InstanceId } else { [string]$clinicEc2.instance_id }
if (-not $idToUse) {
    Write-Host "No clinic EC2 instance ID (use -InstanceId or deployment_credentials.json)" -ForegroundColor Red
    exit 1
}

if (-not $ServiceName) {
    $fromCreds = [string]$clinicEc2.systemd_service
    if ($fromCreds -eq "dental-clinic-api") {
        $ServiceName = "dental-clinic-api-clinic"
    } elseif ($fromCreds) {
        $ServiceName = $fromCreds
    } else {
        $ServiceName = "dental-clinic-api-clinic"
    }
}

Write-Host "`nClinic API smoke test (SSM on $idToUse)" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray
Write-Host "  Service: $ServiceName" -ForegroundColor Gray
Write-Host "  Port:    $ApiPort" -ForegroundColor Gray

$waitAttempts = [Math]::Max(1, [int]($MaxWaitSeconds / 3))

$dashboardBlock = @(
    'CLINIC_API_KEY=$(grep ^CLINIC_API_KEY= /opt/dbt_dental_clinic/api/.env | cut -d= -f2- | tr -d "\r")'
    'if [ -z "$CLINIC_API_KEY" ]; then echo "FAIL: CLINIC_API_KEY missing in api/.env"; exit 1; fi'
    "HTTP=`$(curl -s -o /tmp/mdc_smoke_dash.json -w '%{http_code}' -H `"X-API-Key: `$CLINIC_API_KEY`" http://127.0.0.1:$ApiPort/reports/dashboard/kpis 2>/dev/null || echo 000)"
    'echo dashboard_kpis=$HTTP'
    'if [ "$HTTP" != "200" ]; then echo "FAIL: dashboard/kpis"; head -c 500 /tmp/mdc_smoke_dash.json 2>/dev/null; echo; exit 1; fi'
    'head -c 400 /tmp/mdc_smoke_dash.json 2>/dev/null; echo'
)

$lines = @(
    'fail() { echo "FAIL: $1"; exit 1; }'
    ('for i in $(seq 1 {0}); do systemctl is-active {1} >/dev/null 2>&1 && break; sleep 3; done' -f $waitAttempts, $ServiceName)
    ('systemctl is-active {0} >/dev/null 2>&1 || fail "service {0} not active"' -f $ServiceName)
    'echo service=active'
    "HTTP=`$(curl -s -o /tmp/mdc_smoke_hdb.json -w '%{http_code}' http://127.0.0.1:$ApiPort/health/db 2>/dev/null || echo 000)"
    'echo health_db=$HTTP'
    'if [ "$HTTP" != "200" ]; then echo "FAIL: health/db"; cat /tmp/mdc_smoke_hdb.json 2>/dev/null; exit 1; fi'
    'cat /tmp/mdc_smoke_hdb.json 2>/dev/null; echo'
)

if (-not $SkipDashboard) {
    $lines += $dashboardBlock
} else {
    $lines += 'echo dashboard_kpis=skipped'
}

$lines += 'echo PASS: clinic API smoke'

$remoteScript = ($lines -join "`n") -replace "`r`n", "`n"

$body = @{
    InstanceIds  = @($idToUse)
    DocumentName = "AWS-RunShellScript"
    Parameters   = @{ commands = @($remoteScript) }
}
$tmp = Join-Path $env:TEMP ("ssm-smoke-" + [Guid]::NewGuid().ToString() + ".json")
try {
    [System.IO.File]::WriteAllText($tmp, ($body | ConvertTo-Json -Depth 6 -Compress), [System.Text.UTF8Encoding]::new($false))
    $fileUrl = "file://" + ((Resolve-Path $tmp).Path -replace '\\', '/')
    $send = aws ssm send-command --cli-input-json $fileUrl --output json | ConvertFrom-Json
    $commandId = $send.Command.CommandId
    Write-Host "  SSM CommandId: $commandId" -ForegroundColor Gray

    $deadline = (Get-Date).AddSeconds($MaxWaitSeconds + 30)
    $invocation = $null
    do {
        Start-Sleep -Seconds 3
        $invocation = aws ssm get-command-invocation --command-id $commandId --instance-id $idToUse --output json | ConvertFrom-Json
    } while ($invocation.Status -eq "InProgress" -and (Get-Date) -lt $deadline)

    Write-Host "`n--- output ---" -ForegroundColor Cyan
    if ($invocation.StandardOutputContent) {
        $invocation.StandardOutputContent -split "`n" | ForEach-Object {
            if ($_.Trim()) { Write-Host "  $_" }
        }
    }
    if ($invocation.StandardErrorContent) {
        Write-Host "`n--- stderr ---" -ForegroundColor Yellow
        Write-Host $invocation.StandardErrorContent
    }

    if ($invocation.Status -ne "Success") {
        Write-Host "`nSmoke test failed (SSM status: $($invocation.Status))" -ForegroundColor Red
        exit 1
    }
    if ($invocation.StandardOutputContent -notmatch "PASS: clinic API smoke") {
        Write-Host "`nSmoke test failed (missing PASS marker)" -ForegroundColor Red
        exit 1
    }

    Write-Host "`nSmoke test passed." -ForegroundColor Green
    exit 0
} finally {
    Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
}
