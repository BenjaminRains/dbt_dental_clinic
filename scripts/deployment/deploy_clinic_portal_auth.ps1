<#
.SYNOPSIS
    Deploy clinic portal login (API routes + users JSON) to clinic EC2.

.DESCRIPTION
    mdc deploy api --env clinic only copies api/.env. This script deploys the
    /auth/login code and api/clinic-portal-users.json, then restarts
    dental-clinic-api-clinic.

.EXAMPLE
    .\scripts\deployment\deploy_clinic_portal_auth.ps1
#>

param(
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}

$deployScript = Join-Path $ProjectRoot "scripts\deployment\deploy_api_file.ps1"
if (-not (Test-Path -LiteralPath $deployScript)) {
    Write-Host "deploy_api_file.ps1 not found: $deployScript" -ForegroundColor Red
    exit 1
}

# Sole live credentials path. A filled copy under docs/ is obsolete and easy to edit by mistake.
$apiUsers = Join-Path $ProjectRoot "api\clinic-portal-users.json"
$staleDocsUsers = Join-Path $ProjectRoot "docs\deployment-connections\clinic-portal-users.json"
if (Test-Path -LiteralPath $staleDocsUsers) {
    if (-not (Test-Path -LiteralPath $apiUsers)) {
        Write-Host "Found obsolete docs\deployment-connections\clinic-portal-users.json but api\clinic-portal-users.json is missing." -ForegroundColor Red
        Write-Host "Copy the API file from the template (or from that docs path), then delete the docs copy." -ForegroundColor Yellow
        exit 1
    }
    $apiHash = (Get-FileHash -LiteralPath $apiUsers -Algorithm SHA256).Hash
    $docsHash = (Get-FileHash -LiteralPath $staleDocsUsers -Algorithm SHA256).Hash
    if ($apiHash -ne $docsHash) {
        Write-Host "Stale portal users file differs from the API source of truth:" -ForegroundColor Red
        Write-Host "  Deploy uses: $apiUsers" -ForegroundColor Yellow
        Write-Host "  Obsolete:    $staleDocsUsers" -ForegroundColor Yellow
        Write-Host "Merge any intended edits into api\clinic-portal-users.json, delete the docs copy, then retry." -ForegroundColor Yellow
        exit 1
    }
    Write-Host "Removing obsolete duplicate: docs\deployment-connections\clinic-portal-users.json" -ForegroundColor Yellow
    Remove-Item -LiteralPath $staleDocsUsers -Force
}

$files = @(
    "api\main.py",
    "api\auth\portal.py",
    "api\routers\portal_auth.py",
    "api\clinic-portal-users.json"
)

Write-Host "`nDeploy clinic portal auth to EC2" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray

for ($i = 0; $i -lt $files.Count; $i++) {
    $rel = $files[$i]
    $local = Join-Path $ProjectRoot $rel
    if (-not (Test-Path -LiteralPath $local)) {
        Write-Host "Missing: $local" -ForegroundColor Red
        if ($rel -eq "api\clinic-portal-users.json") {
            Write-Host "Bootstrap: Copy-Item docs\deployment-connections\clinic-portal-users.template.json api\clinic-portal-users.json" -ForegroundColor Yellow
        }
        exit 1
    }
    Write-Host "`n>> $rel" -ForegroundColor Yellow
    & $deployScript -FilePath $rel -Clinic
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Write-Host "`nVerifying POST /auth/login on instance (localhost)..." -ForegroundColor Cyan
$verify = Join-Path $ProjectRoot "scripts\verification\verify_clinic_portal_auth.ps1"
if (Test-Path -LiteralPath $verify) {
    & $verify -ProjectRoot $ProjectRoot
    exit $LASTEXITCODE
}

Write-Host "Done. Test login at https://clinic.dbtdentalclinic.com/login" -ForegroundColor Green
