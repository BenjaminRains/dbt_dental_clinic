# Attach inline IAM policy so a developer user can reconcile legacy dental-clinic/database
# (DEPRECATED — secret removed Jun 2025; use RDS master secret only).
# Kept for reference if an old account still has the duplicate secret.
#
# Current workflow needs GetSecretValue on rds!db-... only — see CLINIC_ANALYTICS_WORKFLOW.md.
#
# Usage:
#   .\scripts\iam\attach_clinic_secrets_user_policy.ps1
#   .\scripts\iam\attach_clinic_secrets_user_policy.ps1 -UserName Corbin22
#
# Verify (as the developer user):
#   mdc secrets pull clinic

param(
    [string]$UserName = "Corbin22",
    [string]$PolicyName = "mdc-clinic-secrets-reconcile",
    [string]$PolicyFile = "",
    [ValidateSet("put-only", "full")]
    [string]$PolicyScope = "put-only"
)

$ErrorActionPreference = "Stop"

function ConvertTo-AwsCliFileUri {
    param([Parameter(Mandatory = $true)][string]$Path)
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $forward = $resolved -replace '\\', '/'
    # AWS CLI on Windows expects file://C:/path (two slashes), not file:///C:/path
    return "file://$forward"
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($PolicyFile)) {
    $PolicyFile = if ($PolicyScope -eq "full") {
        Join-Path $scriptDir "clinic_secrets_developer_policy.json"
    } else {
        Join-Path $scriptDir "clinic_secrets_put_only_policy.json"
    }
}

if (-not (Test-Path -LiteralPath $PolicyFile)) {
    Write-Host "Policy file not found: $PolicyFile" -ForegroundColor Red
    exit 1
}

Write-Host "`nAttach clinic Secrets Manager policy to IAM user" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray
Write-Host "  User:         $UserName"
Write-Host "  Policy name:  $PolicyName"
Write-Host "  Policy file:  $PolicyFile"

$caller = aws sts get-caller-identity --output json 2>&1 | ConvertFrom-Json
if (-not $caller -or -not $caller.Arn) {
    Write-Host "Could not resolve caller identity (aws sts get-caller-identity)." -ForegroundColor Red
    exit 1
}
Write-Host "  Caller:       $($caller.Arn)" -ForegroundColor DarkGray

if ($caller.Arn -match ":user/$([regex]::Escape($UserName))$") {
    Write-Host "`nCannot attach: you are signed in as the target user ($UserName)." -ForegroundColor Red
    Write-Host "IAM users cannot add policies to themselves." -ForegroundColor Yellow
    Write-Host "Re-run with admin credentials (root account, IAM admin user, or admin role)." -ForegroundColor Yellow
    Write-Host "`nConsole fallback (inline, ~180 chars — use if near 2048 limit):" -ForegroundColor Yellow
    Write-Host "  IAM → Users → $UserName → Permissions → Add inline policy → JSON" -ForegroundColor Yellow
    Write-Host "  Paste: $(Join-Path $scriptDir 'clinic_secrets_put_only_policy.json')" -ForegroundColor Yellow
    Write-Host "If inline quota is full, use managed policy instead:" -ForegroundColor Yellow
    Write-Host "  .\scripts\iam\attach_clinic_secrets_managed_policy.ps1 -UserName $UserName" -ForegroundColor Yellow
    exit 1
}

try {
    Get-Content -LiteralPath $PolicyFile -Raw -Encoding UTF8 | ConvertFrom-Json | Out-Null
} catch {
    Write-Host "Policy file is not valid JSON: $PolicyFile" -ForegroundColor Red
    exit 1
}

Write-Host "`nApplying inline policy..." -ForegroundColor Cyan
$tempPolicy = Join-Path $env:TEMP "mdc-clinic-secrets-policy-$([Guid]::NewGuid().ToString('N')).json"
try {
    Copy-Item -LiteralPath $PolicyFile -Destination $tempPolicy -Force
    $fileUri = ConvertTo-AwsCliFileUri -Path $tempPolicy
    aws iam put-user-policy --user-name $UserName --policy-name $PolicyName --policy-document $fileUri
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed. Caller needs iam:PutUserPolicy on user $UserName." -ForegroundColor Red
        Write-Host "Console: IAM → Users → $UserName → Permissions → Add inline policy → JSON → paste $PolicyFile" -ForegroundColor Yellow
        exit 1
    }
} finally {
    if (Test-Path -LiteralPath $tempPolicy) {
        Remove-Item -LiteralPath $tempPolicy -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "OK — inline policy '$PolicyName' attached to $UserName." -ForegroundColor Green
Write-Host "`nNext (as $UserName):" -ForegroundColor Gray
Write-Host "  pip install -e tools/mdc_cli" -ForegroundColor Gray
Write-Host "  mdc secrets pull clinic" -ForegroundColor Gray
Write-Host "  mdc deploy api --env clinic" -ForegroundColor Gray
exit 0
