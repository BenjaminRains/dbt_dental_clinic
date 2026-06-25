# Create a customer-managed IAM policy and attach to a user (no 2048-char inline limit).
# Use when Corbin22 already has many inline policies and the console rejects new ones.
#
# Requires admin: iam:CreatePolicy, iam:AttachUserPolicy (or attach existing policy only).
#
# Usage:
#   .\scripts\iam\attach_clinic_secrets_managed_policy.ps1 -UserName Corbin22
#   .\scripts\iam\attach_clinic_secrets_managed_policy.ps1 -UserName Corbin22 -SkipCreate

param(
    [string]$UserName = "Corbin22",
    [string]$PolicyName = "mdc-clinic-secrets-reconcile",
    [string]$PolicyFile = "",
    [switch]$SkipCreate
)

$ErrorActionPreference = "Stop"

function ConvertTo-AwsCliFileUri {
    param([Parameter(Mandatory = $true)][string]$Path)
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    return "file://" + ($resolved -replace '\\', '/')
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($PolicyFile)) {
    $PolicyFile = Join-Path $scriptDir "clinic_secrets_put_only_policy.json"
}

if (-not (Test-Path -LiteralPath $PolicyFile)) {
    Write-Host "Policy file not found: $PolicyFile" -ForegroundColor Red
    exit 1
}

$caller = aws sts get-caller-identity --output json 2>&1 | ConvertFrom-Json
if (-not $caller -or -not $caller.Arn) {
    Write-Host "Could not resolve caller identity." -ForegroundColor Red
    exit 1
}

if ($caller.Arn -match ":user/$([regex]::Escape($UserName))$") {
    Write-Host "Run as admin (not $UserName)." -ForegroundColor Red
    exit 1
}

$accountId = $caller.Account
$policyArn = "arn:aws:iam::${accountId}:policy/$PolicyName"

Write-Host "`nManaged policy attach: $PolicyName -> $UserName" -ForegroundColor Cyan

if (-not $SkipCreate) {
    $existing = aws iam get-policy --policy-arn $policyArn 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Policy already exists: $policyArn" -ForegroundColor DarkGray
    } else {
        $tempPolicy = Join-Path $env:TEMP "mdc-clinic-managed-$([Guid]::NewGuid().ToString('N')).json"
        try {
            Copy-Item -LiteralPath $PolicyFile -Destination $tempPolicy -Force
            Write-Host "Creating managed policy..." -ForegroundColor Cyan
            aws iam create-policy --policy-name $PolicyName --policy-document (ConvertTo-AwsCliFileUri -Path $tempPolicy)
            if ($LASTEXITCODE -ne 0) { exit 1 }
        } finally {
            Remove-Item -LiteralPath $tempPolicy -Force -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "Attaching to user $UserName..." -ForegroundColor Cyan
aws iam attach-user-policy --user-name $UserName --policy-arn $policyArn
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "OK — $policyArn attached to $UserName." -ForegroundColor Green
exit 0
