# Create (or repair) account CloudTrail for audit — including Secrets Manager GetSecretValue.
#
# Requires admin credentials (NOT a normal developer user):
#   cloudtrail:*, s3:CreateBucket, s3:PutBucketPolicy, s3:PutPublicAccessBlock
#
# Usage (run as admin / root):
#   .\scripts\aws\setup_cloudtrail.ps1
#   .\scripts\aws\setup_cloudtrail.ps1 -WhatIf
#
# After setup, developers can use (with cloudtrail:LookupEvents):
#   .\scripts\aws\lookup_secrets_manager_access.ps1 -SecretName dental-clinic/database

param(
    [string]$TrailName = "dental-clinic-trail",
    [string]$Region = "us-east-1",
    [string]$BucketName = "",
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

function ConvertTo-AwsCliFileUri {
    param([Parameter(Mandatory = $true)][string]$Path)
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    return "file://" + ($resolved -replace '\\', '/')
}

$caller = aws sts get-caller-identity --output json 2>&1 | ConvertFrom-Json
if (-not $caller -or -not $caller.Account) {
    Write-Host "Could not resolve AWS caller (aws sts get-caller-identity)." -ForegroundColor Red
    exit 1
}

$accountId = $caller.Account
if ([string]::IsNullOrWhiteSpace($BucketName)) {
    $BucketName = "dental-clinic-cloudtrail-$accountId"
}

Write-Host "`nCloudTrail setup" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray
Write-Host "  Account:  $accountId"
Write-Host "  Region:   $Region"
Write-Host "  Trail:    $TrailName"
Write-Host "  S3 bucket: $BucketName"
Write-Host "  Caller:   $($caller.Arn)"

if ($caller.Arn -match ":user/Corbin22$") {
    Write-Host "`nThis script needs admin CloudTrail + S3 permissions." -ForegroundColor Red
    Write-Host "Sign in as root or an IAM admin, then rerun." -ForegroundColor Yellow
    Write-Host "Console walkthrough: docs/deployment/CLOUDTRAIL_SETUP.md" -ForegroundColor Yellow
    exit 1
}

if ($WhatIf) {
    Write-Host "`nWhatIf: would create bucket, trail, enable logging, and log Read+Write management events." -ForegroundColor Yellow
    exit 0
}

Write-Host "`n[1/5] S3 bucket..." -ForegroundColor Cyan
$bucketExists = $false
aws s3api head-bucket --bucket $BucketName --region $Region 2>$null
if ($LASTEXITCODE -eq 0) {
    $bucketExists = $true
    Write-Host "  Bucket already exists." -ForegroundColor DarkGray
} else {
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $BucketName --region $Region
    } else {
        aws s3api create-bucket --bucket $BucketName --region $Region `
            --create-bucket-configuration LocationConstraint=$Region
    }
    if ($LASTEXITCODE -ne 0) { exit 1 }
    Write-Host "  Created bucket." -ForegroundColor Green
}

Write-Host "`n[2/5] Block public access..." -ForegroundColor Cyan
aws s3api put-public-access-block --bucket $BucketName --public-access-block-configuration `
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "`n[3/5] Bucket policy for CloudTrail..." -ForegroundColor Cyan
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$policyTemplate = Join-Path $scriptDir "cloudtrail_bucket_policy.template.json"
$policyText = (Get-Content -LiteralPath $policyTemplate -Raw -Encoding UTF8) `
    -replace "__BUCKET_NAME__", $BucketName `
    -replace "__ACCOUNT_ID__", $accountId
$policyFile = Join-Path $env:TEMP "cloudtrail-bucket-policy-$([Guid]::NewGuid().ToString('N')).json"
try {
    $policyText | Set-Content -LiteralPath $policyFile -Encoding UTF8 -NoNewline
    aws s3api put-bucket-policy --bucket $BucketName --policy (ConvertTo-AwsCliFileUri -Path $policyFile)
    if ($LASTEXITCODE -ne 0) { exit 1 }
} finally {
    Remove-Item -LiteralPath $policyFile -Force -ErrorAction SilentlyContinue
}

Write-Host "`n[4/5] CloudTrail trail..." -ForegroundColor Cyan
$trailExists = $false
$describe = aws cloudtrail describe-trails --trail-name-list $TrailName --region $Region --output json 2>$null | ConvertFrom-Json
if ($describe -and $describe.trailList -and $describe.trailList.Count -gt 0) {
    $trailExists = $true
    Write-Host "  Trail already exists." -ForegroundColor DarkGray
} else {
    aws cloudtrail create-trail `
        --name $TrailName `
        --s3-bucket-name $BucketName `
        --is-multi-region-trail `
        --enable-log-file-validation `
        --include-global-service-events `
        --region $Region
    if ($LASTEXITCODE -ne 0) { exit 1 }
    Write-Host "  Created trail." -ForegroundColor Green
}

Write-Host "`n[5/5] Enable logging + Read/Write management events..." -ForegroundColor Cyan
aws cloudtrail start-logging --name $TrailName --region $Region
if ($LASTEXITCODE -ne 0) { exit 1 }

aws cloudtrail put-event-selectors --trail-name $TrailName --region $Region `
    --event-selectors '[{"ReadWriteType":"All","IncludeManagementEvents":true}]'
if ($LASTEXITCODE -ne 0) { exit 1 }

$status = aws cloudtrail get-trail-status --name $TrailName --region $Region --output json | ConvertFrom-Json
Write-Host "`nOK — CloudTrail is logging." -ForegroundColor Green
Write-Host "  IsLogging: $($status.IsLogging)"
Write-Host "`nNext:" -ForegroundColor Gray
Write-Host "  Wait 5–15 minutes, then:" -ForegroundColor Gray
Write-Host "  .\scripts\aws\lookup_secrets_manager_access.ps1" -ForegroundColor Gray
Write-Host "  Grant developers read-only lookup: scripts/iam/cloudtrail_lookup_readonly_policy.json" -ForegroundColor Gray
exit 0
