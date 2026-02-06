<#
.SYNOPSIS
    Check details of WAF Web ACLs (especially CreatedByCloudFront-*) and which CloudFront distributions use them.

.DESCRIPTION
    Uses AWS CLI to get Web ACL details and associated CloudFront distributions.
    Web ACL list is read from deployment_credentials.json (frontend.waf, clinic_frontend.waf).
    Account ID and ARNs are built from the same file; no credentials are hardcoded.

    Requires: AWS CLI configured (aws wafv2, aws cloudfront). WAF for CloudFront is in us-east-1.

    IAM: Your AWS user/role needs WAF read (wafv2:GetWebACL) and CloudFront read (cloudfront:ListDistributionsByWebACLId, cloudfront:ListDistributions) in us-east-1.
    Note: ListResourcesForWebACL does not support CLOUDFRONT; we use cloudfront list-distributions-by-web-acl-id to find which distribution uses each Web ACL.

.EXAMPLE
    .\scripts\check_waf_web_acls.ps1
    .\scripts\check_waf_web_acls.ps1 -ProjectRoot C:\path\to\dbt_dental_clinic
#>

param(
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"
$Region = "us-east-1"
$Scope = "CLOUDFRONT"

# Resolve project root and load deployment_credentials.json
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}
$credentialsPath = Join-Path $ProjectRoot "deployment_credentials.json"
if (-not (Test-Path $credentialsPath)) {
    Write-Host "deployment_credentials.json not found at: $credentialsPath" -ForegroundColor Red
    Write-Host "Run from project root or pass -ProjectRoot." -ForegroundColor Yellow
    exit 1
}
$credentials = Get-Content $credentialsPath -Raw | ConvertFrom-Json
$AccountId = $credentials.aws_account.account_id
if ([string]::IsNullOrEmpty($AccountId)) {
    Write-Host "aws_account.account_id not found in deployment_credentials.json." -ForegroundColor Red
    exit 1
}

# Build Web ACL list from credentials (demo frontend + clinic frontend)
$WebAcls = @()
if ($credentials.frontend -and $credentials.frontend.waf -and $credentials.frontend.waf.web_acl_name -and $credentials.frontend.waf.web_acl_id) {
    $WebAcls += @{ Name = $credentials.frontend.waf.web_acl_name; Id = $credentials.frontend.waf.web_acl_id }
}
if ($credentials.clinic_frontend -and $credentials.clinic_frontend.waf -and $credentials.clinic_frontend.waf.web_acl_name -and $credentials.clinic_frontend.waf.web_acl_id) {
    $WebAcls += @{ Name = $credentials.clinic_frontend.waf.web_acl_name; Id = $credentials.clinic_frontend.waf.web_acl_id }
}
if ($WebAcls.Count -eq 0) {
    Write-Host "No WAF Web ACLs found in deployment_credentials.json (frontend.waf, clinic_frontend.waf)." -ForegroundColor Red
    exit 1
}

function Get-WebAclDetails {
    param ([string]$Name, [string]$Id, [string]$AccountId)
    $arn = "arn:aws:wafv2:us-east-1:$AccountId:global/webacl/$Name/$Id"
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host " Web ACL: $Name" -ForegroundColor Cyan
    Write-Host " ID: $Id" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    # Get Web ACL (capture raw output; AWS CLI may write errors to stderr)
    $aclRaw = aws wafv2 get-web-acl --scope $Scope --id $Id --name $Name --region $Region --output json 2>&1 | Out-String
    $aclRaw = $aclRaw.Trim()
    if (-not $aclRaw -or $aclRaw.StartsWith("{") -eq $false) {
        Write-Host "  Failed to get Web ACL. Output:" -ForegroundColor Red
        Write-Host "  $aclRaw" -ForegroundColor Red
        if ($aclRaw -match "AccessDeniedException") {
            Write-Host "  Tip: Add WAF read permissions (e.g. wafv2:GetWebACL, wafv2:ListResourcesForWebACL) or AWSWAFReadOnlyAccess in us-east-1." -ForegroundColor Yellow
        }
        return
    }
    $acl = $aclRaw | ConvertFrom-Json

    $lockToken = $acl.LockToken
    $summary = $acl.WebACL
    Write-Host "  Description: $($summary.Description)"
    $defaultAction = if ($summary.DefaultAction.PSObject.Properties['Allow']) { "Allow" } else { "Block" }
    Write-Host "  Default action: $defaultAction"
    Write-Host "  Capacity: $($summary.Capacity)"
    Write-Host "  Rules ($($summary.Rules.Count)):"
    foreach ($r in $summary.Rules) {
        $action = if ($r.Action) { $r.Action | ConvertTo-Json -Compress } else { "OverrideToGroup" }
        Write-Host "    - Priority $($r.Priority): $($r.Name)  [Action: $action]"
    }

    # Associated CloudFront distributions (ListResourcesForWebACL does not support CLOUDFRONT; use CloudFront API)
    Write-Host "  Associated resources (CloudFront distributions):"
    $distRaw = aws cloudfront list-distributions-by-web-acl-id --web-acl-id $arn --output json 2>&1 | Out-String
    $distRaw = $distRaw.Trim()
    if ($distRaw -and $distRaw.StartsWith("{")) {
        $distResp = $distRaw | ConvertFrom-Json
        $items = $distResp.DistributionList.Items
        if ($items -and $items.Count -gt 0) {
            foreach ($d in $items) {
                Write-Host "    - Distribution ID: $($d.Id)"
            }
        } else {
            Write-Host "    (none)"
        }
    } else {
        Write-Host "    (none or error)" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "WAF Web ACL details (scope: $Scope, region: $Region)" -ForegroundColor Green
Write-Host "Note: CreatedByCloudFront-* ACLs are auto-created by AWS when you enable WAF on a distribution from the CloudFront console." -ForegroundColor Gray
Write-Host ""
foreach ($w in $WebAcls) {
    Get-WebAclDetails -Name $w.Name -Id $w.Id -AccountId $AccountId
}

# List CloudFront distributions so we can map ID to domain/comment
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " CloudFront distributions (summary)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
try {
    $list = aws cloudfront list-distributions --query "DistributionList.Items[*].[Id,Comment,DomainName,Aliases.Items[0]]" --output text --region $Region 2>&1
    if ($list) {
        $list | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Host "  (none or no permission)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  Could not list distributions: $_" -ForegroundColor Yellow
}

$clinicDistId = $null
if ($credentials.clinic_frontend -and $credentials.clinic_frontend.cloudfront) {
    $clinicDistId = $credentials.clinic_frontend.cloudfront.distribution_id
}
if ($clinicDistId -and $clinicDistId -notmatch '^<') {
    Write-Host ""
    Write-Host "Done. Clinic frontend distribution ID (from credentials): $clinicDistId" -ForegroundColor Gray
} else {
    Write-Host ""
    Write-Host "Done." -ForegroundColor Gray
}
