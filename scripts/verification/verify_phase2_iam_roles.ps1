# Verify Phase 2 IAM roles (demo vs clinic) are separate and clean.
# Demo role must NOT have clinic RDS secret (rds!db-...) or RDS permissions.
# Usage: .\scripts\verify_phase2_iam_roles.ps1
#        .\scripts\verify_phase2_iam_roles.ps1 -FailIfDemoHasClinicAccess
#
# Requires: AWS CLI with iam:GetRole, iam:ListAttachedRolePolicies, iam:ListRolePolicies, iam:GetRolePolicy, iam:ListInstanceProfilesForRole

param(
    [string]$DemoRoleName = "dental-clinic-api-demo-role",
    [string]$ClinicRoleName = "dental-clinic-api-clinic-role",
    [switch]$FailIfDemoHasClinicAccess
)

$ErrorActionPreference = "Stop"

# Clinic RDS credentials demo must NOT have access to (Step 6)
$ClinicSecretName = "rds!db-83a24c7f-7e85-4168-ba14-ad6e63905c49"

function Get-RoleDetails {
    param([string]$RoleName)
    $roleRaw = cmd /c "aws iam get-role --role-name $RoleName 2>&1"
    $roleRaw = ($roleRaw | Out-String).Trim()
    if ($roleRaw -match "AccessDenied|NoSuchEntity|not found") {
        return @{ Role = $null; Raw = $roleRaw }
    }
    try {
        $role = $roleRaw | ConvertFrom-Json
        return @{ Role = $role.Role; Raw = $roleRaw }
    } catch {
        return @{ Role = $null; Raw = $roleRaw }
    }
}

function Get-AttachedPolicies {
    param([string]$RoleName)
    $raw = cmd /c "aws iam list-attached-role-policies --role-name $RoleName 2>&1"
    $raw = ($raw | Out-String).Trim()
    if ($raw -notmatch '^\s*\{') {
        return @()
    }
    $out = $raw | ConvertFrom-Json
    $list = $out.AttachedPolicies
    return @(if ($list) { $list } else { @() })
}

function Get-InlinePoliciesWithDocuments {
    param([string]$RoleName)
    $raw = cmd /c "aws iam list-role-policies --role-name $RoleName 2>&1"
    $raw = ($raw | Out-String).Trim()
    if ($raw -notmatch '^\s*\{') {
        return @()
    }
    $names = $raw | ConvertFrom-Json
    $result = @()
    foreach ($n in $names.PolicyNames) {
        $policyRaw = cmd /c "aws iam get-role-policy --role-name $RoleName --policy-name $n 2>&1"
        $policyRaw = ($policyRaw | Out-String).Trim()
        if ($policyRaw -notmatch '^\s*\{') {
            continue
        }
        $doc = $policyRaw | ConvertFrom-Json
        $result += @{ PolicyName = $n; Document = $doc.PolicyDocument }
    }
    return $result
}

function Test-PolicyDocumentGrantsClinicOrRds {
    param($Document)
    $jsonStr = $Document | ConvertTo-Json -Depth 10 -Compress
    if ($jsonStr -match "dental-clinic/database") { return $true }
    if ($jsonStr -match 'rds!db-') { return $true }
    if ($jsonStr -match '"rds:[^"]*"') { return $true }
    return $false
}

function Get-InstanceProfilesForRole {
    param([string]$RoleName)
    $raw = cmd /c "aws iam list-instance-profiles-for-role --role-name $RoleName 2>&1"
    $raw = ($raw | Out-String).Trim()
    if ($raw -notmatch '^\s*\{') {
        return @()
    }
    $out = $raw | ConvertFrom-Json
    $list = $out.InstanceProfiles
    return @(if ($list) { $list } else { @() })
}

Write-Host "`nPhase 2 – IAM roles verification (demo vs clinic)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Gray
Write-Host "  Demo role:  $DemoRoleName" -ForegroundColor White
Write-Host "  Clinic role: $ClinicRoleName" -ForegroundColor White
Write-Host ""

$demoDetails = Get-RoleDetails -RoleName $DemoRoleName
$clinicDetails = Get-RoleDetails -RoleName $ClinicRoleName

if (-not $demoDetails.Role) {
    Write-Host "Demo role not found or access denied: $DemoRoleName" -ForegroundColor Red
    Write-Host $demoDetails.Raw -ForegroundColor Gray
    if ($demoDetails.Raw -match "AccessDenied|not authorized") {
        Write-Host "`nHint: Your credentials need IAM read on these roles. Attach a policy allowing: iam:GetRole, iam:ListAttachedRolePolicies, iam:ListRolePolicies, iam:GetRolePolicy, iam:ListInstanceProfilesForRole on arn:aws:iam::*:role/dental-clinic-api-demo-role and arn:aws:iam::*:role/dental-clinic-api-clinic-role. Or run this script with an identity that has IAM read access." -ForegroundColor Yellow
    }
    exit 1
}
if (-not $clinicDetails.Role) {
    Write-Host "Clinic role not found or access denied: $ClinicRoleName" -ForegroundColor Red
    Write-Host $clinicDetails.Raw -ForegroundColor Gray
    if ($clinicDetails.Raw -match "AccessDenied|not authorized") {
        Write-Host "`nHint: Your credentials need IAM read on these roles. Attach a policy allowing: iam:GetRole, iam:ListAttachedRolePolicies, iam:ListRolePolicies, iam:GetRolePolicy, iam:ListInstanceProfilesForRole on the two role ARNs above. Or run with an identity that has IAM read access." -ForegroundColor Yellow
    }
    exit 1
}

$demoAttached = Get-AttachedPolicies -RoleName $DemoRoleName
$demoInline = Get-InlinePoliciesWithDocuments -RoleName $DemoRoleName
$clinicAttached = Get-AttachedPolicies -RoleName $ClinicRoleName
$clinicInline = Get-InlinePoliciesWithDocuments -RoleName $ClinicRoleName
$demoProfiles = Get-InstanceProfilesForRole -RoleName $DemoRoleName
$clinicProfiles = Get-InstanceProfilesForRole -RoleName $ClinicRoleName

$demoHasClinicOrRds = $false
$demoViolation = ""
foreach ($inv in $demoInline) {
    if (Test-PolicyDocumentGrantsClinicOrRds -Document $inv.Document) {
        $demoHasClinicOrRds = $true
        $demoViolation = "Inline policy '$($inv.PolicyName)' grants clinic secret ($ClinicSecretName) or RDS access."
        break
    }
}
foreach ($p in $demoAttached) {
    if ($p.PolicyArn -match "rds|RDS" -or $p.PolicyName -match "rds|RDS") {
        $demoHasClinicOrRds = $true
        $demoViolation = "Attached policy '$($p.PolicyName)' appears to grant RDS access."
        break
    }
}
$demoHasBroadSecretsManager = $false
foreach ($p in $demoAttached) {
    if ($p.PolicyName -eq "SecretsManagerReadWrite") {
        $demoHasBroadSecretsManager = $true
        break
    }
}

Write-Host "Demo role: $DemoRoleName" -ForegroundColor Yellow
Write-Host "  ARN: $($demoDetails.Role.Arn)" -ForegroundColor Gray
if ($demoProfiles.Count -gt 0) {
    $profileNames = ($demoProfiles | ForEach-Object { $_.InstanceProfileName }) -join ', '
    Write-Host "  Instance profile(s): $profileNames" -ForegroundColor Gray
}
Write-Host "  Attached: $(($demoAttached | ForEach-Object { $_.PolicyName }) -join ', ')"
$demoInlineStr = if ($demoInline.Count -gt 0) { ($demoInline | ForEach-Object { $_.PolicyName }) -join ', ' } else { '(none)' }
Write-Host "  Inline:   $demoInlineStr"
if ($demoHasClinicOrRds) {
    Write-Host "  Status:   FAIL – $demoViolation" -ForegroundColor Red
} else {
    Write-Host "  Status:   OK – No clinic secret or RDS access" -ForegroundColor Green
}
if ($demoHasBroadSecretsManager) {
    Write-Host "  Note:    SecretsManagerReadWrite is broad; prefer an inline policy with GetSecretValue only on the demo secret(s)." -ForegroundColor Yellow
}
Write-Host ""

Write-Host "Clinic role: $ClinicRoleName" -ForegroundColor Yellow
Write-Host "  ARN: $($clinicDetails.Role.Arn)" -ForegroundColor Gray
if ($clinicProfiles.Count -gt 0) {
    $profileNames = ($clinicProfiles | ForEach-Object { $_.InstanceProfileName }) -join ', '
    Write-Host "  Instance profile(s): $profileNames" -ForegroundColor Gray
}
Write-Host "  Attached: $(($clinicAttached | ForEach-Object { $_.PolicyName }) -join ', ')"
$clinicInlineStr = if ($clinicInline.Count -gt 0) { ($clinicInline | ForEach-Object { $_.PolicyName }) -join ', ' } else { '(none)' }
Write-Host "  Inline:   $clinicInlineStr"
Write-Host "  Status:   OK – Separate role (clinic secrets/RDS allowed here)" -ForegroundColor Green
Write-Host ""

Write-Host "Summary" -ForegroundColor Cyan
Write-Host ("-" * 70) -ForegroundColor Gray
$rolesSeparate = $demoDetails.Role.Arn -ne $clinicDetails.Role.Arn
Write-Host "  Roles are different: $(if ($rolesSeparate) { 'Yes' } else { 'No (ERROR)' })" -ForegroundColor $(if ($rolesSeparate) { 'Green' } else { 'Red' })
Write-Host "  Demo has no clinic/RDS: $(if (-not $demoHasClinicOrRds) { 'Yes' } else { 'No – fix Step 6' })" -ForegroundColor $(if (-not $demoHasClinicOrRds) { 'Green' } else { 'Red' })
Write-Host ""

if ($demoHasClinicOrRds) {
    Write-Host "Remediation: In IAM, edit role '$DemoRoleName'. Remove access to secret '$ClinicSecretName' and any RDS permissions (Step 6.2)." -ForegroundColor Yellow
    if ($FailIfDemoHasClinicAccess) {
        exit 1
    }
}

Write-Host "Done. For full policy details run: .\scripts\verify_clinic_iam_role.ps1 -RoleName $DemoRoleName ; .\scripts\verify_clinic_iam_role.ps1 -RoleName $ClinicRoleName" -ForegroundColor Gray
exit 0
