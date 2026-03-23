# Verify clinic API IAM role (dental-clinic-api-clinic-role) and output details for deployment_credentials.json
# Usage: .\scripts\verify_clinic_iam_role.ps1
# Requires: AWS CLI configured with credentials that can call IAM GetRole, ListAttachedRolePolicies, ListRolePolicies, GetRolePolicy, ListInstanceProfilesForRole

param(
    [string]$RoleName = "dental-clinic-api-clinic-role",
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"

Write-Host "`nVerifying clinic IAM role: $RoleName" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray

# Get role (run via cmd so PowerShell doesn't throw on AWS CLI stderr)
$roleRaw = cmd /c "aws iam get-role --role-name $RoleName 2>&1"
$roleRaw = ($roleRaw | Out-String).Trim()

$isAccessDenied = $roleRaw -match "AccessDenied|not authorized"
$hint = if ($isAccessDenied) {
    "Use credentials that have IAM read (e.g. iam:GetRole, iam:ListAttachedRolePolicies, iam:ListRolePolicies, iam:ListInstanceProfilesForRole), or run from an account/role with IAM access."
} else {
    "Create the role first (Phase 2 Step 2)."
}

try {
    $role = $roleRaw | ConvertFrom-Json
} catch {
    Write-Host "AWS get-role failed or returned non-JSON. Output:" -ForegroundColor Red
    Write-Host $roleRaw
    Write-Host "`n$hint" -ForegroundColor Yellow
    exit 1
}

if (-not $role.Role) {
    Write-Host "Role not found in response (no .Role in JSON). Output:" -ForegroundColor Red
    Write-Host $roleRaw
    Write-Host "`n$hint" -ForegroundColor Yellow
    exit 1
}

$r = $role.Role
Write-Host "`nRole:" -ForegroundColor Green
Write-Host "  RoleName:   $($r.RoleName)"
Write-Host "  RoleId:     $($r.RoleId)"
Write-Host "  Arn:        $($r.Arn)"
Write-Host "  CreateDate: $($r.CreateDate)"

# Trust policy
Write-Host "`nTrust policy (trusted entity):" -ForegroundColor Green
$trustDoc = $r.AssumeRolePolicyDocument | ConvertTo-Json -Depth 5 -Compress
Write-Host "  $trustDoc"

# Attached managed policies
$attached = aws iam list-attached-role-policies --role-name $RoleName 2>&1 | ConvertFrom-Json
Write-Host "`nAttached managed policies:" -ForegroundColor Green
if ($attached.AttachedPolicies.Count -eq 0) {
    Write-Host "  (none)"
} else {
    foreach ($p in $attached.AttachedPolicies) {
        Write-Host "  - $($p.PolicyName)  ARN: $($p.PolicyArn)"
    }
}

# Inline policies
$inlineNames = aws iam list-role-policies --role-name $RoleName 2>&1 | ConvertFrom-Json
Write-Host "`nInline policies:" -ForegroundColor Green
$inlineDetails = @()
if ($inlineNames.PolicyNames.Count -eq 0) {
    Write-Host "  (none)"
} else {
    foreach ($name in $inlineNames.PolicyNames) {
        $doc = aws iam get-role-policy --role-name $RoleName --policy-name $name 2>&1 | ConvertFrom-Json
        Write-Host "  - $name"
        $inlineDetails += [PSCustomObject]@{ PolicyName = $name; Document = $doc.PolicyDocument }
    }
}

# Instance profiles for this role
$profiles = aws iam list-instance-profiles-for-role --role-name $RoleName 2>&1 | ConvertFrom-Json
Write-Host "`nInstance profiles for role:" -ForegroundColor Green
if (-not $profiles.InstanceProfiles -or $profiles.InstanceProfiles.Count -eq 0) {
    Write-Host "  (none - AWS creates one when you attach role to EC2, or create instance profile with same name)"
} else {
    foreach ($ip in $profiles.InstanceProfiles) {
        Write-Host "  - $($ip.InstanceProfileName)  ARN: $($ip.Arn)"
    }
}

# Build JSON block for deployment_credentials.json
$accountId = ($r.Arn -split ":")[4]
$instanceProfileName = $RoleName   # AWS often uses same name for instance profile when created with role
$instanceProfileArn = "arn:aws:iam::${accountId}:instance-profile/${instanceProfileName}"

$jsonBlock = @{
    role_name         = $r.RoleName
    role_arn          = $r.Arn
    role_id           = $r.RoleId
    create_date       = $r.CreateDate
    instance_profile  = @{
        profile_name = $instanceProfileName
        profile_arn  = $instanceProfileArn
        note         = "Use this as IAM instance profile when launching clinic EC2 (Phase 2 Step 3). AWS may create profile with same name as role when first attached to an instance."
    }
    attached_policies = @($attached.AttachedPolicies | ForEach-Object { @{ name = $_.PolicyName; arn = $_.PolicyArn } })
    inline_policies   = @($inlineNames.PolicyNames | ForEach-Object { $_ })
    verified_at       = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
}

Write-Host "`n" -NoNewline
Write-Host "JSON for deployment_credentials.json (backend_api.clinic_api.iam_role or similar):" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray
$jsonBlock | ConvertTo-Json -Depth 5

Write-Host "`nVerification complete." -ForegroundColor Green
