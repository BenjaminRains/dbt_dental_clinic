# Detach the broad SecretsManagerReadWrite policy from the demo API role (dental-clinic-api-role).
# Optionally add a narrow inline policy that allows GetSecretValue only on a specific secret.
#
# Usage:
#   .\scripts\detach_demo_role_secrets_manager.ps1
#       Detach only (use if demo API does not need any Secrets Manager secret).
#
#   .\scripts\detach_demo_role_secrets_manager.ps1 -AddNarrowPolicy -SecretName "my-demo-secret"
#       Add inline policy "dental-clinic-api-demo-secrets" allowing GetSecretValue on that secret, then detach.
#
# Requires: AWS CLI with iam:DetachRolePolicy, and iam:PutRolePolicy if -AddNarrowPolicy.

param(
    [string]$RoleName = "dental-clinic-api-role",
    [string]$Region = "us-east-1",
    [switch]$AddNarrowPolicy,
    [string]$SecretName = ""
)

$ErrorActionPreference = "Stop"

$PolicyArn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
$InlinePolicyName = "dental-clinic-api-demo-secrets"

# Resolve account ID from role
$roleJson = aws iam get-role --role-name $RoleName --query 'Role.Arn' --output text 2>$null
if (-not $roleJson -or $roleJson -match "error|Error") {
    Write-Host "Could not get role $RoleName. Is AWS CLI configured and do you have iam:GetRole?" -ForegroundColor Red
    exit 1
}
$accountId = ($roleJson -split ":")[4]
if (-not $accountId) {
    Write-Host "Could not parse account ID from role ARN: $roleJson" -ForegroundColor Red
    exit 1
}

if ($AddNarrowPolicy) {
    if ([string]::IsNullOrWhiteSpace($SecretName)) {
        Write-Host "When using -AddNarrowPolicy you must provide -SecretName (e.g. my-demo-secret)." -ForegroundColor Red
        exit 1
    }
    # ARN format: arn:aws:secretsmanager:region:account:secret:name-*
    $secretArn = "arn:aws:secretsmanager:${Region}:${accountId}:secret:${SecretName}-*"
    $policyDoc = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "$secretArn"
    }
  ]
}
"@
    Write-Host "Adding inline policy '$InlinePolicyName' for GetSecretValue on secret: $SecretName" -ForegroundColor Cyan
    $policyFile = [System.IO.Path]::GetTempFileName()
    try {
        $policyDoc | Set-Content -Path $policyFile -Encoding UTF8 -NoNewline
        $fileUrl = "file:///" + ($policyFile -replace '\\', '/')
        aws iam put-role-policy --role-name $RoleName --policy-name $InlinePolicyName --policy-document $fileUrl
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Failed to put inline policy." -ForegroundColor Red
            exit 1
        }
        Write-Host "  OK – Inline policy added." -ForegroundColor Green
    } finally {
        if (Test-Path $policyFile) { Remove-Item $policyFile -Force }
    }
}

Write-Host "Detaching $PolicyArn from role $RoleName..." -ForegroundColor Cyan
aws iam detach-role-policy --role-name $RoleName --policy-arn $PolicyArn
if ($LASTEXITCODE -ne 0) {
    Write-Host "Detach failed. Is the policy attached? Check IAM Console → Roles → $RoleName → Permissions." -ForegroundColor Red
    exit 1
}
Write-Host "  OK – SecretsManagerReadWrite detached." -ForegroundColor Green
Write-Host "`nVerify: .\scripts\verify_phase2_iam_roles.ps1" -ForegroundColor Gray
exit 0
