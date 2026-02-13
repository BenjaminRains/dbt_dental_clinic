# Deploy codebase to clinic EC2 by running git clone on the instance via SSM
# Usage: .\scripts\deploy_codebase_to_clinic_ec2.ps1
#        .\scripts\deploy_codebase_to_clinic_ec2.ps1 -RepoUrl "https://github.com/YourOrg/dbt_dental_clinic.git"
#
# Requires: Clinic instance running with SSM agent; instance ID in deployment_credentials.json
#           backend_api.clinic_api.ec2.instance_id. Repo must be reachable from the instance (public or token in URL).
#           Avoid passing tokens in -RepoUrl on the command line; use a deploy key or private repo access on the instance.

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = "",
    [string]$RepoUrl = "https://github.com/YourOrg/dbt_dental_clinic.git",
    [string]$Branch = "main",
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

$credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
if (-not (Test-Path $credentialsFile)) {
    Write-Host "deployment_credentials.json not found: $credentialsFile" -ForegroundColor Red
    exit 1
}

$credentials = Get-Content $credentialsFile -Raw | ConvertFrom-Json
if (-not $InstanceId) {
    $InstanceId = $credentials.backend_api.clinic_api.ec2.instance_id
}
if (-not $InstanceId) {
    Write-Host "Clinic instance ID not found. Set backend_api.clinic_api.ec2.instance_id or pass -InstanceId" -ForegroundColor Red
    exit 1
}

Write-Host "`nDeploying codebase to clinic EC2 (clone on instance)" -ForegroundColor Cyan
Write-Host "Instance: $InstanceId" -ForegroundColor Gray
Write-Host "Repo: $RepoUrl" -ForegroundColor Gray
Write-Host "Branch: $Branch" -ForegroundColor Gray
Write-Host ("=" * 60) -ForegroundColor Gray

$commands = @(
    "set -e",
    "echo '=== Installing git if needed ==='",
    "command -v git >/dev/null 2>&1 || (sudo dnf install -y git 2>/dev/null || sudo yum install -y git)",
    "echo '=== Clone or update /opt/dbt_dental_clinic ==='",
    "if [ -d /opt/dbt_dental_clinic/.git ]; then",
    "  echo 'Existing repo found; pulling latest...'",
    "  sudo git -C /opt/dbt_dental_clinic fetch origin",
    "  sudo git -C /opt/dbt_dental_clinic checkout -B $Branch origin/$Branch 2>/dev/null || sudo git -C /opt/dbt_dental_clinic checkout $Branch",
    "  sudo git -C /opt/dbt_dental_clinic pull --rebase 2>/dev/null || true",
    "else",
    "  sudo rm -rf /opt/dbt_dental_clinic",
    "  sudo git clone -b $Branch '$RepoUrl' /opt/dbt_dental_clinic",
    "fi",
    "echo '=== Set ownership ==='",
    "sudo chown -R ec2-user:ec2-user /opt/dbt_dental_clinic",
    "sudo mkdir -p /opt/dbt_dental_clinic/dbt_dental_models/logs",
    "sudo chown ec2-user:ec2-user /opt/dbt_dental_clinic/dbt_dental_models/logs",
    "sudo chmod 755 /opt/dbt_dental_clinic/dbt_dental_models/logs",
    "echo '=== Done ==='",
    "ls -la /opt/dbt_dental_clinic | head -20"
)

$parameters = @{ commands = $commands }
$parametersJson = $parameters | ConvertTo-Json -Depth 3 -Compress

$raw = aws ssm send-command --instance-ids $InstanceId --document-name "AWS-RunShellScript" --parameters $parametersJson --region $Region --output json 2>&1 | Out-String
$raw = ($raw | Out-String).Trim()

if ($raw -match "AccessDenied|InvalidInstanceId|not authorized") {
    Write-Host "SSM send-command failed:" -ForegroundColor Red
    Write-Host $raw
    exit 1
}

try {
    $result = $raw | ConvertFrom-Json
} catch {
    Write-Host "Failed to parse SSM response." -ForegroundColor Red
    Write-Host $raw
    exit 1
}

$cmdId = $result.Command.CommandId
Write-Host "Command sent. CommandId: $cmdId" -ForegroundColor Green
Write-Host "Waiting 15s for command to run..." -ForegroundColor Gray
Start-Sleep -Seconds 15

$outRaw = aws ssm get-command-invocation --command-id $cmdId --instance-id $InstanceId --region $Region --output json 2>&1 | Out-String
$outRaw = $outRaw.Trim()
try {
    $inv = $outRaw | ConvertFrom-Json
    Write-Host "`n--- Standard output ---" -ForegroundColor Cyan
    Write-Host $inv.StandardOutputContent
    if ($inv.StandardErrorContent) {
        Write-Host "`n--- Standard error ---" -ForegroundColor Yellow
        Write-Host $inv.StandardErrorContent
    }
    Write-Host "`nStatus: $($inv.Status)" -ForegroundColor $(if ($inv.Status -eq 'Success') { 'Green' } else { 'Red' })
    if ($inv.Status -ne 'Success') { exit 1 }
} catch {
    Write-Host "Could not get command output. Check AWS Console > SSM > Run Command > $cmdId" -ForegroundColor Yellow
}

Write-Host "`nNext: SSM into the instance and finish config (api/.env, dbt, systemd). See CLINIC_DEPLOYMENT_PHASE2_ACTION_PLAN.md Step 3.1" -ForegroundColor Cyan
