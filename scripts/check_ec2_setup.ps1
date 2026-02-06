# PowerShell script to check EC2 setup and troubleshoot issues.
# Runs diagnostic commands on the clinic API EC2 instance via SSM (directory layout, .env, dbt, Python, jq).
#
# Usage:
#   .\scripts\check_ec2_setup.ps1
#   .\scripts\check_ec2_setup.ps1 -InstanceId i-xxxxx
#   .\scripts\check_ec2_setup.ps1 -ProjectRoot C:\path\to\dbt_dental_clinic
#
# Prerequisites:
#   - AWS CLI configured with credentials that can use SSM SendCommand on the instance.
#   - EC2 instance has SSM agent and IAM role with AmazonSSMManagedInstanceCore (or equivalent).
#
# When to run:
#   - After deploying code or env to EC2, to confirm /opt/dbt_dental_clinic layout, .env, and dbt.
#   - When troubleshooting "dbt not found" or missing files on the instance.

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`n🔍 Checking EC2 Setup" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        $InstanceId = $credentials.backend_api.ec2.instance_id
        Write-Host "✅ Loaded instance ID: $InstanceId" -ForegroundColor Green
    } else {
        Write-Host "❌ deployment_credentials.json not found" -ForegroundColor Red
        exit 1
    }
}

# Commands to check EC2 state
$checkCommands = @(
    "echo '=== Directory Structure ==='",
    "ls -la /opt/dbt_dental_clinic/ 2>/dev/null || echo 'Directory does not exist'",
    "echo ''",
    "echo '=== Scripts Directory ==='",
    "ls -la /opt/dbt_dental_clinic/scripts/ 2>/dev/null || echo 'Scripts directory does not exist'",
    "echo ''",
    "echo '=== Environment Files ==='",
    "ls -la /opt/dbt_dental_clinic/.env* 2>/dev/null || echo 'No .env files found'",
    "echo ''",
    "echo '=== deployment_credentials.json ==='",
    "ls -la /opt/dbt_dental_clinic/deployment_credentials.json 2>/dev/null || echo 'deployment_credentials.json not found'",
    "echo ''",
    "echo '=== dbt Installation ==='",
    "which dbt 2>/dev/null || echo 'dbt not in PATH'",
    "ls -la /home/ec2-user/.local/bin/dbt 2>/dev/null || echo 'dbt not at /home/ec2-user/.local/bin/dbt'",
    "python3 -m pip list | grep dbt 2>/dev/null || echo 'dbt not installed via pip'",
    "echo ''",
    "echo '=== Python/jq Availability ==='",
    "which python3 2>/dev/null || echo 'python3 not found'",
    "which jq 2>/dev/null || echo 'jq not found'",
    "echo ''",
    "echo '=== dbt_dental_models Directory ==='",
    "ls -la /opt/dbt_dental_clinic/dbt_dental_models/ 2>/dev/null || echo 'dbt_dental_models directory does not exist'",
    "echo ''",
    "echo '=== Pipfile ==='",
    "ls -la /opt/dbt_dental_clinic/dbt_dental_models/Pipfile 2>/dev/null || echo 'Pipfile not found'"
)

$parameters = @{
    commands = $checkCommands
}
$commandJson = $parameters | ConvertTo-Json -Compress

Write-Host "📤 Running diagnostic commands on EC2..." -ForegroundColor Yellow

try {
    $response = aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --parameters $commandJson `
        --output json | ConvertFrom-Json
    
    $commandId = $response.Command.CommandId
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    
    # Wait for command to complete
    Start-Sleep -Seconds 3
    
    $output = aws ssm get-command-invocation `
        --command-id $commandId `
        --instance-id $InstanceId `
        --output json | ConvertFrom-Json
    
    Write-Host "`n📄 Diagnostic Results:" -ForegroundColor Cyan
    Write-Host $output.StandardOutputContent
    
    if ($output.StandardErrorContent) {
        Write-Host "`n⚠️  Errors/Warnings:" -ForegroundColor Yellow
        Write-Host $output.StandardErrorContent
    }
    
    if ($output.Status -eq "Success") {
        Write-Host "`n✅ Diagnostic complete" -ForegroundColor Green
    }
    
} catch {
    Write-Host "`n❌ Error running diagnostic: $_" -ForegroundColor Red
    exit 1
}
