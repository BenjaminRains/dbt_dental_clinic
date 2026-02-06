# PowerShell script to test dbt setup on EC2 (dbt in PATH, setup script, dbt debug --target clinic).
# Runs checks via SSM and prints results; use after fix_ec2_dbt_setup.ps1 or manual setup.
#
# Usage:
#   .\scripts\test_ec2_dbt_setup.ps1
#   .\scripts\test_ec2_dbt_setup.ps1 -InstanceId i-xxxxx -ProjectRoot C:\path\to\dbt_dental_clinic
#
# Prerequisites:
#   - Run from project root or pass -ProjectRoot so deployment_credentials.json is found.
#   - AWS CLI configured with SSM SendCommand; EC2 has SSM agent and IAM for AWS-RunShellScript.
#
# When to run:
#   - After running fix_ec2_dbt_setup.ps1 to confirm dbt, PATH, and setup script work.
#   - When debugging "dbt not found" or dbt debug failures on the instance.

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`nTesting dbt setup on EC2" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        $InstanceId = $credentials.backend_api.ec2.instance_id
        Write-Host "Loaded instance ID: $InstanceId" -ForegroundColor Green
    } else {
        Write-Host "ERROR: deployment_credentials.json not found" -ForegroundColor Red
        exit 1
    }
}

# Commands to test setup
$commands = @(
    "echo '=== Testing dbt Setup ==='",
    "echo ''",
    "echo '1. Checking dbt installation...'",
    "which dbt || echo 'dbt not in PATH'",
    "/home/ec2-user/.local/bin/dbt --version 2>&1 | head -n 3 || echo 'dbt not found at /home/ec2-user/.local/bin/dbt'",
    "echo ''",
    "echo '2. Checking PATH...'",
    "echo `$PATH | grep -q '/home/ec2-user/.local/bin' && echo 'PATH contains dbt directory' || echo 'PATH does not contain dbt directory'",
    "echo ''",
    "echo '3. Testing setup script...'",
    "bash -c 'source /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh && echo `"Environment variables loaded`"'",
    "echo ''",
    "echo '4. Testing dbt with full path...'",
    "cd /opt/dbt_dental_clinic/dbt_dental_models",
    "bash -c 'source ~/.bashrc && source /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh && /home/ec2-user/.local/bin/dbt debug --target clinic 2>&1 | head -n 20'"
)

$parameters = @{
    commands = $commands
}
$commandJson = $parameters | ConvertTo-Json -Compress

Write-Host "Running test commands..." -ForegroundColor Yellow

try {
    $response = aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --parameters $commandJson `
        --output json | ConvertFrom-Json
    
    $commandId = $response.Command.CommandId
    
    # Wait for command to complete
    Start-Sleep -Seconds 5
    
    $output = aws ssm get-command-invocation `
        --command-id $commandId `
        --instance-id $InstanceId `
        --output json | ConvertFrom-Json
    
    if ($output.StandardOutputContent) {
        Write-Host $output.StandardOutputContent
    }
    
    if ($output.StandardErrorContent) {
        Write-Host "`nErrors/Warnings:" -ForegroundColor Yellow
        Write-Host $output.StandardErrorContent
    }
    
    if ($output.Status -eq "Success") {
        Write-Host "`nTest complete!" -ForegroundColor Green
    } else {
        Write-Host "`nTest completed with warnings" -ForegroundColor Yellow
        Write-Host "   Status: $($output.Status)" -ForegroundColor Gray
    }
    
} catch {
    Write-Host "`nError running test: $_" -ForegroundColor Red
    exit 1
}
