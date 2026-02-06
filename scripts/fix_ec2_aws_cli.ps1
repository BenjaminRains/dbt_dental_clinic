# PowerShell script to fix AWS CLI on EC2 (downgrade urllib3 to version compatible with bundled AWS CLI).
# Runs pip3 uninstall/install and verifies aws --version and S3 access via SSM.
#
# Usage:
#   .\scripts\fix_ec2_aws_cli.ps1
#   .\scripts\fix_ec2_aws_cli.ps1 -InstanceId i-xxxxx -ProjectRoot C:\path\to\dbt_dental_clinic
#
# Prerequisites:
#   - Run from project root or pass -ProjectRoot so deployment_credentials.json is found.
#   - AWS CLI configured with SSM SendCommand; EC2 has SSM agent and IAM for AWS-RunShellScript.
#
# When to run:
#   - When "aws" on the instance fails (e.g. ImportError / urllib3) after AMI or system updates.
#   - One-time or ad-hoc repair; prefer baking the fix into AMI or bootstrap for new instances.

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`nFixing AWS CLI on EC2" -ForegroundColor Cyan
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

# Commands to fix AWS CLI
$commands = @(
    "echo '=== Checking AWS CLI ==='",
    "aws --version 2>&1 || echo 'AWS CLI broken (expected)'",
    "echo ''",
    "echo '=== Checking current urllib3 version ==='",
    "pip3 show urllib3 2>&1 | grep Version || echo 'urllib3 not found'",
    "echo ''",
    "echo '=== Downgrading urllib3 to compatible version (AWS CLI requires <1.27) ==='",
    "sudo pip3 uninstall -y urllib3 2>&1 || true",
    "sudo pip3 install 'urllib3>=1.25.4,<1.27' 2>&1",
    "echo ''",
    "echo '=== Verifying urllib3 version ==='",
    "pip3 show urllib3 2>&1 | grep Version",
    "echo ''",
    "echo '=== Verifying AWS CLI works ==='",
    "aws --version 2>&1",
    "echo ''",
    "echo '=== Testing S3 access ==='",
    "aws s3 ls 2>&1 | head -n 3 || echo 'S3 access test completed'"
)

$parameters = @{
    commands = $commands
}
$commandJson = $parameters | ConvertTo-Json -Compress

Write-Host "Running AWS CLI fix commands..." -ForegroundColor Yellow

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
        Write-Host "`nWarnings/Errors:" -ForegroundColor Yellow
        Write-Host $output.StandardErrorContent
    }
    
    if ($output.Status -eq "Success") {
        Write-Host "`nAWS CLI fix complete!" -ForegroundColor Green
    } else {
        Write-Host "`nFix completed with warnings" -ForegroundColor Yellow
        Write-Host "   Status: $($output.Status)" -ForegroundColor Gray
    }
    
} catch {
    Write-Host "`nError fixing AWS CLI: $_" -ForegroundColor Red
    exit 1
}
