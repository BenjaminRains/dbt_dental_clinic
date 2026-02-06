# PowerShell script to fix dbt PATH on EC2 (add ~/.local/bin to PATH in ~/.bashrc).
# Runs via SSM so "dbt" is found after pip install --user; suggests source ~/.bashrc in session.
#
# Usage:
#   .\scripts\fix_ec2_dbt_path.ps1
#   .\scripts\fix_ec2_dbt_path.ps1 -InstanceId i-xxxxx -ProjectRoot C:\path\to\dbt_dental_clinic
#
# Prerequisites:
#   - Run from project root or pass -ProjectRoot so deployment_credentials.json is found.
#   - AWS CLI configured with SSM SendCommand; EC2 has SSM agent and IAM for AWS-RunShellScript.
#
# When to run:
#   - When "dbt" is not found on the instance (dbt installed via pip to ~/.local/bin but not on PATH).
#   - Prefer adding PATH to setup_ec2_dbt_env.sh or AMI so new instances don't need this.

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`nFixing dbt PATH on EC2" -ForegroundColor Cyan
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

# Commands to add dbt to PATH
$commands = @(
    "echo '=== Current PATH ==='",
    "echo `$PATH",
    "echo ''",
    "echo '=== Adding dbt to PATH ==='",
    "if ! echo `$PATH | grep -q '/home/ec2-user/.local/bin'; then",
    "  echo 'export PATH=`"`$HOME/.local/bin:`$PATH`"' >> ~/.bashrc",
    "  echo 'Added to ~/.bashrc'",
    "else",
    "  echo 'Already in PATH'",
    "fi",
    "echo ''",
    "echo '=== Verifying dbt ==='",
    "export PATH=`"`$HOME/.local/bin:`$PATH`"",
    "which dbt",
    "dbt --version 2>&1 | head -n 1"
)

$parameters = @{
    commands = $commands
}
$commandJson = $parameters | ConvertTo-Json -Compress

Write-Host "Running PATH fix commands..." -ForegroundColor Yellow

try {
    $response = aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --parameters $commandJson `
        --output json | ConvertFrom-Json
    
    $commandId = $response.Command.CommandId
    
    # Wait for command to complete
    Start-Sleep -Seconds 3
    
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
        Write-Host "`nPATH fix complete!" -ForegroundColor Green
        Write-Host "`nNote: Run 'source ~/.bashrc' in your EC2 session to apply changes immediately" -ForegroundColor Cyan
    } else {
        Write-Host "`nPATH fix failed" -ForegroundColor Red
        Write-Host "   Status: $($output.Status)" -ForegroundColor Red
        exit 1
    }
    
} catch {
    Write-Host "`nError fixing PATH: $_" -ForegroundColor Red
    exit 1
}
