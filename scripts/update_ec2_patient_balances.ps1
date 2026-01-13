# PowerShell script to update patient balances on EC2 RDS demo database
# Usage: .\scripts\update_ec2_patient_balances.ps1

param(
    [string]$InstanceId = ""
)

$ErrorActionPreference = "Stop"

Write-Host "`n🔧 Updating Patient Balances on EC2 RDS Demo Database" -ForegroundColor Cyan
Write-Host ""

# Load database credentials
$credentialsPath = Join-Path (Split-Path $PSScriptRoot -Parent) "deployment_credentials.json"
if (-not (Test-Path $credentialsPath)) {
    Write-Host "❌ deployment_credentials.json not found" -ForegroundColor Red
    exit 1
}

$credentials = Get-Content $credentialsPath | ConvertFrom-Json
$demo = $credentials.demo_database

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $InstanceId = $credentials.backend_api.ec2.instance_id
    Write-Host "✅ Loaded instance ID from deployment_credentials.json: $InstanceId" -ForegroundColor Green
}

# Get demo database connection details
$demoHost = $null
if ($demo.database_connection -and $demo.database_connection.host) {
    $demoHost = $demo.database_connection.host
} elseif ($demo.postgresql -and $demo.postgresql.host) {
    $demoHost = $demo.postgresql.host
} elseif ($demo.ec2 -and $demo.ec2.private_ip) {
    $demoHost = $demo.ec2.private_ip
} else {
    $demoHost = "172.31.25.7"  # Fallback
}

$demoPort = "5432"
if ($demo.database_connection -and $demo.database_connection.port) {
    $demoPort = $demo.database_connection.port.ToString()
} elseif ($demo.postgresql -and $demo.postgresql.port) {
    $demoPort = $demo.postgresql.port.ToString()
}

Write-Host "📋 Database Connection Details:" -ForegroundColor Yellow
Write-Host "   Host: $demoHost" -ForegroundColor Gray
Write-Host "   Port: $demoPort" -ForegroundColor Gray
Write-Host "   Database: $($demo.postgresql.database)" -ForegroundColor Gray
Write-Host "   User: $($demo.postgresql.user)" -ForegroundColor Gray
Write-Host ""

# Read the SQL update script
$sqlScriptPath = Join-Path (Split-Path $PSScriptRoot -Parent) "docs\dbt\manual_update_patient_balances.sql"
if (-not (Test-Path $sqlScriptPath)) {
    Write-Host "❌ SQL script not found: $sqlScriptPath" -ForegroundColor Red
    exit 1
}

$sqlScript = Get-Content $sqlScriptPath -Raw

# Escape single quotes in password for bash
$escapedPassword = $demo.postgresql.password -replace "'", "'\''"

# Base64 encode the SQL script to avoid escaping issues
$sqlBytes = [System.Text.Encoding]::UTF8.GetBytes($sqlScript)
$sqlBase64 = [Convert]::ToBase64String($sqlBytes)

# Build psql command to execute the SQL script
# We'll write it to a temp file on EC2 and execute it
# Use sudo -u ec2-user to ensure proper permissions
$psqlCmd = @"
cd /tmp && echo '$sqlBase64' | base64 -d > /tmp/update_patient_balances.sql && sudo -u ec2-user bash -c "export PGPASSWORD='$escapedPassword' && psql -h $demoHost -p $demoPort -U $($demo.postgresql.user) -d $($demo.postgresql.database) -f /tmp/update_patient_balances.sql" && rm /tmp/update_patient_balances.sql
"@

$commands = @($psqlCmd)
$parameters = @{
    commands = $commands
}
$parametersJson = $parameters | ConvertTo-Json -Compress

Write-Host "📤 Executing patient balance update on EC2 RDS..." -ForegroundColor Yellow
Write-Host "   This may take a minute..." -ForegroundColor Gray
Write-Host ""

$response = aws ssm send-command `
    --instance-ids $InstanceId `
    --document-name "AWS-RunShellScript" `
    --parameters $parametersJson `
    --output json | ConvertFrom-Json

$commandId = $response.Command.CommandId
Write-Host "   Command ID: $commandId" -ForegroundColor Gray

# Wait for command to complete
$maxRetries = 60  # Allow more time for UPDATE operation
$retryCount = 0

while ($retryCount -lt $maxRetries) {
    Start-Sleep -Seconds 2
    $output = aws ssm get-command-invocation `
        --command-id $commandId `
        --instance-id $InstanceId `
        --output json | ConvertFrom-Json
    
    if ($output.Status -eq "Success" -or $output.Status -eq "Failed" -or $output.Status -eq "Cancelled") {
        break
    }
    $retryCount++
    Write-Host "." -NoNewline -ForegroundColor Gray
}

Write-Host "`n" # New line

if ($output.StandardOutputContent) {
    Write-Host "=== Update Results ===" -ForegroundColor Cyan
    Write-Host $output.StandardOutputContent
}

if ($output.StandardErrorContent) {
    Write-Host "=== Errors ===" -ForegroundColor Red
    Write-Host $output.StandardErrorContent
}

if ($output.Status -eq "Success" -and $output.ResponseCode -eq 0) {
    Write-Host "`n✅ Patient balances updated successfully on EC2 RDS!" -ForegroundColor Green
    Write-Host ""
    Write-Host "💡 Next steps:" -ForegroundColor Yellow
    Write-Host "   1. Run dbt models on EC2 to refresh mart_ar_summary" -ForegroundColor Gray
    Write-Host "   2. Restart the API service if needed" -ForegroundColor Gray
} else {
    Write-Host "`n❌ Update failed" -ForegroundColor Red
    Write-Host "   Status: $($output.Status)" -ForegroundColor Gray
    Write-Host "   Response Code: $($output.ResponseCode)" -ForegroundColor Gray
    exit 1
}
