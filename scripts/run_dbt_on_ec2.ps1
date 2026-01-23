# PowerShell script to run dbt commands on EC2 using AWS Systems Manager
# Usage: 
#   .\scripts\run_dbt_on_ec2.ps1 run --select fact_claim
#   .\scripts\run_dbt_on_ec2.ps1 test --select fact_claim
#   .\scripts\run_dbt_on_ec2.ps1 run --full-refresh
#   .\scripts\run_dbt_on_ec2.ps1 test

param(
    [Parameter(Mandatory=$false, Position=0, ValueFromRemainingArguments=$true)]
    [string[]]$DbtArgs,  # dbt command and arguments (e.g., "run", "--select", "fact_claim")
    
    [string]$InstanceId = "",
    [string]$DbtPath = "/opt/dbt_dental_clinic/dbt_dental_models",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`n🚀 Running dbt on EC2" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        $InstanceId = $credentials.backend_api.ec2.instance_id
        Write-Host "✅ Loaded instance ID from deployment_credentials.json: $InstanceId" -ForegroundColor Green
    } else {
        Write-Host "❌ deployment_credentials.json not found and InstanceId not provided" -ForegroundColor Red
        Write-Host "   Please provide -InstanceId parameter or ensure deployment_credentials.json exists" -ForegroundColor Yellow
        exit 1
    }
}

# Default to "run" if no arguments provided
if (-not $DbtArgs -or $DbtArgs.Count -eq 0) {
    $DbtArgs = @("run")
    Write-Host "⚠️  No dbt command specified, defaulting to 'dbt run'" -ForegroundColor Yellow
}

# Build the dbt command
$dbtCommand = $DbtArgs -join " "
Write-Host "`n📋 dbt command: dbt $dbtCommand" -ForegroundColor Cyan
Write-Host "   Path: $DbtPath" -ForegroundColor Gray
Write-Host "   Instance: $InstanceId" -ForegroundColor Gray
Write-Host ""

try {
    # Escape the command for bash
    # Replace single quotes with '\'' and wrap in single quotes
    $escapedCommand = $dbtCommand -replace "'", "'\''"
    
    # Create the command to run on EC2
    # Source environment setup script, change to dbt directory, activate pipenv if needed, and run dbt
    $projectRoot = "/opt/dbt_dental_clinic"
    $envScript = "$projectRoot/scripts/setup_ec2_dbt_env.sh"
    
    $bashCommand = @"
# Source environment variables if script exists
if [ -f '$envScript' ]; then \
    source '$envScript'; \
fi && \
cd '$DbtPath' && \
if [ -f Pipfile ]; then \
    pipenv run dbt $escapedCommand; \
else \
    /home/ec2-user/.local/bin/dbt $escapedCommand; \
fi
"@
    
    # AWS SSM requires parameters as JSON object: {"commands": [...]}
    $parameters = @{
        commands = @($bashCommand)
    }
    $commandJson = $parameters | ConvertTo-Json -Compress
    
    Write-Host "📤 Sending command to EC2..." -ForegroundColor Yellow
    
    $response = aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --parameters $commandJson `
        --output json | ConvertFrom-Json
    
    $commandId = $response.Command.CommandId
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    
    # Wait for command to complete
    $maxRetries = 120  # Allow up to 4 minutes for dbt commands
    $retryCount = 0
    $output = $null
    
    Write-Host "   Waiting for dbt to complete..." -ForegroundColor Gray
    Write-Host ""
    
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
        if ($retryCount % 5 -eq 0) {
            Write-Host "." -NoNewline -ForegroundColor Gray
        }
    }
    
    Write-Host "`n" # New line
    
    # Display output
    if ($output.StandardOutputContent) {
        Write-Host "📄 Output:" -ForegroundColor Cyan
        Write-Host $output.StandardOutputContent
    }
    
    if ($output.StandardErrorContent) {
        Write-Host "`n⚠️  Errors/Warnings:" -ForegroundColor Yellow
        Write-Host $output.StandardErrorContent
    }
    
    if ($output.Status -eq "Success" -and $output.ResponseCode -eq 0) {
        Write-Host "`n✅ dbt command completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "`n❌ dbt command failed" -ForegroundColor Red
        Write-Host "   Status: $($output.Status)" -ForegroundColor Red
        Write-Host "   Response Code: $($output.ResponseCode)" -ForegroundColor Red
        exit 1
    }
    
} catch {
    Write-Host "`n❌ Error running dbt command: $_" -ForegroundColor Red
    exit 1
}
