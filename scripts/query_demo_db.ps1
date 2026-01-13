# PowerShell script to query the demo database
# Usage: 
#   Local:  .\scripts\query_demo_db.ps1 -Query "SELECT * FROM marts.dim_procedure LIMIT 5" -Local
#   Remote: .\scripts\query_demo_db.ps1 -Query "SELECT * FROM marts.dim_procedure LIMIT 5"

param(
    [Parameter(Mandatory=$true)]
    [string]$Query,
    
    [switch]$Local = $false,
    [string]$InstanceId = "",
    [string]$RemotePath = "/opt/dbt_dental_clinic/dbt_dental_models"
)

$ErrorActionPreference = "Stop"

# Load database credentials
$credentialsPath = Join-Path (Split-Path $PSScriptRoot -Parent) "deployment_credentials.json"
if (-not (Test-Path $credentialsPath)) {
    Write-Host "❌ deployment_credentials.json not found" -ForegroundColor Red
    exit 1
}

$credentials = Get-Content $credentialsPath | ConvertFrom-Json
$demo = $credentials.demo_database

if ($Local) {
    # Query local database directly
    $demoHost = "localhost"
    $demoPort = "5432"
    $demoDatabase = $demo.postgresql.database
    $demoUser = $demo.postgresql.user
    $demoPassword = $demo.postgresql.password
    
    Write-Host "`n🔍 Querying Local Demo Database" -ForegroundColor Cyan
    Write-Host "   Host: $demoHost" -ForegroundColor Gray
    Write-Host "   Port: $demoPort" -ForegroundColor Gray
    Write-Host "   Database: $demoDatabase" -ForegroundColor Gray
    Write-Host ""
    
    # Check if psql is available
    $psqlPath = Get-Command psql -ErrorAction SilentlyContinue
    if (-not $psqlPath) {
        Write-Host "❌ psql not found. Please install PostgreSQL client tools." -ForegroundColor Red
        exit 1
    }
    
    # Set PGPASSWORD environment variable
    $env:PGPASSWORD = $demoPassword
    
    Write-Host "📤 Executing query..." -ForegroundColor Yellow
    Write-Host ""
    
    # Execute query directly
    & psql -h $demoHost -p $demoPort -U $demoUser -d $demoDatabase -c $Query
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✅ Query completed successfully" -ForegroundColor Green
    } else {
        Write-Host "`n❌ Query failed" -ForegroundColor Red
        exit 1
    }
} else {
    # Query remote database via AWS SSM
    # Get instance ID from deployment_credentials.json if not provided
    if (-not $InstanceId) {
        $InstanceId = $credentials.backend_api.ec2.instance_id
        Write-Host "✅ Loaded instance ID from deployment_credentials.json: $InstanceId" -ForegroundColor Green
    }
    
    # Since we're running via SSM on EC2, always use direct connection to RDS
    # Try database_connection first, then postgresql.host, then ec2.private_ip
    $demoHost = $null
    if ($demo.database_connection -and $demo.database_connection.host) {
        $demoHost = $demo.database_connection.host
    } elseif ($demo.postgresql -and $demo.postgresql.host) {
        $demoHost = $demo.postgresql.host
    } elseif ($demo.ec2 -and $demo.ec2.private_ip) {
        $demoHost = $demo.ec2.private_ip
    } else {
        # Fallback to hardcoded value from run_dbt_ssm.ps1
        $demoHost = "172.31.25.7"
    }
    
    $demoPort = "5432"
    if ($demo.database_connection -and $demo.database_connection.port) {
        $demoPort = $demo.database_connection.port.ToString()
    } elseif ($demo.postgresql -and $demo.postgresql.port) {
        $demoPort = $demo.postgresql.port.ToString()
    }
    
    Write-Host "`n🔍 Querying Remote Demo Database" -ForegroundColor Cyan
    Write-Host "   Host: $demoHost" -ForegroundColor Gray
    Write-Host "   Port: $demoPort" -ForegroundColor Gray
    Write-Host "   Database: $($demo.postgresql.database)" -ForegroundColor Gray
    Write-Host ""
    
    # Use base64 encoding to avoid escaping issues with quotes and backslashes
    $queryBytes = [System.Text.Encoding]::UTF8.GetBytes($Query)
    $queryBase64 = [Convert]::ToBase64String($queryBytes)
    
    # Escape single quotes in password for bash (if any)
    $escapedPassword = $demo.postgresql.password -replace "'", "'\''"
    
    # Build psql command using base64-encoded query
    # Use here-string to avoid PowerShell escaping issues
    $psqlCmd = @"
cd $RemotePath && sudo -u ec2-user bash -c "export PGPASSWORD='$escapedPassword' && echo '$queryBase64' | base64 -d | psql -h $demoHost -p $demoPort -U $($demo.postgresql.user) -d $($demo.postgresql.database)"
"@
    
    $commands = @($psqlCmd)
    $parameters = @{
        commands = $commands
    }
    $parametersJson = $parameters | ConvertTo-Json -Compress
    
    Write-Host "📤 Sending query via AWS SSM..." -ForegroundColor Yellow
    
    $response = aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --parameters $parametersJson `
        --output json | ConvertFrom-Json
    
    $commandId = $response.Command.CommandId
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    
    # Wait for command to complete
    $maxRetries = 30
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
        Write-Host "=== Query Results ===" -ForegroundColor Cyan
        Write-Host $output.StandardOutputContent
    }
    
    if ($output.StandardErrorContent) {
        Write-Host "=== Errors ===" -ForegroundColor Red
        Write-Host $output.StandardErrorContent
    }
    
    if ($output.Status -eq "Success" -and $output.ResponseCode -eq 0) {
        Write-Host "`n✅ Query completed successfully" -ForegroundColor Green
    } else {
        Write-Host "`n❌ Query failed" -ForegroundColor Red
        exit 1
    }
}
