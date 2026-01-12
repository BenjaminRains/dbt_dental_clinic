# PowerShell script to deploy a single API file to EC2 using AWS Systems Manager
# Usage: .\scripts\deploy_api_file.ps1 -FilePath "api\services\treatment_acceptance_service.py"

param(
    [Parameter(Mandatory=$true)]
    [string]$FilePath,
    
    [string]$InstanceId = "",
    [string]$RemotePath = "/opt/dbt_dental_clinic/api"
)

$ErrorActionPreference = "Stop"

Write-Host "`n🚀 Deploying API file to EC2" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $PSScriptRoot ".." "deployment_credentials.json"
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

# Resolve the local file path
$localFile = Resolve-Path $FilePath -ErrorAction Stop
if (-not (Test-Path $localFile)) {
    Write-Host "❌ File not found: $FilePath" -ForegroundColor Red
    exit 1
}

# Determine the remote file path
$relativePath = $FilePath -replace "^api\\", "" -replace "^api/", ""
$remoteFilePath = "$RemotePath/$relativePath"

Write-Host "`n📄 File to deploy:" -ForegroundColor Cyan
Write-Host "   Local: $localFile" -ForegroundColor Gray
Write-Host "   Remote: $remoteFilePath" -ForegroundColor Gray
Write-Host "   Instance: $InstanceId" -ForegroundColor Gray

# Read the file content
Write-Host "`n📖 Reading file content..." -ForegroundColor Yellow
$fileContent = Get-Content $localFile -Raw -Encoding UTF8
$fileSize = (Get-Item $localFile).Length
Write-Host "   Size: $([math]::Round($fileSize / 1KB, 2)) KB" -ForegroundColor Gray

# Encode file content as base64 for safe transfer
$base64Content = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($fileContent))

Write-Host "`n📤 Deploying file..." -ForegroundColor Yellow

try {
    # Ensure remote path uses forward slashes (Linux path)
    $remoteFilePath = $remoteFilePath -replace '\\', '/'
    
    # Create deployment commands using base64 encoding
    # Note: AWS SSM expects parameters as JSON object with "commands" key
    $commands = @(
        "REMOTE_FILE='$remoteFilePath'",
        "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
        "if [ -f `"`$REMOTE_FILE`" ]; then BACKUP=`"`${REMOTE_FILE}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$REMOTE_FILE`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
        "sudo mkdir -p `"`$REMOTE_DIR`"",
        "echo '$base64Content' | base64 -d | sudo tee `"`$REMOTE_FILE`" > /dev/null",
        "sudo chmod 644 `"`$REMOTE_FILE`"",
        "if [ -f `"`$REMOTE_FILE`" ]; then SIZE=`$(stat -c%s `"`$REMOTE_FILE`" 2>/dev/null || stat -f%z `"`$REMOTE_FILE`" 2>/dev/null); echo `"Deployed: `$SIZE bytes`"; else echo `"ERROR: Deployment failed`"; exit 1; fi"
    )
    
    # AWS SSM requires parameters as JSON object: {"commands": [...]}
    $parameters = @{
        commands = $commands
    }
    $commandJson = $parameters | ConvertTo-Json -Compress
    
    Write-Host "   Sending deployment command..." -ForegroundColor Gray
    
    $response = aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --parameters $commandJson `
        --output json | ConvertFrom-Json
    
    $commandId = $response.Command.CommandId
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    
    # Wait for command to complete
    Write-Host "   Waiting for deployment..." -ForegroundColor Gray
    $maxWait = 30
    $waited = 0
    do {
        Start-Sleep -Seconds 2
        $waited += 2
        $output = aws ssm get-command-invocation `
            --command-id $commandId `
            --instance-id $InstanceId `
            --output json | ConvertFrom-Json
    } while ($output.Status -eq "InProgress" -and $waited -lt $maxWait)
    
    if ($output.Status -eq "Success") {
        Write-Host "`n✅ Deployment successful!" -ForegroundColor Green
        if ($output.StandardOutputContent) {
            Write-Host "`n📋 Output:" -ForegroundColor Cyan
            $output.StandardOutputContent -split "`n" | ForEach-Object {
                if ($_.Trim()) { Write-Host "   $_" -ForegroundColor Gray }
            }
        }
        
        Write-Host "`n🔄 Restarting API service..." -ForegroundColor Yellow
        
        # Set UTF-8 encoding to handle Unicode characters in systemctl output
        $originalOutputEncoding = [Console]::OutputEncoding
        [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
        
        try {
            # Use simpler status check to avoid Unicode characters in output
            $restartResponse = aws ssm send-command `
                --instance-ids $InstanceId `
                --document-name "AWS-RunShellScript" `
                --parameters '{"commands":["sudo systemctl restart dental-clinic-api","sleep 2","sudo systemctl is-active dental-clinic-api && echo Service is active || echo Service is not active"]}' `
                --output json 2>&1 | Out-String
            
            # Parse JSON with UTF-8 handling
            $restartResponseObj = $restartResponse | ConvertFrom-Json
            
            $restartCommandId = $restartResponseObj.Command.CommandId
            Write-Host "   Command ID: $restartCommandId" -ForegroundColor Gray
            Start-Sleep -Seconds 3
            
            $restartOutputJson = aws ssm get-command-invocation `
                --command-id $restartCommandId `
                --instance-id $InstanceId `
                --output json 2>&1 | Out-String
            
            $restartOutput = $restartOutputJson | ConvertFrom-Json
            
            if ($restartOutput.Status -eq "Success") {
                Write-Host "✅ API service restarted" -ForegroundColor Green
                if ($restartOutput.StandardOutputContent) {
                    $statusLines = $restartOutput.StandardOutputContent -split "`n" | Where-Object { $_.Trim() }
                    if ($statusLines) {
                        Write-Host "   Status: $($statusLines[-1])" -ForegroundColor Gray
                    }
                }
            } else {
                Write-Host "⚠️  Service restart completed with status: $($restartOutput.Status)" -ForegroundColor Yellow
            }
        } catch {
            Write-Host "⚠️  Could not parse restart status, but restart command was sent" -ForegroundColor Yellow
            Write-Host "   Error: $($_.Exception.Message)" -ForegroundColor DarkYellow
        } finally {
            # Restore original encoding
            [Console]::OutputEncoding = $originalOutputEncoding
        }
        
    } else {
        Write-Host "`n❌ Deployment failed!" -ForegroundColor Red
        Write-Host "`n📋 Error Output:" -ForegroundColor Red
        if ($output.StandardErrorContent) {
            Write-Host $output.StandardErrorContent -ForegroundColor Red
        }
        if ($output.StandardOutputContent) {
            Write-Host "Standard Output:" -ForegroundColor Yellow
            Write-Host $output.StandardOutputContent -ForegroundColor Gray
        }
        exit 1
    }
    
} catch {
    Write-Host "`n❌ Deployment error: $_" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Write-Host "`n✅ Done! File deployed and service restarted." -ForegroundColor Green
Write-Host ""
