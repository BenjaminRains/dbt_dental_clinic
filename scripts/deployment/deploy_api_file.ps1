# PowerShell script to deploy a single API file to EC2 using AWS Systems Manager
# Usage: .\scripts\deploy_api_file.ps1 -FilePath "api\services\treatment_acceptance_service.py"
# Clinic env (recommended): writes api/.env (the single systemd EnvironmentFile source of truth)
# and RETIRES any stale api/.env_api_clinic on the instance. config.py treats the OS environment
# (systemd .env) as authoritative and no longer reads .env_api_clinic on EC2, so one file wins
# (see ENVIRONMENT_HANDLING_REVIEW.md, Phase 0).
#        .\scripts\deploy_api_file.ps1 -FilePath "api\.env_api_clinic" -ClinicEnv
# Single file (advanced): .\scripts\deploy_api_file.ps1 -FilePath "api\.env_api_clinic" -Clinic -RemoteFileName ".env"

param(
    [Parameter(Mandatory=$true)]
    [string]$FilePath,
    
    [string]$InstanceId = "",
    [string]$RemotePath = "/opt/dbt_dental_clinic/api",
    [switch]$Clinic,
    # Deploy clinic env to api/.env (single source of truth) and retire any stale .env_api_clinic.
    [switch]$ClinicEnv,
    [string]$RemoteFileName = "",
    # Skip systemctl restart (use when batching multiple files; restart on the last deploy only).
    [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

# SSM send-command with huge base64 in --parameters exceeds Windows CreateProcess cmdline limits (~8191).
# Use --cli-input-json via a temp file instead.
function Invoke-AwsSsmRunShellScript {
    param(
        [Parameter(Mandatory = $true)][string]$InstanceId,
        [Parameter(Mandatory = $true)][string[]]$Commands
    )
    $body = @{
        InstanceIds = @($InstanceId)
        DocumentName = "AWS-RunShellScript"
        Parameters    = @{ commands = @($Commands) }
    }
    $tmp = Join-Path $env:TEMP ("ssm-send-" + [Guid]::NewGuid().ToString() + ".json")
    $outF = Join-Path $env:TEMP ("ssm-aws-out-" + [Guid]::NewGuid().ToString() + ".txt")
    $errF = Join-Path $env:TEMP ("ssm-aws-err-" + [Guid]::NewGuid().ToString() + ".txt")
    try {
        $jsonText = $body | ConvertTo-Json -Depth 12
        [System.IO.File]::WriteAllText($tmp, $jsonText, [System.Text.UTF8Encoding]::new($false))
        $abs = (Resolve-Path -LiteralPath $tmp).Path
        # Avoid merging stderr into stdout (breaks JSON); AWS CLI accepts file://C:/... on Windows
        $fileUrl = "file://" + ($abs -replace '\\', '/')

        $awsExe = (Get-Command aws -CommandType Application -ErrorAction Stop).Source
        $p = Start-Process -FilePath $awsExe -ArgumentList @(
            'ssm', 'send-command',
            '--cli-input-json', $fileUrl,
            '--output', 'json'
        ) -Wait -PassThru -NoNewWindow -RedirectStandardOutput $outF -RedirectStandardError $errF

        $stdout = [System.IO.File]::ReadAllText($outF)
        $stderr = [System.IO.File]::ReadAllText($errF)
        if ($p.ExitCode -ne 0) {
            throw "aws ssm send-command failed (exit $($p.ExitCode)). stderr: $stderr"
        }
        if ([string]::IsNullOrWhiteSpace($stdout)) {
            throw "aws ssm send-command returned empty output. stderr: $stderr"
        }
        return ($stdout | ConvertFrom-Json)
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $outF -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $errF -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "`n🚀 Deploying API file to EC2" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        if (($Clinic -or $ClinicEnv) -and $credentials.backend_api.clinic_api.ec2.instance_id) {
            $InstanceId = $credentials.backend_api.clinic_api.ec2.instance_id
            Write-Host "✅ Loaded clinic instance ID from deployment_credentials.json: $InstanceId" -ForegroundColor Green
        } else {
            $InstanceId = $credentials.backend_api.ec2.instance_id
            Write-Host "✅ Loaded instance ID from deployment_credentials.json: $InstanceId" -ForegroundColor Green
        }
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

# Determine the remote file path(s)
$relativePath = $FilePath -replace "^api\\", "" -replace "^api/", ""
if ($ClinicEnv) {
    if ($RemoteFileName) {
        Write-Host "⚠️  -ClinicEnv ignores -RemoteFileName; writing .env and retiring stale .env_api_clinic." -ForegroundColor Yellow
    }
    $remotePathNorm = $RemotePath -replace '\\', '/'
    $remoteFilePath = "$remotePathNorm/.env"
    $clinicEnvSecondPath = "$remotePathNorm/.env_api_clinic"
} elseif ($RemoteFileName) {
    $remoteFilePath = "$RemotePath/$RemoteFileName"
    $clinicEnvSecondPath = $null
} else {
    $relUnix = $relativePath -replace '\\', '/'
    $remoteFilePath = "$RemotePath/$relUnix"
    $clinicEnvSecondPath = $null
}

$remoteFilePath = $remoteFilePath -replace '\\', '/'
if ($null -ne $clinicEnvSecondPath) {
    $clinicEnvSecondPath = $clinicEnvSecondPath -replace '\\', '/'
}

Write-Host "`n📄 File to deploy:" -ForegroundColor Cyan
Write-Host "   Local: $localFile" -ForegroundColor Gray
if ($ClinicEnv) {
    Write-Host "   Remote: $remoteFilePath (single source of truth)" -ForegroundColor Gray
    Write-Host "   Retiring stale (if present): $clinicEnvSecondPath" -ForegroundColor Gray
} else {
    Write-Host "   Remote: $remoteFilePath" -ForegroundColor Gray
}
Write-Host "   Instance: $InstanceId" -ForegroundColor Gray

# Read the file content
Write-Host "`n📖 Reading file content..." -ForegroundColor Yellow
$fileContent = Get-Content $localFile -Raw -Encoding UTF8
if ($null -eq $fileContent) { $fileContent = "" }
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
    if ($ClinicEnv) {
        $stale = $clinicEnvSecondPath -replace '\\', '/'
        $commands = @(
            "ENV1='$remoteFilePath'",
            "STALE='$stale'",
            "REMOTE_DIR=`$(dirname `"`$ENV1`")",
            "sudo mkdir -p `"`$REMOTE_DIR`"",
            "if [ -f `"`$ENV1`" ]; then BACKUP=`"`${ENV1}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$ENV1`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
            "echo '$base64Content' | base64 -d | sudo tee `"`$ENV1`" > /dev/null",
            "sudo chown ec2-user:ec2-user `"`$ENV1`"",
            "sudo chmod 644 `"`$ENV1`"",
            "if [ -f `"`$STALE`" ]; then RETIRED=`"`${STALE}.retired.`$(date +%Y%m%d_%H%M%S)`"; sudo mv `"`$STALE`" `"`$RETIRED`"; echo `"Retired stale env file: `$STALE -> `$RETIRED`"; fi",
            "if [ -f `"`$ENV1`" ]; then SIZE=`$(stat -c%s `"`$ENV1`" 2>/dev/null || stat -f%z `"`$ENV1`" 2>/dev/null); echo `"Deployed .env: `$SIZE bytes (single source of truth)`"; else echo `"ERROR: Deployment failed`"; exit 1; fi"
        )
    } else {
        $commands = @(
            "REMOTE_FILE='$remoteFilePath'",
            "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
            "if [ -f `"`$REMOTE_FILE`" ]; then BACKUP=`"`${REMOTE_FILE}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$REMOTE_FILE`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
            "sudo mkdir -p `"`$REMOTE_DIR`"",
            "echo '$base64Content' | base64 -d | sudo tee `"`$REMOTE_FILE`" > /dev/null",
            "sudo chown ec2-user:ec2-user `"`$REMOTE_FILE`"",
            "sudo chmod 644 `"`$REMOTE_FILE`"",
            "if [ -f `"`$REMOTE_FILE`" ]; then SIZE=`$(stat -c%s `"`$REMOTE_FILE`" 2>/dev/null || stat -f%z `"`$REMOTE_FILE`" 2>/dev/null); echo `"Deployed: `$SIZE bytes`"; else echo `"ERROR: Deployment failed`"; exit 1; fi"
        )
    }
    
    Write-Host "   Sending deployment command..." -ForegroundColor Gray
    
    $response = Invoke-AwsSsmRunShellScript -InstanceId $InstanceId -Commands $commands
    
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
        
        if (-not $NoRestart) {
            Write-Host "`n🔄 Restarting API service..." -ForegroundColor Yellow
            
            # Set UTF-8 encoding to handle Unicode characters in systemctl output
            $originalOutputEncoding = [Console]::OutputEncoding
            [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
            
            try {
                # Use simpler status check to avoid Unicode characters in output
                $serviceName = if ($Clinic -or $ClinicEnv) { "dental-clinic-api-clinic" } else { "dental-clinic-api" }
                $restartCmds = @(
                    "sudo systemctl restart $serviceName",
                    "sleep 2",
                    "sudo systemctl is-active $serviceName && echo Service is active || echo Service is not active"
                )
                $restartResponseObj = Invoke-AwsSsmRunShellScript -InstanceId $InstanceId -Commands $restartCmds
                
                $restartCommandId = $restartResponseObj.Command.CommandId
                Write-Host "   Command ID: $restartCommandId" -ForegroundColor Gray
                Start-Sleep -Seconds 3
                
                $restartOutput = aws ssm get-command-invocation `
                    --command-id $restartCommandId `
                    --instance-id $InstanceId `
                    --output json | ConvertFrom-Json
                
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
            Write-Host "`n⏭️  Skipping service restart (-NoRestart)" -ForegroundColor Gray
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

if ($NoRestart) {
    Write-Host "`n✅ Done! File deployed (restart skipped)." -ForegroundColor Green
} else {
    Write-Host "`n✅ Done! File deployed and service restarted." -ForegroundColor Green
}
Write-Host ""
