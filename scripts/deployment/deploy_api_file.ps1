# PowerShell script to deploy a single API file to EC2 using AWS Systems Manager
# Usage: .\scripts\deployment\deploy_api_file.ps1 -FilePath "api\services\treatment_acceptance_service.py"
# Clinic env (recommended): writes api/.env (systemd EnvironmentFile source of truth)
# and RETIRES any stale api/.env_api_clinic on the instance.
# Preferred entry: mdc deploy api --env clinic  (from repo root)
# Manual: .\scripts\deployment\deploy_api_file.ps1 -FilePath "api\.env_api_clinic" -ClinicEnv
# Restarts systemd unit dental-clinic-api-clinic on clinic EC2 (see api/dental-clinic-api-clinic.service).
# Demo EC2 uses dental-clinic-api-demo or dental-clinic-api.
# Optional: backend_api.clinic_api.ec2.systemd_service in deployment_credentials.json
# Skip post-deploy /health/db check: add -SkipHealthCheck
# Single file (advanced): .\scripts\deployment\deploy_api_file.ps1 -FilePath "api\.env_api_clinic" -Clinic -RemoteFileName ".env"

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
    [switch]$NoRestart,
    # After -ClinicEnv deploy + restart, curl /health/db on the instance via SSM (default for -ClinicEnv).
    [switch]$SkipHealthCheck
)

$ErrorActionPreference = "Stop"
$script:DeployRestartSucceeded = $null

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

function Wait-AwsSsmCommandInvocation {
    param(
        [Parameter(Mandatory = $true)][string]$InstanceId,
        [Parameter(Mandatory = $true)][string]$CommandId,
        [int]$MaxWaitSeconds = 60,
        [string]$WaitingLabel = "Waiting for command..."
    )
    Write-Host "   $WaitingLabel" -ForegroundColor Gray
    $waited = 0
    $output = $null
    do {
        Start-Sleep -Seconds 2
        $waited += 2
        $output = aws ssm get-command-invocation `
            --command-id $CommandId `
            --instance-id $InstanceId `
            --output json | ConvertFrom-Json
    } while ($output.Status -eq "InProgress" -and $waited -lt $MaxWaitSeconds)
    return $output
}

function Get-ApiSystemdServiceName {
    param(
        [switch]$ClinicEnv,
        [string]$CredentialsFile = ""
    )
    if (-not $CredentialsFile) {
        $CredentialsFile = Join-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) "deployment_credentials.json"
    }
    # Clinic EC2 uses dental-clinic-api-clinic.service (see api/dental-clinic-api-clinic.service).
    if ($ClinicEnv -or $Clinic) {
        if (Test-Path -LiteralPath $CredentialsFile) {
            try {
                $credentials = Get-Content -LiteralPath $CredentialsFile | ConvertFrom-Json
                $fromCreds = [string]$credentials.backend_api.clinic_api.ec2.systemd_service
                if ($fromCreds) {
                    # Legacy credentials used demo unit name "dental-clinic-api" before clinic unit existed.
                    if ($fromCreds -eq 'dental-clinic-api') {
                        return 'dental-clinic-api-clinic'
                    }
                    return $fromCreds
                }
            } catch {
                # fall through
            }
        }
        return "dental-clinic-api-clinic"
    }
    # Demo EC2 uses dental-clinic-api-demo or legacy dental-clinic-api.
    return "dental-clinic-api"
}

function Invoke-ApiHealthDbCheck {
    param(
        [Parameter(Mandatory = $true)][string]$InstanceId,
        [int]$ApiPort = 8000,
        [string]$ServiceName = ""
    )
    Write-Host "`n🏥 Verifying GET /health/db on instance..." -ForegroundColor Yellow
    $healthCmds = @(
        'sleep 3'
    )
    if ($ServiceName) {
        $healthCmds += (
            'for i in $(seq 1 15); do systemctl is-active {0} >/dev/null 2>&1 && break; sleep 2; done' -f $ServiceName
        )
    }
    # Do not use curl -f with "|| echo 000" — failed connections write 000 via -w AND append 000 (HTTP=000000).
    # Capture status code only; retry until 200 or attempts exhausted.
    $healthCmds += @(
        ('for i in $(seq 1 10); do HTTP=$(curl -s -o /tmp/health_db.json -w ''%{{http_code}}'' http://127.0.0.1:{0}/health/db 2>/dev/null); HTTP=${{HTTP:-000}}; echo attempt $i HTTP=$HTTP; if [ "$HTTP" = 200 ]; then break; fi; sleep 3; done' -f $ApiPort),
        'echo HTTP: $HTTP',
        'cat /tmp/health_db.json 2>/dev/null || true',
        'test "$HTTP" = 200'
    )
    $response = Invoke-AwsSsmRunShellScript -InstanceId $InstanceId -Commands $healthCmds
    $commandId = $response.Command.CommandId
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    $output = Wait-AwsSsmCommandInvocation -InstanceId $InstanceId -CommandId $commandId -MaxWaitSeconds 120 -WaitingLabel "Waiting for /health/db check..."
    if ($output.Status -eq "Success") {
        Write-Host "✅ /health/db returned 200" -ForegroundColor Green
        if ($output.StandardOutputContent) {
            $output.StandardOutputContent -split "`n" | ForEach-Object {
                if ($_.Trim()) { Write-Host "   $_" -ForegroundColor Gray }
            }
        }
        return $true
    }
    Write-Host "❌ /health/db check failed (SSM status: $($output.Status))" -ForegroundColor Red
    if ($output.StandardErrorContent) {
        Write-Host $output.StandardErrorContent -ForegroundColor Red
    }
    if ($output.StandardOutputContent) {
        Write-Host $output.StandardOutputContent -ForegroundColor Gray
    }
    return $false
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
if ($relativePath -match '^[A-Za-z]:') {
    # Absolute Windows path passed; derive api-relative path from repo layout
    $relFromRoot = $localFile.Path
    $repoRootGuess = (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent)
    if ($relFromRoot.StartsWith($repoRootGuess, [StringComparison]::OrdinalIgnoreCase)) {
        $relativePath = $relFromRoot.Substring($repoRootGuess.Length).TrimStart('\', '/')
        $relativePath = $relativePath -replace "^api[/\\]", ""
    }
}
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
            ('ENV1=''{0}''' -f $remoteFilePath),
            ('STALE=''{0}''' -f $stale),
            'REMOTE_DIR=$(dirname "$ENV1")',
            'sudo mkdir -p "$REMOTE_DIR"',
            'if [ -f "$ENV1" ]; then BACKUP="${ENV1}.backup.$(date +%Y%m%d_%H%M%S)"; sudo cp "$ENV1" "$BACKUP"; echo "Backup: $BACKUP"; fi',
            ('echo ''{0}'' | base64 -d | sudo tee "$ENV1" > /dev/null' -f $base64Content),
            'sudo chown ec2-user:ec2-user "$ENV1"',
            'sudo chmod 644 "$ENV1"',
            'if [ -f "$STALE" ]; then RETIRED="${STALE}.retired.$(date +%Y%m%d_%H%M%S)"; sudo mv "$STALE" "$RETIRED"; echo "Retired stale env file: $STALE -> $RETIRED"; fi',
            'if [ -f "$ENV1" ]; then SIZE=$(stat -c%s "$ENV1" 2>/dev/null || stat -f%z "$ENV1" 2>/dev/null); echo "Deployed .env: $SIZE bytes (single source of truth)"; else echo "ERROR: Deployment failed"; exit 1; fi'
        )
    } else {
        $commands = @(
            ('REMOTE_FILE=''{0}''' -f $remoteFilePath),
            'REMOTE_DIR=$(dirname "$REMOTE_FILE")',
            'if [ -f "$REMOTE_FILE" ]; then BACKUP="${REMOTE_FILE}.backup.$(date +%Y%m%d_%H%M%S)"; sudo cp "$REMOTE_FILE" "$BACKUP"; echo "Backup: $BACKUP"; fi',
            'sudo mkdir -p "$REMOTE_DIR"',
            ('echo ''{0}'' | base64 -d | sudo tee "$REMOTE_FILE" > /dev/null' -f $base64Content),
            'sudo chown ec2-user:ec2-user "$REMOTE_FILE"',
            'sudo chmod 644 "$REMOTE_FILE"',
            'if [ -f "$REMOTE_FILE" ]; then SIZE=$(stat -c%s "$REMOTE_FILE" 2>/dev/null || stat -f%z "$REMOTE_FILE" 2>/dev/null); echo "Deployed: $SIZE bytes"; else echo "ERROR: Deployment failed"; exit 1; fi'
        )
    }
    
    Write-Host "   Sending deployment command..." -ForegroundColor Gray
    
    $response = Invoke-AwsSsmRunShellScript -InstanceId $InstanceId -Commands $commands
    
    $commandId = $response.Command.CommandId
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    
    # Wait for command to complete
    $output = Wait-AwsSsmCommandInvocation -InstanceId $InstanceId -CommandId $commandId -MaxWaitSeconds 30 -WaitingLabel "Waiting for deployment..."
    
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
            
            $restartSucceeded = $false
            try {
                # Clinic: dental-clinic-api-clinic (Get-ApiSystemdServiceName); demo: dental-clinic-api-demo / dental-clinic-api.
                $serviceName = Get-ApiSystemdServiceName -ClinicEnv:($Clinic -or $ClinicEnv)
                $restartCmds = @(
                    ('sudo systemctl restart {0}' -f $serviceName),
                    ('for i in $(seq 1 15); do if systemctl is-active {0} >/dev/null 2>&1; then echo Service is active; exit 0; fi; sleep 2; done; echo Service is not active; systemctl status {0} --no-pager -l 2>&1 | tail -15; exit 1' -f $serviceName)
                )
                $restartResponseObj = Invoke-AwsSsmRunShellScript -InstanceId $InstanceId -Commands $restartCmds
                
                $restartCommandId = $restartResponseObj.Command.CommandId
                Write-Host "   Command ID: $restartCommandId" -ForegroundColor Gray
                
                $restartOutput = Wait-AwsSsmCommandInvocation -InstanceId $InstanceId -CommandId $restartCommandId -MaxWaitSeconds 90 -WaitingLabel "Waiting for service restart..."
                
                if ($restartOutput.Status -eq "Success") {
                    $restartSucceeded = $true
                    Write-Host "✅ API service restarted ($serviceName)" -ForegroundColor Green
                    if ($restartOutput.StandardOutputContent) {
                        $statusLines = $restartOutput.StandardOutputContent -split "`n" | Where-Object { $_.Trim() }
                        if ($statusLines) {
                            Write-Host "   Status: $($statusLines[-1])" -ForegroundColor Gray
                        }
                    }
                    if ($ClinicEnv -and -not $SkipHealthCheck) {
                        $healthOk = Invoke-ApiHealthDbCheck -InstanceId $InstanceId -ServiceName $serviceName
                        if (-not $healthOk) {
                            Write-Host "`n⚠️  Deploy succeeded but /health/db did not return 200. Check api/.env and RDS connectivity." -ForegroundColor Yellow
                            exit 1
                        }
                    }
                } else {
                    Write-Host "⚠️  Service restart finished with status: $($restartOutput.Status)" -ForegroundColor Yellow
                    if ($restartOutput.StandardErrorContent) {
                        Write-Host $restartOutput.StandardErrorContent -ForegroundColor Red
                    }
                    if ($ClinicEnv -and -not $SkipHealthCheck) {
                        exit 1
                    }
                }
            } catch {
                Write-Host "⚠️  Service restart failed: $($_.Exception.Message)" -ForegroundColor Yellow
                if ($ClinicEnv -and -not $SkipHealthCheck) {
                    exit 1
                }
            } finally {
                # Restore original encoding
                [Console]::OutputEncoding = $originalOutputEncoding
            }
            $script:DeployRestartSucceeded = $restartSucceeded
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
} elseif ($script:DeployRestartSucceeded -eq $true) {
    if ($ClinicEnv -and -not $SkipHealthCheck) {
        Write-Host "`n✅ Done! File deployed, service restarted, and /health/db OK." -ForegroundColor Green
    } else {
        Write-Host "`n✅ Done! File deployed and service restarted." -ForegroundColor Green
    }
} else {
    Write-Host "`n✅ Done! File deployed." -ForegroundColor Green
}
Write-Host ""
