# PowerShell script to deploy deployment_credentials.json to EC2
# Usage: .\scripts\deploy_credentials_json.ps1

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`nDeploying deployment_credentials.json to EC2" -ForegroundColor Cyan
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

# Read the credentials file
$credentialsPath = Join-Path $ProjectRoot "deployment_credentials.json"
if (-not (Test-Path $credentialsPath)) {
    Write-Host "ERROR: deployment_credentials.json not found at: $credentialsPath" -ForegroundColor Red
    exit 1
}

# Read file with UTF-8 encoding (no BOM)
$fileContent = [System.IO.File]::ReadAllText($credentialsPath, [System.Text.Encoding]::UTF8)
$fileSize = (Get-Item $credentialsPath).Length
$remotePath = "/opt/dbt_dental_clinic/deployment_credentials.json"

# Initialize variables for cleanup
$useS3 = $false
$s3Path = $null

Write-Host "Source: $credentialsPath" -ForegroundColor Cyan
Write-Host "Remote: $remotePath" -ForegroundColor Cyan
Write-Host "Size: $([math]::Round($fileSize / 1KB, 2)) KB" -ForegroundColor Cyan
Write-Host ""

# Windows command-line limit is ~8191 characters, AWS SSM has similar limits
# Use S3 as intermediary for files larger than 15KB or if base64 would exceed limits
$base64Content = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($fileContent))
$useS3 = $fileSize -gt 15360 -or $base64Content.Length -gt 8000  # 15KB threshold or ~8KB base64

try {
    if ($useS3) {
        Write-Host "Using S3 transfer (file > 15KB or base64 too long)..." -ForegroundColor Gray
        
        # Get S3 bucket from deployment credentials or environment
        $s3Bucket = $null
        $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
        if (Test-Path $credentialsFile) {
            $credentials = Get-Content $credentialsFile | ConvertFrom-Json
            # Try to get bucket from frontend config (nested structure)
            if ($credentials.frontend -and $credentials.frontend.s3_buckets -and $credentials.frontend.s3_buckets.frontend -and $credentials.frontend.s3_buckets.frontend.bucket_name) {
                $s3Bucket = $credentials.frontend.s3_buckets.frontend.bucket_name
            } elseif ($env:FRONTEND_BUCKET_NAME) {
                $s3Bucket = $env:FRONTEND_BUCKET_NAME
            }
        }
        
        if (-not $s3Bucket) {
            Write-Host "ERROR: S3 bucket not found in deployment_credentials.json" -ForegroundColor Red
            Write-Host "Large files (>15KB) require S3 for deployment" -ForegroundColor Yellow
            Write-Host "Please configure frontend.s3_buckets.frontend.bucket_name in deployment_credentials.json" -ForegroundColor Yellow
            Write-Host "Or set FRONTEND_BUCKET_NAME environment variable" -ForegroundColor Yellow
            throw "S3 bucket required for large file deployment"
            } else {
                Write-Host "Using S3 bucket: $s3Bucket" -ForegroundColor Gray
                # Generate unique S3 key for temp file
                $fileName = Split-Path $remotePath -Leaf
                $s3Key = "dbt-deploy-temp/$(Get-Random)_$fileName"
                $s3Path = "s3://$s3Bucket/$s3Key"
                
                Write-Host "Uploading to S3: $s3Path" -ForegroundColor Gray
                
                # Upload file to S3
                $tempFile = [System.IO.Path]::GetTempFileName()
                try {
                    [System.IO.File]::WriteAllText($tempFile, $fileContent, [System.Text.Encoding]::UTF8)
                    $uploadOutput = aws s3 cp $tempFile $s3Path 2>&1
                    if ($LASTEXITCODE -ne 0) {
                        Write-Host "ERROR: Failed to upload to S3: $uploadOutput" -ForegroundColor Red
                        throw "S3 upload failed"
                    }
                    Write-Host "Uploaded to S3" -ForegroundColor Green
                    
                    # Generate presigned URL (valid for 5 minutes)
                    # This allows EC2 to download without S3 permissions
                    Write-Host "Generating presigned URL..." -ForegroundColor Gray
                    $presignedUrlOutput = aws s3 presign "$s3Path" --expires-in 300 2>&1
                    if ($LASTEXITCODE -ne 0) {
                        Write-Host "ERROR: Failed to generate presigned URL: $presignedUrlOutput" -ForegroundColor Red
                        Write-Host "EC2 instance needs S3 permissions or presigned URL generation failed" -ForegroundColor Yellow
                        throw "Presigned URL generation failed"
                    } else {
                        $presignedUrl = $presignedUrlOutput.Trim()
                        Write-Host "Presigned URL generated (valid for 5 minutes)" -ForegroundColor Green
                    }
                } finally {
                    if (Test-Path $tempFile) {
                        Remove-Item $tempFile -Force
                    }
                }
            
            # Create deployment commands to download from S3 using presigned URL
            # Escape the presigned URL for bash (replace single quotes with '\''')
            $escapedPresignedUrl = $presignedUrl -replace "'", "'\''"
            
            $commands = @(
                "REMOTE_FILE='$remotePath'",
                "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
                "S3_PATH='$s3Path'",
                "PRESIGNED_URL='$escapedPresignedUrl'",
                "if [ -f `"`$REMOTE_FILE`" ]; then BACKUP=`"`${REMOTE_FILE}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$REMOTE_FILE`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
                "sudo mkdir -p `"`$REMOTE_DIR`"",
                "echo 'Attempting S3 download using presigned URL (EC2 has no S3 permissions)...'",
                "if [ -n `"`$PRESIGNED_URL`" ]; then",
                "  echo 'Downloading via presigned URL...'",
                "  curl -s `"`$PRESIGNED_URL`" -o `"`$REMOTE_FILE`"",
                "  if [ `$? -eq 0 ] && [ -f `"`$REMOTE_FILE`" ] && [ -s `"`$REMOTE_FILE`" ]; then",
                "    echo 'Downloaded via presigned URL'",
                "    sudo chown ec2-user:ec2-user `"`$REMOTE_FILE`"",
                "  else",
                "    echo 'ERROR: Presigned URL download failed'",
                "    exit 1",
                "  fi",
                "else",
                "  echo 'ERROR: No presigned URL available'",
                "  exit 1",
                "fi",
                "sudo chmod 644 `"`$REMOTE_FILE`"",
                "if [ -f `"`$REMOTE_FILE`" ]; then SIZE=`$(stat -c%s `"`$REMOTE_FILE`" 2>/dev/null || stat -f%z `"`$REMOTE_FILE`" 2>/dev/null); echo `"Deployed: `$SIZE bytes`"; else echo `"ERROR: Deployment failed`"; exit 1; fi"
            )
        }
    } else {
        # For smaller files, use direct base64 transfer
        Write-Host "Using direct transfer (file <= 15KB)..." -ForegroundColor Gray
        $commands = @(
            "REMOTE_FILE='$remotePath'",
            "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
            "if [ -f `"`$REMOTE_FILE`" ]; then BACKUP=`"`${REMOTE_FILE}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$REMOTE_FILE`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
            "sudo mkdir -p `"`$REMOTE_DIR`"",
            "echo '$base64Content' | base64 -d | sudo tee `"`$REMOTE_FILE`" > /dev/null",
            "sudo chown ec2-user:ec2-user `"`$REMOTE_FILE`"",
            "sudo chmod 644 `"`$REMOTE_FILE`"",
            "if [ -f `"`$REMOTE_FILE`" ]; then SIZE=`$(stat -c%s `"`$REMOTE_FILE`" 2>/dev/null || stat -f%z `"`$REMOTE_FILE`" 2>/dev/null); echo `"Deployed: `$SIZE bytes`"; else echo `"Deployment failed`"; exit 1; fi"
        )
    }
    
    $parameters = @{
        commands = $commands
    }
    $commandJson = $parameters | ConvertTo-Json -Compress
    
    Write-Host "Deploying credentials file..." -ForegroundColor Yellow
    
    # Set console output encoding to UTF-8
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    
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
        Write-Host "`nWarnings:" -ForegroundColor Yellow
        Write-Host $output.StandardErrorContent
    }
    
    if ($output.Status -eq "Success" -and $output.ResponseCode -eq 0) {
        Write-Host "`nCredentials file deployed successfully!" -ForegroundColor Green
        
        # Clean up S3 temp file from local machine (EC2 doesn't have S3 permissions)
        if ($useS3 -and $s3Path) {
            Write-Host "`nCleaning up S3 temp file..." -ForegroundColor Gray
            $cleanupOutput = aws s3 rm $s3Path 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "S3 temp file cleaned up successfully" -ForegroundColor Green
            } else {
                Write-Host "Warning: Could not clean up S3 temp file: $cleanupOutput" -ForegroundColor Yellow
                Write-Host "File remains at: $s3Path" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "`nDeployment failed" -ForegroundColor Red
        Write-Host "   Status: $($output.Status)" -ForegroundColor Red
        
        # Still try to clean up S3 temp file even on failure
        if ($useS3 -and $s3Path) {
            Write-Host "`nCleaning up S3 temp file..." -ForegroundColor Gray
            aws s3 rm $s3Path 2>&1 | Out-Null
        }
        
        exit 1
    }
    
} catch {
    Write-Host "`nError deploying credentials file: $_" -ForegroundColor Red
    exit 1
}
