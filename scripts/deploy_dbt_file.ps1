# PowerShell script to deploy dbt model files to EC2 using AWS Systems Manager
# Usage: 
#   Single file: .\scripts\deploy_dbt_file.ps1 -FilePath "dbt_dental_models\models\staging\opendental\stg_opendental__insplan.sql"
#   Multiple files: .\scripts\deploy_dbt_file.ps1 -FilePath "stg_opendental__insplan.sql","int_procedure_catalog.sql"
#   Relative paths: .\scripts\deploy_dbt_file.ps1 -FilePath "models\staging\opendental\stg_opendental__insplan.sql"
#
# Note: If a file already exists on the remote server, a timestamped backup will be created
#       (e.g., filename.backup.YYYYMMDD_HHMMSS) before the file is overwritten. Backups
#       accumulate over time and are not automatically cleaned up.

param(
    [Parameter(Mandatory=$false, Position=0, ValueFromRemainingArguments=$true)]
    [string[]]$FilePath,  # Accept multiple files (can be positional)
    
    [string]$InstanceId = "",
    [string]$RemotePath = "/opt/dbt_dental_clinic/dbt_dental_models",
    [string]$ProjectRoot = "",
    [switch]$Clinic = $false
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`n🚀 Deploying dbt model files to EC2" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        if ($Clinic -and $credentials.backend_api.clinic_api.ec2.instance_id) {
            $InstanceId = $credentials.backend_api.clinic_api.ec2.instance_id
            Write-Host "✅ Loaded clinic instance ID: $InstanceId" -ForegroundColor Green
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

# Process each file
$dbtModelsPath = Join-Path $ProjectRoot "dbt_dental_models"
$filesToDeploy = @()

# Handle case where FilePath might be empty (user passed no arguments)
if (-not $FilePath -or $FilePath.Count -eq 0) {
    Write-Host "❌ No files specified to deploy" -ForegroundColor Red
    Write-Host "   Usage: .\deploy_dbt_file.ps1 file1.sql file2.sql" -ForegroundColor Yellow
    Write-Host "   Or: .\deploy_dbt_file.ps1 -FilePath file1.sql,file2.sql" -ForegroundColor Yellow
    exit 1
}

foreach ($file in $FilePath) {
    # Try to resolve the file path
    $resolvedFile = $null
    
    # Try as absolute path first
    if (Test-Path $file) {
        $resolvedFile = (Resolve-Path $file).Path
    }
    # Try relative to project root
    elseif (Test-Path (Join-Path $ProjectRoot $file)) {
        $resolvedFile = (Resolve-Path (Join-Path $ProjectRoot $file)).Path
    }
    # Try relative to dbt_dental_models
    elseif (Test-Path (Join-Path $dbtModelsPath $file)) {
        $resolvedFile = (Resolve-Path (Join-Path $dbtModelsPath $file)).Path
    }
    # Try searching for the file by name in dbt_dental_models
    else {
        $fileName = Split-Path $file -Leaf
        $foundFiles = Get-ChildItem -Path $dbtModelsPath -Filter $fileName -Recurse -ErrorAction SilentlyContinue
        if ($foundFiles) {
            # If multiple matches, prefer files in models/ subdirectories
            $foundFile = $foundFiles | Where-Object { $_.FullName -like "*\models\*" } | Select-Object -First 1
            if (-not $foundFile) {
                $foundFile = $foundFiles | Select-Object -First 1
            }
            if ($foundFile) {
                $resolvedFile = $foundFile.FullName
            }
        }
    }
    
    if ([string]::IsNullOrEmpty($resolvedFile) -or -not (Test-Path $resolvedFile)) {
        Write-Host "❌ File not found: $file" -ForegroundColor Red
        Write-Host "   Searched:" -ForegroundColor Yellow
        Write-Host "     - $file" -ForegroundColor Gray
        Write-Host "     - $(Join-Path $ProjectRoot $file)" -ForegroundColor Gray
        Write-Host "     - $(Join-Path $dbtModelsPath $file)" -ForegroundColor Gray
        Write-Host "     - By filename '$fileName' in $dbtModelsPath" -ForegroundColor Gray
        continue
    }
    
    # Get the full path as a string (already resolved above)
    $resolvedPath = $resolvedFile
    
    # Determine the remote file path (preserve dbt_dental_models structure)
    $relativePath = ""
    
    if ($resolvedPath -like "*dbt_dental_models*") {
        # Extract path relative to dbt_dental_models
        $dbtIndex = $resolvedPath.IndexOf("dbt_dental_models")
        $relativePath = $resolvedPath.Substring($dbtIndex + "dbt_dental_models".Length + 1)
    } elseif ($resolvedPath -like "*models*") {
        # Extract path relative to models/
        $modelsIndex = $resolvedPath.IndexOf("models")
        $relativePath = $resolvedPath.Substring($modelsIndex + "models".Length + 1)
    } else {
        # Fallback: use the original file name
        $relativePath = Split-Path $file -Leaf
    }
    
    $remoteFilePath = "$RemotePath/$relativePath" -replace '\\', '/'
    
    $filesToDeploy += @{
        Local = $resolvedPath
        Remote = $remoteFilePath
        Relative = $relativePath
    }
}

if ($filesToDeploy.Count -eq 0) {
    Write-Host "❌ No valid files found to deploy" -ForegroundColor Red
    exit 1
}

Write-Host "`n📄 Files to deploy ($($filesToDeploy.Count)):" -ForegroundColor Cyan
foreach ($file in $filesToDeploy) {
    Write-Host "   Local:  $($file.Local)" -ForegroundColor Gray
    Write-Host "   Remote: $($file.Remote)" -ForegroundColor Gray
    Write-Host ""
}

# Deploy each file
$failedFiles = @()
foreach ($file in $filesToDeploy) {
    Write-Host "📤 Deploying: $($file.Relative)..." -ForegroundColor Yellow
    
    # Read the file content
    $fileContent = Get-Content $file.Local -Raw -Encoding UTF8
    $fileSize = (Get-Item $file.Local).Length
    Write-Host "   Size: $([math]::Round($fileSize / 1KB, 2)) KB" -ForegroundColor Gray
    
    # Encode file content as base64 for safe transfer
    $base64Content = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($fileContent))
    $base64Length = $base64Content.Length
    
    # Windows command-line limit is ~8191 characters, AWS SSM has similar limits
    # Use S3 as intermediary for files larger than 15KB
    $useS3 = $fileSize -gt 15360  # 15KB threshold
    
    try {
        if ($useS3) {
            Write-Host "   Using S3 transfer (file > 15KB)..." -ForegroundColor Gray
            
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
                Write-Host "   ❌ S3 bucket not found in deployment_credentials.json" -ForegroundColor Red
                Write-Host "   Large files (>15KB) require S3 for deployment" -ForegroundColor Yellow
                Write-Host "   Please configure frontend.s3_buckets.frontend.bucket_name in deployment_credentials.json" -ForegroundColor Yellow
                Write-Host "   Or set FRONTEND_BUCKET_NAME environment variable" -ForegroundColor Yellow
                throw "S3 bucket required for large file deployment"
            } else {
                Write-Host "   Using S3 bucket: $s3Bucket" -ForegroundColor Gray
                # Generate unique S3 key for temp file
                $fileName = Split-Path $file.Remote -Leaf
                $s3Key = "dbt-deploy-temp/$(Get-Random)_$fileName"
                $s3Path = "s3://$s3Bucket/$s3Key"
                
                Write-Host "   Uploading to S3: $s3Path" -ForegroundColor Gray
                
                # Upload file to S3
                $tempFile = [System.IO.Path]::GetTempFileName()
                try {
                    Set-Content -Path $tempFile -Value $fileContent -Encoding UTF8 -NoNewline
                    $uploadOutput = aws s3 cp $tempFile $s3Path 2>&1
                    if ($LASTEXITCODE -ne 0) {
                        Write-Host "   ❌ Failed to upload to S3: $uploadOutput" -ForegroundColor Red
                        throw "S3 upload failed"
                    }
                    Write-Host "   ✅ Uploaded to S3" -ForegroundColor Green
                } finally {
                    if (Test-Path $tempFile) {
                        Remove-Item $tempFile -Force
                    }
                }
                
                # Create deployment commands to download from S3
                $commands = @(
                    "REMOTE_FILE='$($file.Remote)'",
                    "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
                    "S3_PATH='$s3Path'",
                    "if [ -f `"`$REMOTE_FILE`" ]; then BACKUP=`"`${REMOTE_FILE}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$REMOTE_FILE`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
                    "sudo mkdir -p `"`$REMOTE_DIR`"",
                    "aws s3 cp `"`$S3_PATH`" - | sudo tee `"`$REMOTE_FILE`" > /dev/null",
                    "if [ `$? -ne 0 ]; then echo `"ERROR: S3 download failed`"; exit 1; fi",
                    "sudo chown ec2-user:ec2-user `"`$REMOTE_FILE`"",
                    "sudo chmod 644 `"`$REMOTE_FILE`"",
                    "aws s3 rm `"`$S3_PATH`" 2>/dev/null || true",
                    "if [ -f `"`$REMOTE_FILE`" ]; then SIZE=`$(stat -c%s `"`$REMOTE_FILE`" 2>/dev/null || stat -f%z `"`$REMOTE_FILE`" 2>/dev/null); echo `"Deployed: `$SIZE bytes`"; else echo `"ERROR: Deployment failed`"; exit 1; fi"
                )
            }
        }
        
        if (-not $useS3) {
            # For smaller files, use the original direct approach
            # Escape single quotes in base64 content
            $escapedBase64 = $base64Content -replace "'", "'\''"
            
            $commands = @(
                "REMOTE_FILE='$($file.Remote)'",
                "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
                "if [ -f `"`$REMOTE_FILE`" ]; then BACKUP=`"`${REMOTE_FILE}.backup.`$(date +%Y%m%d_%H%M%S)`"; sudo cp `"`$REMOTE_FILE`" `"`$BACKUP`"; echo `"Backup: `$BACKUP`"; fi",
                "sudo mkdir -p `"`$REMOTE_DIR`"",
                "echo '$escapedBase64' | base64 -d | sudo tee `"`$REMOTE_FILE`" > /dev/null",
                "sudo chown ec2-user:ec2-user `"`$REMOTE_FILE`"",
                "sudo chmod 644 `"`$REMOTE_FILE`"",
                "if [ -f `"`$REMOTE_FILE`" ]; then SIZE=`$(stat -c%s `"`$REMOTE_FILE`" 2>/dev/null || stat -f%z `"`$REMOTE_FILE`" 2>/dev/null); echo `"Deployed: `$SIZE bytes`"; else echo `"ERROR: Deployment failed`"; exit 1; fi"
            )
        }
        
        # Execute the deployment command
        
        # AWS SSM requires parameters as JSON object: {"commands": [...]}
        $parameters = @{
            commands = $commands
        }
        $commandJson = $parameters | ConvertTo-Json -Compress
        
        $response = aws ssm send-command `
            --instance-ids $InstanceId `
            --document-name "AWS-RunShellScript" `
            --parameters $commandJson `
            --output json | ConvertFrom-Json
        
        $commandId = $response.Command.CommandId
        
        # Wait for command to complete
        $maxRetries = 30
        $retryCount = 0
        $output = $null
        
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
        
        Write-Host "" # New line
        
        if ($output.Status -eq "Success" -and $output.ResponseCode -eq 0) {
            Write-Host "   ✅ Deployed successfully!" -ForegroundColor Green
        } else {
            Write-Host "   ❌ Deployment failed" -ForegroundColor Red
            if ($output.StandardErrorContent) {
                Write-Host "   Error: $($output.StandardErrorContent)" -ForegroundColor Red
            }
            $failedFiles += $file.Relative
        }
        
    } catch {
        Write-Host "   ❌ Deployment error: $_" -ForegroundColor Red
        $failedFiles += $file.Relative
    }
    
    Write-Host ""
}

if ($failedFiles.Count -eq 0) {
    Write-Host "✅ All files deployed successfully!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Some files failed to deploy:" -ForegroundColor Yellow
    foreach ($file in $failedFiles) {
        Write-Host "   - $file" -ForegroundColor Red
    }
    exit 1
}
