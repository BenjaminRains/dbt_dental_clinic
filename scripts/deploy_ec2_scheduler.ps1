# Deploy EC2 Scheduler Lambda Function
# This script creates a Lambda function and EventBridge rules to schedule EC2 instances

param(
    [string]$Region = "us-east-1",
    [string]$FunctionName = "ec2-clinic-scheduler",
    [string]$ScheduleTag = "clinic-extended-hours",
    [string]$AccountId = ""
)

Write-Host "🚀 Deploying EC2 Scheduler Lambda Function..." -ForegroundColor Green

# Try to load AccountId from deployment_credentials.json if not provided
if ([string]::IsNullOrEmpty($AccountId)) {
    $credentialsPath = Join-Path $PSScriptRoot ".." "deployment_credentials.json"
    if (Test-Path $credentialsPath) {
        try {
            $credentials = Get-Content $credentialsPath | ConvertFrom-Json
            if ($credentials.aws_account.account_id -and $credentials.aws_account.account_id -ne "<AWS_ACCOUNT_ID>") {
                $AccountId = $credentials.aws_account.account_id
                Write-Host "✅ Loaded AWS Account ID from deployment_credentials.json" -ForegroundColor Green
            }
        } catch {
            Write-Host "⚠️  Failed to load AccountId from deployment_credentials.json: $_" -ForegroundColor Yellow
        }
    }
}

# Step 1: Create IAM Role for Lambda
Write-Host "`n📋 Step 1: Creating IAM Role..." -ForegroundColor Yellow

$trustPolicy = @{
    Version = "2012-10-17"
    Statement = @(
        @{
            Effect = "Allow"
            Principal = @{
                Service = "lambda.amazonaws.com"
            }
            Action = "sts:AssumeRole"
        }
    )
} | ConvertTo-Json -Depth 10

$roleName = "$FunctionName-role"

try {
    # Check if role exists
    $existingRole = aws iam get-role --role-name $roleName --region $Region 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ IAM Role already exists: $roleName" -ForegroundColor Green
        $roleArn = (aws iam get-role --role-name $roleName --query 'Role.Arn' --output text)
    } else {
        Write-Host "Creating IAM role: $roleName..." -ForegroundColor Yellow
        aws iam create-role `
            --role-name $roleName `
            --assume-role-policy-document $trustPolicy `
            --description "Role for EC2 Scheduler Lambda function" `
            --output json | Out-Null
        
        # Attach basic Lambda execution policy
        aws iam attach-role-policy `
            --role-name $roleName `
            --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
        
        # Create and attach EC2 policy
        $ec2Policy = @{
            Version = "2012-10-17"
            Statement = @(
                @{
                    Effect = "Allow"
                    Action = @(
                        "ec2:DescribeInstances",
                        "ec2:StartInstances",
                        "ec2:StopInstances",
                        "ec2:DescribeInstanceStatus"
                    )
                    Resource = "*"
                }
            )
        } | ConvertTo-Json -Depth 10
        
        $policyName = "$FunctionName-ec2-policy"
        aws iam put-role-policy `
            --role-name $roleName `
            --policy-name $policyName `
            --policy-document $ec2Policy
        
        $roleArn = (aws iam get-role --role-name $roleName --query 'Role.Arn' --output text)
        Write-Host "✅ IAM Role created: $roleArn" -ForegroundColor Green
        
        # Wait for role to be available
        Write-Host "⏳ Waiting for IAM role to propagate..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
    }
} catch {
    Write-Host "❌ Error creating IAM role: $_" -ForegroundColor Red
    exit 1
}

# Step 2: Create Lambda deployment package
Write-Host "`n📦 Step 2: Creating Lambda deployment package..." -ForegroundColor Yellow

$lambdaDir = ".\lambda_package"
if (Test-Path $lambdaDir) {
    Remove-Item -Path $lambdaDir -Recurse -Force
}
New-Item -ItemType Directory -Path $lambdaDir | Out-Null

# Copy Lambda function
Copy-Item -Path ".\scripts\ec2_scheduler_lambda.py" -Destination "$lambdaDir\lambda_function.py"

# Install dependencies (pytz for timezone handling)
Write-Host "Installing dependencies..." -ForegroundColor Yellow
pip install pytz -t $lambdaDir --quiet

# Create zip file
$zipFile = ".\lambda_package.zip"
if (Test-Path $zipFile) {
    Remove-Item -Path $zipFile -Force
}
Compress-Archive -Path "$lambdaDir\*" -DestinationPath $zipFile -Force
Write-Host "✅ Deployment package created: $zipFile" -ForegroundColor Green

# Step 3: Create or Update Lambda Function
Write-Host "`n⚡ Step 3: Creating/Updating Lambda Function..." -ForegroundColor Yellow

$zipContent = [Convert]::ToBase64String([IO.File]::ReadAllBytes((Resolve-Path $zipFile)))

try {
    # Check if function exists
    $existingFunction = aws lambda get-function --function-name $FunctionName --region $Region 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Updating existing Lambda function..." -ForegroundColor Yellow
        aws lambda update-function-code `
            --function-name $FunctionName `
            --zip-file "fileb://$zipFile" `
            --region $Region | Out-Null
        
        # Update environment variables
        aws lambda update-function-configuration `
            --function-name $FunctionName `
            --environment "Variables={SCHEDULE_TAG=$ScheduleTag}" `
            --region $Region | Out-Null
        
        Write-Host "✅ Lambda function updated" -ForegroundColor Green
    } else {
        Write-Host "Creating new Lambda function..." -ForegroundColor Yellow
        aws lambda create-function `
            --function-name $FunctionName `
            --runtime python3.11 `
            --role $roleArn `
            --handler lambda_function.lambda_handler `
            --zip-file "fileb://$zipFile" `
            --timeout 60 `
            --memory-size 128 `
            --environment "Variables={SCHEDULE_TAG=$ScheduleTag}" `
            --description "Schedules EC2 instances based on Schedule tag" `
            --region $Region | Out-Null
        
        Write-Host "✅ Lambda function created" -ForegroundColor Green
    }
} catch {
    Write-Host "❌ Error creating Lambda function: $_" -ForegroundColor Red
    exit 1
}

# Step 4: Create EventBridge Rules
Write-Host "`n⏰ Step 4: Creating EventBridge Rules..." -ForegroundColor Yellow

# We'll create rules that run every hour, and the Lambda will check Central Time
# This handles DST automatically since Lambda checks the actual Central Time

# Start rule - runs at 7 AM Central (check every hour around that time)
# 7 AM Central = 12:00 UTC (CDT) or 13:00 UTC (CST)
# We'll run at 12:00 UTC and Lambda will check if it's 7 AM Central
$startRuleName = "$FunctionName-start"
$startCron = "cron(0 12 * * ? *)"  # 12:00 UTC - Lambda will check if it's 7 AM Central

# Stop rule - runs at 9 PM Central (check every hour around that time)  
# 9 PM Central = 02:00 UTC next day (CDT) or 03:00 UTC next day (CST)
# We'll run at 02:00 UTC and Lambda will check if it's 9 PM Central
$stopRuleName = "$FunctionName-stop"
$stopCron = "cron(0 2 * * ? *)"  # 02:00 UTC - Lambda will check if it's 9 PM Central

# Also create hourly check rule for reliability
$hourlyRuleName = "$FunctionName-hourly-check"
$hourlyCron = "cron(0 * * * ? *)"  # Every hour

# Get AWS account ID if still not provided (fallback to AWS CLI)
if ([string]::IsNullOrEmpty($AccountId)) {
    $AccountId = (aws sts get-caller-identity --query Account --output text)
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Error: Could not retrieve AWS account ID. Please provide --AccountId parameter, ensure deployment_credentials.json exists, or ensure AWS CLI is configured." -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ Retrieved AWS Account ID from AWS CLI" -ForegroundColor Green
}

$functionArn = "arn:aws:lambda:$Region:$AccountId:function:$FunctionName"

# Create start rule
try {
    aws events put-rule `
        --name $startRuleName `
        --schedule-expression $startCron `
        --description "Start clinic API instance at 7 AM Central" `
        --region $Region | Out-Null
    
    aws lambda add-permission `
        --function-name $FunctionName `
        --statement-id "$startRuleName-permission" `
        --action 'lambda:InvokeFunction' `
        --principal events.amazonaws.com `
        --source-arn "arn:aws:events:$Region:$AccountId:rule/$startRuleName" `
        --region $Region 2>&1 | Out-Null
    
    $startPayload = '{"action":"start"}' | ConvertTo-Json -Compress
    aws events put-targets `
        --rule $startRuleName `
        --targets "Id=1,Arn=$functionArn,Input=`"$startPayload`"" `
        --region $Region | Out-Null
    
    Write-Host "✅ Start rule created: $startRuleName (7 AM Central)" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Warning creating start rule: $_" -ForegroundColor Yellow
}

# Create stop rule
try {
    aws events put-rule `
        --name $stopRuleName `
        --schedule-expression $stopCron `
        --description "Stop clinic API instance at 9 PM Central" `
        --region $Region | Out-Null
    
    aws lambda add-permission `
        --function-name $FunctionName `
        --statement-id "$stopRuleName-permission" `
        --action 'lambda:InvokeFunction' `
        --principal events.amazonaws.com `
        --source-arn "arn:aws:events:$Region:$AccountId:rule/$stopRuleName" `
        --region $Region 2>&1 | Out-Null
    
    $stopPayload = '{"action":"stop"}' | ConvertTo-Json -Compress
    aws events put-targets `
        --rule $stopRuleName `
        --targets "Id=1,Arn=$functionArn,Input=`"$stopPayload`"" `
        --region $Region | Out-Null
    
    Write-Host "✅ Stop rule created: $stopRuleName (9 PM Central)" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Warning creating stop rule: $_" -ForegroundColor Yellow
}

# Create hourly check rule (for reliability and DST handling)
try {
    aws events put-rule `
        --name $hourlyRuleName `
        --schedule-expression $hourlyCron `
        --description "Hourly check to ensure instances follow schedule (handles DST)" `
        --region $Region | Out-Null
    
    aws lambda add-permission `
        --function-name $FunctionName `
        --statement-id "$hourlyRuleName-permission" `
        --action 'lambda:InvokeFunction' `
        --principal events.amazonaws.com `
        --source-arn "arn:aws:events:$Region:$AccountId:rule/$hourlyRuleName" `
        --region $Region 2>&1 | Out-Null
    
    # Hourly check doesn't need explicit action - Lambda will determine based on Central Time
    aws events put-targets `
        --rule $hourlyRuleName `
        --targets "Id=1,Arn=$functionArn" `
        --region $Region | Out-Null
    
    Write-Host "✅ Hourly check rule created: $hourlyRuleName (runs every hour)" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Warning creating hourly rule: $_" -ForegroundColor Yellow
}

# Cleanup
Write-Host "`n🧹 Cleaning up..." -ForegroundColor Yellow
Remove-Item -Path $lambdaDir -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path $zipFile -Force -ErrorAction SilentlyContinue

Write-Host "`n✅ EC2 Scheduler deployment complete!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Cyan

# Try to load instance IDs from deployment_credentials.json for helpful output
$credentialsPath = Join-Path $PSScriptRoot ".." "deployment_credentials.json"
$instanceIds = @()
if (Test-Path $credentialsPath) {
    try {
        $credentials = Get-Content $credentialsPath | ConvertFrom-Json
        if ($credentials.backend_api.ec2.instance_id -and $credentials.backend_api.ec2.instance_id -ne "<EC2_INSTANCE_ID>") {
            $instanceIds += $credentials.backend_api.ec2.instance_id
        }
        if ($credentials.ec2_scheduler -and $credentials.ec2_scheduler.instances) {
            foreach ($instance in $credentials.ec2_scheduler.instances.PSObject.Properties) {
                if ($instance.Value.instance_id -and $instance.Value.instance_id -notlike "<*") {
                    $instanceIds += $instance.Value.instance_id
                }
            }
        }
    } catch {
        # Silently fail - we'll just show generic instructions
    }
}

Write-Host "1. Tag EC2 instances with Schedule tag:" -ForegroundColor White
if ($instanceIds.Count -gt 0) {
    foreach ($instanceId in $instanceIds) {
        Write-Host "   aws ec2 create-tags --region $Region --resources $instanceId --tags Key=Schedule,Value=$ScheduleTag" -ForegroundColor Gray
    }
} else {
    Write-Host "   aws ec2 create-tags --region $Region --resources <INSTANCE_ID> --tags Key=Schedule,Value=$ScheduleTag" -ForegroundColor Gray
}
Write-Host "2. Monitor CloudWatch logs: /aws/lambda/$FunctionName" -ForegroundColor White
