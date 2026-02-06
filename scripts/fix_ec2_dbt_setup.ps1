# PowerShell script to fix all EC2 dbt setup issues
# Usage: .\scripts\fix_ec2_dbt_setup.ps1
# 
# This script will:
# 1. Deploy setup_ec2_dbt_env.sh script
# 2. Deploy deployment_credentials.json
# 3. Fix dbt PATH issue

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = "",
    [switch]$SkipCredentials = $false
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`n🔧 Fixing EC2 dbt Setup" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        $InstanceId = $credentials.backend_api.ec2.instance_id
        Write-Host "✅ Loaded instance ID: $InstanceId" -ForegroundColor Green
    } else {
        Write-Host "❌ deployment_credentials.json not found" -ForegroundColor Red
        exit 1
    }
}

Write-Host "`nSteps to execute:" -ForegroundColor Yellow
Write-Host "   1. Deploy setup_ec2_dbt_env.sh script" -ForegroundColor Gray
if (-not $SkipCredentials) {
    Write-Host "   2. Deploy deployment_credentials.json" -ForegroundColor Gray
    Write-Host "   3. Fix dbt PATH" -ForegroundColor Gray
    Write-Host "   4. Fix dbt directory permissions" -ForegroundColor Gray
} else {
    Write-Host "   2. Fix dbt PATH (skipping credentials deployment)" -ForegroundColor Gray
    Write-Host "   3. Fix dbt directory permissions" -ForegroundColor Gray
}
Write-Host ""

# Step 1: Deploy setup script
$stepNum = 1
$totalSteps = if ($SkipCredentials) { 3 } else { 4 }
Write-Host "`n[$stepNum/$totalSteps] Deploying setup_ec2_dbt_env.sh..." -ForegroundColor Cyan
& "$PSScriptRoot\deploy_setup_script.ps1" -InstanceId $InstanceId -ProjectRoot $ProjectRoot
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to deploy setup script" -ForegroundColor Red
    exit 1
}

# Step 2: Deploy credentials (if not skipped)
if (-not $SkipCredentials) {
    $stepNum++
    Write-Host "`n[$stepNum/$totalSteps] Deploying deployment_credentials.json..." -ForegroundColor Cyan
    & "$PSScriptRoot\deploy_credentials_json.ps1" -InstanceId $InstanceId -ProjectRoot $ProjectRoot
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to deploy credentials file" -ForegroundColor Red
        exit 1
    }
}

# Step 3: Fix PATH
$stepNum++
Write-Host "`n[$stepNum/$totalSteps] Fixing dbt PATH..." -ForegroundColor Cyan
& "$PSScriptRoot\fix_ec2_dbt_path.ps1" -InstanceId $InstanceId -ProjectRoot $ProjectRoot
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to fix PATH" -ForegroundColor Red
    exit 1
}

# Step 4: Fix permissions
$stepNum++
Write-Host "`n[$stepNum/$totalSteps] Fixing dbt directory permissions..." -ForegroundColor Cyan
& "$PSScriptRoot\fix_ec2_dbt_permissions.ps1" -InstanceId $InstanceId -ProjectRoot $ProjectRoot
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to fix permissions" -ForegroundColor Red
    exit 1
}

Write-Host "`nAll fixes completed successfully!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "   1. Connect to EC2: aws ssm start-session --target $InstanceId" -ForegroundColor Gray
Write-Host "   2. Source the setup script: source /opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh" -ForegroundColor Gray
Write-Host "   3. Source bashrc for PATH: source ~/.bashrc" -ForegroundColor Gray
Write-Host "   4. Test dbt: cd /opt/dbt_dental_clinic/dbt_dental_models && dbt debug --target clinic" -ForegroundColor Gray
