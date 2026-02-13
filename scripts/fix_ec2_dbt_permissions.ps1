# Fix dbt directory ownership and logs dir on EC2 via SSM
# Usage: .\scripts\fix_ec2_dbt_permissions.ps1
#        .\scripts\fix_ec2_dbt_permissions.ps1 -Clinic
#        .\scripts\fix_ec2_dbt_permissions.ps1 -InstanceId i-xxxxx

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = "",
    [switch]$Clinic = $false
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        if ($Clinic -and $credentials.backend_api.clinic_api.ec2.instance_id) {
            $InstanceId = $credentials.backend_api.clinic_api.ec2.instance_id
            Write-Host "Using clinic instance from deployment_credentials.json" -ForegroundColor Gray
        } else {
            $InstanceId = $credentials.backend_api.ec2.instance_id
        }
    }
}
if (-not $InstanceId) {
    Write-Host "InstanceId required (deployment_credentials.json or -InstanceId)" -ForegroundColor Red
    exit 1
}

$commands = @(
    "sudo chown -R ec2-user:ec2-user /opt/dbt_dental_clinic/dbt_dental_models",
    "sudo mkdir -p /opt/dbt_dental_clinic/dbt_dental_models/logs",
    "sudo chown ec2-user:ec2-user /opt/dbt_dental_clinic/dbt_dental_models/logs",
    "sudo chmod 755 /opt/dbt_dental_clinic/dbt_dental_models/logs",
    "echo 'Permissions set for dbt_dental_models'"
)

$parameters = @{ commands = $commands }
$commandJson = $parameters | ConvertTo-Json -Compress

$response = aws ssm send-command --instance-ids $InstanceId --document-name "AWS-RunShellScript" --parameters $commandJson --output json | ConvertFrom-Json
$commandId = $response.Command.CommandId
Start-Sleep -Seconds 3
$output = aws ssm get-command-invocation --command-id $commandId --instance-id $InstanceId --output json | ConvertFrom-Json

if ($output.StandardOutputContent) { Write-Host $output.StandardOutputContent }
if ($output.StandardErrorContent) { Write-Host $output.StandardErrorContent -ForegroundColor Yellow }
if ($output.Status -ne "Success") {
    Write-Host "Failed: $($output.Status)" -ForegroundColor Red
    exit 1
}
Write-Host "Permissions fixed." -ForegroundColor Green
exit 0
