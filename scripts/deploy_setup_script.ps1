# Deploy setup_ec2_dbt_env.sh to EC2 via SSM
# Usage: .\scripts\deploy_setup_script.ps1
#        .\scripts\deploy_setup_script.ps1 -InstanceId i-xxxxx -ProjectRoot C:\path

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

$setupScriptPath = Join-Path $ProjectRoot "scripts\setup_ec2_dbt_env.sh"
if (-not (Test-Path $setupScriptPath)) {
    Write-Host "setup_ec2_dbt_env.sh not found: $setupScriptPath" -ForegroundColor Red
    exit 1
}

if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        $InstanceId = $credentials.backend_api.ec2.instance_id
    }
}
if (-not $InstanceId) {
    Write-Host "InstanceId required (deployment_credentials.json or -InstanceId)" -ForegroundColor Red
    exit 1
}

# Read and normalize to Unix line endings (LF) so script runs on Linux
$fileContent = [System.IO.File]::ReadAllText($setupScriptPath, [System.Text.Encoding]::UTF8) -replace "`r`n", "`n" -replace "`r", "`n"
$base64Content = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($fileContent))
$remoteFilePath = "/opt/dbt_dental_clinic/scripts/setup_ec2_dbt_env.sh"

$commands = @(
    "REMOTE_FILE='$remoteFilePath'",
    "REMOTE_DIR=`$(dirname `"`$REMOTE_FILE`")",
    "sudo mkdir -p `"`$REMOTE_DIR`"",
    "echo '$base64Content' | base64 -d | sudo tee `"`$REMOTE_FILE`" > /dev/null",
    "sudo chown ec2-user:ec2-user `"`$REMOTE_FILE`"",
    "sudo chmod 755 `"`$REMOTE_FILE`"",
    "if [ -f `"`$REMOTE_FILE`" ]; then echo 'Deployed setup_ec2_dbt_env.sh'; else echo 'Deployment failed'; exit 1; fi"
)

$parameters = @{ commands = $commands }
$commandJson = $parameters | ConvertTo-Json -Compress

$response = aws ssm send-command --instance-ids $InstanceId --document-name "AWS-RunShellScript" --parameters $commandJson --output json | ConvertFrom-Json
$commandId = $response.Command.CommandId
Start-Sleep -Seconds 5
$output = aws ssm get-command-invocation --command-id $commandId --instance-id $InstanceId --output json | ConvertFrom-Json

if ($output.StandardOutputContent) { Write-Host $output.StandardOutputContent }
if ($output.StandardErrorContent) { Write-Host $output.StandardErrorContent -ForegroundColor Yellow }
if ($output.Status -ne "Success") {
    Write-Host "Deploy failed: $($output.Status)" -ForegroundColor Red
    exit 1
}
Write-Host "Setup script deployed." -ForegroundColor Green
exit 0
