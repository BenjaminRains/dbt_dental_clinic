<#
.SYNOPSIS
    Verify clinic portal /auth/login on EC2 via SSM (localhost curl).
#>

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}

$credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
$credentials = Get-Content -LiteralPath $credentialsFile -Raw | ConvertFrom-Json
$idToUse = if ($InstanceId) { $InstanceId } else { [string]$credentials.backend_api.clinic_api.ec2.instance_id }

$usersFile = Join-Path $ProjectRoot "api\clinic-portal-users.json"
if (-not (Test-Path -LiteralPath $usersFile)) {
    Write-Host "Missing $usersFile — cannot verify portal login." -ForegroundColor Red
    exit 1
}
$portalUsers = Get-Content -LiteralPath $usersFile -Raw | ConvertFrom-Json
$manager = $portalUsers | Where-Object { $_.username -eq "manager" } | Select-Object -First 1
if (-not $manager) {
    Write-Host "manager user not found in clinic-portal-users.json" -ForegroundColor Red
    exit 1
}
$loginPayload = (@{ username = $manager.username; password = $manager.password } | ConvertTo-Json -Compress)
$loginPayloadBash = $loginPayload -replace "'", "'\''"

# Single-line SSH/SSM commands — avoid CRLF (Windows here-strings break bash as echo\r)
$commands = @(
    "HTTP=`$(curl -s -o /tmp/portal_login.json -w '%{http_code}' -X POST http://127.0.0.1:8000/auth/login -H 'Content-Type: application/json' -d '$loginPayloadBash'); echo login_http=`$HTTP; head -c 300 /tmp/portal_login.json 2>/dev/null; echo; test `"`$HTTP`" = `"200`""
)

$body = @{
    InstanceIds  = @($idToUse)
    DocumentName = "AWS-RunShellScript"
    Parameters   = @{ commands = $commands }
}
$tmp = Join-Path $env:TEMP ("ssm-portal-verify-" + [Guid]::NewGuid().ToString() + ".json")
$json = ($body | ConvertTo-Json -Depth 6) -replace "`r`n", "`n" -replace "`r", "`n"
[System.IO.File]::WriteAllText($tmp, $json, [System.Text.UTF8Encoding]::new($false))
$fileUrl = "file://" + ((Resolve-Path -LiteralPath $tmp).Path -replace '\\', '/')

$result = aws ssm send-command --cli-input-json $fileUrl --region us-east-1 --output json | ConvertFrom-Json
Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
$cmdId = $result.Command.CommandId
Start-Sleep -Seconds 10
$inv = aws ssm get-command-invocation --command-id $cmdId --instance-id $idToUse --region us-east-1 --output json | ConvertFrom-Json
Write-Host $inv.StandardOutputContent
if ($inv.StandardErrorContent) { Write-Host $inv.StandardErrorContent -ForegroundColor Yellow }
if ($inv.Status -ne "Success") {
    Write-Host "Portal auth check failed: $($inv.Status)" -ForegroundColor Red
    exit 1
}
Write-Host "Portal login OK on instance." -ForegroundColor Green
