# Search CloudTrail for Secrets Manager GetSecretValue calls (last 90 days in Event history).
#
# Requires: cloudtrail:LookupEvents
#
# Usage:
#   .\scripts\aws\lookup_secrets_manager_access.ps1
#   .\scripts\aws\lookup_secrets_manager_access.ps1 -SecretName "rds!db-83a24c7f-7e85-4168-ba14-ad6e63905c49" -MaxResults 100

param(
    [string]$SecretName = "rds!db-83a24c7f-7e85-4168-ba14-ad6e63905c49",
    [string]$Region = "us-east-1",
    [int]$MaxResults = 50
)

$ErrorActionPreference = "Stop"

Write-Host "`nCloudTrail lookup: GetSecretValue" -ForegroundColor Cyan
Write-Host "  Secret filter: $SecretName"
Write-Host "  Region:        $Region"
Write-Host "  Note: Event history covers ~90 days; new trails need a few minutes before events appear.`n"

$eventsJson = aws cloudtrail lookup-events `
    --lookup-attributes AttributeKey=EventName,AttributeValue=GetSecretValue `
    --region $Region `
    --max-results $MaxResults `
    --output json 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host $eventsJson -ForegroundColor Red
    Write-Host "`nIf AccessDenied: ask admin to attach scripts/iam/cloudtrail_lookup_readonly_policy.json" -ForegroundColor Yellow
    Write-Host "If trail missing: run scripts/aws/setup_cloudtrail.ps1 as admin (see docs/deployment/CLOUDTRAIL_SETUP.md)" -ForegroundColor Yellow
    exit 1
}

$data = $eventsJson | ConvertFrom-Json
$matches = @()

foreach ($event in ($data.Events | ForEach-Object { $_ })) {
    $record = $event.CloudTrailEvent | ConvertFrom-Json
    $secretId = $record.requestParameters.secretId
    if (-not $secretId) { continue }
    if ($secretId -notlike "*$SecretName*") { continue }
    $matches += [PSCustomObject]@{
        Time       = $event.EventTime
        User       = $record.userIdentity.arn
        SecretId   = $secretId
        SourceIP   = $record.sourceIPAddress
    }
}

if ($matches.Count -eq 0) {
    Write-Host "No GetSecretValue events found for secret name containing '$SecretName' in the last batch ($MaxResults events scanned)." -ForegroundColor Yellow
    Write-Host "Try increasing -MaxResults or wait if the trail was just created." -ForegroundColor DarkGray
    exit 0
}

$matches | Sort-Object Time -Descending | Format-Table -AutoSize
Write-Host "Matched $($matches.Count) event(s)." -ForegroundColor Green
exit 0
