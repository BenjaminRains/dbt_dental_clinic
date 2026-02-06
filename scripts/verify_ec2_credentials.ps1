# PowerShell script to verify credential paths in deployment_credentials.json
# Usage: .\scripts\verify_ec2_credentials.ps1

param(
    [string]$ProjectRoot = ""
)

$ErrorActionPreference = "Stop"

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

Write-Host "`n🔍 Verifying credential paths in deployment_credentials.json" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

$credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"

if (-not (Test-Path $credentialsFile)) {
    Write-Host "❌ deployment_credentials.json not found: $credentialsFile" -ForegroundColor Red
    exit 1
}

try {
    $credentials = Get-Content $credentialsFile | ConvertFrom-Json
    
    # Test the credential path
    $credPath = "backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value"
    Write-Host "`n📋 Testing path: $credPath" -ForegroundColor Yellow
    
    $host = $credentials.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.host
    $port = $credentials.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.port
    $dbname = $credentials.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.dbname
    $username = $credentials.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.username
    $password = $credentials.backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value.password
    $region = $credentials.aws_account.region
    
    Write-Host "`n✅ Credentials extracted successfully:" -ForegroundColor Green
    Write-Host "   POSTGRES_ANALYTICS_HOST: $host" -ForegroundColor Gray
    Write-Host "   POSTGRES_ANALYTICS_PORT: $port" -ForegroundColor Gray
    Write-Host "   POSTGRES_ANALYTICS_DB: $dbname" -ForegroundColor Gray
    Write-Host "   POSTGRES_ANALYTICS_USER: $username" -ForegroundColor Gray
    Write-Host "   POSTGRES_ANALYTICS_PASSWORD: $($password.Substring(0, [Math]::Min(10, $password.Length)))..." -ForegroundColor Gray
    Write-Host "   AWS_DEFAULT_REGION: $region" -ForegroundColor Gray
    
    # Validate all required fields are present
    $missing = @()
    if ([string]::IsNullOrEmpty($host)) { $missing += "host" }
    if ([string]::IsNullOrEmpty($port)) { $missing += "port" }
    if ([string]::IsNullOrEmpty($dbname)) { $missing += "dbname" }
    if ([string]::IsNullOrEmpty($username)) { $missing += "username" }
    if ([string]::IsNullOrEmpty($password)) { $missing += "password" }
    if ([string]::IsNullOrEmpty($region)) { $missing += "aws_account.region" }
    
    if ($missing.Count -gt 0) {
        Write-Host "`n❌ Missing required fields:" -ForegroundColor Red
        foreach ($field in $missing) {
            Write-Host "   - $field" -ForegroundColor Red
        }
        exit 1
    }
    
    Write-Host "`n✅ All credential paths verified successfully!" -ForegroundColor Green
    
} catch {
    Write-Host "`n❌ Error verifying credentials: $_" -ForegroundColor Red
    Write-Host "   Stack trace: $($_.ScriptStackTrace)" -ForegroundColor Yellow
    exit 1
}
