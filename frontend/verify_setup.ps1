# PowerShell script to verify frontend-dev setup for opendental_analytics
# This checks that all required configuration is in place

$projectRoot = Split-Path -Parent $PSScriptRoot
$pemFile = "$projectRoot\.ssh\dbt-dental-clinic-api-key.pem"
$envLocalFile = "$PSScriptRoot\.env.local"
$apiEnvFile = "$projectRoot\api\.env_api_local"

Write-Host "`n🔍 Verifying frontend-dev setup for opendental_analytics..." -ForegroundColor Cyan
Write-Host ""

$allGood = $true

# Check 1: API key file exists
Write-Host "1. Checking API key file..." -ForegroundColor Yellow
if (Test-Path $pemFile) {
    Write-Host "   ✅ API key file found: $pemFile" -ForegroundColor Green
} else {
    Write-Host "   ❌ API key file NOT found: $pemFile" -ForegroundColor Red
    Write-Host "      Run: frontend\setup_api_key.ps1" -ForegroundColor Yellow
    $allGood = $false
}

# Check 2: Frontend .env.local exists
Write-Host "`n2. Checking frontend .env.local file..." -ForegroundColor Yellow
if (Test-Path $envLocalFile) {
    Write-Host "   ✅ .env.local file found: $envLocalFile" -ForegroundColor Green
    $envContent = Get-Content $envLocalFile -Raw
    if ($envContent -match "VITE_API_KEY=") {
        Write-Host "   ✅ VITE_API_KEY is set" -ForegroundColor Green
    } else {
        Write-Host "   ❌ VITE_API_KEY is NOT set in .env.local" -ForegroundColor Red
        Write-Host "      Run: frontend\setup_api_key.ps1" -ForegroundColor Yellow
        $allGood = $false
    }
    if ($envContent -match "VITE_API_URL=http://localhost:8000") {
        Write-Host "   ✅ VITE_API_URL is set to http://localhost:8000" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️  VITE_API_URL may not be set correctly" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ❌ .env.local file NOT found: $envLocalFile" -ForegroundColor Red
    Write-Host "      Run: frontend\setup_api_key.ps1" -ForegroundColor Yellow
    $allGood = $false
}

# Check 3: API environment file exists
Write-Host "`n3. Checking API environment configuration..." -ForegroundColor Yellow
if (Test-Path $apiEnvFile) {
    Write-Host "   ✅ API .env_api_local file found: $apiEnvFile" -ForegroundColor Green
    $apiEnvContent = Get-Content $apiEnvFile -Raw
    if ($apiEnvContent -match "API_ENVIRONMENT=local") {
        Write-Host "   ✅ API_ENVIRONMENT=local is set" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️  API_ENVIRONMENT may not be set to 'local'" -ForegroundColor Yellow
    }
    if ($apiEnvContent -match "POSTGRES_ANALYTICS_DB=opendental_analytics") {
        Write-Host "   ✅ Database is set to opendental_analytics" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️  Database may not be set to opendental_analytics" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ⚠️  API .env_api_local file NOT found: $apiEnvFile" -ForegroundColor Yellow
    Write-Host "      This is OK if API_ENVIRONMENT is set via environment variable" -ForegroundColor Gray
}

# Check 4: API server status
Write-Host "`n4. Checking if API server is running..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/docs" -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
    if ($response.StatusCode -eq 200) {
        Write-Host "   ✅ API server is running on http://localhost:8000" -ForegroundColor Green
    }
} catch {
    Write-Host "   ❌ API server is NOT running on http://localhost:8000" -ForegroundColor Red
    Write-Host "      Start API with: api-run" -ForegroundColor Yellow
    Write-Host "      Make sure API_ENVIRONMENT=local is set" -ForegroundColor Yellow
    $allGood = $false
}

# Summary
Write-Host "`n" + ("=" * 60) -ForegroundColor Gray
if ($allGood) {
    Write-Host "✅ All checks passed! Frontend-dev is ready for opendental_analytics." -ForegroundColor Green
    Write-Host "`nNext steps:" -ForegroundColor Cyan
    Write-Host "   1. Make sure API is running: api-run" -ForegroundColor White
    Write-Host "   2. Start frontend: frontend-dev" -ForegroundColor White
    Write-Host "   3. Open browser: http://localhost:3000" -ForegroundColor White
} else {
    Write-Host "❌ Some checks failed. Please fix the issues above." -ForegroundColor Red
    Write-Host "`nQuick fix:" -ForegroundColor Cyan
    Write-Host "   Run: frontend\setup_api_key.ps1" -ForegroundColor White
}
Write-Host ""
