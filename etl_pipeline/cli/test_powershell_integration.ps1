# PowerShell Integration Test Script for ETL Pipeline CLI
# File: etl_pipeline/cli/test_powershell_integration.ps1

Write-Host "🧪 Testing PowerShell CLI Integration" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan

# Test 1: Environment initialization
Write-Host "`n1️⃣  Testing ETL environment initialization..."
try {
    etl-init
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ ETL environment initialized" -ForegroundColor Green
    } else {
        Write-Host "❌ ETL environment initialization failed" -ForegroundColor Red
    }
} catch {
    Write-Host "❌ Exception during initialization: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 2: Help command
Write-Host "`n2️⃣  Testing CLI help..."
try {
    etl --help
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ CLI help works" -ForegroundColor Green
    } else {
        Write-Host "❌ CLI help failed" -ForegroundColor Red
    }
} catch {
    Write-Host "❌ Exception during help: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 3: Status command
Write-Host "`n3️⃣  Testing status command..."
try {
    etl-status --format summary
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Status command works" -ForegroundColor Green
    } else {
        Write-Host "❌ Status command failed" -ForegroundColor Red
    }
} catch {
    Write-Host "❌ Exception during status: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 4: Dry run commands
Write-Host "`n4️⃣  Testing dry run commands..."
$dryRunCommands = @(
    "etl run --full --dry-run",
    "etl-validate --table patient --dry-run", 
    "etl-patient-sync --dry-run",
    "etl-appointment-metrics --dry-run",
    "etl-compliance-check --dry-run"
)

foreach ($cmd in $dryRunCommands) {
    Write-Host "   Testing: $cmd" -ForegroundColor Yellow
    try {
        Invoke-Expression $cmd
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Success" -ForegroundColor Green
        } else {
            Write-Host "   ❌ Failed" -ForegroundColor Red
        }
    } catch {
        Write-Host "   ❌ Exception: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Test 5: Quick commands
Write-Host "`n5️⃣  Testing quick commands..."
try {
    etl-quick-status
    Write-Host "✅ Quick status works" -ForegroundColor Green
} catch {
    Write-Host "❌ Quick status failed: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`n🎉 PowerShell integration testing completed!" -ForegroundColor Green
Write-Host "Run 'etl-deactivate' to clean up." -ForegroundColor Cyan 