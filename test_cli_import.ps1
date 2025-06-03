# Quick CLI Import Test
# File: test_cli_import.ps1

Write-Host "🔍 Testing CLI Import Issues" -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan

if (-not $script:IsETLInitialized) {
    Write-Host "❌ ETL environment not initialized. Run 'etl-init' first." -ForegroundColor Red
    exit 1
}

Write-Host "📦 Testing individual imports..."

# Test each import step by step
$imports = @(
    "import sys",
    "import click", 
    "from typing import Optional",
    "import yaml",
    "from pathlib import Path",
    "from etl_pipeline.config.logging import setup_logging, configure_sql_logging",
    "from etl_pipeline.core.logger import get_logger, ETLLogger", 
    "from etl_pipeline.config.settings import settings"
)

foreach ($import in $imports) {
    try {
        $result = pipenv run python -c "$import; print('OK')" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✅ $import" -ForegroundColor Green
        } else {
            Write-Host "  ❌ $import" -ForegroundColor Red
            Write-Host "     Error: $result" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  ❌ $import" -ForegroundColor Red
        Write-Host "     Exception: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

Write-Host "`n🧪 Testing full CLI import..."

try {
    $result = pipenv run python -c "
try:
    from etl_pipeline.cli.main import main
    print('CLI import successful')
except ImportError as e:
    print(f'Import error: {e}')
except Exception as e:
    print(f'Other error: {e}')
" 2>&1

    if ($result -match "CLI import successful") {
        Write-Host "✅ CLI import works!" -ForegroundColor Green
    } else {
        Write-Host "❌ CLI import failed:" -ForegroundColor Red
        Write-Host "   $result" -ForegroundColor Yellow
    }
} catch {
    Write-Host "❌ CLI test exception: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`n💡 If imports are failing:"
Write-Host "1. Install missing packages: pipenv install python-dotenv pyyaml psycopg2-binary" -ForegroundColor Cyan
Write-Host "2. Re-run: .\check_dependencies.ps1" -ForegroundColor Cyan
Write-Host "3. Test connections: etl-test-connections" -ForegroundColor Cyan