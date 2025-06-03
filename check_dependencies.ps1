# ETL Pipeline Dependency Check Script
# File: check_dependencies.ps1

param(
    [switch]$Install,
    [switch]$Verbose
)

Write-Host "🔍 ETL Pipeline Dependency Check" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan

# Check if ETL environment is initialized
if (-not $script:IsETLInitialized) {
    Write-Host "⚠️  ETL environment not initialized. Run 'etl-init' first." -ForegroundColor Yellow
    exit 1
}

# Core Python packages needed for dental clinic ETL
$requiredPackages = @{
    'click' = 'CLI framework'
    'pyyaml' = 'YAML configuration files'
    'python-dotenv' = 'Environment variable loading'
    'sqlalchemy' = 'Database ORM'
    'pymysql' = 'MySQL connector for MariaDB'
    'psycopg2-binary' = 'PostgreSQL connector'
    'pandas' = 'Data manipulation'
    'numpy' = 'Numerical computing'
    'requests' = 'HTTP requests'
    'cryptography' = 'Encryption for secure connections'
}

# Optional packages for advanced features
$optionalPackages = @{
    'great-expectations' = 'Data quality validation'
    'airflow' = 'Workflow orchestration'
    'prefect' = 'Modern workflow orchestration'
    'dbt-core' = 'Data transformation'
    'jupyter' = 'Interactive development'
    'matplotlib' = 'Data visualization'
    'seaborn' = 'Statistical visualization'
}

function Test-PythonPackage {
    param(
        [string]$PackageName,
        [string]$Description
    )
    
    try {
        $result = pipenv run python -c "import $($PackageName.Replace('-', '_')); print('OK')" 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✅ $PackageName" -ForegroundColor Green
            if ($Verbose) {
                Write-Host "     $Description" -ForegroundColor Gray
            }
            return $true
        } else {
            Write-Host "  ❌ $PackageName - Not installed" -ForegroundColor Red
            if ($Verbose) {
                Write-Host "     $Description" -ForegroundColor Gray
            }
            return $false
        }
    } catch {
        Write-Host "  ❌ $PackageName - Import failed" -ForegroundColor Red
        return $false
    }
}

function Install-MissingPackages {
    param(
        [hashtable]$Packages,
        [string]$Category
    )
    
    Write-Host "`n📦 Installing missing $Category packages..." -ForegroundColor Yellow
    
    $missingPackages = @()
    foreach ($package in $Packages.Keys) {
        if (-not (Test-PythonPackage $package $Packages[$package])) {
            $missingPackages += $package
        }
    }
    
    if ($missingPackages.Count -gt 0) {
        $packageList = $missingPackages -join " "
        Write-Host "Installing: $packageList" -ForegroundColor Cyan
        
        try {
            pipenv install $packageList
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ Successfully installed $Category packages" -ForegroundColor Green
            } else {
                Write-Host "⚠️  Some packages may have failed to install" -ForegroundColor Yellow
            }
        } catch {
            Write-Host "❌ Failed to install packages: $($_.Exception.Message)" -ForegroundColor Red
        }
    } else {
        Write-Host "✅ All $Category packages already installed" -ForegroundColor Green
    }
}

# Check Python version
Write-Host "`n🐍 Python Environment Check"
Write-Host "============================"

try {
    $pythonVersion = pipenv run python --version
    Write-Host "✅ Python version: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python not accessible through pipenv" -ForegroundColor Red
    exit 1
}

# Check virtual environment
try {
    $venvPath = pipenv --venv 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Virtual environment: $venvPath" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Virtual environment path not found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠️  Could not determine virtual environment status" -ForegroundColor Yellow
}

# Test core packages
Write-Host "`n📚 Core Package Check"
Write-Host "====================="

$coreInstalled = 0
$coreTotal = $requiredPackages.Count

foreach ($package in $requiredPackages.Keys) {
    if (Test-PythonPackage $package $requiredPackages[$package]) {
        $coreInstalled++
    }
}

Write-Host "`nCore packages: $coreInstalled/$coreTotal installed" -ForegroundColor $(if ($coreInstalled -eq $coreTotal) { "Green" } else { "Yellow" })

# Test optional packages
Write-Host "`n🎁 Optional Package Check"
Write-Host "========================="

$optionalInstalled = 0
$optionalTotal = $optionalPackages.Count

foreach ($package in $optionalPackages.Keys) {
    if (Test-PythonPackage $package $optionalPackages[$package]) {
        $optionalInstalled++
    }
}

Write-Host "`nOptional packages: $optionalInstalled/$optionalTotal installed" -ForegroundColor Cyan

# Install missing packages if requested
if ($Install) {
    if ($coreInstalled -lt $coreTotal) {
        Install-MissingPackages $requiredPackages "core"
    }
    
    $installOptional = Read-Host "`nInstall missing optional packages? (y/n)"
    if ($installOptional -eq "y") {
        Install-MissingPackages $optionalPackages "optional"
    }
}

# Test ETL-specific imports
Write-Host "`n🏥 ETL Pipeline Module Check"
Write-Host "============================"

$etlModules = @(
    'etl_pipeline.config.logging',
    'etl_pipeline.core.logger',
    'etl_pipeline.config.settings',
    'etl_pipeline.cli.main'
)

$modulesPassed = 0
foreach ($module in $etlModules) {
    try {
        $result = pipenv run python -c "import $module; print('OK')" 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✅ $module" -ForegroundColor Green
            $modulesPassed++
        } else {
            Write-Host "  ❌ $module - Import failed" -ForegroundColor Red
        }
    } catch {
        Write-Host "  ❌ $module - Exception" -ForegroundColor Red
    }
}

Write-Host "`nETL modules: $modulesPassed/$($etlModules.Count) working" -ForegroundColor $(if ($modulesPassed -eq $etlModules.Count) { "Green" } else { "Yellow" })

# Summary and recommendations
Write-Host "`n📊 Dependency Check Summary"
Write-Host "============================"

if ($coreInstalled -eq $coreTotal -and $modulesPassed -eq $etlModules.Count) {
    Write-Host "🎉 All core dependencies are working!" -ForegroundColor Green
    Write-Host "✅ Your dental clinic ETL pipeline is ready to use" -ForegroundColor Green
} else {
    Write-Host "⚠️  Some issues found:" -ForegroundColor Yellow
    
    if ($coreInstalled -lt $coreTotal) {
        Write-Host "  📦 Missing core packages: $($coreTotal - $coreInstalled)" -ForegroundColor Red
        Write-Host "     Run: .\check_dependencies.ps1 -Install" -ForegroundColor Cyan
    }
    
    if ($modulesPassed -lt $etlModules.Count) {
        Write-Host "  🐍 ETL module import issues: $($etlModules.Count - $modulesPassed)" -ForegroundColor Red
        Write-Host "     Check Python path and module structure" -ForegroundColor Cyan
    }
}

Write-Host "`n💡 Next Steps:"
Write-Host "1. If packages are missing: .\check_dependencies.ps1 -Install" -ForegroundColor Cyan
Write-Host "2. Test database connections: etl-test-connections" -ForegroundColor Cyan
Write-Host "3. Validate configuration: etl config --action validate" -ForegroundColor Cyan
Write-Host "4. Run a dry-run pipeline: etl run --full --dry-run" -ForegroundColor Cyan