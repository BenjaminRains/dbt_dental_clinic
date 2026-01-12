# PowerShell Script to Test GLIC Database Connection
# Purpose: Simple connection test before running full table list export
# Usage: .\scripts\test_glic_connection.ps1

# Database connection parameters for GLIC
$glicPort = 3306
$glicDatabase = "opendental"
$glicUser = "readonly_user"  # Try readonly_user first
$glicRootUser = "root"  # Fallback to root if readonly_user fails

Write-Host "GLIC Database Connection Test" -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan
Write-Host ""

# Check if mysql client is available
$mysqlCmd = Get-Command mysql -ErrorAction SilentlyContinue
if (-not $mysqlCmd) {
    Write-Host "ERROR: mysql client not found in PATH" -ForegroundColor Red
    Write-Host "Please install MySQL client or add it to your PATH" -ForegroundColor Red
    exit 1
}

Write-Host "✓ MySQL client found: $($mysqlCmd.Source)" -ForegroundColor Green
Write-Host ""

# Load configuration from .env_production file
$envFilePath = Join-Path $PSScriptRoot "..\etl_pipeline\.env_production"
$glicHost = $null
$glicPassword = $null
$mdcPassword = $null

if (Test-Path $envFilePath) {
    Write-Host "Loading configuration from: $envFilePath" -ForegroundColor Yellow
    $envContent = Get-Content $envFilePath
    
    foreach ($line in $envContent) {
        # Skip empty lines
        if ($line -match "^\s*$") { continue }
        
        # Skip regular comments
        if ($line -match "^\s*#") { 
            continue 
        }
        
        # Parse GLIC_OPENDENTAL_SOURCE_HOST (required)
        if ($line -match "^\s*GLIC_OPENDENTAL_SOURCE_HOST=(.*)$") {
            $glicHost = $matches[1].Trim()
        }
        
        # Parse GLIC_OPENDENTAL_SOURCE_PASSWORD
        if ($line -match "^\s*GLIC_OPENDENTAL_SOURCE_PASSWORD=(.*)$") {
            $glicPassword = $matches[1].Trim()
            if ([string]::IsNullOrWhiteSpace($glicPassword)) {
                $glicPassword = $null
            }
        }
        
        # Also load MDC password in case GLIC uses the same
        if ($line -match "^\s*OPENDENTAL_SOURCE_PASSWORD=(.*)$") {
            $mdcPassword = $matches[1].Trim()
        }
    }
}

# Fallback to environment variables
if (-not $glicHost) {
    $glicHost = $env:GLIC_OPENDENTAL_SOURCE_HOST
}
if (-not $glicPassword -and $env:GLIC_OPENDENTAL_SOURCE_PASSWORD) {
    $glicPassword = $env:GLIC_OPENDENTAL_SOURCE_PASSWORD
    Write-Host "✓ GLIC password found in environment variable" -ForegroundColor Green
}

# If still no password, try MDC password automatically (they're on same server)
if (-not $glicPassword -and $mdcPassword) {
    Write-Host "ℹ GLIC password is empty, trying MDC password" -ForegroundColor Yellow
    $glicPassword = $mdcPassword
}

# Validate required configuration
if (-not $glicHost) {
    Write-Host "ERROR: GLIC_OPENDENTAL_SOURCE_HOST not found in .env_production or environment variable" -ForegroundColor Red
    Write-Host "Please set it in etl_pipeline\.env_production" -ForegroundColor Yellow
    exit 1
}

# If no password found, we'll try root user with no password
$useRootUser = [string]::IsNullOrWhiteSpace($glicPassword)
if ($useRootUser) {
    Write-Host "ℹ No password found. Will try root user with no password as fallback." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Host: $glicHost" -ForegroundColor Yellow
Write-Host "Port: $glicPort" -ForegroundColor Yellow
Write-Host "Database: $glicDatabase" -ForegroundColor Yellow

# Determine which user to try
$actualUser = $glicUser
if ($useRootUser) {
    $actualUser = $glicRootUser
    Write-Host "User: $actualUser (root - no password)" -ForegroundColor Yellow
} else {
    Write-Host "User: $actualUser" -ForegroundColor Yellow
    Write-Host "Password: ***" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "Testing connection..." -ForegroundColor Yellow
Write-Host ""

# Helper function to execute MySQL query with automatic fallback to root
function Invoke-MySQLQuery {
    param(
        [string]$Query,
        [string]$User = $glicUser,
        [string]$Password = $glicPassword
    )
    
    # Try with provided user first
    if ($Password) {
        $env:MYSQL_PWD = $Password
        $output = echo $Query | mysql -h $glicHost -P $glicPort -u $User $glicDatabase --batch --raw 2>&1
        Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
    } else {
        $output = echo $Query | mysql -h $glicHost -P $glicPort -u $User $glicDatabase --batch --raw 2>&1
    }
    
    # If failed, try root with no password (only if wasn't already root)
    if ($LASTEXITCODE -ne 0 -or $output -match "ERROR|Access denied|Can't connect") {
        if ($User -ne "root") {
            Write-Host "  Trying root user with no password..." -ForegroundColor Yellow
            $output = echo $Query | mysql -h $glicHost -P $glicPort -u root $glicDatabase --batch --raw 2>&1
            if ($LASTEXITCODE -eq 0 -and $output -notmatch "ERROR|Access denied|Can't connect") {
                $script:actualUser = "root"
            }
        }
    } else {
        $script:actualUser = $User
    }
    
    return $output
}

$actualUser = $glicUser  # Track which user actually worked
Write-Host ""

# Test 1: Simple SELECT query - Get database version
Write-Host "Test 1: Getting MySQL version..." -ForegroundColor Cyan
try {
    $versionQuery = "SELECT VERSION() as mysql_version;"
    $connectionSucceeded = $false
    $actualUser = $glicUser
    $versionOutput = $null
    
    # Try readonly_user first (if we have a password or empty password)
    if (-not $useRootUser) {
        if ($glicPassword) {
            $env:MYSQL_PWD = $glicPassword
            $versionOutput = echo $versionQuery | mysql -h $glicHost -P $glicPort -u $glicUser $glicDatabase --batch --raw 2>&1
            Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
        } else {
            # Try readonly_user with no password
            $versionOutput = echo $versionQuery | mysql -h $glicHost -P $glicPort -u $glicUser $glicDatabase --batch --raw 2>&1
        }
        
        if ($LASTEXITCODE -eq 0 -and $versionOutput -notmatch "ERROR|Access denied|Can't connect") {
            $connectionSucceeded = $true
            Write-Host "✓ Connected with readonly_user" -ForegroundColor Green
        } else {
            Write-Host "⚠ readonly_user failed, trying root user with no password..." -ForegroundColor Yellow
        }
    }
    
    # Try root user with no password if readonly_user failed
    if (-not $connectionSucceeded) {
        $actualUser = "root"
        $versionOutput = echo $versionQuery | mysql -h $glicHost -P $glicPort -u root $glicDatabase --batch --raw 2>&1
        
        if ($LASTEXITCODE -eq 0 -and $versionOutput -notmatch "ERROR|Access denied|Can't connect") {
            $connectionSucceeded = $true
            Write-Host "✓ Connected with root user (no password)" -ForegroundColor Green
        }
    }
    
    if (-not $connectionSucceeded) {
        Write-Host "✗ Connection failed with both users!" -ForegroundColor Red
        Write-Host $versionOutput -ForegroundColor Red
        exit 1
    }
    
    $version = ($versionOutput | Where-Object { $_ -match "^[\d.]+" } | Select-Object -First 1).Trim()
    Write-Host "  MySQL Version: $version" -ForegroundColor White
    Write-Host "  Connected as: $actualUser" -ForegroundColor White
    Write-Host ""
    
} catch {
    Write-Host "✗ Connection failed with exception!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Test 2: Count tables
Write-Host "Test 2: Counting tables..." -ForegroundColor Cyan
try {
    $countQuery = "SELECT COUNT(*) as table_count FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$glicDatabase' AND TABLE_TYPE = 'BASE TABLE';"
    $countOutput = Invoke-MySQLQuery -Query $countQuery
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Query failed!" -ForegroundColor Red
        Write-Host $countOutput -ForegroundColor Red
        exit 1
    }
    
    $tableCount = ($countOutput | Where-Object { $_ -match "^\d+$" } | Select-Object -First 1).Trim()
    Write-Host "✓ Query successful!" -ForegroundColor Green
    Write-Host "  Total tables: $tableCount" -ForegroundColor White
    Write-Host ""
    
} catch {
    Write-Host "✗ Query failed with exception!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Test 3: Get patient count
Write-Host "Test 3: Getting patient count..." -ForegroundColor Cyan
try {
    $patientQuery = "SELECT COUNT(*) as patient_count FROM patient;"
    $patientOutput = Invoke-MySQLQuery -Query $patientQuery
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Query failed!" -ForegroundColor Red
        Write-Host $patientOutput -ForegroundColor Red
        exit 1
    }
    
    $patientCount = ($patientOutput | Where-Object { $_ -match "^\d+$" } | Select-Object -First 1).Trim()
    Write-Host "✓ Query successful!" -ForegroundColor Green
    Write-Host "  Patient count: $patientCount" -ForegroundColor White
    Write-Host ""
    
} catch {
    Write-Host "✗ Query failed with exception!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Test 4: Get sample table list (first 10 tables)
Write-Host "Test 4: Getting sample table list (first 10)..." -ForegroundColor Cyan
try {
    # Ensure MYSQL_PWD is cleared before this test (in case it was set earlier)
    Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
    
    $sampleQuery = @"
SELECT 
    TABLE_NAME,
    TABLE_ROWS as approximate_rows,
    ROUND(DATA_LENGTH / 1024 / 1024, 2) as data_size_mb,
    ROUND(INDEX_LENGTH / 1024 / 1024, 2) as index_size_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = '$glicDatabase'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME
LIMIT 10;
"@
    
    # Use the same user that worked in previous tests
    # If root worked (no password), use root with no password. Otherwise use readonly_user.
    if ($actualUser -eq "root") {
        # Root user - no password (don't set MYSQL_PWD)
        $sampleOutput = echo $sampleQuery | mysql -h $glicHost -P $glicPort -u root $glicDatabase --batch --raw 2>&1
    } elseif ($glicPassword) {
        # readonly_user with password
        $env:MYSQL_PWD = $glicPassword
        $sampleOutput = echo $sampleQuery | mysql -h $glicHost -P $glicPort -u $actualUser $glicDatabase --batch --raw 2>&1
        Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
    } else {
        # readonly_user without password
        $sampleOutput = echo $sampleQuery | mysql -h $glicHost -P $glicPort -u $actualUser $glicDatabase --batch --raw 2>&1
    }
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Query failed!" -ForegroundColor Red
        Write-Host $sampleOutput -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✓ Query successful!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Sample output (first 10 tables):" -ForegroundColor Cyan
    Write-Host "--------------------------------" -ForegroundColor Cyan
    
    # Display the output - MySQL --batch --raw outputs tab-separated
    $sampleOutput | ForEach-Object {
        if ($_ -match "^\s*$") { return }  # Skip empty lines
        # Replace tabs with pipes for display (like DBeaver)
        $displayLine = $_ -replace "`t", " | "
        Write-Host $displayLine -ForegroundColor White
    }
    
    Write-Host ""
    
} catch {
    Write-Host "✗ Query failed with exception!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Write-Host "========================================" -ForegroundColor Green
Write-Host "All connection tests passed!" -ForegroundColor Green
Write-Host "You can now run the full export script:" -ForegroundColor Yellow
Write-Host "  .\scripts\export_glic_table_list.ps1" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Green
