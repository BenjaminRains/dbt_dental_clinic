# PowerShell Script to Export MDC Database Table List
# Purpose: Generate comprehensive table list from MDC OpenDental database for comparison with GLIC
# Usage: .\scripts\export_mdc_table_list.ps1

# Configuration
$outputDir = "docs\glic_investigation"
$outputFile = "mdc_table_list_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
$outputPath = Join-Path $outputDir $outputFile

# Database connection parameters for MDC
$mdcPort = 3306
$mdcDatabase = "opendental"
$mdcUser = "readonly_user"

Write-Host "MDC Database Table List Export" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

# Check if mysql client is available
$mysqlCmd = Get-Command mysql -ErrorAction SilentlyContinue
if (-not $mysqlCmd) {
    Write-Host "ERROR: mysql client not found in PATH" -ForegroundColor Red
    Write-Host "Please install MySQL client or add it to your PATH" -ForegroundColor Red
    exit 1
}

# Load configuration from .env_production file
$envFilePath = Join-Path $PSScriptRoot "..\etl_pipeline\.env_production"
$mdcHost = $null
$mdcPassword = $null

if (Test-Path $envFilePath) {
    Write-Host "Loading configuration from $envFilePath" -ForegroundColor Yellow
    $envContent = Get-Content $envFilePath
    
    foreach ($line in $envContent) {
        # Skip empty lines
        if ($line -match "^\s*$") { continue }
        
        # Skip regular comments
        if ($line -match "^\s*#") { 
            continue 
        }
        
        # Parse OPENDENTAL_SOURCE_HOST (required)
        if ($line -match "^\s*OPENDENTAL_SOURCE_HOST=(.*)$") {
            $mdcHost = $matches[1].Trim()
        }
        
        # Parse OPENDENTAL_SOURCE_PASSWORD
        if ($line -match "^\s*OPENDENTAL_SOURCE_PASSWORD=(.*)$") {
            $mdcPassword = $matches[1].Trim()
            if ([string]::IsNullOrWhiteSpace($mdcPassword)) {
                $mdcPassword = $null
            }
        }
    }
}

# Fallback to environment variables
if (-not $mdcHost) {
    $mdcHost = $env:OPENDENTAL_SOURCE_HOST
}
if (-not $mdcPassword -and $env:OPENDENTAL_SOURCE_PASSWORD) {
    $mdcPassword = $env:OPENDENTAL_SOURCE_PASSWORD
}

# Validate required configuration
if (-not $mdcHost) {
    Write-Host "ERROR: OPENDENTAL_SOURCE_HOST not found in .env_production or environment variable" -ForegroundColor Red
    Write-Host "Please set it in etl_pipeline\.env_production" -ForegroundColor Yellow
    exit 1
}

Write-Host "Host: $mdcHost" -ForegroundColor Yellow
Write-Host "Database: $mdcDatabase" -ForegroundColor Yellow
Write-Host "Output: $outputPath" -ForegroundColor Yellow
Write-Host ""

# Fallback to environment variable
if (-not $mdcPassword -and $env:OPENDENTAL_SOURCE_PASSWORD) {
    $mdcPassword = $env:OPENDENTAL_SOURCE_PASSWORD
    Write-Host "✓ Password found in environment variable" -ForegroundColor Green
}

# If no password found, we'll try root user with no password
if (-not $mdcPassword) {
    Write-Host "ℹ MDC password not found. Will try root user with no password as fallback." -ForegroundColor Yellow
}

# Create output directory if it doesn't exist
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    Write-Host "Created output directory: $outputDir" -ForegroundColor Green
}

# SQL Query to get table list
$sqlQuery = @"
SELECT 
    TABLE_NAME,
    TABLE_ROWS as approximate_rows,
    ROUND(DATA_LENGTH / 1024 / 1024, 2) as data_size_mb,
    ROUND(INDEX_LENGTH / 1024 / 1024, 2) as index_size_mb,
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as total_size_mb,
    CREATE_TIME,
    UPDATE_TIME,
    TABLE_COLLATION
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = '$mdcDatabase'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
"@

Write-Host "Connecting to MDC database..." -ForegroundColor Yellow

try {
    # Execute MySQL query and export to CSV
    # Try readonly_user first, then fall back to root with no password
    $actualUser = $mdcUser
    $connectionSucceeded = $false
    
    # Try readonly_user first (if we have a password)
    if ($mdcPassword) {
        $env:MYSQL_PWD = $mdcPassword
        $queryOutput = echo $sqlQuery | mysql -h $mdcHost -P $mdcPort -u $mdcUser $mdcDatabase --batch --raw 2>&1
        Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
        
        if ($LASTEXITCODE -eq 0 -and $queryOutput -notmatch "ERROR|Access denied|Can't connect") {
            $connectionSucceeded = $true
            Write-Host "✓ Connected with readonly_user" -ForegroundColor Green
        } else {
            Write-Host "⚠ readonly_user failed, trying root user with no password..." -ForegroundColor Yellow
        }
    } else {
        # Try readonly_user with no password
        $queryOutput = echo $sqlQuery | mysql -h $mdcHost -P $mdcPort -u $mdcUser $mdcDatabase --batch --raw 2>&1
        
        if ($LASTEXITCODE -eq 0 -and $queryOutput -notmatch "ERROR|Access denied|Can't connect") {
            $connectionSucceeded = $true
            Write-Host "✓ Connected with readonly_user (no password)" -ForegroundColor Green
        } else {
            Write-Host "⚠ readonly_user failed, trying root user with no password..." -ForegroundColor Yellow
        }
    }
    
    # Fall back to root user with no password if readonly_user failed
    if (-not $connectionSucceeded) {
        Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue  # Ensure no password is set
        $actualUser = "root"
        $queryOutput = echo $sqlQuery | mysql -h $mdcHost -P $mdcPort -u root $mdcDatabase --batch --raw 2>&1
        
        if ($LASTEXITCODE -eq 0 -and $queryOutput -notmatch "ERROR|Access denied|Can't connect") {
            $connectionSucceeded = $true
            Write-Host "✓ Connected with root user (no password)" -ForegroundColor Green
        }
    }
    
    if (-not $connectionSucceeded) {
        Write-Host "ERROR: Failed to connect to database with both users" -ForegroundColor Red
        Write-Host $queryOutput -ForegroundColor Red
        exit 1
    }
    
    # Filter out ErrorRecord objects and convert to strings
    $validLines = @()
    foreach ($line in $queryOutput) {
        if ($line -is [string]) {
            $validLines += $line
        } elseif ($line -is [System.Management.Automation.ErrorRecord]) {
            # Skip error records - we've already checked for errors above
            continue
        } else {
            # Convert other types to string
            $validLines += $line.ToString()
        }
    }
    
    # Convert tab-separated output to CSV
    $csvLines = @()
    $headerProcessed = $false
    
    # Process each valid line
    foreach ($line in $validLines) {
        $trimmedLine = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmedLine)) { continue }  # Skip empty lines
        
        # Check for header row (first non-empty line should be the header)
        if (-not $headerProcessed) {
            # Header row - convert tabs to commas
            $header = $trimmedLine -replace "`t", ","
            $csvLines += $header
            $headerProcessed = $true
            continue
        }
        
        # Data rows - convert tabs to commas
        # Handle NULL values: replace NULL with empty string for CSV
        $csvLine = $trimmedLine -replace "`t", ","
        # Replace NULL (word boundary) with empty string
        $csvLine = $csvLine -replace "`bNULL`b", ""
        # Also handle tab-separated NULL values
        $csvLine = $csvLine -replace ",NULL,", ",,"
        $csvLine = $csvLine -replace "^NULL,", ","
        $csvLine = $csvLine -replace ",NULL$", ","
        
        $csvLines += $csvLine
    }
    
    # Write to file with UTF-8 encoding (no BOM)
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllLines($outputPath, $csvLines, $utf8NoBom)
    
    Write-Host ""
    Write-Host "SUCCESS: Table list exported to:" -ForegroundColor Green
    Write-Host $outputPath -ForegroundColor White
    Write-Host ""
    
    # Display summary
    $rowCount = ($csvLines | Measure-Object).Count - 1  # Subtract header
    Write-Host "Summary:" -ForegroundColor Cyan
    Write-Host "  Total tables: $rowCount" -ForegroundColor White
    Write-Host ""
    
    # Display first 10 tables as preview
    Write-Host "Preview (first 10 tables):" -ForegroundColor Cyan
    $previewLines = $csvLines | Select-Object -First 11
    foreach ($line in $previewLines) {
        # Display with pipes for readability (like DBeaver)
        $displayLine = $line -replace ",", " | "
        Write-Host "  $displayLine" -ForegroundColor Gray
    }
    
    if ($rowCount -gt 10) {
        Write-Host "  ... and $($rowCount - 10) more" -ForegroundColor Gray
    }
    
    # Calculate total size
    $totalSize = 0
    $csvLines | Select-Object -Skip 1 | ForEach-Object {
        $fields = $_ -split ","
        if ($fields.Length -gt 4) {
            $sizeStr = $fields[4]
            if ($sizeStr -match "^[\d.]+$") {
                $totalSize += [double]$sizeStr
            }
        }
    }
    
    Write-Host ""
    Write-Host "Total database size: $([math]::Round($totalSize, 2)) MB" -ForegroundColor Cyan
    Write-Host "Note: CSV file uses comma separators. Display above shows pipes for readability." -ForegroundColor Yellow
    
} catch {
    Write-Host "ERROR: Failed to export table list" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "1. Review the exported CSV file: $outputPath" -ForegroundColor White
Write-Host "2. Compare with GLIC table list (run export_glic_table_list.ps1)" -ForegroundColor White
Write-Host "3. Document any differences in GLIC_INTEGRATION_PLAN.md" -ForegroundColor White

Write-Host "2. Compare with GLIC table list (run export_glic_table_list.ps1)" -ForegroundColor White
Write-Host "3. Document any differences in GLIC_INTEGRATION_PLAN.md" -ForegroundColor White
