# PowerShell Script to Export GLIC Database Table List
# Purpose: Generate comprehensive table list from GLIC OpenDental database for comparison with MDC
# Usage: .\scripts\export_glic_table_list.ps1

# Configuration
$outputDir = "docs\glic_investigation"
$outputFile = "glic_table_list_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
$outputPath = Join-Path $outputDir $outputFile

# Database connection parameters for GLIC
$glicPort = 3306
$glicDatabase = "opendental"
$glicUser = "readonly_user"

Write-Host "GLIC Database Table List Export" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

# Check if mysql client is available
$mysqlCmd = Get-Command mysql -ErrorAction SilentlyContinue
if (-not $mysqlCmd) {
    Write-Host "ERROR: mysql client not found in PATH" -ForegroundColor Red
    Write-Host "Please install MySQL client or add it to your PATH" -ForegroundColor Red
    exit 1
}

# Load configuration from .env_clinic file (clinic GLIC source)
$envFile = Join-Path $PSScriptRoot "..\etl_pipeline\.env_clinic"
$glicHost = $null
$glicPassword = $null
$mdcPassword = $null

if (Test-Path $envFile) {
    Write-Host "Loading configuration from $envFile..." -ForegroundColor Yellow
    $envContent = Get-Content $envFile
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
        
        # Also load MDC password as fallback
        if ($line -match "^\s*OPENDENTAL_SOURCE_PASSWORD=(.*)$") {
            $mdcPassword = $matches[1].Trim()
        }
    }
    
    if ($glicPassword) {
        Write-Host "✓ GLIC password found in .env file" -ForegroundColor Green
    } elseif ($mdcPassword) {
        Write-Host "ℹ GLIC password is empty in .env file, using MDC password" -ForegroundColor Yellow
        $glicPassword = $mdcPassword
    }
}

# Fallback to environment variables
if (-not $glicHost) {
    $glicHost = $env:GLIC_OPENDENTAL_SOURCE_HOST
}
if (-not $glicPassword -and $env:GLIC_OPENDENTAL_SOURCE_PASSWORD) {
    $glicPassword = $env:GLIC_OPENDENTAL_SOURCE_PASSWORD
    Write-Host "✓ Password found in environment variable" -ForegroundColor Green
}

# Validate required configuration
if (-not $glicHost) {
    Write-Host "ERROR: GLIC_OPENDENTAL_SOURCE_HOST not found in .env_clinic or environment variable" -ForegroundColor Red
    Write-Host "Please set it in etl_pipeline\.env_clinic" -ForegroundColor Yellow
    exit 1
}

# Final fallback: exit if no password found
if (-not $glicPassword) {
    Write-Host "ERROR: GLIC password not found in .env file or environment variable" -ForegroundColor Red
    Write-Host "Please add it to etl_pipeline\.env_clinic or set `$env:GLIC_OPENDENTAL_SOURCE_PASSWORD" -ForegroundColor Yellow
    exit 1
}

Write-Host "Host: $glicHost" -ForegroundColor Yellow
Write-Host "Database: $glicDatabase" -ForegroundColor Yellow
Write-Host "Output: $outputPath" -ForegroundColor Yellow
Write-Host ""

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
WHERE TABLE_SCHEMA = '$glicDatabase'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
"@

Write-Host "Connecting to GLIC database..." -ForegroundColor Yellow

try {
    # Execute MySQL query and export to CSV
    # Note: mysql --batch --raw outputs tab-separated values (TSV)
    # DBeaver displays with pipes, but actual output is tabs
    # Try readonly_user first, then fall back to root with no password
    $actualUser = $glicUser
    $connectionSucceeded = $false
    
    # Try readonly_user first (if we have a password)
    if ($glicPassword) {
        $env:MYSQL_PWD = $glicPassword
        $queryOutput = echo $sqlQuery | mysql -h $glicHost -P $glicPort -u $glicUser $glicDatabase --batch --raw 2>&1
        Remove-Item Env:\MYSQL_PWD -ErrorAction SilentlyContinue
        
        if ($LASTEXITCODE -eq 0 -and $queryOutput -notmatch "ERROR|Access denied|Can't connect") {
            $connectionSucceeded = $true
            Write-Host "✓ Connected with readonly_user" -ForegroundColor Green
        } else {
            Write-Host "⚠ readonly_user failed, trying root user with no password..." -ForegroundColor Yellow
        }
    } else {
        # Try readonly_user with no password
        $queryOutput = echo $sqlQuery | mysql -h $glicHost -P $glicPort -u $glicUser $glicDatabase --batch --raw 2>&1
        
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
        $queryOutput = echo $sqlQuery | mysql -h $glicHost -P $glicPort -u root $glicDatabase --batch --raw 2>&1
        
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
    
    # Check for MySQL errors (check both exit code and output content)
    $errorOutput = $queryOutput | Where-Object { $_ -match "ERROR|Access denied|Can't connect" }
    if ($LASTEXITCODE -ne 0 -or $errorOutput) {
        Write-Host "ERROR: Failed to connect to database" -ForegroundColor Red
        if ($errorOutput) {
            Write-Host ($errorOutput | Out-String) -ForegroundColor Red
        } else {
            Write-Host ($queryOutput | Out-String) -ForegroundColor Red
        }
        exit 1
    }
    
    # Convert tab-separated output to CSV
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
    
    Write-Host ""
    Write-Host "Note: CSV file uses comma separators. Display above shows pipes for readability." -ForegroundColor Yellow
    
} catch {
    Write-Host "ERROR: Failed to export table list" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "1. Review the exported CSV file: $outputPath" -ForegroundColor White
Write-Host "2. Compare with MDC table list (run same query against MDC)" -ForegroundColor White
Write-Host "3. Document any differences in GLIC_INTEGRATION_PLAN.md" -ForegroundColor White
