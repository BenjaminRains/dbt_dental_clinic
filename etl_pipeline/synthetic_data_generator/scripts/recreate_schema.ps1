#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Recreate opendental_demo.raw schema from production opendental_analytics.raw schema
    
.DESCRIPTION
    Safely recreates the raw schema in opendental_demo database by:
    1. Dropping existing raw schema (CASCADE - removes all tables and data)
    2. Recreating the raw schema
    3. Extracting current schema structure from production (NO DATA)
    4. Applying fresh schema to demo database
    5. Creating additional schemas for dbt
    
    This ensures the demo database has the latest schema structure matching production.
    
    SAFETY MEASURES:
    - Production database is READ-ONLY (pg_dump only reads, never writes)
    - Demo database name is HARDCODED to "opendental_demo" (cannot be changed)
    - Script verifies production and demo databases are different before proceeding
    - Explicit confirmation required before any destructive operations
    - Clear warnings about what will be modified
    
.PARAMETER ProductionDbHost
    Production database host (default: localhost)
    
.PARAMETER ProductionDbUser
    Production database user (default: postgres)
    
.PARAMETER ProductionDbPassword
    Production database password (required)
    
.PARAMETER ProductionDbName
    Production database name (default: opendental_analytics)
    
.PARAMETER DemoDbHost
    Demo database host (default: localhost)
    
.PARAMETER DemoDbUser
    Demo database user (default: postgres)
    
.PARAMETER DemoDbPassword
    Demo database password (required)
    
.PARAMETER SkipConfirmation
    Skip confirmation prompt (use with caution)

.EXAMPLE
    .\recreate_schema.ps1 -ProductionDbPassword "prodpass" -DemoDbPassword "demopass"
    
.EXAMPLE
    .\recreate_schema.ps1 -ProductionDbHost "prod-server" -ProductionDbUser "analytics_user" -ProductionDbPassword "prodpass" -DemoDbPassword "demopass"
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$ProductionDbHost,
    
    [Parameter(Mandatory=$false)]
    [string]$ProductionDbUser,
    
    [Parameter(Mandatory=$false)]
    [string]$ProductionDbPassword,
    
    [Parameter(Mandatory=$false)]
    [string]$ProductionDbName,
    
    [Parameter(Mandatory=$false)]
    [string]$DemoDbHost,
    
    [Parameter(Mandatory=$false)]
    [string]$DemoDbUser,
    
    [Parameter(Mandatory=$false)]
    [string]$DemoDbPassword,
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipConfirmation
)

# Load .env_demo if exists (provides defaults for demo database)
# Check parent directory first (etl_pipeline/synthetic_data_generator/), then scripts directory
$parentDir = Split-Path $PSScriptRoot -Parent
$envFile = Join-Path $parentDir ".env_demo"
if (-not (Test-Path $envFile)) {
    # Try scripts directory as fallback
    $envFile = Join-Path $PSScriptRoot ".env_demo"
}

if (Test-Path $envFile) {
    Write-Host "📄 Loading configuration from .env_demo" -ForegroundColor Gray
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.*)$' -and $_ -notmatch '^\s*#') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            Set-Variable -Name $name -Value $value -Scope Script
        }
    }
} else {
    Write-Host "📄 No .env_demo file found - using ETL environment variables and CLI arguments" -ForegroundColor Gray
}

# Check if ETL environment variables are available (set by etl-init)
if ($env:POSTGRES_ANALYTICS_HOST -or $env:POSTGRES_ANALYTICS_PASSWORD) {
    Write-Host "✅ ETL environment variables detected (from etl-init with production)" -ForegroundColor Green
}

# Apply defaults from ETL pipeline environment variables (set by etl-init), .env_demo, or fallback to hardcoded defaults
# Priority: 1) CLI args, 2) ETL env vars (POSTGRES_ANALYTICS_* from .env_production), 3) .env_demo (PRODUCTION_POSTGRES_*), 4) defaults
# Note: When etl-init is run with "production", it loads .env_production and sets POSTGRES_ANALYTICS_* in the session

if (-not $ProductionDbHost) { 
    $ProductionDbHost = if ($env:POSTGRES_ANALYTICS_HOST) { 
        Write-Host "  Using production host from ETL environment: $env:POSTGRES_ANALYTICS_HOST" -ForegroundColor Gray
        $env:POSTGRES_ANALYTICS_HOST 
    } elseif ($PRODUCTION_POSTGRES_HOST) { 
        $PRODUCTION_POSTGRES_HOST 
    } else { 
        "localhost" 
    } 
}
if (-not $ProductionDbUser) { 
    $ProductionDbUser = if ($env:POSTGRES_ANALYTICS_USER) { 
        Write-Host "  Using production user from ETL environment: $env:POSTGRES_ANALYTICS_USER" -ForegroundColor Gray
        $env:POSTGRES_ANALYTICS_USER 
    } elseif ($PRODUCTION_POSTGRES_USER) { 
        $PRODUCTION_POSTGRES_USER 
    } else { 
        "postgres" 
    } 
}
if (-not $ProductionDbPassword) { 
    $ProductionDbPassword = if ($env:POSTGRES_ANALYTICS_PASSWORD) { 
        Write-Host "  Using production password from ETL environment" -ForegroundColor Gray
        $env:POSTGRES_ANALYTICS_PASSWORD 
    } elseif ($PRODUCTION_POSTGRES_PASSWORD) { 
        $PRODUCTION_POSTGRES_PASSWORD 
    } else { 
        $null 
    } 
}
if (-not $ProductionDbName) { 
    $ProductionDbName = if ($env:POSTGRES_ANALYTICS_DB) { 
        Write-Host "  Using production database from ETL environment: $env:POSTGRES_ANALYTICS_DB" -ForegroundColor Gray
        $env:POSTGRES_ANALYTICS_DB 
    } elseif ($PRODUCTION_POSTGRES_DB) { 
        $PRODUCTION_POSTGRES_DB 
    } else { 
        "opendental_analytics" 
    } 
}

if (-not $DemoDbHost) { $DemoDbHost = if ($DEMO_POSTGRES_HOST) { $DEMO_POSTGRES_HOST } else { "localhost" } }
if (-not $DemoDbUser) { $DemoDbUser = if ($DEMO_POSTGRES_USER) { $DEMO_POSTGRES_USER } else { "postgres" } }
if (-not $DemoDbPassword) { $DemoDbPassword = if ($DEMO_POSTGRES_PASSWORD) { $DEMO_POSTGRES_PASSWORD } else { $null } }

# Fixed database names (safety)
$demoDbName = "opendental_demo"  # HARDCODED - Never changes
$productionDbName = $ProductionDbName
$schemaName = "raw"

# CRITICAL SAFETY CHECK: Ensure production and demo databases are different
if ($productionDbName -eq $demoDbName) {
    Write-Host "`n❌ CRITICAL ERROR: Production and demo databases cannot be the same!" -ForegroundColor Red
    Write-Host "Production database: $productionDbName" -ForegroundColor Yellow
    Write-Host "Demo database: $demoDbName" -ForegroundColor Yellow
    Write-Host "`nThis would overwrite production data! Operation aborted." -ForegroundColor Red
    exit 1
}

# SAFETY CHECK: Warn if production database name is not the expected one
$expectedProductionDb = "opendental_analytics"
if ($productionDbName -ne $expectedProductionDb) {
    Write-Host "`n⚠️  WARNING: Production database name is '$productionDbName', expected '$expectedProductionDb'" -ForegroundColor Yellow
    Write-Host "This script is designed to read from '$expectedProductionDb'" -ForegroundColor Yellow
    $confirm = Read-Host "Continue anyway? (type 'yes' to proceed)"
    if ($confirm -ne "yes") {
        Write-Host "`n❌ Operation cancelled" -ForegroundColor Red
        exit 1
    }
}

# Password validation
if (-not $ProductionDbPassword) {
    Write-Host "`n❌ Error: Production database password required!" -ForegroundColor Red
    Write-Host "Provide via:" -ForegroundColor Yellow
    Write-Host "  1. CLI: .\recreate_schema.ps1 -ProductionDbPassword 'yourpassword'" -ForegroundColor Gray
    Write-Host "  2. ETL environment: Run 'etl-init' with production environment (sets POSTGRES_ANALYTICS_PASSWORD)" -ForegroundColor Gray
    Write-Host "  3. .env_demo: Add PRODUCTION_POSTGRES_PASSWORD=yourpassword" -ForegroundColor Gray
    exit 1
}

if (-not $DemoDbPassword) {
    Write-Host "`n❌ Error: Demo database password required!" -ForegroundColor Red
    Write-Host "Provide via:" -ForegroundColor Yellow
    Write-Host "  1. CLI: .\recreate_schema.ps1 -DemoDbPassword 'yourpassword'" -ForegroundColor Gray
    Write-Host "  2. .env_demo: Set DEMO_POSTGRES_PASSWORD=yourpassword" -ForegroundColor Gray
    exit 1
}

# Safety check
Write-Host "`n🔒 Safety Check: Schema Recreation" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan

Write-Host "`nSource (Production):" -ForegroundColor Yellow
Write-Host "  Database: $productionDbName" -ForegroundColor Gray
Write-Host "  Host: $ProductionDbHost" -ForegroundColor Gray
Write-Host "  User: $ProductionDbUser" -ForegroundColor Gray
Write-Host "  Schema: $schemaName" -ForegroundColor Gray

Write-Host "`nTarget (Demo):" -ForegroundColor Yellow
Write-Host "  Database: $demoDbName" -ForegroundColor Gray
Write-Host "  Host: $DemoDbHost" -ForegroundColor Gray
Write-Host "  User: $DemoDbUser" -ForegroundColor Gray
Write-Host "  Schema: $schemaName" -ForegroundColor Gray

Write-Host "`n⚠️  WARNING: This will:" -ForegroundColor Red
Write-Host "  1. READ schema structure from '$productionDbName' (READ-ONLY, no data copied)" -ForegroundColor Cyan
Write-Host "  2. DROP the existing '$schemaName' schema in '$demoDbName' (ALL DATA WILL BE LOST)" -ForegroundColor Yellow
Write-Host "  3. Recreate the schema in '$demoDbName' from '$productionDbName' structure" -ForegroundColor Yellow
Write-Host "  4. This operation CANNOT be undone!" -ForegroundColor Red
Write-Host "`n✅ SAFETY: Production database '$productionDbName' will NOT be modified (read-only)" -ForegroundColor Green
Write-Host "✅ SAFETY: Only demo database '$demoDbName' will be modified" -ForegroundColor Green

if (-not $SkipConfirmation) {
    $confirm = Read-Host "`nContinue? (type 'yes' to confirm)"
    
    if ($confirm -ne "yes") {
        Write-Host "`n❌ Operation cancelled" -ForegroundColor Red
        exit 1
    }
}

Write-Host "`n🚀 Starting schema recreation..." -ForegroundColor Green
Write-Host "═══════════════════════════════════════════`n" -ForegroundColor Cyan

# Final safety verification: Test connections to both databases
Write-Host "🔍 Verifying database connections..." -ForegroundColor Cyan

# Test production connection (read-only)
$env:PGPASSWORD = $ProductionDbPassword
$prodTest = psql -h $ProductionDbHost -U $ProductionDbUser -d $productionDbName -c "SELECT 1;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Cannot connect to production database: $productionDbName" -ForegroundColor Red
    Write-Host "Error: $prodTest" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Production database connection verified: $productionDbName (READ-ONLY)" -ForegroundColor Green

# Test demo connection
$env:PGPASSWORD = $DemoDbPassword
$demoTest = psql -h $DemoDbHost -U $DemoDbUser -d $demoDbName -c "SELECT 1;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Cannot connect to demo database: $demoDbName" -ForegroundColor Red
    Write-Host "Error: $demoTest" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Demo database connection verified: $demoDbName" -ForegroundColor Green
Write-Host ""

# Create schema directory if it doesn't exist
$schemaDir = Join-Path $PSScriptRoot "schema"
if (-not (Test-Path $schemaDir)) {
    Write-Host "📁 Creating schema directory..." -ForegroundColor Gray
    New-Item -ItemType Directory -Path $schemaDir | Out-Null
}

$schemaFile = Join-Path $schemaDir "create_tables.sql"

# Step 1: Drop existing raw schema
Write-Host "Step 1/5: Dropping existing '$schemaName' schema..." -ForegroundColor Cyan
$dropSchemaCmd = "DROP SCHEMA IF EXISTS $schemaName CASCADE;"
$env:PGPASSWORD = $DemoDbPassword
$dropResult = psql -h $DemoDbHost -U $DemoDbUser -d $demoDbName -c $dropSchemaCmd 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to drop schema: $dropResult" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Schema dropped successfully" -ForegroundColor Green

# Step 2: Recreate the raw schema
Write-Host "`nStep 2/5: Recreating '$schemaName' schema..." -ForegroundColor Cyan
$createSchemaCmd = "CREATE SCHEMA $schemaName;"
$createResult = psql -h $DemoDbHost -U $DemoDbUser -d $demoDbName -c $createSchemaCmd 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to create schema: $createResult" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Schema created successfully" -ForegroundColor Green

# Step 3: Extract schema structure from production (NO DATA)
# SAFETY: pg_dump is READ-ONLY - it only reads, never writes
Write-Host "`nStep 3/5: Extracting schema structure from production (READ-ONLY)..." -ForegroundColor Cyan
Write-Host "  Source: $productionDbName.$schemaName (READ-ONLY - no data copied)" -ForegroundColor Gray
Write-Host "  Output: $schemaFile" -ForegroundColor Gray
Write-Host "  ⚠️  This step only READS from production, never writes" -ForegroundColor Green

$env:PGPASSWORD = $ProductionDbPassword
$dumpResult = pg_dump -h $ProductionDbHost -U $ProductionDbUser -d $productionDbName `
    --schema-only `
    --schema=$schemaName `
    --no-owner `
    --no-acl `
    -f $schemaFile 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to extract schema: $dumpResult" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $schemaFile)) {
    Write-Host "❌ Schema file was not created: $schemaFile" -ForegroundColor Red
    exit 1
}

$fileSize = (Get-Item $schemaFile).Length
Write-Host "✅ Schema extracted successfully ($([math]::Round($fileSize/1KB, 2)) KB)" -ForegroundColor Green

# Step 4: Apply schema to demo database
Write-Host "`nStep 4/5: Applying schema to demo database..." -ForegroundColor Cyan
$env:PGPASSWORD = $DemoDbPassword
$applyResult = psql -h $DemoDbHost -U $DemoDbUser -d $demoDbName -f $schemaFile 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to apply schema: $applyResult" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Schema applied successfully" -ForegroundColor Green

# Step 5: Create additional schemas for dbt
Write-Host "`nStep 5/5: Creating additional dbt schemas..." -ForegroundColor Cyan
$dbtSchemas = @("raw_staging", "raw_intermediate", "raw_marts")
foreach ($dbtSchema in $dbtSchemas) {
    $createDbtSchemaCmd = "CREATE SCHEMA IF NOT EXISTS $dbtSchema;"
    $createDbtResult = psql -h $DemoDbHost -U $DemoDbUser -d $demoDbName -c $createDbtSchemaCmd 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  Warning: Failed to create schema $dbtSchema : $createDbtResult" -ForegroundColor Yellow
    } else {
        Write-Host "✅ Created schema: $dbtSchema" -ForegroundColor Green
    }
}

# Verification
Write-Host "`n📊 Verification:" -ForegroundColor Cyan
$env:PGPASSWORD = $DemoDbPassword
$tableCount = psql -h $DemoDbHost -U $DemoDbUser -d $demoDbName -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$schemaName';" 2>&1
$tableCount = $tableCount.Trim()

if ($LASTEXITCODE -eq 0 -and $tableCount -match '^\d+$') {
    Write-Host "✅ Schema '$schemaName' contains $tableCount tables" -ForegroundColor Green
} else {
    Write-Host "⚠️  Could not verify table count" -ForegroundColor Yellow
}

Write-Host "`n✅ Schema recreation completed successfully!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  1. Verify schema: psql -d $demoDbName -c '\dt $schemaName.*'" -ForegroundColor Gray
Write-Host "  2. Generate synthetic data: .\generate.ps1 -Patients 5000" -ForegroundColor Gray
Write-Host "`n" -ForegroundColor Gray

