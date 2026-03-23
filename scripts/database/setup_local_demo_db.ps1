# PowerShell script to set up local PostgreSQL demo database
# Usage: .\scripts\setup_local_demo_db.ps1

param(
    [string]$PostgresHost = "localhost",
    [int]$PostgresPort = 5432,
    [string]$PostgresUser = "postgres",
    [string]$PostgresPassword = "",
    [string]$DatabaseName = "opendental_demo",
    [switch]$CopySchemaFromProduction = $false,
    [string]$ProductionDatabase = "opendental_analytics"
)

$ErrorActionPreference = "Stop"

Write-Host "`n🔧 Setting up local demo database..." -ForegroundColor Cyan
Write-Host ""

# Set PGPASSWORD environment variable if provided
if ($PostgresPassword) {
    $env:PGPASSWORD = $PostgresPassword
}

# Check if psql is available
$psqlPath = Get-Command psql -ErrorAction SilentlyContinue
if (-not $psqlPath) {
    Write-Host "❌ psql not found. Please install PostgreSQL client tools." -ForegroundColor Red
    exit 1
}

Write-Host "📋 Configuration:" -ForegroundColor Yellow
Write-Host "   Host: $PostgresHost" -ForegroundColor Gray
Write-Host "   Port: $PostgresPort" -ForegroundColor Gray
Write-Host "   User: $PostgresUser" -ForegroundColor Gray
Write-Host "   Database: $DatabaseName" -ForegroundColor Gray
Write-Host ""

# Test connection
Write-Host "🔌 Testing PostgreSQL connection..." -ForegroundColor Yellow
try {
    $testResult = & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -c "SELECT version();" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Connection failed: $testResult"
    }
    Write-Host "✅ Connection successful" -ForegroundColor Green
} catch {
    Write-Host "❌ Connection failed: $_" -ForegroundColor Red
    Write-Host "   Make sure PostgreSQL is running and credentials are correct." -ForegroundColor Yellow
    exit 1
}

# Check if database exists
Write-Host "`n🔍 Checking if database exists..." -ForegroundColor Yellow
$dbExists = & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DatabaseName';" 2>&1

$needToCreate = $false

if ($dbExists -eq "1") {
    Write-Host "⚠️  Database '$DatabaseName' already exists." -ForegroundColor Yellow
    $response = Read-Host "   Do you want to drop and recreate it? (y/N)"
    if ($response -eq "y" -or $response -eq "Y") {
        Write-Host "🗑️  Dropping existing database..." -ForegroundColor Yellow
        & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -c "DROP DATABASE IF EXISTS $DatabaseName;" 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ Failed to drop database" -ForegroundColor Red
            exit 1
        }
        Write-Host "✅ Database dropped" -ForegroundColor Green
        $needToCreate = $true
    } else {
        Write-Host "ℹ️  Using existing database" -ForegroundColor Cyan
    }
} else {
    $needToCreate = $true
}

# Create database if needed
if ($needToCreate) {
    Write-Host "`n📦 Creating database '$DatabaseName'..." -ForegroundColor Yellow
    & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -c "CREATE DATABASE $DatabaseName;" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to create database" -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ Database created" -ForegroundColor Green
}

# Create schemas
Write-Host "`n📋 Creating schemas..." -ForegroundColor Yellow
$schemas = @("raw", "raw_staging", "raw_intermediate", "raw_marts")
foreach ($schema in $schemas) {
    Write-Host "   Creating schema: $schema" -ForegroundColor Gray
    & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d $DatabaseName -c "CREATE SCHEMA IF NOT EXISTS $schema;" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to create schema: $schema" -ForegroundColor Red
        exit 1
    }
}
Write-Host "✅ All schemas created" -ForegroundColor Green

# Copy schema structure from production if requested
if ($CopySchemaFromProduction) {
    Write-Host "`n📥 Copying schema structure from production database..." -ForegroundColor Yellow
    Write-Host "   Source: $ProductionDatabase (raw schema)" -ForegroundColor Gray
    Write-Host "   Target: $DatabaseName (raw schema)" -ForegroundColor Gray
    
    # Check if production database exists
    $prodExists = & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$ProductionDatabase';" 2>&1
    if ($prodExists -ne "1") {
        Write-Host "⚠️  Production database '$ProductionDatabase' not found. Skipping schema copy." -ForegroundColor Yellow
        Write-Host "   You can manually create tables or run dbt models to create the structure." -ForegroundColor Gray
    } else {
        # Export schema structure (no data)
        $tempFile = [System.IO.Path]::GetTempFileName() + ".sql"
        Write-Host "   Exporting schema structure..." -ForegroundColor Gray
        
        $pgDumpPath = Get-Command pg_dump -ErrorAction SilentlyContinue
        if (-not $pgDumpPath) {
            Write-Host "⚠️  pg_dump not found. Skipping schema copy." -ForegroundColor Yellow
            Write-Host "   You can manually create tables or run dbt models to create the structure." -ForegroundColor Gray
        } else {
            if ($PostgresPassword) {
                $env:PGPASSWORD = $PostgresPassword
            }
            
            & pg_dump -h $PostgresHost -p $PostgresPort -U $PostgresUser -d $ProductionDatabase `
                --schema-only --schema=raw `
                --no-owner --no-acl `
                -f $tempFile 2>&1 | Out-Null
            
            if ($LASTEXITCODE -eq 0) {
                # Modify the SQL to use the raw schema (in case it's hardcoded)
                $sqlContent = Get-Content $tempFile -Raw
                $sqlContent = $sqlContent -replace "CREATE SCHEMA IF NOT EXISTS raw;", ""
                $sqlContent = $sqlContent -replace "SET search_path = raw, public;", ""
                
                # Apply schema
                Write-Host "   Applying schema structure..." -ForegroundColor Gray
                & psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d $DatabaseName -f $tempFile 2>&1 | Out-Null
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "✅ Schema structure copied successfully" -ForegroundColor Green
                } else {
                    Write-Host "⚠️  Some errors occurred while applying schema. Check the output above." -ForegroundColor Yellow
                }
            } else {
                Write-Host "⚠️  Failed to export schema. Skipping schema copy." -ForegroundColor Yellow
            }
            
            # Clean up temp file
            Remove-Item $tempFile -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "`n✅ Local demo database setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "📝 Next steps:" -ForegroundColor Cyan
Write-Host "   1. Generate synthetic data:" -ForegroundColor Gray
Write-Host "      cd etl_pipeline/synthetic_data_generator" -ForegroundColor Gray
Write-Host "      etl-init demo" -ForegroundColor Gray
Write-Host "      .\generate.ps1" -ForegroundColor Gray
Write-Host ""
Write-Host "   2. Run dbt models:" -ForegroundColor Gray
Write-Host "      cd dbt_dental_models" -ForegroundColor Gray
Write-Host "      dbt run --target demo" -ForegroundColor Gray
Write-Host ""
Write-Host "   3. Query the database:" -ForegroundColor Gray
Write-Host "      .\scripts\query_demo_db.ps1 -Query \"SELECT * FROM marts.dim_procedure LIMIT 5\" -Local" -ForegroundColor Gray
Write-Host ""
