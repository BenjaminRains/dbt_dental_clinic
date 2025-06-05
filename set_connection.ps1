# Database Connection Setup Script for ETL Pipeline
# Sets up staging database on your local MySQL server and PostgreSQL
# Updated to work with ETL Pipeline PowerShell integration

param(
    [switch]$SkipMySQL,
    [switch]$SkipPostgreSQL,
    [switch]$Verbose
)

Write-Host "🔧 ETL Pipeline Database Setup" -ForegroundColor Cyan
Write-Host "==============================" -ForegroundColor Cyan
Write-Host ""

# Function to test port connectivity
function Test-Port($hostname, $port) {
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $connect = $tcpClient.BeginConnect($hostname, $port, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne(3000, $false)
        if ($wait) {
            $tcpClient.EndConnect($connect)
            $tcpClient.Close()
            return $true
        } else {
            $tcpClient.Close()
            return $false
        }
    } catch {
        return $false
    }
}

# Function to extract environment variable values
function Get-EnvValue {
    param (
        [string]$content,
        [string]$pattern
    )
    if ($content -match $pattern) {
        return $matches[1]
    }
    return $null
}

# Function to load environment variables (replaces set_env.ps1)
function Import-ETLEnvironment {
    # Try multiple possible locations for .env file
    $possiblePaths = @(
        ".\.env",                           # Current directory
        ".\etl_pipeline\.env",              # ETL pipeline subdirectory  
        "$PSScriptRoot\.env",               # Same directory as script
        "$PSScriptRoot\..\.env"             # Parent directory
    )
    
    $envPath = $null
    foreach ($path in $possiblePaths) {
        if (Test-Path $path) {
            $envPath = $path
            break
        }
    }
    
    if (-not $envPath) {
        Write-Host "❌ .env file not found in any expected location:" -ForegroundColor Red
        foreach ($path in $possiblePaths) {
            Write-Host "   - $path" -ForegroundColor Gray
        }
        Write-Host ""
        Write-Host "💡 Please ensure your .env file exists in the project root directory" -ForegroundColor Yellow
        return $false
    }
    
    if ($Verbose) {
        Write-Host "✅ Found .env file at: $envPath" -ForegroundColor Green
    }
    
    # Load environment variables
    try {
        Get-Content $envPath | ForEach-Object {
            if ($_ -match '^([^#][^=]+)=(.*)$') {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
            }
        }
        
        # Return the content for parsing
        return Get-Content $envPath -Raw
    } catch {
        Write-Host "❌ Failed to load environment variables: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Check if we're in ETL environment
if ($script:IsETLInitialized) {
    Write-Host "✅ ETL environment is active" -ForegroundColor Green
} else {
    Write-Host "⚠️  ETL environment not initialized. Loading environment variables..." -ForegroundColor Yellow
}

# Load environment variables
Write-Host "📝 Loading environment configuration..." -ForegroundColor Cyan
$envContent = Import-ETLEnvironment

if (-not $envContent) {
    Write-Host "❌ Cannot proceed without environment configuration" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Environment variables loaded" -ForegroundColor Green

# Read the .env file
$envPath = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path $envPath)) {
    Write-Host "❌ .env file not found at $envPath" -ForegroundColor Red
    exit 1
}

$envContent = Get-Content $envPath -Raw

# Extract environment variables
$sourceHost = Get-EnvValue $envContent "SOURCE_MYSQL_HOST=(.*)"
$sourcePort = Get-EnvValue $envContent "SOURCE_MYSQL_PORT=(.*)"
$sourceDb = Get-EnvValue $envContent "SOURCE_MYSQL_DB=(.*)"
$sourceUser = Get-EnvValue $envContent "SOURCE_MYSQL_USER=(.*)"
$sourcePassword = Get-EnvValue $envContent "SOURCE_MYSQL_PASSWORD=(.*)"

$replicationHost = Get-EnvValue $envContent "REPLICATION_MYSQL_HOST=(.*)"
$replicationPort = Get-EnvValue $envContent "REPLICATION_MYSQL_PORT=(.*)"
$replicationDb = Get-EnvValue $envContent "REPLICATION_MYSQL_DB=(.*)"
$replicationUser = Get-EnvValue $envContent "REPLICATION_MYSQL_USER=(.*)"
$replicationPassword = Get-EnvValue $envContent "REPLICATION_MYSQL_PASSWORD=(.*)"

$analyticsHost = Get-EnvValue $envContent "ANALYTICS_POSTGRES_HOST=(.*)"
$analyticsPort = Get-EnvValue $envContent "ANALYTICS_POSTGRES_PORT=(.*)"
$analyticsDb = Get-EnvValue $envContent "ANALYTICS_POSTGRES_DB=(.*)"
$analyticsSchema = Get-EnvValue $envContent "ANALYTICS_POSTGRES_SCHEMA=(.*)"
$analyticsUser = Get-EnvValue $envContent "ANALYTICS_POSTGRES_USER=(.*)"
$analyticsPassword = Get-EnvValue $envContent "ANALYTICS_POSTGRES_PASSWORD=(.*)"

# Check for missing variables
$missingVars = @()

# Source MySQL
if ([string]::IsNullOrEmpty($sourceHost)) { $missingVars += "SOURCE_MYSQL_HOST" }
if ([string]::IsNullOrEmpty($sourcePort)) { $missingVars += "SOURCE_MYSQL_PORT" }
if ([string]::IsNullOrEmpty($sourceDb)) { $missingVars += "SOURCE_MYSQL_DB" }
if ([string]::IsNullOrEmpty($sourceUser)) { $missingVars += "SOURCE_MYSQL_USER" }
if ([string]::IsNullOrEmpty($sourcePassword)) { $missingVars += "SOURCE_MYSQL_PASSWORD" }

# Replication MySQL
if ([string]::IsNullOrEmpty($replicationHost)) { $missingVars += "REPLICATION_MYSQL_HOST" }
if ([string]::IsNullOrEmpty($replicationPort)) { $missingVars += "REPLICATION_MYSQL_PORT" }
if ([string]::IsNullOrEmpty($replicationDb)) { $missingVars += "REPLICATION_MYSQL_DB" }
if ([string]::IsNullOrEmpty($replicationUser)) { $missingVars += "REPLICATION_MYSQL_USER" }
if ([string]::IsNullOrEmpty($replicationPassword)) { $missingVars += "REPLICATION_MYSQL_PASSWORD" }

# Analytics PostgreSQL
if ([string]::IsNullOrEmpty($analyticsHost)) { $missingVars += "ANALYTICS_POSTGRES_HOST" }
if ([string]::IsNullOrEmpty($analyticsPort)) { $missingVars += "ANALYTICS_POSTGRES_PORT" }
if ([string]::IsNullOrEmpty($analyticsDb)) { $missingVars += "ANALYTICS_POSTGRES_DB" }
if ([string]::IsNullOrEmpty($analyticsSchema)) { $missingVars += "ANALYTICS_POSTGRES_SCHEMA" }
if ([string]::IsNullOrEmpty($analyticsUser)) { $missingVars += "ANALYTICS_POSTGRES_USER" }
if ([string]::IsNullOrEmpty($analyticsPassword)) { $missingVars += "ANALYTICS_POSTGRES_PASSWORD" }

if ($missingVars.Count -gt 0) {
    Write-Host "❌ Missing required environment variables:" -ForegroundColor Red
    foreach ($var in $missingVars) {
        Write-Host "  - $var" -ForegroundColor Red
    }
    exit 1
}

# Set environment variables
$env:SOURCE_MYSQL_HOST = $sourceHost
$env:SOURCE_MYSQL_PORT = $sourcePort
$env:SOURCE_MYSQL_DB = $sourceDb
$env:SOURCE_MYSQL_USER = $sourceUser
$env:SOURCE_MYSQL_PASSWORD = $sourcePassword

$env:REPLICATION_MYSQL_HOST = $replicationHost
$env:REPLICATION_MYSQL_PORT = $replicationPort
$env:REPLICATION_MYSQL_DB = $replicationDb
$env:REPLICATION_MYSQL_USER = $replicationUser
$env:REPLICATION_MYSQL_PASSWORD = $replicationPassword

$env:ANALYTICS_POSTGRES_HOST = $analyticsHost
$env:ANALYTICS_POSTGRES_PORT = $analyticsPort
$env:ANALYTICS_POSTGRES_DB = $analyticsDb
$env:ANALYTICS_POSTGRES_SCHEMA = $analyticsSchema
$env:ANALYTICS_POSTGRES_USER = $analyticsUser
$env:ANALYTICS_POSTGRES_PASSWORD = $analyticsPassword

# Verify database configurations
$mysqlConfig = @{
    Host = $replicationHost
    Port = $replicationPort
    Database = $replicationDb
    User = $replicationUser
    Password = $replicationPassword
}

$pgConfig = @{
    Host = $analyticsHost
    Port = $analyticsPort
    Database = $analyticsDb
    Schema = $analyticsSchema
    User = $analyticsUser
    Password = $analyticsPassword
}

# Check MySQL configuration
if ([string]::IsNullOrEmpty($replicationDb)) {
    Write-Host "⏭️  MySQL database not configured (REPLICATION_MYSQL_DB missing)" -ForegroundColor Yellow
} else {
    Write-Host "✅ MySQL database configured: $replicationDb" -ForegroundColor Green
}

# Check PostgreSQL configuration
if ([string]::IsNullOrEmpty($analyticsDb)) {
    Write-Host "⏭️  PostgreSQL database not configured (ANALYTICS_POSTGRES_DB missing)" -ForegroundColor Yellow
} else {
    Write-Host "✅ PostgreSQL database configured: $analyticsDb" -ForegroundColor Green
}

Write-Host "`n✨ Environment variables set successfully!" -ForegroundColor Green

# MySQL Setup
if (-not $SkipMySQL) {
    Write-Host "`n🐬 MySQL Staging Database Setup" -ForegroundColor Cyan
    Write-Host "================================" -ForegroundColor Cyan
    
    # Validate required staging variables
    $missingVars = @()
    if ([string]::IsNullOrEmpty($replicationDb)) { $missingVars += "REPLICATION_MYSQL_DB" }
    if ([string]::IsNullOrEmpty($replicationUser)) { $missingVars += "REPLICATION_MYSQL_USER" }
    if ([string]::IsNullOrEmpty($replicationPassword)) { $missingVars += "REPLICATION_MYSQL_PASSWORD" }
    
    if ($missingVars.Count -gt 0) {
        Write-Host "❌ Missing MySQL environment variables:" -ForegroundColor Red
        foreach ($var in $missingVars) {
            Write-Host "   - $var" -ForegroundColor Yellow
        }
        Write-Host "⏭️  Skipping MySQL setup" -ForegroundColor Yellow
    } else {
        # Check if MySQL is running
        Write-Host "Checking MySQL service on $replicationHost`:$replicationPort..." -ForegroundColor Yellow
        
        $mysqlRunning = Test-Port $replicationHost $replicationPort
        if ($mysqlRunning) {
            Write-Host "✅ MySQL is running on $replicationHost`:$replicationPort" -ForegroundColor Green
            
            # MySQL Setup SQL
            $mysqlSetupSql = @"
-- Show current MySQL version and port
SELECT VERSION() as MySQL_Version, @@port as Port;

-- Create staging database if it doesn't exist
CREATE DATABASE IF NOT EXISTS $replicationDb
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

-- Create staging user if it doesn't exist
CREATE USER IF NOT EXISTS '$replicationUser'@'localhost' IDENTIFIED BY '$replicationPassword';
CREATE USER IF NOT EXISTS '$replicationUser'@'%' IDENTIFIED BY '$replicationPassword';

-- Grant permissions
GRANT ALL PRIVILEGES ON $replicationDb.* TO '$replicationUser'@'localhost';
GRANT ALL PRIVILEGES ON $replicationDb.* TO '$replicationUser'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Show created resources
SHOW DATABASES LIKE '$replicationDb';
SELECT User, Host FROM mysql.user WHERE User = '$replicationUser';
"@

            Write-Host "🔧 Setting up MySQL staging database..." -ForegroundColor Yellow
            
            try {
                # Try without password first
                $setupOutput = $mysqlSetupSql | mysql -h $replicationHost -P $replicationPort -u root --password="" 2>$null
                if ($LASTEXITCODE -ne 0) {
                    # Try with password prompt
                    Write-Host "Please enter your MySQL root password:" -ForegroundColor Cyan
                    $setupOutput = $mysqlSetupSql | mysql -h $replicationHost -P $replicationPort -u root -p
                }
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "✅ MySQL staging setup completed!" -ForegroundColor Green
                } else {
                    Write-Host "⚠️  MySQL setup had issues (you may need to run manually)" -ForegroundColor Yellow
                }
            } catch {
                Write-Host "❌ MySQL setup failed: $($_.Exception.Message)" -ForegroundColor Red
            }
        } else {
            Write-Host "❌ MySQL is not running on $replicationHost`:$replicationPort" -ForegroundColor Red
            Write-Host "   Please start MySQL and try again" -ForegroundColor Gray
        }
    }
} else {
    Write-Host "`n⏭️  Skipping MySQL setup (--SkipMySQL specified)" -ForegroundColor Yellow
}

# PostgreSQL Setup
if (-not $SkipPostgreSQL) {
    Write-Host "`n🐘 PostgreSQL Analytics Database Setup" -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan
    
    if ([string]::IsNullOrEmpty($analyticsDb)) {
        Write-Host "⏭️  PostgreSQL database not configured (ANALYTICS_POSTGRES_DB missing)" -ForegroundColor Yellow
    } else {
        $postgresRunning = Test-Port $analyticsHost $analyticsPort
        if ($postgresRunning) {
            Write-Host "✅ PostgreSQL is running on $analyticsHost`:$analyticsPort" -ForegroundColor Green
            
            $setupPG = Read-Host "Do you want to set up PostgreSQL analytics database '$analyticsDb'? (y/n)"
            if ($setupPG -eq "y") {
                Write-Host "🔧 Setting up PostgreSQL database and user..." -ForegroundColor Yellow
                
                # Note: This is a simplified setup - you may need to adjust for your PostgreSQL configuration
                Write-Host "   Please run these commands in your PostgreSQL admin tool:" -ForegroundColor Cyan
                Write-Host "   CREATE DATABASE $analyticsDb;" -ForegroundColor Gray
                Write-Host "   CREATE USER $analyticsUser WITH PASSWORD '$analyticsPassword';" -ForegroundColor Gray
                Write-Host "   GRANT ALL PRIVILEGES ON DATABASE $analyticsDb TO $analyticsUser;" -ForegroundColor Gray
                Write-Host "   \c $analyticsDb;" -ForegroundColor Gray
                Write-Host "   CREATE SCHEMA IF NOT EXISTS $analyticsSchema;" -ForegroundColor Gray
                Write-Host "   GRANT ALL ON SCHEMA $analyticsSchema TO $analyticsUser;" -ForegroundColor Gray
                
                Write-Host "✅ PostgreSQL setup instructions provided" -ForegroundColor Green
            } else {
                Write-Host "⏭️  Skipping PostgreSQL setup" -ForegroundColor Yellow
            }
        } else {
            Write-Host "❌ PostgreSQL not running on $analyticsHost`:$analyticsPort" -ForegroundColor Red
            Write-Host "   Install and start PostgreSQL if you need the analytics database" -ForegroundColor Gray
        }
    }
} else {
    Write-Host "`n⏭️  Skipping PostgreSQL setup (--SkipPostgreSQL specified)" -ForegroundColor Yellow
}

Write-Host "`n🎉 Database setup completed!" -ForegroundColor Green
Write-Host ""
Write-Host "📋 Configuration Summary:" -ForegroundColor White
Write-Host "- MySQL staging: $replicationHost`:$replicationPort / $replicationDb" -ForegroundColor Cyan
if (-not [string]::IsNullOrEmpty($analyticsDb)) {
    Write-Host "- PostgreSQL analytics: $analyticsHost`:$analyticsPort / $analyticsDb" -ForegroundColor Cyan
}
Write-Host ""
Write-Host "🔧 Next steps:" -ForegroundColor White
Write-Host "1. Test connections: etl-test-connections" -ForegroundColor Cyan
Write-Host "2. Initialize ETL environment: etl-init" -ForegroundColor Cyan
Write-Host "3. Run your ETL pipeline: etl run --full" -ForegroundColor Cyan
Write-Host ""