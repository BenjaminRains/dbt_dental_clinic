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

# Function to safely extract env variable value
function Get-EnvValue($content, $pattern) {
    $match = $content | Select-String -Pattern $pattern -AllMatches
    if ($match -and $match.Matches.Count -gt 0 -and $match.Matches[0].Groups.Count -gt 1) {
        return $match.Matches[0].Groups[1].Value.Trim()
    }
    return ""
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

# MySQL Setup
if (-not $SkipMySQL) {
    Write-Host "`n🐬 MySQL Staging Database Setup" -ForegroundColor Cyan
    Write-Host "================================" -ForegroundColor Cyan
    
    # Extract MySQL connection information
    $stagingHost = Get-EnvValue $envContent "STAGING_MYSQL_HOST=(.*)"
    $stagingPort = Get-EnvValue $envContent "STAGING_MYSQL_PORT=(.*)"
    $stagingDb = Get-EnvValue $envContent "STAGING_MYSQL_DB=(.*)"
    $stagingUser = Get-EnvValue $envContent "STAGING_MYSQL_USER=(.*)"
    $stagingPassword = Get-EnvValue $envContent "STAGING_MYSQL_PASSWORD=(.*)"
    
    # Set defaults if not specified
    if ([string]::IsNullOrEmpty($stagingHost)) { $stagingHost = "localhost" }
    if ([string]::IsNullOrEmpty($stagingPort)) { $stagingPort = "3306" }
    
    # Validate required staging variables
    $missingVars = @()
    if ([string]::IsNullOrEmpty($stagingDb)) { $missingVars += "STAGING_MYSQL_DB" }
    if ([string]::IsNullOrEmpty($stagingUser)) { $missingVars += "STAGING_MYSQL_USER" }
    if ([string]::IsNullOrEmpty($stagingPassword)) { $missingVars += "STAGING_MYSQL_PASSWORD" }
    
    if ($missingVars.Count -gt 0) {
        Write-Host "❌ Missing MySQL environment variables:" -ForegroundColor Red
        foreach ($var in $missingVars) {
            Write-Host "   - $var" -ForegroundColor Yellow
        }
        Write-Host "⏭️  Skipping MySQL setup" -ForegroundColor Yellow
    } else {
        # Check if MySQL is running
        Write-Host "Checking MySQL service on $stagingHost`:$stagingPort..." -ForegroundColor Yellow
        
        $mysqlRunning = Test-Port $stagingHost $stagingPort
        if ($mysqlRunning) {
            Write-Host "✅ MySQL is running on $stagingHost`:$stagingPort" -ForegroundColor Green
            
            # MySQL Setup SQL
            $mysqlSetupSql = @"
-- Show current MySQL version and port
SELECT VERSION() as MySQL_Version, @@port as Port;

-- Create staging database if it doesn't exist
CREATE DATABASE IF NOT EXISTS $stagingDb
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

-- Create staging user if it doesn't exist
CREATE USER IF NOT EXISTS '$stagingUser'@'localhost' IDENTIFIED BY '$stagingPassword';
CREATE USER IF NOT EXISTS '$stagingUser'@'%' IDENTIFIED BY '$stagingPassword';

-- Grant permissions
GRANT ALL PRIVILEGES ON $stagingDb.* TO '$stagingUser'@'localhost';
GRANT ALL PRIVILEGES ON $stagingDb.* TO '$stagingUser'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Show created resources
SHOW DATABASES LIKE '$stagingDb';
SELECT User, Host FROM mysql.user WHERE User = '$stagingUser';
"@

            Write-Host "🔧 Setting up MySQL staging database..." -ForegroundColor Yellow
            
            try {
                # Try without password first
                $setupOutput = $mysqlSetupSql | mysql -h $stagingHost -P $stagingPort -u root --password="" 2>$null
                if ($LASTEXITCODE -ne 0) {
                    # Try with password prompt
                    Write-Host "Please enter your MySQL root password:" -ForegroundColor Cyan
                    $setupOutput = $mysqlSetupSql | mysql -h $stagingHost -P $stagingPort -u root -p
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
            Write-Host "❌ MySQL is not running on $stagingHost`:$stagingPort" -ForegroundColor Red
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
    
    # Extract PostgreSQL connection information
    $pgHost = Get-EnvValue $envContent "TARGET_POSTGRES_HOST=(.*)"
    $pgPort = Get-EnvValue $envContent "TARGET_POSTGRES_PORT=(.*)"
    $pgDb = Get-EnvValue $envContent "TARGET_POSTGRES_DB=(.*)"
    $pgSchema = Get-EnvValue $envContent "TARGET_POSTGRES_SCHEMA=(.*)"
    $pgUser = Get-EnvValue $envContent "TARGET_POSTGRES_USER=(.*)"
    $pgPassword = Get-EnvValue $envContent "TARGET_POSTGRES_PASSWORD=(.*)"
    
    # Set defaults
    if ([string]::IsNullOrEmpty($pgHost)) { $pgHost = "localhost" }
    if ([string]::IsNullOrEmpty($pgPort)) { $pgPort = "5432" }
    if ([string]::IsNullOrEmpty($pgSchema)) { $pgSchema = "analytics" }
    
    if ([string]::IsNullOrEmpty($pgDb)) {
        Write-Host "⏭️  PostgreSQL database not configured (TARGET_POSTGRES_DB missing)" -ForegroundColor Yellow
    } else {
        $postgresRunning = Test-Port $pgHost $pgPort
        if ($postgresRunning) {
            Write-Host "✅ PostgreSQL is running on $pgHost`:$pgPort" -ForegroundColor Green
            
            $setupPG = Read-Host "Do you want to set up PostgreSQL analytics database '$pgDb'? (y/n)"
            if ($setupPG -eq "y") {
                Write-Host "🔧 Setting up PostgreSQL database and user..." -ForegroundColor Yellow
                
                # Note: This is a simplified setup - you may need to adjust for your PostgreSQL configuration
                Write-Host "   Please run these commands in your PostgreSQL admin tool:" -ForegroundColor Cyan
                Write-Host "   CREATE DATABASE $pgDb;" -ForegroundColor Gray
                Write-Host "   CREATE USER $pgUser WITH PASSWORD '$pgPassword';" -ForegroundColor Gray
                Write-Host "   GRANT ALL PRIVILEGES ON DATABASE $pgDb TO $pgUser;" -ForegroundColor Gray
                Write-Host "   \c $pgDb;" -ForegroundColor Gray
                Write-Host "   CREATE SCHEMA IF NOT EXISTS $pgSchema;" -ForegroundColor Gray
                Write-Host "   GRANT ALL ON SCHEMA $pgSchema TO $pgUser;" -ForegroundColor Gray
                
                Write-Host "✅ PostgreSQL setup instructions provided" -ForegroundColor Green
            } else {
                Write-Host "⏭️  Skipping PostgreSQL setup" -ForegroundColor Yellow
            }
        } else {
            Write-Host "❌ PostgreSQL not running on $pgHost`:$pgPort" -ForegroundColor Red
            Write-Host "   Install and start PostgreSQL if you need the analytics database" -ForegroundColor Gray
        }
    }
} else {
    Write-Host "`n⏭️  Skipping PostgreSQL setup (--SkipPostgreSQL specified)" -ForegroundColor Yellow
}

Write-Host "`n🎉 Database setup completed!" -ForegroundColor Green
Write-Host ""
Write-Host "📋 Configuration Summary:" -ForegroundColor White
Write-Host "- MySQL staging: $stagingHost`:$stagingPort / $stagingDb" -ForegroundColor Cyan
if (-not [string]::IsNullOrEmpty($pgDb)) {
    Write-Host "- PostgreSQL analytics: $pgHost`:$pgPort / $pgDb" -ForegroundColor Cyan
}
Write-Host ""
Write-Host "🔧 Next steps:" -ForegroundColor White
Write-Host "1. Test connections: etl-test-connections" -ForegroundColor Cyan
Write-Host "2. Initialize ETL environment: etl-init" -ForegroundColor Cyan
Write-Host "3. Run your ETL pipeline: etl run --full" -ForegroundColor Cyan
Write-Host ""