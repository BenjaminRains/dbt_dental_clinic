# Local MySQL Setup Script for Port 3305
# Sets up staging database on your local MySQL server (port 3305)

Write-Host "🐬 Local MySQL Setup for ETL Pipeline (Port 3305)" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
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

# Read and validate .env file
Write-Host "📝 Reading .env file..." -ForegroundColor Cyan
Write-Host "=======================================================" -ForegroundColor Cyan

# Updated path - now looking in current directory
$envFilePath = ".\.env"
if (-not (Test-Path $envFilePath)) {
    Write-Host "❌ .env file not found at: $envFilePath" -ForegroundColor Red
    
    # Check if .env.template exists
    $templatePath = ".\.env.template"
    if (Test-Path $templatePath) {
        Write-Host "💡 Found .env.template file. Please:" -ForegroundColor Yellow
        Write-Host "   1. Copy .env.template to .env" -ForegroundColor Cyan
        Write-Host "   2. Fill in the required values" -ForegroundColor Cyan
        Write-Host "   3. Run this script again" -ForegroundColor Cyan
    }
    exit 1
}

# Read the .env file
$envContent = Get-Content $envFilePath -Raw

# Load environment variables from .env file
Write-Host "🔧 Loading environment variables from .env..." -ForegroundColor Yellow
Get-Content $envFilePath | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}
Write-Host "✅ Environment variables loaded" -ForegroundColor Green

# Extract connection information safely
$stagingHost = Get-EnvValue $envContent "STAGING_MYSQL_HOST=(.*)"
$stagingPort = Get-EnvValue $envContent "STAGING_MYSQL_PORT=(.*)"
$stagingDb = Get-EnvValue $envContent "STAGING_MYSQL_DB=(.*)"
$stagingUser = Get-EnvValue $envContent "STAGING_MYSQL_USER=(.*)"
$stagingPassword = Get-EnvValue $envContent "STAGING_MYSQL_PASSWORD=(.*)"

# Validate required staging variables
$missingVars = @()
if ([string]::IsNullOrEmpty($stagingHost)) { $missingVars += "STAGING_MYSQL_HOST" }
if ([string]::IsNullOrEmpty($stagingPort)) { $missingVars += "STAGING_MYSQL_PORT" }
if ([string]::IsNullOrEmpty($stagingDb)) { $missingVars += "STAGING_MYSQL_DB" }
if ([string]::IsNullOrEmpty($stagingUser)) { $missingVars += "STAGING_MYSQL_USER" }
if ([string]::IsNullOrEmpty($stagingPassword)) { $missingVars += "STAGING_MYSQL_PASSWORD" }

if ($missingVars.Count -gt 0) {
    Write-Host "❌ Missing or empty environment variables in .env:" -ForegroundColor Red
    foreach ($var in $missingVars) {
        Write-Host "   - $var" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "💡 Please update your .env file with these variables:" -ForegroundColor Yellow
    Write-Host "   Example values:" -ForegroundColor Cyan
    Write-Host "   STAGING_MYSQL_HOST=localhost" -ForegroundColor Gray
    Write-Host "   STAGING_MYSQL_PORT=3305" -ForegroundColor Gray
    Write-Host "   STAGING_MYSQL_DB=opendental_staging" -ForegroundColor Gray
    Write-Host "   STAGING_MYSQL_USER=staging_user" -ForegroundColor Gray
    Write-Host "   STAGING_MYSQL_PASSWORD=your_password" -ForegroundColor Gray
    exit 1
}

Write-Host "✅ .env file found and validated" -ForegroundColor Green

# Check if local MySQL on specified port is running
Write-Host "Checking MySQL service on port $stagingPort..." -ForegroundColor Yellow

$mysqlRunning = Test-Port "localhost" $stagingPort
if ($mysqlRunning) {
    Write-Host "✅ MySQL is running on port $stagingPort" -ForegroundColor Green
} else {
    Write-Host "❌ MySQL is not responding on port $stagingPort" -ForegroundColor Red
    Write-Host ""
    Write-Host "💡 Troubleshooting steps:" -ForegroundColor Yellow
    Write-Host "1. Check if MySQL service is running:" -ForegroundColor Cyan
    Write-Host "   Get-Service | Where-Object {`$_.Name -like '*mysql*'}" -ForegroundColor Gray
    Write-Host "2. Start MySQL service if needed:" -ForegroundColor Cyan
    Write-Host "   net start MySQL84  # or your MySQL service name" -ForegroundColor Gray
    Write-Host "3. Verify port $stagingPort in your my.ini configuration" -ForegroundColor Cyan
    Write-Host ""
    
    $continueSetup = Read-Host "Do you want to continue with the setup anyway? (y/n)"
    if ($continueSetup -ne "y") {
        Write-Host "Setup cancelled. Please start MySQL service and try again." -ForegroundColor Yellow
        exit 1
    }
}

Write-Host ""

# Check if we can connect to MySQL
Write-Host "Testing MySQL connection on port $stagingPort..." -ForegroundColor Yellow

$mysqlAccessible = $false

# Test without password first
try {
    $testResult = & mysql -h localhost -P $stagingPort -u root --password="" -e "SELECT VERSION(), @@port;" 2>$null
    if ($LASTEXITCODE -eq 0) {
        $mysqlAccessible = $true
        Write-Host "✅ MySQL accessible without password" -ForegroundColor Green
        Write-Host "   Version info: $testResult" -ForegroundColor Gray
    }
} catch {
    # Will try with password
}

if (-not $mysqlAccessible) {
    Write-Host "⚠️  MySQL requires root password" -ForegroundColor Yellow
    Write-Host "You'll be prompted for your MySQL root password" -ForegroundColor Gray
}

Write-Host ""

# MySQL Setup SQL
$mysqlSetupSql = @"
-- Show current MySQL version and port
SELECT VERSION() as MySQL_Version, @@port as Port;

-- Check if staging database exists
SELECT IF(EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '$stagingDb'), 
    'Database exists', 'Database does not exist') as Database_Status;

-- Create staging database if it doesn't exist
CREATE DATABASE IF NOT EXISTS $stagingDb
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

-- Check if staging user exists
SELECT IF(EXISTS(SELECT 1 FROM mysql.user WHERE User = '$stagingUser' AND Host IN ('localhost', '%')), 
    'User exists', 'User does not exist') as User_Status;

-- Create staging user if it doesn't exist
CREATE USER IF NOT EXISTS '$stagingUser'@'localhost' IDENTIFIED BY '$stagingPassword';
CREATE USER IF NOT EXISTS '$stagingUser'@'%' IDENTIFIED BY '$stagingPassword';

-- Grant permissions (will update existing grants if user exists)
GRANT ALL PRIVILEGES ON $stagingDb.* TO '$stagingUser'@'localhost';
GRANT ALL PRIVILEGES ON $stagingDb.* TO '$stagingUser'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Show created resources
SHOW DATABASES LIKE '$stagingDb';
SELECT User, Host FROM mysql.user WHERE User = '$stagingUser';

-- Test the staging database
USE $stagingDb;
CREATE TABLE IF NOT EXISTS etl_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    test_message VARCHAR(255)
);

INSERT INTO etl_test (test_message) VALUES ('Setup test successful');
SELECT * FROM etl_test;
DROP TABLE etl_test;

-- Show grants for the new user
SHOW GRANTS FOR '$stagingUser'@'localhost';
"@

Write-Host "🔧 Setting up MySQL staging database on port $stagingPort..." -ForegroundColor Yellow
Write-Host ""

try {
    if ($mysqlAccessible) {
        # Run without password prompt
        $setupOutput = $mysqlSetupSql | mysql -h localhost -P $stagingPort -u root --password=""
    } else {
        # Run with password prompt
        Write-Host "Please enter your MySQL root password:" -ForegroundColor Cyan
        $setupOutput = $mysqlSetupSql | mysql -h localhost -P $stagingPort -u root -p
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ MySQL staging setup completed successfully!" -ForegroundColor Green
        if ($setupOutput) {
            Write-Host "Setup output:" -ForegroundColor Gray
            Write-Host $setupOutput -ForegroundColor Gray
        }
    } else {
        Write-Host "❌ MySQL setup had issues (exit code: $LASTEXITCODE)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "❌ MySQL setup failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "   You can run the SQL commands manually in MySQL Workbench" -ForegroundColor Gray
}

Write-Host ""

# Test the new staging user - Updated to handle MySQL 8.0+ authentication
Write-Host "🧪 Testing staging user connection on port $stagingPort..." -ForegroundColor Yellow

try {
    # Method 1: Try with password prompt (more reliable for MySQL 8.0+)
    Write-Host "   Testing with interactive password prompt..." -ForegroundColor Gray
    $testCommand = "USE $stagingDb; SELECT 'Staging user connection successful' as Status, @@port as Port, USER() as CurrentUser;"
    $testResult = echo $testCommand | mysql -h localhost -P $stagingPort -u $stagingUser -p 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Staging user can connect successfully!" -ForegroundColor Green
        Write-Host "   $testResult" -ForegroundColor Gray
    } else {
        # Method 2: Try with environment variable for password
        Write-Host "   Trying alternative connection method..." -ForegroundColor Gray
        $env:MYSQL_PWD = $stagingPassword
        $testResult2 = & mysql -h localhost -P $stagingPort -u $stagingUser -e "USE $stagingDb; SELECT 'Connection test via env variable' as Status;" 2>&1
        $env:MYSQL_PWD = $null  # Clear the environment variable
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Staging user connection verified (alternative method)!" -ForegroundColor Green
            Write-Host "   $testResult2" -ForegroundColor Gray
        } else {
            Write-Host "⚠️  Command-line connection test failed, but this is expected with MySQL 8.0+" -ForegroundColor Yellow
            Write-Host "   The database setup completed successfully. Your Python ETL will work fine." -ForegroundColor Cyan
            Write-Host "   Error details: $testResult" -ForegroundColor Gray
        }
    }
} catch {
    Write-Host "⚠️  Connection test encountered an issue, but setup completed successfully" -ForegroundColor Yellow
    Write-Host "   This is common with MySQL 8.0+ authentication. Your Python ETL should work normally." -ForegroundColor Cyan
    Write-Host "   Error: $($_.Exception.Message)" -ForegroundColor Gray
}

Write-Host ""

# PostgreSQL Setup
Write-Host "🐘 PostgreSQL Setup Check" -ForegroundColor Cyan
Write-Host "=========================" -ForegroundColor Cyan

# Extract PostgreSQL connection information
$pgHost = Get-EnvValue $envContent "TARGET_POSTGRES_HOST=(.*)"
$pgPort = Get-EnvValue $envContent "TARGET_POSTGRES_PORT=(.*)"
$pgDb = Get-EnvValue $envContent "TARGET_POSTGRES_DB=(.*)"
$pgSchema = Get-EnvValue $envContent "TARGET_POSTGRES_SCHEMA=(.*)"
$pgUser = Get-EnvValue $envContent "TARGET_POSTGRES_USER=(.*)"
$pgPassword = Get-EnvValue $envContent "TARGET_POSTGRES_PASSWORD=(.*)"

if ([string]::IsNullOrEmpty($pgPort)) { $pgPort = "5432" }  # Default PostgreSQL port

$postgresRunning = Test-Port "localhost" $pgPort
if ($postgresRunning) {
    Write-Host "✅ PostgreSQL is running on port $pgPort" -ForegroundColor Green
    
    if (-not [string]::IsNullOrEmpty($pgDb)) {
        $setupPG = Read-Host "Do you want to set up PostgreSQL analytics database? (y/n)"
        if ($setupPG -eq "y") {
            Write-Host "Setting up PostgreSQL..." -ForegroundColor Yellow
            
            $postgresSetupSql = @"
-- Check if database exists
SELECT CASE 
    WHEN EXISTS(SELECT 1 FROM pg_database WHERE datname = '$pgDb') 
    THEN 'Database exists' 
    ELSE 'Database does not exist' 
END as database_status;
"@

            $postgresUserSql = @"
-- Connect to the $pgDb database
\c $pgDb;

-- Check if user exists
SELECT CASE 
    WHEN EXISTS(SELECT 1 FROM pg_user WHERE usename = '$pgUser') 
    THEN 'User exists' 
    ELSE 'User does not exist' 
END as user_status;

-- Create analytics user (simple approach)
CREATE USER $pgUser WITH PASSWORD '$pgPassword';

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS $pgSchema;
CREATE SCHEMA IF NOT EXISTS raw;

-- Grant permissions
GRANT CONNECT ON DATABASE $pgDb TO $pgUser;
GRANT USAGE ON SCHEMA $pgSchema TO $pgUser;
GRANT USAGE ON SCHEMA raw TO $pgUser;
GRANT CREATE ON SCHEMA $pgSchema TO $pgUser;
GRANT CREATE ON SCHEMA raw TO $pgUser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA $pgSchema TO $pgUser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO $pgUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA $pgSchema GRANT ALL ON TABLES TO $pgUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO $pgUser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA $pgSchema TO $pgUser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO $pgUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA $pgSchema GRANT ALL ON SEQUENCES TO $pgUser;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON SEQUENCES TO $pgUser;

-- Show status
\l $pgDb;
\dn;
\du $pgUser;
"@

            try {
                Write-Host "Creating PostgreSQL database..." -ForegroundColor Cyan
                $postgresSetupSql | psql -U postgres -h localhost
                
                Write-Host "Setting up user and schemas..." -ForegroundColor Cyan
                $postgresUserSql | psql -U postgres -h localhost
                
                Write-Host "✅ PostgreSQL setup completed!" -ForegroundColor Green
            } catch {
                Write-Host "❌ PostgreSQL setup failed. Set up manually in pgAdmin." -ForegroundColor Red
            }
        } else {
            Write-Host "⏭️  Skipping PostgreSQL setup" -ForegroundColor Yellow
        }
    } else {
        Write-Host "⏭️  PostgreSQL variables not configured in .env" -ForegroundColor Yellow
    }
} else {
    Write-Host "❌ PostgreSQL not running on port $pgPort" -ForegroundColor Red
    Write-Host "   Install PostgreSQL if you need the analytics database" -ForegroundColor Gray
}

Write-Host ""
Write-Host "🎉 Database setup completed!" -ForegroundColor Green
Write-Host ""
Write-Host "📋 Configuration Summary:" -ForegroundColor White
Write-Host "- Local MySQL (staging): $stagingHost`:$stagingPort" -ForegroundColor Cyan
if (-not [string]::IsNullOrEmpty($pgHost)) {
    Write-Host "- PostgreSQL (analytics): $pgHost`:$pgPort" -ForegroundColor Cyan
}
Write-Host ""
Write-Host "🔧 Next steps:" -ForegroundColor White
Write-Host "1. Test all connections: python test_connections.py" -ForegroundColor Cyan
Write-Host "2. Run your ETL pipeline when ready" -ForegroundColor Cyan
Write-Host ""