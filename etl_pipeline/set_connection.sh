#!/bin/bash

# Local MySQL Setup Script for Port 3305
# Sets up staging database on your local MySQL server (port 3305)

echo -e "\e[36m🐬 Local MySQL Setup for ETL Pipeline (Port 3305)\e[0m"
echo -e "\e[36m==================================================\e[0m"
echo

# Function to test port connectivity
test_port() {
    local hostname=$1
    local port=$2
    if timeout 3 bash -c ">/dev/tcp/$hostname/$port" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to safely extract env variable value
get_env_value() {
    local content=$1
    local pattern=$2
    echo "$content" | grep -oP "$pattern=\K.*" | tr -d '"' | tr -d "'"
}

# Read and validate .env file
echo -e "\e[36m📝 Reading .env file...\e[0m"
echo -e "\e[36m=======================================================\e[0m"

# Updated path - now looking in current directory
envPath=".env"
if [ ! -f "$envPath" ]; then
    echo -e "\e[31m❌ .env file not found at: $envPath\e[0m"
    
    # Check if .env.template exists
    templatePath=".env.template"
    if [ -f "$templatePath" ]; then
        echo -e "\e[33m💡 Found .env.template file. Please:\e[0m"
        echo -e "\e[36m   1. Copy .env.template to .env\e[0m"
        echo -e "\e[36m   2. Fill in the required values\e[0m"
        echo -e "\e[36m   3. Run this script again\e[0m"
    fi
    exit 1
fi

# Read the .env file
envContent=$(cat "$envPath")

# Extract connection information safely
stagingHost=$(get_env_value "$envContent" "STAGING_MYSQL_HOST")
stagingPort=$(get_env_value "$envContent" "STAGING_MYSQL_PORT")
stagingDb=$(get_env_value "$envContent" "STAGING_MYSQL_DB")
stagingUser=$(get_env_value "$envContent" "STAGING_MYSQL_USER")
stagingPassword=$(get_env_value "$envContent" "STAGING_MYSQL_PASSWORD")

# Validate required staging variables
missingVars=()
[ -z "$stagingHost" ] && missingVars+=("STAGING_MYSQL_HOST")
[ -z "$stagingPort" ] && missingVars+=("STAGING_MYSQL_PORT")
[ -z "$stagingDb" ] && missingVars+=("STAGING_MYSQL_DB")
[ -z "$stagingUser" ] && missingVars+=("STAGING_MYSQL_USER")
[ -z "$stagingPassword" ] && missingVars+=("STAGING_MYSQL_PASSWORD")

if [ ${#missingVars[@]} -gt 0 ]; then
    echo -e "\e[31m❌ Missing or empty environment variables in .env:\e[0m"
    for var in "${missingVars[@]}"; do
        echo -e "\e[33m   - $var\e[0m"
    done
    echo
    echo -e "\e[33m💡 Please update your .env file with these variables:\e[0m"
    echo -e "\e[36m   Example values:\e[0m"
    echo -e "\e[90m   STAGING_MYSQL_HOST=localhost\e[0m"
    echo -e "\e[90m   STAGING_MYSQL_PORT=3305\e[0m"
    echo -e "\e[90m   STAGING_MYSQL_DB=opendental_staging\e[0m"
    echo -e "\e[90m   STAGING_MYSQL_USER=staging_user\e[0m"
    echo -e "\e[90m   STAGING_MYSQL_PASSWORD=your_password\e[0m"
    exit 1
fi

echo -e "\e[32m✅ .env file found and validated\e[0m"

# Check if local MySQL on specified port is running
echo -e "\e[33mChecking MySQL service on port $stagingPort...\e[0m"

if test_port "localhost" "$stagingPort"; then
    echo -e "\e[32m✅ MySQL is running on port $stagingPort\e[0m"
else
    echo -e "\e[31m❌ MySQL is not responding on port $stagingPort\e[0m"
    echo
    echo -e "\e[33m💡 Troubleshooting steps:\e[0m"
    echo -e "\e[36m1. Check if MySQL service is running:\e[0m"
    echo -e "\e[90m   systemctl status mysql\e[0m"
    echo -e "\e[36m2. Start MySQL service if needed:\e[0m"
    echo -e "\e[90m   sudo systemctl start mysql\e[0m"
    echo -e "\e[36m3. Verify port $stagingPort in your my.cnf configuration\e[0m"
    echo
    
    read -p "Do you want to continue with the setup anyway? (y/n) " continueSetup
    if [ "$continueSetup" != "y" ]; then
        echo -e "\e[33mSetup cancelled. Please start MySQL service and try again.\e[0m"
        exit 1
    fi
fi

echo

# Check if we can connect to MySQL
echo -e "\e[33mTesting MySQL connection on port $stagingPort...\e[0m"

mysqlAccessible=false

# Test without password first
if mysql -h localhost -P "$stagingPort" -u root --password="" -e "SELECT VERSION(), @@port;" 2>/dev/null; then
    mysqlAccessible=true
    echo -e "\e[32m✅ MySQL accessible without password\e[0m"
    echo -e "\e[90m   Version info: $(mysql -h localhost -P "$stagingPort" -u root --password="" -e "SELECT VERSION(), @@port;" 2>/dev/null)\e[0m"
fi

if [ "$mysqlAccessible" = false ]; then
    echo -e "\e[33m⚠️  MySQL requires root password\e[0m"
    echo -e "\e[90mYou'll be prompted for your MySQL root password\e[0m"
fi

echo

# MySQL Setup SQL
mysqlSetupSql="
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
"

echo -e "\e[33m🔧 Setting up MySQL staging database on port $stagingPort...\e[0m"
echo

if [ "$mysqlAccessible" = true ]; then
    # Run without password prompt
    setupOutput=$(echo "$mysqlSetupSql" | mysql -h localhost -P "$stagingPort" -u root --password="")
else
    # Run with password prompt
    echo -e "\e[36mPlease enter your MySQL root password:\e[0m"
    setupOutput=$(echo "$mysqlSetupSql" | mysql -h localhost -P "$stagingPort" -u root -p)
fi

if [ $? -eq 0 ]; then
    echo -e "\e[32m✅ MySQL staging setup completed successfully!\e[0m"
    if [ ! -z "$setupOutput" ]; then
        echo -e "\e[90mSetup output:\e[0m"
        echo -e "\e[90m$setupOutput\e[0m"
    fi
else
    echo -e "\e[33m❌ MySQL setup had issues\e[0m"
fi

echo

# Test the new staging user
echo -e "\e[33m🧪 Testing staging user connection on port $stagingPort...\e[0m"

if mysql -h localhost -P "$stagingPort" -u "$stagingUser" -p"$stagingPassword" -e "USE $stagingDb; SELECT 'Staging user connection successful' as Status, @@port as Port;" 2>/dev/null; then
    echo -e "\e[32m✅ Staging user can connect successfully!\e[0m"
    echo -e "\e[90m   $(mysql -h localhost -P "$stagingPort" -u "$stagingUser" -p"$stagingPassword" -e "USE $stagingDb; SELECT 'Staging user connection successful' as Status, @@port as Port;" 2>/dev/null)\e[0m"
else
    echo -e "\e[31m❌ Staging user connection failed\e[0m"
fi

echo

# PostgreSQL Setup
echo -e "\e[36m🐘 PostgreSQL Setup Check\e[0m"
echo -e "\e[36m=========================\e[0m"

# Extract PostgreSQL connection information
pgHost=$(get_env_value "$envContent" "TARGET_POSTGRES_HOST")
pgPort=$(get_env_value "$envContent" "TARGET_POSTGRES_PORT")
pgDb=$(get_env_value "$envContent" "TARGET_POSTGRES_DB")
pgSchema=$(get_env_value "$envContent" "TARGET_POSTGRES_SCHEMA")
pgUser=$(get_env_value "$envContent" "TARGET_POSTGRES_USER")
pgPassword=$(get_env_value "$envContent" "TARGET_POSTGRES_PASSWORD")

[ -z "$pgPort" ] && pgPort="5432"  # Default PostgreSQL port

if test_port "localhost" "$pgPort"; then
    echo -e "\e[32m✅ PostgreSQL is running on port $pgPort\e[0m"
    
    if [ ! -z "$pgDb" ]; then
        read -p "Do you want to set up PostgreSQL analytics database? (y/n) " setupPG
        if [ "$setupPG" = "y" ]; then
            echo -e "\e[33mSetting up PostgreSQL...\e[0m"
            
            postgresSetupSql="
-- Check if database exists
SELECT CASE 
    WHEN EXISTS(SELECT 1 FROM pg_database WHERE datname = '$pgDb') 
    THEN 'Database exists' 
    ELSE 'Database does not exist' 
END as database_status;
"

            postgresUserSql="
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
"

            echo -e "\e[36mCreating PostgreSQL database...\e[0m"
            echo "$postgresSetupSql" | psql -U postgres -h localhost
            
            echo -e "\e[36mSetting up user and schemas...\e[0m"
            echo "$postgresUserSql" | psql -U postgres -h localhost
            
            if [ $? -eq 0 ]; then
                echo -e "\e[32m✅ PostgreSQL setup completed!\e[0m"
            else
                echo -e "\e[31m❌ PostgreSQL setup failed. Set up manually in pgAdmin.\e[0m"
            fi
        else
            echo -e "\e[33m⏭️  Skipping PostgreSQL setup\e[0m"
        fi
    else
        echo -e "\e[33m⏭️  PostgreSQL variables not configured in .env\e[0m"
    fi
else
    echo -e "\e[31m❌ PostgreSQL not running on port $pgPort\e[0m"
    echo -e "\e[90m   Install PostgreSQL if you need the analytics database\e[0m"
fi

echo
echo -e "\e[32m🎉 Database setup completed!\e[0m"
echo
echo -e "\e[37m📋 Configuration Summary:\e[0m"
echo -e "\e[36m- Local MySQL (staging): $stagingHost:$stagingPort\e[0m"
if [ ! -z "$pgHost" ]; then
    echo -e "\e[36m- PostgreSQL (analytics): $pgHost:$pgPort\e[0m"
fi
echo
echo -e "\e[37m🔧 Next steps:\e[0m"
echo -e "\e[36m1. Test all connections: python test_connections.py\e[0m"
echo -e "\e[36m2. Run your ETL pipeline when ready\e[0m"
echo 