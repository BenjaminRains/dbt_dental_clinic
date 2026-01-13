# Synthetic Data Generator Wrapper Script
# Automatically loads credentials from environment variables or deployment_credentials.json
# Usage: .\generate.ps1 -Patients 5000
# For local database: .\generate.ps1 -Patients 5000 -DbPort 5432 -DbUser postgres -DbPassword "your_password"

param(
    [Parameter(Mandatory=$false)]
    [int]$Patients = 5000,
    
    [Parameter(Mandatory=$false)]
    [string]$DbHost,
    
    [Parameter(Mandatory=$false)]
    [string]$DbPort,
    
    [Parameter(Mandatory=$false)]
    [string]$DbName,
    
    [Parameter(Mandatory=$false)]
    [string]$DbUser,
    
    [Parameter(Mandatory=$false)]
    [string]$DbPassword,
    
    [Parameter(Mandatory=$false)]
    [string]$StartDate = "2023-01-01"
)

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptDir)
$credentialsPath = Join-Path $projectRoot "deployment_credentials.json"

Write-Host "`n🦷 Synthetic Data Generator" -ForegroundColor Cyan
Write-Host "============================" -ForegroundColor Cyan

# Check if ANY command-line parameters were provided
$hasCommandLineParams = $DbHost -or $DbPort -or $DbName -or $DbUser -or $DbPassword

if ($hasCommandLineParams) {
    # Command-line parameters provided - use them directly, skip all detection
    Write-Host "✅ Command-line parameters detected - using provided values directly" -ForegroundColor Green
    
    $dbHost = if ($DbHost) { $DbHost } else { $null }
    $dbPort = if ($DbPort) { [int]$DbPort } else { $null }
    $dbName = if ($DbName) { $DbName } else { "opendental_demo" }
    $dbUser = if ($DbUser) { $DbUser } else { $null }
    $dbPassword = if ($DbPassword) { $DbPassword } else { $null }
    
    Write-Host "   Host: $(if ($dbHost) { $dbHost } else { '(will use env/default)' })" -ForegroundColor Gray
    Write-Host "   Port: $(if ($dbPort) { $dbPort.ToString() } else { '(will use env/default)' })" -ForegroundColor Gray
    Write-Host "   Database: $dbName" -ForegroundColor Gray
    Write-Host "   User: $(if ($dbUser) { $dbUser } else { '(will use env/default)' })" -ForegroundColor Gray
    Write-Host "   Password: $(if ($dbPassword) { '***' } else { '(will use env/default)' })" -ForegroundColor Gray
    
    # Fill in missing values from environment variables
    if (-not $dbHost -and $env:DEMO_POSTGRES_HOST) { $dbHost = $env:DEMO_POSTGRES_HOST }
    if (-not $dbPort -and $env:DEMO_POSTGRES_PORT) { $dbPort = [int]$env:DEMO_POSTGRES_PORT }
    if (-not $dbUser -and $env:DEMO_POSTGRES_USER) { $dbUser = $env:DEMO_POSTGRES_USER }
    if (-not $dbPassword -and $env:DEMO_POSTGRES_PASSWORD) { $dbPassword = $env:DEMO_POSTGRES_PASSWORD }
    
} else {
    # No command-line parameters - use environment variables or credentials file
    Write-Host "💡 No command-line parameters - checking environment variables and credentials..." -ForegroundColor Cyan
    
    # Try environment variables first
    $dbHost = $env:DEMO_POSTGRES_HOST
    $dbPort = if ($env:DEMO_POSTGRES_PORT) { [int]$env:DEMO_POSTGRES_PORT } else { $null }
    $dbName = if ($env:DEMO_POSTGRES_DB) { $env:DEMO_POSTGRES_DB } else { "opendental_demo" }
    $dbUser = $env:DEMO_POSTGRES_USER
    $dbPassword = $env:DEMO_POSTGRES_PASSWORD
    
    # If not in environment, try credentials file
    if (Test-Path $credentialsPath) {
        try {
            $credentials = Get-Content $credentialsPath | ConvertFrom-Json
            if ($credentials.demo_database.postgresql) {
                $demo = $credentials.demo_database.postgresql
                
                # Fill in missing values from credentials
                if (-not $dbHost) {
                    # Check if we're on EC2 or local
                    try {
                        $ec2Metadata = Invoke-WebRequest -Uri "http://169.254.169.254/latest/meta-data/instance-id" -TimeoutSec 1 -ErrorAction Stop
                        if ($ec2Metadata.StatusCode -eq 200) {
                            $dbHost = $credentials.demo_database.ec2.private_ip
                            Write-Host "🌐 Detected EC2 environment - using direct connection" -ForegroundColor Cyan
                        }
                    } catch {
                        $dbHost = "localhost"
                        Write-Host "💻 Detected local environment" -ForegroundColor Cyan
                    }
                }
                
                if (-not $dbPort) {
                    # Check if we're on EC2
                    try {
                        $ec2Metadata = Invoke-WebRequest -Uri "http://169.254.169.254/latest/meta-data/instance-id" -TimeoutSec 1 -ErrorAction Stop
                        if ($ec2Metadata.StatusCode -eq 200) {
                            $dbPort = $demo.port
                            Write-Host "🌐 Using EC2 database port: $dbPort" -ForegroundColor Cyan
                        } else {
                            # Local environment - try to detect local PostgreSQL
                            $localPostgresFound = $false
                            try {
                                $tcpConnections = Get-NetTCPConnection -LocalPort 5432 -State Listen -ErrorAction SilentlyContinue
                                if ($tcpConnections) {
                                    $localPostgresFound = $true
                                    $dbPort = 5432
                                    Write-Host "💻 Detected local PostgreSQL on port 5432" -ForegroundColor Cyan
                                }
                            } catch {
                                # Connection check failed
                            }
                            
                            if (-not $localPostgresFound) {
                                # Default to port forwarding
                                $dbPort = 5434
                                Write-Host "💻 Using port forwarding (5434)" -ForegroundColor Cyan
                                Write-Host "   💡 Tip: For local database, use: -DbPort 5432 -DbUser postgres -DbPassword 'your_password'" -ForegroundColor Yellow
                            }
                        }
                    } catch {
                        # Not on EC2, default to port forwarding
                        $dbPort = 5434
                        Write-Host "💻 Using port forwarding (5434)" -ForegroundColor Cyan
                    }
                }
                
                if (-not $dbName) { $dbName = $demo.database }
                if (-not $dbUser) { 
                    # If local port 5432, suggest postgres user
                    if ($dbPort -eq 5432 -and $dbHost -eq "localhost") {
                        $dbUser = "postgres"
                    } else {
                        $dbUser = $demo.user
                    }
                }
                if (-not $dbPassword) { 
                    # Don't set password if using local - user must provide
                    if (-not ($dbPort -eq 5432 -and $dbHost -eq "localhost")) {
                        $dbPassword = $demo.password
                    }
                }
                
                Write-Host "✅ Loaded credentials from deployment_credentials.json" -ForegroundColor Green
            }
        } catch {
            Write-Host "⚠️  Could not parse deployment_credentials.json: $_" -ForegroundColor Yellow
        }
    }
}

# Set defaults for any remaining null values
if (-not $dbHost) { $dbHost = "localhost" }
if (-not $dbPort) { $dbPort = 5432 }
if (-not $dbName) { $dbName = "opendental_demo" }
if (-not $dbUser) { $dbUser = "postgres" }

# Password is required - check if missing
if (-not $dbPassword) {
    Write-Host "`n❌ Database password not found!" -ForegroundColor Red
    Write-Host "   Please provide password using:" -ForegroundColor Yellow
    Write-Host "   - Command-line: -DbPassword 'your_password'" -ForegroundColor Gray
    Write-Host "   - Environment: `$env:DEMO_POSTGRES_PASSWORD = 'your_password'" -ForegroundColor Gray
    exit 1
}

# Safety check: Always use opendental_demo
if ($dbName -ne "opendental_demo") {
    Write-Host "⚠️  WARNING: Database name is '$dbName', not 'opendental_demo'!" -ForegroundColor Yellow
    Write-Host "   Overriding to 'opendental_demo' for safety." -ForegroundColor Yellow
    $dbName = "opendental_demo"
}

# Display final configuration
Write-Host "`n📋 Final Configuration:" -ForegroundColor Cyan
Write-Host "  Patients: $Patients" -ForegroundColor White
Write-Host "  Database: $dbName" -ForegroundColor White
Write-Host "  Host: $dbHost" -ForegroundColor White
Write-Host "  Port: $dbPort" -ForegroundColor White
Write-Host "  User: $dbUser" -ForegroundColor White
Write-Host "  Password: ***" -ForegroundColor White
Write-Host "  Start Date: $StartDate" -ForegroundColor White

# Set environment variables for Python script
$env:DEMO_POSTGRES_HOST = $dbHost
$env:DEMO_POSTGRES_PORT = $dbPort.ToString()
$env:DEMO_POSTGRES_DB = $dbName
$env:DEMO_POSTGRES_USER = $dbUser
$env:DEMO_POSTGRES_PASSWORD = $dbPassword

Write-Host "`n🚀 Starting data generation..." -ForegroundColor Green
Write-Host ""

# Run the Python script
python main.py `
    --patients $Patients `
    --db-host $dbHost `
    --db-port $dbPort `
    --db-name $dbName `
    --db-user $dbUser `
    --db-password $dbPassword `
    --start-date $StartDate

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Data generation completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`n❌ Data generation failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}
