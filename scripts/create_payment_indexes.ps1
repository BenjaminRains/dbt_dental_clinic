# Set environment variables
$env:DBT_PROFILES_DIR = $PWD
$env:DBT_PROFILE = "dbt_dental_clinic"  # Updated to match profiles.yml

# Run the index creation macro
Write-Host "Creating indexes for payment models..." -ForegroundColor Cyan
dbt run-operation create_payment_indexes --profiles-dir $env:DBT_PROFILES_DIR --profile $env:DBT_PROFILE

# Check if the operation was successful
if ($LASTEXITCODE -eq 0) {
    Write-Host "Indexes created successfully" -ForegroundColor Green
} else {
    Write-Host "Failed to create indexes" -ForegroundColor Red
    Write-Host "Error code: $LASTEXITCODE" -ForegroundColor Red
    Write-Host "Please check your dbt profile configuration and database permissions" -ForegroundColor Yellow
    exit 1
} 