# Load environment variables from .env file
$envPath = Join-Path $PSScriptRoot "..\.env"

if (-not (Test-Path $envPath)) {
    Write-Host "❌ .env file not found at: $envPath" -ForegroundColor Red
    Write-Host "Please ensure the .env file exists in the etl_pipeline directory" -ForegroundColor Yellow
    exit 1
}

# Read the .env file
$envContent = Get-Content $envPath -Raw

# Extract and set environment variables
$envContent -split "`n" | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}

Write-Host "✅ Environment variables loaded from .env" -ForegroundColor Green 