# PowerShell script to test Treatment Acceptance API endpoints
# This helps diagnose server errors on the Treatment Acceptance dashboard

param(
    [string]$ApiKey = "",
    [string]$BaseUrl = "https://api.dbtdentalclinic.com",
    [string]$StartDate = "2026-01-01",
    [string]$EndDate = "2026-01-31"
)

$ErrorActionPreference = "Stop"

Write-Host "`n🔍 Treatment Acceptance API Diagnostic Tool" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Try to load API key - prioritize DEMO_API_KEY for production URLs
if (-not $ApiKey) {
    # For production API, check DEMO_API_KEY first
    if ($BaseUrl -like "*api.dbtdentalclinic.com*" -or $BaseUrl -like "*dbtdentalclinic.com*") {
        # First check environment variable
        $envKey = $env:DEMO_API_KEY
        if ($envKey) {
            Write-Host "`n✅ Using DEMO_API_KEY from environment variable" -ForegroundColor Green
            $ApiKey = $envKey
        } else {
            # Try loading from .env_api_demo file (previously .env_api_production)
            $projectRoot = Split-Path -Parent $PSScriptRoot
            $envFile = Join-Path $projectRoot "api" ".env_api_demo"
            if (Test-Path $envFile) {
                Write-Host "`n📄 Loading DEMO_API_KEY from: $envFile" -ForegroundColor Gray
                try {
                    Get-Content $envFile | ForEach-Object {
                        if ($_ -match '^DEMO_API_KEY\s*=\s*(.+)$' -and $_ -notmatch '^\s*#') {
                            $ApiKey = $matches[1].Trim()
                            Write-Host "✅ DEMO_API_KEY loaded from .env_api_demo" -ForegroundColor Green
                        }
                    }
                } catch {
                    Write-Host "⚠️  Could not read DEMO_API_KEY from file: $_" -ForegroundColor Yellow
                }
            }
        }
    }
    
    # If still no key, try loading from PEM file (for local/test environments)
    if (-not $ApiKey) {
        $projectRoot = Split-Path -Parent $PSScriptRoot
        $pemFile = Join-Path $projectRoot ".ssh" "dbt-dental-clinic-api-key.pem"
        
        if (Test-Path $pemFile) {
            Write-Host "`n📄 Loading API key from: $pemFile" -ForegroundColor Gray
            Write-Host "   ⚠️  Note: This key is for local/test environments" -ForegroundColor Yellow
            Write-Host "   For production API, use DEMO_API_KEY environment variable" -ForegroundColor Yellow
            try {
                $keyContent = Get-Content $pemFile -Raw
                $lines = $keyContent -split "`n"
                $keyLines = $lines | Where-Object { 
                    $_ -and 
                    -not $_.StartsWith('-----BEGIN') -and 
                    -not $_.StartsWith('-----END') 
                }
                $ApiKey = ($keyLines | ForEach-Object { $_.Trim() }) -join ''
                Write-Host "✅ API key loaded from file" -ForegroundColor Green
            } catch {
                Write-Host "⚠️  Could not read API key from file: $_" -ForegroundColor Yellow
            }
        }
    }
}

# If still no API key, prompt user
if (-not $ApiKey) {
    Write-Host "`n⚠️  API key not found. You can:" -ForegroundColor Yellow
    Write-Host "   1. Set DEMO_API_KEY environment variable (recommended for production)" -ForegroundColor Gray
    Write-Host "   2. Provide it via -ApiKey parameter" -ForegroundColor Gray
    Write-Host "   3. Enter it when prompted" -ForegroundColor Gray
    Write-Host "`n   For production API (api.dbtdentalclinic.com), you need DEMO_API_KEY" -ForegroundColor Cyan
    Write-Host "   This is different from the local .pem file key." -ForegroundColor Cyan
    
    $envKey = $env:DEMO_API_KEY
    if ($envKey) {
        Write-Host "`n✅ Using DEMO_API_KEY from environment" -ForegroundColor Green
        $ApiKey = $envKey
    } else {
        $ApiKey = Read-Host "`nEnter API key (or press Enter to test without key)"
    }
}

Write-Host "`n📋 Configuration:" -ForegroundColor Cyan
Write-Host "   Base URL: $BaseUrl" -ForegroundColor Gray
Write-Host "   Start Date: $StartDate" -ForegroundColor Gray
Write-Host "   End Date: $EndDate" -ForegroundColor Gray
Write-Host "   API Key: $(if ($ApiKey) { $ApiKey.Substring(0, [Math]::Min(8, $ApiKey.Length)) + '...' } else { 'Not provided' })" -ForegroundColor Gray

# Function to test an endpoint
function Test-Endpoint {
    param(
        [string]$Name,
        [string]$Path,
        [hashtable]$Params = @{}
    )
    
    Write-Host "`n🧪 Testing: $Name" -ForegroundColor Yellow
    Write-Host "   Endpoint: $Path" -ForegroundColor Gray
    
    # Build query string
    $queryParams = @{
        start_date = $StartDate
        end_date = $EndDate
    }
    
    # Add endpoint-specific parameters
    foreach ($key in $Params.Keys) {
        $queryParams[$key] = $Params[$key]
    }
    
    # Build query string with proper URL encoding
    $queryParts = @()
    foreach ($param in $queryParams.GetEnumerator()) {
        $key = [System.Uri]::EscapeDataString($param.Key)
        $value = [System.Uri]::EscapeDataString($param.Value)
        $queryParts += "$key=$value"
    }
    $queryString = $queryParts -join "&"
    
    # Construct full URL properly - ensure path starts with /
    if (-not $Path.StartsWith("/")) {
        $Path = "/" + $Path
    }
    
    # Build URL with proper query string
    if ($queryString) {
        $fullUrl = $BaseUrl.TrimEnd('/') + $Path + "?" + $queryString
    } else {
        $fullUrl = $BaseUrl.TrimEnd('/') + $Path
    }
    Write-Host "   Full URL: $fullUrl" -ForegroundColor DarkGray
    
    # Prepare headers
    $headers = @{
        "Content-Type" = "application/json"
    }
    
    if ($ApiKey) {
        $headers["X-API-Key"] = $ApiKey
    }
    
    try {
        # Use Invoke-RestMethod for better JSON handling and error details
        $response = Invoke-RestMethod -Uri $fullUrl -Headers $headers -Method Get -ErrorAction Stop
        
        Write-Host "   ✅ Status: 200 OK" -ForegroundColor Green
        
        # Invoke-RestMethod automatically parses JSON
        $jsonData = $response
        Write-Host "   📊 Response Type: $($jsonData.GetType().Name)" -ForegroundColor Gray
        
        if ($jsonData -is [System.Array]) {
            Write-Host "   📊 Records: $($jsonData.Count)" -ForegroundColor Gray
            if ($jsonData.Count -gt 0) {
                Write-Host "   📋 Sample keys: $($jsonData[0].PSObject.Properties.Name -join ', ')" -ForegroundColor DarkGray
            }
        } elseif ($jsonData -is [PSCustomObject]) {
            Write-Host "   📋 Keys: $($jsonData.PSObject.Properties.Name -join ', ')" -ForegroundColor DarkGray
        }
        
        # Show first few records or summary
        if ($jsonData -is [System.Array] -and $jsonData.Count -gt 0) {
            Write-Host "`n   First record:" -ForegroundColor DarkGray
            $jsonData[0] | ConvertTo-Json -Depth 2 | ForEach-Object { 
                Write-Host "   $_" -ForegroundColor DarkGray 
            }
        } elseif ($jsonData -is [PSCustomObject]) {
            Write-Host "`n   Response:" -ForegroundColor DarkGray
            $jsonData | ConvertTo-Json -Depth 3 | ForEach-Object { 
                Write-Host "   $_" -ForegroundColor DarkGray 
            }
        }
        
        return @{
            Success = $true
            StatusCode = 200
            Data = $jsonData
        }
        
    } catch {
        # Invoke-RestMethod puts error details in ErrorDetails.Message
        $statusCode = $null
        $errorDetail = $null
        
        # Try to extract status code from exception
        if ($_.Exception.Response) {
            try {
                $statusCode = [int]$_.Exception.Response.StatusCode.value__
            } catch {
                # Try alternative method
                if ($_.Exception.Response.StatusCode) {
                    $statusCode = [int]$_.Exception.Response.StatusCode
                }
            }
        }
        
        # Get error details - Invoke-RestMethod puts JSON error in ErrorDetails.Message
        if ($_.ErrorDetails) {
            $errorDetail = $_.ErrorDetails.Message
            try {
                $errorJson = $errorDetail | ConvertFrom-Json
                if ($errorJson.detail) {
                    $errorDetail = $errorJson.detail
                }
            } catch {
                # Not JSON, use as-is
            }
        } elseif ($_.Exception.Message) {
            $errorDetail = $_.Exception.Message
        }
        
        if ($statusCode) {
            Write-Host "   ❌ Error: $statusCode" -ForegroundColor Red
        } else {
            Write-Host "   ❌ Error occurred" -ForegroundColor Red
        }
        
        if ($errorDetail) {
            Write-Host "`n   Error Details: $errorDetail" -ForegroundColor Red
        }
        
        Write-Host "`n   Full Exception: $($_.Exception.Message)" -ForegroundColor DarkRed
        
        return @{
            Success = $false
            StatusCode = if ($statusCode) { $statusCode } else { 0 }
            Error = if ($errorDetail) { $errorDetail } else { $_.Exception.Message }
        }
    }
}

# Test all three endpoints
Write-Host "`n" + ("=" * 60) -ForegroundColor Gray

$results = @{}

# Test 1: KPI Summary
$results["KPI Summary"] = Test-Endpoint -Name "KPI Summary" -Path "/treatment-acceptance/kpi-summary"

# Test 2: Trends
$results["Trends"] = Test-Endpoint -Name "Trends" -Path "/treatment-acceptance/trends" -Params @{
    group_by = "month"
}

# Test 3: Provider Performance
$results["Provider Performance"] = Test-Endpoint -Name "Provider Performance" -Path "/treatment-acceptance/provider-performance"

# Summary
Write-Host "`n" + ("=" * 60) -ForegroundColor Gray
Write-Host "📊 Summary" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

$successCount = ($results.Values | Where-Object { $_.Success }).Count
$totalCount = $results.Count

foreach ($key in $results.Keys) {
    $result = $results[$key]
    $status = if ($result.Success) { "✅" } else { "❌" }
    $statusColor = if ($result.Success) { "Green" } else { "Red" }
    Write-Host "$status $key : $(if ($result.Success) { "Success ($($result.StatusCode))" } else { "Failed ($($result.StatusCode))" })" -ForegroundColor $statusColor
}

Write-Host "`nResults: $successCount/$totalCount endpoints succeeded" -ForegroundColor $(if ($successCount -eq $totalCount) { "Green" } else { "Yellow" })

if ($successCount -lt $totalCount) {
    Write-Host "`n⚠️  Some endpoints failed. Check the error details above." -ForegroundColor Yellow
    Write-Host "   Common issues:" -ForegroundColor Gray
    Write-Host "   - Database connection problems" -ForegroundColor Gray
    Write-Host "   - Missing or invalid API key" -ForegroundColor Gray
    Write-Host "   - SQL query errors in the service layer" -ForegroundColor Gray
    Write-Host "   - Missing database tables or views" -ForegroundColor Gray
}

Write-Host "`n"
