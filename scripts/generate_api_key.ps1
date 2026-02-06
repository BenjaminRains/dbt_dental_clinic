#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Generate a secure API key for local development, demo (portfolio), or clinic (api-clinic.dbtdentalclinic.com).

.DESCRIPTION
    Naming (aligned with environment_manager.ps1): local | demo | clinic (avoid "production").
    - Default: local key → .ssh/dbt-dental-clinic-api-key.pem. Frontend local dev uses VITE_API_KEY (from .env or this + -UpdateFrontend).
    - Demo: use -Demo to generate DEMO_API_KEY and write to api/.env_api_demo (portfolio api.dbtdentalclinic.com).
    - Clinic: use -Clinic to generate CLINIC_API_KEY and write to api/.env_api_clinic (clinic frontend deploy and EC2 API for clinic host).

.EXAMPLE
    .\generate_api_key.ps1
.EXAMPLE
    .\generate_api_key.ps1 -UpdateFrontend
.EXAMPLE
    .\generate_api_key.ps1 -Demo
.EXAMPLE
    .\generate_api_key.ps1 -Clinic
#>

param(
    [Parameter(Mandatory=$false)]
    [switch]$UpdateFrontend,
    [Parameter(Mandatory=$false)]
    [switch]$Clinic,
    [Parameter(Mandatory=$false)]
    [switch]$Demo
)

# Get project root (assumes script is in scripts/ directory)
$scriptDir = Split-Path $PSScriptRoot -Parent
$projectRoot = if ($scriptDir) { $scriptDir } else { Get-Location }

# Generate cryptographically secure random API key (shared logic)
$bytes = New-Object byte[] 32
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$rng.GetBytes($bytes)
$apiKey = [Convert]::ToBase64String($bytes) -replace '\+', '-' -replace '/', '_' -replace '=', ''

# ---- Demo key path (demo = portfolio api.dbtdentalclinic.com) ----
if ($Demo) {
    Write-Host "`n🔐 Generating DEMO_API_KEY (demo = api.dbtdentalclinic.com, portfolio)" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   DEMO_API_KEY is used by:" -ForegroundColor Gray
    Write-Host "   - Portfolio API on EC2 (set DEMO_API_KEY in API .env)" -ForegroundColor Gray
    Write-Host "   - Frontend at dbtdentalclinic.com (build-time or runtime)" -ForegroundColor Gray
    Write-Host ""
    $apiDir = Join-Path $projectRoot "api"
    $envFile = Join-Path $apiDir ".env_api_demo"
    if (-not (Test-Path $apiDir)) {
        New-Item -ItemType Directory -Path $apiDir -Force | Out-Null
    }
    $updated = $false
    if (Test-Path $envFile) {
        $lines = Get-Content $envFile -Raw
        if ($lines -match 'DEMO_API_KEY=') {
            $lines = $lines -replace '(DEMO_API_KEY=)[^\r\n]*', "`${1}$apiKey"
            Set-Content -Path $envFile -Value $lines -NoNewline -Force
            $updated = $true
        }
    }
    if (-not $updated) {
        if (Test-Path $envFile) {
            Add-Content -Path $envFile -Value "`nDEMO_API_KEY=$apiKey"
        } else {
            Set-Content -Path $envFile -Value "# Demo API env - DEMO_API_KEY set by generate_api_key.ps1 -Demo`nDEMO_API_KEY=$apiKey" -Force
        }
    }
    Write-Host "✅ DEMO_API_KEY written to: $envFile" -ForegroundColor Green
    $preview = $apiKey.Substring(0, 8) + "..." + $apiKey.Substring($apiKey.Length - 8)
    Write-Host "   Key preview: $preview" -ForegroundColor Gray
    Write-Host "`n📝 Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Set same DEMO_API_KEY on EC2 API .env for portfolio host" -ForegroundColor Gray
    Write-Host ""
    exit 0
}

# ---- Clinic key path (nomenclature: clinic = api-clinic.dbtdentalclinic.com, per environment_manager.ps1) ----
if ($Clinic) {
    Write-Host "`n🔐 Generating CLINIC_API_KEY (clinic = api-clinic.dbtdentalclinic.com)" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   CLINIC_API_KEY is used by:" -ForegroundColor Gray
    Write-Host "   - clinic-frontend-deploy (build-time VITE_API_KEY from this file)" -ForegroundColor Gray
    Write-Host "   - API on EC2 for clinic host (set CLINIC_API_KEY in API .env)" -ForegroundColor Gray
    Write-Host ""
    $apiDir = Join-Path $projectRoot "api"
    $envFile = Join-Path $apiDir ".env_api_clinic"
    if (-not (Test-Path $apiDir)) {
        New-Item -ItemType Directory -Path $apiDir -Force | Out-Null
    }
    # Update only CLINIC_API_KEY line in existing .env_api_clinic; preserve rest of file
    $updated = $false
    if (Test-Path $envFile) {
        $lines = Get-Content $envFile -Raw
        if ($lines -match 'CLINIC_API_KEY=') {
            $lines = $lines -replace '(CLINIC_API_KEY=)[^\r\n]*', "`${1}$apiKey"
            Set-Content -Path $envFile -Value $lines -NoNewline -Force
            $updated = $true
        }
    }
    if (-not $updated) {
        if (Test-Path $envFile) {
            Add-Content -Path $envFile -Value "`nCLINIC_API_KEY=$apiKey"
        } else {
            Set-Content -Path $envFile -Value "# Clinic API env - CLINIC_API_KEY set by generate_api_key.ps1 -Clinic`nCLINIC_API_KEY=$apiKey" -Force
        }
    }
    try {
        Write-Host "✅ CLINIC_API_KEY written to: $envFile" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to write $envFile : $_" -ForegroundColor Red
        exit 1
    }
    $preview = $apiKey.Substring(0, 8) + "..." + $apiKey.Substring($apiKey.Length - 8)
    Write-Host "   Key preview: $preview" -ForegroundColor Gray
    Write-Host "`n📝 Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Run clinic-frontend-deploy (loads CLINIC_API_KEY from api\.env_api_clinic)" -ForegroundColor Gray
    Write-Host "   2. Phase 2: set same CLINIC_API_KEY on EC2 API .env for clinic host" -ForegroundColor Gray
    Write-Host ""
    exit 0
}

# ---- Local development key path ----
$sshDir = Join-Path $projectRoot ".ssh"
$apiKeyFile = Join-Path $sshDir "dbt-dental-clinic-api-key.pem"

Write-Host "`n🔐 Generating API key (this run: local development)" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "   This run will create/overwrite the LOCAL key (.ssh/dbt-dental-clinic-api-key.pem)." -ForegroundColor Gray
Write-Host "   Naming (per environment_manager.ps1): local | demo | clinic" -ForegroundColor Gray
Write-Host "   - Demo key: run with -Demo to set DEMO_API_KEY in api\.env_api_demo" -ForegroundColor Gray
Write-Host "   - Clinic key: run with -Clinic to set CLINIC_API_KEY in api\.env_api_clinic" -ForegroundColor Gray
Write-Host ""
$confirm = Read-Host "Continue with local development key generation? (y/N)"
if ($confirm -ne 'y' -and $confirm -ne 'Y') {
    Write-Host "Cancelled." -ForegroundColor Gray
    exit 0
}
Write-Host ""

if (-not (Test-Path $sshDir)) {
    Write-Host "📁 Creating .ssh directory..." -ForegroundColor Gray
    New-Item -ItemType Directory -Path $sshDir -Force | Out-Null
}

Write-Host "🔑 Generating cryptographically secure API key..." -ForegroundColor Gray
Write-Host "✅ API key generated successfully" -ForegroundColor Green
Write-Host "   Length: $($apiKey.Length) characters" -ForegroundColor Gray
Write-Host "   Format: Base64url (URL-safe)" -ForegroundColor Gray

try {
    $apiKey | Out-File -FilePath $apiKeyFile -Encoding UTF8 -NoNewline -Force
    Write-Host "`n📄 API key saved to: $apiKeyFile" -ForegroundColor Green
    try {
        $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        icacls $apiKeyFile /inheritance:r /grant "${currentUser}:F" | Out-Null
        Write-Host "🔒 File permissions set (restricted to current user)" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Could not set file permissions: $_" -ForegroundColor Yellow
    }
} catch {
    Write-Host "❌ Failed to write API key: $_" -ForegroundColor Red
    exit 1
}

$preview = $apiKey.Substring(0, 8) + "..." + $apiKey.Substring($apiKey.Length - 8)
Write-Host "`n📋 API Key Preview: $preview" -ForegroundColor Cyan
Write-Host "   (Full key stored in: $apiKeyFile)" -ForegroundColor Gray

if ($UpdateFrontend) {
    $frontendEnvFile = Join-Path $projectRoot "frontend" ".env"
    $frontendDir = Join-Path $projectRoot "frontend"
    if (Test-Path $frontendDir) {
        Write-Host "`n🔄 Updating frontend .env file..." -ForegroundColor Gray
        $envContent = @"
# Frontend Environment Variables
# Auto-generated by generate_api_key.ps1

# API Configuration
VITE_API_URL=http://localhost:8000

# Local dev: VITE_API_KEY for frontend (API key from .ssh/dbt-dental-clinic-api-key.pem)
VITE_API_KEY=$apiKey
"@
        try {
            Set-Content -Path $frontendEnvFile -Value $envContent -Force
            Write-Host "✅ Updated frontend .env file" -ForegroundColor Green
        } catch {
            Write-Host "⚠️  Could not update frontend .env: $_" -ForegroundColor Yellow
        }
    } else {
        Write-Host "⚠️  Frontend directory not found, skipping .env update" -ForegroundColor Yellow
    }
}

Write-Host "`n✅ Local development API key generation complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "`n📝 Next Steps (LOCAL DEVELOPMENT):" -ForegroundColor Yellow
Write-Host "   1. API will automatically load key from: $apiKeyFile" -ForegroundColor Gray
if (-not $UpdateFrontend) {
    Write-Host "   2. Update frontend .env with VITE_API_KEY=(value from $apiKeyFile)" -ForegroundColor Gray
    Write-Host "      Or run: .\generate_api_key.ps1 -UpdateFrontend" -ForegroundColor Gray
}
Write-Host "   3. Restart API server to load new key" -ForegroundColor Gray
Write-Host "   4. Restart frontend dev server if updated" -ForegroundColor Gray
Write-Host ""
Write-Host "   For demo key (DEMO_API_KEY): .\generate_api_key.ps1 -Demo" -ForegroundColor Gray
Write-Host "   For clinic key (CLINIC_API_KEY): .\generate_api_key.ps1 -Clinic" -ForegroundColor Gray
Write-Host ""

