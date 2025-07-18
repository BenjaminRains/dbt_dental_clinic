# Load Project Profile
# This script loads the project-specific environment manager

Write-Host "🔄 Loading project profile..." -ForegroundColor Cyan

# Load the project profile
. .\scripts\project_profile.ps1

Write-Host "✅ Project profile loaded!" -ForegroundColor Green 