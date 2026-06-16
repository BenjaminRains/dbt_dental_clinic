# Project-Specific PowerShell Profile
# Loads thin mdc aliases (Phase 4.6). Use load_project.ps1 -Legacy for deploy/SSM/frontend.

$projectDir = Get-Location

if (Test-Path "$projectDir\scripts\mdc_aliases.ps1") {
    $loadScript = Join-Path $projectDir "load_project.ps1"
    if (Test-Path $loadScript) {
        . $loadScript
    } else {
        . (Join-Path $projectDir "scripts\mdc_aliases.ps1")
    }
} elseif (Test-Path "$projectDir\scripts\environment_manager.ps1") {
    Write-Host "Loading legacy environment manager (load_project.ps1 not found)..." -ForegroundColor Yellow
    . (Join-Path $projectDir "scripts\environment_manager.ps1")
} else {
    Write-Host "No project CLI scripts found in this directory." -ForegroundColor Gray
}
