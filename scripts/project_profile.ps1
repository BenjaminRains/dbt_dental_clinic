# Project-Specific PowerShell Profile — thin mdc aliases (Phase 5.5).

$projectDir = Get-Location

if (Test-Path "$projectDir\load_project.ps1") {
    . (Join-Path $projectDir "load_project.ps1")
} elseif (Test-Path "$projectDir\scripts\mdc_aliases.ps1") {
    . (Join-Path $projectDir "scripts\mdc_aliases.ps1")
} else {
    Write-Host "No project CLI scripts found in this directory." -ForegroundColor Gray
}
