<#
.SYNOPSIS
    Backs up the dbt_dental_clinic project to C:\backup\dbt_dental_clinic (overwrites existing).

.DESCRIPTION
    Routine backup script: copies the entire project to C:\backup\dbt_dental_clinic.
    The destination folder is removed and recreated so each run is a full overwrite.
    Run this script regularly (e.g., before major changes or on a schedule) to keep
    a local backup of the codebase.

.EXAMPLE
    .\backup_dbt_dental_clinic.ps1
#>

$ErrorActionPreference = 'Stop'

$BackupRoot = 'C:\backup'
$BackupFolder = Join-Path $BackupRoot 'dbt_dental_clinic'
$ProjectRoot = Split-Path $PSScriptRoot -Parent

Write-Host "Backup: dbt_dental_clinic -> $BackupFolder" -ForegroundColor Cyan
Write-Host "Source: $ProjectRoot" -ForegroundColor Gray

if (-not (Test-Path $ProjectRoot)) {
    Write-Error "Project root not found: $ProjectRoot"
}

# Ensure backup root exists
if (-not (Test-Path $BackupRoot)) {
    New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null
    Write-Host "Created: $BackupRoot" -ForegroundColor Gray
}

# Remove existing backup so we overwrite with a clean copy
if (Test-Path $BackupFolder) {
    Write-Host "Removing existing backup..." -ForegroundColor Yellow
    Remove-Item -Path $BackupFolder -Recurse -Force
}

Write-Host "Copying project..." -ForegroundColor Yellow
Copy-Item -Path $ProjectRoot -Destination $BackupFolder -Recurse -Force

Write-Host "Backup complete: $BackupFolder" -ForegroundColor Green
