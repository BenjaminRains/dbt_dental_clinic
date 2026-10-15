<#
.SYNOPSIS
    Copies untracked local config into dated folders under C:\backup\dbt_dental_clinic_config.

.DESCRIPTION
    GitHub is the offsite copy of tracked code. This script backs up files that are not in
    git: tables.yml, deployment_credentials.json, and component .env files.

    Each run writes a new timestamped folder, then keeps the newest 5 folders.
    It does not delete C:\backup\dbt_dental_clinic (the old full-tree copy).
    It does not copy virtualenvs, logs, or .git.

.EXAMPLE
    .\scripts\utils\backup_dbt_dental_clinic.ps1
#>

$ErrorActionPreference = 'Stop'

$BackupRoot = 'C:\backup\dbt_dental_clinic_config'
$KeepNewest = 5
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$Stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$DestRoot = Join-Path $BackupRoot $Stamp

if (-not (Test-Path -LiteralPath $ProjectRoot)) {
    Write-Error "Project root not found: $ProjectRoot"
}

New-Item -ItemType Directory -Path $DestRoot -Force | Out-Null

function Copy-ConfigFile([string]$RelativePath) {
    $src = Join-Path $ProjectRoot $RelativePath
    if (-not (Test-Path -LiteralPath $src)) {
        return $false
    }
    $dest = Join-Path $DestRoot $RelativePath
    $destParent = Split-Path -Parent $dest
    if (-not (Test-Path -LiteralPath $destParent)) {
        New-Item -ItemType Directory -Path $destParent -Force | Out-Null
    }
    Copy-Item -LiteralPath $src -Destination $dest -Force
    return $true
}

$explicit = @(
    'etl_pipeline\etl_pipeline\config\tables.yml',
    'deployment_credentials.json',
    '.env',
    '.env_ec2',
    'consult_audio_pipe\.env',
    'frontend\.env.local',
    'frontend\.env.production'
)

$copied = New-Object System.Collections.Generic.List[string]
$missing = New-Object System.Collections.Generic.List[string]

foreach ($rel in $explicit) {
    if (Copy-ConfigFile $rel) {
        $copied.Add($rel)
    } else {
        $missing.Add($rel)
    }
}

$envDirs = @(
    'api',
    'etl_pipeline',
    'dbt_dental_models'
)
foreach ($dirName in $envDirs) {
    $dir = Join-Path $ProjectRoot $dirName
    if (-not (Test-Path -LiteralPath $dir)) { continue }
    $files = @(Get-ChildItem -LiteralPath $dir -File -Force -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like '.env_*' -and $_.Name -notlike '*.template' })
    foreach ($file in $files) {
        $rel = $file.FullName.Substring($ProjectRoot.Length).TrimStart('\')
        if (Copy-ConfigFile $rel) {
            $copied.Add($rel)
        }
    }
}

if ($copied.Count -eq 0) {
    Remove-Item -LiteralPath $DestRoot -Recurse -Force -ErrorAction SilentlyContinue
    Write-Error "No config files copied."
}

$dated = @(Get-ChildItem -LiteralPath $BackupRoot -Directory | Sort-Object Name -Descending)
if ($dated.Count -gt $KeepNewest) {
    foreach ($old in ($dated | Select-Object -Skip $KeepNewest)) {
        Remove-Item -LiteralPath $old.FullName -Recurse -Force
        Write-Host "Removed old config snapshot: $($old.Name)" -ForegroundColor DarkGray
    }
}

Write-Host "Config backup: $DestRoot ($($copied.Count) files)" -ForegroundColor Green
Write-Host "Kept newest $KeepNewest snapshots under $BackupRoot" -ForegroundColor Gray
if ($missing.Count -gt 0) {
    Write-Host ("Not present (skipped): " + ($missing -join ', ')) -ForegroundColor DarkGray
}
