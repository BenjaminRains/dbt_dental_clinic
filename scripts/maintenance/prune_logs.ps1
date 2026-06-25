<#
.SYNOPSIS
    Prune old log files across the monorepo by age (and optional keep-newest rules).

.DESCRIPTION
    Removes files older than per-category retention windows. By default files are sent to
    the Windows Recycle Bin (recoverable). Use -Permanent for hard delete. Uses
    LastWriteTime only (not filename parsing). Directories are never removed.

    Canonical layout: etl_pipeline/logs/ (see etl_pipeline/etl_pipeline/config/paths.py).
    Airflow task logs: airflow/logs/. Legacy repo-root logs/ is pruned with a warning.

    Run weekly on the Airflow host (Task Scheduler) or ad hoc before disk gets tight.

.PARAMETER DryRun
    List files that would be removed without touching them.

.PARAMETER Permanent
    Permanently delete files (Remove-Item). Default is Recycle Bin on Windows.

.PARAMETER Category
    Run one or more categories only. Default: all. Use -ListCategories to see names.

.PARAMETER WarnSizeMB
    Emit a warning when a monitored root exceeds this size in MB (default 500).

.EXAMPLE
    .\scripts\maintenance\prune_logs.ps1 -DryRun

.EXAMPLE
    .\scripts\maintenance\prune_logs.ps1 -Category etl_test,airflow

.EXAMPLE
    .\scripts\maintenance\prune_logs.ps1 -EtlRunDays 90 -EtlTestDays 14
#>

param(
    [switch]$DryRun,
    [switch]$Permanent,
    [switch]$ListCategories,
    [string[]]$Category = @(),
    [int]$WarnSizeMB = 500,

    # Per-category retention overrides (days). Defaults match LOGGING_MAINTENANCE plan.
    [int]$EtlRunDays = 90,
    [int]$EtlTestDays = 14,
    [int]$SchemaSessionDays = 30,
    [int]$SchemaReportsDays = 365,
    [int]$SchemaBackupDays = 90,
    [int]$SchemaBackupKeepNewest = 20,
    [int]$CompareDatabasesDays = 30,
    [int]$AirflowDays = 60,
    [int]$ApiDevDays = 30,
    [int]$DbtModelsDays = 30,
    [int]$LegacyRepoLogsDays = 90,
    [int]$StaleScriptLogsDays = 30,
    [int]$ConsultAudioDays = 90
)

$ErrorActionPreference = "Stop"

# Allow -Category etl_test,airflow (single comma-separated argument)
if ($Category.Count -eq 1 -and $Category[0] -match ",") {
    $Category = $Category[0].Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

$script:RecycleBinAvailable = $false
if (-not $Permanent -and $env:OS -eq "Windows_NT") {
    try {
        Add-Type -AssemblyName Microsoft.VisualBasic -ErrorAction Stop
        $script:RecycleBinAvailable = $true
    } catch {
        Write-Warning "Recycle Bin API unavailable; will permanently delete files. Use -DryRun to preview."
    }
}

function Get-RemovalVerb {
    if ($Permanent) { return "delete" }
    if ($script:RecycleBinAvailable) { return "recycle" }
    return "delete"
}

function Remove-PrunedFile([string]$LiteralPath) {
    if ($Permanent -or -not $script:RecycleBinAvailable) {
        Remove-Item -LiteralPath $LiteralPath -Force
        return
    }
    [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile(
        $LiteralPath,
        [Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs,
        [Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin
    )
}

function Format-SizeMB([long]$Bytes) {
    if ($null -eq $Bytes -or $Bytes -lt 0) { return "0" }
    return "{0:N2}" -f ($Bytes / 1MB)
}

function Get-DirectoryStats([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        return @{ Exists = $false; FileCount = 0; SizeBytes = 0; Oldest = $null; Newest = $null }
    }
    $files = @(Get-ChildItem -LiteralPath $Path -Recurse -File -ErrorAction SilentlyContinue)
    if ($files.Count -eq 0) {
        return @{ Exists = $true; FileCount = 0; SizeBytes = 0; Oldest = $null; Newest = $null }
    }
    $sorted = $files | Sort-Object LastWriteTime
    return @{
        Exists     = $true
        FileCount  = $files.Count
        SizeBytes  = ($files | Measure-Object -Property Length -Sum).Sum
        Oldest     = $sorted[0].LastWriteTime
        Newest     = $sorted[-1].LastWriteTime
    }
}

function Test-CategorySelected([string]$Name, [string[]]$Selected) {
    if ($null -eq $Selected -or $Selected.Count -eq 0) { return $true }
    return $Name -in $Selected
}

function Invoke-PruneFiles {
    param(
        [string]$CategoryName,
        [string]$TargetPath,
        [int]$RetentionDays,
        [int]$KeepNewest = 0,
        [string]$Note = ""
    )

    $result = [ordered]@{
        Category      = $CategoryName
        Path          = $TargetPath
        RetentionDays = $RetentionDays
        KeepNewest    = $KeepNewest
        Exists        = $false
        Candidates    = 0
        Deleted       = 0
        ReclaimedMB   = 0.0
        SkippedNewest = 0
        Note          = $Note
    }

    if (-not (Test-Path -LiteralPath $TargetPath)) {
        $result.Note = "path missing"
        return [pscustomobject]$result
    }

    $result.Exists = $true
    $cutoff = (Get-Date).AddDays(-$RetentionDays)
    $allFiles = @(Get-ChildItem -LiteralPath $TargetPath -Recurse -File -ErrorAction SilentlyContinue)
    if ($allFiles.Count -eq 0) {
        return [pscustomobject]$result
    }

    $protected = @{}
    if ($KeepNewest -gt 0) {
        $newest = $allFiles | Sort-Object LastWriteTime -Descending | Select-Object -First $KeepNewest
        foreach ($f in $newest) {
            $protected[$f.FullName] = $true
        }
    }

    $toDelete = @()
    foreach ($file in $allFiles) {
        if ($protected.ContainsKey($file.FullName)) {
            $result.SkippedNewest++
            continue
        }
        if ($file.LastWriteTime -lt $cutoff) {
            $toDelete += $file
        }
    }

    $result.Candidates = $toDelete.Count
    $actionVerb = Get-RemovalVerb
    foreach ($file in $toDelete) {
        $size = $file.Length
        if ($DryRun) {
            Write-Host "  [dry-run] would $actionVerb $($file.FullName) ($((Format-SizeMB $size)) MB, $($file.LastWriteTime))" -ForegroundColor DarkGray
        } else {
            Remove-PrunedFile -LiteralPath $file.FullName
        }
        $result.Deleted++
        $result.ReclaimedMB += ($size / 1MB)
    }

    return [pscustomobject]$result
}

# Category registry: Name -> { RelativePath, RetentionDays, KeepNewest, Note }
$CategoryDefs = @(
    @{
        Name          = "etl_run"
        RelativePath  = "etl_pipeline\logs\etl_pipeline"
        RetentionDays = $EtlRunDays
        KeepNewest    = 0
        Note          = "Per-run ETL logs (canonical)"
    },
    @{
        Name          = "etl_test"
        RelativePath  = "etl_pipeline\logs\tests"
        RetentionDays = $EtlTestDays
        KeepNewest    = 0
        Note          = "Test run logs"
    },
    @{
        Name          = "schema_session"
        RelativePath  = "etl_pipeline\logs\schema_analysis\logs"
        RetentionDays = $SchemaSessionDays
        KeepNewest    = 0
        Note          = "Schema analyzer session logs"
    },
    @{
        Name          = "schema_reports"
        RelativePath  = "etl_pipeline\logs\schema_analysis\reports"
        RetentionDays = $SchemaReportsDays
        KeepNewest    = 0
        Note          = "Schema change reports and changelogs"
    },
    @{
        Name          = "schema_backups"
        RelativePath  = "etl_pipeline\logs\schema_analysis\backups"
        RetentionDays = $SchemaBackupDays
        KeepNewest    = $SchemaBackupKeepNewest
        Note          = "tables.yml backups; git is source of truth"
    },
    @{
        Name          = "compare_databases"
        RelativePath  = "etl_pipeline\logs\compare_databases"
        RetentionDays = $CompareDatabasesDays
        KeepNewest    = 0
        Note          = "compare_databases script output"
    },
    @{
        Name          = "airflow"
        RelativePath  = "airflow\logs"
        RetentionDays = $AirflowDays
        KeepNewest    = 0
        Note          = "Airflow task, scheduler, DAG processor logs"
    },
    @{
        Name          = "api_dev"
        RelativePath  = "api\logs"
        RetentionDays = $ApiDevDays
        KeepNewest    = 0
        Note          = "Local API debug logs (rate_limit_debug_*.log)"
    },
    @{
        Name          = "dbt_models"
        RelativePath  = "dbt_dental_models\logs"
        RetentionDays = $DbtModelsDays
        KeepNewest    = 0
        Note          = "dbt project logs (dbt also rotates at ~10 MB)"
    },
    @{
        Name          = "legacy_repo_logs"
        RelativePath  = "logs"
        RetentionDays = $LegacyRepoLogsDays
        KeepNewest    = 0
        Note          = "Legacy repo-root logs/ (prefer etl_pipeline/logs/)"
    },
    @{
        Name          = "stale_script_logs"
        RelativePath  = "etl_pipeline\scripts\logs"
        RetentionDays = $StaleScriptLogsDays
        KeepNewest    = 0
        Note          = "Duplicate artifacts from running scripts with CWD=scripts/"
    },
    @{
        Name          = "consult_audio"
        RelativePath  = "consult_audio_pipe\logs"
        RetentionDays = $ConsultAudioDays
        KeepNewest    = 0
        Note          = "Consult audio pipeline run logs"
    }
)

if ($ListCategories) {
    Write-Host ""
    Write-Host "Log prune categories" -ForegroundColor Cyan
    Write-Host ("-" * 72) -ForegroundColor Gray
    foreach ($def in $CategoryDefs) {
        $keep = if ($def.KeepNewest -gt 0) { ", keep newest $($def.KeepNewest)" } else { "" }
        Write-Host ("  {0,-22} {1,4} days{2}" -f $def.Name, $def.RetentionDays, $keep) -ForegroundColor White
        Write-Host ("    $($def.RelativePath)") -ForegroundColor DarkGray
        Write-Host ("    $($def.Note)") -ForegroundColor DarkGray
    }
    Write-Host ""
    exit 0
}

Write-Host ""
Write-Host "Log maintenance prune" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Gray
Write-Host "  Repo:    $RepoRoot" -ForegroundColor White
$disposeMode = if ($DryRun) {
    "DRY RUN (no files removed)"
} elseif ($Permanent) {
    "PERMANENT DELETE"
} elseif ($script:RecycleBinAvailable) {
    "RECYCLE BIN"
} else {
    "PERMANENT DELETE (Recycle Bin unavailable)"
}
Write-Host "  Mode:    $disposeMode" -ForegroundColor $(if ($DryRun) { "Yellow" } else { "White" })
if ($Category.Count -gt 0) {
    Write-Host "  Filter:  $($Category -join ', ')" -ForegroundColor White
}
Write-Host ""

$legacySchema = Join-Path $RepoRoot "logs\schema_analysis"
if (Test-Path -LiteralPath $legacySchema) {
    Write-Host "  WARNING: Legacy logs/schema_analysis still exists. New artifacts go to etl_pipeline/logs/schema_analysis/." -ForegroundColor Yellow
    Write-Host "           Review and remove the legacy tree after confirming nothing is needed." -ForegroundColor Yellow
    Write-Host ""
}

$sizeWatchRoots = @(
    @{ Label = "etl_pipeline/logs"; Path = Join-Path $RepoRoot "etl_pipeline\logs"; LimitMB = $WarnSizeMB },
    @{ Label = "airflow/logs"; Path = Join-Path $RepoRoot "airflow\logs"; LimitMB = [math]::Max(200, [int]($WarnSizeMB / 2)) }
)

Write-Host "Size check" -ForegroundColor Cyan
Write-Host ("-" * 72) -ForegroundColor Gray
foreach ($watch in $sizeWatchRoots) {
    $stats = Get-DirectoryStats $watch.Path
    if (-not $stats.Exists) {
        Write-Host "  $($watch.Label): (missing)" -ForegroundColor DarkGray
        continue
    }
    $sizeMB = [double](Format-SizeMB $stats.SizeBytes)
    $color = if ($sizeMB -ge $watch.LimitMB) { "Yellow" } else { "Green" }
    Write-Host ("  {0}: {1} MB, {2} files" -f $watch.Label, (Format-SizeMB $stats.SizeBytes), $stats.FileCount) -ForegroundColor $color
    if ($sizeMB -ge $watch.LimitMB) {
        Write-Host "    exceeds warning threshold ($($watch.LimitMB) MB)" -ForegroundColor Yellow
    }
}
Write-Host ""

$results = @()
Write-Host "Pruning" -ForegroundColor Cyan
Write-Host ("-" * 72) -ForegroundColor Gray

foreach ($def in $CategoryDefs) {
    if (-not (Test-CategorySelected $def.Name $Category)) {
        continue
    }

    $target = Join-Path $RepoRoot $def.RelativePath
    Write-Host ""
    Write-Host "[$($def.Name)] $target" -ForegroundColor Yellow
    Write-Host "  retention=$($def.RetentionDays) days$(if ($def.KeepNewest -gt 0) { ", keep newest $($def.KeepNewest)" })" -ForegroundColor Gray

    $r = Invoke-PruneFiles `
        -CategoryName $def.Name `
        -TargetPath $target `
        -RetentionDays $def.RetentionDays `
        -KeepNewest $def.KeepNewest `
        -Note $def.Note

    $results += $r

    if (-not $r.Exists) {
        Write-Host "  skipped (path missing)" -ForegroundColor DarkGray
    } elseif ($r.Candidates -eq 0) {
        Write-Host "  nothing to prune" -ForegroundColor DarkGray
    } else {
        $verb = if ($DryRun) { "would $(Get-RemovalVerb)" } else { "$(Get-RemovalVerb)d" }
        Write-Host "  $verb $($r.Deleted) file(s), reclaimed $("{0:N2}" -f $r.ReclaimedMB) MB" -ForegroundColor $(if ($DryRun) { "Yellow" } else { "Green" })
        if ($r.SkippedNewest -gt 0) {
            Write-Host "  protected $($r.SkippedNewest) newest file(s) from age rule" -ForegroundColor DarkGray
        }
    }
}

Write-Host ""
Write-Host "Summary" -ForegroundColor Cyan
Write-Host ("-" * 72) -ForegroundColor Gray

$totalDeleted = ($results | Measure-Object -Property Deleted -Sum).Sum
$totalReclaimed = ($results | Measure-Object -Property ReclaimedMB -Sum).Sum
$summaryVerb = if ($DryRun) { "Would $(Get-RemovalVerb)" } else { "$(Get-RemovalVerb)d" }

Write-Host "  $summaryVerb $totalDeleted file(s), total $("{0:N2}" -f $totalReclaimed) MB" -ForegroundColor $(if ($DryRun) { "Yellow" } else { "Green" })
if (-not $DryRun -and -not $Permanent -and $script:RecycleBinAvailable -and $totalDeleted -gt 0) {
    Write-Host "  Files are in the Recycle Bin until you empty it." -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Post-prune sizes" -ForegroundColor Cyan
Write-Host ("-" * 72) -ForegroundColor Gray
foreach ($watch in $sizeWatchRoots) {
    $stats = Get-DirectoryStats $watch.Path
    if ($stats.Exists -and $stats.FileCount -gt 0) {
        Write-Host ("  {0}: {1} MB ({2} files), oldest={3}, newest={4}" -f `
            $watch.Label,
            (Format-SizeMB $stats.SizeBytes),
            $stats.FileCount,
            $stats.Oldest.ToString("yyyy-MM-dd"),
            $stats.Newest.ToString("yyyy-MM-dd")) -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "Tip: schedule weekly with Task Scheduler:" -ForegroundColor DarkGray
Write-Host "  powershell -NoProfile -ExecutionPolicy Bypass -File `"$RepoRoot\scripts\maintenance\prune_logs.ps1`"" -ForegroundColor DarkGray
Write-Host ""
