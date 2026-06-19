<#
.SYNOPSIS
    Samples CPU, memory, disk I/O, and top processes for workload characterization.

.DESCRIPTION
    Writes under a timestamped folder in workload_monitor_logs (or -OutputRoot):
      - system_metrics.csv       — time series
      - top_processes.csv        — top N processes per sample
      - system_metrics_summary.csv — min / mean / p50 / p95 / max per metric (baseline bands)
      - process_frequency.csv    — how often each process appeared in the top-N list
      - run_metadata.txt         — run context and optional label

    Use the same parameters for baseline nights (background load) and workload nights, then
    compare p95 values in system_metrics_summary.csv across folders.

    Notes:
    - Get-Process CPU is cumulative CPU seconds, not percent.
    - Performance counters are warmed up before sampling.
    - On servers or SYSTEM accounts, Desktop may be missing; default output is under this repo.

.PARAMETER DurationMinutes
    How long to collect samples. Default: 60.

.PARAMETER IntervalSeconds
    Seconds between samples. Minimum 1. Default: 5.

.PARAMETER OutputRoot
    Parent directory for timestamped run folders. Default: <repo>\workload_monitor_logs

.PARAMETER UseDesktop
    Write under Desktop\workload_monitor_<timestamp> instead of OutputRoot.

.PARAMETER Interactive
    Prompt for duration and interval (overrides parameter defaults for those two).

.PARAMETER TopProcessCount
    Number of processes to log per sample. Default: 15.

.PARAMETER RunLabel
    Tag for this run (e.g. baseline_night1, dbt_full_refresh). Stored in metadata and summary CSV.

.EXAMPLE
    # Baseline: overnight background usage
    .\monitor_workflow.ps1 -DurationMinutes 480 -IntervalSeconds 10 -RunLabel baseline_night1

.EXAMPLE
    # Scheduled task (non-interactive)
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\path\dbt_dental_clinic\monitor_workflow.ps1" -DurationMinutes 480 -IntervalSeconds 10 -RunLabel baseline
#>

[CmdletBinding()]
param(
    [int]$DurationMinutes = 60,
    [int]$IntervalSeconds = 5,
    [string]$OutputRoot = "",
    [switch]$UseDesktop,
    [switch]$Interactive,
    [ValidateRange(1, 100)]
    [int]$TopProcessCount = 15,
    [string]$RunLabel = ""
)

$ErrorActionPreference = "Stop"

function ConvertTo-CsvField {
    param([AllowNull()][string]$Value)
    if ([string]::IsNullOrEmpty($Value)) {
        return '""'
    }
    if ($Value -match '[",\r\n]') {
        return '"' + ($Value -replace '"', '""') + '"'
    }
    return $Value
}

function Get-Percentile {
    param(
        [double[]]$Values,
        [double]$Percentile
    )
    if (-not $Values -or $Values.Count -eq 0) {
        return $null
    }
    $sorted = @($Values | Sort-Object)
    $rank = [math]::Ceiling(($Percentile / 100.0) * $sorted.Count)
    $index = [math]::Max(0, [math]::Min($sorted.Count - 1, $rank - 1))
    return $sorted[$index]
}

function Get-CounterSample {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$CounterPaths
    )
    $samples = Get-Counter -Counter $CounterPaths -ErrorAction Stop
    $result = @{}
    for ($i = 0; $i -lt $CounterPaths.Count; $i++) {
        if ($i -ge $samples.CounterSamples.Count) {
            throw "Performance counter not found: $($CounterPaths[$i])"
        }
        # CounterSamples order matches -Counter list; Path is machine-localized (\\host\...).
        $result[$CounterPaths[$i]] = $samples.CounterSamples[$i].CookedValue
    }
    return $result
}

function Write-SystemMetricsSummary {
    param(
        [string]$MetricsFile,
        [string]$SummaryFile,
        [string]$Label,
        [string]$TextEncoding
    )

    $rows = @(Import-Csv -Path $MetricsFile)
    if ($rows.Count -eq 0) {
        Write-Warning "No samples in $MetricsFile; skipping system metrics summary."
        return
    }

    $metricColumns = @(
        @{ Name = "CPU_Percent"; Property = "CPU_Percent" },
        @{ Name = "AvailableMemory_MB"; Property = "AvailableMemory_MB" },
        @{ Name = "CommittedMemory_Percent"; Property = "CommittedMemory_Percent" },
        @{ Name = "DiskRead_MBps"; Property = "DiskRead_MBps" },
        @{ Name = "DiskWrite_MBps"; Property = "DiskWrite_MBps" }
    )

    $summaryRows = foreach ($col in $metricColumns) {
        $values = @($rows | ForEach-Object { [double]$_.($col.Property) })
        [pscustomobject]@{
            RunLabel     = $Label
            Metric       = $col.Name
            SampleCount  = $values.Count
            Min          = [math]::Round(($values | Measure-Object -Minimum).Minimum, 2)
            Mean         = [math]::Round(($values | Measure-Object -Average).Average, 2)
            P50          = [math]::Round((Get-Percentile -Values $values -Percentile 50), 2)
            P95          = [math]::Round((Get-Percentile -Values $values -Percentile 95), 2)
            Max          = [math]::Round(($values | Measure-Object -Maximum).Maximum, 2)
        }
    }

    $summaryRows | Export-Csv -Path $SummaryFile -NoTypeInformation -Encoding $TextEncoding
}

function Write-ProcessFrequencySummary {
    param(
        [string]$ProcessFile,
        [string]$FrequencyFile,
        [string]$Label,
        [string]$TextEncoding
    )

    $rows = @(Import-Csv -Path $ProcessFile)
    if ($rows.Count -eq 0) {
        Write-Warning "No process rows in $ProcessFile; skipping process frequency summary."
        return
    }

    $freq = $rows |
        Group-Object ProcessName |
        ForEach-Object {
            $memValues = @($_.Group | ForEach-Object { [double]$_.Memory_MB })
            [pscustomobject]@{
                RunLabel           = $Label
                ProcessName        = $_.Name
                AppearancesInTopN  = $_.Count
                AvgMemory_MB       = [math]::Round(($memValues | Measure-Object -Average).Average, 2)
                MaxMemory_MB       = [math]::Round(($memValues | Measure-Object -Maximum).Maximum, 2)
            }
        } |
        Sort-Object AppearancesInTopN -Descending

    $freq | Export-Csv -Path $FrequencyFile -NoTypeInformation -Encoding $TextEncoding
}

function Write-RunSummaries {
    param(
        [string]$OutDir,
        [string]$Label,
        [string]$TextEncoding
    )

    $metricsFile = Join-Path $OutDir "system_metrics.csv"
    $processFile = Join-Path $OutDir "top_processes.csv"
    if (-not (Test-Path $metricsFile) -or (Get-Content $metricsFile | Measure-Object -Line).Lines -le 1) {
        return
    }

    $summaryFile = Join-Path $OutDir "system_metrics_summary.csv"
    $freqFile = Join-Path $OutDir "process_frequency.csv"

    Write-SystemMetricsSummary -MetricsFile $metricsFile -SummaryFile $summaryFile -Label $Label -TextEncoding $TextEncoding
    if (Test-Path $processFile) {
        Write-ProcessFrequencySummary -ProcessFile $processFile -FrequencyFile $freqFile -Label $Label -TextEncoding $TextEncoding
    }

    if (Test-Path $summaryFile) {
        Write-Host ""
        Write-Host "Summary (compare P95 across baseline vs workload runs):" -ForegroundColor Cyan
        Import-Csv $summaryFile | Format-Table Metric, SampleCount, Min, Mean, P50, P95, Max -AutoSize
    }
}

if ($Interactive) {
    $durationInput = Read-Host "How many minutes should I monitor?"
    $intervalInput = Read-Host "Sampling interval in seconds? Example: 5"
    if (-not [int]::TryParse($durationInput, [ref]$DurationMinutes) -or $DurationMinutes -lt 1) {
        throw "Invalid duration: '$durationInput'. Enter a positive integer."
    }
    if (-not [int]::TryParse($intervalInput, [ref]$IntervalSeconds) -or $IntervalSeconds -lt 1) {
        throw "Invalid interval: '$intervalInput'. Enter a positive integer."
    }
    if ([string]::IsNullOrWhiteSpace($RunLabel)) {
        $RunLabel = Read-Host "Run label (optional, e.g. baseline_night1)"
    }
}

if ($DurationMinutes -lt 1) {
    throw "DurationMinutes must be at least 1."
}
if ($IntervalSeconds -lt 1) {
    throw "IntervalSeconds must be at least 1."
}

$repoRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot "workload_monitor_logs"
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$labelSuffix = if ([string]::IsNullOrWhiteSpace($RunLabel)) { "" } else { "_$($RunLabel -replace '[^\w\-]', '_')" }
if ($UseDesktop) {
    $desktop = [Environment]::GetFolderPath("Desktop")
    if ([string]::IsNullOrWhiteSpace($desktop)) {
        throw "Desktop folder is not available for this user (common under SYSTEM or on headless servers). Use -OutputRoot instead."
    }
    $outDir = Join-Path $desktop "workload_monitor_$timestamp$labelSuffix"
} else {
    $outDir = Join-Path $OutputRoot "workload_monitor_$timestamp$labelSuffix"
}

New-Item -ItemType Directory -Force -Path $outDir | Out-Null
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$logFile = Join-Path $outDir "system_metrics.csv"
$processFile = Join-Path $outDir "top_processes.csv"
$metaFile = Join-Path $outDir "run_metadata.txt"

$endTime = (Get-Date).AddMinutes($DurationMinutes)
$encoding = "utf8"
$startedUtc = (Get-Date).ToUniversalTime().ToString("o")

$counterPaths = @(
    '\Processor(_Total)\% Processor Time',
    '\Memory\Available MBytes',
    '\Memory\% Committed Bytes In Use',
    '\PhysicalDisk(_Total)\Disk Read Bytes/sec',
    '\PhysicalDisk(_Total)\Disk Write Bytes/sec'
)

@"
StartedUtc=$startedUtc
DurationMinutes=$DurationMinutes
IntervalSeconds=$IntervalSeconds
OutputDirectory=$outDir
RunLabel=$RunLabel
ComputerName=$env:COMPUTERNAME
UserName=$env:USERDOMAIN\$env:USERNAME
TopProcessCount=$TopProcessCount
Purpose=Compare system_metrics_summary.csv P95 across baseline and workload runs.
"@ | Set-Content -Path $metaFile -Encoding $encoding

"Timestamp,CPU_Percent,AvailableMemory_MB,CommittedMemory_Percent,DiskRead_MBps,DiskWrite_MBps" |
    Set-Content -Path $logFile -Encoding $encoding

"Timestamp,ProcessName,Id,CPU_Seconds,Memory_MB,Path" |
    Set-Content -Path $processFile -Encoding $encoding

Write-Host "Monitoring started."
Write-Host "  Output: $outDir"
if ($RunLabel) { Write-Host "  Label:  $RunLabel" }
Write-Host "  Ends at: $endTime (local)"
Write-Host "  Press Ctrl+C to stop early (partial data will still be summarized)."

try {
    Get-Counter -Counter $counterPaths -ErrorAction Stop | Out-Null
    Start-Sleep -Seconds 1
} catch {
    Write-Warning "Performance counters unavailable: $($_.Exception.Message)"
    throw
}

try {
    while ((Get-Date) -lt $endTime) {
        $now = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

        $c = Get-CounterSample -CounterPaths $counterPaths
        $cpu = $c['\Processor(_Total)\% Processor Time']
        $memAvail = $c['\Memory\Available MBytes']
        $memCommitted = $c['\Memory\% Committed Bytes In Use']
        $diskRead = $c['\PhysicalDisk(_Total)\Disk Read Bytes/sec'] / 1MB
        $diskWrite = $c['\PhysicalDisk(_Total)\Disk Write Bytes/sec'] / 1MB

        $line = "{0},{1},{2},{3},{4},{5}" -f `
            $now, `
            ([math]::Round($cpu, 2)), `
            ([math]::Round($memAvail, 0)), `
            ([math]::Round($memCommitted, 2)), `
            ([math]::Round($diskRead, 2)), `
            ([math]::Round($diskWrite, 2))
        Add-Content -Path $logFile -Value $line -Encoding $encoding

        Get-Process -ErrorAction SilentlyContinue |
            Sort-Object CPU -Descending |
            Select-Object -First $TopProcessCount |
            ForEach-Object {
                $memMb = [math]::Round($_.WorkingSet64 / 1MB, 2)
                $path = ""
                try {
                    $path = $_.Path
                } catch {}
                $fields = @(
                    $now,
                    (ConvertTo-CsvField $_.ProcessName),
                    $_.Id,
                    $_.CPU,
                    $memMb,
                    (ConvertTo-CsvField $path)
                )
                Add-Content -Path $processFile -Value ($fields -join ",") -Encoding $encoding
            }

        Start-Sleep -Seconds $IntervalSeconds
    }
} finally {
    $endedUtc = (Get-Date).ToUniversalTime().ToString("o")
    "EndedUtc=$endedUtc" | Add-Content -Path $metaFile -Encoding $encoding
    Write-RunSummaries -OutDir $outDir -Label $RunLabel -TextEncoding $encoding
}

Write-Host ""
Write-Host "Monitoring complete."
Write-Host "Saved to: $outDir"
