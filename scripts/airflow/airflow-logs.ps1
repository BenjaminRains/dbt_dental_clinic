# List recent etl_pipeline runs and log folder paths (Windows-safe).
#
# Usage (repo root):
#   .\scripts\airflow\airflow-logs.ps1
#   .\scripts\airflow\airflow-logs.ps1 -RunId "manual__2026-06-20T03:55:54.158231+00:00"
#   .\scripts\airflow\airflow-logs.ps1 -Task refresh_schema_configuration
#   .\scripts\airflow\airflow-logs.ps1 -Tail 30

param(
    [string]$DagId = "etl_pipeline",
    [string]$RunId = "",
    [string]$Task = "",
    [int]$Limit = 8,
    [int]$Tail = 0
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$AirflowHome = Join-Path $RepoRoot "airflow"
$LogsRoot = Join-Path $AirflowHome "logs\dag_id=$DagId"
$DbPath = Join-Path $AirflowHome "airflow.db"
$Python = Join-Path $RepoRoot ".venv-airflow\Scripts\python.exe"

function ConvertTo-LogRunFolder([string]$runId) {
    return $runId -replace ':', '-'
}

function Get-RunRows {
    if (-not (Test-Path $DbPath)) {
        Write-Error "No airflow.db at $DbPath - start Airflow first."
    }
    $tmpPy = Join-Path $env:TEMP "airflow_logs_query.py"
    @'
import sqlite3
import sys

dag_id = sys.argv[1]
limit = int(sys.argv[2])
db_path = sys.argv[3]

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute(
    "SELECT run_id, state, execution_date, start_date, end_date, run_type "
    "FROM dag_run WHERE dag_id=? ORDER BY execution_date DESC LIMIT ?",
    (dag_id, limit),
)
for row in cur.fetchall():
    print("|".join(str(x) if x is not None else "" for x in row))
conn.close()
'@ | Set-Content -LiteralPath $tmpPy -Encoding utf8
    & $Python $tmpPy $DagId $Limit $DbPath
}

function Show-TaskLog([string]$runFolder, [string]$taskId, [int]$tailLines) {
    $taskDir = Join-Path $LogsRoot "run_id=$runFolder\task_id=$taskId"
    if (-not (Test-Path -LiteralPath $taskDir)) {
        Write-Host "  (no log folder: $taskDir)" -ForegroundColor DarkGray
        return
    }
    $logs = Get-ChildItem -LiteralPath $taskDir -Filter "*.log" | Sort-Object Name
    foreach ($log in $logs) {
        Write-Host "  $($log.FullName)" -ForegroundColor Cyan
        if ($tailLines -gt 0) {
            Get-Content -LiteralPath $log.FullName -Tail $tailLines
        }
    }
}

Write-Host ""
Write-Host "Airflow task logs: $LogsRoot" -ForegroundColor Green
Write-Host "UI: http://localhost:8080 -> DAGs -> $DagId -> run -> task -> Log"
Write-Host ""

$rows = @(Get-RunRows | Where-Object { $_ })
if ($rows.Count -eq 0) {
    Write-Host "No DAG runs found for $DagId."
    exit 0
}

$selectedRunId = $RunId
if (-not $selectedRunId) {
    $first = $rows[0].Split('|')
    $selectedRunId = $first[0]
}

Write-Host "Recent runs (execution_date = schedule time; run_id = log folder name):" -ForegroundColor Yellow
foreach ($line in $rows) {
    $p = $line.Split('|')
    $rid = $p[0]
    $marker = if ($rid -eq $selectedRunId) { " <<" } else { "" }
    $kind = if ($rid -like "manual__*") { "manual" } elseif ($rid -like "scheduled__*") { "scheduled" } else { "other" }
    Write-Host ("  [{0}] {1}  state={2}  exec={3}{4}" -f $kind, $rid, $p[1], $p[2], $marker)
}

$runFolder = ConvertTo-LogRunFolder $selectedRunId
$runPath = Join-Path $LogsRoot "run_id=$runFolder"
Write-Host ""
Write-Host "Selected run folder:" -ForegroundColor Yellow
Write-Host "  $runPath"

if (-not (Test-Path -LiteralPath $runPath)) {
    Write-Host "  (folder not created yet - task may not have started)" -ForegroundColor DarkGray
}

$keyTasks = @(
    "guard_business_hours",
    "refresh_schema_configuration",
    "validation.validate_configuration",
    "etl_processing.process_large_tables",
    "reporting.generate_pipeline_report",
    "dbt_build.dbt_build"
)

if ($Task) {
    Write-Host ""
    Write-Host "Task log: $Task" -ForegroundColor Yellow
    Show-TaskLog $runFolder $Task $Tail
} else {
    Write-Host ""
    Write-Host "Key task logs (attempt=*.log):" -ForegroundColor Yellow
    foreach ($t in $keyTasks) {
        Write-Host ""
        Write-Host "[$t]"
        Show-TaskLog $runFolder $t $(if ($Tail -gt 0) { $Tail } else { 0 })
    }
}

Write-Host ""
Write-Host "Tip: run_id uses UTC. On disk, colon becomes hyphen (Windows)."
Write-Host ""
