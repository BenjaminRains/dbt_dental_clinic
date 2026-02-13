# List dbt models by execution time (slowest first) using target/run_results.json.
# Run from repo root after: cd dbt_dental_models; dbt run
# Usage: .\scripts\list_slow_models.ps1 [ -Top 20 ]

param(
    [int]$Top = 25,
    [string]$ResultsPath = "target\run_results.json"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$resultsFile = Join-Path $projectRoot $ResultsPath

if (-not (Test-Path $resultsFile)) {
    Write-Error "Run results not found: $resultsFile. Run 'dbt run' first."
    exit 1
}

$j = Get-Content $resultsFile -Raw | ConvertFrom-Json
$totalTime = [double]$j.elapsed_time
$results = $j.results | Sort-Object { [double]$_.execution_time } -Descending

Write-Host ""
Write-Host "Total run time: $([math]::Round($totalTime, 1))s ($([math]::Round($totalTime/60, 1)) min)"
Write-Host "Models: $($results.Count)"
Write-Host ""
Write-Host "Top $Top slowest models (execution_time):"
Write-Host ("-" * 60)

$top = $results | Select-Object -First $Top
foreach ($r in $top) {
    $name = $r.unique_id -replace '^model\.dbt_dental_models\.',''
    $sec = [math]::Round([double]$r.execution_time, 1)
    $pct = if ($totalTime -gt 0) { [math]::Round(100 * [double]$r.execution_time / $totalTime, 1) } else { 0 }
    Write-Host ("{0,9:N1}s  ({1,5}%)  {2}" -f $sec, $pct, $name)
}

Write-Host ""
Write-Host "To investigate a model in the database:"
Write-Host "  -- Compile: dbt compile --select <model_name>"
Write-Host "  -- Then run EXPLAIN (ANALYZE, BUFFERS) SELECT ... in DBeaver on the compiled SQL in target/run/"
