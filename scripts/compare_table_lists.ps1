# PowerShell Script to Compare MDC and GLIC Table Lists
# Purpose: Analyze differences between MDC and GLIC database table structures
# Usage: .\scripts\compare_table_lists.ps1 -GlicFile <path> -MdcFile <path>

param(
    [Parameter(Mandatory=$false)]
    [string]$GlicFile,
    
    [Parameter(Mandatory=$false)]
    [string]$MdcFile,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputDir = "docs\glic_investigation"
)

Write-Host "MDC vs GLIC Table List Comparison" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

# Find latest files if not specified
if (-not $GlicFile) {
    $glicFiles = Get-ChildItem -Path $OutputDir -Filter "glic_table_list_*.csv" | Sort-Object LastWriteTime -Descending
    if ($glicFiles.Count -eq 0) {
        Write-Host "ERROR: No GLIC table list file found in $OutputDir" -ForegroundColor Red
        exit 1
    }
    $GlicFile = $glicFiles[0].FullName
    Write-Host "Using latest GLIC file: $(Split-Path -Leaf $GlicFile)" -ForegroundColor Yellow
}

if (-not $MdcFile) {
    $mdcFiles = Get-ChildItem -Path $OutputDir -Filter "mdc_table_list_*.csv" | Sort-Object LastWriteTime -Descending
    if ($mdcFiles.Count -eq 0) {
        Write-Host "ERROR: No MDC table list file found in $OutputDir" -ForegroundColor Red
        exit 1
    }
    $MdcFile = $mdcFiles[0].FullName
    Write-Host "Using latest MDC file: $(Split-Path -Leaf $MdcFile)" -ForegroundColor Yellow
}

Write-Host ""

# Read CSV files
Write-Host "Loading GLIC table list..." -ForegroundColor Yellow
$glicData = Import-Csv -Path $GlicFile
$glicTables = @{}
foreach ($row in $glicData) {
    $glicTables[$row.TABLE_NAME] = $row
}

Write-Host "Loading MDC table list..." -ForegroundColor Yellow
$mdcData = Import-Csv -Path $MdcFile
$mdcTables = @{}
foreach ($row in $mdcData) {
    $mdcTables[$row.TABLE_NAME] = $row
}

# Analysis
Write-Host ""
Write-Host "Comparison Results" -ForegroundColor Cyan
Write-Host "==================" -ForegroundColor Cyan
Write-Host ""

# Count comparison
$glicCount = $glicTables.Count
$mdcCount = $mdcTables.Count
Write-Host "Table Count:" -ForegroundColor Yellow
Write-Host "  GLIC: $glicCount tables" -ForegroundColor White
Write-Host "  MDC:  $mdcCount tables" -ForegroundColor White
Write-Host "  Difference: $([Math]::Abs($glicCount - $mdcCount)) tables" -ForegroundColor White
Write-Host ""

# Find tables only in GLIC
$onlyInGlic = $glicTables.Keys | Where-Object { -not $mdcTables.ContainsKey($_) }
if ($onlyInGlic.Count -gt 0) {
    Write-Host "Tables ONLY in GLIC ($($onlyInGlic.Count)):" -ForegroundColor Yellow
    $onlyInGlic | Sort-Object | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }
    Write-Host ""
}

# Find tables only in MDC
$onlyInMdc = $mdcTables.Keys | Where-Object { -not $glicTables.ContainsKey($_) }
if ($onlyInMdc.Count -gt 0) {
    Write-Host "Tables ONLY in MDC ($($onlyInMdc.Count)):" -ForegroundColor Yellow
    $onlyInMdc | Sort-Object | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }
    Write-Host ""
}

# Find common tables
$commonTables = $glicTables.Keys | Where-Object { $mdcTables.ContainsKey($_) }
Write-Host "Common tables (in both): $($commonTables.Count)" -ForegroundColor Green
Write-Host ""

# Compare row counts for key tables
Write-Host "Key Table Row Count Comparison:" -ForegroundColor Yellow
$keyTables = @("patient", "appointment", "procedurelog", "payment", "claim", "provider", "commlog", "document", "schedule")
Write-Host "Table Name        | GLIC Rows  | MDC Rows   | Difference" -ForegroundColor Cyan
Write-Host "------------------|------------|------------|------------" -ForegroundColor Cyan

foreach ($tableName in $keyTables) {
    if ($glicTables.ContainsKey($tableName) -and $mdcTables.ContainsKey($tableName)) {
        $glicRows = $glicTables[$tableName].approximate_rows
        $mdcRows = $mdcTables[$tableName].approximate_rows
        $diff = [int]$mdcRows - [int]$glicRows
        $glicRowsStr = $glicRows.ToString().PadLeft(10)
        $mdcRowsStr = $mdcRows.ToString().PadLeft(10)
        $diffStr = $diff.ToString().PadLeft(10)
        Write-Host "$($tableName.PadRight(17)) | $glicRowsStr | $mdcRowsStr | $diffStr" -ForegroundColor White
    } elseif ($glicTables.ContainsKey($tableName)) {
        Write-Host "$($tableName.PadRight(17)) | $($glicTables[$tableName].approximate_rows.ToString().PadLeft(10)) | MISSING    | -" -ForegroundColor Red
    } elseif ($mdcTables.ContainsKey($tableName)) {
        Write-Host "$($tableName.PadRight(17)) | MISSING    | $($mdcTables[$tableName].approximate_rows.ToString().PadLeft(10)) | -" -ForegroundColor Red
    }
}

Write-Host ""

# Total size comparison
$glicTotalSize = ($glicData | Measure-Object -Property { [double]$_."total_size_mb" } -Sum).Sum
$mdcTotalSize = ($mdcData | Measure-Object -Property { [double]$_."total_size_mb" } -Sum).Sum

Write-Host "Total Database Size:" -ForegroundColor Yellow
Write-Host "  GLIC: $([math]::Round($glicTotalSize, 2)) MB" -ForegroundColor White
Write-Host "  MDC:  $([math]::Round($mdcTotalSize, 2)) MB" -ForegroundColor White
Write-Host "  Ratio: GLIC is $([math]::Round(($glicTotalSize / $mdcTotalSize) * 100, 1))% of MDC size" -ForegroundColor White
Write-Host ""

# Export comparison report
$comparisonFile = Join-Path $OutputDir "table_comparison_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
$report = @"
MDC vs GLIC Table List Comparison
Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
========================================================

Table Count:
  GLIC: $glicCount tables
  MDC:  $mdcCount tables
  Difference: $([Math]::Abs($glicCount - $mdcCount)) tables

Common Tables: $($commonTables.Count)

Tables Only in GLIC: $($onlyInGlic.Count)
$($onlyInGlic -join ", ")

Tables Only in MDC: $($onlyInMdc.Count)
$($onlyInMdc -join ", ")

Total Database Size:
  GLIC: $([math]::Round($glicTotalSize, 2)) MB
  MDC:  $([math]::Round($mdcTotalSize, 2)) MB
  Ratio: GLIC is $([math]::Round(($glicTotalSize / $mdcTotalSize) * 100, 1))% of MDC size

Key Table Row Count Comparison:
$(($keyTables | ForEach-Object {
    if ($glicTables.ContainsKey($_) -and $mdcTables.ContainsKey($_)) {
        $glicRows = $glicTables[$_].approximate_rows
        $mdcRows = $mdcTables[$_].approximate_rows
        "$_`t$glicRows`t$mdcRows"
    }
}) -join "`n")
"@

$report | Out-File -FilePath $comparisonFile -Encoding UTF8
Write-Host "Comparison report saved to:" -ForegroundColor Green
Write-Host $comparisonFile -ForegroundColor White
Write-Host ""
