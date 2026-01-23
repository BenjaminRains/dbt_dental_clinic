# Count lines of code excluding documentation and dependencies
#
# Purpose: This script is used for codebase monitoring and maintenance.
# It provides metrics on code volume across different file types and helps
# track codebase growth, identify large files, and maintain code quality standards.

# Find project root by looking for .git directory or other markers
$scriptPath = $PSScriptRoot
$projectRoot = $scriptPath
$maxDepth = 10
$depth = 0

while ($depth -lt $maxDepth) {
    if ((Test-Path (Join-Path $projectRoot '.git')) -or 
        (Test-Path (Join-Path $projectRoot 'dbt_project.yml')) -or
        (Test-Path (Join-Path $projectRoot 'docker-compose.yml'))) {
        break
    }
    $parent = Split-Path $projectRoot -Parent
    if ($parent -eq $projectRoot) {
        # Reached filesystem root, use current directory
        $projectRoot = Get-Location
        break
    }
    $projectRoot = $parent
    $depth++
}

Write-Host "Counting lines of code from: $projectRoot" -ForegroundColor Gray
Push-Location $projectRoot
try {

$codeExtensions = @('*.py', '*.sql', '*.ts', '*.tsx', '*.js', '*.jsx', '*.yml', '*.yaml', '*.json', '*.ps1', '*.sh', '*.bat', '*.dockerfile', '*.toml', '*.ini')

# Directories to exclude
$excludeDirs = @(
    'node_modules',
    '.git',
    '__pycache__',
    '.venv',
    'venv',
    'site-packages',
    '.pytest_cache',
    '.mypy_cache',
    '.cursor',
    'dist',
    'build',
    '.eggs',
    '*.egg-info',
    'dbt_packages',
    'logs',
    'docs',
    'analysis',
    'analysis_intermediate',
    'stakeholders',
    'dbt_docs'
)

# File patterns to exclude (documentation)
$excludeFiles = @('*.md', '*.txt', '*.rst', '*.log')

$totalLines = 0
$fileCount = 0
$fileDetails = @()

Get-ChildItem -Path $projectRoot -Include $codeExtensions -Recurse -File | 
    Where-Object { 
        $fullPath = $_.FullName
        # Normalize paths for comparison
        $normalizedFullPath = $fullPath.Replace('/', '\')
        $normalizedProjectRoot = $projectRoot.Replace('/', '\')
        
        # Get relative path more reliably
        if ($normalizedFullPath.StartsWith($normalizedProjectRoot)) {
            $relativePath = $normalizedFullPath.Substring($normalizedProjectRoot.Length).TrimStart('\')
        } else {
            $relativePath = $_.Name
        }
        
        # Check if file is in excluded directory
        $inExcludedDir = $false
        foreach ($dir in $excludeDirs) {
            if ($relativePath -like "*\$dir\*" -or $relativePath -like "$dir\*" -or $relativePath -like "$dir\*") {
                $inExcludedDir = $true
                break
            }
        }
        
        # Check if file matches excluded pattern
        $isExcludedFile = $false
        foreach ($pattern in $excludeFiles) {
            if ($_.Name -like $pattern) {
                $isExcludedFile = $true
                break
            }
        }
        
        # Exclude specific config/data files
        if ($_.Name -eq 'tables.yml' -or $_.Name -eq 'package-lock.json') {
            $isExcludedFile = $true
        }
        
        # Exclude generated report files (schema_analysis JSON files)
        $isReportFile = $false
        if ($_.Name -like '*schema_analysis*.json' -or 
            $relativePath -like '*\reports\*' -or
            $relativePath -like '*\logs\*') {
            $isReportFile = $true
        }
        
        # Only include SQL files from dbt_dental_models (the main dbt models)
        # Exclude all other SQL files (tests, data, migrations, dbt_packages, etc.)
        $isNonCodeSql = $false
        if ($_.Extension -eq '.sql') {
            # Only include SQL files from dbt_dental_models directory
            # Explicitly exclude dbt_packages and any path that doesn't contain dbt_dental_models
            if ($relativePath -match 'dbt_packages' -or $relativePath -notmatch 'dbt_dental_models') {
                $isNonCodeSql = $true
            }
        }
        
        -not $inExcludedDir -and -not $isExcludedFile -and -not $isReportFile -and -not $isNonCodeSql
    } | 
    ForEach-Object { 
        try { 
            $lines = (Get-Content $_.FullName -ErrorAction Stop | Measure-Object -Line).Lines
            
            # Skip very large JSON files that are likely data/reports rather than code
            # (e.g., > 5000 lines for JSON files)
            if ($_.Extension -eq '.json' -and $lines -gt 5000) {
                return
            }
            
            $totalLines += $lines
            $fileCount++
            
            # Calculate relative path for output
            $normalizedFull = $_.FullName.Replace('/', '\')
            $normalizedProjectRoot = $projectRoot.Replace('/', '\')
            if ($normalizedFull.StartsWith($normalizedProjectRoot)) {
                $relativeFile = $normalizedFull.Substring($normalizedProjectRoot.Length).TrimStart('\')
            } else {
                $relativeFile = $_.Name
            }
            
            $fileDetails += [PSCustomObject]@{
                File = $relativeFile
                Lines = $lines
            }
        } 
        catch { 
            Write-Warning "Could not read $($_.FullName): $_"
        } 
    }

Write-Host "`n=== Lines of Code Summary ===" -ForegroundColor Green
Write-Host "Total files: $fileCount" -ForegroundColor Cyan
Write-Host "Total lines: $totalLines" -ForegroundColor Cyan

# Show breakdown by extension
Write-Host "`n=== Breakdown by File Type ===" -ForegroundColor Green
$fileDetails | 
    Group-Object { [System.IO.Path]::GetExtension($_.File) } | 
    Sort-Object Count -Descending | 
    ForEach-Object {
        $extLines = ($_.Group | Measure-Object -Property Lines -Sum).Sum
        Write-Host "$($_.Name): $($_.Count) files, $extLines lines" -ForegroundColor Yellow
    }

# Show top 20 largest files
Write-Host "`n=== Top 20 Largest Files ===" -ForegroundColor Green
$fileDetails | 
    Sort-Object Lines -Descending | 
    Select-Object -First 20 | 
    ForEach-Object {
        Write-Host "$($_.File): $($_.Lines) lines" -ForegroundColor Yellow
    }
}
finally {
    Pop-Location
}
