# Run a single function from environment_manager.ps1 without interactive startup noise.
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$FunctionName
)

$ErrorActionPreference = 'Stop'
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path $scriptsRoot -Parent
Set-Location $projectRoot

$env:ENV_MANAGER_QUIET_RELOAD = '1'
try {
    . (Join-Path $scriptsRoot 'environment_manager.ps1')
} finally {
    Remove-Item Env:ENV_MANAGER_QUIET_RELOAD -ErrorAction SilentlyContinue
}

if (-not (Get-Command $FunctionName -ErrorAction SilentlyContinue)) {
    Write-Error "Function not found after loading environment manager: $FunctionName"
    exit 2
}

& $FunctionName
exit $LASTEXITCODE
