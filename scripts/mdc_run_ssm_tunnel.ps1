# Run a single SSM tunnel function from scripts/ssm_tunnels.ps1 (used by mdc tunnel).
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$FunctionName
)

$ErrorActionPreference = 'Stop'
$scriptsRoot = $PSScriptRoot
$projectRoot = Split-Path $scriptsRoot -Parent
Set-Location $projectRoot

. (Join-Path $scriptsRoot 'ssm_tunnels.ps1')

if (-not (Get-Command $FunctionName -ErrorAction SilentlyContinue)) {
    Write-Error "Function not found in ssm_tunnels.ps1: $FunctionName"
    exit 2
}

$code = & $FunctionName
if ($null -eq $code) { $code = $LASTEXITCODE }
if ($null -eq $code) { $code = 0 }
exit [int]$code
