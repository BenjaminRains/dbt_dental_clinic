# Shared helper to run mdc from repo root (Phase 4.3+).
# Dot-sourced by mdc_aliases.ps1 and environment_manager.ps1.

function Invoke-MDC {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    $scriptsRoot = $PSScriptRoot
    if (-not $scriptsRoot) {
        $scriptsRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
    }
    $projectRoot = Split-Path $scriptsRoot -Parent
    Push-Location $projectRoot
    try {
        $mdc = Get-Command mdc -ErrorAction SilentlyContinue
        if ($mdc) {
            & mdc @Args
        } else {
            python -m mdc_cli @Args
        }
        return $LASTEXITCODE
    } finally {
        Pop-Location
    }
}
