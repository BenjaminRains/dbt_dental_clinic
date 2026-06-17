# Phase 5.5 — thin PowerShell aliases over mdc (default daily dev workflow).
# Load via: .\load_project.ps1
# One-time: pip install -e tools/mdc_cli

. (Join-Path $PSScriptRoot "mdc_invoke.ps1")

function ssm-connect-api {
    Invoke-MDC @("ssm", "connect", "api") + $args
}

function ssm-connect-clinic-api {
    Invoke-MDC @("ssm", "connect", "clinic-api") + $args
}

function ssm-connect-demo-db {
    Invoke-MDC @("ssm", "connect", "demo-db") + $args
}

function Get-MdcStageDefault {
    param(
        [string]$Name,
        [string]$ShellValue,
        [string]$Fallback
    )
    if ($ShellValue) { return $ShellValue }
    return $Fallback
}

function status {
    Invoke-MDC @("status") + $args
}

function env-status {
    status @args
}

function api-run {
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = ""
    )
    $stage = Get-MdcStageDefault -Name "api" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "api" -ShellValue $env:API_ENVIRONMENT -Fallback "local")
    $mdcArgs = @("api", "run", "--env", $stage)
    if ($args.Count -gt 0) { $mdcArgs += $args }
    Invoke-MDC @mdcArgs
}

function api-test {
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = ""
    )
    $stage = Get-MdcStageDefault -Name "api" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "api" -ShellValue $env:API_ENVIRONMENT -Fallback "local")
    Invoke-MDC @("api", "test-config", "--env", $stage)
}

function etl-run {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = "full"
    )
    $stage = Get-MdcStageDefault -Name "etl" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "etl" -ShellValue $env:ETL_ENVIRONMENT -Fallback "clinic")
    $mdcArgs = @("etl", "run", "--env", $stage, "--profile", $Profile)
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
}

function etl-validate {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = ""
    )
    $stage = Get-MdcStageDefault -Name "etl" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "etl" -ShellValue $env:ETL_ENVIRONMENT -Fallback "local")
    $resolvedProfile = if ($Profile) { $Profile } elseif ($stage -eq "local") { "load" } else { "full" }
    Invoke-MDC @("etl", "validate", "--env", $stage, "--profile", $resolvedProfile)
}

function etl-test {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = "full"
    )
    $stage = Get-MdcStageDefault -Name "etl" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "etl" -ShellValue $env:ETL_ENVIRONMENT -Fallback "clinic")
    $mdcArgs = @("etl", "test-connections", "--env", $stage, "--profile", $Profile)
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
}

function etl-status {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = ""
    )
    $stage = Get-MdcStageDefault -Name "etl" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "etl" -ShellValue $env:ETL_ENVIRONMENT -Fallback "clinic")
    $mdcArgs = @("etl", "status", "--env", $stage)
    if ($Profile) { $mdcArgs += @("--profile", $Profile) }
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
}

function frontend-dev {
    Invoke-MDC @("frontend", "dev") + $args
}

function frontend-status {
    Invoke-MDC @("frontend", "status") + $args
}

function demo-frontend-deploy {
    Invoke-MDC @("deploy", "frontend", "--target", "demo") + $args
}

function clinic-frontend-deploy {
    Invoke-MDC @("deploy", "frontend", "--target", "clinic") + $args
}

function dbt-docs-deploy {
    Invoke-MDC @("deploy", "dbt-docs") + $args
}

function consult-audio-init {
    Write-Host "`n⚠️  consult-audio-init is deprecated. Use: mdc consult-audio install" -ForegroundColor Yellow
    Invoke-MDC @("consult-audio", "install") + $args
}

function consult-audio-deactivate {
    Write-Host 'consult-audio-deactivate is a no-op (Phase 5.4). mdc runs are stateless - no shell env to clear.' -ForegroundColor DarkGray
}

function consult-audio-validate {
    Invoke-MDC @("consult-audio", "validate") + $args
}

function consult-audio-run {
    Invoke-MDC @("consult-audio", "pipeline", "run") + $args
}

function dbt-init {
    param(
        [ValidateSet("local", "demo", "clinic")]
        [string]$Target = "local"
    )
    Write-Host "`n⚠️  dbt-init is deprecated. Use: mdc dbt run --env $Target" -ForegroundColor Yellow
    Invoke-MDC @("dbt", "validate", "--env", $Target)
}

function dbt-deactivate {
    Write-Host 'dbt-deactivate is a no-op (Phase 4.5). mdc runs are stateless - no shell env to clear.' -ForegroundColor DarkGray
}

function dbt {
    if (-not $args -or $args.Count -eq 0) {
        Write-Host "Usage: dbt run|test|deps|docs ... (delegates to mdc dbt)" -ForegroundColor Yellow
        return
    }

    $target = if ($env:DBT_TARGET -in @('local', 'demo', 'clinic')) { $env:DBT_TARGET } else { 'local' }
    foreach ($arg in $args) {
        if ($arg -match '^--target=(.+)$') {
            $target = $Matches[1]
            break
        }
    }

    $sub = $args[0]
    $rest = @()
    if ($args.Count -gt 1) { $rest = $args[1..($args.Count - 1)] }

    switch ($sub) {
        'run' { Invoke-MDC @('dbt', 'run', '--env', $target) + $rest; return }
        'test' { Invoke-MDC @('dbt', 'test', '--env', $target) + $rest; return }
        'docs' {
            if ($rest.Count -gt 0 -and $rest[0] -eq 'serve') {
                Invoke-MDC @('dbt', 'docs', '--env', $target, '--serve') + $rest[1..($rest.Count - 1)]
            } else {
                $extra = $rest
                if ($rest.Count -gt 0 -and $rest[0] -eq 'generate') { $extra = $rest[1..($rest.Count - 1)] }
                Invoke-MDC @('dbt', 'docs', '--env', $target) + $extra
            }
            return
        }
        'deps' {
            Invoke-MDC @('dbt', 'invoke', '--env', $target, '--', 'deps') + $rest
            return
        }
        default {
            Invoke-MDC @('dbt', 'invoke', '--env', $target, '--') + $args
        }
    }
}

Write-Host ''
Write-Host 'Dental Clinic Dev CLI (mdc aliases)' -ForegroundColor Blue
Write-Host '-----------------------------------' -ForegroundColor DarkBlue
Write-Host ""
Write-Host "Quick start (no *-init required):" -ForegroundColor White
Write-Host "  status / env-status          mdc status" -ForegroundColor Cyan
Write-Host "  api-run                      mdc api run --env local" -ForegroundColor Cyan
Write-Host "  api-test                     mdc api test-config --env local" -ForegroundColor Cyan
Write-Host "  etl-validate                 mdc etl validate --env local --profile load" -ForegroundColor Cyan
Write-Host "  etl-run                      mdc etl run --env clinic --profile full" -ForegroundColor Cyan
Write-Host "  etl-test                     mdc etl test-connections --env clinic" -ForegroundColor Cyan
Write-Host "  mdc dbt run --env local      dbt via stateless subprocess" -ForegroundColor Cyan
Write-Host "  mdc tunnel clinic-db         SSM port forward (Python)" -ForegroundColor Cyan
Write-Host "  frontend-dev                 mdc frontend dev (local Vite)" -ForegroundColor Cyan
Write-Host "  clinic-frontend-deploy       mdc deploy frontend --target clinic" -ForegroundColor Cyan
Write-Host "  dbt-docs-deploy              mdc deploy dbt-docs" -ForegroundColor Cyan
Write-Host "  consult-audio-validate       mdc consult-audio validate" -ForegroundColor Cyan
Write-Host "  consult-audio-run            mdc consult-audio pipeline run" -ForegroundColor Cyan
Write-Host "  mdc deploy api --env clinic   copy api/.env_api_clinic to EC2; restart dental-clinic-api" -ForegroundColor Cyan
Write-Host "  mdc api run --env clinic --tunnel-db  clinic API via mdc tunnel clinic-db" -ForegroundColor Cyan
Write-Host "  ssm-connect-clinic-api       SSM shell on clinic API EC2" -ForegroundColor Cyan
Write-Host ""
