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

function status-clinic-rds {
    Invoke-MDC @("status", "--env", "clinic", "--tunnel-db") + $args
}

function secrets-pull-clinic {
    Invoke-MDC @("secrets", "pull", "clinic") + $args
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

function api-health {
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = ""
    )
    $stage = Get-MdcStageDefault -Name "api" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "api" -ShellValue $env:API_ENVIRONMENT -Fallback "local")
    Invoke-MDC @("api", "health", "--env", $stage)
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
    # Default stage clinic + profile full (same as etl-run). Use -Env local for local warehouse.
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

function etl-schema {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = "full"
    )
    $stage = Get-MdcStageDefault -Name "etl" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "etl" -ShellValue $env:ETL_ENVIRONMENT -Fallback "clinic")
    $mdcArgs = @("etl", "schema", "--env", $stage, "--profile", $Profile)
    if ($args.Count -gt 0) {
        $mdcArgs += "--"
        $mdcArgs += $args
    }
    Invoke-MDC @mdcArgs
}

function etl-exec {
    param(
        [string]$Env = "",
        [ValidateSet("load", "full", "")]
        [string]$Profile = "full"
    )
    $stage = Get-MdcStageDefault -Name "etl" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "etl" -ShellValue $env:ETL_ENVIRONMENT -Fallback "clinic")
    $mdcArgs = @("etl", "exec", "--env", $stage, "--profile", $Profile)
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

function portfolio-frontend-deploy {
    Invoke-MDC @("deploy", "frontend", "--target", "demo") + $args
}

function clinic-frontend-deploy {
    Invoke-MDC @("deploy", "frontend", "--target", "clinic") + $args
}

function dbt-docs-deploy {
    Invoke-MDC @("deploy", "dbt-docs") + $args
}

function deploy-api {
    param(
        [ValidateSet("local", "demo", "clinic", "test", "")]
        [string]$Env = "clinic"
    )
    Invoke-MDC @("deploy", "api", "--env", $Env) + $args
}

function publish-analytics {
    Invoke-MDC @("publish", "analytics", "--env", "clinic") + $args
}

function tunnel-clinic-db {
    Invoke-MDC @("tunnel", "clinic-db") + $args
}

function tunnel-demo-db {
    Invoke-MDC @("tunnel", "demo-db") + $args
}

function tunnel-rds {
    Invoke-MDC @("tunnel", "rds") + $args
}

function tunnel-close {
    Invoke-MDC @("tunnel", "close") + $args
}

function ssm-status {
    Invoke-MDC @("ssm", "status") + $args
}

function dbt-validate {
    param(
        [ValidateSet("local", "demo", "clinic", "")]
        [string]$Env = ""
    )
    $stage = Get-MdcStageDefault -Name "dbt" -ShellValue $Env -Fallback $(Get-MdcStageDefault -Name "dbt" -ShellValue $env:DBT_TARGET -Fallback "local")
    Invoke-MDC @("dbt", "validate", "--env", $stage)
}

function consult-audio-install {
    Invoke-MDC @("consult-audio", "install") + $args
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

function airflow-init {
    Invoke-MDC @("airflow", "init") + $args
}

function airflow-start-scheduler {
    Invoke-MDC @("airflow", "start", "--scheduler") + $args
}

function airflow-start-dag-processor {
    Invoke-MDC @("airflow", "start", "--dag-processor") + $args
}

function airflow-start-api-server {
    Invoke-MDC @("airflow", "start", "--api-server") + $args
}

function airflow-start-webserver {
    # Alias for api-server (Airflow 2 name)
    Invoke-MDC @("airflow", "start", "--webserver") + $args
}

function airflow-logs {
    Invoke-MDC @("airflow", "logs") + $args
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
Write-Host "Quick start (no *-init required). Prefer aliases below or raw mdc --help:" -ForegroundColor White
Write-Host ""
Write-Host "Status / secrets" -ForegroundColor White
Write-Host "  status / env-status          mdc status (freshness + secrets check)" -ForegroundColor Cyan
Write-Host "  status-clinic-rds            mdc status --env clinic --tunnel-db" -ForegroundColor Cyan
Write-Host "  secrets-pull-clinic          mdc secrets pull clinic" -ForegroundColor Cyan
Write-Host ""
Write-Host "API (local/clinic require CLINIC_PORTAL_SESSION_SECRET or CLINIC_API_KEY)" -ForegroundColor White
Write-Host "  api-run                      mdc api run --env local" -ForegroundColor Cyan
Write-Host "  api-test                     mdc api test-config --env local" -ForegroundColor Cyan
Write-Host "  api-health                   mdc api health --env local" -ForegroundColor Cyan
Write-Host "  api-run -Env clinic          mdc api run --env clinic --tunnel-db  (add --tunnel-db)" -ForegroundColor Cyan
Write-Host ""
Write-Host "ETL" -ForegroundColor White
Write-Host "  etl-validate                 mdc etl validate --env local --profile load" -ForegroundColor Cyan
Write-Host "  etl-run                      mdc etl run --env clinic --profile full" -ForegroundColor Cyan
Write-Host "  etl-status                   mdc etl status --env clinic (use -Env local for local)" -ForegroundColor Cyan
Write-Host "  etl-test                     mdc etl test-connections --env clinic" -ForegroundColor Cyan
Write-Host "  etl-schema                   mdc etl schema --env clinic --profile full" -ForegroundColor Cyan
Write-Host "  etl-exec                     mdc etl exec --env clinic --profile full -- <cmd>" -ForegroundColor Cyan
Write-Host ""
Write-Host "dbt" -ForegroundColor White
Write-Host "  dbt-validate                 mdc dbt validate --env local" -ForegroundColor Cyan
Write-Host "  dbt run|test|docs ...        mdc dbt run|test|docs --env local" -ForegroundColor Cyan
Write-Host "  dbt deps                     mdc dbt invoke --env local -- deps" -ForegroundColor Cyan
Write-Host ""
Write-Host "Frontend / deploy" -ForegroundColor White
Write-Host "  frontend-dev --app clinic    mdc frontend dev --app clinic (proxy auto-picks healthy API)" -ForegroundColor Cyan
Write-Host "  frontend-dev --app portfolio mdc frontend dev --app portfolio" -ForegroundColor Cyan
Write-Host "  frontend-status              mdc frontend status" -ForegroundColor Cyan
Write-Host "  demo-frontend-deploy         mdc deploy frontend --target demo" -ForegroundColor Cyan
Write-Host "  portfolio-frontend-deploy    mdc deploy frontend --target demo (alias)" -ForegroundColor Cyan
Write-Host "  clinic-frontend-deploy       mdc deploy frontend --target clinic" -ForegroundColor Cyan
Write-Host "  dbt-docs-deploy              mdc deploy dbt-docs" -ForegroundColor Cyan
Write-Host "  deploy-api                   mdc deploy api --env clinic" -ForegroundColor Cyan
Write-Host "  publish-analytics            mdc publish analytics --env clinic" -ForegroundColor Cyan
Write-Host ""
Write-Host "Tunnels / SSM" -ForegroundColor White
Write-Host "  tunnel-clinic-db             mdc tunnel clinic-db" -ForegroundColor Cyan
Write-Host "  tunnel-demo-db               mdc tunnel demo-db" -ForegroundColor Cyan
Write-Host "  tunnel-rds                   mdc tunnel rds" -ForegroundColor Cyan
Write-Host "  tunnel-close                 mdc tunnel close" -ForegroundColor Cyan
Write-Host "  ssm-status                   mdc ssm status" -ForegroundColor Cyan
Write-Host "  ssm-connect-clinic-api       mdc ssm connect clinic-api" -ForegroundColor Cyan
Write-Host "  ssm-connect-api              mdc ssm connect api" -ForegroundColor Cyan
Write-Host "  ssm-connect-demo-db          mdc ssm connect demo-db" -ForegroundColor Cyan
Write-Host ""
Write-Host "Consult audio / Airflow" -ForegroundColor White
Write-Host "  consult-audio-install        mdc consult-audio install" -ForegroundColor Cyan
Write-Host "  consult-audio-validate       mdc consult-audio validate" -ForegroundColor Cyan
Write-Host "  consult-audio-run            mdc consult-audio pipeline run" -ForegroundColor Cyan
Write-Host "  airflow-init                 mdc airflow init" -ForegroundColor Cyan
Write-Host "  airflow-start-scheduler      mdc airflow start --scheduler" -ForegroundColor Cyan
Write-Host "  airflow-start-dag-processor  mdc airflow start --dag-processor" -ForegroundColor Cyan
Write-Host "  airflow-start-api-server     mdc airflow start --api-server" -ForegroundColor Cyan
Write-Host "  airflow-start-webserver      mdc airflow start --webserver (alias)" -ForegroundColor Cyan
Write-Host "  airflow-logs                 mdc airflow logs" -ForegroundColor Cyan
Write-Host ""
Write-Host "Full command tree: mdc --help  |  docs: tools/mdc_cli/README.md" -ForegroundColor DarkGray
Write-Host ""
