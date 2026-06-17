# PowerShell script to run dbt commands on EC2 using AWS Systems Manager
# Usage (from repo root):
#   .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic run --select +mart_referral_source_kpis
#   .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic -RefreshProject                    # full project → RDS (stale data / post-ETL)
#   .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic RefreshProject                       # same (bare token accepted)
#   .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic -RefreshProject -FullRefresh        # full project + rebuild incremental tables
#   .\scripts\ec2\run_dbt_on_ec2.ps1 -Clinic test --select mart_referral_source_kpis
#   .\scripts\ec2\run_dbt_on_ec2.ps1 run --select fact_claim   # uses backend_api.ec2 (often demo)
#   .\scripts\ec2\run_dbt_on_ec2.ps1 -InstanceId i-xxx run

param(
    [Parameter(Mandatory=$false, Position=0, ValueFromRemainingArguments=$true)]
    [string[]]$DbtArgs,  # dbt command and arguments (e.g., "run", "--select", "fact_claim")
    
    [string]$InstanceId = "",
    [string]$DbtPath = "/opt/dbt_dental_clinic/dbt_dental_models",
    [string]$ProjectRoot = "",
    # Use dental-clinic-api-clinic (deployment_credentials: backend_api.clinic_api.ec2.instance_id)
    [switch]$Clinic = $false,
    # Run `dbt run` for the entire project (no --select). Use after raw/ETL loads when marts are stale.
    [switch]$RefreshProject = $false,
    # Append dbt --full-refresh for run/build (rebuilds incremental models from scratch).
    [switch]$FullRefresh = $false
)

$ErrorActionPreference = "Stop"

# Merge -RefreshProject / -FullRefresh with common mistake: bare words in remaining args (e.g. -Clinic RefreshProject)
$refreshProjectMode = [bool]$RefreshProject
$fullRefreshMode = [bool]$FullRefresh
if ($DbtArgs -and $DbtArgs.Count -gt 0) {
    $kept = New-Object System.Collections.Generic.List[string]
    foreach ($t in $DbtArgs) {
        if ($t -ieq 'RefreshProject') { $refreshProjectMode = $true; continue }
        if ($t -ieq 'FullRefresh') { $fullRefreshMode = $true; continue }
        [void]$kept.Add($t)
    }
    $DbtArgs = if ($kept.Count -gt 0) { @($kept) } else { $null }
}

# Determine project root
if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}

Write-Host "`n🚀 Running dbt on EC2" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Gray

# Get instance ID from deployment_credentials.json if not provided
if (-not $InstanceId) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        $credentials = Get-Content $credentialsFile | ConvertFrom-Json
        if ($Clinic -and $credentials.backend_api.clinic_api.ec2.instance_id) {
            $InstanceId = $credentials.backend_api.clinic_api.ec2.instance_id
            Write-Host "✅ Loaded clinic EC2 instance ID: $InstanceId" -ForegroundColor Green
        } else {
            $InstanceId = $credentials.backend_api.ec2.instance_id
            if ($Clinic -and -not $credentials.backend_api.clinic_api.ec2.instance_id) {
                Write-Host "⚠️  -Clinic set but backend_api.clinic_api.ec2.instance_id missing; using backend_api.ec2.instance_id" -ForegroundColor Yellow
            }
            Write-Host "✅ Loaded instance ID from deployment_credentials.json: $InstanceId" -ForegroundColor Green
        }
    } else {
        Write-Host "❌ deployment_credentials.json not found and InstanceId not provided" -ForegroundColor Red
        Write-Host "   Please provide -InstanceId parameter or ensure deployment_credentials.json exists" -ForegroundColor Yellow
        exit 1
    }
}

if ($refreshProjectMode) {
    $DbtArgs = @('run')
    Write-Host "📦 RefreshProject: full dbt run (all models) → clinic RDS. Expect a long run." -ForegroundColor Yellow
} elseif (-not $DbtArgs -or $DbtArgs.Count -eq 0) {
    $DbtArgs = @("run")
    Write-Host "⚠️  No dbt command specified, defaulting to 'dbt run'" -ForegroundColor Yellow
}

# Default to --target clinic when running on EC2 (clinic RDS); allow override via explicit --target
$hasTarget = $DbtArgs | Where-Object { $_ -match '^--target' -or $_ -eq '-t' }
if (-not $hasTarget) {
    $DbtArgs = $DbtArgs + @('--target', 'clinic')
    Write-Host "🎯 Using target: clinic (default for EC2)" -ForegroundColor Gray
}

$dbtSubcommand = $DbtArgs | Select-Object -First 1
if ($fullRefreshMode -and ($dbtSubcommand -eq 'run' -or $dbtSubcommand -eq 'build')) {
    if ($DbtArgs -notcontains '--full-refresh') {
        $DbtArgs = $DbtArgs + '--full-refresh'
        Write-Host "♻️  FullRefresh: appended --full-refresh (incremental models fully rebuild)" -ForegroundColor Yellow
    }
}

# Build the dbt command
$dbtCommand = $DbtArgs -join " "
Write-Host "`n📋 dbt command: dbt $dbtCommand" -ForegroundColor Cyan
Write-Host "   Path: $DbtPath" -ForegroundColor Gray
Write-Host "   Instance: $InstanceId" -ForegroundColor Gray
Write-Host ""

try {
    # Escape for bash: SSM RunCommand often runs as root — dbt is usually installed for ec2-user (~/.local/bin).
    $escapedCommand = $dbtCommand -replace "'", "'\''"

    $remoteProjectRoot = "/opt/dbt_dental_clinic"
    $envScriptRepo = "$remoteProjectRoot/scripts/ec2/setup_ec2_dbt_env.sh"
    $envScriptLegacy = "$remoteProjectRoot/scripts/setup_ec2_dbt_env.sh"

    # bash -lc + sudo -u ec2-user loads ~/.bash_profile so PATH includes ~/.local/bin; fix wrong env path (repo uses scripts/ec2/).
    # Prefer api/.env on clinic EC2 (deployed via mdc deploy api --env clinic); fall back to setup_ec2_dbt_env.sh + deployment_credentials.json.
    $inner = "cd '$DbtPath' && if [ -f '$remoteProjectRoot/api/.env' ]; then set -a; . '$remoteProjectRoot/api/.env'; set +a; elif [ -f '$envScriptRepo' ]; then . '$envScriptRepo'; elif [ -f '$envScriptLegacy' ]; then . '$envScriptLegacy'; fi && export PATH=`$HOME/.local/bin:/usr/local/bin:`$PATH && if command -v pipenv >/dev/null 2>&1 && [ -f Pipfile ]; then exec pipenv run dbt $escapedCommand; elif command -v dbt >/dev/null 2>&1; then exec dbt $escapedCommand; elif [ -x `$HOME/.local/bin/dbt ]; then exec `$HOME/.local/bin/dbt $escapedCommand; elif python3 -m dbt --version >/dev/null 2>&1; then exec python3 -m dbt $escapedCommand; else echo 'dbt not found. On instance as ec2-user: python3 -m pip install --user dbt-postgres' >&2; exit 127; fi"
    $bashCommand = 'sudo -u ec2-user -H /bin/bash -lc ' + "'" + ($inner -replace "'", "'\''") + "'"
    $bashCommand = $bashCommand -replace "`r`n", "" -replace "`r", ""
    
    # AWS SSM requires parameters as JSON object: {"commands": [...]}
    $parameters = @{
        commands = @($bashCommand)
    }
    $commandJson = $parameters | ConvertTo-Json -Compress
    
    Write-Host "📤 Sending command to EC2..." -ForegroundColor Yellow
    
    $commandId = (
        aws ssm send-command `
            --instance-ids $InstanceId `
            --document-name "AWS-RunShellScript" `
            --parameters $commandJson `
            --query 'Command.CommandId' `
            --output text 2>$null
    ).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $commandId) {
        Write-Host "❌ aws ssm send-command failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "   Command ID: $commandId" -ForegroundColor Gray
    
    # Poll with --query Status only. Full JSON embeds StandardOutputContent (dbt logs); that often breaks ConvertFrom-Json (size/escaping/truncation).
    $maxRetries = if ($refreshProjectMode) { 1200 } elseif ($fullRefreshMode) { 600 } else { 300 }
    $retryCount = 0
    $finalStatus = $null
    
    Write-Host "   Waiting for dbt to complete..." -ForegroundColor Gray
    Write-Host ""
    
    while ($retryCount -lt $maxRetries) {
        Start-Sleep -Seconds 2
        $finalStatus = (
            aws ssm get-command-invocation `
                --command-id $commandId `
                --instance-id $InstanceId `
                --query 'Status' `
                --output text 2>$null
        )
        if ($finalStatus) { $finalStatus = $finalStatus.Trim() }
        if (-not $finalStatus) {
            $retryCount++
            continue
        }
        if ($finalStatus -eq 'Success' -or $finalStatus -eq 'Failed' -or $finalStatus -eq 'Cancelled') {
            break
        }
        $retryCount++
        if ($retryCount % 5 -eq 0) {
            Write-Host "." -NoNewline -ForegroundColor Gray
        }
    }
    
    Write-Host "`n" # New line
    
    if (-not $finalStatus) {
        Write-Host "❌ No SSM status (timeout or get-command-invocation failed)." -ForegroundColor Red
        exit 1
    }
    
    $respText = (
        aws ssm get-command-invocation `
            --command-id $commandId `
            --instance-id $InstanceId `
            --query 'ResponseCode' `
            --output text 2>$null
    )
    $responseCode = -1
    if ($respText -match '^-?\d+\s*$') { $responseCode = [int]$respText.Trim() }
    
    $stdOut = aws ssm get-command-invocation `
        --command-id $commandId `
        --instance-id $InstanceId `
        --query 'StandardOutputContent' `
        --output text 2>$null
    $stdErr = aws ssm get-command-invocation `
        --command-id $commandId `
        --instance-id $InstanceId `
        --query 'StandardErrorContent' `
        --output text 2>$null
    
    if ($stdOut) {
        Write-Host "📄 Output:" -ForegroundColor Cyan
        Write-Host $stdOut
    }
    
    if ($stdErr) {
        Write-Host "`n⚠️  Errors/Warnings:" -ForegroundColor Yellow
        Write-Host $stdErr
    }
    
    if ($finalStatus -eq 'Success' -and $responseCode -eq 0) {
        Write-Host "`n✅ dbt command completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "`n❌ dbt command failed" -ForegroundColor Red
        Write-Host "   Status: $finalStatus" -ForegroundColor Red
        Write-Host "   Response Code: $responseCode" -ForegroundColor Red
        exit 1
    }
    
} catch {
    Write-Host "`n❌ Error running dbt command: $_" -ForegroundColor Red
    exit 1
}
