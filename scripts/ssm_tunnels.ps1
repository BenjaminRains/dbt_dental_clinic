# SSM port-forward helpers — legacy; prefer mdc tunnel and mdc ssm connect.
# UTF-8 with BOM so emoji in Write-Host do not break PowerShell 5.1 parsing.

$script:SSMProjectRoot = Split-Path $PSScriptRoot -Parent
$script:APIInstanceId = $null
$script:ClinicAPIInstanceId = $null
$script:DemoDBInstanceId = $null
$script:RDSEndpoint = $null
$script:DemoDBHost = $null
$script:DemoDBPort = $null

function Ensure-SSMInstanceIdsLoaded {
    param([string]$ProjectPath = $script:SSMProjectRoot)
    $credPath = Join-Path $ProjectPath 'deployment_credentials.json'
    if (-not (Test-Path -LiteralPath $credPath)) { return $false }
    if ($script:APIInstanceId -and $script:ClinicAPIInstanceId -and $script:DemoDBInstanceId) { return $true }
    try {
        $credentials = Get-Content -LiteralPath $credPath | ConvertFrom-Json
        if ($credentials.backend_api.ec2.instance_id) {
            $script:APIInstanceId = $credentials.backend_api.ec2.instance_id
        }
        if ($credentials.backend_api.clinic_api -and $credentials.backend_api.clinic_api.ec2.instance_id) {
            $script:ClinicAPIInstanceId = $credentials.backend_api.clinic_api.ec2.instance_id
        }
        if ($credentials.demo_database.ec2.instance_id) {
            $script:DemoDBInstanceId = $credentials.demo_database.ec2.instance_id
        }
        if (-not $script:RDSEndpoint) {
            if ($credentials.backend_api.clinic_database_reference -and $credentials.backend_api.clinic_database_reference.rds.endpoint) {
                $script:RDSEndpoint = $credentials.backend_api.clinic_database_reference.rds.endpoint
            } elseif ($credentials.backend_api.production_database_reference -and $credentials.backend_api.production_database_reference.rds.endpoint) {
                $script:RDSEndpoint = $credentials.backend_api.production_database_reference.rds.endpoint
            }
        }
        if (-not $script:DemoDBHost -and $credentials.demo_database.database_connection) {
            $script:DemoDBHost = $credentials.demo_database.database_connection.host
            $script:DemoDBPort = $credentials.demo_database.database_connection.port
        }
        return $true
    } catch {
        return $false
    }
}

function New-SsmPortForwardParametersJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostName,
        [string]$RemotePort = '5432',
        [Parameter(Mandatory = $true)]
        [string]$LocalPort
    )

    # AWS CLI on Windows strips quotes from ConvertTo-Json output; escaped JSON is required.
    return '{\"host\":[\"' + $HostName + '\"],\"portNumber\":[\"' + $RemotePort + '\"],\"localPortNumber\":[\"' + $LocalPort + '\"]}'
}

function Invoke-SsmPortForwardSession {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetInstanceId,
        [Parameter(Mandatory = $true)]
        [string]$HostName,
        [string]$RemotePort = '5432',
        [Parameter(Mandatory = $true)]
        [string]$LocalPort
    )

    $paramsJson = New-SsmPortForwardParametersJson -HostName $HostName -RemotePort $RemotePort -LocalPort $LocalPort
    & aws ssm start-session `
        --target $TargetInstanceId `
        --document-name AWS-StartPortForwardingSessionToRemoteHost `
        --parameters $paramsJson
    return $LASTEXITCODE
}

function Start-SSMPortForwardRDS {
    $projectRoot = $script:SSMProjectRoot
    if (-not (Ensure-SSMInstanceIdsLoaded -ProjectPath $projectRoot)) {
        Write-Host 'Could not load credentials. Run from project root or ensure deployment_credentials.json exists.' -ForegroundColor Red
        return 1
    }
    if (-not $script:APIInstanceId) {
        Write-Host 'dental-clinic-api-demo instance ID not in deployment_credentials.json.' -ForegroundColor Red
        return 1
    }
    if (-not $script:RDSEndpoint) {
        Write-Host 'RDS endpoint not loaded. Check deployment_credentials.json.' -ForegroundColor Red
        return 1
    }

    $localPort = $env:POSTGRES_PORT
    if (-not $localPort) { $localPort = '5433' }

    Write-Host 'Starting port forwarding to RDS...' -ForegroundColor Cyan
    Write-Host "  Local port: $localPort" -ForegroundColor Gray
    Write-Host "  Remote: $($script:RDSEndpoint):5432" -ForegroundColor Gray
    Write-Host "  Via: dental-clinic-api-demo $script:APIInstanceId" -ForegroundColor Gray
    Write-Host 'Keep this terminal open. Use Ctrl+C to stop forwarding.' -ForegroundColor Yellow

    return Invoke-SsmPortForwardSession `
        -TargetInstanceId $script:APIInstanceId `
        -HostName $script:RDSEndpoint `
        -LocalPort $localPort
}

function Start-SSMPortForwardRDSClinic {
    $projectRoot = $script:SSMProjectRoot
    if (-not (Ensure-SSMInstanceIdsLoaded -ProjectPath $projectRoot)) {
        Write-Host 'Could not load credentials. Run from project root or ensure deployment_credentials.json exists.' -ForegroundColor Red
        return 1
    }
    if (-not $script:ClinicAPIInstanceId) {
        Write-Host 'dental-clinic-api-clinic instance ID not in deployment_credentials.json.' -ForegroundColor Red
        return 1
    }
    if (-not $script:RDSEndpoint) {
        Write-Host 'RDS endpoint not loaded. Check deployment_credentials.json.' -ForegroundColor Red
        return 1
    }

    $localPort = $env:POSTGRES_PORT
    if (-not $localPort) { $localPort = '5433' }

    Write-Host 'Starting port forwarding to RDS (via dental-clinic-api-clinic)...' -ForegroundColor Cyan
    Write-Host "  Local port: $localPort" -ForegroundColor Gray
    Write-Host "  Remote: $($script:RDSEndpoint):5432" -ForegroundColor Gray
    Write-Host "  Via: dental-clinic-api-clinic $script:ClinicAPIInstanceId" -ForegroundColor Gray
    Write-Host 'Keep this terminal open. Use Ctrl+C to stop forwarding.' -ForegroundColor Yellow

    return Invoke-SsmPortForwardSession `
        -TargetInstanceId $script:ClinicAPIInstanceId `
        -HostName $script:RDSEndpoint `
        -LocalPort $localPort
}

function Start-SSMPortForwardDemoDB {
    $projectRoot = $script:SSMProjectRoot
    if (-not (Ensure-SSMInstanceIdsLoaded -ProjectPath $projectRoot)) {
        Write-Host 'Could not load credentials. Run from project root or ensure deployment_credentials.json exists.' -ForegroundColor Red
        return 1
    }
    if (-not $script:APIInstanceId) {
        Write-Host 'dental-clinic-api-demo instance ID not in deployment_credentials.json (needed to tunnel to demo DB).' -ForegroundColor Red
        return 1
    }
    if (-not $script:DemoDBHost) {
        Write-Host 'Demo DB host not loaded. Check deployment_credentials.json.' -ForegroundColor Red
        return 1
    }

    $localPort = $env:DEMO_POSTGRES_PORT
    if (-not $localPort) { $localPort = '5434' }

    Write-Host 'Starting port forwarding to Demo Database...' -ForegroundColor Cyan
    Write-Host "  Local port: $localPort" -ForegroundColor Gray
    Write-Host "  Remote: $($script:DemoDBHost):5432" -ForegroundColor Gray
    Write-Host "  Via: dental-clinic-api-demo $script:APIInstanceId" -ForegroundColor Gray
    Write-Host 'Keep this terminal open. Use Ctrl+C to stop forwarding.' -ForegroundColor Yellow

    return Invoke-SsmPortForwardSession `
        -TargetInstanceId $script:APIInstanceId `
        -HostName $script:DemoDBHost `
        -LocalPort $localPort
}

function Connect-SSMSession {
    param(
        [Parameter(Mandatory = $true)][string]$TargetInstanceId,
        [Parameter(Mandatory = $true)][string]$Label
    )
    if (-not (Get-Command aws -CommandType Application -ErrorAction SilentlyContinue)) {
        Write-Host 'AWS CLI not found.' -ForegroundColor Red
        return 1
    }
    $plugin = Get-Command session-manager-plugin -ErrorAction SilentlyContinue
    if (-not $plugin) {
        Write-Host 'Session Manager plugin not found. Install: winget install Amazon.SessionManagerPlugin' -ForegroundColor Yellow
    }
    Write-Host "Starting SSM session: $Label ($TargetInstanceId)" -ForegroundColor Cyan
    & aws ssm start-session --target $TargetInstanceId
    return $LASTEXITCODE
}

function Connect-SSMAPI {
    if (-not (Ensure-SSMInstanceIdsLoaded)) {
        Write-Host 'Could not load deployment_credentials.json from project root.' -ForegroundColor Red
        return 1
    }
    if (-not $script:APIInstanceId) {
        Write-Host 'Demo API instance ID missing (backend_api.ec2.instance_id).' -ForegroundColor Red
        return 1
    }
    return Connect-SSMSession -TargetInstanceId $script:APIInstanceId -Label 'dental-clinic-api-demo'
}

function Connect-SSMClinicAPI {
    if (-not (Ensure-SSMInstanceIdsLoaded)) {
        Write-Host 'Could not load deployment_credentials.json from project root.' -ForegroundColor Red
        return 1
    }
    if (-not $script:ClinicAPIInstanceId) {
        Write-Host 'Clinic API instance ID missing (backend_api.clinic_api.ec2.instance_id).' -ForegroundColor Red
        return 1
    }
    return Connect-SSMSession -TargetInstanceId $script:ClinicAPIInstanceId -Label 'dental-clinic-api-clinic'
}

function Connect-SSMDemoDB {
    if (-not (Ensure-SSMInstanceIdsLoaded)) {
        Write-Host 'Could not load deployment_credentials.json from project root.' -ForegroundColor Red
        return 1
    }
    if (-not $script:DemoDBInstanceId) {
        Write-Host 'Demo DB instance ID missing (demo_database.ec2.instance_id).' -ForegroundColor Red
        return 1
    }
    return Connect-SSMSession -TargetInstanceId $script:DemoDBInstanceId -Label 'dental-clinic-demo-db'
}
