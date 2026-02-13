# Verify ALB target groups and target health for dental clinic API EC2 instances
# Usage: .\scripts\verify_target_groups.ps1
#        .\scripts\verify_target_groups.ps1 -TargetGroupNames "dental-clinic-api-demo-tg","dental-clinic-api-clinic-tg"
#        .\scripts\verify_target_groups.ps1 -FailIfUnhealthy
#
# Optionally reads deployment_credentials.json for target group ARNs. Otherwise discovers
# target groups by name pattern (dental-clinic-api*tg) in the region and reports health.
# Use after Step 4 (Create Clinic Target Group) to confirm targets are healthy.

param(
    [string]$ProjectRoot = "",
    [string]$Region = "us-east-1",
    [string[]]$TargetGroupNames = @(),
    [switch]$FailIfUnhealthy
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

# Optional: read ARNs from deployment_credentials.json
$tgArnsFromCreds = @()
$credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
if (Test-Path $credentialsFile) {
    try {
        $creds = Get-Content $credentialsFile -Raw | ConvertFrom-Json
        # Demo target group
        $demoTg = $creds.backend_api.application_load_balancer.target_group
        if ($demoTg -and $demoTg.arn -and $demoTg.arn -notmatch "^\s*<") {
            $tgArnsFromCreds += $demoTg.arn
        }
        # Clinic target group (if present in creds)
        $clinicTg = $creds.backend_api.clinic_api.target_group
        if ($clinicTg) {
            $arn = if ($clinicTg.arn) { $clinicTg.arn } else { $null }
            if ($arn -and $arn -notmatch "^\s*<") {
                $tgArnsFromCreds += $arn
            }
        }
    } catch {
        # Ignore; we'll discover by name
    }
}

# Resolve target groups: by ARN from creds, or by name (explicit or pattern)
$tgArnsToCheck = @()
if ($tgArnsFromCreds.Count -gt 0) {
    $tgArnsToCheck = @($tgArnsFromCreds)
}

if ($tgArnsToCheck.Count -eq 0) {
    # Discover by name
    $namesToFind = @()
    if ($TargetGroupNames.Count -gt 0) {
        $namesToFind = $TargetGroupNames
    } else {
        $namesToFind = @("dental-clinic-api-demo-tg", "dental-clinic-api-clinic-tg")
    }
    $listJson = aws elbv2 describe-target-groups --region $Region --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "AWS error listing target groups: $listJson" -ForegroundColor Red
        exit 1
    }
    $list = $listJson | ConvertFrom-Json
    foreach ($tg in $list.TargetGroups) {
        if ($namesToFind -contains $tg.TargetGroupName) {
            $tgArnsToCheck += $tg.TargetGroupArn
        }
    }
    if ($tgArnsToCheck.Count -eq 0) {
        Write-Host "No target groups found for names: $($namesToFind -join ', ')" -ForegroundColor Yellow
        Write-Host "List all in region with: aws elbv2 describe-target-groups --region $Region" -ForegroundColor Gray
        exit 1
    }
}

Write-Host "`nDental Clinic – Target Group Health" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Gray

$anyUnhealthy = $false
$instanceTargets = @{}
foreach ($tgArn in $tgArnsToCheck) {
    $tgDesc = aws elbv2 describe-target-groups --target-group-arns $tgArn --region $Region --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to describe target group: $tgArn" -ForegroundColor Red
        Write-Host $tgDesc
        $anyUnhealthy = $true
        continue
    }
    $tgObj = ($tgDesc | ConvertFrom-Json).TargetGroups[0]
    $name = $tgObj.TargetGroupName
    $protocol = $tgObj.Protocol
    $port = $tgObj.Port
    $vpcId = $tgObj.VpcId
    $healthPath = $tgObj.HealthCheckPath
    $healthPort = if ($tgObj.HealthCheckPort) { $tgObj.HealthCheckPort } else { "traffic" }

    Write-Host "`n--- $name ---" -ForegroundColor Yellow
    Write-Host "  ARN:    $tgArn" -ForegroundColor Gray
    Write-Host "  VPC:    $vpcId" -ForegroundColor Gray
    Write-Host "  Port:   $protocol`:$port" -ForegroundColor Gray
    Write-Host "  Health: $healthPath (port $healthPort)" -ForegroundColor Gray

    $healthJson = aws elbv2 describe-target-health --target-group-arn $tgArn --region $Region --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Targets: Failed to get health - $healthJson" -ForegroundColor Red
        $anyUnhealthy = $true
        continue
    }
    $health = $healthJson | ConvertFrom-Json
    $targets = @($health.TargetHealthDescriptions)
    if ($targets.Count -eq 0) {
        Write-Host "  Targets: (none registered)" -ForegroundColor Yellow
        $anyUnhealthy = $true
        continue
    }
    Write-Host "  Targets:" -ForegroundColor Cyan
    foreach ($th in $targets) {
        $id = $th.Target.Id
        $portTarget = $th.Target.Port
        $state = $th.TargetHealth.State
        $reason = if ($th.TargetHealth.Reason) { " - $($th.TargetHealth.Reason)" } else { "" }
        $desc = if ($th.TargetHealth.Description) { " ($($th.TargetHealth.Description))" } else { "" }
        $color = switch ($state) {
            "healthy"   { "Green" }
            "initial"   { "Yellow" }
            "draining"  { "DarkYellow" }
            default     { "Red" }
        }
        if ($state -ne "healthy") { $anyUnhealthy = $true }
        if (-not $instanceTargets.ContainsKey($id)) {
            $instanceTargets[$id] = @{
                Ports        = @()
                TargetGroups = @()
            }
        }
        if ($instanceTargets[$id].Ports -notcontains $portTarget) {
            $instanceTargets[$id].Ports += $portTarget
        }
        if ($instanceTargets[$id].TargetGroups -notcontains $name) {
            $instanceTargets[$id].TargetGroups += $name
        }
        Write-Host "    $id : $portTarget -> $state$reason$desc" -ForegroundColor $color
    }
}

if ($instanceTargets.Count -gt 0) {
    Write-Host "`nEC2 instances and their target groups" -ForegroundColor Cyan
    Write-Host ("-" * 70) -ForegroundColor Gray

    $instanceInfo = @{}

    foreach ($targetId in $instanceTargets.Keys) {
        $ec2Raw = $null
        if ($targetId -match '^\d{1,3}(\.\d{1,3}){3}$') {
            $ec2Raw = aws ec2 describe-instances --filters "Name=private-ip-address,Values=$targetId" --region $Region --output json 2>&1
        } elseif ($targetId -like "i-*") {
            $ec2Raw = aws ec2 describe-instances --instance-ids $targetId --region $Region --output json 2>&1
        } else {
            continue
        }

        if ($LASTEXITCODE -ne 0) {
            continue
        }

        try {
            $ec2 = $ec2Raw | ConvertFrom-Json
        } catch {
            continue
        }

        if (-not $ec2.Reservations -or $ec2.Reservations.Count -eq 0) {
            continue
        }

        $inst = $ec2.Reservations[0].Instances[0]
        if (-not $inst) { continue }

        $nameTag = ($inst.Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -First 1).Value
        $info = @{
            InstanceId = $inst.InstanceId
            Name       = $nameTag
            PrivateIp  = $inst.PrivateIpAddress
            State      = $inst.State.Name
        }
        $instanceInfo[$targetId] = $info
    }

    foreach ($targetId in $instanceTargets.Keys | Sort-Object) {
        $info = $instanceInfo[$targetId]
        if ($info) {
            $header = if ($info.Name) { "$($info.Name) ($($info.InstanceId))" } else { $info.InstanceId }
            Write-Host "`n$header" -ForegroundColor Yellow
            Write-Host "  Private IP: $($info.PrivateIp)" -ForegroundColor Gray
            Write-Host "  State:      $($info.State)" -ForegroundColor Gray
        } else {
            Write-Host "`nTarget: $targetId" -ForegroundColor Yellow
        }

        $tgNames = $instanceTargets[$targetId].TargetGroups -join ", "
        $ports = ($instanceTargets[$targetId].Ports | Sort-Object -Unique) -join ", "
        Write-Host "  Target groups: $tgNames" -ForegroundColor White
        Write-Host "  Ports:         $ports" -ForegroundColor White
    }
}

Write-Host ""
if ($anyUnhealthy) {
    if ($FailIfUnhealthy) {
        Write-Host "Unhealthy targets detected. Exiting with error (-FailIfUnhealthy)." -ForegroundColor Red
        exit 1
    }
    Write-Host "Some targets are not healthy. See Step 4 Troubleshooting in CLINIC_DEPLOYMENT_PHASE2_ACTION_PLAN.md" -ForegroundColor Yellow
    exit 0
}
Write-Host "All target groups have healthy targets." -ForegroundColor Green
exit 0
