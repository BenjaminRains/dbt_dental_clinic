# Check dental clinic security groups and their rules
# Usage: .\scripts\check_security_groups.ps1
#        .\scripts\check_security_groups.ps1 -InstanceRole clinic
#        .\scripts\check_security_groups.ps1 -InstanceId i-0d17b38e2e68137ec
#        .\scripts\check_security_groups.ps1 -InstanceRole demo
#
# Reads security group IDs from deployment_credentials.json and outputs inbound/outbound rules.
# Also shows which security groups are attached to the specified EC2 instance.
# Use -InstanceRole to check clinic or demo instance from creds; use -InstanceId to override.

param(
    [string]$ProjectRoot = "",
    [string]$Region = "us-east-1",
    [ValidateSet("clinic", "demo", "both")]
    [string]$InstanceRole = "clinic",
    [string]$InstanceId = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

$credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
if (-not (Test-Path $credentialsFile)) {
    Write-Host "deployment_credentials.json not found: $credentialsFile" -ForegroundColor Red
    exit 1
}

$creds = Get-Content $credentialsFile -Raw | ConvertFrom-Json

# Get SG IDs from backend_api.network
$network = $creds.backend_api.network
if (-not $network -or -not $network.security_groups) {
    Write-Host "Could not find security_groups in deployment_credentials.json" -ForegroundColor Red
    exit 1
}

$sgIds = @(
    $network.security_groups.alb.security_group_id,
    $network.security_groups.api.security_group_id,
    $network.security_groups.rds.security_group_id
) | Where-Object { $_ }

$albSgId = $network.security_groups.alb.security_group_id
$apiSgId = $network.security_groups.api.security_group_id
$rdsSgId = $network.security_groups.rds.security_group_id

Write-Host "`nDental Clinic Security Groups" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Gray
Write-Host "  ALB: $albSgId" -ForegroundColor White
Write-Host "  API: $apiSgId (expected on demo + clinic EC2)" -ForegroundColor White
Write-Host "  RDS: $rdsSgId" -ForegroundColor White
Write-Host ""

# Describe all security groups
$sgJson = aws ec2 describe-security-groups --group-ids $sgIds --region $Region --output json 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "AWS error: $sgJson" -ForegroundColor Red
    exit 1
}

$sgs = $sgJson | ConvertFrom-Json

foreach ($sg in $sgs.SecurityGroups) {
    $isApi = ($sg.GroupId -eq $apiSgId)
    Write-Host "`n--- $($sg.GroupName) ($($sg.GroupId)) ---" -ForegroundColor Yellow
    Write-Host "  Description: $($sg.Description)" -ForegroundColor Gray
    Write-Host "  VPC: $($sg.VpcId)" -ForegroundColor Gray

    Write-Host "`n  INBOUND RULES:" -ForegroundColor Cyan
    if ($sg.IpPermissions.Count -eq 0) {
        Write-Host "    (none)" -ForegroundColor DarkGray
    } else {
        foreach ($perm in $sg.IpPermissions) {
            $fromPort = if ($perm.FromPort) { $perm.FromPort } else { "all" }
            $toPort = if ($perm.ToPort) { $perm.ToPort } else { "all" }
            $portRange = if ($fromPort -eq $toPort) { $fromPort } else { "$fromPort-$toPort" }
            $protocol = $perm.IpProtocol
            if ($protocol -eq "-1") { $protocol = "all" }

            foreach ($range in $perm.IpRanges) {
                $source = $range.CidrIp
                $desc = if ($range.Description) { "  # $($range.Description)" } else { "" }
                Write-Host "    $protocol $portRange from $source$desc" -ForegroundColor White
            }
            foreach ($ug in $perm.UserIdGroupPairs) {
                $source = if ($ug.GroupId -like "sg-*") { $ug.GroupId } else { "sg-$($ug.GroupId)" }
                $sourceName = if ($ug.GroupName) { " ($($ug.GroupName))" } else { "" }
                $desc = if ($ug.Description) { "  # $($ug.Description)" } else { "" }
                $highlight = if ($isApi -and $source -eq $albSgId) { " <-- ALB (needed for health checks)" } else { "" }
                Write-Host "    $protocol $portRange from $source$sourceName$desc$highlight" -ForegroundColor White
            }
        }
    }

    Write-Host "`n  OUTBOUND RULES:" -ForegroundColor Cyan
    if ($sg.IpPermissionsEgress.Count -eq 0) {
        Write-Host "    (none)" -ForegroundColor DarkGray
    } else {
        foreach ($perm in $sg.IpPermissionsEgress) {
            $fromPort = if ($perm.FromPort) { $perm.FromPort } else { "all" }
            $toPort = if ($perm.ToPort) { $perm.ToPort } else { "all" }
            $portRange = if ($fromPort -eq $toPort) { $fromPort } else { "$fromPort-$toPort" }
            $protocol = $perm.IpProtocol
            if ($protocol -eq "-1") { $protocol = "all" }

            foreach ($range in $perm.IpRanges) {
                Write-Host "    $protocol $portRange to $($range.CidrIp)" -ForegroundColor White
            }
            foreach ($ug in $perm.UserIdGroupPairs) {
                $dest = if ($ug.GroupId -like "sg-*") { $ug.GroupId } else { "sg-$($ug.GroupId)" }
                $destName = if ($ug.GroupName) { " ($($ug.GroupName))" } else { "" }
                Write-Host "    $protocol $portRange to $dest$destName" -ForegroundColor White
            }
        }
    }
}

# Resolve which EC2 instance(s) to check
$instancesToCheck = @()
if ($InstanceId) {
    $instancesToCheck += @{ Id = $InstanceId; Role = "specified (-InstanceId)"; Source = "parameter" }
} else {
    if ($InstanceRole -eq "clinic" -or $InstanceRole -eq "both") {
        $cid = $creds.backend_api.clinic_api.ec2.instance_id
        if ($cid) { $instancesToCheck += @{ Id = $cid; Role = "clinic"; Source = "backend_api.clinic_api.ec2.instance_id" } }
    }
    if ($InstanceRole -eq "demo" -or $InstanceRole -eq "both") {
        $did = $creds.backend_api.ec2.instance_id
        if (-not $did) { $did = $creds.environments.demo.ec2_instance }
        if ($did) { $instancesToCheck += @{ Id = $did; Role = "demo"; Source = "backend_api.ec2.instance_id" } }
    }
}

if ($instancesToCheck.Count -eq 0) {
    Write-Host "`nNo EC2 instance to check. " -ForegroundColor Yellow -NoNewline
    Write-Host "Use -InstanceId i-xxxxx to specify one, or ensure $InstanceRole instance exists in deployment_credentials.json" -ForegroundColor Gray
} elseif ($instancesToCheck.Count -gt 1) {
    Write-Host "`nChecking $($instancesToCheck.Count) instance(s):" -ForegroundColor Cyan
}

foreach ($inst in $instancesToCheck) {
    $instId = $inst.Id
    $instRole = $inst.Role
    $instSource = $inst.Source
    Write-Host "`n`n--- EC2 Instance: $instId ($instRole) ---" -ForegroundColor Yellow
    Write-Host "  Source: $instSource" -ForegroundColor Gray
    $instJson = aws ec2 describe-instances --instance-ids $instId --region $Region --output json 2>&1
    if ($LASTEXITCODE -eq 0) {
        $instData = $instJson | ConvertFrom-Json
        $instance = $instData.Reservations[0].Instances[0]
        $nameTag = ($instance.Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -First 1).Value
        if ($nameTag) {
            Write-Host "  Name: $nameTag" -ForegroundColor Gray
        }
        $hasApiSg = $false
        foreach ($isg in $instance.SecurityGroups) {
            $isApiSg = ($isg.GroupId -eq $apiSgId)
            if ($isApiSg) { $hasApiSg = $true }
            $note = if ($isApiSg) { " (required for ALB health checks)" } elseif ($isg.GroupName -eq "default") { " (VPC default; often present)" } else { "" }
            $color = if ($isApiSg) { "Green" } else { "White" }
            Write-Host "  Attached: $($isg.GroupId) - $($isg.GroupName)$note" -ForegroundColor $color
        }
        if (-not $hasApiSg) {
            Write-Host "  WARNING: Instance is missing dental-clinic-api-sg ($apiSgId). ALB health checks will fail." -ForegroundColor Red
        }
    } else {
        Write-Host "  Could not fetch instance: $instJson" -ForegroundColor Red
    }
}

Write-Host "`n" -ForegroundColor Gray
