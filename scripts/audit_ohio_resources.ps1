# PowerShell script to audit AWS resources in Ohio region (us-east-2)
# This script helps identify all resources that can potentially be deleted

param(
    [string]$Region = "us-east-2",
    [switch]$ExportToFile = $false
)

$ErrorActionPreference = "Continue"

Write-Host "`n🔍 Auditing AWS Resources in Ohio Region (us-east-2)" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Gray
Write-Host ""

# Check AWS credentials
try {
    $identity = aws sts get-caller-identity 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ AWS credentials configured" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "❌ AWS credentials not configured. Please run 'aws configure' first." -ForegroundColor Red
    exit 1
}

$output = @()
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"

# Function to add output line
function Add-Output {
    param([string]$Category, [string]$Resource, [string]$Details, [string]$Action)
    $output += [PSCustomObject]@{
        Category = $Category
        Resource = $Resource
        Details = $Details
        Action = $Action
        Timestamp = $timestamp
    }
}

Write-Host "📋 EC2 Instances" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws ec2 describe-instances --region $Region --query 'Reservations[*].Instances[*].[InstanceId,State.Name,InstanceType,LaunchTime,Tags[?Key==`Name`].Value|[0],Tags[?Key==`Purpose`].Value|[0]]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $instances = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure - flatten if needed
    if ($instances -is [System.Array] -and $instances.Count -gt 0 -and $instances[0] -is [System.Array]) {
        $instances = $instances | ForEach-Object { $_ }
    }
    
    if ($null -eq $instances -or $instances.Count -eq 0 -or ($instances.Count -eq 1 -and $null -eq $instances[0])) {
        Write-Host "  ✅ No EC2 instances found in Ohio region" -ForegroundColor Green
        Add-Output "EC2" "None" "No instances found" "N/A"
    } else {
        foreach ($instance in $instances) {
            if ($null -eq $instance -or $instance.Count -lt 3) { continue }
            
            $instanceId = $instance[0]
            $state = $instance[1]
            $type = $instance[2]
            $launchTime = if ($instance.Count -gt 3) { $instance[3] } else { "Unknown" }
            $name = if ($instance.Count -gt 4 -and $instance[4]) { $instance[4] } else { "No Name Tag" }
            $purpose = if ($instance.Count -gt 5 -and $instance[5]) { $instance[5] } else { "Unknown" }
            
            $action = if ($state -eq "running") { "⚠️  STOP/TERMINATE (running)" } 
                     elseif ($state -eq "stopped") { "✅ SAFE TO DELETE (stopped)" }
                     else { "⚠️  REVIEW (state: $state)" }
            
            Write-Host "  Instance ID: $instanceId" -ForegroundColor White
            Write-Host "    Name: $name" -ForegroundColor Gray
            Write-Host "    State: $state" -ForegroundColor $(if ($state -eq "stopped") { "Green" } else { "Yellow" })
            Write-Host "    Type: $type" -ForegroundColor Gray
            Write-Host "    Purpose: $purpose" -ForegroundColor Gray
            Write-Host "    Launched: $launchTime" -ForegroundColor Gray
            Write-Host "    Action: $action" -ForegroundColor $(if ($state -eq "stopped") { "Green" } else { "Yellow" })
            Write-Host ""
            
            Add-Output "EC2" $instanceId "$name | $type | $state | $purpose" $action
        }
    }
} catch {
    Write-Host "  ⚠️  Error listing EC2 instances: $_" -ForegroundColor Yellow
    Add-Output "EC2" "Error" "Failed to list instances: $_" "REVIEW"
}

Write-Host "📋 RDS Instances" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws rds describe-db-instances --region $Region --query 'DBInstances[*].[DBInstanceIdentifier,DBInstanceStatus,DBInstanceClass,Engine,AllocatedStorage,DBInstanceArn]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $rdsInstances = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure - flatten if needed
    if ($rdsInstances -is [System.Array] -and $rdsInstances.Count -gt 0 -and $rdsInstances[0] -is [System.Array]) {
        $rdsInstances = $rdsInstances | ForEach-Object { $_ }
    }
    
    if ($null -eq $rdsInstances -or $rdsInstances.Count -eq 0 -or ($rdsInstances.Count -eq 1 -and $null -eq $rdsInstances[0])) {
        Write-Host "  ✅ No RDS instances found in Ohio region" -ForegroundColor Green
        Add-Output "RDS" "None" "No instances found" "N/A"
    } else {
        foreach ($rds in $rdsInstances) {
            if ($null -eq $rds -or $rds.Count -lt 3) { continue }
            
            $dbId = $rds[0]
            $status = $rds[1]
            $class = if ($rds.Count -gt 2) { $rds[2] } else { "Unknown" }
            $engine = if ($rds.Count -gt 3) { $rds[3] } else { "Unknown" }
            $storage = if ($rds.Count -gt 4) { $rds[4] } else { "Unknown" }
            $arn = if ($rds.Count -gt 5) { $rds[5] } else { "Unknown" }
            
            $action = if ($status -eq "available") { "⚠️  DELETE (create snapshot first!)" } 
                     else { "⚠️  REVIEW (status: $status)" }
            
            Write-Host "  DB Instance: $dbId" -ForegroundColor White
            Write-Host "    Status: $status" -ForegroundColor $(if ($status -eq "stopped") { "Green" } else { "Yellow" })
            Write-Host "    Class: $class" -ForegroundColor Gray
            Write-Host "    Engine: $engine" -ForegroundColor Gray
            Write-Host "    Storage: $storage GB" -ForegroundColor Gray
            Write-Host "    Action: $action" -ForegroundColor Yellow
            Write-Host ""
            
            Add-Output "RDS" $dbId "$class | $engine | $status | ${storage}GB" $action
        }
    }
} catch {
    Write-Host "  ⚠️  Error listing RDS instances: $_" -ForegroundColor Yellow
    Add-Output "RDS" "Error" "Failed to list instances: $_" "REVIEW"
}

Write-Host "📋 Elastic IPs" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws ec2 describe-addresses --region $Region --query 'Addresses[*].[PublicIp,AllocationId,AssociationId,InstanceId,NetworkInterfaceId]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $eips = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure
    if ($eips -is [System.Array] -and $eips.Count -gt 0 -and $eips[0] -is [System.Array]) {
        $eips = $eips | ForEach-Object { $_ }
    }
    
    if ($null -eq $eips -or $eips.Count -eq 0 -or ($eips.Count -eq 1 -and $null -eq $eips[0])) {
        Write-Host "  ✅ No Elastic IPs found in Ohio region" -ForegroundColor Green
        Add-Output "ElasticIP" "None" "No Elastic IPs found" "N/A"
    } else {
        foreach ($eip in $eips) {
            if ($null -eq $eip -or $eip.Count -lt 2) { continue }
            
            $publicIp = $eip[0]
            $allocationId = $eip[1]
            $associationId = if ($eip.Count -gt 2) { $eip[2] } else { $null }
            $instanceId = if ($eip.Count -gt 3) { $eip[3] } else { $null }
            $eniId = if ($eip.Count -gt 4) { $eip[4] } else { $null }
            
            $action = if (-not $associationId) { "✅ RELEASE (not associated)" } 
                     else { "⚠️  REVIEW (associated with $instanceId)" }
            
            Write-Host "  Elastic IP: $publicIp" -ForegroundColor White
            Write-Host "    Allocation ID: $allocationId" -ForegroundColor Gray
            Write-Host "    Associated: $(if ($associationId) { "Yes - $instanceId" } else { "No" })" -ForegroundColor Gray
            Write-Host "    Action: $action" -ForegroundColor $(if (-not $associationId) { "Green" } else { "Yellow" })
            Write-Host ""
            
            Add-Output "ElasticIP" $allocationId "$publicIp | $(if ($associationId) { "Associated" } else { "Unassociated" })" $action
        }
    }
} catch {
    Write-Host "  ⚠️  Error listing Elastic IPs: $_" -ForegroundColor Yellow
    Add-Output "ElasticIP" "Error" "Failed to list Elastic IPs: $_" "REVIEW"
}

Write-Host "📋 VPCs" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws ec2 describe-vpcs --region $Region --query 'Vpcs[*].[VpcId,CidrBlock,IsDefault,Tags[?Key==`Name`].Value|[0]]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $vpcs = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure
    if ($vpcs -is [System.Array] -and $vpcs.Count -gt 0 -and $vpcs[0] -is [System.Array]) {
        $vpcs = $vpcs | ForEach-Object { $_ }
    }
    
    if ($null -eq $vpcs -or $vpcs.Count -eq 0 -or ($vpcs.Count -eq 1 -and $null -eq $vpcs[0])) {
        Write-Host "  ✅ No VPCs found in Ohio region" -ForegroundColor Green
        Add-Output "VPC" "None" "No VPCs found" "N/A"
    } else {
        foreach ($vpc in $vpcs) {
            if ($null -eq $vpc -or $vpc.Count -lt 3) { continue }
            
            $vpcId = $vpc[0]
            $cidr = $vpc[1]
            $isDefault = $vpc[2]
            $name = if ($vpc.Count -gt 3 -and $vpc[3]) { $vpc[3] } else { "No Name Tag" }
            
            $action = if ($isDefault) { "⚠️  KEEP (default VPC)" } 
                     else { "⚠️  REVIEW (check for dependencies)" }
            
            Write-Host "  VPC: $vpcId" -ForegroundColor White
            Write-Host "    Name: $name" -ForegroundColor Gray
            Write-Host "    CIDR: $cidr" -ForegroundColor Gray
            Write-Host "    Default: $isDefault" -ForegroundColor Gray
            Write-Host "    Action: $action" -ForegroundColor Yellow
            Write-Host ""
            
            Add-Output "VPC" $vpcId "$name | $cidr | Default: $isDefault" $action
        }
    }
} catch {
    Write-Host "  ⚠️  Error listing VPCs: $_" -ForegroundColor Yellow
    Add-Output "VPC" "Error" "Failed to list VPCs" "REVIEW"
}

Write-Host "📋 EBS Volumes" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws ec2 describe-volumes --region $Region --query 'Volumes[*].[VolumeId,Size,State,VolumeType,Attachments[0].InstanceId,Tags[?Key==`Name`].Value|[0]]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $volumes = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure
    if ($volumes -is [System.Array] -and $volumes.Count -gt 0 -and $volumes[0] -is [System.Array]) {
        $volumes = $volumes | ForEach-Object { $_ }
    }
    
    if ($null -eq $volumes -or $volumes.Count -eq 0 -or ($volumes.Count -eq 1 -and $null -eq $volumes[0])) {
        Write-Host "  ✅ No EBS volumes found in Ohio region" -ForegroundColor Green
        Add-Output "EBS" "None" "No volumes found" "N/A"
    } else {
        foreach ($vol in $volumes) {
            if ($null -eq $vol -or $vol.Count -lt 4) { continue }
            
            $volumeId = $vol[0]
            $size = $vol[1]
            $state = $vol[2]
            $type = $vol[3]
            $attachedTo = if ($vol.Count -gt 4) { $vol[4] } else { $null }
            $name = if ($vol.Count -gt 5 -and $vol[5]) { $vol[5] } else { "No Name Tag" }
            
            $action = if ($state -eq "available" -and -not $attachedTo) { "✅ DELETE (unattached)" } 
                     elseif ($attachedTo) { "⚠️  REVIEW (attached to $attachedTo)" }
                     else { "⚠️  REVIEW (state: $state)" }
            
            Write-Host "  Volume: $volumeId" -ForegroundColor White
            Write-Host "    Name: $name" -ForegroundColor Gray
            Write-Host "    Size: $size GB" -ForegroundColor Gray
            Write-Host "    Type: $type" -ForegroundColor Gray
            Write-Host "    State: $state" -ForegroundColor Gray
            Write-Host "    Attached: $(if ($attachedTo) { "Yes - $attachedTo" } else { "No" })" -ForegroundColor Gray
            Write-Host "    Action: $action" -ForegroundColor $(if ($state -eq "available" -and -not $attachedTo) { "Green" } else { "Yellow" })
            Write-Host ""
            
            Add-Output "EBS" $volumeId "$name | ${size}GB | $type | $state" $action
        }
    }
} catch {
    Write-Host "  ⚠️  Error listing EBS volumes: $_" -ForegroundColor Yellow
    Add-Output "EBS" "Error" "Failed to list volumes" "REVIEW"
}

Write-Host "📋 Load Balancers" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    # Application Load Balancers
    $jsonOutput = aws elbv2 describe-load-balancers --region $Region --query 'LoadBalancers[*].[LoadBalancerName,LoadBalancerArn,State.Code,Type]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $albs = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure
    if ($albs -is [System.Array] -and $albs.Count -gt 0 -and $albs[0] -is [System.Array]) {
        $albs = $albs | ForEach-Object { $_ }
    }
    
    if ($null -eq $albs -or $albs.Count -eq 0 -or ($albs.Count -eq 1 -and $null -eq $albs[0])) {
        Write-Host "  ✅ No Load Balancers found in Ohio region" -ForegroundColor Green
        Add-Output "LoadBalancer" "None" "No load balancers found" "N/A"
    } else {
        foreach ($alb in $albs) {
            if ($null -eq $alb -or $alb.Count -lt 2) { continue }
            
            $name = $alb[0]
            $arn = if ($alb.Count -gt 1) { $alb[1] } else { "Unknown" }
            $state = if ($alb.Count -gt 2) { $alb[2] } else { "Unknown" }
            $type = if ($alb.Count -gt 3) { $alb[3] } else { "Unknown" }
            
            Write-Host "  Load Balancer: $name" -ForegroundColor White
            Write-Host "    Type: $type" -ForegroundColor Gray
            Write-Host "    State: $state" -ForegroundColor Gray
            Write-Host "    Action: ⚠️  DELETE (if not needed)" -ForegroundColor Yellow
            Write-Host ""
            
            Add-Output "LoadBalancer" $name "$type | $state" "DELETE"
        }
    }
} catch {
    Write-Host "  ⚠️  Error listing Load Balancers: $_" -ForegroundColor Yellow
    Add-Output "LoadBalancer" "Error" "Failed to list load balancers: $_" "REVIEW"
}

Write-Host "📋 Snapshots" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws ec2 describe-snapshots --region $Region --owner-ids self --query 'Snapshots[*].[SnapshotId,VolumeSize,StartTime,Description,Tags[?Key==`Name`].Value|[0]]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $snapshots = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure
    if ($snapshots -is [System.Array] -and $snapshots.Count -gt 0 -and $snapshots[0] -is [System.Array]) {
        $snapshots = $snapshots | ForEach-Object { $_ }
    }
    
    if ($null -eq $snapshots -or $snapshots.Count -eq 0 -or ($snapshots.Count -eq 1 -and $null -eq $snapshots[0])) {
        Write-Host "  ✅ No snapshots found in Ohio region" -ForegroundColor Green
        Add-Output "Snapshot" "None" "No snapshots found" "N/A"
    } else {
        Write-Host "  Found $($snapshots.Count) snapshot(s)" -ForegroundColor White
        $totalSize = ($snapshots | Where-Object { $null -ne $_ -and $_.Count -gt 1 } | ForEach-Object { $_[1] } | Measure-Object -Sum).Sum
        Write-Host "  Total Size: $totalSize GB" -ForegroundColor Gray
        Write-Host "  Action: ⚠️  REVIEW (delete old/unused snapshots)" -ForegroundColor Yellow
        Write-Host ""
        
        Add-Output "Snapshot" "Multiple" "$($snapshots.Count) snapshots, ${totalSize}GB total" "REVIEW"
    }
} catch {
    Write-Host "  ⚠️  Error listing snapshots: $_" -ForegroundColor Yellow
    Add-Output "Snapshot" "Error" "Failed to list snapshots: $_" "REVIEW"
}

Write-Host "📋 Security Groups" -ForegroundColor Yellow
Write-Host "-" * 70 -ForegroundColor Gray
try {
    $jsonOutput = aws ec2 describe-security-groups --region $Region --query 'SecurityGroups[*].[GroupId,GroupName,Description]' --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error: $jsonOutput"
    }
    
    $sgs = $jsonOutput | ConvertFrom-Json
    
    # Handle nested array structure
    if ($sgs -is [System.Array] -and $sgs.Count -gt 0 -and $sgs[0] -is [System.Array]) {
        $sgs = $sgs | ForEach-Object { $_ }
    }
    
    if ($null -eq $sgs -or $sgs.Count -eq 0 -or ($sgs.Count -eq 1 -and $null -eq $sgs[0])) {
        Write-Host "  ✅ No Security Groups found in Ohio region" -ForegroundColor Green
        Add-Output "SecurityGroup" "None" "No security groups found" "N/A"
    } else {
        Write-Host "  Found $($sgs.Count) security group(s)" -ForegroundColor White
        Write-Host "  Action: ⚠️  REVIEW (delete if not used)" -ForegroundColor Yellow
        Write-Host ""
        
        Add-Output "SecurityGroup" "Multiple" "$($sgs.Count) security groups" "REVIEW"
    }
} catch {
    Write-Host "  ⚠️  Error listing security groups: $_" -ForegroundColor Yellow
    Add-Output "SecurityGroup" "Error" "Failed to list security groups: $_" "REVIEW"
}

# Summary
Write-Host "`n📊 Summary" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Gray

$totalResources = $output.Count
$safeToDelete = ($output | Where-Object { $_.Action -like "*SAFE*" -or $_.Action -like "*DELETE*" }).Count
$needsReview = ($output | Where-Object { $_.Action -like "*REVIEW*" }).Count

Write-Host "Total Resources Found: $totalResources" -ForegroundColor White
Write-Host "Safe to Delete: $safeToDelete" -ForegroundColor Green
Write-Host "Needs Review: $needsReview" -ForegroundColor Yellow
Write-Host ""

# Export to CSV if requested
if ($ExportToFile) {
    # Save to project root directory
    $projectRoot = Split-Path $PSScriptRoot -Parent
    $csvFile = Join-Path $projectRoot "ohio_resources_audit_$timestamp.csv"
    $output | Export-Csv -Path $csvFile -NoTypeInformation
    Write-Host "✅ Audit results exported to: $csvFile" -ForegroundColor Green
    Write-Host "   (Project root directory)" -ForegroundColor Gray
    Write-Host ""
}

# Recommendations
Write-Host "💡 Recommendations" -ForegroundColor Cyan
Write-Host "-" * 70 -ForegroundColor Gray
Write-Host "1. Review all resources listed above" -ForegroundColor White
Write-Host "2. For RDS instances: Create final snapshot before deleting" -ForegroundColor Yellow
Write-Host "3. For running EC2 instances: Stop first, then terminate" -ForegroundColor Yellow
Write-Host "4. For Elastic IPs: Release unattached IPs immediately" -ForegroundColor Green
Write-Host "5. For EBS volumes: Delete unattached volumes" -ForegroundColor Green
Write-Host "6. Verify N. Virginia resources are working before deleting Ohio" -ForegroundColor Yellow
Write-Host ""

Write-Host "✅ Audit Complete!" -ForegroundColor Green
Write-Host ""
