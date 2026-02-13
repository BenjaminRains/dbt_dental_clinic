# Verify clinic API EC2 instance details against deployment_credentials.json
# Usage: .\scripts\verify_clinic_ec2_details.ps1
#        .\scripts\verify_clinic_ec2_details.ps1 -InstanceId i-xxxxx -ProjectRoot C:\path\to\project
#
# Reads backend_api.clinic_api.ec2 from deployment_credentials.json, calls AWS describe-instances,
# and compares key fields. Use to confirm recorded config matches live instance.

param(
    [string]$InstanceId = "",
    [string]$ProjectRoot = "",
    [string]$Region = "us-east-1"
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

$credentials = Get-Content $credentialsFile -Raw | ConvertFrom-Json
$clinicEc2 = $credentials.backend_api.clinic_api.ec2
if (-not $clinicEc2) {
    Write-Host "backend_api.clinic_api.ec2 not found in deployment_credentials.json" -ForegroundColor Red
    exit 1
}

$idToUse = if ($InstanceId) { $InstanceId } else { $clinicEc2.instance_id }
if (-not $idToUse) {
    Write-Host "No instance ID (deployment_credentials or -InstanceId)" -ForegroundColor Red
    exit 1
}

Write-Host "`nVerifying clinic EC2: $idToUse" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Gray

$describeRaw = cmd /c "aws ec2 describe-instances --instance-ids $idToUse --region $Region 2>&1"
$describeRaw = ($describeRaw | Out-String).Trim()

if ($describeRaw -match "AccessDenied|not authorized|InvalidInstanceID") {
    Write-Host "AWS error:" -ForegroundColor Red
    Write-Host $describeRaw
    exit 1
}

try {
    $describe = $describeRaw | ConvertFrom-Json
} catch {
    Write-Host "Failed to parse describe-instances output." -ForegroundColor Red
    Write-Host $describeRaw
    exit 1
}

$reservations = $describe.Reservations
if (-not $reservations -or $reservations.Count -eq 0) {
    Write-Host "No reservation found for instance $idToUse" -ForegroundColor Red
    exit 1
}

$instance = $reservations[0].Instances[0]
if (-not $instance) {
    Write-Host "No instance in reservation" -ForegroundColor Red
    exit 1
}

# IAM role name from instance profile ARN (e.g. arn:.../instance-profile/dental-clinic-api-clinic-role -> dental-clinic-api-clinic-role)
$awsIamRole = ""
if ($instance.IamInstanceProfile -and $instance.IamInstanceProfile.Arn) {
    $awsIamRole = $instance.IamInstanceProfile.Arn -replace '^.*/', ''
}

$nameTag = ($instance.Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -First 1).Value
$state = $instance.State.Name
$awsPrivateIp = $instance.PrivateIpAddress
$awsPublicIp = if ($instance.PublicIpAddress) { $instance.PublicIpAddress } else { "" }
$awsSubnetId = $instance.SubnetId
$awsVpcId = $instance.VpcId
$awsAmiId = $instance.ImageId
$awsInstanceType = $instance.InstanceType
$awsKeyName = $instance.KeyName

# Build comparison (recorded vs AWS)
$checks = @(
    @{ Name = "Instance ID";     Recorded = $clinicEc2.instance_id;     AWS = $instance.InstanceId }
    @{ Name = "Name (tag)";      Recorded = $clinicEc2.name;             AWS = $nameTag }
    @{ Name = "State";           Recorded = "(n/a)";                     AWS = $state }
    @{ Name = "Instance type";  Recorded = $clinicEc2.instance_type;    AWS = $awsInstanceType }
    @{ Name = "Private IP";     Recorded = $clinicEc2.private_ip;       AWS = $awsPrivateIp }
    @{ Name = "Public IP";      Recorded = $clinicEc2.public_ip;        AWS = $awsPublicIp }
    @{ Name = "VPC ID";         Recorded = $clinicEc2.vpc_id;           AWS = $awsVpcId }
    @{ Name = "Subnet ID";      Recorded = $clinicEc2.subnet_id;        AWS = $awsSubnetId }
    @{ Name = "IAM role";       Recorded = $clinicEc2.iam_role;           AWS = $awsIamRole }
    @{ Name = "AMI ID";         Recorded = $clinicEc2.ami_id;            AWS = $awsAmiId }
    @{ Name = "Key pair";       Recorded = $clinicEc2.key_name;         AWS = $awsKeyName }
)

$mismatches = @()
foreach ($c in $checks) {
    $match = ($c.Recorded -eq $c.AWS) -or ($c.Name -eq "State")
    if ($c.Name -eq "State") {
        Write-Host ("  {0,-18} AWS: {1}" -f $c.Name, $c.AWS) -ForegroundColor $(if ($state -eq "running") { "Green" } else { "Yellow" })
    } elseif ($match) {
        Write-Host ("  {0,-18} OK   {1}" -f $c.Name, $c.AWS) -ForegroundColor Green
    } else {
        Write-Host ("  {0,-18} MISMATCH  recorded={1}  aws={2}" -f $c.Name, $c.Recorded, $c.AWS) -ForegroundColor Red
        $mismatches += $c.Name
    }
}

# Optional: public DNS and private DNS from AWS (for reference)
if ($instance.PublicDnsName) {
    Write-Host ("  {0,-18} AWS: {1}" -f "Public DNS", $instance.PublicDnsName) -ForegroundColor Gray
}
if ($instance.PrivateDnsName) {
    Write-Host ("  {0,-18} AWS: {1}" -f "Private DNS", $instance.PrivateDnsName) -ForegroundColor Gray
}

Write-Host ""
if ($mismatches.Count -gt 0) {
    Write-Host "Mismatches: $($mismatches -join ', '). Update deployment_credentials.json or the instance if needed." -ForegroundColor Yellow
    exit 1
}

Write-Host "All recorded details match the live instance." -ForegroundColor Green
exit 0
