# Verify ALB listener rules for dental clinic API (demo vs clinic host routing)
# Usage: .\scripts\verify_alb_listener_rules.ps1
#        .\scripts\verify_alb_listener_rules.ps1 -AlbName dental-clinic-api-alb
#
# Reads ALB name from deployment_credentials.json or uses dental-clinic-api-alb.
# Lists listeners and rules on HTTPS:443; shows host header -> target group mapping.
# Use after Step 5 (Update ALB Listener Rules) to confirm api / api-clinic routing.

param(
    [string]$ProjectRoot = "",
    [string]$Region = "us-east-1",
    [string]$AlbName = "",
    [switch]$FailIfMismatch
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrEmpty($ProjectRoot)) {
    $ProjectRoot = Split-Path $PSScriptRoot -Parent
}

# Resolve ALB name
if ([string]::IsNullOrWhiteSpace($AlbName)) {
    $credentialsFile = Join-Path $ProjectRoot "deployment_credentials.json"
    if (Test-Path $credentialsFile) {
        try {
            $creds = Get-Content $credentialsFile -Raw | ConvertFrom-Json
            $AlbName = $creds.backend_api.application_load_balancer.name
        } catch { }
    }
    if ([string]::IsNullOrWhiteSpace($AlbName)) {
        $AlbName = "dental-clinic-api-alb"
    }
}

# Get ALB ARN
$albJson = aws elbv2 describe-load-balancers --names $AlbName --region $Region --output json 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ALB not found: $AlbName" -ForegroundColor Red
    Write-Host $albJson
    exit 1
}
$alb = ($albJson | ConvertFrom-Json).LoadBalancers[0]
$albArn = $alb.LoadBalancerArn
$albDns = $alb.DNSName
$albState = $alb.State.Code

Write-Host "`nDental Clinic – ALB Listener Rules" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Gray
Write-Host "  ALB:   $AlbName" -ForegroundColor White
Write-Host "  ARN:   $albArn" -ForegroundColor Gray
Write-Host "  DNS:   $albDns" -ForegroundColor Gray
Write-Host "  State: $albState" -ForegroundColor $(if ($albState -eq "active") { "Green" } else { "Yellow" })
Write-Host ""

# List listeners
$listenersJson = aws elbv2 describe-listeners --load-balancer-arn $albArn --region $Region --output json 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to list listeners: $listenersJson" -ForegroundColor Red
    exit 1
}
$listeners = ($listenersJson | ConvertFrom-Json).Listeners

# Find HTTPS:443 (and optionally HTTP:80)
$httpsListener = $listeners | Where-Object { $_.Port -eq 443 } | Select-Object -First 1
if (-not $httpsListener) {
    Write-Host "No HTTPS:443 listener found on $AlbName" -ForegroundColor Red
    exit 1
}

$listenerArn = $httpsListener.ListenerArn
Write-Host "Listener HTTPS:443" -ForegroundColor Yellow
Write-Host "  ARN: $listenerArn" -ForegroundColor Gray
Write-Host ""

# Describe rules
$rulesJson = aws elbv2 describe-rules --listener-arn $listenerArn --region $Region --output json 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to describe rules: $rulesJson" -ForegroundColor Red
    exit 1
}
$rules = ($rulesJson | ConvertFrom-Json).Rules

# Resolve target group ARNs to names (cache)
$tgArnToName = @{}
$allTgArns = @()
foreach ($r in $rules) {
    foreach ($action in $r.Actions) {
        if ($action.Type -ne "forward") { continue }
        if ($action.TargetGroupArn) {
            $allTgArns += $action.TargetGroupArn
        }
        if ($action.ForwardConfig -and $action.ForwardConfig.TargetGroups) {
            foreach ($tg in $action.ForwardConfig.TargetGroups) {
                if ($tg.TargetGroupArn) { $allTgArns += $tg.TargetGroupArn }
            }
        }
    }
}
$allTgArns = $allTgArns | Select-Object -Unique
if ($allTgArns.Count -gt 0) {
    $tgListJson = aws elbv2 describe-target-groups --target-group-arns $allTgArns --region $Region --output json 2>&1
    if ($LASTEXITCODE -eq 0) {
        $tgList = ($tgListJson | ConvertFrom-Json).TargetGroups
        foreach ($t in $tgList) {
            $tgArnToName[$t.TargetGroupArn] = $t.TargetGroupName
        }
    }
}

# Expected Phase 2 mapping (for validation)
$expectedHostToTg = @{
    "api.dbtdentalclinic.com"        = "dental-clinic-api-demo-tg"
    "api-clinic.dbtdentalclinic.com" = "dental-clinic-api-clinic-tg"
}
$actualHostToTg = @{}
$mismatches = @()

Write-Host "Rules (priority, host header -> target group)" -ForegroundColor Cyan
Write-Host ("-" * 70) -ForegroundColor Gray

foreach ($rule in ($rules | Sort-Object { if ($_.Priority -eq 'default') { [int]::MaxValue } else { [int]$_.Priority } })) {
    $pri = $rule.Priority
    $isDefault = $rule.IsDefault
    $hostValues = @()
    foreach ($cond in $rule.Conditions) {
        if ($cond.Field -eq "host-header" -and $cond.Values) {
            $hostValues = @($cond.Values)
            break
        }
    }
    $tgNames = @()
    foreach ($action in $rule.Actions) {
        if ($action.Type -eq "forward") {
            if ($action.TargetGroupArn) {
                $name = $tgArnToName[$action.TargetGroupArn]
                if ($name) { $tgNames += $name } else { $tgNames += $action.TargetGroupArn }
            }
            if ($action.ForwardConfig -and $action.ForwardConfig.TargetGroups) {
                foreach ($tg in $action.ForwardConfig.TargetGroups) {
                    $name = $tgArnToName[$tg.TargetGroupArn]
                    if ($name) { $tgNames += $name } else { $tgNames += $tg.TargetGroupArn }
                }
            }
        }
    }
    $hostStr = if ($hostValues.Count -gt 0) { $hostValues -join ", " } else { "(default)" }
    $tgStr = if ($tgNames.Count -gt 0) { $tgNames -join ", " } else { "(none)" }
    $color = if ($isDefault) { "DarkGray" } else { "White" }
    Write-Host "  $pri  $hostStr  ->  $tgStr" -ForegroundColor $color
    foreach ($h in $hostValues) {
        $actualHostToTg[$h] = $tgStr
    }
}

# Validate expected mapping
Write-Host ""
foreach ($h in $expectedHostToTg.Keys) {
    $expectedTg = $expectedHostToTg[$h]
    $actualTg = $actualHostToTg[$h]
    if ($null -eq $actualTg) {
        Write-Host "  Missing rule for host: $h (expected -> $expectedTg)" -ForegroundColor Yellow
        if ($FailIfMismatch) { $mismatches += "Missing: $h" }
    } elseif ($actualTg -ne $expectedTg) {
        Write-Host "  Mismatch $h : expected $expectedTg, got $actualTg" -ForegroundColor Red
        if ($FailIfMismatch) { $mismatches += "$h" }
    } else {
        Write-Host "  OK  $h  ->  $expectedTg" -ForegroundColor Green
    }
}

Write-Host ""
if ($FailIfMismatch -and $mismatches.Count -gt 0) {
    Write-Host "Validation failed. Fix ALB listener rules (Step 5)." -ForegroundColor Red
    exit 1
}
Write-Host "Done. Validate endpoints: curl -I https://api.dbtdentalclinic.com/health ; curl -I https://api-clinic.dbtdentalclinic.com/health" -ForegroundColor Gray
exit 0
