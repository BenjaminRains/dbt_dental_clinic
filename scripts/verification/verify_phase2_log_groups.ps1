# Verify Phase 2 CloudWatch log groups for demo and clinic API (Step 7).
# Usage: .\scripts\verify_phase2_log_groups.ps1
#        .\scripts\verify_phase2_log_groups.ps1 -FailIfMissing
#
# Accepts either full names (/dental-clinic/api-demo, /dental-clinic/api-clinic) or
# short names (api-demo, api-clinic). Requires: AWS CLI with logs:DescribeLogGroups

param(
    [string]$Region = "us-east-1",
    [switch]$FailIfMissing
)

$ErrorActionPreference = "Stop"

# Each slot can be satisfied by either the full or short log group name
$ExpectedSlots = @(
    @{ Demo = @("/dental-clinic/api-demo", "api-demo") },
    @{ Clinic = @("/dental-clinic/api-clinic", "api-clinic") }
) | ForEach-Object { $_.GetEnumerator() } | ForEach-Object { @{ Name = $_.Key; AcceptNames = $_.Value } }

Write-Host "`nPhase 2 – CloudWatch log groups verification" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Gray
Write-Host "  Region: $Region" -ForegroundColor White
Write-Host "  Expected: demo → /dental-clinic/api-demo or api-demo; clinic → /dental-clinic/api-clinic or api-clinic" -ForegroundColor White
Write-Host ""

# Query with multiple prefixes so we find both /dental-clinic/* and api-* names
$found = @{}
$prefixes = @("/dental-clinic/", "api")
foreach ($prefix in $prefixes) {
    $raw = cmd /c "aws logs describe-log-groups --log-group-name-prefix $prefix --region $Region --output json 2>&1"
    $raw = ($raw | Out-String).Trim()
    if ($raw -match "AccessDenied|not authorized") {
        Write-Host "Access denied listing log groups. Your credentials need logs:DescribeLogGroups." -ForegroundColor Red
        Write-Host $raw -ForegroundColor Gray
        exit 1
    }
    if ($raw -match '^\s*\{') {
        try {
            $result = $raw | ConvertFrom-Json
            $list = $result.logGroups
            if ($list) {
                foreach ($lg in $list) {
                    $found[$lg.logGroupName] = @{
                        RetentionInDays = $lg.retentionInDays
                        StoredBytes    = $lg.storedBytes
                    }
                }
            }
        } catch {
            Write-Host "Failed to parse describe-log-groups output (prefix $prefix)." -ForegroundColor Red
            Write-Host $raw -ForegroundColor Gray
            exit 1
        }
    }
}

Write-Host "Log groups" -ForegroundColor Cyan
Write-Host ("-" * 70) -ForegroundColor Gray

$missing = @()
foreach ($slot in $ExpectedSlots) {
    $slotName = $slot.Name
    $acceptNames = $slot.AcceptNames
    $matchedName = $null
    foreach ($n in $acceptNames) {
        if ($found.ContainsKey($n)) {
            $matchedName = $n
            break
        }
    }
    if ($matchedName) {
        $info = $found[$matchedName]
        $retention = if ($null -eq $info.RetentionInDays) { "Never expire" } else { "$($info.RetentionInDays) days" }
        Write-Host "  OK   $slotName  →  $matchedName" -ForegroundColor Green
        Write-Host "       Retention: $retention" -ForegroundColor Gray
    } else {
        Write-Host "  MISSING   $slotName  (none of: $($acceptNames -join ', '))" -ForegroundColor Red
        $missing += $slotName
    }
}

# List any other relevant groups found (informational)
$allAccept = $ExpectedSlots | ForEach-Object { $_.AcceptNames } | ForEach-Object { $_ }
$others = @($found.Keys | Where-Object { $_ -notin $allAccept })
if ($others.Count -gt 0) {
    Write-Host ""
    Write-Host "  Other groups found:" -ForegroundColor Gray
    foreach ($o in $others) {
        Write-Host "       $o" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "Summary" -ForegroundColor Cyan
Write-Host ("-" * 70) -ForegroundColor Gray
if ($missing.Count -eq 0) {
    Write-Host "  All expected log groups present." -ForegroundColor Green
} else {
    Write-Host "  Missing: $($missing -join ', ')" -ForegroundColor Red
    Write-Host "  Create in CloudWatch → Log groups → Create log group (Step 7.1)." -ForegroundColor Yellow
    if ($FailIfMissing) {
        exit 1
    }
}

Write-Host ""
Write-Host "Done." -ForegroundColor Gray
exit 0
