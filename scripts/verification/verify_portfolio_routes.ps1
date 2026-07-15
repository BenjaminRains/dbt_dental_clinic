<#
.SYNOPSIS
    Smoke-check portfolio (demo) routes: clinic paths must not serve clinic UI.

.DESCRIPTION
    Hits the public demo host and asserts:
      - / returns 200
      - /dashboard returns 200 (SPA deep link / fallback)
      - /home/owner does not return a clinic portal page (HTML must not contain
        clinic-only markers such as "MDC & GLIC Analytics" or Sign out chrome)

    Usage:
      .\scripts\verification\verify_portfolio_routes.ps1
      .\scripts\verification\verify_portfolio_routes.ps1 -BaseUrl https://dbtdentalclinic.com
#>

param(
    [string]$BaseUrl = "https://dbtdentalclinic.com"
)

$ErrorActionPreference = "Stop"
$BaseUrl = $BaseUrl.TrimEnd("/")

function Get-Page([string]$Path) {
    $uri = "$BaseUrl$Path"
    try {
        $resp = Invoke-WebRequest -Uri $uri -UseBasicParsing -MaximumRedirection 5
        return [pscustomobject]@{
            Uri        = $uri
            StatusCode = [int]$resp.StatusCode
            Content    = [string]$resp.Content
        }
    }
    catch {
        $code = $null
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $code = [int]$_.Exception.Response.StatusCode.value__
        }
        return [pscustomobject]@{
            Uri        = $uri
            StatusCode = $code
            Content    = ""
            Error      = $_.Exception.Message
        }
    }
}

$failed = $false

Write-Host "Verifying portfolio routes at $BaseUrl" -ForegroundColor Cyan

$home = Get-Page "/"
if ($home.StatusCode -ne 200) {
    Write-Host "FAIL / -> HTTP $($home.StatusCode)" -ForegroundColor Red
    $failed = $true
}
elseif ($home.Content -notmatch "Benjamin Rains|Dental") {
    Write-Host "FAIL / -> 200 but unexpected content (expected portfolio markers)" -ForegroundColor Red
    $failed = $true
}
else {
    Write-Host "OK   / -> $($home.StatusCode)" -ForegroundColor Green
}

$dashboard = Get-Page "/dashboard"
if ($dashboard.StatusCode -ne 200) {
    Write-Host "FAIL /dashboard -> HTTP $($dashboard.StatusCode) (check SPA fallbacks)" -ForegroundColor Red
    $failed = $true
}
else {
    Write-Host "OK   /dashboard -> $($dashboard.StatusCode)" -ForegroundColor Green
}

$owner = Get-Page "/home/owner"
# After client redirect the initial HTML is still the SPA shell (200). Assert no clinic product title.
if ($owner.Content -match "MDC\s*&\s*GLIC Analytics") {
    Write-Host "FAIL /home/owner HTML contains clinic product marker" -ForegroundColor Red
    $failed = $true
}
elseif ($owner.StatusCode -eq 200 -or $owner.StatusCode -eq 404) {
    Write-Host "OK   /home/owner -> HTTP $($owner.StatusCode) (no clinic product marker)" -ForegroundColor Green
}
else {
    Write-Host "WARN /home/owner -> HTTP $($owner.StatusCode)" -ForegroundColor Yellow
}

if ($failed) {
    Write-Host "Portfolio route verification FAILED" -ForegroundColor Red
    exit 1
}

Write-Host "Portfolio route verification PASSED" -ForegroundColor Green
exit 0
