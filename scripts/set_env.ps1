# Read the .env file and set environment variables
param(
    [string]$ProjectPath = (Get-Location)
)

$envFile = Join-Path $ProjectPath ".env"

Get-Content $envFile | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}