# Load environment variables from .dbt-env
if (Test-Path -Path ".dbt-env") {
    Get-Content ".dbt-env" | ForEach-Object {
        if (-not $_.StartsWith("#") -and $_.Contains("=")) {
            $name, $value = $_.Split("=", 2)
            Set-Item -Path "env:$name" -Value $value
        }
    }
}

# Run dbt with all arguments passed to this script
dbt $args
