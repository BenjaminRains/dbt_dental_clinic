# set_env.ps1
Get-Content .dbt-env | ForEach-Object {
    if ($_ -match '^([^#][^=]+)=(.*)$') {
        $name = $matches[1]
        $value = $matches[2]
        if ($name -eq "DBT_MYSQL_PORT") {
            $value = [int]$value
        }
        [Environment]::SetEnvironmentVariable($name, $value)
    }
}