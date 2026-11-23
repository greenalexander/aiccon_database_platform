# Load config/.env into this PowerShell session for dbt.
# Usage, from the repository root:  .\dbt\load_env.ps1
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$envFile  = Join-Path $repoRoot "config\.env"

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }   # skip blanks and comments
    $name, $value = $line -split "=", 2
    $name  = $name.Trim()
    $value = $value.Trim().Trim('"').Trim("'")
    if ($name -eq "GOOGLE_APPLICATION_CREDENTIALS" -and -not [System.IO.Path]::IsPathRooted($value)) {
        $value = Join-Path $repoRoot $value                   # make the key path absolute
    }
    Set-Item -Path "Env:$name" -Value $value
}
$env:DBT_PROFILES_DIR = Join-Path $repoRoot "dbt"
Write-Host "Loaded config/.env. DBT_PROFILES_DIR = $env:DBT_PROFILES_DIR"