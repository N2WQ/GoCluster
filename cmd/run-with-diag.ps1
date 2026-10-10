<#
.SYNOPSIS
Starts the root gocluster executable with local diagnostic settings.
.DESCRIPTION
Sets initial GOGC=50 and requests pprof/heap-dump HTTP on localhost:6061.
Positive go_runtime.gc_percent in loaded YAML overrides GOGC; zero retains it.
The executable keeps normal configuration selection and the current directory.
The script propagates its exit code; an executable launch failure exits 1.
.EXAMPLE
pwsh -NoProfile -File ./cmd/run-with-diag.ps1
Run from the repository root after building gocluster.exe.
.NOTES
Prerequisites: the root gocluster.exe and a complete configuration directory.
This launches the normal cluster and its configured services. It sets environment
variables in this PowerShell process and requests the diagnostic listener.
#>
$env:GOGC = "50"
$env:DXC_PPROF_ADDR = "localhost:6061"

Write-Host "Starting gocluster with initial GOGC=$env:GOGC and DXC_PPROF_ADDR=$env:DXC_PPROF_ADDR" -ForegroundColor Cyan
Write-Host "Loaded YAML: positive go_runtime.gc_percent overrides GOGC; zero retains it."
try {
    & "$PSScriptRoot/../gocluster.exe"
    exit $LASTEXITCODE
} catch {
    Write-Error "Unable to launch gocluster: $_" -ErrorAction Continue
    exit 1
}
