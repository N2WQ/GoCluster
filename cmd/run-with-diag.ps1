# Runs gocluster with diagnostic settings: lower GOGC and enables diag HTTP/heap dump.
# Usage: powershell -File cmd/run-with-diag.ps1
$env:GOGC = "50"           # more frequent GC to keep heap smaller
$env:HEAP_DIAG_ADDR = "localhost:6061"  # exposes /debug/pprof and /debug/heapdump

Write-Host "Starting gocluster with GOGC=$env:GOGC and HEAP_DIAG_ADDR=$env:HEAP_DIAG_ADDR" -ForegroundColor Cyan
& "$PSScriptRoot/../gocluster.exe"
