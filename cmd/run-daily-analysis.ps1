# Runs the daily call-correction analysis for a given date (default: yesterday, i.e., prior-day data).
# Requirements: Go toolchain (uses modernc sqlite driver), network access for RBN zip if not already downloaded.
param(
    [string]$Date,
    [switch]$ForceDownload
)

$repoRoot   = Split-Path -Parent $PSScriptRoot
$analysisDt = if ($Date) { [DateTime]::Parse($Date) } else { (Get-Date).AddDays(-1) }
$dayStr     = $analysisDt.ToString('yyyy-MM-dd')
$dayCompact = $analysisDt.ToString('yyyyMMdd')
$bustedStr  = $analysisDt.ToString('dd-MMM-yyyy')

$dbPath     = Join-Path $repoRoot "data/logs/callcorr_debug_modified_$dayStr.db"
$bustedPath = Join-Path $repoRoot "data/logs/Busted-$bustedStr.txt"
$zipPath    = Join-Path $repoRoot "data/logs/$dayCompact.zip"
$csvPath    = Join-Path $repoRoot "data/logs/$dayCompact.csv"
$reportDir  = Join-Path $repoRoot "data/reports"
$reportPath = Join-Path $reportDir "analysis-$dayStr.txt"
$rbnUrl     = "https://data.reversebeacon.net/rbn_history/$dayCompact.zip"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "Go toolchain is required (go command not found)."
}

New-Item -ItemType Directory -Path (Split-Path $csvPath) -Force | Out-Null
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null

if (-not (Test-Path $dbPath)) {
    throw "Database not found: $dbPath"
}

if (-not (Test-Path $bustedPath)) {
    throw "Reference busted log not found: $bustedPath"
}

if ($ForceDownload -or -not (Test-Path $zipPath)) {
    Write-Host "Downloading RBN archive $rbnUrl ..."
    Invoke-WebRequest -Uri $rbnUrl -OutFile $zipPath -UseBasicParsing
}

if (-not (Test-Path $csvPath)) {
    Write-Host "Extracting $zipPath ..."
    Expand-Archive -Path $zipPath -DestinationPath (Split-Path $csvPath) -Force
}

Write-Host "Running analysis for $dayStr ..."
Push-Location $repoRoot
try {
    go run ./cmd/run_daily_analysis.go -date $dayStr -db $dbPath -busted $bustedPath -csv $csvPath -report $reportPath
} finally {
    Pop-Location
}
