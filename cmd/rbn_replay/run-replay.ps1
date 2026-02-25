param(
  [Parameter(Mandatory = $true)]
  [string[]]$Dates,

  [string]$ReplayConfig = "cmd/rbn_replay/replay.yaml",
  [string]$RepoRoot = "C:\src\gocluster",
  [switch]$ForceDownload
)

$ErrorActionPreference = "Stop"

Set-Location $RepoRoot

foreach ($date in $Dates) {
  $start = Get-Date
  Write-Host ("[{0}] Starting replay for {1}" -f $start.ToString("s"), $date)

  $args = @(
    "run", "./cmd/rbn_replay",
    "-date", $date,
    "-replay-config", $ReplayConfig
  )
  if ($ForceDownload) {
    $args += "-force-download"
  }

  go @args
  if ($LASTEXITCODE -ne 0) {
    throw "Replay failed for $date (exit code $LASTEXITCODE)"
  }

  $end = Get-Date
  Write-Host ("[{0}] Completed replay for {1} (duration {2})" -f $end.ToString("s"), $date, ($end - $start))
}

Write-Host "All replay runs completed."
