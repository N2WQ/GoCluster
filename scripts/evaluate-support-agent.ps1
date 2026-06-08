<#
.SYNOPSIS
  Runs local GoCluster support-agent evaluation cases.

.DESCRIPTION
  Imports the checked-in support-agent Cloudflare Worker through Node.js,
  shims GitHub retrieval to the current workspace, executes the action plan in
  docs/support-agent-eval-cases.json, and scores retrieval plus optional answer
  text. The harness is local-only: it does not call the deployed Worker and it
  uses a dummy bearer token for the in-process Worker.

.PARAMETER CasesPath
  Repo-relative path to the machine-readable support-agent eval case catalog.

.PARAMETER CaseId
  Optional case IDs to run. When omitted, all cases run.

.PARAMETER AnswersPath
  Optional path to a JSON answer map, JSON answer list, or a directory
  containing <case-id>.md / <case-id>.txt transcript answers to score.

.PARAMETER RequireAnswers
  Fail cases when no answer text is available. Without this switch, answer
  scoring is skipped for cases with no supplied or live-generated answer.

.PARAMETER LiveModel
  Generate answers through the OpenAI Responses API when OPENAI_API_KEY is set.
  Retrieval still uses the local in-process Worker.

.PARAMETER Model
  Model name for LiveModel mode. Defaults to OPENAI_MODEL or gpt-4.1-mini.

.PARAMETER OutputRoot
  Directory root for generated JSON and Markdown reports.

.NOTES
  Prerequisites: PowerShell and Node.js 18 or newer. LiveModel also requires
  OPENAI_API_KEY.
  Side effects: creates one timestamped report directory under .tmp by default.
  Safety: does not print API keys, bearer tokens, private config, or deployed
  Worker URLs; does not modify runtime config or call the deployed Worker.
#>
[CmdletBinding()]
param(
  [string]$CasesPath = "docs/support-agent-eval-cases.json",
  [string[]]$CaseId = @(),
  [string]$AnswersPath = "",
  [switch]$RequireAnswers,
  [switch]$LiveModel,
  [string]$Model = "",
  [string]$OutputRoot = ".tmp/support-agent-evals"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$nodeCommand = Get-Command node -ErrorAction SilentlyContinue
if ($null -eq $nodeCommand) {
  throw "Node.js 18 or newer is required to run support-agent evals."
}

$timestamp = (Get-Date -Format "yyyyMMdd-HHmmss-fff") + "-" + ([System.Guid]::NewGuid().ToString("N").Substring(0, 8))
$outputDir = Join-Path (Join-Path $repoRoot $OutputRoot) $timestamp
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$runnerPath = Join-Path $repoRoot "scripts/support-agent-eval-runner.mjs"
$casePath = Join-Path $repoRoot $CasesPath

$args = @(
  $runnerPath,
  "--repo-root", $repoRoot.Path,
  "--cases", $casePath,
  "--output", $outputDir
)

foreach ($id in $CaseId) {
  if (-not [string]::IsNullOrWhiteSpace($id)) {
    $args += @("--case", $id)
  }
}

if (-not [string]::IsNullOrWhiteSpace($AnswersPath)) {
  $resolvedAnswersPath = Resolve-Path -LiteralPath $AnswersPath
  $args += @("--answers", $resolvedAnswersPath.Path)
}

if ($RequireAnswers) {
  $args += "--require-answers"
}

if ($LiveModel) {
  $args += "--live-model"
  $resolvedModel = $Model
  if ([string]::IsNullOrWhiteSpace($resolvedModel)) {
    $resolvedModel = [Environment]::GetEnvironmentVariable("OPENAI_MODEL")
  }
  if ([string]::IsNullOrWhiteSpace($resolvedModel)) {
    $resolvedModel = "gpt-5-nano"
  }
  $args += @("--model", $resolvedModel)
}

& $nodeCommand.Source @args
if ($LASTEXITCODE -ne 0) {
  throw "Support-agent eval harness failed with exit code $LASTEXITCODE. Reports: $outputDir"
}

Write-Host "Support-agent eval reports: $outputDir"
