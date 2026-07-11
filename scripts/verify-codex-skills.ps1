<#
.SYNOPSIS
  Verify the repo-managed Codex skills bundle.
.DESCRIPTION
  Checks required metadata, names, positive triggers, preserved specialist
  methods, referenced assets, and stale user-level installation paths.
#>

[CmdletBinding()]
Param(
  [string[]]$Skills = @(),
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repoRoot = if ($RepoRoot -eq "") {
  (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
} else {
  (Resolve-Path -LiteralPath $RepoRoot).Path
}
$sourceRoot = Join-Path $repoRoot "codex-skills"
if (-not (Test-Path -LiteralPath $sourceRoot -PathType Container)) { throw "Missing source directory: $sourceRoot" }

if ($Skills.Count -eq 0) {
  $Skills = Get-ChildItem -LiteralPath $sourceRoot -Directory | Sort-Object Name | Select-Object -ExpandProperty Name
}

$failures = [System.Collections.Generic.List[string]]::new()
$seenNames = @{}
$textExtensions = @(".json", ".md", ".ps1", ".py", ".sh", ".svg", ".swift", ".txt", ".yaml", ".yml")
$forbiddenPatterns = @(
  @{ Label="CODEX_HOME skills path"; Pattern="CODEX_HOME[\\/]+skills" },
  @{ Label="user .codex skills path"; Pattern="\.codex[\\/]+skills" },
  @{ Label="USERPROFILE Codex skills path"; Pattern="USERPROFILE.*\.codex.*skills" },
  @{ Label="user-home Codex wording"; Pattern="local Codex home" }
)
$specialistInvariants = @{
  "requirements-ambiguity-review" = @("Do not trigger merely", "ambiguity register", "do not choose product policy", "lead agent")
  "scientific-model-oracle" = @("Do not trigger", "source hierarchy", "independent golden vectors", "Do not choose architecture", "lead agent")
  "design-challenger" = @("Do not trigger", "at least two viable approaches", "Do not approve scope", "lead")
  "scope-ledger-adversarial-review" = @("Do not trigger for every", "What edge case would make this scope unsafe or incomplete?", "Requires revised Scope Ledger", "lead agent")
  "test-strategy-adversary" = @("Do not trigger", "Build the Contract-to-Test Matrix", "False-green risk", "Do not", "lead")
  "go-code-quality-review" = @("Do not trigger for every", "Review GoCluster code-quality standards", "Do not run final validation", "lead")
  "go-code-walk" = @("unfamiliar or cross-package", "Walk one material level up and down", "Unknown from inspected code")
  "go-blast-radius-audit" = @("uncertain blast radius", "Map semantic callers and callees", "not concrete runtime proof")
  "decision-memory-audit" = @("Do not trigger merely", "durable decision changes", "Preserve accepted history")
  "workflow-contract-audit" = @("Do not trigger", "Preserve authority routes", "Static checks may establish")
}

function Add-Failure([string]$Message) {
  $failures.Add($Message)
  Write-Host "FAIL $Message"
}

function Get-FrontMatterValue([string]$FrontMatter, [string]$Key) {
  $pattern = "(?m)^$([regex]::Escape($Key)):\s*['`"]?([^'`"\r\n]+)['`"]?\s*$"
  $match = [regex]::Match($FrontMatter, $pattern)
  if (-not $match.Success) { return $null }
  return $match.Groups[1].Value.Trim()
}

foreach ($skill in $Skills) {
  $before = $failures.Count
  $skillDir = Join-Path $sourceRoot $skill
  $skillFile = Join-Path $skillDir "SKILL.md"
  if (-not (Test-Path -LiteralPath $skillFile -PathType Leaf)) {
    Add-Failure "[$skill] missing SKILL.md"
    continue
  }

  $content = Get-Content -LiteralPath $skillFile -Raw
  $front = [regex]::Match($content, "(?s)^---\r?\n(.*?)\r?\n---")
  if (-not $front.Success) {
    Add-Failure "[$skill] missing SKILL.md front matter"
    continue
  }
  $declaredName = Get-FrontMatterValue $front.Groups[1].Value "name"
  $description = Get-FrontMatterValue $front.Groups[1].Value "description"
  if ([string]::IsNullOrWhiteSpace($declaredName)) {
    Add-Failure "[$skill] missing front matter name"
  } elseif ($declaredName -ne $skill) {
    Add-Failure "[$skill] front matter name '$declaredName' does not match directory"
  } elseif ($seenNames.ContainsKey($declaredName)) {
    Add-Failure "[$skill] duplicate skill name '$declaredName'"
  } else {
    $seenNames[$declaredName] = $true
  }
  if ([string]::IsNullOrWhiteSpace($description)) { Add-Failure "[$skill] missing front matter description" }

  if ($specialistInvariants.ContainsKey($skill)) {
    foreach ($required in $specialistInvariants[$skill]) {
      if (-not $content.Contains($required)) { Add-Failure "[$skill] missing preserved trigger or method invariant: $required" }
    }
  }

  $agentFile = Join-Path $skillDir "agents\openai.yaml"
  if (Test-Path -LiteralPath $agentFile -PathType Leaf) {
    foreach ($line in Get-Content -LiteralPath $agentFile) {
      $match = [regex]::Match($line, "^\s*icon_(small|large):\s*['`"]?(.+?)['`"]?\s*$")
      if ($match.Success) {
        $asset = $match.Groups[2].Value.Trim().TrimStart('.', '/')
        if (-not (Test-Path -LiteralPath (Join-Path $skillDir $asset))) { Add-Failure "[$skill] missing agent asset: $asset" }
      }
    }
  }

  foreach ($file in Get-ChildItem -LiteralPath $skillDir -Recurse -File | Where-Object { $textExtensions -contains $_.Extension.ToLowerInvariant() }) {
    $fileText = Get-Content -LiteralPath $file.FullName -Raw
    foreach ($forbidden in $forbiddenPatterns) {
      if ($fileText -match $forbidden.Pattern) { Add-Failure "[$skill] stale $($forbidden.Label): $($file.Name)" }
    }
  }

  if ($failures.Count -eq $before) { Write-Host "PASS [$skill] metadata, trigger, and method invariants checked." }
}

if ($failures.Count -gt 0) {
  Write-Host "FAIL repo skill verification found $($failures.Count) issue(s)."
  exit 1
}
Write-Host "PASS all requested repo skills verified."
exit 0
