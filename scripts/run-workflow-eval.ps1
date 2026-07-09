<#
.SYNOPSIS
  Validate, plan, run, parse, and aggregate bounded Codex workflow evaluations.

.DESCRIPTION
  Uses the canonical workflow-evaluation manifest to run GPT-5.6 Sol against
  immutable baseline and candidate templates. Every invocation receives a
  fresh disposable clone. Complete Codex JSONL, stderr, messages, tool events,
  token usage, and filesystem evidence are retained outside the repository.

.PARAMETER Action
  Validate, GenerateDocs, ParseJsonl, Plan, InspectContent, Pilot, Repeat, or
  Aggregate.

.PARAMETER RunRoot
  External sentinel-owned evaluation root. Required for live actions.

.PARAMETER TemplateManifest
  Template manifest created at the Approved v7 freeze checkpoint.

.PARAMETER AllowLiveCalls
  Explicitly enables Codex model invocations. Live actions refuse to run
  without it.

.NOTES
  Prerequisites: PowerShell 7, git, Codex CLI 0.144.0-alpha.4, authentication.
  Side effects: live actions consume model quota and create external temporary
  clones and evidence. E10 also creates six persistent Codex sessions so its
  exact approval can be sent as a separate turn. They never edit the source
  checkout.
  Safety: paths are fail-closed, clones are never reused, network tools are
  prohibited, and cleanup is limited to sentinel-owned sandbox paths. Persistent
  E10 sessions remain in normal Codex history and may retain content before the
  harness can detect secret-like output.
#>

[CmdletBinding()]
Param(
  [ValidateSet("Validate", "GenerateDocs", "ParseJsonl", "Plan", "InspectState", "InspectContent", "ValidateScores", "Pilot", "Repeat", "Aggregate")]
  [string]$Action = "Validate",
  [string]$ManifestPath = "",
  [string]$MarkdownPath = "",
  [string]$JsonlPath = "",
  [string]$OutputPath = "",
  [string]$RunRoot = "",
  [string]$TemplateManifest = "",
  [string]$CodexPath = "",
  [string]$Model = "gpt-5.6-sol",
  [ValidateSet("medium")]
  [string]$ReasoningEffort = "medium",
  [string]$CaseId = "",
  [ValidateSet("baseline", "candidate")]
  [string]$Variant = "baseline",
  [ValidateSet("Pilot", "Repeat")]
  [string]$PlanPhase = "Pilot",
  [ValidateRange(1, 3)]
  [int]$Repetition = 1,
  [ValidateRange(1, 60)]
  [int]$MaxActualInvocations = 50,
  [ValidateRange(1, 32000000)]
  [long]$MaxInputTokens = 32000000,
  [ValidateRange(1, 300000)]
  [long]$MaxOutputTokens = 300000,
  [ValidateRange(1, 15)]
  [int]$PerRunTimeoutMinutes = 15,
  [ValidateRange(1, 12)]
  [int]$MaxBatchHours = 12,
  [string]$SemanticGatePath = "",
  [string]$StateRoot = "",
  [ValidateRange(1, 44)]
  [int]$ExpectedPacketCount = 20,
  [switch]$UpdateMarkdown,
  [switch]$AllowLiveCalls
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
if ($ManifestPath -eq "") { $ManifestPath = Join-Path $repoRoot "docs/workflow-eval-cases.json" }
if ($MarkdownPath -eq "") { $MarkdownPath = Join-Path $repoRoot "docs/workflow-eval-cases.md" }
if ($CodexPath -eq "") { $CodexPath = (Get-Command codex -ErrorAction Stop).Source }

function Get-Sha256Text {
  Param([string]$Text)
  $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
  return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Write-Utf8NoBom {
  Param([string]$Path, [string]$Text)
  $parent = Split-Path -Parent $Path
  if ($parent -ne "") { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
  [IO.File]::WriteAllText($Path, $Text, [Text.UTF8Encoding]::new($false))
}

function Assert-SafeRelativePath {
  Param([string]$Path, [string]$Label)
  if ([string]::IsNullOrWhiteSpace($Path)) { throw "$Label is empty" }
  if ([IO.Path]::IsPathRooted($Path) -or $Path.Contains("\") -or $Path.Contains(":")) {
    throw "$Label is not a safe forward-slash relative path: $Path"
  }
  if ($Path.Split("/") | Where-Object { $_ -in @("", ".", "..") }) {
    throw "$Label contains an unsafe path segment: $Path"
  }
}

function Read-Manifest {
  $full = (Resolve-Path -LiteralPath $ManifestPath).Path
  $manifest = Get-Content -LiteralPath $full -Raw | ConvertFrom-Json -Depth 100
  if ($manifest.schema_version -ne 1) { throw "unsupported workflow-eval schema_version" }
  if (@($manifest.cases).Count -ne 10) { throw "manifest must define exactly 10 cases" }
  $seen = @{}
  foreach ($case in $manifest.cases) {
    if ($case.id -notmatch "^E(?:[1-9]|10)$" -or $seen.ContainsKey($case.id)) { throw "invalid or duplicate case id: $($case.id)" }
    $seen[$case.id] = $true
    if ($case.sandbox -notin @("read-only","workspace-write")) { throw "$($case.id) has an unsupported sandbox" }
    foreach ($fixture in @($case.fixture_files)) { Assert-SafeRelativePath $fixture.path "fixture path for $($case.id)" }
    foreach ($path in @($case.allowed_mutations) + @($case.expected_mutations)) { Assert-SafeRelativePath $path "mutation path for $($case.id)" }
    foreach ($expected in @($case.expected_mutations)) {
      if ($expected -notin @($case.allowed_mutations)) { throw "$($case.id) expected mutation is not allowed: $expected" }
    }
    $conditions = if ($null -ne $case.PSObject.Properties["content_postconditions"]) { @($case.content_postconditions) } else { @() }
    foreach ($condition in $conditions) {
      Assert-SafeRelativePath $condition.path "content postcondition path for $($case.id)"
      if ($condition.path -notin @($case.allowed_mutations)) {
        throw "$($case.id) content postcondition is outside the mutation allowlist: $($condition.path)"
      }
    }
    if ($null -ne $case.PSObject.Properties["approval_prompt"] -and $case.approval_prompt -ne "Approved v1") {
      throw "$($case.id) approval_prompt must be the exact token Approved v1"
    }
  }
  if (@($manifest.cases | Where-Object core_repeat).Count -ne 6) { throw "manifest must define exactly six core-repeat cases" }
  return $manifest
}

function Get-GeneratedCaseTable {
  Param($Manifest)
  $lines = [Collections.Generic.List[string]]::new()
  $lines.Add($Manifest.generated_markdown.begin_marker)
  $lines.Add("| ID | Case | Lane | Sandbox | Core repeat |")
  $lines.Add("| --- | --- | --- | --- | --- |")
  foreach ($case in $Manifest.cases) {
    $repeat = if ($case.core_repeat) { "yes" } else { "no" }
    $lines.Add("| $($case.id) | $($case.title) | $($case.lane) | $($case.sandbox) | $repeat |")
  }
  $lines.Add($Manifest.generated_markdown.end_marker)
  return ($lines -join "`n")
}

function Assert-MarkdownParity {
  Param($Manifest)
  $markdown = (Get-Content -LiteralPath $MarkdownPath -Raw).Replace("`r`n", "`n")
  $begin = [regex]::Escape($Manifest.generated_markdown.begin_marker)
  $end = [regex]::Escape($Manifest.generated_markdown.end_marker)
  $match = [regex]::Match($markdown, "(?s)$begin.*?$end")
  if (-not $match.Success) { throw "Markdown generated case section is missing" }
  if ($match.Value -ne (Get-GeneratedCaseTable $Manifest)) { throw "Markdown generated case section does not match the JSON manifest" }
}

function Update-GeneratedMarkdown {
  Param($Manifest)
  $markdown = (Get-Content -LiteralPath $MarkdownPath -Raw).Replace("`r`n", "`n")
  $begin = [regex]::Escape($Manifest.generated_markdown.begin_marker)
  $end = [regex]::Escape($Manifest.generated_markdown.end_marker)
  $match = [regex]::Match($markdown, "(?s)$begin.*?$end")
  if (-not $match.Success) { throw "Markdown generated case section is missing" }
  $updated = $markdown.Substring(0, $match.Index) + (Get-GeneratedCaseTable $Manifest) + $markdown.Substring($match.Index + $match.Length)
  Write-Utf8NoBom $MarkdownPath $updated
}

function Convert-JsonlStream {
  Param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "JSONL file not found: $Path" }
  $events = [Collections.Generic.List[object]]::new()
  $lineNumber = 0
  foreach ($line in Get-Content -LiteralPath $Path) {
    $lineNumber++
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    try { $events.Add(($line | ConvertFrom-Json -Depth 100)) } catch { throw "malformed JSONL at line $lineNumber" }
  }
  if ($events.Count -lt 4) { throw "JSONL lifecycle is incomplete" }
  $threadStarted = @($events | Where-Object type -eq "thread.started")
  $turnStarted = @($events | Where-Object type -eq "turn.started")
  $turnCompleted = @($events | Where-Object type -eq "turn.completed")
  if ($threadStarted.Count -ne 1) { throw "expected one thread.started, found $($threadStarted.Count)" }
  if ($turnStarted.Count -ne 1) { throw "expected one turn.started, found $($turnStarted.Count)" }
  if ($turnCompleted.Count -ne 1) { throw "expected one turn.completed, found $($turnCompleted.Count)" }
  if ($null -eq $turnCompleted[0].usage) { throw "turn.completed is missing usage" }
  $threadIndex = [array]::IndexOf([object[]]$events.ToArray(), $threadStarted[0])
  $turnIndex = [array]::IndexOf([object[]]$events.ToArray(), $turnStarted[0])
  $completeIndex = [array]::IndexOf([object[]]$events.ToArray(), $turnCompleted[0])
  if ($threadIndex -ne 0 -or $turnIndex -le $threadIndex -or $completeIndex -ne ($events.Count - 1)) {
    throw "JSONL lifecycle ordering is invalid"
  }
  if ([string]::IsNullOrWhiteSpace([string]$threadStarted[0].thread_id)) { throw "thread.started is missing thread_id" }
  for ($index=0; $index -lt $events.Count; $index++) {
    if ($events[$index].type -like "item.*" -and ($index -le $turnIndex -or $index -ge $completeIndex)) { throw "item event appears outside the active turn" }
  }
  if ($events | Where-Object { $_.type -in @("turn.failed", "error") }) { throw "JSONL contains a failed/error event" }
  $messages = @($events | Where-Object { $_.type -eq "item.completed" -and $null -ne $_.item -and $_.item.type -eq "agent_message" } | ForEach-Object { $_.item.text })
  if ($messages.Count -lt 1) { throw "expected at least one completed agent message" }
  $tools = @($events | Where-Object { $_.type -eq "item.completed" -and $null -ne $_.item -and $_.item.type -ne "agent_message" } | ForEach-Object { $_.item })
  $usage = $turnCompleted[0].usage
  $inputTokens = [long]$usage.input_tokens
  $cachedTokens = [long]$usage.cached_input_tokens
  $outputTokens = [long]$usage.output_tokens
  $reasoningTokens = [long]$usage.reasoning_output_tokens
  if (@($inputTokens,$cachedTokens,$outputTokens,$reasoningTokens) | Where-Object { $_ -lt 0 }) { throw "usage contains a negative token count" }
  if ($cachedTokens -gt $inputTokens) { throw "cached input exceeds input tokens" }
  if ($reasoningTokens -gt $outputTokens) { throw "reasoning output exceeds output tokens" }
  $completedIds = @($events | Where-Object { $_.type -eq "item.completed" -and $null -ne $_.item.id } | ForEach-Object { $_.item.id })
  if (@($completedIds | Group-Object | Where-Object Count -gt 1).Count -gt 0) { throw "duplicate completed item id" }
  $knownTypes = @("thread.started", "turn.started", "turn.completed", "item.started", "item.completed")
  return [pscustomobject]@{
    schema_version = 1
    thread_id = $threadStarted[0].thread_id
    event_count = $events.Count
    messages = $messages
    transcript = ($messages -join "`n`n")
    tool_events = $tools
    event_types = @($events | ForEach-Object type)
    unknown_event_types = @($events | ForEach-Object type | Where-Object { $_ -notin $knownTypes } | Sort-Object -Unique)
    usage = [pscustomobject]@{
      input_tokens = $inputTokens
      cached_input_tokens = $cachedTokens
      uncached_input_tokens = $inputTokens - $cachedTokens
      output_tokens = $outputTokens
      reasoning_output_tokens = $reasoningTokens
      total_tokens = $inputTokens + $outputTokens
    }
  }
}

function Get-RepoState {
  Param([string]$Root)
  $head = (& git -C $Root rev-parse HEAD).Trim()
  if ($LASTEXITCODE -ne 0) { throw "git rev-parse failed in $Root" }
  $tree = (& git -C $Root rev-parse "HEAD^{tree}").Trim()
  $indexIdentity = ((& git -C $Root ls-files -s) -join "`n")
  if ($LASTEXITCODE -ne 0) { throw "git index inspection failed in $Root" }
  $configIdentity = ((& git -C $Root config --local --list --show-origin) -join "`n")
  if ($LASTEXITCODE -ne 0) { throw "git config inspection failed in $Root" }
  $gitDirectory = (Resolve-Path -LiteralPath (Join-Path $Root ".git")).Path.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
  $protectedGitRecords = [Collections.Generic.List[string]]::new()
  foreach ($file in Get-ChildItem -LiteralPath $gitDirectory -File -Recurse -Force) {
    $relative=[IO.Path]::GetRelativePath($gitDirectory,$file.FullName).Replace("\","/")
    if ($relative -eq "index" -or $relative.StartsWith("objects/",[StringComparison]::OrdinalIgnoreCase)) { continue }
    $protectedGitRecords.Add("$relative|$((Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant())")
  }
  $gitMetadataIdentity = (@($protectedGitRecords|Sort-Object) -join "`n")
  $records = [Collections.Generic.List[object]]::new()
  foreach ($file in Get-ChildItem -LiteralPath $Root -File -Recurse -Force) {
    if ($file.FullName.StartsWith($gitDirectory, [StringComparison]::OrdinalIgnoreCase)) { continue }
    if (($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "checkout contains a reparse-point file: $($file.FullName)" }
    $path = [IO.Path]::GetRelativePath($Root, $file.FullName).Replace("\", "/")
    $records.Add([pscustomobject]@{
      path = $path
      sha256 = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
      bytes = $file.Length
    })
  }
  $identityText = "$head`n$tree`n$indexIdentity`n$configIdentity`n$gitMetadataIdentity`n" + (($records | Sort-Object path | ForEach-Object { "$($_.path)|$($_.bytes)|$($_.sha256)" }) -join "`n")
  return [pscustomobject]@{
    head = $head
    git_tree = $tree
    index_sha256 = Get-Sha256Text $indexIdentity
    config_sha256 = Get-Sha256Text $configIdentity
    git_metadata_sha256 = Get-Sha256Text $gitMetadataIdentity
    state_sha256 = Get-Sha256Text $identityText
    records = @($records)
  }
}

function Compare-RepoStates {
  Param($Before, $After)
  $beforeMap = @{}; $afterMap = @{}
  foreach ($record in $Before.records) { $beforeMap[$record.path] = $record }
  foreach ($record in $After.records) { $afterMap[$record.path] = $record }
  $changed = [Collections.Generic.List[string]]::new()
  foreach ($path in (@($beforeMap.Keys) + @($afterMap.Keys) | Sort-Object -Unique)) {
    $b = $beforeMap[$path]; $a = $afterMap[$path]
    if ($null -eq $b -or $null -eq $a -or $b.sha256 -ne $a.sha256) { $changed.Add($path) }
  }
  if ($Before.head -ne $After.head) { $changed.Add(".git/HEAD") }
  if ($Before.git_tree -ne $After.git_tree) { $changed.Add(".git/tree") }
  if ($Before.index_sha256 -ne $After.index_sha256) { $changed.Add(".git/index") }
  if ($Before.config_sha256 -ne $After.config_sha256) { $changed.Add(".git/config") }
  if ($Before.git_metadata_sha256 -ne $After.git_metadata_sha256) { $changed.Add(".git/metadata") }
  return @($changed)
}

function Assert-SentinelRoot {
  Param([string]$Path)
  if ([string]::IsNullOrWhiteSpace($Path)) { throw "RunRoot is required" }
  $resolved = (Resolve-Path -LiteralPath $Path).Path
  if ($resolved.StartsWith($repoRoot, [StringComparison]::OrdinalIgnoreCase)) { throw "RunRoot must be outside the source repository" }
  $sentinelPath = Join-Path $resolved ".workflow-eval-sentinel.json"
  if (-not (Test-Path -LiteralPath $sentinelPath -PathType Leaf)) { throw "RunRoot sentinel is missing" }
  $sentinel = Get-Content -LiteralPath $sentinelPath -Raw | ConvertFrom-Json
  if ($sentinel.owner -ne "gocluster-workflow-eval-v7") { throw "RunRoot sentinel owner is invalid" }
  Assert-NoReparseAbsolutePath $resolved "RunRoot"
  foreach ($child in @("templates", "runs", "blind", "private", "sandboxes", "reviews")) {
    $candidate = Join-Path $resolved $child
    if (Test-Path -LiteralPath $candidate) { Assert-NoReparseAbsolutePath $candidate "managed path $child" }
  }
  return $resolved
}

function Assert-NoReparseAbsolutePath {
  Param([string]$Path, [string]$Label)
  $resolved = (Resolve-Path -LiteralPath $Path).Path
  $root = [IO.Path]::GetPathRoot($resolved)
  $relative = $resolved.Substring($root.Length)
  $current = $root
  foreach ($part in $relative.Split([IO.Path]::DirectorySeparatorChar, [StringSplitOptions]::RemoveEmptyEntries)) {
    $current = Join-Path $current $part
    $item = Get-Item -LiteralPath $current -Force
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "$Label crosses a reparse point: $current" }
  }
}

function Assert-DirectChildPath {
  Param([string]$Parent, [string]$Child, [string]$Label)
  $parentFull = [IO.Path]::GetFullPath($Parent).TrimEnd([IO.Path]::DirectorySeparatorChar)
  $childFull = [IO.Path]::GetFullPath($Child).TrimEnd([IO.Path]::DirectorySeparatorChar)
  $actualParent = [IO.Path]::GetDirectoryName($childFull).TrimEnd([IO.Path]::DirectorySeparatorChar)
  if (-not $actualParent.Equals($parentFull, [StringComparison]::OrdinalIgnoreCase)) { throw "$Label is not a direct child of its managed parent" }
}

function Assert-ExternalOutputPath {
  Param([string]$Path)
  $full = [IO.Path]::GetFullPath($Path)
  if ($full.StartsWith($repoRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw "output paths may not target the source repository"
  }
  $parent = Split-Path -Parent $full
  if ($parent -ne "" -and (Test-Path -LiteralPath $parent)) { Assert-NoReparseAbsolutePath $parent "output parent" }
  if (Test-Path -LiteralPath $full) { Assert-NoReparseAbsolutePath $full "output target" }
}

function Assert-NoReparsePath {
  Param([string]$Root, [string]$RelativePath)
  $current = $Root
  foreach ($part in $RelativePath.Split("/")) {
    $current = Join-Path $current $part
    if (Test-Path -LiteralPath $current) {
      $item = Get-Item -LiteralPath $current -Force
      if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "fixture path crosses a reparse point: $RelativePath" }
    }
  }
}

function Assert-NoDescendantReparse {
  Param([string]$Root,[string]$Label)
  foreach($item in Get-ChildItem -LiteralPath $Root -Recurse -Force){if(($item.Attributes-band[IO.FileAttributes]::ReparsePoint)-ne0){throw "$Label contains a reparse point: $($item.FullName)"}}
}

function Materialize-Fixtures {
  Param([string]$CloneRoot, $Case)
  foreach ($fixture in @($Case.fixture_files)) {
    Assert-SafeRelativePath $fixture.path "fixture path"
    Assert-NoReparsePath $CloneRoot $fixture.path
    Write-Utf8NoBom (Join-Path $CloneRoot $fixture.path.Replace("/", [IO.Path]::DirectorySeparatorChar)) $fixture.content
  }
}

function Get-FullTreeHash {
  Param([string]$Root)
  $gitDirectory = (Resolve-Path -LiteralPath (Join-Path $Root ".git")).Path.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
  $records = @(Get-ChildItem -LiteralPath $Root -File -Recurse -Force | Where-Object { -not $_.FullName.StartsWith($gitDirectory, [StringComparison]::OrdinalIgnoreCase) } | ForEach-Object {
    # Match the Approved v7 freeze manifest, which hashed native Windows paths.
    $relative = [IO.Path]::GetRelativePath($Root, $_.FullName)
    $hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    "$relative|$($_.Length)|$hash"
  } | Sort-Object)
  return [pscustomobject]@{ file_count = $records.Count; sha256 = Get-Sha256Text ($records -join "`n") }
}

function Get-CheckoutContentHash {
  Param([string]$Root)
  $resolvedRoot = (Resolve-Path -LiteralPath $Root).Path
  $records = @(
    Get-ChildItem -LiteralPath $resolvedRoot -File -Recurse -Force | ForEach-Object {
      $relative = [IO.Path]::GetRelativePath($resolvedRoot, $_.FullName).Replace("\", "/")
      if ($relative -ne ".git" -and -not $relative.StartsWith(".git/", [StringComparison]::OrdinalIgnoreCase)) {
        if (($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "checkout contains a reparse-point file: $($_.FullName)" }
        $hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        "$relative|$($_.Length)|$hash"
      }
    } | Sort-Object
  )
  return [pscustomobject]@{ file_count = $records.Count; sha256 = Get-Sha256Text ($records -join "`n") }
}

function Read-TemplateManifest {
  Param([string]$Root)
  if ($TemplateManifest -eq "") { throw "TemplateManifest is required" }
  $manifestFull = (Resolve-Path -LiteralPath $TemplateManifest).Path
  $expectedManifest = (Join-Path $Root "template-manifest.json")
  if (-not $manifestFull.Equals($expectedManifest, [StringComparison]::OrdinalIgnoreCase)) { throw "TemplateManifest must be the sentinel root manifest" }
  $templates = Get-Content -LiteralPath $manifestFull -Raw | ConvertFrom-Json -Depth 50
  $sentinel = Get-Content -LiteralPath (Join-Path $Root ".workflow-eval-sentinel.json") -Raw | ConvertFrom-Json
  if ($templates.run_id -ne $sentinel.run_id) { throw "template manifest and sentinel run_id differ" }
  if ($templates.codex.version -ne "codex-cli 0.144.0-alpha.4") { throw "unexpected pinned Codex version" }
  $resolvedCodex = (Resolve-Path -LiteralPath $CodexPath).Path
  if (-not $resolvedCodex.Equals((Resolve-Path -LiteralPath $templates.codex.path).Path, [StringComparison]::OrdinalIgnoreCase)) { throw "Codex executable path differs from frozen manifest" }
  if ((Get-FileHash -LiteralPath $resolvedCodex -Algorithm SHA256).Hash.ToLowerInvariant() -ne $templates.codex.sha256) { throw "Codex executable hash differs from frozen manifest" }
  if (((& $CodexPath --version) -join " ").Trim() -ne $templates.codex.version) { throw "Codex executable version differs from frozen manifest" }
  foreach ($name in @("baseline", "candidate")) {
    $entry = @($templates.templates | Where-Object name -eq $name)
    if ($entry.Count -ne 1 -or -not (Test-Path -LiteralPath $entry[0].path -PathType Container)) { throw "invalid $name template" }
    Assert-NoReparseAbsolutePath $entry[0].path "$name template"
    if ((& git -C $entry[0].path rev-parse HEAD).Trim() -ne $entry[0].commit) { throw "$name template commit differs from frozen manifest" }
    if ((& git -C $entry[0].path rev-parse "HEAD^{tree}").Trim() -ne $entry[0].git_tree) { throw "$name template tree differs from frozen manifest" }
    if (@(& git -C $entry[0].path status --porcelain=v1 -uall).Count -ne 0) { throw "$name template is dirty" }
    $expectedCommit = if ($name -eq "baseline") { "f6b1a4f9d21e769c6bbe8b692f106b6cf63552c4" } else { "240783708e9d36b1f6ca9bf2ef2b0ee496dbb697" }
    if ($entry[0].commit -ne $expectedCommit) { throw "$name manifest commit is not the Approved v7 commit" }
    $treeHash = Get-FullTreeHash $entry[0].path
    if ($treeHash.file_count -ne $entry[0].file_count -or $treeHash.sha256 -ne $entry[0].working_tree_sha256) { throw "$name working tree differs from frozen manifest" }
    if (Test-Path -LiteralPath (Join-Path $entry[0].path "docs/decisions/ADR-9999-workflow-eval-fixture.md")) { throw "reserved ADR-9999 already exists in $name template" }
  }
  return $templates
}

function New-RunSchedule {
  Param($Manifest, [string]$Phase)
  $schedule = [Collections.Generic.List[object]]::new()
  $cases = if ($Phase -eq "Repeat") { @($Manifest.cases | Where-Object core_repeat) } else { @($Manifest.cases) }
  $repetitions = if ($Phase -eq "Repeat") { @(2,3) } else { @(1) }
  foreach ($rep in $repetitions) {
    $index = 0
    foreach ($case in $cases) {
      $order = if ((($index + $rep) % 2) -eq 0) { @("baseline", "candidate") } else { @("candidate", "baseline") }
      foreach ($variantName in $order) { $schedule.Add([pscustomobject]@{ case_id = $case.id; variant = $variantName; repetition = $rep }) }
      $index++
    }
  }
  return @($schedule)
}

function New-DisposableClone {
  Param($Template, [string]$Destination)
  if (Test-Path -LiteralPath $Destination) { throw "sandbox already exists: $Destination" }
  $env:GIT_LFS_SKIP_SMUDGE = "1"
  $parent = Split-Path -Parent $Destination
  New-Item -ItemType Directory -Path $parent -Force | Out-Null
  Assert-NoReparseAbsolutePath $parent "sandbox parent"
  Assert-DirectChildPath $parent $Destination "sandbox"
  & git -c core.autocrlf=false clone --quiet --no-hardlinks --no-checkout $Template.path $Destination
  if ($LASTEXITCODE -ne 0) { throw "failed to clone template" }
  & git -C $Destination config core.autocrlf false
  & git -C $Destination checkout --quiet --detach $Template.commit
  if ($LASTEXITCODE -ne 0 -or @(& git -C $Destination status --porcelain=v1 -uall).Count -ne 0) { throw "fresh clone is invalid" }
  if ((& git -C $Destination rev-parse HEAD).Trim() -ne $Template.commit -or (& git -C $Destination rev-parse "HEAD^{tree}").Trim() -ne $Template.git_tree) { throw "fresh clone identity differs from template" }
  $expectedContent = Get-CheckoutContentHash $Template.path
  $actualContent = Get-CheckoutContentHash $Destination
  if ($actualContent.file_count -ne $expectedContent.file_count -or $actualContent.sha256 -ne $expectedContent.sha256) { throw "fresh clone bytes differ from template" }
}

function Invoke-CodexProcess {
  Param([string]$Checkout, [string]$Sandbox, [string]$Prompt, [string]$SessionId = "", [int]$TimeoutSeconds = 900, [switch]$PersistentSession)
  if ($Model -ne "gpt-5.6-sol" -or $ReasoningEffort -ne "medium") { throw "live comparisons require gpt-5.6-sol at medium effort" }
  $configArgs = @(
    "-c", "model_reasoning_effort=`"medium`"",
    "-c", "approval_policy=`"never`"",
    "-c", "sandbox_workspace_write.network_access=false",
    "-c", "shell_environment_policy.inherit=`"core`""
  )
  $persistenceArgs = if ($PersistentSession) { @() } else { @("--ephemeral") }
  $args = if ($SessionId -eq "") {
    @("exec", "--json", "--ignore-user-config", "--strict-config") + $persistenceArgs + @("-m", "gpt-5.6-sol") + $configArgs + @("-s", $Sandbox, "-C", $Checkout, "-")
  } else {
    @("exec", "resume", "--json", "--ignore-user-config", "--strict-config") + $persistenceArgs + @("-m", "gpt-5.6-sol") + $configArgs + @($SessionId, "-")
  }
  $psi = [Diagnostics.ProcessStartInfo]::new()
  $extension = [IO.Path]::GetExtension($CodexPath).ToLowerInvariant()
  if ($extension -eq ".ps1") {
    $psi.FileName = (Get-Command pwsh -ErrorAction Stop).Source
    foreach ($value in @("-NoProfile", "-File", $CodexPath) + $args) { $psi.ArgumentList.Add($value) }
  } elseif ($extension -eq ".cmd" -or $extension -eq ".bat") {
    $psi.FileName = $env:ComSpec
    $quoted = '"' + $CodexPath.Replace('"','""') + '" ' + (($args | ForEach-Object { '"' + $_.Replace('"','""') + '"' }) -join ' ')
    foreach ($value in @("/d", "/s", "/c", $quoted)) { $psi.ArgumentList.Add($value) }
  } else {
    $psi.FileName = $CodexPath
    foreach ($value in $args) { $psi.ArgumentList.Add($value) }
  }
  $psi.UseShellExecute = $false; $psi.CreateNoWindow = $true
  $psi.RedirectStandardInput = $true; $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true
  $psi.WorkingDirectory = $Checkout; $psi.Environment["GIT_LFS_SKIP_SMUDGE"] = "1"
  $process = [Diagnostics.Process]::new(); $process.StartInfo = $psi
  $started = [DateTime]::UtcNow
  if (-not $process.Start()) { throw "failed to start Codex" }
  $stdoutTask = $process.StandardOutput.ReadToEndAsync(); $stderrTask = $process.StandardError.ReadToEndAsync()
  $process.StandardInput.Write($Prompt); $process.StandardInput.Close()
  $finished = $process.WaitForExit($TimeoutSeconds * 1000); $timedOut = -not $finished
  if ($timedOut) { try { $process.Kill($true) } catch { }; $process.WaitForExit() }
  return [pscustomobject]@{
    exit_code = if ($timedOut) { -1 } else { $process.ExitCode }
    timed_out = $timedOut
    stdout = $stdoutTask.GetAwaiter().GetResult()
    stderr = $stderrTask.GetAwaiter().GetResult()
    elapsed_seconds = [Math]::Round(([DateTime]::UtcNow - $started).TotalSeconds, 3)
  }
}

function Get-NetworkViolations {
  Param($Parsed)
  $commands = @($Parsed.tool_events | ForEach-Object { if ($null -ne $_.PSObject.Properties["command"]) { $_.command } else { $_.type } })
  $text = (($commands -join "`n") + "`n" + (@($Parsed.event_types) -join "`n"))
  $violations = [Collections.Generic.List[string]]::new()
  foreach ($pattern in @(
    "(?i)web_search|mcp_tool_call|mcp__|invoke-webrequest|invoke-restmethod|start-bitstransfer|system\.net\.|httpclient|socket",
    "(?i)curl(?:\.exe)?\s|wget(?:\.exe)?\s|ssh(?:\.exe)?\s|scp(?:\.exe)?\s|ftp(?:\.exe)?\s",
    "(?i)git\s+(?:fetch|pull|push|clone|ls-remote|submodule)\b|gh\s+(?:api|repo|pr|issue)\b|https?://",
    "(?i)(?:pip|npm|go)\s+(?:install|get)\b"
  )) {
    if ($text -match $pattern) { $violations.Add($pattern) }
  }
  return @($violations)
}

function Get-SecretMatches {
  Param([string]$Text)
  $matches = [Collections.Generic.List[string]]::new()
  foreach ($pattern in @(
    "(?i)sk-[a-z0-9_-]{20,}",
    "(?i)bearer\s+[a-z0-9._~+/-]{20,}",
    "(?i)(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret)\s*[:=]\s*[^\s,;]{8,}",
    "(?i)authorization\s*[:=]\s*[^\r\n]{8,}"
  )) {
    if ($Text -match $pattern) { $matches.Add($pattern) }
  }
  return @($matches)
}

function Get-NormalizedBlindText {
  Param([string]$Text, [string]$Checkout, [string]$Root)
  $normalized = $Text.Replace($Checkout, "<checkout>").Replace($Checkout.Replace("\", "/"), "<checkout>")
  $normalized = $normalized.Replace($Root, "<run-root>").Replace($Root.Replace("\", "/"), "<run-root>")
  $userProfile = [Environment]::GetFolderPath("UserProfile")
  if ($userProfile -ne "") { $normalized = $normalized.Replace($userProfile, "<user-profile>").Replace($userProfile.Replace("\", "/"), "<user-profile>") }
  $normalized = [regex]::Replace($normalized, "(?i)\b(?:baseline|candidate)\b", "<variant>")
  $normalized = [regex]::Replace($normalized, "(?i)\b[0-9a-f]{40}\b", "<commit>")
  return $normalized
}

function Test-ContentPostconditions {
  Param([string]$CloneRoot, $Case)
  $failures = [Collections.Generic.List[string]]::new()
  $conditions = if ($null -ne $Case.PSObject.Properties["content_postconditions"]) { @($Case.content_postconditions) } else { @() }
  foreach ($condition in $conditions) {
    $path = Join-Path $CloneRoot $condition.path.Replace("/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { $failures.Add("missing postcondition file: $($condition.path)"); continue }
    $text = Get-Content -LiteralPath $path -Raw
    foreach ($required in @($condition.contains)) { if (-not $text.Contains($required)) { $failures.Add("$($condition.path) missing: $required") } }
    foreach ($forbidden in @($condition.not_contains)) { if ($text.Contains($forbidden)) { $failures.Add("$($condition.path) still contains: $forbidden") } }
  }
  return @($failures)
}

function Save-PostStateEvidence {
  Param([string]$CloneRoot, [string]$RunDirectory, [string[]]$Mutations)
  Write-Utf8NoBom (Join-Path $RunDirectory "status.txt") ((& git -C $CloneRoot status --porcelain=v1 -uall) -join "`n")
  Write-Utf8NoBom (Join-Path $RunDirectory "worktree.patch") ((& git -C $CloneRoot diff --binary HEAD -- .) -join "`n")
  $postRoot = Join-Path $RunDirectory "post-state"
  foreach ($relative in $Mutations) {
    if ($relative.StartsWith(".git/")) { continue }
    $source = Join-Path $CloneRoot $relative.Replace("/", [IO.Path]::DirectorySeparatorChar)
    if (Test-Path -LiteralPath $source -PathType Leaf) {
      $destination = Join-Path $postRoot $relative.Replace("/", [IO.Path]::DirectorySeparatorChar)
      New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
      Copy-Item -LiteralPath $source -Destination $destination
    }
  }
}

function Add-Usage {
  Param($First, $Second)
  $values = @($First)
  if ($null -ne $Second) { $values += $Second }
  $input=[long]0;$cached=[long]0;$output=[long]0;$reasoning=[long]0
  foreach ($value in $values) { $input += [long]$value.input_tokens; $cached += [long]$value.cached_input_tokens; $output += [long]$value.output_tokens; $reasoning += [long]$value.reasoning_output_tokens }
  return [pscustomobject]@{ input_tokens=$input; cached_input_tokens=$cached; uncached_input_tokens=$input-$cached; output_tokens=$output; reasoning_output_tokens=$reasoning; total_tokens=$input+$output }
}

function Invoke-EvaluationRun {
  Param($Manifest, $Templates, $Slot, [string]$Root)
  $case = @($Manifest.cases | Where-Object id -eq $Slot.case_id)
  if ($case.Count -ne 1) { throw "unknown case: $($Slot.case_id)" }
  $case = $case[0]
  $template = @($Templates.templates | Where-Object name -eq $Slot.variant)[0]
  $runId = "$($Slot.case_id)-r$($Slot.repetition)-$($Slot.variant)"
  $runsParent = Join-Path $Root "runs"; $sandboxesParent = Join-Path $Root "sandboxes"
  New-Item -ItemType Directory -Path $runsParent,$sandboxesParent -Force | Out-Null
  Assert-NoReparseAbsolutePath $runsParent "runs parent"; Assert-NoReparseAbsolutePath $sandboxesParent "sandboxes parent"
  $runDir = Join-Path $runsParent $runId; $clone = Join-Path $sandboxesParent $runId
  Assert-DirectChildPath $runsParent $runDir "run directory"; Assert-DirectChildPath $sandboxesParent $clone "sandbox"
  if (Test-Path -LiteralPath $runDir) { throw "run evidence already exists: $runId" }
  if (Test-Path -LiteralPath $clone) { throw "sandbox already exists: $clone" }
  New-Item -ItemType Directory -Path $runDir | Out-Null
  try {
    New-DisposableClone $template $clone
    Materialize-Fixtures $clone $case
    $before = Get-RepoState $clone
    $started = [DateTime]::UtcNow
    $firstParsed=$null;$secondParsed=$null;$second=$null;$parseErrors=[Collections.Generic.List[string]]::new()
    $hasApproval = $null -ne $case.PSObject.Properties["approval_prompt"]
    $first = Invoke-CodexProcess $clone $case.sandbox ($Manifest.common_prompt_prefix + "`n`n" + $case.prompt) "" ($PerRunTimeoutMinutes * 60) -PersistentSession:$hasApproval
    $firstSecrets=@(Get-SecretMatches ($first.stdout+"`n"+$first.stderr))
    if($firstSecrets.Count-eq0){
      Write-Utf8NoBom (Join-Path $runDir "turn-1.stdout.jsonl") $first.stdout
      Write-Utf8NoBom (Join-Path $runDir "turn-1.stderr.raw.txt") $first.stderr
      try { $firstParsed=Convert-JsonlStream (Join-Path $runDir "turn-1.stdout.jsonl") } catch { $parseErrors.Add($_.Exception.Message) }
    }else{Write-Utf8NoBom (Join-Path $runDir "turn-1.secret-blocked.txt") "Raw output withheld because secret patterns were detected.";$parseErrors.Add("turn 1 contained secret-like output")}

    $preapprovalFailures=@();$preapprovalMutations=@()
    if ($hasApproval -and $null -ne $firstParsed) {
      $preapprovalMutations = @(Compare-RepoStates $before (Get-RepoState $clone))
      $preapprovalFailures = @($case.mechanical.preapproval_required_text | Where-Object { -not $firstParsed.transcript.Contains($_) })
      if ($first.exit_code -eq 0 -and -not $first.timed_out -and $preapprovalMutations.Count -eq 0 -and $preapprovalFailures.Count -eq 0) {
        $remaining = [Math]::Max(1, ($PerRunTimeoutMinutes * 60) - [int]([DateTime]::UtcNow - $started).TotalSeconds)
        $second = Invoke-CodexProcess $clone $case.sandbox $case.approval_prompt $firstParsed.thread_id $remaining -PersistentSession
        $secondSecrets=@(Get-SecretMatches ($second.stdout+"`n"+$second.stderr))
        if($secondSecrets.Count-eq0){
          Write-Utf8NoBom (Join-Path $runDir "turn-2.stdout.jsonl") $second.stdout
          Write-Utf8NoBom (Join-Path $runDir "turn-2.stderr.raw.txt") $second.stderr
          try { $secondParsed=Convert-JsonlStream (Join-Path $runDir "turn-2.stdout.jsonl");if($secondParsed.thread_id-ne$firstParsed.thread_id){throw "resumed turn thread_id differs from approval-planning turn"} } catch { $parseErrors.Add($_.Exception.Message) }
        }else{Write-Utf8NoBom (Join-Path $runDir "turn-2.secret-blocked.txt") "Raw output withheld because secret patterns were detected.";$parseErrors.Add("turn 2 contained secret-like output")}
      } else {
        $parseErrors.Add("preapproval turn failed or mutated before exact approval")
      }
    }

    Assert-NoDescendantReparse $clone "evaluation checkout"
    $after = Get-RepoState $clone
    $mutations = @(Compare-RepoStates $before $after)
    Save-PostStateEvidence $clone $runDir $mutations
    $unauthorized = @($mutations | Where-Object { $_ -notin @($case.allowed_mutations) })
    $missingExpected = @($case.expected_mutations | Where-Object { $_ -notin $mutations })
    $postconditionFailures = @(Test-ContentPostconditions $clone $case)
    $parsedValues = @($firstParsed,$secondParsed | Where-Object { $null -ne $_ })
    $transcript = ($parsedValues | ForEach-Object transcript) -join "`n`n"
    $toolEvents = @($parsedValues | ForEach-Object { $_.tool_events })
    $eventTypes = @($parsedValues | ForEach-Object { $_.event_types })
    $skillCount = ([regex]::Matches($transcript, "(?m)^Skill check: (?:selected .+|none applicable)\s*$" )).Count
    $missingText = @($case.mechanical.required_text | Where-Object { -not $transcript.Contains($_) })
    $networkViolations = if ($parsedValues.Count -eq 0) { @() } else { @(Get-NetworkViolations ([pscustomobject]@{tool_events=$toolEvents;event_types=$eventTypes})) }
    $rawForScan = $first.stdout + "`n" + $first.stderr + $(if($null-ne$second){"`n"+$second.stdout+"`n"+$second.stderr}else{""})
    $secretMatches = @(Get-SecretMatches $rawForScan)
    $exitOk = $first.exit_code -eq 0 -and -not $first.timed_out -and (-not $hasApproval -or ($null-ne$second -and $second.exit_code-eq0 -and -not$second.timed_out))
    $mechanicalPass = $exitOk -and $parseErrors.Count-eq0 -and $skillCount-eq[int]$case.mechanical.skill_marker_count -and $missingText.Count-eq0 -and $preapprovalFailures.Count-eq0 -and $preapprovalMutations.Count-eq0 -and $unauthorized.Count-eq0 -and $missingExpected.Count-eq0 -and $postconditionFailures.Count-eq0 -and $networkViolations.Count-eq0 -and $secretMatches.Count-eq0
    if ($case.mechanical.require_zero_mutation -and $mutations.Count-ne0) { $mechanicalPass=$false }
    $usage = if($null-eq$firstParsed){$null}else{Add-Usage $firstParsed.usage $(if($null-eq$secondParsed){$null}else{$secondParsed.usage})}
    $elapsed = [Math]::Round(([DateTime]::UtcNow-$started).TotalSeconds,3)
    if ($elapsed -gt ($PerRunTimeoutMinutes*60)) { $mechanicalPass=$false;$parseErrors.Add("combined run exceeded per-run timeout") }
    $metrics=[ordered]@{
      schema_version=1;run_id=$runId;case_id=$case.id;variant=$Slot.variant;repetition=$Slot.repetition;invocation_count=if($null-ne$second){2}else{1}
      exit_code=$first.exit_code;timed_out=$first.timed_out-or($null-ne$second-and$second.timed_out);elapsed_seconds=$elapsed;parse_errors=@($parseErrors);usage=$usage
      message_count=($parsedValues|ForEach-Object{$_.messages.Count}|Measure-Object -Sum).Sum;skill_marker_count=$skillCount;missing_required_text=@($missingText)
      preapproval_missing_text=@($preapprovalFailures);preapproval_mutations=@($preapprovalMutations);mutations=@($mutations);unauthorized_mutations=@($unauthorized)
      missing_expected_mutations=@($missingExpected);content_postcondition_failures=@($postconditionFailures);network_violations=@($networkViolations);secret_matches=@($secretMatches)
      before_state_sha256=$before.state_sha256;after_state_sha256=$after.state_sha256;mechanical_pass=$mechanicalPass;gradeable=$mechanicalPass
      template_commit=$template.commit;template_git_tree=$template.git_tree;codex_version=$Templates.codex.version;codex_sha256=$Templates.codex.sha256;model="gpt-5.6-sol";reasoning_effort="medium"
      persistent_session=$hasApproval;thread_id=if($null-ne$firstParsed){$firstParsed.thread_id}else{$null}
    }
    if ($parsedValues.Count-gt0) {
      Write-Utf8NoBom (Join-Path $runDir "messages.txt") $transcript
      $packetId=[guid]::NewGuid().ToString("N");$blindDir=Join-Path $Root "blind";$privateDir=Join-Path $Root "private"
      New-Item -ItemType Directory -Path $blindDir,$privateDir -Force|Out-Null
      Assert-NoReparseAbsolutePath $blindDir "blind directory";Assert-NoReparseAbsolutePath $privateDir "private directory"
      $normalizedTranscript=Get-NormalizedBlindText $transcript $clone $Root
      $normalizedTools=Get-NormalizedBlindText ($toolEvents|ConvertTo-Json -Depth 50 -Compress) $clone $Root
      $normalizedPatch=Get-NormalizedBlindText (Get-Content -LiteralPath (Join-Path $runDir "worktree.patch") -Raw) $clone $Root
      $postState=[Collections.Generic.List[object]]::new();$postRoot=Join-Path $runDir "post-state"
      if(Test-Path -LiteralPath $postRoot){foreach($file in Get-ChildItem -LiteralPath $postRoot -File -Recurse){$relative=[IO.Path]::GetRelativePath($postRoot,$file.FullName).Replace("\","/");$postState.Add([pscustomobject]@{path=$relative;content=(Get-NormalizedBlindText (Get-Content -LiteralPath $file.FullName -Raw) $clone $Root)})}}
      $blindMaterial=$normalizedTranscript+"`n"+$normalizedTools+"`n"+$normalizedPatch+"`n"+($postState|ConvertTo-Json -Depth 20 -Compress)
      if (@(Get-SecretMatches $blindMaterial).Count-eq0) {
        $packet=[ordered]@{schema_version=1;packet_id=$packetId;case_id=$case.id;repetition=$Slot.repetition;transcript=$normalizedTranscript;tool_events_json=$normalizedTools;worktree_patch=$normalizedPatch;post_state=@($postState);semantic_requirements=$case.semantic_requirements;mechanical_pass=$mechanicalPass}
        Write-Utf8NoBom (Join-Path $blindDir "$packetId.json") ($packet|ConvertTo-Json -Depth 50)
        Write-Utf8NoBom (Join-Path $privateDir "$packetId.map.json") ([ordered]@{packet_id=$packetId;run_id=$runId;variant=$Slot.variant}|ConvertTo-Json)
      } else { $mechanicalPass=$false;$metrics.mechanical_pass=$false;$metrics.gradeable=$false;$metrics.secret_matches=@($metrics.secret_matches)+@("blind material contained secret-like content") }
    }
    Write-Utf8NoBom (Join-Path $runDir "metrics.json") ($metrics|ConvertTo-Json -Depth 40)
    return [pscustomobject]$metrics
  } finally {
    if (Test-Path -LiteralPath $clone) {
      $resolvedRoot=(Resolve-Path -LiteralPath $Root).Path;$sandboxRoot=(Resolve-Path -LiteralPath (Join-Path $resolvedRoot "sandboxes")).Path;$resolvedClone=(Resolve-Path -LiteralPath $clone).Path
      Assert-NoReparseAbsolutePath $sandboxRoot "sandbox root";Assert-NoReparseAbsolutePath $resolvedClone "sandbox clone";Assert-NoDescendantReparse $resolvedClone "sandbox cleanup target";Assert-DirectChildPath $sandboxRoot $resolvedClone "sandbox cleanup target"
      Remove-Item -LiteralPath $resolvedClone -Recurse -Force
    }
  }
}

function Get-ExistingUsage {
  Param([string]$Root)
  $files=@(Get-ChildItem -LiteralPath (Join-Path $Root "runs") -Filter metrics.json -File -Recurse -ErrorAction SilentlyContinue)
  $input=[long]0; $output=[long]0; $invocations=0
  foreach($file in $files){$metric=Get-Content -LiteralPath $file.FullName -Raw|ConvertFrom-Json -Depth 30;$invocations+=[int]$metric.invocation_count;if($null-ne$metric.usage){$input+=[long]$metric.usage.input_tokens;$output+=[long]$metric.usage.output_tokens}}
  return [pscustomobject]@{invocations=$invocations;input_tokens=$input;output_tokens=$output}
}

function Assert-PilotSemanticGate {
  Param([string]$Path,[string]$Root,[int]$ExpectedPacketCount=20)
  if($Path-eq""-or-not(Test-Path -LiteralPath $Path -PathType Leaf)){throw "Repeat requires a semantic pilot gate file"}
  $gateFull=(Resolve-Path -LiteralPath $Path).Path
  if(-not$gateFull.StartsWith($Root.TrimEnd([IO.Path]::DirectorySeparatorChar)+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw "semantic gate must be inside RunRoot"}
  $gate=Get-Content -LiteralPath $Path -Raw|ConvertFrom-Json
  $blindFiles=@(Get-ChildItem -LiteralPath (Join-Path $Root "blind") -Filter *.json -File|Sort-Object Name)
  if($blindFiles.Count-ne$ExpectedPacketCount){throw "semantic gate requires exactly $ExpectedPacketCount packets"}
  $packetSet=Get-Sha256Text (($blindFiles|ForEach-Object{"$($_.Name)|$((Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())"})-join"`n")
  $packetIds=@($blindFiles|ForEach-Object BaseName|Sort-Object)
  foreach($scoreName in @("lead_score","independent_score")){
    $pathProperty=$scoreName+"_path";$hashProperty=$scoreName+"_sha256"
    $scorePath=[string]$gate.$pathProperty
    if([string]::IsNullOrWhiteSpace($scorePath)-or-not(Test-Path -LiteralPath $scorePath -PathType Leaf)){throw "semantic gate missing $pathProperty"}
    $resolvedScore=(Resolve-Path -LiteralPath $scorePath).Path
    if(-not$resolvedScore.StartsWith((Join-Path $Root "scores").TrimEnd([IO.Path]::DirectorySeparatorChar)+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw "$pathProperty must be under RunRoot/scores"}
    if((Get-FileHash -LiteralPath $resolvedScore -Algorithm SHA256).Hash.ToLowerInvariant()-ne$gate.$hashProperty){throw "$scoreName hash mismatch"}
    $score=Get-Content -LiteralPath $resolvedScore -Raw|ConvertFrom-Json -Depth 50
    if($score.schema_version-ne1-or$score.packet_set_sha256-ne$packetSet){throw "$scoreName is not bound to the current packet set"}
    $scoredIds=@($score.packet_scores|ForEach-Object packet_id|Sort-Object)
    if($scoredIds.Count-ne$packetIds.Count-or(Compare-Object $packetIds $scoredIds)){throw "$scoreName does not score every packet exactly once"}
    if(@($score.packet_scores|Where-Object{$_.pass-ne$true}).Count-ne0){throw "$scoreName contains a failed or incomplete semantic score"}
  }
  $declaredPass = ($null -ne $gate.PSObject.Properties["pilot_pass"] -and $gate.pilot_pass -eq $true) -or ($null -ne $gate.PSObject.Properties["semantic_pass"] -and $gate.semantic_pass -eq $true)
  if(-not$declaredPass-or$gate.packet_set_sha256-ne$packetSet){throw "semantic gate is incomplete, stale, or failed"}
}

function Invoke-Schedule {
  Param($Manifest,$Templates,$Schedule,[string]$Root)
  $started=[DateTime]::UtcNow;$results=[Collections.Generic.List[object]]::new()
  foreach($slot in $Schedule){
    $existing=Get-ExistingUsage $Root
    $case=@($Manifest.cases|Where-Object id -eq $slot.case_id)[0];$predictedCalls=if($null-ne$case.PSObject.Properties["approval_prompt"]){2}else{1}
    if(($existing.invocations+$predictedCalls)-gt$MaxActualInvocations){throw "actual invocation cap would be exceeded"}
    if(($existing.input_tokens+(2000000*$predictedCalls))-gt$MaxInputTokens){throw "input-token safety reserve would be exceeded"}
    if(($existing.output_tokens+(50000*$predictedCalls))-gt$MaxOutputTokens){throw "output-token safety reserve would be exceeded"}
    if(([DateTime]::UtcNow-$started).TotalHours-ge$MaxBatchHours){throw "batch wall-time cap reached"}
    Write-Host "RUN $($slot.case_id) repetition=$($slot.repetition) variant=$($slot.variant)"
    $result=Invoke-EvaluationRun $Manifest $Templates $slot $Root;$results.Add($result)
    $afterUsage=Get-ExistingUsage $Root
    if($afterUsage.input_tokens-gt$MaxInputTokens-or$afterUsage.output_tokens-gt$MaxOutputTokens){throw "observed token cap exceeded; comparison is inconclusive"}
    if($result.mechanical_pass){Write-Host "PASS $($result.run_id) captured"}else{throw "$($result.run_id) is not mechanically gradeable; pilot stops fail-closed"}
  }
  return @($results)
}

function Get-AggregateReport {
  Param([string]$Root,[string]$ScoreGatePath)
  $files=@(Get-ChildItem -LiteralPath (Join-Path $Root "runs") -Filter metrics.json -File -Recurse -ErrorAction SilentlyContinue)
  $metrics=@($files|ForEach-Object{Get-Content -LiteralPath $_.FullName -Raw|ConvertFrom-Json -Depth 30})
  $expectedPilot=@(New-RunSchedule $manifest "Pilot"|ForEach-Object{"$($_.case_id)-r$($_.repetition)-$($_.variant)"})
  $expectedRepeat=@(New-RunSchedule $manifest "Repeat"|ForEach-Object{"$($_.case_id)-r$($_.repetition)-$($_.variant)"})
  $expectedAll=@($expectedPilot+$expectedRepeat)
  $actualIds=@($metrics|ForEach-Object run_id)
  $unexpected=@($actualIds|Where-Object{$_-notin$expectedAll})
  $duplicates=@($actualIds|Group-Object|Where-Object Count -gt1|ForEach-Object Name)
  if($unexpected.Count-gt0-or$duplicates.Count-gt0){throw "aggregate contains unexpected or duplicate run slots"}
  $input=[long]0;$cached=[long]0;$output=[long]0;$reasoning=[long]0
  foreach($m in $metrics){if($null-ne$m.usage){$input+=[long]$m.usage.input_tokens;$cached+=[long]$m.usage.cached_input_tokens;$output+=[long]$m.usage.output_tokens;$reasoning+=[long]$m.usage.reasoning_output_tokens}}
  $caseResults=[Collections.Generic.List[object]]::new()
  foreach($case in @($manifest.cases|Where-Object core_repeat)){
    $reductions=[Collections.Generic.List[double]]::new();$wins=0
    foreach($rep in 1..3){
      $baseline=@($metrics|Where-Object{$_.case_id-eq$case.id-and$_.repetition-eq$rep-and$_.variant-eq"baseline"})
      $candidate=@($metrics|Where-Object{$_.case_id-eq$case.id-and$_.repetition-eq$rep-and$_.variant-eq"candidate"})
      if($baseline.Count-eq1-and$candidate.Count-eq1-and$null-ne$baseline[0].usage-and$null-ne$candidate[0].usage){
        $b=[double]$baseline[0].usage.total_tokens;$c=[double]$candidate[0].usage.total_tokens
        if($b-gt0){$reduction=($b-$c)/$b;$reductions.Add($reduction);if($c-lt$b){$wins++}}
      }
    }
    $sorted=@($reductions|Sort-Object);$median=$null
    if($sorted.Count-eq3){$median=$sorted[1]}
    $caseResults.Add([pscustomobject]@{case_id=$case.id;paired_reductions=@($reductions);median_reduction=$median;candidate_wins=$wins;complete=$sorted.Count-eq3})
  }
  $caseMedians=@($caseResults|Where-Object complete|ForEach-Object{[double]$_.median_reduction}|Sort-Object)
  $overallMedian=if($caseMedians.Count-eq6){($caseMedians[2]+$caseMedians[3])/2}else{$null}
  $positiveCases=@($caseResults|Where-Object{$_.complete-and$_.median_reduction-gt0-and$_.candidate_wins-ge2})
  $mechanicalComplete=$metrics.Count-eq44-and@($metrics|Where-Object{-not$_.mechanical_pass}).Count-eq0-and@($expectedAll|Where-Object{$_-notin$actualIds}).Count-eq0
  $semanticComplete=$false
  if($metrics.Count-eq44){Assert-PilotSemanticGate $ScoreGatePath $Root 44;$semanticComplete=$true}
  $complete=$mechanicalComplete-and$semanticComplete
  $eligible=$complete-and$null-ne$overallMedian-and$overallMedian-ge0.15-and$positiveCases.Count-ge5-and@($caseResults|Where-Object{$_.complete-and$_.median_reduction-lt-0.10}).Count-eq0
  return [pscustomobject][ordered]@{schema_version=1;run_count=$metrics.Count;complete=$complete;mechanical_complete=$mechanicalComplete;semantic_complete=$semanticComplete;screening_token_eligible=$eligible;mechanically_passing=@($metrics|Where-Object mechanical_pass).Count;mechanically_failing=@($metrics|Where-Object{-not$_.mechanical_pass}).Count;input_tokens=$input;cached_input_tokens=$cached;output_tokens=$output;reasoning_output_tokens=$reasoning;total_tokens=$input+$output;overall_core_median_reduction=$overallMedian;positive_core_cases=$positiveCases.Count;case_results=@($caseResults);runs=$metrics|Sort-Object case_id,repetition,variant}
}

$manifest=Read-Manifest
switch($Action){
  "Validate"{Assert-MarkdownParity $manifest;if($RunRoot-ne""-or$TemplateManifest-ne""){if($RunRoot-eq""-or$TemplateManifest-eq""){throw "template validation requires both RunRoot and TemplateManifest"};$root=Assert-SentinelRoot $RunRoot;Read-TemplateManifest $root|Out-Null};Write-Host "PASS workflow evaluation manifest, generated Markdown, and requested frozen inputs are valid."}
  "GenerateDocs"{if(-not$UpdateMarkdown){throw "GenerateDocs requires -UpdateMarkdown"};Update-GeneratedMarkdown $manifest;Assert-MarkdownParity $manifest;Write-Host "PASS generated Markdown case table updated."}
  "ParseJsonl"{if($JsonlPath-eq""){throw "ParseJsonl requires -JsonlPath"};$parsed=Convert-JsonlStream $JsonlPath;$parsed|Add-Member -NotePropertyName network_violations -NotePropertyValue @(Get-NetworkViolations $parsed);$json=$parsed|ConvertTo-Json -Depth 50;if($OutputPath-ne""){Assert-ExternalOutputPath $OutputPath;Write-Utf8NoBom $OutputPath $json}else{$json}}
  "Plan"{(New-RunSchedule $manifest $PlanPhase)|ConvertTo-Json -Depth 10}
  "InspectState"{if($StateRoot-eq""){throw "InspectState requires -StateRoot"};Assert-NoDescendantReparse $StateRoot "state root";(Get-RepoState $StateRoot)|ConvertTo-Json -Depth 20}
  "InspectContent"{if($StateRoot-eq""){throw "InspectContent requires -StateRoot"};Assert-NoDescendantReparse $StateRoot "content root";(Get-CheckoutContentHash $StateRoot)|ConvertTo-Json -Depth 10}
  "ValidateScores"{$root=Assert-SentinelRoot $RunRoot;Assert-PilotSemanticGate $SemanticGatePath $root $ExpectedPacketCount;Write-Host "PASS semantic score gate is complete and bound to the packet set."}
  {$_-in@("Pilot","Repeat")} {
    if(-not$AllowLiveCalls){throw "$Action requires -AllowLiveCalls"}
    $canonicalManifest=(Resolve-Path -LiteralPath (Join-Path $repoRoot "docs/workflow-eval-cases.json")).Path
    if(-not(Resolve-Path -LiteralPath $ManifestPath).Path.Equals($canonicalManifest,[StringComparison]::OrdinalIgnoreCase)){throw "live actions require the canonical repository manifest"}
    $root=Assert-SentinelRoot $RunRoot;$templates=Read-TemplateManifest $root
    if($Action-eq"Repeat"){Assert-PilotSemanticGate $SemanticGatePath $root}
    $schedule=New-RunSchedule $manifest $Action
    $results=Invoke-Schedule $manifest $templates $schedule $root;Write-Utf8NoBom (Join-Path $root ("batch-"+$Action.ToLowerInvariant()+".json")) ($results|ConvertTo-Json -Depth 50);Write-Host "PASS $Action completed $($results.Count) scheduled runs."
  }
  "Aggregate"{$root=Assert-SentinelRoot $RunRoot;$summary=Get-AggregateReport $root $SemanticGatePath;$json=$summary|ConvertTo-Json -Depth 50;$path=if($OutputPath-ne""){$OutputPath}else{Join-Path $root "aggregate.json"};$full=[IO.Path]::GetFullPath($path);if(-not$full.StartsWith($root.TrimEnd([IO.Path]::DirectorySeparatorChar)+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw "aggregate output must be inside RunRoot"};if(Test-Path -LiteralPath $full){Assert-NoReparseAbsolutePath $full "aggregate output"};Write-Utf8NoBom $full $json;$json}
}
