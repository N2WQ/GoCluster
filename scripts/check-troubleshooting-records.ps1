<#
.SYNOPSIS
	Check troubleshooting record index and RCA summary consistency.

.DESCRIPTION
	Verifies that docs/troubleshooting-log.md and docs/troubleshooting/TSR-*.md
	stay aligned for support-agent and maintainer retrieval. The checker confirms
	index links exist, each real TSR file is indexed, file/index statuses match,
	each TSR has an RCA Summary, root-cause and fix evidence exists, and indexed
	ADR references are present in the TSR body.

.NOTES
	Prerequisites: run from a Git worktree with git available.
	Side effects: none; this script is read-only.
	Safety: failures report missing or inconsistent documentation only and do not
	modify repository files.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-RepoRoot {
	$root = & git rev-parse --show-toplevel
	if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($root)) {
		throw "Unable to resolve repository root with git."
	}
	return $root.Trim()
}

function ConvertTo-RepoPath {
	Param([string]$Path)
	return $Path.Replace('\', '/').Trim()
}

function Get-TSRRows {
	Param([string]$IndexPath)
	$rows = @()
	$lines = Get-Content -LiteralPath $IndexPath
	foreach ($line in $lines) {
		if ($line -notmatch '^\|\s*TSR-\d{4}\s*\|') {
			continue
		}
		$cells = @($line.Split('|') | ForEach-Object { $_.Trim() })
		if ($cells.Count -lt 8) {
			throw "Malformed TSR index row: $line"
		}
		$linkCell = $cells[7]
		$linkMatch = [regex]::Match($linkCell, '`([^`]+)`')
		$link = ""
		if ($linkMatch.Success) {
			$link = ConvertTo-RepoPath $linkMatch.Groups[1].Value
		}
		$rows += [pscustomobject]@{
			Id = $cells[1]
			Title = $cells[2]
			Status = $cells[3]
			Date = $cells[4]
			Area = $cells[5]
			LedToADR = $cells[6]
			Link = $link
			Raw = $line
		}
	}
	return $rows
}

function Get-FileStatus {
	Param([string]$Text)
	$match = [regex]::Match($Text, '(?im)^\s*-?\s*Status:\s*(\S+)')
	if (-not $match.Success) {
		return ""
	}
	return $match.Groups[1].Value.Trim()
}

function Get-IndexedADRs {
	Param([string]$LedToADR)
	if ([string]::IsNullOrWhiteSpace($LedToADR)) {
		return @()
	}
	if ($LedToADR.Trim() -in @("-", "none", "None", "n/a", "N/A")) {
		return @()
	}
	return @([regex]::Matches($LedToADR, 'ADR-\d{4}') | ForEach-Object { $_.Value } | Select-Object -Unique)
}

$repoRoot = Resolve-RepoRoot
$indexPath = Join-Path $repoRoot "docs/troubleshooting-log.md"
$tsrDir = Join-Path $repoRoot "docs/troubleshooting"
$decisionDir = Join-Path $repoRoot "docs/decisions"
$errors = New-Object System.Collections.Generic.List[string]

if (-not (Test-Path -LiteralPath $indexPath)) {
	throw "Missing docs/troubleshooting-log.md."
}

$rows = Get-TSRRows -IndexPath $indexPath
$indexedPaths = New-Object System.Collections.Generic.HashSet[string]

foreach ($row in $rows) {
	if ([string]::IsNullOrWhiteSpace($row.Link)) {
		$errors.Add("$($row.Id): index row is missing a markdown code link.")
		continue
	}

	[void]$indexedPaths.Add($row.Link)
	$filePath = Join-Path $repoRoot $row.Link.Replace('/', [System.IO.Path]::DirectorySeparatorChar)
	if (-not (Test-Path -LiteralPath $filePath)) {
		$errors.Add("$($row.Id): linked file does not exist: $($row.Link)")
		continue
	}

	$text = Get-Content -LiteralPath $filePath -Raw
	$fileStatus = Get-FileStatus -Text $text
	if ($fileStatus -eq "") {
		$errors.Add("$($row.Id): linked file has no Status metadata.")
	} elseif ($fileStatus -ne $row.Status) {
		$errors.Add("$($row.Id): index status '$($row.Status)' does not match file status '$fileStatus'.")
	}

	if ($text -notmatch '(?m)^## RCA Summary\s*$') {
		$errors.Add("$($row.Id): missing required '## RCA Summary' section.")
	}

	if ($text -notmatch '(?im)^## Root cause or best current explanation\s*$|Root cause \(or best current explanation\):|^- Root cause:') {
		$errors.Add("$($row.Id): missing root-cause evidence section or bullet.")
	}

	if ($text -notmatch '(?im)^## Fix or mitigation\s*$|^## Fix or Mitigation\s*$|^## Decision Linkage\s*$|Decision delta summary:') {
		$errors.Add("$($row.Id): missing fix/remediation evidence section.")
	}

	foreach ($adr in (Get-IndexedADRs -LedToADR $row.LedToADR)) {
		if ($text -notmatch [regex]::Escape($adr)) {
			$errors.Add("$($row.Id): index Led To ADR references $adr but the TSR body does not mention it.")
		}
		$adrFiles = @(Get-ChildItem -LiteralPath $decisionDir -Filter "$adr-*.md" -File)
		if ($adrFiles.Count -eq 0) {
			$errors.Add("$($row.Id): index Led To ADR references $adr but no docs/decisions/$adr-*.md file exists.")
		}
	}
}

$realTSRFiles = @(Get-ChildItem -LiteralPath $tsrDir -Filter "TSR-*.md" -File | Where-Object { $_.Name -ne "TSR-TEMPLATE.md" })
foreach ($file in $realTSRFiles) {
	$repoPath = ConvertTo-RepoPath ($file.FullName.Substring($repoRoot.Length).TrimStart('\', '/'))
	if (-not $indexedPaths.Contains($repoPath)) {
		$errors.Add("${repoPath}: real TSR file is not indexed in docs/troubleshooting-log.md.")
	}
}

if ($errors.Count -gt 0) {
	Write-Host "FAIL: troubleshooting record check found $($errors.Count) issue(s)."
	foreach ($e in $errors) {
		Write-Host " - $e"
	}
	exit 1
}

Write-Host "PASS: troubleshooting records are indexed, status-aligned, RCA summarized, and ADR-linked."
exit 0
