<#
.SYNOPSIS
	Verify the repo-managed Codex skills bundle.

.DESCRIPTION
	Checks skill directories under codex-skills/ for required SKILL.md metadata,
	duplicate names, missing asset references from agents/openai.yaml, and stale
	user-skill install path references. This verifier does not read, write, or
	compare user-level Codex skill directories.

.PARAMETER Skills
	Repo-managed skill names to verify. Defaults to every directory directly
	under codex-skills/.

.NOTES
	Prerequisites: run from this repository.
	Side effects: reads repo files only.
	Safety: this verifier does not install, delete, or modify skill files.
#>

Param(
    [string[]]$Skills = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Add-Failure {
    Param(
        [System.Collections.Generic.List[string]]$Failures,
        [string]$Message
    )
    $Failures.Add($Message) | Out-Null
    Write-Host "FAIL $Message"
}

function Get-RelativePath {
    Param(
        [string]$BasePath,
        [string]$ChildPath
    )
    $baseUri = New-Object System.Uri(($BasePath.TrimEnd('\') + '\'))
    $childUri = New-Object System.Uri($ChildPath)
    $relativeUri = $baseUri.MakeRelativeUri($childUri)
    return [System.Uri]::UnescapeDataString($relativeUri.ToString()).Replace('/', '\')
}

function Get-FrontMatterValue {
    Param(
        [string]$FrontMatter,
        [string]$Key
    )
    $pattern = "(?m)^$([regex]::Escape($Key)):\s*['""]?([^'""\r\n]+)['""]?\s*$"
    $match = [regex]::Match($FrontMatter, $pattern)
    if (-not $match.Success) {
        return $null
    }
    return $match.Groups[1].Value.Trim()
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sourceRoot = Join-Path $repoRoot "codex-skills"
if (-not (Test-Path -LiteralPath $sourceRoot)) {
    throw "Missing source directory: $sourceRoot"
}

if ($Skills.Count -eq 0) {
    $Skills = Get-ChildItem -LiteralPath $sourceRoot -Directory |
        Sort-Object Name |
        Select-Object -ExpandProperty Name
}

$failures = New-Object System.Collections.Generic.List[string]
$seenNames = @{}
$textExtensions = @(".json", ".md", ".ps1", ".py", ".sh", ".svg", ".swift", ".txt", ".yaml", ".yml")
$forbiddenPatterns = @(
    @{ Label = "CODEX_HOME skills path"; Pattern = "CODEX_HOME[\\/]+skills" },
    @{ Label = "user .codex skills path"; Pattern = "\.codex[\\/]+skills" },
    @{ Label = "USERPROFILE Codex skills path"; Pattern = "USERPROFILE.*\.codex.*skills" },
    @{ Label = "user-home Codex wording"; Pattern = ("local Codex " + "home") }
)
$independentSpecialists = @(
    "design-challenger",
    "go-code-quality-review",
    "requirements-ambiguity-review",
    "scientific-model-oracle",
    "scope-ledger-adversarial-review",
    "test-strategy-adversary"
)
$selectedIndependentSpecialists = @($Skills | Where-Object { $independentSpecialists -contains $_ })
$canonicalOwnerValid = $true
if ($selectedIndependentSpecialists.Count -gt 0) {
    $agentsPath = Join-Path $repoRoot "AGENTS.md"
    if (-not (Test-Path -LiteralPath $agentsPath -PathType Leaf)) {
        Add-Failure -Failures $failures -Message "missing canonical independent-review owner: AGENTS.md"
        $canonicalOwnerValid = $false
    } else {
        $agentsContent = Get-Content -LiteralPath $agentsPath -Raw
        $canonicalFields = @(
            "Agent status: completed | unsupported | not authorized/not requested | explicitly prohibited | failed | timed out | inconclusive",
            "Status detail: none | no independent context | context contaminated | <failure or timeout detail>",
            "Role outcome: used when status is completed | N/A",
            "Waiver disposition: none | <scope, owner, mitigation, expiry>"
        )
        foreach ($field in $canonicalFields) {
            $count = ([regex]::Matches($agentsContent, [regex]::Escape($field))).Count
            if ($count -ne 1) {
                Add-Failure -Failures $failures -Message "AGENTS.md must own canonical independent-review field exactly once: $field (found $count)"
                $canonicalOwnerValid = $false
            }
        }
    }
}

foreach ($skill in $Skills) {
    $failureCountBeforeSkill = $failures.Count
    $skillDir = Join-Path $sourceRoot $skill
    if (-not (Test-Path -LiteralPath $skillDir)) {
        Add-Failure -Failures $failures -Message "[$skill] missing repo source: $skillDir"
        continue
    }

    $skillFile = Join-Path $skillDir "SKILL.md"
    if (-not (Test-Path -LiteralPath $skillFile)) {
        Add-Failure -Failures $failures -Message "[$skill] missing SKILL.md"
        continue
    }

    $content = Get-Content -LiteralPath $skillFile -Raw
    $frontMatterMatch = [regex]::Match($content, "(?s)^---\r?\n(.*?)\r?\n---")
    if (-not $frontMatterMatch.Success) {
        Add-Failure -Failures $failures -Message "[$skill] missing SKILL.md front matter"
        continue
    }

    $frontMatter = $frontMatterMatch.Groups[1].Value
    $declaredName = Get-FrontMatterValue -FrontMatter $frontMatter -Key "name"
    $description = Get-FrontMatterValue -FrontMatter $frontMatter -Key "description"
    if ([string]::IsNullOrWhiteSpace($declaredName)) {
        Add-Failure -Failures $failures -Message "[$skill] missing front matter name"
    } elseif ($declaredName -ne $skill) {
        Add-Failure -Failures $failures -Message "[$skill] front matter name '$declaredName' does not match directory"
    } elseif ($seenNames.ContainsKey($declaredName)) {
        Add-Failure -Failures $failures -Message "[$skill] duplicate skill name '$declaredName' also declared by '$($seenNames[$declaredName])'"
    } else {
        $seenNames[$declaredName] = $skill
    }

    if ([string]::IsNullOrWhiteSpace($description)) {
        Add-Failure -Failures $failures -Message "[$skill] missing front matter description"
    }

    if ($independentSpecialists -contains $skill) {
        if ($content -notmatch 'independent-review contract in `AGENTS\.md`') {
            Add-Failure -Failures $failures -Message "[$skill] missing canonical independent-review contract reference to AGENTS.md"
        }
        if ($content -notmatch 'canonical four-field independent-result envelope from\s+`AGENTS\.md`') {
            Add-Failure -Failures $failures -Message "[$skill] missing canonical four-field result-envelope reference to AGENTS.md"
        }
        foreach ($fieldName in @('Agent status', 'Status detail', 'Role outcome', 'Waiver disposition')) {
            if ($content -match "(?m)^\s*-?\s*$([regex]::Escape($fieldName)):") {
                Add-Failure -Failures $failures -Message "[$skill] duplicates the AGENTS.md-owned $fieldName field"
            }
        }
    }

    $agentFile = Join-Path $skillDir "agents\openai.yaml"
    if (Test-Path -LiteralPath $agentFile) {
        $agentLines = Get-Content -LiteralPath $agentFile
        foreach ($line in $agentLines) {
            $match = [regex]::Match($line, "^\s*icon_(small|large):\s*['""]?(.+?)['""]?\s*$")
            if (-not $match.Success) {
                continue
            }
            $assetPath = $match.Groups[2].Value.Trim()
            if ($assetPath.StartsWith("./")) {
                $assetPath = $assetPath.Substring(2)
            }
            $assetFullPath = Join-Path $skillDir $assetPath
            if (-not (Test-Path -LiteralPath $assetFullPath)) {
                Add-Failure -Failures $failures -Message "[$skill] missing agent asset: $assetPath"
            }
        }
    }

    $textFiles = Get-ChildItem -LiteralPath $skillDir -Recurse -File |
        Where-Object { $textExtensions -contains $_.Extension.ToLowerInvariant() }
    foreach ($file in $textFiles) {
        $relativePath = Get-RelativePath -BasePath $skillDir -ChildPath $file.FullName
        $fileText = Get-Content -LiteralPath $file.FullName -Raw
        foreach ($forbidden in $forbiddenPatterns) {
            if ($fileText -match $forbidden.Pattern) {
                Add-Failure -Failures $failures -Message "[$skill] stale $($forbidden.Label): $relativePath"
            }
        }
    }

    if ($failures.Count -eq $failureCountBeforeSkill -and (-not ($independentSpecialists -contains $skill) -or $canonicalOwnerValid)) {
        Write-Host "PASS [$skill] repo skill metadata and canonical routes checked."
    }
}

if ($failures.Count -gt 0) {
    Write-Host "FAIL repo skill verification found $($failures.Count) issue(s)."
    exit 1
}

Write-Host "PASS all requested repo skills verified."
exit 0
