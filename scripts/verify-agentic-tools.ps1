<#
.SYNOPSIS
	Verify local tools used by the repo's agentic development workflow.

.DESCRIPTION
	Checks required repository workflow tools, required semantic/navigation
	helpers, recommended Go developer helpers, and optional investigation tools.
	Missing required tools fail the script. Missing recommended or optional tools
	are reported separately so they do not block ordinary Go implementation,
	review, or validation.

.PARAMETER Quiet
	Suppress successful tool lines and show only missing tools plus the summary.

.NOTES
	Prerequisites: PowerShell and the current process/user/machine PATH.
	Side effects: reads PATH and runs lightweight version probes only.
	Safety: no files, environment variables, packages, or repo state are modified.
#>

Param(
    [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$processPath = $env:Path
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
$env:Path = "$processPath;$machinePath;$userPath"

$versionArgs = @{
    "go" = @("version")
    "git" = @("--version")
    "rg" = @("--version")
    "staticcheck" = @("-version")
    "golangci-lint" = @("--version")
    "gopls" = @("version")
    "jq" = @("--version")
    "yq" = @("--version")
    "fd" = @("--version")
    "bat" = @("--version")
    "govulncheck" = @("-version")
    "dlv" = @("version")
    "gotestsum" = @("--version")
    "benchstat" = @("-h")
    "delta" = @("--version")
    "fzf" = @("--version")
    "goda" = @("version")
    "go-callvis" = @("-version")
    "semgrep" = @("--version")
    "ast-grep" = @("--version")
    "osv-scanner" = @("--version")
    "gitleaks" = @("version")
}

$toolGroups = @(
    @{
        Label = "required repo workflow"
        Required = $true
        Tools = @("go", "git", "rg", "staticcheck", "golangci-lint")
    },
    @{
        Label = "required agentic navigation"
        Required = $true
        Tools = @("gopls", "callgraph", "jq", "yq", "fd", "bat")
    },
    @{
        Label = "recommended developer helpers"
        Required = $false
        Tools = @("govulncheck", "dlv", "goimports", "gotestsum", "benchstat", "delta", "fzf")
    },
    @{
        Label = "optional investigation helpers"
        Required = $false
        Tools = @("goda", "go-callvis", "semgrep", "ast-grep", "osv-scanner", "gitleaks", "handle", "tcpview")
    }
)

function Get-VersionLine {
    Param(
        [string]$CommandName,
        [string]$CommandSource
    )

    if (-not $versionArgs.ContainsKey($CommandName)) {
        return "found"
    }

    try {
        $output = & $CommandSource @($versionArgs[$CommandName]) 2>&1 |
            Where-Object { $_ -and $_.ToString().Trim() -ne "" } |
            Select-Object -First 2
        if ($output) {
            return (($output | ForEach-Object { $_.ToString().Trim() }) -join " | ")
        }
        return "found"
    } catch {
        return "found; version probe failed: $($_.Exception.Message)"
    }
}

$missingRequired = New-Object System.Collections.Generic.List[string]
$missingRecommended = New-Object System.Collections.Generic.List[string]

foreach ($group in $toolGroups) {
    Write-Host "[$($group.Label)]"
    foreach ($tool in $group.Tools) {
        $cmd = Get-Command $tool -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $cmd) {
            if ($group.Required) {
                $missingRequired.Add($tool) | Out-Null
                Write-Host "FAIL  $tool (missing)"
            } else {
                $missingRecommended.Add($tool) | Out-Null
                Write-Host "WARN  $tool (missing)"
            }
            continue
        }

        if (-not $Quiet) {
            $version = Get-VersionLine -CommandName $tool -CommandSource $cmd.Source
            Write-Host "PASS  $tool - $version"
        }
    }
}

if ($missingRequired.Count -gt 0) {
    Write-Host ""
    Write-Host "FAIL missing required tools: $($missingRequired -join ', ')"
    if ($missingRecommended.Count -gt 0) {
        Write-Host "WARN missing recommended/optional tools: $($missingRecommended -join ', ')"
    }
    exit 1
}

Write-Host ""
Write-Host "PASS required agentic workflow tools are available."
if ($missingRecommended.Count -gt 0) {
    Write-Host "WARN missing recommended/optional tools: $($missingRecommended -join ', ')"
    Write-Host "WARN optional absence is a conditional evidence gap only when a workflow specifically needs that tool."
}
exit 0
