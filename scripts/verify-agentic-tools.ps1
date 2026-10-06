<#
.SYNOPSIS
	Verify local tools used by the repo's agentic development workflow.

.DESCRIPTION
	Checks required repository workflow tools, required semantic/navigation
	helpers, recommended Go developer helpers, and optional investigation tools.
	Missing required tools or failed required version probes fail the script.
	Missing or failed recommended/optional tools are reported separately and do
	not block ordinary Go implementation,
	review, or validation.

.PARAMETER Quiet
	Suppress successful tool lines; all checks still run and failures remain visible.

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

$windowsHost = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT
$pathVariable = if ($windowsHost) { 'Path' } else { 'PATH' }
$processPath = [Environment]::GetEnvironmentVariable($pathVariable, 'Process')

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
    "delta" = @("--version")
    "fzf" = @("--version")
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
        Tools = @("dot", "goda", "go-callvis", "semgrep", "ast-grep", "osv-scanner", "gitleaks", "handle", "tcpview")
    }
)

function Get-ToolProbe {
    Param([string]$CommandName, [System.Management.Automation.CommandInfo]$Command)

    if (-not $versionArgs.ContainsKey($CommandName)) {
        return @{ Success = $true; Detail = "presence only; no version probe configured" }
    }
    try {
        # Reset stale native status and consume all output before reading exit status.
        $global:LASTEXITCODE = 0
        $output = @(& $Command @($versionArgs[$CommandName]) 2>&1)
        if ($LASTEXITCODE -ne 0) {
            return @{ Success = $false; Detail = "version probe failed with exit code $LASTEXITCODE" }
        }
        $lines = @($output | Where-Object { $_ -and $_.ToString().Trim() -ne "" } |
            Select-Object -First 2 | ForEach-Object { $_.ToString().Trim() })
        $detail = if ($lines.Count) { $lines -join " | " } else { "version probe succeeded; no version text returned" }
        return @{ Success = $true; Detail = $detail }
    } catch {
        return @{ Success = $false; Detail = "version probe failed: $($_.Exception.Message)" }
    }
}

$failedRequired = New-Object System.Collections.Generic.List[string]
$failedRecommended = New-Object System.Collections.Generic.List[string]
$exitCode = 0
try {
    # User/machine PATH and Graphviz installation conventions are Windows-only.
    # On other hosts the process PATH is already the authoritative search path.
    if ($windowsHost) {
        $extraPaths = @(
            [Environment]::GetEnvironmentVariable("Path", "Machine")
            [Environment]::GetEnvironmentVariable("Path", "User")
        )
        if ($env:LOCALAPPDATA) {
            $extraPaths += Join-Path $env:LOCALAPPDATA "Programs\Graphviz\bin"
            $extraPaths += Join-Path $env:LOCALAPPDATA "VirtualStore\Program Files\Graphviz\bin"
        }
        if ($env:ProgramFiles) { $extraPaths += Join-Path $env:ProgramFiles "Graphviz\bin" }
        [Environment]::SetEnvironmentVariable($pathVariable,
            ((@($processPath) + @($extraPaths | Where-Object { $_ })) -join [IO.Path]::PathSeparator), 'Process')
    }

    foreach ($group in $toolGroups) {
        Write-Host "[$($group.Label)]"
        foreach ($tool in $group.Tools) {
            $cmd = Get-Command $tool -ErrorAction SilentlyContinue | Select-Object -First 1
            $probe = if ($cmd) { Get-ToolProbe -CommandName $tool -Command $cmd } else {
                @{ Success = $false; Detail = "missing" }
            }
            if (-not $probe.Success) {
                if ($group.Required) {
                    $failedRequired.Add($tool)
                    Write-Host "FAIL  $tool ($($probe.Detail))"
                } else {
                    $failedRecommended.Add($tool)
                    Write-Host "WARN  $tool ($($probe.Detail))"
                }
            } elseif (-not $Quiet) {
                Write-Host "PASS  $tool - $($probe.Detail)"
            }
        }
    }
    Write-Host ""
    if ($failedRequired.Count) {
        Write-Host "FAIL unavailable required tools: $($failedRequired -join ', ')"
        $exitCode = 1
    } else {
        Write-Host "PASS required agentic workflow tools are available."
    }
    if ($failedRecommended.Count) {
        Write-Host "WARN unavailable recommended/optional tools: $($failedRecommended -join ', ')"
        Write-Host "WARN optional absence is a conditional evidence gap only when a workflow specifically needs that tool."
    }
} finally {
    if ($null -eq $processPath) {
        Remove-Item -LiteralPath "Env:$pathVariable" -ErrorAction SilentlyContinue
    } else {
        [Environment]::SetEnvironmentVariable($pathVariable, $processPath, 'Process')
    }
}
exit $exitCode
