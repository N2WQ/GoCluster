<#
.SYNOPSIS
    Holds program-specific Windows Firewall blocks for one release smoke process.
.DESCRIPTION
    This is the privileged half of test-release-smoke.ps1. It never launches the
    package, writes an output file, changes a profile, or targets a node outside
    the uniquely named extraction. Named events coordinate with the ordinary
    build process. Losing the owner triggers child retirement and rule cleanup.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{32}$')][string]$RunId,
    [Parameter(Mandatory)][string]$Executable,
    [Parameter(Mandatory)][int]$OwnerPid
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Executable = [IO.Path]::GetFullPath($Executable)
$folder = [IO.Directory]::GetParent($Executable)
if ([IO.Path]::GetFileName($Executable) -cne 'gocluster.exe' -or
    $folder.Parent.Name -cne "gocluster-release-smoke-$RunId" -or
    -not [IO.File]::Exists($Executable)) {
    throw 'Unexpected executable for the release smoke firewall rules.'
}
$name = "GoCluster-release-smoke-$RunId"
$ready = [Threading.EventWaitHandle]::OpenExisting("Local\$name-ready")
$done = [Threading.EventWaitHandle]::OpenExisting("Local\$name-done")
$owner = Get-Process -Id $OwnerPid -ErrorAction Stop
$ownedRules = @()
$failed = $false
try {
    $profiles = @(Get-NetFirewallProfile -PolicyStore ActiveStore)
    if ($profiles.Count -ne 3 -or @($profiles | Where-Object {
        $_.Enabled -ne 'True' -or $_.AllowLocalFirewallRules -eq 'False' -or $_.DisabledInterfaceAliases
    }).Count -ne 0) {
        throw 'The active firewall policy cannot enforce program-specific rules on every profile and interface.'
    }
    # Cover IPv4 and IPv6 except their loopback ranges. The probe and reference
    # sink run on IPv4 loopback and their ports are owned by this smoke run.
    $remote = @('0.0.0.0-126.255.255.255', '128.0.0.0-255.255.255.255',
        '::2-ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')
    foreach ($direction in 'Inbound', 'Outbound') {
        $ruleName = "$name-$direction"
        New-NetFirewallRule -Name $ruleName -DisplayName $ruleName -PolicyStore PersistentStore `
            -Program $Executable -Profile Any -Direction $direction -Action Block `
            -Protocol Any -RemoteAddress $remote -Enabled True | Out-Null
        $ownedRules += $ruleName
        $effective = Get-NetFirewallRule -Name $ruleName -PolicyStore ActiveStore
        $program = ($effective | Get-NetFirewallApplicationFilter).Program
        if ($effective.Enabled -ne 'True' -or $effective.Action -ne 'Block' -or
            $effective.Direction -ne $direction -or
            (@($effective.EnforcementStatus) -notcontains 'Enforced' -and @($effective.EnforcementStatus) -notcontains 'Full') -or
            $program -ine $Executable) {
            throw "Windows did not activate the $direction rule for the extracted executable."
        }
    }
    [void]$ready.Set()
    while (-not $done.WaitOne(500) -and -not $owner.HasExited) {}
} catch {
    Write-Error $_ -ErrorAction Continue
    $failed = $true
} finally {
    $retired = $false
    try {
        # Ordinarily the parent has already retired its child. If the caller
        # was interrupted, hold the firewall rules until its unique image exits.
        $image = $Executable.Replace('\', '\\').Replace("'", "\'")
        $match = @(Get-CimInstance Win32_Process -Filter "Name='gocluster.exe' AND ExecutablePath='$image'")
        foreach ($record in $match) {
            $proc = Get-Process -Id $record.ProcessId -ErrorAction SilentlyContinue
            if ($null -eq $proc) { continue }
            try {
                if (-not $proc.HasExited -and $proc.Path -ieq $Executable) {
                    $proc.Kill()
                    if (-not $proc.WaitForExit(10000)) { throw 'Could not retire the orphaned smoke executable.' }
                }
            } finally { $proc.Dispose() }
        }
        $retired = $true
    } finally {
        if ($retired) {
            foreach ($ruleName in $ownedRules) {
                try { Remove-NetFirewallRule -Name $ruleName -PolicyStore PersistentStore }
                catch { Write-Error "Remove the stale smoke rule $ruleName. $_" -ErrorAction Continue; $failed = $true }
            }
        } elseif ($ownedRules.Count -gt 0) {
            Write-Error "Unable to verify child retirement. Firewall blocks retained: $($ownedRules -join ', ')." -ErrorAction Continue
            $failed = $true
        }
        $owner.Dispose()
        $ready.Dispose()
        $done.Dispose()
    }
}
if ($failed) { exit 1 }
