<#
.SYNOPSIS
    Exercise the release smoke acceptance checks with a synthetic loopback child.
.DESCRIPTION
    Runs as the current user; the child only accepts two IPv4 loopback
    connections. This test never starts the cluster or changes firewall rules.
#>
[CmdletBinding()]
param([string]$ServePortFile, [string]$Prompt)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ($ServePortFile) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        [IO.File]::WriteAllText($ServePortFile, $listener.LocalEndpoint.Port.ToString())
        foreach ($response in 'other listener', $Prompt) {
            $accept = $listener.AcceptTcpClientAsync()
            if (-not $accept.Wait(30000)) { throw 'Timed out waiting for fixture probe.' }
            $client = $accept.Result
            try {
                $bytes = [Text.Encoding]::ASCII.GetBytes($response)
                $client.GetStream().Write($bytes, 0, $bytes.Length)
            } finally { $client.Dispose() }
        }
    } finally { $listener.Stop() }
    return
}

function Assert-Refused([scriptblock]$Check, [string]$Expected) {
    try { & $Check }
    catch {
        if ($_.Exception.Message.Contains($Expected)) { return }
        throw
    }
    throw "Smoke check unexpectedly passed; expected '$Expected'."
}

$target = Join-Path $PSScriptRoot 'test-release-smoke.ps1'
$parseErrors = $null; $tokens = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($target, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors) { throw $parseErrors[0] }
foreach ($name in 'Assert-SmokeModels','Assert-SmokeListener') {
    $func = $ast.Find({ param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name
    }, $true)
    if (-not $func) { throw "Missing $name in smoke harness." }
    . ([scriptblock]::Create($func.Extent.Text))
}

$id = [guid]::NewGuid().ToString('N')
$root = Join-Path ([IO.Path]::GetTempPath()) "gocluster-smoke-checks-$id"
$portFile = Join-Path $root 'port'
$log = Join-Path $root 'system.log'
$nonce = "smoke-$id"
$child = $null
$null = New-Item -Path $root -ItemType Directory
try {
    $argsForChild = @('-NoProfile', '-File', ('"' + $PSCommandPath + '"'),
        '-ServePortFile', ('"' + $portFile + '"'), '-Prompt', $nonce)
    $child = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList $argsForChild -PassThru -WindowStyle Hidden
    $deadline = [datetime]::UtcNow.AddSeconds(15)
    while (-not [IO.File]::Exists($portFile)) {
        if ($child.HasExited -or [datetime]::UtcNow -ge $deadline) { throw 'Fixture child failed to start.' }
        Start-Sleep -Milliseconds 50
    }
    $port = [int][IO.File]::ReadAllText($portFile)
    $self = [Diagnostics.Process]::GetCurrentProcess()
    Assert-Refused { Assert-SmokeListener $self $port $nonce ([datetime]::UtcNow.AddSeconds(1)) } 'Timed out waiting'
    Assert-Refused { Assert-SmokeListener $child $port $nonce ([datetime]::UtcNow.AddSeconds(5)) } 'unique login prompt'
    Assert-SmokeListener $child $port $nonce ([datetime]::UtcNow.AddSeconds(5))

    $lines = @('Loaded 12 spotter reliability weights from a.txt', 'Loaded 3 CW spotter reliability weights from b.txt',
        'Loaded 19 RTTY spotter reliability weights from c.txt', 'Loaded call correction confusion model from model.json')
    [IO.File]::WriteAllLines($log, [string[]]$lines)
    Assert-SmokeModels $self $log ([datetime]::UtcNow.AddSeconds(1))
    foreach ($index in 0..3) {
        $bad = [string[]]$lines.Clone()
        $bad[$index] = if ($index -eq 3) { '' } else { $bad[$index] -replace '^Loaded \d+', 'Loaded 0' }
        [IO.File]::WriteAllLines($log, $bad)
        Assert-Refused { Assert-SmokeModels $self $log ([datetime]::UtcNow) } 'three nonempty reliability tables'
    }
    Write-Host 'Release smoke assertions passed: owner PID, unique prompt, three nonempty tables and confusion model.'
} finally {
    if ($child) {
        if (-not $child.HasExited) { $child.Kill() }
        if (-not $child.WaitForExit(5000)) { throw 'Could not retire the synthetic listener.' }
        $child.Dispose()
    }
    $owned = (Resolve-Path -LiteralPath $root).ProviderPath
    if ($owned -ine [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetTempPath()) "gocluster-smoke-checks-$id"))) {
        throw "Unexpected fixture cleanup path: $owned"
    }
    Remove-Item -LiteralPath $owned -Recurse -Force
}
