<#
.SYNOPSIS
    Check isolated PGO pair publication using mocked Go and Git commands.
.DESCRIPTION
    Creates only synthetic text executables below a fresh temporary directory.
    Neither Go nor Git is run. Both build failures and two successful runs must
    preserve the original executable pair and every prior PGO output.
#>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$source = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'consolidate-and-build-pgo.ps1') -Raw
$rootAssignment = '$repoRoot = "C:\src\gocluster"'
if (-not $source.Contains($rootAssignment)) { throw 'Fixture repository substitution no longer matches the script.' }
$fixtureBase = [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetTempPath()) ('pgo-pair-fixtures-' + [guid]::NewGuid().ToString('N'))))
$null = New-Item -ItemType Directory -Path $fixtureBase
$originalLocation = Get-Location
$originalExitCode = Get-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue
$savedExitCode = if ($originalExitCode) { $originalExitCode.Value } else { $null }
$passed = 0

function Invoke-PGOPairFixture([string]$Name, [int]$FailBuild, [int]$Successes = 1) {
    $root = Join-Path $fixtureBase $Name
    $null = New-Item -ItemType Directory -Path (Join-Path $root 'logs') -Force
    $prior = Join-Path $root '.tmp/pgo/pgo-existing'
    $null = New-Item -ItemType Directory -Path $prior -Force
    $protected = @('gocluster.exe', 'peerdiag.exe', 'gocluster_pgo.exe',
        '.tmp/pgo/pgo-existing/gocluster_pgo.exe', '.tmp/pgo/pgo-existing/peerdiag.exe', '.tmp/pgo/pgo-existing/binaries.json')
    $before = @{}
    foreach ($file in $protected) {
        $path = Join-Path $root $file
        Set-Content -LiteralPath $path -Value ('ORIGINAL ' + $file)
        $before[$file] = (Get-FileHash -LiteralPath $path).Hash
    }
    foreach ($profile in @('cpu-old.pprof', 'cpu-new.pprof')) {
        Set-Content -LiteralPath (Join-Path $root ('logs/' + $profile)) -Value 'SYNTHETIC PROFILE'
    }
    (Get-Item -LiteralPath (Join-Path $root 'logs/cpu-old.pprof')).LastWriteTimeUtc = [datetime]'2026-01-01T00:00:00Z'
    (Get-Item -LiteralPath (Join-Path $root 'logs/cpu-new.pprof')).LastWriteTimeUtc = [datetime]'2026-01-02T00:00:00Z'
    $fixtureScript = Join-Path $root 'subject.ps1'
    # Replace only the existing fixed repository location in a temporary copy;
    # all production branching, command arguments and filesystem work are real.
    $source.Replace($rootAssignment, ('$repoRoot = ' + "'" + $root.Replace("'", "''") + "'")) |
        Set-Content -LiteralPath $fixtureScript
    $state = @{ build = 0 }
    function git {
        $global:LASTEXITCODE = 0
        if ($args[0] -eq 'rev-parse') { return '0123456789ab' }
        if ($args[0] -eq 'status') { return }
        throw 'Unexpected mocked Git command.'
    }
    function go {
        $global:LASTEXITCODE = 0
        if ($args[0] -eq 'tool') {
            if ($args[1] -ne 'pprof' -or $args[4] -ne (Join-Path $root 'gocluster.exe') -or
                [IO.Path]::GetFileName($args[5]) -ne 'cpu-old.pprof' -or
                [IO.Path]::GetFileName($args[6]) -ne 'cpu-new.pprof') { throw 'Profile selection or merge changed.' }
            Set-Content -LiteralPath $args[3].Substring(8) -Value 'SYNTHETIC MERGE'
            return
        }
        if ($args[0] -ne 'build') { throw 'Unexpected mocked Go command.' }
        $state.build++
        $output = if ($args[1] -like '-pgo=*') { $args[3].Substring(3) } else { $args[3] }
        $stage = [IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($output))
        if ([IO.Path]::GetDirectoryName($stage) -ne (Join-Path $root '.tmp/pgo') -or
            -not [IO.Path]::GetFileName($stage).StartsWith('.stage-')) { throw 'Build escaped isolated staging.' }
        $published = @(Get-ChildItem -LiteralPath (Join-Path $root '.tmp/pgo') -Directory -Force | Where-Object Name -NotLike '.stage-*')
        if ($published.Count -ne ($attempt + 1)) { throw 'Pair published before both builds completed.' }
        foreach ($unsafe in @($root, (Join-Path $root '.tmp/pgo'), (Join-Path $root '.tmp/pgo/../outside'))) {
            $refused = $false
            try { Assert-OwnedPGODirectory $unsafe } catch { $refused = $true }
            if (-not $refused) { throw 'Directory move/delete guard accepted an unowned path.' }
        }
        Set-Content -LiteralPath $output -Value ('SYNTHETIC BUILD ' + $state.build)
        if ($state.build -eq $FailBuild) { $global:LASTEXITCODE = 1 }
    }
    for ($attempt = 0; $attempt -lt $Successes; $attempt++) {
        $state.build = 0
        $caught = ''
        try { & $fixtureScript 6>$null } catch { $caught = $_.Exception.Message }
        $expected = switch ($FailBuild) { 1 { 'go build failed' }; 2 { 'peer diagnostic companion build failed' }; default { '' } }
        if ($caught -cne $expected) { throw "${Name}: unexpected result '$caught'" }
        if ($state.build -ne $(if ($FailBuild -eq 1) { 1 } else { 2 })) { throw "${Name}: incorrect build sequence" }
        foreach ($file in $protected) {
            if ((Get-FileHash -LiteralPath (Join-Path $root $file)).Hash -cne $before[$file]) { throw "${Name}: changed prior output $file" }
        }
        $outputs = @(Get-ChildItem -LiteralPath (Join-Path $root '.tmp/pgo') -Directory -Force)
        $expectedCount = if ($FailBuild) { 1 } else { $attempt + 2 }
        if ($outputs.Count -ne $expectedCount -or @($outputs | Where-Object Name -Like '.stage-*').Count) {
            throw "${Name}: incomplete stage published or retained"
        }
        foreach ($output in @($outputs | Where-Object Name -NE 'pgo-existing')) {
            $manifest = @(Get-Content -LiteralPath (Join-Path $output.FullName 'binaries.json') -Raw | ConvertFrom-Json)
            if ($manifest.Count -ne 2 -or ($manifest.file -join ',') -cne 'gocluster_pgo.exe,peerdiag.exe') { throw 'Incorrect pair manifest.' }
            foreach ($item in $manifest) {
                $binary = Join-Path $output.FullName $item.file
                if ($item.sha256 -cne (Get-FileHash -LiteralPath $binary).Hash) { throw 'Manifest binary hash mismatch.' }
                $relative = [IO.Path]::GetRelativePath($root, $binary)
                $before[$relative] = $item.sha256
                if ($protected -notcontains $relative) { $protected += $relative }
            }
            $manifestRelative = [IO.Path]::GetRelativePath($root, (Join-Path $output.FullName 'binaries.json'))
            $before[$manifestRelative] = (Get-FileHash -LiteralPath (Join-Path $root $manifestRelative)).Hash
            if ($protected -notcontains $manifestRelative) { $protected += $manifestRelative }
        }
        $script:passed++
    }
}

try {
    Invoke-PGOPairFixture 'first-build-fails' 1
    Invoke-PGOPairFixture 'companion-build-fails' 2
    Invoke-PGOPairFixture 'complete-pairs' 0 2
    Write-Host "PASS $passed mocked PGO pair fixtures; no Go builds or workspace executable changes."
}
finally {
    Set-Location $originalLocation
    if ($originalExitCode) { $global:LASTEXITCODE = $savedExitCode } else { Remove-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue }
    $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $fixtureBase.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
        (Get-Item -LiteralPath $fixtureBase -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'Fixture cleanup path is outside its owned temporary directory.'
    }
    Remove-Item -LiteralPath $fixtureBase -Recurse -Force
}
