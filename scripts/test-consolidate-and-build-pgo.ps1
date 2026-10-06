<# Synthetic behavioral checks: no real Go builds or cluster launches. #>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$fixtureBase = Join-Path ([IO.Path]::GetTempPath()) ('pair-fixtures-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $fixtureBase
$originalLocation = Get-Location
$savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue | Select-Object Value
$originalGoEnvironment = @{}
foreach ($name in @('GOOS','GOARCH')) {
    $originalGoEnvironment[$name] = @{ Exists = Test-Path ('Env:' + $name); Value = [Environment]::GetEnvironmentVariable($name, 'Process') }
}
$passed = 0
function Invoke-PairFixture([string]$Mode, [int]$FailBuild, [bool]$FailMerge = $false, [int]$Runs = 1, [string]$EnvironmentState = 'absent') {
    foreach ($name in @('GOOS','GOARCH')) {
        if ($EnvironmentState -eq 'absent') { Remove-Item ('Env:' + $name) -ErrorAction SilentlyContinue }
        else { Set-Item ('Env:' + $name) $(if ($EnvironmentState -eq 'empty') { '' } else { 'ORIGINAL_' + $name }) }
    }
    $root = Join-Path $fixtureBase ([guid]::NewGuid().ToString('N'))
    $scripts = Join-Path $root 'scripts'
    $null = New-Item -ItemType Directory -Path $scripts -Force
    Copy-Item (Join-Path $PSScriptRoot 'build-executable-pair.ps1') $scripts
    Copy-Item (Join-Path $PSScriptRoot 'consolidate-and-build-pgo.ps1') $scripts
    $null = New-Item -ItemType Directory -Path (Join-Path $root 'logs')
    $buildRoot = Join-Path $root ('.tmp/' + $Mode)
    $prior = Join-Path $buildRoot 'prior'
    $null = New-Item -ItemType Directory -Path $prior -Force
    $protected = @{}
    foreach ($path in @((Join-Path $root 'gocluster.exe'), (Join-Path $root 'gocluster_pgo.exe'), (Join-Path $root 'peerdiag.exe'), (Join-Path $prior 'sentinel'))) {
        Set-Content -LiteralPath $path 'ORIGINAL'
        $protected[$path] = (Get-FileHash -LiteralPath $path).Hash
    }
    Set-Content (Join-Path $root 'logs/cpu-test.pprof') 'PROFILE'
    $state = @{ builds = 0; merges = 0; publishedBefore = 1 }
    function git { $global:LASTEXITCODE = 0; '0123456789ab' }
    function go {
        $global:LASTEXITCODE = 0
        if ($args[0] -eq 'tool') {
            $state.merges++
            if ($args[1] -ne 'pprof' -or $args[4] -ne (Join-Path $root 'gocluster.exe') -or $args[5] -ne (Join-Path $root 'logs/cpu-test.pprof')) { throw 'Wrong profile merge.' }
            if ($FailMerge) { $global:LASTEXITCODE = 1; return }
            Set-Content -LiteralPath $args[3].Substring(8) 'MERGED'
            return
        }
        if ($args[0] -ne 'build') { throw 'Unexpected Go call.' }
        $state.builds++
        if ($env:GOOS -ne 'windows' -or $env:GOARCH -ne 'amd64') { throw 'Incorrect operational target.' }
        $output = if ($state.builds -eq 1) { $args[3].Substring(3) } else { $args[4] }
        if ($state.builds -eq 1 -and (($Mode -eq 'ordinary' -and $args[1] -ne '-pgo=off') -or ($Mode -eq 'pgo' -and $args[1] -ne ('-pgo=' + (Join-Path $root 'logs/pgo-merged.pprof'))))) { throw 'Wrong PGO selection.' }
        $stage = [IO.Path]::GetDirectoryName($output)
        if ([IO.Path]::GetDirectoryName($stage) -ne $buildRoot -or -not [IO.Path]::GetFileName($stage).StartsWith('.stage-')) { throw 'Build escaped staging.' }
        $published = @(Get-ChildItem $buildRoot -Directory -Force | Where-Object Name -NotLike '.stage-*')
        if ($published.Count -ne $state.publishedBefore) { throw 'Early publication.' }
        foreach ($unsafe in @($root, $buildRoot, (Join-Path $buildRoot '../outside'))) {
            $refused = $false
            try { Assert-OwnedPairDirectory $unsafe } catch { $refused = $true }
            if (-not $refused) { throw 'Unsafe cleanup path accepted.' }
        }
        Set-Content -LiteralPath $output ('BUILD ' + $state.builds)
        if ($state.builds -eq $FailBuild) { $global:LASTEXITCODE = 1 }
    }
    for ($run = 0; $run -lt $Runs; $run++) {
        $state.builds = 0
        $state.merges = 0
        $location = (Get-Location).Path
        $goos = $env:GOOS
        $goarch = $env:GOARCH
        $hadGoos = Test-Path Env:GOOS
        $hadGoarch = Test-Path Env:GOARCH
        $caught = ''
        $result = $null
        $scriptName = if ($Mode -eq 'pgo') { 'consolidate-and-build-pgo.ps1' } else { 'build-executable-pair.ps1' }
        try { $result = & (Join-Path $scripts $scriptName) 6>$null } catch { $caught = $_.Exception.Message }
        $expected = if ($FailMerge) { 'pprof merge failed' } elseif ($FailBuild -eq 1) { 'go build failed' } elseif ($FailBuild -eq 2) { 'peer diagnostic companion build failed' } else { '' }
        if ($caught -cne $expected) { throw "Unexpected result: $caught (expected $expected)" }
        if ((Get-Location).Path -ne $location -or $env:GOOS -ne $goos -or $env:GOARCH -ne $goarch -or
            (Test-Path Env:GOOS) -ne $hadGoos -or (Test-Path Env:GOARCH) -ne $hadGoarch) { throw 'Caller state changed.' }
        $expectedBuilds = if ($FailMerge) { 0 } elseif ($FailBuild -eq 1) { 1 } else { 2 }
        if ($state.builds -ne $expectedBuilds -or $state.merges -ne $(if ($Mode -eq 'pgo') { 1 } else { 0 })) { throw 'Wrong build/merge count.' }
        foreach ($path in $protected.Keys) { if ((Get-FileHash -LiteralPath $path).Hash -ne $protected[$path]) { throw "Prior output changed: $path" } }
        $directories = @(Get-ChildItem $buildRoot -Directory -Force)
        if (@($directories | Where-Object Name -Like '.stage-*').Count) { throw 'Stage retained.' }
        if ($FailBuild -or $FailMerge) {
            if ($directories.Count -ne 1 -or $result) { throw 'Failed pair published.' }
        } else {
            $state.publishedBefore++
            if ($directories.Count -ne $state.publishedBefore -or @($result).Count -ne 1 -or $result.Mode -ne $Mode) { throw 'Bad publication result.' }
            $clusterName = if ($Mode -eq 'pgo') { 'gocluster_pgo.exe' } else { 'gocluster.exe' }
            if ($result.ClusterPath -ne (Join-Path $result.OutputDirectory $clusterName) -or $result.PeerDiagnosticPath -ne (Join-Path $result.OutputDirectory 'peerdiag.exe')) { throw 'Wrong pair paths.' }
            $manifest = @(Get-Content $result.ManifestPath -Raw | ConvertFrom-Json)
            if ($manifest.Count -ne 2 -or ($manifest.file -join ',') -ne "$clusterName,peerdiag.exe") { throw 'Wrong manifest.' }
            foreach ($entry in $manifest) {
                $path = Join-Path $result.OutputDirectory $entry.file
                if ((Get-FileHash $path).Hash -ne $entry.sha256) { throw 'Manifest mismatch.' }
                $protected[$path] = $entry.sha256
            }
            $protected[$result.ManifestPath] = (Get-FileHash $result.ManifestPath).Hash
        }
        $script:passed++
    }
}
try {
    foreach ($environmentState in @('absent','value','empty')) {
        foreach ($mode in @('pgo', 'ordinary')) {
            Invoke-PairFixture $mode 1 $false 1 $environmentState
            Invoke-PairFixture $mode 2 $false 1 $environmentState
            Invoke-PairFixture $mode 0 $false 2 $environmentState
        }
        Invoke-PairFixture 'pgo' 0 $true 1 $environmentState
    }
    Write-Host "PASS $passed isolated pair fixtures; no Go builds."
} finally {
    Set-Location $originalLocation
    foreach ($name in $originalGoEnvironment.Keys) {
        if ($originalGoEnvironment[$name].Exists) { Set-Item ('Env:' + $name) $originalGoEnvironment[$name].Value }
        else { Remove-Item ('Env:' + $name) -ErrorAction SilentlyContinue }
    }
    if ($savedExitCode) { $global:LASTEXITCODE = $savedExitCode.Value } else { Remove-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue }
    $absolute = [IO.Path]::GetFullPath($fixtureBase)
    $tempPrefix = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $absolute.StartsWith($tempPrefix, [StringComparison]::OrdinalIgnoreCase) -or (Get-Item $absolute -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Unsafe fixture cleanup.' }
    Remove-Item -LiteralPath $absolute -Recurse -Force
}
