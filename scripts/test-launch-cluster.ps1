<# Synthetic builders and executable functions; no cluster is launched. #>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$fixtureBase = Join-Path ([IO.Path]::GetTempPath()) ('launcher-fixtures-' + [guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $fixtureBase
$originalLocation = Get-Location
$originalConfig = $env:DXC_CONFIG_PATH
$hadOriginalConfig = Test-Path Env:DXC_CONFIG_PATH
$savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue | Select-Object Value
$passed = 0
function Invoke-LauncherFixture([string]$Name, [string]$Profiles, [string]$Failure = '', [string]$ConfigState = 'value') {
    $root = Join-Path $fixtureBase $Name
    $scripts = Join-Path $root 'scripts'
    $null = New-Item -ItemType Directory -Path $scripts -Force
    Copy-Item (Join-Path $PSScriptRoot '../launch-cluster.ps1') (Join-Path $root 'launch-cluster.ps1')
    if ($Profiles -ne 'absent') {
        $null = New-Item -ItemType Directory -Path (Join-Path $root 'logs')
        if ($Profiles -eq 'present') { Set-Content (Join-Path $root 'logs/cpu-one.pprof') 'PROFILE' }
        if ($Profiles -eq 'directory') { $null = New-Item -ItemType Directory -Path (Join-Path $root 'logs/cpu-dir.pprof') }
    }
    $expectedMode = if ($Profiles -eq 'present') { 'pgo' } else { 'ordinary' }
    $freshRoot = Join-Path $root ('.tmp/' + $expectedMode + '/fresh')
    $null = New-Item -ItemType Directory -Path $freshRoot -Force
    $clusterPath = Join-Path $freshRoot 'gocluster.exe'
    $peerPath = Join-Path $freshRoot 'peerdiag.exe'
    $protected = @{}
    foreach ($path in @((Join-Path $root 'gocluster.exe'), (Join-Path $root 'gocluster_pgo.exe'), (Join-Path $root 'peerdiag.exe'), $clusterPath, $peerPath)) {
        Set-Content $path 'SENTINEL'
        $protected[$path] = (Get-FileHash $path).Hash
    }
    $fixtureState = @{ builds = 0; versions = 0; launches = 0; mode = ''; cluster = $clusterPath; peer = $peerPath; failure = $Failure; root = $root }
    foreach ($mode in @('pgo','ordinary')) {
        $name = if ($mode -eq 'pgo') { 'consolidate-and-build-pgo.ps1' } else { 'build-executable-pair.ps1' }
        $builder = @'
$fixtureState.builds++
$fixtureState.mode = 'MODE'
if ($fixtureState.failure -eq 'build') { throw 'fixture build failed' }
if ($fixtureState.failure -eq 'incomplete') { return }
[pscustomobject]@{ ClusterPath=$fixtureState.cluster; PeerDiagnosticPath=$fixtureState.peer }
'@
        $builder.Replace('MODE',$mode) | Set-Content (Join-Path $scripts $name)
    }
    $executable = {
        if ($env:DXC_CONFIG_PATH -ne (Join-Path $fixtureState.root 'data/config')) { throw 'Wrong launch configuration.' }
        if ((Get-Location).Path -ne $fixtureState.root) { throw 'Wrong launch location.' }
        $global:LASTEXITCODE = 0
        if ($args.Count -eq 1 -and $args[0] -eq '--version') {
            $fixtureState.versions++
            if ($fixtureState.failure -eq 'version') { $global:LASTEXITCODE = 7 }
        } elseif ($args.Count -eq 0) {
            $fixtureState.launches++
            if ($fixtureState.failure -eq 'runtime') { $global:LASTEXITCODE = 8 }
        } else { throw 'Unexpected executable arguments.' }
    }
    Set-Item -Path ('Function:global:' + $clusterPath) -Value $executable
    foreach ($stale in @('gocluster.exe','gocluster_pgo.exe')) { Set-Item -Path ('Function:global:' + (Join-Path $root $stale)) -Value { throw 'Stale root executable selected.' } }
    try {
        if ($ConfigState -eq 'absent') { Remove-Item Env:DXC_CONFIG_PATH -ErrorAction SilentlyContinue }
        else { $env:DXC_CONFIG_PATH = if ($ConfigState -eq 'empty') { '' } else { 'ORIGINAL_CONFIG' } }
        $hadConfig = Test-Path Env:DXC_CONFIG_PATH
        $configValue = $env:DXC_CONFIG_PATH
        $callerLocation = (Get-Location).Path
        $caught = ''
        try { & (Join-Path $root 'launch-cluster.ps1') 6>$null } catch { $caught = $_.Exception.Message }
        $expectedError = switch ($Failure) {
            'build' { 'fixture build failed' }
            'incomplete' { 'Builder did not return a complete executable pair.' }
            'version' { 'Fresh cluster version probe failed.' }
            'runtime' { 'Fresh cluster exited with code 8.' }
            default { '' }
        }
        if ($caught -ne $expectedError) { throw "${Name}: unexpected error '$caught'" }
        if ($fixtureState.builds -ne 1 -or $fixtureState.mode -ne $expectedMode) { throw 'Launcher selected wrong route or built twice.' }
        $expectedVersions = if ($Failure -in @('build','incomplete')) { 0 } else { 1 }
        $expectedLaunches = if ($Failure -in @('build','incomplete','version')) { 0 } else { 1 }
        if ($fixtureState.versions -ne $expectedVersions -or $fixtureState.launches -ne $expectedLaunches) { throw 'Wrong fresh executable sequence.' }
        if ((Get-Location).Path -ne $callerLocation -or $env:DXC_CONFIG_PATH -ne $configValue -or
            (Test-Path Env:DXC_CONFIG_PATH) -ne $hadConfig) { throw 'Caller location/config not restored.' }
        foreach ($path in $protected.Keys) { if ((Get-FileHash $path).Hash -ne $protected[$path]) { throw 'Prior executable changed.' } }
        $script:passed++
    } finally {
        foreach ($path in @($clusterPath, (Join-Path $root 'gocluster.exe'), (Join-Path $root 'gocluster_pgo.exe'))) { Remove-Item -LiteralPath ('Function:global:' + $path) }
    }
}
try {
    Invoke-LauncherFixture 'absent-logs' 'absent'
    Invoke-LauncherFixture 'empty-logs' 'empty' '' 'absent'
    Invoke-LauncherFixture 'directory-not-profile' 'directory'
    Invoke-LauncherFixture 'profiled' 'present'
    Invoke-LauncherFixture 'ordinary-build-fails' 'empty' 'build'
    Invoke-LauncherFixture 'pgo-build-fails' 'present' 'build'
    Invoke-LauncherFixture 'incomplete-result' 'empty' 'incomplete'
    Invoke-LauncherFixture 'version-fails' 'empty' 'version'
    Invoke-LauncherFixture 'runtime-fails' 'empty' 'runtime'
    foreach ($configState in @('absent','empty')) {
        Invoke-LauncherFixture ($configState + '-success') 'empty' '' $configState
        foreach ($failure in @('build','version','runtime')) {
            Invoke-LauncherFixture ($configState + '-' + $failure) 'empty' $failure $configState
        }
    }
    $strictRoot = Join-Path $fixtureBase 'strict'
    $null = New-Item -ItemType Directory -Path (Join-Path $strictRoot 'scripts') -Force
    $null = New-Item -ItemType Directory -Path (Join-Path $strictRoot 'logs')
    Copy-Item (Join-Path $PSScriptRoot 'consolidate-and-build-pgo.ps1') (Join-Path $strictRoot 'scripts')
    $caught = ''
    try { & (Join-Path $strictRoot 'scripts/consolidate-and-build-pgo.ps1') } catch { $caught = $_.Exception.Message }
    if ($caught -notlike 'No cpu-*.pprof files found*') { throw 'Standalone PGO no-profile behavior changed.' }
    $passed++
    Write-Host "PASS $passed launcher fixtures; no cluster launched."
} finally {
    if ($hadOriginalConfig) { $env:DXC_CONFIG_PATH = $originalConfig } else { Remove-Item Env:DXC_CONFIG_PATH -ErrorAction SilentlyContinue }
    Set-Location $originalLocation
    if ($savedExitCode) { $global:LASTEXITCODE = $savedExitCode.Value } else { Remove-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue }
    $absolute = [IO.Path]::GetFullPath($fixtureBase)
    $tempPrefix = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $absolute.StartsWith($tempPrefix, [StringComparison]::OrdinalIgnoreCase) -or (Get-Item $absolute -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Unsafe fixture cleanup.' }
    Remove-Item -LiteralPath $absolute -Recurse -Force
}
