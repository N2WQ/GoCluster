<#
.SYNOPSIS
    Prove that the actual extracted Windows package can start and accept Telnet.
.DESCRIPTION
    Runs with private config and fresh runtime data. On a networked machine a
    separate helper holds temporary firewall blocks scoped to the extracted
    executable. Only that helper requests elevation; it never executes the ZIP.
    Any isolation, startup, verification or cleanup error rejects the package.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ZipPath,
    [Parameter(Mandatory)][string]$PackageDirectoryName
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if ([Environment]::OSVersion.Platform -ne 'Win32NT') { throw 'Release smoke requires native Windows.' }
if ($PackageDirectoryName -ne [IO.Path]::GetFileName($PackageDirectoryName) -or $PackageDirectoryName -in '.', '..') {
    throw 'Invalid package directory for the release smoke test.'
}

function New-LocalListener {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Server.ExclusiveAddressUse = $true
    $listener.Start()
    return $listener
}

function Assert-SmokeListener([Diagnostics.Process]$Process, [int]$Port, [string]$Prompt, [datetime]$Deadline) {
    # Confirm the kernel's PID -> listener binding before connecting. A random
    # prompt from the private config also authenticates the server's response.
    $bound = @()
    while ([datetime]::UtcNow -lt $Deadline) {
        if ($Process.HasExited) { throw "Extracted node exited early with code $($Process.ExitCode)." }
        $bound = @(Get-NetTCPConnection -State Listen -OwningProcess $Process.Id -LocalPort $Port -ErrorAction SilentlyContinue)
        if ($bound.Count -gt 0) { break }
        Start-Sleep -Milliseconds 200
    }
    if ($bound.Count -eq 0) { throw "Timed out waiting for the extracted node to listen on $Port." }
    $client = [Net.Sockets.TcpClient]::new()
    try {
        if (-not $client.ConnectAsync([Net.IPAddress]::Loopback, $Port).Wait(2000)) { throw 'Loopback connection timed out.' }
        $stream = $client.GetStream()
        $stream.ReadTimeout = 3000
        $buffer = [byte[]]::new(512)
        $greeting = ''
        while ($greeting.Length -lt 8192 -and [datetime]::UtcNow -lt $Deadline) {
            $count = $stream.Read($buffer, 0, $buffer.Length)
            if ($count -eq 0) { break }
            $greeting += [Text.Encoding]::ASCII.GetString($buffer, 0, $count)
            if ($greeting.Contains($Prompt)) { return }
        }
        throw 'The extracted Telnet listener did not return its unique login prompt.'
    } finally { $client.Dispose() }
}

function Assert-SmokeModels([Diagnostics.Process]$Process, [string]$LogPath, [datetime]$Deadline) {
    $expected = @('Loaded [1-9][0-9]* spotter reliability weights from ',
        'Loaded [1-9][0-9]* CW spotter reliability weights from ',
        'Loaded [1-9][0-9]* RTTY spotter reliability weights from ',
        'Loaded call correction confusion model from ')
    do {
        if ($Process.HasExited) { throw "Extracted node exited with code $($Process.ExitCode)." }
        if ([IO.File]::Exists($LogPath)) {
            $log = Get-Content -LiteralPath $LogPath -Raw
            if (@($expected | Where-Object { $log -cnotmatch $_ }).Count -eq 0) { return }
        }
        Start-Sleep -Milliseconds 200
    } while ([datetime]::UtcNow -lt $Deadline)
    throw 'The extracted node did not log three nonempty reliability tables and a loaded confusion model.'
}

$id = [guid]::NewGuid().ToString('N')
$temp = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'Temp'
$root = Join-Path $temp "gocluster-release-smoke-$id"
$pkg = Join-Path $root $PackageDirectoryName
$exe = Join-Path $pkg 'gocluster.exe'
$cfg = Join-Path $root 'smoke-config'
$logFile = Join-Path $pkg 'data/release-smoke/system/system.log'
$guard = $null; $child = $null; $ready = $null; $done = $null
$probe = $null; $sink = $null; $stdout = $null; $stderr = $null
$outCopy = $null; $errCopy = $null
$passed = $false
$prompt = "release-smoke-$id> "
$null = New-Item -Path $root -ItemType Directory
try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [IO.Compression.ZipFile]::ExtractToDirectory([IO.Path]::GetFullPath($ZipPath), $root)
    if (-not [IO.File]::Exists($exe)) { throw "Missing extracted executable: $exe" }
    Copy-Item -LiteralPath (Join-Path $pkg 'data/config') -Destination $cfg -Recurse
    $probe = New-LocalListener
    $sink = New-LocalListener
    $port = $probe.LocalEndpoint.Port
    $goEnv = @{}
    Push-Location (Split-Path -Parent $PSScriptRoot)
    try {
        foreach ($key in 'GOOS', 'GOARCH', 'CGO_ENABLED', 'GOPROXY', 'GOSUMDB', 'GONOPROXY', 'GOTOOLCHAIN') {
            $goEnv[$key] = [Environment]::GetEnvironmentVariable($key)
            [Environment]::SetEnvironmentVariable($key, $null)
        }
        $env:CGO_ENABLED = '0'
        # Packaging has already built these dependencies. Even the test setup
        # must fail locally on a cache miss instead of fetching tools or modules.
        $env:GOPROXY = 'off'
        $env:GOSUMDB = 'off'
        $env:GONOPROXY = 'none'
        $env:GOTOOLCHAIN = 'local'
        & go run ./cmd/release_smoke_config -config $cfg -port $port -sink $sink.LocalEndpoint.Port -prompt $prompt
        if ($LASTEXITCODE -ne 0) { throw 'Unable to prepare the smoke configuration.' }
        & go run ./cmd/release_assets -check-smoke-config $cfg
        if ($LASTEXITCODE -ne 0) { throw 'Unsafe or unclassified smoke configuration. The executable was not started.' }
    } finally {
        foreach ($key in $goEnv.Keys) { [Environment]::SetEnvironmentVariable($key, $goEnv[$key]) }
        Pop-Location
    }

    # A disconnected guest with only loopback has no external network endpoint.
    # Count even down adapters; a dormant adapter on a host can be brought up.
    $adapters = @([Net.NetworkInformation.NetworkInterface]::GetAllNetworkInterfaces() |
        Where-Object { $_.NetworkInterfaceType -ne 'Loopback' })
    if ($adapters.Count -gt 0) {
        $name = "GoCluster-release-smoke-$id"
        $ready = [Threading.EventWaitHandle]::new($false, 'ManualReset', "Local\$name-ready")
        $done = [Threading.EventWaitHandle]::new($false, 'ManualReset', "Local\$name-done")
        $helper = Join-Path $PSScriptRoot 'release-smoke-firewall.ps1'
        $shell = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell/v1.0/powershell.exe'
        $argsForGuard = @('-NoProfile', '-NonInteractive', '-File', ('"' + $helper + '"'),
            '-RunId', $id, '-Executable', ('"' + $exe + '"'), '-OwnerPid', $PID)
        $launch = @{ FilePath = $shell; ArgumentList = $argsForGuard; PassThru = $true; WindowStyle = 'Hidden' }
        $principal = [Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())
        if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
            $launch.Verb = 'RunAs'
            Write-Host 'Requesting elevation for temporary firewall rules scoped to the extracted smoke executable.'
        }
        $guard = Start-Process @launch
        $deadline = [datetime]::UtcNow.AddSeconds(45)
        while (-not $ready.WaitOne(200)) {
            if ($guard.HasExited -or [datetime]::UtcNow -ge $deadline) {
                throw 'Could not establish the program-specific firewall rules. The smoke executable was not started.'
            }
        }
    }
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $exe
    $info.WorkingDirectory = $pkg
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    # Start from a minimal environment. An inherited temp, profile, proxy,
    # credential or diagnostics variable could otherwise refer to the live node.
    $info.EnvironmentVariables.Clear()
    $systemRoot = [IO.Directory]::GetParent([Environment]::SystemDirectory).FullName
    $info.EnvironmentVariables['SystemRoot'] = $systemRoot
    $info.EnvironmentVariables['WINDIR'] = $systemRoot
    $info.EnvironmentVariables['SystemDrive'] = [IO.Path]::GetPathRoot($systemRoot).TrimEnd('\')
    $homeDir = Join-Path $root 'user'
    $tempDir = Join-Path $root 'temp'
    $null = [IO.Directory]::CreateDirectory($homeDir)
    $null = [IO.Directory]::CreateDirectory($tempDir)
    foreach ($key in 'USERPROFILE', 'HOME', 'APPDATA', 'LOCALAPPDATA', 'PROGRAMDATA', 'ALLUSERSPROFILE') {
        $info.EnvironmentVariables[$key] = $homeDir
    }
    $info.EnvironmentVariables['TEMP'] = $tempDir
    $info.EnvironmentVariables['TMP'] = $tempDir
    $info.EnvironmentVariables['DXC_CONFIG_PATH'] = $cfg
    $info.EnvironmentVariables['PATH'] = $pkg
    $info.EnvironmentVariables['NO_PROXY'] = '127.0.0.1,localhost'
    $stdout = [IO.File]::Create((Join-Path $root 'stdout.log'))
    $stderr = [IO.File]::Create((Join-Path $root 'stderr.log'))
    $probe.Stop()
    $child = [Diagnostics.Process]::Start($info)
    $outCopy = $child.StandardOutput.BaseStream.CopyToAsync($stdout)
    $errCopy = $child.StandardError.BaseStream.CopyToAsync($stderr)
    $deadline = [datetime]::UtcNow.AddSeconds(60)
    Assert-SmokeListener $child $port $prompt $deadline
    Assert-SmokeModels $child $logFile $deadline
    if ($guard -and $guard.HasExited) { throw 'Firewall isolation ended before the startup checks completed.' }
    $smokePid = $child.Id
    $passed = $true
} finally {
    try {
        if ($child) {
            if (-not $child.HasExited) { $child.Kill() }
            if (-not $child.WaitForExit(10000)) { throw 'Timed out retiring the extracted smoke executable.' }
            if ($outCopy -and -not $outCopy.Wait(5000)) { throw 'Timed out draining smoke stdout.' }
            if ($errCopy -and -not $errCopy.Wait(5000)) { throw 'Timed out draining smoke stderr.' }
            $child.Dispose()
        }
    } finally {
        if ($stdout) { $stdout.Dispose() }; if ($stderr) { $stderr.Dispose() }
        if ($sink) { $sink.Stop() }; if ($probe) { $probe.Stop() }
        if ($done) { [void]$done.Set() }
        try {
            if ($child -and -not $passed) {
                foreach ($path in @($logFile, (Join-Path $root 'stderr.log'))) {
                    if ([IO.File]::Exists($path)) { Get-Content -LiteralPath $path -Tail 20 | Out-Host }
                }
            }
            if ($guard) {
                if (-not $guard.WaitForExit(30000) -or $guard.ExitCode -ne 0) {
                    throw "Smoke firewall helper failed. Check rules GoCluster-release-smoke-$id-Inbound and -Outbound."
                }
                $guard.Dispose()
            }
        } finally {
            if ($done) { $done.Dispose() }; if ($ready) { $ready.Dispose() }
            $actual = (Resolve-Path -LiteralPath $root).ProviderPath
            if ($actual -ine [IO.Path]::GetFullPath((Join-Path $temp "gocluster-release-smoke-$id"))) {
                throw "Unexpected cleanup path: $actual"
            }
            Remove-Item -LiteralPath $actual -Recurse -Force
        }
    }
}
Write-Host "Release smoke passed: PID $smokePid answered on loopback port $port and loaded all four model assets."
