<#
.SYNOPSIS
    Exercise release safety against isolated Git repositories and native failures.
.DESCRIPTION
    Runs the actual release script and extracted production helpers. Git reads
    use disposable real repositories; publication and Go commands are substitutes
    that obtain their exit status from a real native child. No GitHub calls occur.
    A separate trivial Go module checks the actual module/LF contract.
.NOTES
    Run with PowerShell 7 and Windows PowerShell 5.1. Requires Git and Go.
    Creates local commits only in temporary repositories and deletes its own
    temporary fixtures. Does not build or publish the live repository.
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:RealGit = (Get-Command git.exe -ErrorAction Stop).Source
$script:RealGo = (Get-Command go.exe -ErrorAction Stop).Source
Import-Module Microsoft.PowerShell.Archive -Scope Global
$script:Source = Join-Path $PSScriptRoot 'create-release.ps1'
$script:FixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-release-safety-' + [guid]::NewGuid().ToString('N'))
$script:Passed = 0
$script:Skipped = 0
$script:OriginalLocation = Get-Location
$script:OriginalGOOS = $env:GOOS
$script:OriginalGOARCH = $env:GOARCH
$script:OriginalGHRepo = $env:GH_REPO
$script:OriginalGHHost = $env:GH_HOST
New-Item -ItemType Directory -Path $script:FixtureRoot | Out-Null

function Assert-Fixture([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}
function Write-FixtureFile([string]$Path, [string]$Text) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force | Out-Null
    [IO.File]::WriteAllText($Path, $Text, [Text.UTF8Encoding]::new($false))
}
function Invoke-FixtureGit([string]$Repo, [string[]]$Arguments) {
    $ErrorActionPreference = 'Continue'
    $text = @(& $script:RealGit -C $Repo @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "Fixture Git failed: $($text -join ' ')" }
    return ($text -join "`n")
}
function New-ReleaseFixture {
    $repo = Join-Path $script:FixtureRoot ([guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path (Join-Path $repo 'scripts') -Force | Out-Null
    Copy-Item -LiteralPath $script:Source -Destination (Join-Path $repo 'scripts/create-release.ps1')
    Write-FixtureFile (Join-Path $repo '.gitignore') ".tmp/`nready_to_run/`ngocluster-windows-amd64.zip`n"
    Write-FixtureFile (Join-Path $repo 'main.go') "package main`nfunc main() {}`n"
    Write-FixtureFile (Join-Path $repo 'go.mod') "module example.test/releasefixture`n`ngo 1.26`n"
    Write-FixtureFile (Join-Path $repo 'data/config/app.yaml') 'node_id: "N0CALL-1"'
    Write-FixtureFile (Join-Path $repo 'data/config/ingest.yaml') "callsign: `"N0CALL-2`"`nhost: `"telnet.reversebeacon.net`"`n"
    Write-FixtureFile (Join-Path $repo 'data/config/peering.yaml') "local_callsign: `"N0CALL-3`"`nenabled: false`nhost: `"peer1.example.invalid`"`npassword: `"`"`nlogin_callsign: `"N0CALL-4`"`nremote_callsign: `"N0PEER-1`"`n"
    Write-FixtureFile (Join-Path $repo 'data/config/reputation.yaml') "ipinfo_download_enabled: false`nipinfo_download_token: `"REPLACE_WITH_IPINFO_TOKEN`"`nipinfo_api_enabled: false`nipinfo_api_token: `"`"`n"
    foreach ($relative in @('docs/release/README.md.template', 'docs/OPERATOR_GUIDE.md', 'third_party/go-sqlite3/LICENSE', 'third_party/go-sqlite3/engine/LICENSE', 'third_party/go-sqlite3/provenance/GO-LICENSE.txt')) {
        Write-FixtureFile (Join-Path $repo $relative) "public fixture: $relative`n"
    }
    Invoke-FixtureGit $repo @('init', '-q') | Out-Null
    Invoke-FixtureGit $repo @('config', 'user.name', 'Release safety fixture') | Out-Null
    Invoke-FixtureGit $repo @('config', 'user.email', 'release-fixture@example.invalid') | Out-Null
    Invoke-FixtureGit $repo @('config', 'core.hooksPath', (Join-Path $repo '.git/disabled-fixture-hooks')) | Out-Null
    Invoke-FixtureGit $repo @('config', 'commit.gpgsign', 'false') | Out-Null
    Invoke-FixtureGit $repo @('config', 'core.autocrlf', 'false') | Out-Null
    Invoke-FixtureGit $repo @('add', '.') | Out-Null
    Invoke-FixtureGit $repo @('commit', '-q', '-m', 'isolated fixture') | Out-Null
    Invoke-FixtureGit $repo @('remote', 'add', 'origin', 'https://github.com/fixture/target.git') | Out-Null
    return $repo
}
function Reset-ReleaseNative([string]$Repo, [string]$Fault = '') {
    $fixtureCommit = Invoke-FixtureGit $Repo @('rev-parse', 'HEAD')
    $fixtureTag = (Get-Date).ToUniversalTime().ToString('yyMMdd') + 'r' + $fixtureCommit.Substring($fixtureCommit.Length - 4)
    $global:ReleaseSafetyFixture = [pscustomobject]@{
        Repo = $Repo; RealGit = $script:RealGit; Fault = $Fault; FaultCode = 19; ReleaseTag = $fixtureTag
        Calls = [Collections.Generic.List[object]]::new(); SuccessStderr = $false
        LocalCode = 2; RemoteCode = 2; Listing = 'absent'; Permission = 'WRITE'
        Drift = ''; FinalDrift = ''; HeadCalls = 0; BuildSerial = [guid]::NewGuid().ToString('N')
    }
}
function New-LegacyReleaseOutputs([string]$Repo, [string]$Shape) {
    $stage = Join-Path $Repo 'ready_to_run'
    $zip = Join-Path $Repo 'gocluster-windows-amd64.zip'
    if ($Shape -in @('pair', 'stage-only')) {
        Write-FixtureFile (Join-Path $stage 'operator-state/private.txt') 'legacy private operator bytes'
        New-Item -ItemType Directory -Path (Join-Path $stage 'empty-runtime-directory') | Out-Null
    }
    if ($Shape -in @('pair', 'zip-only')) {
        $seed = Join-Path $Repo '.tmp/legacy-archive-input.txt'
        Write-FixtureFile $seed 'legacy archive private bytes'
        Microsoft.PowerShell.Archive\Compress-Archive -LiteralPath $seed -DestinationPath $zip
        Remove-Item -LiteralPath $seed
    }
    return [pscustomobject]@{
        StageRoot = $stage; ZipPath = $zip
        StageBytes = if (Test-Path -LiteralPath $stage) { Get-FixtureBytes $stage } else { $null }
        ZipHash = if (Test-Path -LiteralPath $zip) { (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash } else { '' }
    }
}
function Invoke-ReleaseNativeStatus([string]$Operation, [string[]]$Arguments, [int]$Code = 0, [string]$Output = '') {
    $state = $global:ReleaseSafetyFixture
    $state.Calls.Add([pscustomobject]@{ Operation = $Operation; Arguments = @($Arguments); GOOS = $env:GOOS; GOARCH = $env:GOARCH })
    if ($state.Fault -ceq $Operation) { $Code = $state.FaultCode; $Output = '' }
    if ($Output) { Write-Output $Output }
    if ($state.SuccessStderr -and $Operation.StartsWith('go-') -and $Code -eq 0) {
        & $env:ComSpec /d /c "echo successful native diagnostic 1>&2 & exit /b $Code"
    } else { & $env:ComSpec /d /c "exit /b $Code" }
}
function global:git {
    $nativeArgs = @($args | ForEach-Object { [string]$_ })
    $state = $global:ReleaseSafetyFixture
    $operationArgs = $nativeArgs
    if ($operationArgs.Count -ge 2 -and $operationArgs[0] -eq '-C') { $operationArgs = @($operationArgs | Select-Object -Skip 2) }
    $op = switch ($operationArgs[0]) {
        'show-ref' { 'git-local' }
        'ls-remote' { 'git-remote' }
        'tag' { 'git-tag' }
        'push' { 'git-push' }
        'status' { 'git-status' }
        'ls-files' { 'git-files' }
        'remote' { 'git-target' }
        'rev-parse' {
            if ($operationArgs -contains '--show-toplevel') { 'git-root' }
            elseif ($operationArgs -contains '--short=12') { 'git-abbrev' }
            else {
                $state.HeadCalls++
                if ($state.HeadCalls -eq 3 -and $state.FinalDrift) {
                    switch ($state.FinalDrift) {
                        'tracked' { [IO.File]::AppendAllText((Join-Path $state.Repo 'main.go'), "`n// final tracked edit") }
                        'untracked' { [IO.File]::WriteAllText((Join-Path $state.Repo 'custom-out/final-unrelated.txt'), 'final unrelated file beside generated ZIP') }
                        'head' {
                            & $state.RealGit -C $state.Repo commit --allow-empty -q -m 'fixture final HEAD drift'
                            if ($LASTEXITCODE -ne 0) { throw 'Unable to stimulate final HEAD drift.' }
                        }
                    }
                }
                'git-head'
            }
        }
        default { throw "Unplanned Git operation: $($nativeArgs -join ' ')" }
    }
    if ($op -eq 'git-local') { Invoke-ReleaseNativeStatus $op $nativeArgs $state.LocalCode; return }
    if ($op -eq 'git-remote') { Invoke-ReleaseNativeStatus $op $nativeArgs $state.RemoteCode; return }
    if ($op -in @('git-tag', 'git-push') -or $state.Fault -ceq $op) { Invoke-ReleaseNativeStatus $op $nativeArgs; return }
    $state.Calls.Add([pscustomobject]@{ Operation = $op; Arguments = $nativeArgs; GOOS = $env:GOOS; GOARCH = $env:GOARCH })
    & $state.RealGit @nativeArgs
}
function global:go {
    $nativeArgs = @($args | ForEach-Object { [string]$_ })
    $state = $global:ReleaseSafetyFixture
    $op = if ($nativeArgs[0] -eq 'mod') { 'go-tidy' }
        elseif ($nativeArgs -contains './cmd/codemap') { 'go-map' }
        elseif ($nativeArgs -contains './cmd/release_readme') { 'go-readme' }
        elseif ($nativeArgs -contains './cmd/peerdiag') { 'go-build-peer' }
        elseif ($nativeArgs[0] -eq 'build') { 'go-build-main' }
        else { throw "Unplanned Go operation: $($nativeArgs -join ' ')" }
    if ($state.Fault -cne $op) {
        if ($op -eq 'go-readme') {
            $out = [Array]::IndexOf($nativeArgs, '-out')
            [IO.File]::WriteAllText($nativeArgs[$out + 1], 'rendered public fixture README')
        } elseif ($op.StartsWith('go-build-')) {
            $out = [Array]::IndexOf($nativeArgs, '-o')
            [IO.File]::WriteAllText($nativeArgs[$out + 1], "$op-$($state.BuildSerial)")
            if ($op -eq 'go-build-peer') {
                if ($state.Fault -eq 'marker') {
                    New-Item -ItemType Directory -Path (Join-Path (Split-Path -Parent $nativeArgs[$out + 1]) '.gocluster-release-owner.json') | Out-Null
                }
                switch ($state.Drift) {
                    'tracked' { [IO.File]::AppendAllText((Join-Path $state.Repo 'main.go'), "`n// unrelated drift") }
                    'untracked' { [IO.File]::WriteAllText((Join-Path $state.Repo 'unrelated.txt'), 'unrelated drift') }
                    'head' {
                        & $state.RealGit -C $state.Repo commit --allow-empty -q -m 'fixture HEAD drift'
                        if ($LASTEXITCODE -ne 0) { throw 'Unable to stimulate HEAD drift.' }
                    }
                }
            }
        }
    }
    Invoke-ReleaseNativeStatus $op $nativeArgs
}
function global:gh {
    $nativeArgs = @($args | ForEach-Object { [string]$_ })
    $state = $global:ReleaseSafetyFixture
    $output = ''
    $op = switch ($nativeArgs[0]) {
        'auth' { 'gh-auth' }
        'repo' {
            $output = @{ nameWithOwner = 'fixture/target'; viewerPermission = $state.Permission } | ConvertTo-Json -Compress
            'gh-repo'
        }
        'api' {
            $page = if ($nativeArgs[-1] -match 'page=(\d+)$') { [int]$Matches[1] } else { throw 'Missing explicit listing page.' }
            switch ($state.Listing) {
                'absent' { $output = '[]' }
                'duplicate' { $output = '[{"tag_name":"' + $state.ReleaseTag + '","draft":false}]' }
                'draft' { $output = '[{"tag_name":"' + $state.ReleaseTag + '","draft":true}]' }
                'late-draft' {
                    if ($page -eq 1) { $output = (@(1..100 | ForEach-Object { @{ tag_name = "other$_"; draft = $false } }) | ConvertTo-Json -Compress) }
                    else { $output = '[{"tag_name":"' + $state.ReleaseTag + '","draft":true}]' }
                }
                'malformed' { $output = '{"message":"not a release list"}' }
                'invalid-entry' { $output = '[{"tag_name":"x","draft":"false"}]' }
                default { throw 'Unplanned release listing fixture.' }
            }
            "gh-page$page"
        }
        'release' { 'gh-create' }
        default { throw "Unplanned GitHub operation: $($nativeArgs -join ' ')" }
    }
    Invoke-ReleaseNativeStatus $op $nativeArgs 0 $output
}
function global:Compress-Archive {
    param([string]$LiteralPath, [string]$DestinationPath)
    $state = $global:ReleaseSafetyFixture
    $state.Calls.Add([pscustomobject]@{ Operation = 'archive'; Arguments = @($LiteralPath, $DestinationPath); GOOS = $env:GOOS; GOARCH = $env:GOARCH })
    if ($state.Fault -eq 'archive') { throw 'Injected archive preparation failed.' }
    Microsoft.PowerShell.Archive\Compress-Archive -LiteralPath $LiteralPath -DestinationPath $DestinationPath
}
function Invoke-ActualRelease([string]$Repo, [hashtable]$Parameters, [string]$ExpectedFailure = '') {
    $beforeLocation = (Get-Location).Path
    $beforeOS = $env:GOOS; $beforeArch = $env:GOARCH
    $beforeOSExists = Test-Path Env:GOOS; $beforeArchExists = Test-Path Env:GOARCH
    $errorSeen = $null
    try { & (Join-Path $Repo 'scripts/create-release.ps1') @Parameters 6>$null | Out-Null }
    catch { $errorSeen = $_ }
    Assert-Fixture ((Get-Location).Path -ceq $beforeLocation) 'Caller location was not restored.'
    Assert-Fixture ((Test-Path Env:GOOS) -eq $beforeOSExists -and (Test-Path Env:GOARCH) -eq $beforeArchExists) 'Caller environment existence was not restored.'
    Assert-Fixture ($env:GOOS -ceq $beforeOS -and $env:GOARCH -ceq $beforeArch) 'Caller environment values were not restored.'
    if ($ExpectedFailure) {
        Assert-Fixture ($null -ne $errorSeen) "Expected refusal: $ExpectedFailure"
        Assert-Fixture ($errorSeen.Exception.Message -match $ExpectedFailure) "Wrong refusal; expected $ExpectedFailure; got $($errorSeen.Exception.Message)"
    } elseif ($null -ne $errorSeen) { throw $errorSeen }
}
function Import-ReleaseHelpers {
    $tokens = $null; $errors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile($script:Source, [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw ($errors | Out-String) }
    foreach ($definition in $ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $true)) {
        . ([scriptblock]::Create($definition.Extent.Text.Replace(('function ' + $definition.Name), ('function global:' + $definition.Name))))
    }
}
function Expect-ReleaseRefusal([scriptblock]$Action, [string]$Pattern) {
    $caught = $null
    try { & $Action | Out-Null } catch { $caught = $_ }
    Assert-Fixture ($null -ne $caught) "Expected refusal matching $Pattern"
    Assert-Fixture ($caught.Exception.Message -match $Pattern) "Wrong refusal: $($caught.Exception.Message); expected $Pattern"
}
function Get-FixtureBytes([string]$Root) {
    $inventory = @{}
    foreach ($item in Get-ChildItem -LiteralPath $Root -Recurse -Force) {
        if ($item.FullName -match '[\\/]\.git(?:[\\/]|$)') { continue }
        $value = if ($item.PSIsContainer) { 'directory' } else { (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash }
        $inventory[$item.FullName.Substring($Root.Length)] = $value
    }
    return $inventory
}
function Assert-FixtureBytes([string]$Root, [hashtable]$Before) {
    $after = Get-FixtureBytes $Root
    Assert-Fixture ($after.Count -eq $Before.Count) 'Unexpected files added or removed.'
    foreach ($key in $Before.Keys) { Assert-Fixture ($after[$key] -ceq $Before[$key]) "Fixture bytes changed: $key" }
}
function Run-ReleaseCase([string]$Name, [scriptblock]$Body) {
    $script:CurrentCaseSkipped = $false
    try {
        & $Body
        if (-not $script:CurrentCaseSkipped) { $script:Passed++; Write-Host "PASS $Name" }
    }
    catch { throw "Release safety fixture '$Name' failed: $($_.Exception.Message)" }
}
function Assert-NoReleaseAfter([string]$Operation) {
    $ops = @($global:ReleaseSafetyFixture.Calls | ForEach-Object { $_.Operation })
    $index = [Array]::LastIndexOf($ops, $Operation)
    Assert-Fixture ($index -ge 0) "Fault operation was never reached: $Operation"
    Assert-Fixture ($index -eq $ops.Count - 1) "Native operations continued after $Operation`: $($ops -join ', ')"
}

try {
    Import-ReleaseHelpers
    # Prove native refusal remains exit-code based even when PS7 callers opt in
    # to automatic native error handling. On PS5 this is an ordinary variable.
    $PSNativeCommandUseErrorActionPreference = $true
    Set-Location $script:FixtureRoot
    $env:GH_REPO = 'wrong.invalid/unrelated/repository'; $env:GH_HOST = 'wrong.invalid'
    $env:GOOS = 'linux'; $env:GOARCH = 'arm64'

    Run-ReleaseCase 'native diagnostics stay plain and separate from stdout' {
        foreach ($code in @(0, 19)) {
            $result = Invoke-NativeResult -CommandName $env:ComSpec -Arguments @('/d', '/c', "echo native output & echo native diagnostic 1>&2 & exit /b $code")
            Assert-Fixture ($result.ExitCode -eq $code) 'Native exit code changed.'
            Assert-Fixture ($result.Lines.Count -eq 1 -and $result.Output.Trim() -ceq 'native output') 'Native stdout was lost or mixed with stderr.'
            Assert-Fixture ($result.Diagnostic.Trim() -ceq 'native diagnostic') 'Native stderr contains PowerShell formatting or lost its text.'
        }
        $display = (Invoke-CheckedCommand -CommandName $env:ComSpec -Arguments @('/d', '/c', 'echo native diagnostic 1>&2 & exit /b 0') -FailureMessage 'native fixture failed' 6>&1 | Out-String).Trim()
        Assert-Fixture ($display -ceq 'native diagnostic') 'Successful stderr was not displayed as plain text.'
        Expect-ReleaseRefusal {
            Invoke-CheckedCommand -CommandName $env:ComSpec -Arguments @('/d', '/c', 'echo failed diagnostic 1>&2 & exit /b 19') -FailureMessage 'native fixture failed'
        } 'native fixture failed[\s\S]*failed diagnostic'
    }
    Run-ReleaseCase 'actual native exit and successful stderr' {
        $repo = New-ReleaseFixture
        Reset-ReleaseNative $repo
        $global:ReleaseSafetyFixture.SuccessStderr = $true
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object Operation -eq 'go-build-peer').Count -eq 1) 'Success with stderr did not reach both builds.'
        $global:ReleaseSafetyFixture.Fault = 'go-tidy'
        Expect-ReleaseRefusal { Invoke-GoRunHost @('mod', 'tidy', '-diff') } 'failed'
    }
    Run-ReleaseCase 'repeat owned package and private marker exclusion' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        Reset-ReleaseNative $repo
        Remove-Item Env:GOOS, Env:GOARCH -ErrorAction SilentlyContinue
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zip = [IO.Compression.ZipFile]::OpenRead((Join-Path $repo 'gocluster-windows-amd64.zip'))
        try {
            $names = @($zip.Entries | ForEach-Object { $_.FullName.Replace('\', '/') })
            foreach ($required in @('ready_to_run/gocluster.exe', 'ready_to_run/peerdiag.exe', 'ready_to_run/binaries.json', 'ready_to_run/README.md', 'ready_to_run/docs/OPERATOR_GUIDE.md')) {
                Assert-Fixture ($names -contains $required) "Missing archive payload: $required"
            }
            Assert-Fixture (@($names | Where-Object { $_ -match 'owner|rollback|\.git|main\.go|data/users' }).Count -eq 0) 'Private bookkeeping or source entered the ZIP.'
        } finally { $zip.Dispose() }
        $hashes = [IO.File]::ReadAllText((Join-Path $repo 'ready_to_run/binaries.json')) | ConvertFrom-Json
        foreach ($hash in $hashes) { Assert-Fixture ($hash.sha256 -ceq (Get-FileHash -LiteralPath (Join-Path $repo ('ready_to_run/' + $hash.file)) -Algorithm SHA256).Hash) 'Binary manifest checksum mismatch.' }
        $env:GOOS = 'linux'; $env:GOARCH = 'arm64'
    }
    Run-ReleaseCase 'custom bracket paths and explicit publication target' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo @{ PackageDirectoryName = 'package[one]'; PackageName = 'asset[two]'; OutputDir = 'custom-out' }
        $calls = $global:ReleaseSafetyFixture.Calls
        $tag = @($calls | Where-Object Operation -eq 'git-tag')[0]
        $head = Invoke-FixtureGit $repo @('rev-parse', 'HEAD')
        Assert-Fixture ($tag.Arguments -contains $head) 'Tag did not name the captured full HEAD.'
        $push = @($calls | Where-Object Operation -eq 'git-push')[0]
        Assert-Fixture ($push.Arguments[1] -ceq 'https://github.com/fixture/target.git') 'Push target drifted.'
        $create = @($calls | Where-Object Operation -eq 'gh-create')[0]
        Assert-Fixture ($create.Arguments -contains 'github.com/fixture/target') 'GH_REPO or GH_HOST overrode explicit repository.'
        foreach ($api in @($calls | Where-Object Operation -like 'gh-page*')) { Assert-Fixture ($api.Arguments -contains 'github.com') 'Release API host was not explicit.' }
        $build = @($calls | Where-Object Operation -eq 'go-build-main')[0]
        Assert-Fixture (($build.Arguments -join '|') -match ('main.Commit=' + $head.Substring(0,12))) 'Displayed commit stamp changed.'
        Assert-Fixture (Test-Path -LiteralPath (Join-Path $repo 'custom-out/asset[two].zip')) 'Custom literal ZIP was not created.'
    }
    Run-ReleaseCase 'custom package rebuild with explicit dirty bypass and absolute output' {
        $repo = New-ReleaseFixture
        $output = Join-Path $script:FixtureRoot ('absolute-output-' + [guid]::NewGuid().ToString('N'))
        Reset-ReleaseNative $repo
        $parameters = @{ PackageOnly = $true; AllowDirty = $true; PackageDirectoryName = 'custom-stage'; PackageName = 'custom-asset'; OutputDir = $output }
        Invoke-ActualRelease $repo $parameters
        Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo $parameters
        Assert-Fixture (Test-Path -LiteralPath (Join-Path $output 'custom-asset.zip')) 'Absolute ZIP output was not promoted.'
        Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object { $_.Operation -in @('git-tag', 'git-push', 'gh-create') }).Count -eq 0) 'PackageOnly created publication operations.'
    }
    foreach ($combination in @('os-only', 'arch-only', 'neither', 'both')) {
        Run-ReleaseCase "environment restoration after failure: $combination" {
            if ($combination -in @('os-only', 'both')) { $env:GOOS = 'linux' } else { Remove-Item Env:GOOS -ErrorAction SilentlyContinue }
            if ($combination -in @('arch-only', 'both')) { $env:GOARCH = 'arm64' } else { Remove-Item Env:GOARCH -ErrorAction SilentlyContinue }
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo 'go-build-peer'
            Invoke-ActualRelease $repo @{ PackageOnly = $true } 'failed'
        }
    }
    $env:GOOS = 'linux'; $env:GOARCH = 'arm64'
    foreach ($forbiddenSwitch in @('AllowDirty', 'SkipCodeMapCheck')) {
        Run-ReleaseCase "publish switch guard: $forbiddenSwitch" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            $parameters = @{}; $parameters[$forbiddenSwitch] = $true
            Invoke-ActualRelease $repo $parameters 'only permitted'
            Assert-Fixture ($global:ReleaseSafetyFixture.Calls.Count -eq 0) 'Forbidden publish switch reached native operations.'
        }
    }
    foreach ($fault in @('git-root', 'git-status', 'go-tidy', 'go-map', 'git-head', 'git-abbrev', 'git-files', 'git-target', 'gh-auth', 'git-local', 'git-remote', 'gh-repo', 'gh-page1', 'go-readme', 'go-build-main', 'go-build-peer', 'archive', 'git-tag', 'git-push', 'gh-create')) {
        Run-ReleaseCase "native stop: $fault" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo $fault
            Invoke-ActualRelease $repo @{} '.'
            Assert-NoReleaseAfter $fault
            if ($fault -notin @('git-tag', 'git-push', 'gh-create')) {
                Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object { $_.Operation -in @('git-tag', 'git-push', 'gh-create') }).Count -eq 0) 'Publication followed refusal.'
            }
        }
    }
    Run-ReleaseCase 'dirty diagnostics before staging' {
        $repo = New-ReleaseFixture
        Write-FixtureFile (Join-Path $repo 'unrelated.txt') 'untouched user content'
        $before = Get-FixtureBytes $repo
        Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo @{ PackageOnly = $true } 'unrelated.txt'
        Assert-FixtureBytes $repo $before
        Assert-NoReleaseAfter 'git-status'
    }
    foreach ($drift in @('tracked', 'untracked', 'head')) {
        Run-ReleaseCase "final source guard: $drift" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            $global:ReleaseSafetyFixture.Drift = $drift
            Invoke-ActualRelease $repo @{} 'dirty worktree|HEAD changed'
            Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object { $_.Operation -in @('git-tag', 'git-push', 'gh-create') }).Count -eq 0) 'Source drift reached refs.'
            Assert-Fixture (-not (Test-Path -LiteralPath (Join-Path $repo 'ready_to_run'))) 'Source drift promoted outputs.'
        }
        Run-ReleaseCase "guard immediately before refs: $drift" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            $global:ReleaseSafetyFixture.FinalDrift = $drift
            Invoke-ActualRelease $repo @{ PackageDirectoryName = 'custom-stage'; PackageName = 'custom-asset'; OutputDir = 'custom-out' } 'dirty worktree|HEAD changed'
            Assert-Fixture (Test-Path -LiteralPath (Join-Path $repo 'custom-out/custom-asset.zip')) 'Final refusal fixture never reached output promotion.'
            Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object { $_.Operation -in @('git-tag', 'git-push', 'gh-create') }).Count -eq 0) 'Final source drift reached refs.'
        }
    }
    foreach ($listing in @('duplicate', 'draft', 'late-draft', 'malformed', 'invalid-entry')) {
        Run-ReleaseCase "release listing: $listing" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            $global:ReleaseSafetyFixture.Listing = $listing
            Invoke-ActualRelease $repo @{} 'already exists|Invalid'
            Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object Operation -eq 'go-readme').Count -eq 0) 'Invalid/duplicate release lookup began staging.'
            if ($listing -eq 'late-draft') { Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object Operation -eq 'gh-page2').Count -eq 1) 'Listing did not inspect its second page.' }
        }
    }
    foreach ($permission in @('READ', 'TRIAGE')) {
        Run-ReleaseCase "draft visibility: $permission" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            $global:ReleaseSafetyFixture.Permission = $permission
            Invoke-ActualRelease $repo @{} 'draft visibility'
        }
    }
    foreach ($kind in @('local', 'remote')) {
        Run-ReleaseCase "duplicate $kind tag" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            if ($kind -eq 'local') { $global:ReleaseSafetyFixture.LocalCode = 0 } else { $global:ReleaseSafetyFixture.RemoteCode = 0 }
            Invoke-ActualRelease $repo @{} 'already exists'
        }
    }
    Run-ReleaseCase 'ambiguous push remotes and different fetch URL' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        Invoke-FixtureGit $repo @('remote', 'set-url', '--push', 'origin', 'git@github.com:fixture/target.git') | Out-Null
        Push-Location $repo
        try {
            $target = Resolve-PublicationTarget 'origin'
            Assert-Fixture ($target.Repository -ceq 'github.com/fixture/target' -and $target.PushUrl -ceq 'git@github.com:fixture/target.git') 'Fetch URL substituted for selected push URL.'
            Invoke-FixtureGit $repo @('remote', 'set-url', '--add', '--push', 'origin', 'https://github.com/other/target.git') | Out-Null
            Expect-ReleaseRefusal { Resolve-PublicationTarget 'origin' } 'unambiguous'
        } finally { Pop-Location }
    }
    $pathRepo = New-ReleaseFixture; Reset-ReleaseNative $pathRepo
    foreach ($name in @('', '.', '..', '../outside', 'C:\outside', 'CON', 'nul.txt', 'COM1.log', 'LPT9', ('COM' + [char]0xB9), 'trailing.', 'trailing ', 'bad*name', 'bad?name', 'a:b', '.git', '.tmp')) {
        Run-ReleaseCase "unsafe stage name: '$name'" {
            Expect-ReleaseRefusal { Get-ReleasePaths $pathRepo '.' 'asset' $name } 'safe|overlap|metadata'
        }
    }
    foreach ($output in @('ready_to_run', 'READY_TO_RUN/inside', '..', '..\sibling', 'inside\..\sibling', 'trailing.\nested', 'trailing \nested', '.git/outputs', 'C:', 'C:folder', '\folder')) {
        Run-ReleaseCase "unsafe output overlap: $output" {
            Expect-ReleaseRefusal { Get-ReleasePaths $pathRepo $output 'asset' 'ready_to_run' } 'overlap|metadata|safe|traversal|fully qualified'
        }
    }
    Run-ReleaseCase 'stage directory and ZIP alias collision' {
        Expect-ReleaseRefusal { Get-ReleasePaths $pathRepo '.' 'bundle' 'bundle.zip' } 'overlap|collid'
        Expect-ReleaseRefusal { Get-ReleasePaths $pathRepo '.' 'BUNDLE' 'bundle.zip' } 'overlap|collid'
    }
    Run-ReleaseCase 'absolute external drive root stays absolute' {
        $repoDrive = [IO.Path]::GetPathRoot($pathRepo)
        $external = @(Get-PSDrive -PSProvider FileSystem | Where-Object { $_.Root -match '^[A-Za-z]:\\$' -and $_.Root -ine $repoDrive })
        if ($external.Count -eq 0) {
            $script:CurrentCaseSkipped = $true
            $script:Skipped++
            Write-Host 'SKIP absolute external drive root stays absolute: no second filesystem drive is available.'
            return
        }
        $paths = Get-ReleasePaths $pathRepo $external[0].Root 'fixture-resolve-only' 'ready_to_run'
        Assert-Fixture ($paths.OutputRoot -ceq $external[0].Root) 'Canonicalization removed the absolute drive-root delimiter.'
        Assert-Fixture ($paths.ZipPath -ceq (Join-Path $external[0].Root 'fixture-resolve-only.zip')) 'ZIP path became relative to another drive current directory.'
    }
    Run-ReleaseCase 'tracked source and missing tracked target collisions' {
        Expect-ReleaseRefusal { Get-ReleasePaths $pathRepo '.' 'asset' 'data' } 'tracked source'
        Write-FixtureFile (Join-Path $pathRepo 'protected.zip') 'tracked archive'
        Invoke-FixtureGit $pathRepo @('add', 'protected.zip') | Out-Null
        Invoke-FixtureGit $pathRepo @('commit', '-q', '-m', 'tracked collision') | Out-Null
        Remove-Item -LiteralPath (Join-Path $pathRepo 'protected.zip')
        Expect-ReleaseRefusal { Get-ReleasePaths $pathRepo '.' 'protected' 'ready_to_run' } 'tracked source'
    }
    foreach ($shape in @('pair', 'stage-only', 'zip-only')) {
        Run-ReleaseCase "markerless legacy migration preserves $shape" {
            $repo = New-ReleaseFixture
            $legacy = New-LegacyReleaseOutputs $repo $shape
            Reset-ReleaseNative $repo
            Invoke-ActualRelease $repo @{ PackageOnly = $true }
            Assert-Fixture (Test-Path -LiteralPath (Join-Path $legacy.StageRoot '.gocluster-release-owner.json')) 'Replacement did not acquire an ownership marker.'
            Assert-Fixture (-not (Test-Path -LiteralPath (Join-Path $legacy.StageRoot 'operator-state'))) 'Private legacy state entered the replacement stage.'
            $runs = @(Get-ChildItem -LiteralPath (Join-Path $repo '.tmp') -Filter 'release-*' -Directory)
            Assert-Fixture ($runs.Count -eq 1) 'Legacy backup run directory was not retained exactly once.'
            $run = $runs[0].FullName
            $stageBackup = Join-Path $run 'previous-stage'; $zipBackup = Join-Path $run 'previous-package.zip'
            if ($null -ne $legacy.StageBytes) { Assert-FixtureBytes $stageBackup $legacy.StageBytes }
            else { Assert-Fixture (-not (Test-Path -LiteralPath $stageBackup)) 'ZIP-only migration invented a previous-stage backup.' }
            if ($legacy.ZipHash) { Assert-Fixture ((Get-FileHash -LiteralPath $zipBackup -Algorithm SHA256).Hash -ceq $legacy.ZipHash) 'Legacy archive bytes were not preserved.' }
            else { Assert-Fixture (-not (Test-Path -LiteralPath $zipBackup)) 'Stage-only migration invented a previous-package backup.' }
            Assert-Fixture (@(Get-ChildItem -LiteralPath $repo -Filter '*.rollback-*' -File).Count -eq 0) 'Successful legacy migration left a sibling rollback ZIP.'
            $zip = [IO.Compression.ZipFile]::OpenRead($legacy.ZipPath)
            try { Assert-Fixture (@($zip.Entries | Where-Object { $_.FullName -match 'operator-state|private|previous-|owner' }).Count -eq 0) 'Legacy bytes or ownership bookkeeping entered the public ZIP.' }
            finally { $zip.Dispose() }
            Reset-ReleaseNative $repo
            Invoke-ActualRelease $repo @{ PackageOnly = $true }
            if ($null -ne $legacy.StageBytes) { Assert-FixtureBytes $stageBackup $legacy.StageBytes }
            if ($legacy.ZipHash) { Assert-Fixture ((Get-FileHash -LiteralPath $zipBackup -Algorithm SHA256).Hash -ceq $legacy.ZipHash) 'Owned rebuild discarded the retained legacy ZIP.' }
        }
        foreach ($fault in @('go-build-main', 'go-build-peer')) {
            Run-ReleaseCase "legacy $shape survives $fault" {
                $repo = New-ReleaseFixture
                $legacy = New-LegacyReleaseOutputs $repo $shape
                Reset-ReleaseNative $repo $fault
                Invoke-ActualRelease $repo @{ PackageOnly = $true } 'failed'
                if ($null -ne $legacy.StageBytes) { Assert-FixtureBytes $legacy.StageRoot $legacy.StageBytes }
                else { Assert-Fixture (-not (Test-Path -LiteralPath $legacy.StageRoot)) 'Failed build promoted a new stage over ZIP-only legacy output.' }
                if ($legacy.ZipHash) { Assert-Fixture ((Get-FileHash -LiteralPath $legacy.ZipPath -Algorithm SHA256).Hash -ceq $legacy.ZipHash) 'Build failure changed a legacy ZIP.' }
                else { Assert-Fixture (-not (Test-Path -LiteralPath $legacy.ZipPath)) 'Failed build created a ZIP over stage-only legacy output.' }
                Assert-Fixture (-not (Test-Path -LiteralPath (Join-Path $legacy.StageRoot '.gocluster-release-owner.json'))) 'Failed build wrote ownership into legacy data.'
                Assert-Fixture (@(Get-ChildItem -LiteralPath (Join-Path $repo '.tmp') -Filter 'release-*' -Directory -ErrorAction SilentlyContinue).Count -eq 0) 'Failed build moved legacy data into a retained run.'
            }
        }
    }
    foreach ($mutation in @('edited-file', 'missing-file', 'extra-file', 'extra-dir', 'zip-edited', 'zip-directory', 'stage-file', 'marker-malformed', 'marker-directory', 'marker-identity', 'inventory-duplicate')) {
        Run-ReleaseCase "owned output refusal: $mutation" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            Invoke-ActualRelease $repo @{ PackageOnly = $true }
            $stage = Join-Path $repo 'ready_to_run'; $zip = Join-Path $repo 'gocluster-windows-amd64.zip'; $marker = Join-Path $stage '.gocluster-release-owner.json'
            switch ($mutation) {
                'edited-file' { [IO.File]::AppendAllText((Join-Path $stage 'data/config/app.yaml'), 'operator private edit') }
                'missing-file' { Remove-Item -LiteralPath (Join-Path $stage 'README.md') }
                'extra-file' { Write-FixtureFile (Join-Path $stage 'data/users/private.txt') 'private state' }
                'extra-dir' { New-Item -ItemType Directory -Path (Join-Path $stage 'empty-runtime-state') | Out-Null }
                'zip-edited' { [IO.File]::AppendAllText($zip, 'unrelated ZIP edit') }
                'zip-directory' { Remove-Item -LiteralPath $zip; New-Item -ItemType Directory -Path $zip | Out-Null }
                'stage-file' { Remove-Item -LiteralPath $stage -Recurse -Force; Write-FixtureFile $stage 'unrelated file' }
                'marker-malformed' { Write-FixtureFile $marker '{broken JSON' }
                'marker-directory' { Remove-Item -LiteralPath $marker; New-Item -ItemType Directory -Path $marker | Out-Null }
                'marker-identity' { $owner = [IO.File]::ReadAllText($marker) | ConvertFrom-Json; $owner.RepoRoot = 'C:\unrelated'; $owner | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $marker }
                'inventory-duplicate' { $owner = [IO.File]::ReadAllText($marker) | ConvertFrom-Json; $owner.Entries[1] = $owner.Entries[0]; $owner | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $marker }
            }
            $before = Get-FixtureBytes $repo
            Reset-ReleaseNative $repo
            Invoke-ActualRelease $repo @{ PackageOnly = $true } '.'
            Assert-FixtureBytes $repo $before
            Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object Operation -eq 'go-readme').Count -eq 0) 'Unowned existing output began preparation.'
        }
    }
    Run-ReleaseCase 'junction ancestor and nested payload preserve target' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        $outside = Join-Path $script:FixtureRoot ('outside-' + [guid]::NewGuid().ToString('N'))
        Write-FixtureFile (Join-Path $outside 'sentinel.txt') 'unrelated target content'
        $before = Get-FixtureBytes $outside
        $link = Join-Path $repo 'linked-output'
        New-Item -ItemType Junction -Path $link -Target $outside | Out-Null
        try { Expect-ReleaseRefusal { Get-ReleasePaths $repo 'linked-output/subdir' 'asset' 'ready_to_run' } 'Reparse' }
        finally { [IO.Directory]::Delete($link) }
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        $nested = Join-Path $repo 'ready_to_run/linked-state'
        New-Item -ItemType Junction -Path $nested -Target $outside | Out-Null
        try { Invoke-ActualRelease $repo @{ PackageOnly = $true } 'Reparse'; Assert-FixtureBytes $outside $before }
        finally { [IO.Directory]::Delete($nested) }
    }
    Run-ReleaseCase 'markerless legacy junction refuses before replacement' {
        $repo = New-ReleaseFixture
        $legacy = New-LegacyReleaseOutputs $repo 'pair'
        $outside = Join-Path $script:FixtureRoot ('legacy-junction-target-' + [guid]::NewGuid().ToString('N'))
        Write-FixtureFile (Join-Path $outside 'sentinel.txt') 'untouched external legacy state'
        $before = Get-FixtureBytes $outside
        $link = Join-Path $legacy.StageRoot 'linked-state'
        New-Item -ItemType Junction -Path $link -Target $outside | Out-Null
        try {
            Reset-ReleaseNative $repo
            Invoke-ActualRelease $repo @{ PackageOnly = $true } 'Reparse'
            Assert-FixtureBytes $outside $before
            Assert-Fixture ((Get-FileHash -LiteralPath $legacy.ZipPath -Algorithm SHA256).Hash -ceq $legacy.ZipHash) 'Legacy reparse refusal changed the ZIP.'
            Assert-Fixture (@($global:ReleaseSafetyFixture.Calls | Where-Object Operation -eq 'go-readme').Count -eq 0) 'Unsafe legacy staging began preparation.'
        } finally { [IO.Directory]::Delete($link) }
        Assert-FixtureBytes $legacy.StageRoot $legacy.StageBytes
    }
    Run-ReleaseCase 'markerless legacy rollback restores original pair' {
        $repo = New-ReleaseFixture
        $legacy = New-LegacyReleaseOutputs $repo 'pair'
        Reset-ReleaseNative $repo
        $PackageName = 'gocluster-windows-amd64'; $PackageDirectoryName = 'ready_to_run'
        $paths = Get-ReleasePaths $repo '.' $PackageName $PackageDirectoryName
        $runRoot = Join-Path $repo '.tmp/legacy-rollback-fixture'
        $preparedStage = Join-Path $runRoot $PackageDirectoryName; $preparedZip = Join-Path $runRoot 'prepared.zip'
        Write-FixtureFile (Join-Path $preparedStage 'new.txt') 'new generated bytes'
        Write-FixtureFile $preparedZip 'new archive bytes'
        $owner = Write-ReleaseOwnership $paths $preparedStage $preparedZip
        Remove-Item -LiteralPath $preparedZip
        $script:retainRunRoot = $false
        Expect-ReleaseRefusal { Complete-ReleaseOutputs $paths $preparedStage $preparedZip $runRoot $owner } '.'
        Assert-FixtureBytes $legacy.StageRoot $legacy.StageBytes
        Assert-Fixture ((Get-FileHash -LiteralPath $legacy.ZipPath -Algorithm SHA256).Hash -ceq $legacy.ZipHash) 'Legacy rollback lost the original ZIP.'
        Assert-Fixture (-not (Test-Path -LiteralPath (Join-Path $legacy.StageRoot '.gocluster-release-owner.json'))) 'Rollback rewrote ownership into unknown legacy contents.'
        Remove-RunDirectory $runRoot $runRoot
    }
    foreach ($fault in @('go-tidy', 'go-readme', 'go-build-main', 'go-build-peer', 'archive', 'marker')) {
        Run-ReleaseCase "prior outputs survive: $fault" {
            $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
            Invoke-ActualRelease $repo @{ PackageOnly = $true }
            $before = Get-FixtureBytes $repo
            Reset-ReleaseNative $repo $fault
            Invoke-ActualRelease $repo @{ PackageOnly = $true } '.'
            Assert-FixtureBytes $repo $before
        }
    }
    Run-ReleaseCase 'second promotion failure restores prior bytes' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        $PackageName = 'gocluster-windows-amd64'; $PackageDirectoryName = 'ready_to_run'
        $paths = Get-ReleasePaths $repo '.' $PackageName $PackageDirectoryName
        $beforeStage = Get-FixtureBytes $paths.StageRoot
        $beforeZip = (Get-FileHash -LiteralPath $paths.ZipPath -Algorithm SHA256).Hash
        $runRoot = Join-Path $repo '.tmp/promotion-fixture'
        $preparedStage = Join-Path $runRoot $PackageDirectoryName; $preparedZip = Join-Path $runRoot 'prepared.zip'
        Write-FixtureFile (Join-Path $preparedStage 'new.txt') 'new generated bytes'
        Write-FixtureFile $preparedZip 'new archive bytes'
        $newOwner = Write-ReleaseOwnership $paths $preparedStage $preparedZip
        Remove-Item -LiteralPath $preparedZip
        Expect-ReleaseRefusal { Complete-ReleaseOutputs $paths $preparedStage $preparedZip $runRoot $newOwner } '.'
        Assert-FixtureBytes $paths.StageRoot $beforeStage
        Assert-Fixture ((Get-FileHash -LiteralPath $paths.ZipPath -Algorithm SHA256).Hash -ceq $beforeZip) 'Prior ZIP was lost during recovery.'
        Remove-RunDirectory $runRoot $runRoot
    }
    Run-ReleaseCase 'rollback failure retains named recovery backups' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        $PackageName = 'gocluster-windows-amd64'; $PackageDirectoryName = 'ready_to_run'
        $paths = Get-ReleasePaths $repo '.' $PackageName $PackageDirectoryName
        $priorStage = Get-FixtureBytes $paths.StageRoot
        $priorZip = (Get-FileHash -LiteralPath $paths.ZipPath -Algorithm SHA256).Hash
        $runRoot = Join-Path $repo '.tmp/rollback-fixture'
        $preparedStage = Join-Path $runRoot $PackageDirectoryName; $preparedZip = Join-Path $runRoot 'prepared.zip'
        Write-FixtureFile (Join-Path $preparedStage 'new.txt') 'new generated bytes'
        Write-FixtureFile $preparedZip 'new archive bytes'
        $owner = Write-ReleaseOwnership $paths $preparedStage $preparedZip
        Remove-Item -LiteralPath $preparedZip
        $expectedEntries = $owner.Entries
        $faultOwner = [pscustomobject]@{ ZipSha256 = $owner.ZipSha256 }
        # Recovery reads Entries only after stage promotion. An accessor inserts
        # a real filesystem obstruction at that point: the production move must
        # fail, rather than a mock merely claiming that rollback failed.
        $obstruction = {
            [IO.Directory]::CreateDirectory($preparedStage) | Out-Null
            [IO.File]::WriteAllText((Join-Path $preparedStage 'obstruction.txt'), 'filesystem recovery obstruction')
            return $expectedEntries
        }.GetNewClosure()
        Add-Member -InputObject $faultOwner -MemberType ScriptProperty -Name Entries -Value $obstruction
        $script:retainRunRoot = $false
        $script:recoveryFailure = $null
        $messages = @(& {
            try { Complete-ReleaseOutputs $paths $preparedStage $preparedZip $runRoot $faultOwner }
            catch { $script:recoveryFailure = $_ }
        } 3>&1)
        Assert-Fixture ($null -ne $script:recoveryFailure) 'Missing ZIP did not fail promotion.'
        Assert-Fixture $script:retainRunRoot 'Failed rollback did not retain its run directory.'
        Assert-FixtureBytes (Join-Path $runRoot 'previous-stage') $priorStage
        $zipBackups = @(Get-ChildItem -LiteralPath $repo -Filter 'gocluster-windows-amd64.zip.rollback-*' -File)
        Assert-Fixture ($zipBackups.Count -eq 1) 'Prior ZIP recovery backup missing or ambiguous.'
        Assert-Fixture ((Get-FileHash -LiteralPath $zipBackups[0].FullName -Algorithm SHA256).Hash -ceq $priorZip) 'Prior ZIP bytes lost after failed recovery.'
        $warningText = (@($messages | ForEach-Object { $_.ToString() }) -join "`n")
        Assert-Fixture ($warningText.Contains($runRoot) -and $warningText.Contains($zipBackups[0].FullName)) 'Recovery diagnostic did not name both retained backup locations.'
        Assert-Fixture (Test-Path -LiteralPath (Join-Path $paths.StageRoot 'new.txt')) 'New promoted stage disappeared after failed recovery.'
        $script:retainRunRoot = $false
    }
    Run-ReleaseCase 'failed old-backup verification survives actual outer cleanup guard' {
        $repo = New-ReleaseFixture; Reset-ReleaseNative $repo
        Invoke-ActualRelease $repo @{ PackageOnly = $true }
        $PackageName = 'gocluster-windows-amd64'; $PackageDirectoryName = 'ready_to_run'
        $paths = Get-ReleasePaths $repo '.' $PackageName $PackageDirectoryName
        $priorStage = Get-FixtureBytes $paths.StageRoot
        $priorZip = (Get-FileHash -LiteralPath $paths.ZipPath -Algorithm SHA256).Hash
        $runRoot = Join-Path $repo '.tmp/disposal-fixture'
        $preparedStage = Join-Path $runRoot $PackageDirectoryName; $preparedZip = Join-Path $runRoot 'prepared.zip'
        Write-FixtureFile (Join-Path $preparedStage 'new.txt') 'new generated bytes'
        Write-FixtureFile $preparedZip 'new archive bytes'
        $owner = Write-ReleaseOwnership $paths $preparedStage $preparedZip
        $original = (Get-Command Assert-OwnedReleaseOutputs).ScriptBlock
        $global:ReleaseDisposalFault = [pscustomobject]@{ Original = $original; Calls = 0; RunRoot = $runRoot }
        function global:Assert-OwnedReleaseOutputs {
            param([object]$Paths, [switch]$AllowLegacy)
            $fault = $global:ReleaseDisposalFault
            $result = & $fault.Original $Paths -AllowLegacy:$AllowLegacy
            $fault.Calls++
            if ($fault.Calls -eq 2) {
                # Inject an actual unexpected file after new outputs verify.
                [IO.File]::WriteAllText((Join-Path $fault.RunRoot 'previous-stage/unverified.txt'), 'must preserve unexpected backup bytes')
            }
            return $result
        }
        $script:retainRunRoot = $false
        $script:disposalFailure = $null
        $tokens = $null; $errors = $null
        $releaseAst = [Management.Automation.Language.Parser]::ParseFile($script:Source, [ref]$tokens, [ref]$errors)
        $cleanup = $releaseAst.Find({ param($node)
            $node -is [Management.Automation.Language.IfStatementAst] -and $node.Extent.Text -match 'if \(\$null -ne \$runRoot -and -not \$script:retainRunRoot\)'
        }, $true)
        Assert-Fixture ($null -ne $cleanup) 'Actual release outer-cleanup guard not found.'
        $messages = @()
        try {
            $messages = @(& {
                try { Complete-ReleaseOutputs $paths $preparedStage $preparedZip $runRoot $owner }
                catch { $script:disposalFailure = $_ }
                finally { . ([scriptblock]::Create($cleanup.Extent.Text)) }
            } 3>&1)
        } finally {
            Set-Item Function:global:Assert-OwnedReleaseOutputs -Value $original
            Remove-Variable ReleaseDisposalFault -Scope Global
        }
        Assert-Fixture ($null -ne $script:disposalFailure -and $script:retainRunRoot) 'Backup verification failure did not stop and retain the run.'
        $backup = Join-Path $runRoot 'previous-stage'
        $retained = Get-FixtureBytes $backup
        foreach ($key in $priorStage.Keys) { Assert-Fixture ($retained[$key] -ceq $priorStage[$key]) "Prior backup lost original content: $key" }
        Assert-Fixture ([IO.File]::ReadAllText((Join-Path $backup 'unverified.txt')) -ceq 'must preserve unexpected backup bytes') 'Outer cleanup deleted unverified bytes.'
        $zipBackups = @(Get-ChildItem -LiteralPath $repo -Filter 'gocluster-windows-amd64.zip.rollback-*' -File)
        Assert-Fixture ($zipBackups.Count -eq 1 -and (Get-FileHash -LiteralPath $zipBackups[0].FullName -Algorithm SHA256).Hash -ceq $priorZip) 'Disposal failure lost the previous ZIP.'
        $warningText = (@($messages | ForEach-Object { $_.ToString() }) -join "`n")
        Assert-Fixture ($warningText.Contains($backup) -and $warningText.Contains($zipBackups[0].FullName)) 'Disposal warning omitted retained paths.'
        $script:retainRunRoot = $false
    }
    Run-ReleaseCase 'real Git autocrlf LF checkout and substantive tidy no-write' {
        $repo = Join-Path $script:FixtureRoot 'real-module'
        Write-FixtureFile (Join-Path $repo 'main.go') "package main`nfunc main() {}`n"
        Write-FixtureFile (Join-Path $repo 'go.mod') "module example.test/lfcheck`n`ngo 1.26`n"
        # Nonempty content makes the go.sum checkout oracle sensitive to a
        # missing LF attribute; an empty file would pass either EOL policy.
        Write-FixtureFile (Join-Path $repo 'go.sum') "example.invalid/unused v1.0.0 h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=`n"
        Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../.gitattributes') -Destination (Join-Path $repo '.gitattributes')
        Invoke-FixtureGit $repo @('init', '-q') | Out-Null
        Invoke-FixtureGit $repo @('config', 'user.name', 'Release LF fixture') | Out-Null
        Invoke-FixtureGit $repo @('config', 'user.email', 'release-lf@example.invalid') | Out-Null
        Invoke-FixtureGit $repo @('config', 'core.hooksPath', (Join-Path $repo '.git/disabled-fixture-hooks')) | Out-Null
        Invoke-FixtureGit $repo @('config', 'commit.gpgsign', 'false') | Out-Null
        Invoke-FixtureGit $repo @('config', 'core.autocrlf', 'true') | Out-Null
        Invoke-FixtureGit $repo @('add', '.') | Out-Null
        Invoke-FixtureGit $repo @('commit', '-q', '-m', 'LF fixture') | Out-Null
        Remove-Item -LiteralPath (Join-Path $repo 'go.mod'), (Join-Path $repo 'go.sum')
        Invoke-FixtureGit $repo @('checkout', '--', 'go.mod', 'go.sum') | Out-Null
        foreach ($relative in @('go.mod', 'go.sum')) { Assert-Fixture (-not ([IO.File]::ReadAllBytes((Join-Path $repo $relative)) -contains [byte]13)) "$relative was checked out with CRLF." }
        Assert-Fixture ((Invoke-FixtureGit $repo @('status', '--porcelain')) -ceq '') 'LF checkout became dirty.'
        # The synthetic checksum exists only to exercise checkout EOL handling.
        # Clear its unused entry before the separate real tidy oracle, avoiding
        # network/dependency lookup and keeping a trivial clean module.
        Write-FixtureFile (Join-Path $repo 'go.sum') ''
        Push-Location $repo
        $priorOS = $env:GOOS; $priorArch = $env:GOARCH
        try {
            Remove-Item Env:GOOS, Env:GOARCH -ErrorAction SilentlyContinue
            & $script:RealGo mod tidy -diff
            Assert-Fixture ($LASTEXITCODE -eq 0) 'Actual tidy rejected clean LF module.'
            [IO.File]::AppendAllText((Join-Path $repo 'go.mod'), "`nrequire example.invalid/unused v1.0.0`n")
            $before = Get-FixtureBytes $repo
            $ErrorActionPreference = 'Continue'
            $tidyOutput = @(& $script:RealGo mod tidy -diff 2>&1)
            $tidyCode = $LASTEXITCODE
            $ErrorActionPreference = 'Stop'
            Assert-Fixture ($tidyCode -ne 0) 'Substantive module change escaped actual tidy.'
            Assert-Fixture (($tidyOutput | Out-String) -match '(?m)^-require example\.invalid/unused v1\.0\.0') 'Tidy failure did not demonstrate the expected substantive module diff.'
            Assert-FixtureBytes $repo $before
        } finally {
            $env:GOOS = $priorOS; $env:GOARCH = $priorArch
            Pop-Location
        }
    }
    Write-Output "Release safety fixtures passed: $script:Passed; skipped: $script:Skipped ($($PSVersionTable.PSVersion))."
    $global:LASTEXITCODE = 0
} finally {
    Set-Location $script:OriginalLocation
    foreach ($entry in @(@('GOOS', $script:OriginalGOOS), @('GOARCH', $script:OriginalGOARCH), @('GH_REPO', $script:OriginalGHRepo), @('GH_HOST', $script:OriginalGHHost))) {
        if ($null -eq $entry[1]) { Remove-Item ('Env:' + $entry[0]) -ErrorAction SilentlyContinue }
        else { Set-Item ('Env:' + $entry[0]) $entry[1] }
    }
    Remove-Item Function:git, Function:go, Function:gh, Function:Compress-Archive -ErrorAction SilentlyContinue
    Remove-Variable ReleaseSafetyFixture -Scope Global -ErrorAction SilentlyContinue
    # The root is a fresh absolute path created by this harness, never a caller
    # parameter. Junction fixtures are detached before recursive fixture cleanup.
    $absoluteFixture = [IO.Path]::GetFullPath($script:FixtureRoot)
    $absoluteTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
    if (-not $absoluteFixture.StartsWith($absoluteTemp, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetFileName($absoluteFixture) -notlike 'gocluster-release-safety-*') { throw 'Unsafe fixture cleanup path.' }
    Remove-Item -LiteralPath $absoluteFixture -Recurse -Force
}
