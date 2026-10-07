<#
.SYNOPSIS
    Check automatic commit-suffix release tags and publishing identity without publishing.
.DESCRIPTION
    Parses the release script, exercises its parameter block in a noninteractive
    child process, and runs its actual identity assignments and publishing helpers
    with mocked Git/GitHub commands. No repository refs or packages are changed.
.NOTES
    Prerequisites: Windows PowerShell. Writes one temporary parameter probe file.
    Side effects: no network calls, Git mutations, or release publication.
#>
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null
$parseErrors = $null
$sourcePath = Join-Path $PSScriptRoot 'create-release.ps1'
$ast = [Management.Automation.Language.Parser]::ParseFile($sourcePath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw ($parseErrors | Out-String) }

$probePath = Join-Path ([IO.Path]::GetTempPath()) ('release-parameters-' + [guid]::NewGuid().ToString('N') + '.ps1')
function Invoke-ParameterProbe([string[]]$ProbeArguments) {
    # Windows PowerShell wraps native stderr in ErrorRecords even when redirected.
    # Expected binding failures are evidence, not terminating fixture failures.
    $ErrorActionPreference = 'Continue'
    $output = & powershell.exe -NoProfile -NonInteractive -File $probePath @ProbeArguments 2>&1
    return [pscustomobject]@{ Output = ($output | Out-String).Trim(); ExitCode = $LASTEXITCODE }
}
try {
    $parameterAttributes = ($ast.ParamBlock.Attributes | ForEach-Object { $_.Extent.Text }) -join "`n"
    ($parameterAttributes + "`n" + $ast.ParamBlock.Extent.Text + "`n" + 'Write-Output "bound"') | Set-Content -LiteralPath $probePath
    foreach ($packageOnly in @($false, $true)) {
        $probeArguments = if ($packageOnly) { @('-PackageOnly') } else { @() }
        $result = Invoke-ParameterProbe $probeArguments
        if ($result.ExitCode -ne 0 -or $result.Output -cne 'bound') { throw 'Release parameters prompted or failed without a release number' }
    }
    $result = Invoke-ParameterProbe @('-ReleaseNumber', '2')
    if ($result.ExitCode -eq 0 -or $result.Output -notmatch 'ReleaseNumber') { throw 'Removed release number parameter was accepted' }
} finally {
    Remove-Item -LiteralPath $probePath -Force -ErrorAction SilentlyContinue
}

foreach ($name in @('Invoke-NativeResult', 'Invoke-CheckedCommand', 'Assert-ReleaseTargetsAvailable', 'New-ReleaseNotes', 'Publish-GitHubRelease')) {
    $definition = $ast.Find({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name }, $true)
    if (-not $definition) { throw "Missing helper: $name" }
    . ([scriptblock]::Create($definition.Extent.Text))
}
function Get-IdentityAssignment([string]$Name) {
    $assignment = @($ast.FindAll({ param($node)
        $node -is [Management.Automation.Language.AssignmentStatementAst] -and
        $node.Left -is [Management.Automation.Language.VariableExpressionAst] -and
        $node.Left.VariablePath.UserPath -ceq $Name
    }, $true))
    if ($assignment.Count -ne 1) { throw "Expected one assignment for $Name" }
    return [scriptblock]::Create($assignment[0].Extent.Text)
}
$commit = '3d8c007ae164'
$buildTime = '2026-10-03T16:42:10Z'
foreach ($vector in @(
    @{ CommitId = '3d8c007ae164012345678901234567890123459abc'; Tag = '261003r9abc' },
    @{ CommitId = '3d8c007ae16401234567890123456789012345000f'; Tag = '261003r000f' },
    @{ CommitId = ('3d8c007ae164' + ('0' * 48) + '9abc'); Tag = '261003r9abc' }
)) {
    $commitId = $vector.CommitId
    $buildUtc = [DateTimeOffset]::Parse($buildTime).UtcDateTime
    . (Get-IdentityAssignment 'version')
    . (Get-IdentityAssignment 'releaseTag')
    . (Get-IdentityAssignment 'ldflags')
    if ($version -cne '261003' -or $releaseTag -cne $vector.Tag -or
        $ldflags -cne "-X main.Version=261003 -X main.ReleaseTag=$($vector.Tag) -X main.Commit=$commit -X main.BuildTime=$buildTime") {
        throw 'Product version, full commit suffix, or executable stamping drifted'
    }
}
# The UTC date changes across midnight even with identical source.
$buildUtc = [DateTimeOffset]::Parse('2026-10-04T00:00:00Z').UtcDateTime
. (Get-IdentityAssignment 'version')
. (Get-IdentityAssignment 'releaseTag')
if ($releaseTag -cne '261004r9abc') { throw 'Release date did not advance at UTC midnight' }
$buildUtc = [DateTimeOffset]::Parse($buildTime).UtcDateTime
. (Get-IdentityAssignment 'version')
. (Get-IdentityAssignment 'releaseTag')
$Remote = 'fixture-origin'
$target = [pscustomobject]@{ PushUrl = 'https://github.com/fixture/release.git'; Repository = 'github.com/fixture/release'; NameWithOwner = 'fixture/release'; HostName = 'github.com' }
$PackageName = 'gocluster-windows-amd64'
$PackageDirectoryName = 'ready_to_run'
$calls = [Collections.Generic.List[object]]::new()
$duplicate = ''
$failPush = $false
$failRelease = $false
function git {
    $calls.Add([pscustomobject]@{ Command = 'git'; Arguments = @($args) })
    $global:LASTEXITCODE = 0
    switch ($args[0]) {
        'show-ref' { if ($duplicate -ne 'local') { $global:LASTEXITCODE = 2 } }
        'ls-remote' { if ($duplicate -ne 'remote') { $global:LASTEXITCODE = 2 } }
        'tag' { }
        'push' { if ($failPush) { $global:LASTEXITCODE = 1 } }
        default { throw 'Unexpected mocked Git command' }
    }
}
function gh {
    $notes = if ($args[1] -eq 'create') { [IO.File]::ReadAllText($args[-1]) } else { '' }
    $calls.Add([pscustomobject]@{ Command = 'gh'; Arguments = @($args); Notes = $notes })
    $global:LASTEXITCODE = 0
    switch ($args[0]) {
        'repo' { '{"nameWithOwner":"fixture/release","viewerPermission":"WRITE"}' }
        'api' { if ($duplicate -eq 'release') { '[{"tag_name":"261003r9abc","draft":true}]' } else { '[]' } }
        'release' { if ($args[1] -ne 'create') { throw 'Unexpected mocked GitHub release operation' }; if ($failRelease) { $global:LASTEXITCODE = 1 } }
        default { throw 'Unexpected mocked GitHub command' }
    }
}
$targetCheck = $ast.Find({ param($node)
    $node -is [Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'Assert-ReleaseTargetsAvailable'
}, $true)
. ([scriptblock]::Create($targetCheck.Extent.Text))
if ($calls.Count -ne 4 -or $calls[0].Arguments[-1] -cne 'refs/tags/261003r9abc' -or
    $calls[1].Arguments[-1] -cne 'refs/tags/261003r9abc' -or $calls[2].Arguments[2] -cne 'github.com/fixture/release' -or
    $calls[3].Arguments[-1] -cne 'repos/fixture/release/releases?per_page=100&page=1') {
    throw 'Duplicate checks used the product version instead of the release tag'
}
foreach ($duplicate in @('local', 'remote', 'release')) {
    $refused = $false
    try { . ([scriptblock]::Create($targetCheck.Extent.Text)) }
    catch { if ($_.Exception.Message -notmatch '261003r9abc already exists') { throw }; $refused = $true }
    if (-not $refused) { throw "Duplicate $duplicate identity was accepted" }
}
$duplicate = ''
$zipPath = 'fixture.zip'
$publish = $ast.Find({ param($node)
    $node -is [Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'Publish-GitHubRelease'
}, $true)
$calls.Clear()
. ([scriptblock]::Create($publish.Extent.Text))
if ($calls.Count -ne 3 -or ($calls[0].Arguments -join '|') -cne "tag|-a|261003r9abc|$commitId|-m|Release 261003r9abc" -or
    ($calls[1].Arguments -join '|') -cne 'push|https://github.com/fixture/release.git|refs/tags/261003r9abc:refs/tags/261003r9abc' -or
    $calls[2].Arguments[2] -cne '261003r9abc' -or $calls[2].Arguments[8] -cne '261003r9abc' -or
    $calls[2].Arguments[5] -cne 'github.com/fixture/release' -or $calls[2].Arguments[6] -cne '--verify-tag' -or
    $calls[2].Arguments[-2] -cne '--notes-file' -or
    $calls[2].Notes -notmatch 'Product version: 261003' -or
    $calls[2].Notes -notmatch 'Release tag: 261003r9abc' -or
    (Test-Path -LiteralPath $calls[2].Arguments[-1])) { throw 'Publishing identity, release notes, or notes cleanup drifted' }
$failPush = $true
$calls.Clear()
$refused = $false
try { . ([scriptblock]::Create($publish.Extent.Text)) }
catch { if ($_.Exception.Message -notmatch 'Failed to push tag 261003r9abc') { throw }; $refused = $true }
if (-not $refused -or $calls.Count -ne 2) { throw 'Failed tag push did not stop release creation' }
$failPush = $false
$failRelease = $true
$calls.Clear()
$refused = $false
try { . ([scriptblock]::Create($publish.Extent.Text)) }
catch { if ($_.Exception.Message -notmatch 'Failed to create GitHub Release 261003r9abc') { throw }; $refused = $true }
if (-not $refused -or $calls.Count -ne 3 -or (Test-Path -LiteralPath $calls[2].Arguments[-1])) {
    throw 'Release failure did not propagate or clean up notes'
}
# Deliberate native-command failures above are successful negative fixtures.
$global:LASTEXITCODE = 0
Write-Output 'Automatic release tags, noninteractive parameters, duplicate checks, stamping, and mocked publishing passed.'
