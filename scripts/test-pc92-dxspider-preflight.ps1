<#
.SYNOPSIS
    Verify missing DXSpider Perl dependencies stop qualification and restore environment.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'pc92-dxspider-interop.ps1'
$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ('gocluster-dx-preflight-' + [guid]::NewGuid().ToString('N'))
$names = @('DXSPIDER_ROOT','DXSPIDER_PERL','DXSPIDER_PERL_LIB','PATH','LC_ALL')
$previous = @{}
foreach ($name in $names) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue | Select-Object Value
$global:FixtureGoCalls = 0
function go { $global:FixtureGoCalls++; $global:LASTEXITCODE = $global:FixtureGoExit }
try {
    New-Item -ItemType Directory -Path $fixtureRoot | Out-Null
    $perl = Join-Path $fixtureRoot 'perl-fixture.ps1'
    @'
if (-not ($args -contains "-MJSON") -or -not ($args -contains "-MMath::Round")) { throw 'Missing JSON or Math::Round preflight argument' }
$global:LASTEXITCODE = $global:FixturePerlExit
'@ | Set-Content -LiteralPath $perl
    foreach ($case in @('missing-JSON','missing-Math-Round','success','qualification-failure')) {
        $global:FixtureGoCalls = 0
        $global:FixturePerlExit = if ($case -like 'missing-*') { 19 } else { 0 }
        $global:FixtureGoExit = if ($case -eq 'qualification-failure') { 29 } else { 0 }
        $caught = $null
        try {
            & $runner -DXSpiderRoot $fixtureRoot -PerlPath $perl -PerlLibrary 'fixture-lib' -PerlDLLDirectory $fixtureRoot
        } catch { $caught = $_ }
        if ($case -like 'missing-*') {
            if (-not $caught -or -not $caught.ToString().Contains('DXSpider runtime dependencies are unavailable')) { throw "Expected dependency rejection, got $caught" }
            if ($global:FixtureGoCalls) { throw 'Go qualification ran after failed Perl preflight' }
        } else {
            if ($global:FixtureGoCalls -ne 1) { throw 'Expected exactly one Go qualification after successful preflight' }
            if ($case -eq 'success' -and $caught) { throw $caught }
            if ($case -eq 'qualification-failure' -and (-not $caught -or -not $caught.ToString().Contains('qualification failed with exit code 29'))) { throw 'Expected Go qualification failure' }
        }
        foreach ($name in $names) {
            if ([Environment]::GetEnvironmentVariable($name, 'Process') -cne $previous[$name]) { throw "Environment $name was not restored" }
        }
        Write-Host "PASS $case prerequisite/qualification control flow and caller environment restoration"
    }
} finally {
    # A simulated failure must not become the successful fixture process's exit code.
    if ($savedExitCode) { $global:LASTEXITCODE = $savedExitCode.Value }
    else { Remove-Variable LASTEXITCODE -Scope Global -ErrorAction SilentlyContinue }
    foreach ($name in $names) {
        if ($null -eq $previous[$name]) { Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue }
        else { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
    }
    $resolved = [IO.Path]::GetFullPath($fixtureRoot)
    $tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $resolved.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase)) { throw 'Fixture cleanup escaped temp directory' }
    Remove-Item -LiteralPath $resolved -Recurse -Force
}
