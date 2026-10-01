[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$DXSpiderRoot,
    [Parameter(Mandatory = $true)][string]$PerlPath,
    [string]$PerlLibrary = '',
    [string]$PerlDLLDirectory = ''
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$referenceRoot = (Resolve-Path -LiteralPath $DXSpiderRoot).Path
$interpreter = (Resolve-Path -LiteralPath $PerlPath).Path
$environmentNames = @('DXSPIDER_ROOT', 'DXSPIDER_PERL', 'DXSPIDER_PERL_LIB', 'PATH', 'LC_ALL')
$previousEnvironment = @{}
foreach ($name in $environmentNames) {
    $previousEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
try {
    $env:DXSPIDER_ROOT = $referenceRoot
    $env:DXSPIDER_PERL = $interpreter
    $env:DXSPIDER_PERL_LIB = $PerlLibrary
    $env:LC_ALL = 'C'
    if ($PerlDLLDirectory) {
        $dllDirectory = (Resolve-Path -LiteralPath $PerlDLLDirectory).Path
        $env:PATH = $dllDirectory + [IO.Path]::PathSeparator + $env:PATH
    }
    $perlArguments = @()
    if ($PerlLibrary) { $perlArguments += @('-I', $PerlLibrary) }
    $perlArguments += @('-MDB_File', '-MDBI', '-MMojo::IOLoop', '-MData::Structure::Util', '-MNet::CIDR::Lite', '-e', 'print "DXSpider runtime prerequisites available\n"')
    & $interpreter @perlArguments
    if ($LASTEXITCODE -ne 0) { throw 'DXSpider runtime dependencies are unavailable; no interoperability claim can be made.' }
    & go -C $repositoryRoot test ./peer -run '^TestDXSpiderReference' -count=1 -v -timeout=2m
    $qualificationExitCode = $LASTEXITCODE
    if ($qualificationExitCode -ne 0) { throw "DXSpider receiver qualification failed with exit code $qualificationExitCode" }
}
finally {
    foreach ($name in $environmentNames) {
        [Environment]::SetEnvironmentVariable($name, $previousEnvironment[$name], 'Process')
    }
}
