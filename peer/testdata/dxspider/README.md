These fixtures execute EA3CV/DXSpider commit
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d` through its actual Perl modules.
`DXVars.pm` supplies an isolated site configuration. The test verifies both the
checkout commit and the absence of modifications to the receiver source/data.

Run from PowerShell:

```powershell
./scripts/pc92-dxspider-interop.ps1 -DXSpiderRoot C:/temp/dxspider -PerlPath C:/temp/perl/perl/bin/perl.exe -PerlLibrary C:/temp/perl/deps/lib/perl5 -PerlDLLDirectory C:/temp/perl/c/bin
```

The Perl runtime must provide the dependencies imported by the pinned source,
including Mojo::IOLoop, DB_File, DBI, JSON, Data::Structure::Util and
Net::CIDR::Lite. The script checks prerequisites; it installs nothing and
restores its process environment afterward. Ordinary Go tests skip the external
suite when its explicit environment configuration is absent. That skip is not
interoperability evidence.

The receiver harness runs unmodified `Msg::_rcv`, `ExtMsg::dequeue`,
`DXProt::normal`, PC18/PC92 handlers, `DXUser` with an actual temporary BerkeleyDB,
and the actual `Route` classes. A socket adapter captures output writes; incoming
records pass through real framing in bounded chunks. Tests exchange those
outputs with a real GoCluster session over `net.Pipe` in both directions and
exercise legacy negotiation. This proves receiver-component interoperability.
It does not claim a full DXSpider daemon deployment, OS network/listener
qualification, or interoperability with a running CCCluster implementation.

Required results include channel/user/route metadata after PC18 and K, all 1,000
users plus 64 peer memberships from a 62,171-byte C frame, and the existing-user
IP recovery that requires C followed by A. Tests obtain expected state by
inspecting actual receiver objects, not by duplicating receiver algorithms.

Timestamp cases use the production Go controller, timestamp generator and sole
session writer, with explicitly scheduled boundary seconds. The actual receiver
accepts all 100 values in one second, receives one coalesced K after 150 periodic
requests, and accepts C/A membership replacement across UTC midnight while
rejecting a delayed pre-midnight replay. A fresh sender controller exercises the
startup-second wait against the same running DXSpider receiver and its retained
`Route.lastid`; the receiver rejects a same-second integer replay and accepts the
following second's C/A. This is a sender-component restart, not an OS-process or
full-daemon reconnect test. Only the receiver's normal runtime clock input is
controlled; its route membership and freshness state are never rewritten.
