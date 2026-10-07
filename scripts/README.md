# GoCluster Scripts

## Development And Launcher Validation

Use Go 1.27.1 and the analyzer pins in `docs/dev-runbook.md`. Run
`verify-agentic-tools.ps1` in the actual development shell; quiet mode still
runs version probes. Its fixtures are `test-agentic-tools.ps1` and the DXSpider
dependency preflight fixtures are `test-pc92-dxspider-preflight.ps1`.

From a Windows source checkout, `pwsh -NoProfile -File ./launch-cluster.ps1`
builds one Windows amd64 cluster/peerdiag pair and launches that exact pair.
`logs/cpu-*.pprof` selects the strict PGO route; an absent/empty profile set
selects `build-executable-pair.ps1` with `-pgo=off`. A malformed profile, failed
merge, or failed build is an error, with no launch or stale-binary fallback.
The standalone `consolidate-and-build-pgo.ps1` still requires profiles and the
matching root `gocluster.exe` that captured them.

Both routes publish a unique directory under `.tmp/ordinary` or `.tmp/pgo`
only after both builds and the hash manifest succeed. They return an object
with `Mode`, `OutputDirectory`, `ClusterPath`, `PeerDiagnosticPath`, and
`ManifestPath`. Prior pairs and root executables remain intact. The launcher
uses the repository's config directory and restores the caller's location and
configuration environment after the process exits. Pair-build target settings
are restored too. This does not start a Linux server from WSL or qualify PGO
performance; matching capture provenance still matters.

Run `test-consolidate-and-build-pgo.ps1` and `test-launch-cluster.ps1` for
successful publication, build/merge failures, exact fresh-path handoff, and
caller-state preservation. These fixtures use controlled tool/process stubs;
they do not start a production cluster.

Tracked PowerShell scripts in this directory are operational tooling for local
builds, release packaging, profiling, console setup, workflow checks, and Codex
skill installation.

Release and PGO scripts stamp the UTC compile date as exactly `YYMMDD`, without
a prefix, commit suffix, or dirty suffix. Commit and build time are stamped
separately; GoCluster does not display or broadcast a dirty flag. Release
tags and names automatically use `YYMMDDrhhhh`, where `hhhh` is the last four
characters of the captured full Git commit hash, for example `261006r9abc`.
No release number is supplied or prompted for. `-PackageOnly` stamps an
intended tag without publishing it. Duplicate tags are rejected, including
same-day retries of the same commit; plain and PGO builds omit release-tag metadata. See [build notes](../README.md#build-and-service-notes).

Run `test-release-identity.ps1` for parameter, stamping, duplicate-target, and
mocked publishing checks; its Git/GitHub operations never reach external tools.
Run `test-release-safety.ps1` for disposable Git/native-command/filesystem
fixtures, failure ordering, output ownership, and caller-state restoration.

## Release Preparation

`create-release.ps1` builds both binaries itself. A manual `go build` is not a
prerequisite. Normal releases require clean committed source and fresh module
files/code maps; a refusal includes the repository and offending Git paths.
`-AllowDirty` and `-SkipCodeMapCheck` remain package-only exceptions. Root
`go.mod` and `go.sum` use LF even with `core.autocrlf=true`; ordinary `go mod tidy`
can leave an otherwise tidy CRLF `go.sum` untouched, while `tidy -diff` rejects it.

Preparation uses one ignored `.tmp/release-<id>/` directory. Existing output
directories and ZIPs are replaced only when the private
`.gocluster-release-owner.json` manifest proves their origin and unchanged
contents. It records all staged files/directories and the ZIP hash, and is
written after archiving so it is excluded from the shipped ZIP. Legacy
`ready_to_run/` directories and ZIPs without this manifest are accepted without
manual migration. After the replacement is ready, their contents are preserved
under the reported `.tmp/release-<id>/previous-stage/` and
`previous-package.zip` paths. A lone legacy directory or ZIP is also preserved.
These backups are retained, never automatically deleted. Marked packages with
edited or missing contents or additional runtime files are still refused.
Keep a previously run package as a deployment directory, separate from build
outputs. The script does not delete legacy nested staging directories.

Custom `-PackageDirectoryName` and `-PackageName` are single Windows path
components; bracket-containing names are supported. Reserved device names,
traversal, aliases, Git/source collisions, and junction/symlink traversal are
refused. `-OutputDir` is relative to the repository root or a fully qualified
absolute directory; paths such as `C:`/`C:folder` and `\folder` are refused.
Default outputs remain repo-root `ready_to_run/` and `gocluster-windows-amd64.zip`.
Custom artifacts created by the current invocation are the only final source
check exclusions. Existing custom outputs must be ignored or moved aside to
satisfy the initial comprehensive clean-worktree gate; no ignore settings are
changed automatically.

`-Remote` selects one Git push URL and the same explicit GitHub host/repository
for lookup and publication, overriding ambient `GH_REPO`/`GH_HOST`. Supported
URLs use HTTPS, `ssh://`, or `git@host:owner/repository`; ambiguous push URLs are
refused. Repository push access must be verifiable so draft releases are visible.
Git/GitHub lookup failures stop preparation; only established absence permits a
new release. The annotated tag points explicitly to the captured full commit ID.

Source and output directories must have no concurrent writers. Persistent HEAD
or source changes abort publication; rechecks cannot detect transient changes
that are reverted between checks. Caller location and `GOOS`/`GOARCH` are restored
on every exit. The code-map checker and README renderer run with
`CGO_ENABLED=0`, restoring the caller setting on success and failure. Module
tidy and both packaged binary builds retain the caller CGO setting. This avoids
the observed CGO helper execution denial without changing shipped binaries;
other Windows Application Control denials still require policy diagnosis.
Local output promotion retains recoverable previous artifacts;
failed recovery or backup disposal preserves and reports the retained paths.
Tag/push/release failures require
manual inspection of local/remote state before retrying, without automatic ref
deletion. See [ADR-0242](../docs/decisions/ADR-0242-release-preparation-safety.md)
and [TSR-0040](../docs/troubleshooting/TSR-0040-release-preflight-and-output-safety.md).

## Operational Helpers

- `watch-voacap-ssn.ps1` runs the repo-local lightweight SSN watcher
  (`cmd/voacap_sunspot_watch`) to watch NOAA fetches, raw SSN, rounded EWMA
  SSN, recompute delta, and recompute markers without launching VOACAP
  forecasts. Example:

  ```powershell
  .\scripts\watch-voacap-ssn.ps1
  ```

## Pinned DXSpider Spot Interoperability

The peer reference tests use DXSpider revision
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`, selected with `DXSPIDER_ROOT`.
`DXSPIDER_PERL` selects an existing Perl runtime; both variables are required
for the externally configured tests. Optional `DXSPIDER_PERL_LIB` adds its
dependency search path. These harnesses are test tooling, not runtime peer
transports:

- `spot-dxspider-generate.pl` calls the pinned PC11, PC61 and PC26 generators
  with a controlled test clock in a separate process and temporary state.
  It retains the real `cldate` formatter; days 1-9 therefore have one leading
  ASCII space. Sender-generated cases cover days 1, 9, 10 and 31. PC26 emits
  no hop, so its native-input case establishes local admission and timestamp,
  while hop-bearing relay is tested separately.
- `spot-dxspider-interop.pl` sends production writer bytes through the pinned
  receiver and observes its own cache and disk storage. Receiver behavior such
  as trimming or rounding is separate from GoCluster's byte preservation.

The Go harness supplies `DXSPIDER_TEST_STATE`, `GOCLUSTER_ROOT` and the decimal
`DXSPIDER_TEST_AT` timestamp for each one-shot sender process. Optional
`DXSPIDER_TEST_COMMENT` and `DXSPIDER_TEST_IP` select test payloads; unchanged
defaults are `CQ TEST` and `203.0.113.7`. The Go default helper supplies those
values explicitly. The PC11/PC61 generators perform their own caret-to-tilde
replacement; PC26 receives literal comment tildes and emits no hop. Its stdout
is a
JSON object containing base64 PC11, PC61 and PC26 sentences; the actual modules
format those sentences. These test-only variables do not affect cluster runtime.

See [sender-date tests](../peer/spot_relay_date_test.go),
[sender-comment/native framing tests](../peer/spot_relay_framing_test.go),
[receiver tests](../peer/spot_relay_interop_test.go) and
[TSR-0039](../docs/troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md)
for commands, actual results and claim boundaries. Standard suites skip
externally configured cases when the reference runtime is unavailable; those
skips do not establish interoperability. Literal comment preservation is checked
through native input, production writers and a second native reader; pinned
receiver storage is observed separately. Dotted IPv4 tails in legitimate IPv6
are accepted by GoCluster but rejected by the pinned receiver's narrower IP
predicate. Use a fresh fixture/key for each positive IP case because existing
dedupe identity excludes IP text.

## Workflow Checkers

- `measure-codex-workflow-context.ps1` reports declared Codex instruction-path
  scenarios at immutable Git revisions using deterministic words, characters,
  and UTF-8 bytes. Results are informational context-footprint proxies, not
  adoption gates, model-token evidence, or quality proof.
- `test-measure-codex-workflow-context.ps1` exercises the measurement script in
  a disposable Git repository and proves dirty worktree bytes cannot alter a
  pinned candidate comparison.

- `check-workflow-contract.ps1` verifies mechanically representable Codex
  authority routes, positive and negative risk routing, retired Codex-only
  requirements, references, optional changed-path exclusions, and the
  repository's push-CI, Codex-contract-CI, nightly-race, conditional-check,
  workflow-permission, and Actionlint invariants. It explicitly disclaims
  conversational, hosted-run, and engineering proof.
- `test-workflow-contract.ps1` runs positive and named negative fixtures. Each
  negative case asserts its invariant-specific failure so an unrelated checker
  error cannot create a false green.
- `check-yaml-doc-rigor.ps1` checks first-party runtime YAML headers and
  comment-only YAML scope.
- `check-go-crawler-entry-comments.ps1` checks changed support-critical Go files
  for package/file entry comments. It is a mechanical review aid; source-aware
  review still decides whether comments explain useful intent and why.
- `update-code-maps.ps1` regenerates checked-in Markdown code maps from Go
  package metadata and ADR records.
- `check-code-maps.ps1` verifies checked-in Markdown code maps are fresh without
  modifying files. Use this in CI and release freshness gates.
- `check-troubleshooting-records.ps1` verifies troubleshooting-log rows and
  `docs/troubleshooting/TSR-*.md` records stay link-complete, status-aligned,
  ADR-linked, and readable through the required `RCA Summary` block.
- `check-support-agent.ps1` verifies the custom GPT support-agent deployment
  bundle, support-route contracts, bounded support search, routing docs, local
  Worker behavior, and optional deployed public Worker health without
  credentials.
- `evaluate-support-agent.ps1` runs the local support-agent eval harness against
  `docs/support-agent-eval-cases.json`, using the checked-in Worker and current
  workspace files to validate retrieval/source coverage and optionally score
  pasted or live-generated answers.
- `verify-agentic-tools.ps1` checks required repo workflow tools and required
  semantic/navigation helpers, then reports recommended or optional
  investigation helpers separately so missing optional tools do not block
  ordinary Go work. It also reports optional dependency-visualization helpers
  such as Graphviz `dot` and `goda`; summarize durable graph findings in
  `docs/code-maps/` when the custom GPT support agent should use them later.

## Header Standard

Every tracked first-party `.ps1` script should start with PowerShell
comment-based help before executable statements:

```powershell
<#
.SYNOPSIS
  One-line purpose.

.DESCRIPTION
  What the script does, when to use it, and what it changes.

.PARAMETER Name
  Parameter meaning and default.

.NOTES
  Prerequisites: required tools, auth, binaries, logs, or environment.
  Side effects: files created, processes started, releases published, or state changed.
  Safety: dirty-worktree behavior, secret handling, generated artifacts, or production cautions.
#>
```

Use the header as local context for operators, support agents, and developers.
The script body remains authoritative for actual behavior. Header-only updates
must not change parameters, commands, generated paths, process launch behavior,
release publishing behavior, profiling cadence, or local Codex skill state.
