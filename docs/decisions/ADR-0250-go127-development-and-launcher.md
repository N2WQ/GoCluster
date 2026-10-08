# ADR-0250: Go 1.27 Development And Fresh Launcher Builds

- Status: Accepted (Codex environment selection superseded by ADR-0259)
- Date: 2026-10-06
- Decision Origin: Troubleshooting chat

## Context

The migrated checkout retained a Go 1.26 module requirement and analyzers that
could not load Go 1.27 export data. Project skills existed outside Codex's
discovery directory. The Windows launcher ran the strict PGO builder twice,
then selected an old executable from the repository root instead of its newly
published isolated pair. No CPU profiles were present.

## Decision

- Require Go 1.27.1, target Go 1.27 in lint, and pin Staticcheck v0.8.1 and
  golangci-lint v2.14.0 in CI and enforcing fixtures.
- Keep one authoritative skill bundle in real `.agents/skills` directories.
  Preserve specialist methods and repo authority. Metadata checks and actual
  Codex inventory checks establish different facts; verify both.
- Use WSL2 for Codex, with Linux tools installed inside Ubuntu 24.04. Keep the
  existing checkout authoritative and the operational launcher Windows-native.
- The launcher chooses exactly one build route. Existing CPU profiles select
  PGO; no profiles select a fresh ordinary build. The standalone PGO builder
  continues to reject missing profiles, and failed profiled builds cannot
  silently fall back to ordinary builds.
- Publish a unique cluster/peerdiag pair only after both builds succeed. Launch
  the exact path returned by that invocation. Preserve root binaries and prior
  published pairs; remove only an invocation's owned staging directory.
- Required tool version probes must succeed, with quiet mode affecting output
  only. Preserve existing tool groups and caller environment. DXSpider preflight
  includes the external receiver's JSON and Math::Round dependencies.
- Preserve direct-to-main, post-push CI and nightly race verification.

## Alternatives considered

1. Keep Go 1.26 analyzer workarounds: rejected because the requested target is
   Go 1.27 and matching supported analyzers are available.
2. Link or copy skills into discovery: rejected because Windows symlink
   privileges and user-level copies introduce portability or drift problems.
3. Relax the standalone PGO builder: rejected because ordinary launcher startup
   can use a separate route while preserving the strict profile contract.
4. Select the newest-looking root binary: rejected because timestamps do not
   prove this invocation built a successful matching executable pair.

## Consequences

### Benefits

The checkout carries discoverable skills and supported Go 1.27 validation.
Startup without profiles builds current source, and build failures cannot launch
stale executables or replace previous pairs.

### Risks

Linux login, user settings, external references, and tools remain machine-local.
Windows Application Control can deny CGO executables despite successful builds.
WSL validation does not establish native Windows execution permission. Pair
hashes identify artifacts; they do not establish PGO performance qualification.

### Operational impact

Reinstall matching analyzers and reload Codex skills after migration. Run the
launcher from its checkout; do not promote isolated artifacts by manually
overwriting a companion belonging to another binary. Set Linux passwords and
perform account login interactively. Do not disable OS security policy to make
tests green.

## Links

- Related tests: workflow-contract fixtures, launcher/build fixtures, tool
  verifier fixtures, configured DXSpider sender/receiver qualification
- Related docs: `docs/ENVIRONMENT.md`, `docs/dev-runbook.md`,
  `.agents/skills/README.md`, `scripts/README.md`, `launch-cluster.ps1`
- Related TSRs: TSR-0038
- Supersedes / superseded by: selectively supersedes ADR-0155's source-directory
  choice; repo authority and machine-local responsibility remain accepted.
  [ADR-0259](ADR-0259-native-windows-codex-environment.md) supersedes only the
  WSL2 selection for Codex; the other decisions above remain accepted.
