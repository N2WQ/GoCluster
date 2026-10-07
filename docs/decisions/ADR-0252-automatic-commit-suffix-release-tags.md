# ADR-0252: Automatic Commit-Suffix Release Tags

- Status: Accepted
- Date: 2026-10-06
- Decision Origin: Design

## Context

Mandatory release-number input interrupts builds. The owner selected automatic
release tags such as `261006r9abc` and approved Scope Ledger v1.

## Decision

Release-script builds use `YYMMDDrhhhh`: the UTC build date followed by `r`
and the last four lowercase hexadecimal characters of the captured full Git
commit hash. Remove `-ReleaseNumber`; no interactive number input remains.
Package-only builds stamp the same intended identity without publication.

Keep the date-only product version and separate longer commit/build-time
metadata. Plain and PGO builds continue to omit release-tag metadata. Git tags,
GitHub Release names, notes, and duplicate checks use the generated release tag.
Existing source, publication, ownership, and duplicate-refusal gates remain.

## Alternatives considered

1. Explicit numbering: rejected because it requires manual input.
2. Last four characters of the abbreviated hash: rejected; they are not the
   ending of the full source commit hash.
3. A longer suffix: improves collision resistance but differs from the owner's
   selected compact format.

## Consequences

### Benefits

- Release identity is generated without a prompt or numbering bookkeeping.
- The suffix ties the release label to captured committed source.

### Risks

- Distinct commits can share four ending characters. A same-day collision is
  rejected by existing duplicate checks; the suffix is not a unique commit ID.
- Releasing the same commit twice on the same UTC day is rejected.

### Operational impact

- Invoke `create-release.ps1` without `-ReleaseNumber`; old invocations using
  that removed parameter fail binding.
- UTC midnight changes the tag date. Dirty package-only builds identify HEAD,
  not uncommitted changes; longer commit and timestamp metadata remain separate.

## Links

- Related tests: `scripts/test-release-identity.ps1`, `scripts/test-release-safety.ps1`
- Related docs: `README.md`, `scripts/README.md`, `peer/README.md`, `customgpt/source-map.md`, `customgpt/operator-guide-index.md`
- Supersedes: release-number parameter and numbered-tag clauses of [ADR-0236](ADR-0236-date-only-build-version.md).
- Preserves: [ADR-0242](ADR-0242-release-preparation-safety.md) release preparation and publication safety.
