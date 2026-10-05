# TSR-0040 - Release Preflight and Output Safety

Status: Monitoring
Date Opened: 2026-10-03
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Windows release tooling, Git, module files
Trigger Source: Chat request
Led To ADR(s): ADR-0242
Tags: release, PowerShell, CRLF, ownership, publication

## RCA Summary

- What happened: release preparation rejected module metadata after ordinary
  tidy, then reported dirty source without showing the responsible Git paths.
- Why: the working checksum file used CRLF while canonical tidy output used LF.
  Git normalized the content for status, and ordinary tidy skipped rewriting
  unchanged checksums. A deleted tracked `gocluster.exe~` was observed during
  one dirty-state investigation; later status snapshots cannot prove every
  earlier invocation's state.
- What fixed it: LF module attributes, explicit Git-path diagnostics, verified
  output ownership/preparation, process-state restoration, and commit/repository
  binding under ADR-0242.
- How we know: the original 268 removed/added checksum lines had identical text;
  extracted clean-status logic passed with no entries and rejected a deleted
  file. Native behavior and package validation are covered by the commands in
  Verification and Monitoring.
- Operator/support answer: inspect the exact gate and its evidence. A manual
  build is unnecessary. Move legacy or edited output packages aside after
  inspection; keep deployed runtime state separate from build destinations.

## Triggering Request

- Request date: 2026-10-03; implementation approved 2026-10-04.
- Request summary: determine release-script root causes, architect fixes, and
  implement the explicitly approved Scope Ledger v1.
- Request reference: maintainer troubleshooting chat and exact approval.

## Symptoms and Impact

`go mod tidy -diff` failed on a Git-clean CRLF `go.sum`. A separate clean-source
exception omitted the offending paths. Static review found unverified recursive
cleanup, environment leakage, tag-target drift, remote/GitHub target mismatch,
and failure-to-absence conversion in local-tag and release lookups.

## Timeline

1. 2026-10-03 - Compared the pasted and live tidy output; confirmed identical
   line text and working-file CRLF.
2. 2026-10-03 - Exercised the actual clean-worktree helper; clean status passed
   and deleted-file status reproduced the refusal. Revised the earlier overly
   strong explanation based on a later clean snapshot.
3. 2026-10-04 - Refreshed the clean baseline, accepted detailed failure-fixture
   refinements, and implemented approved release-safety changes.
4. 2026-10-04 - Final failure fixtures found raw Windows path normalization and
   failed-backup-disposal cleanup gaps; fixed both and added regression checks.

## Hypotheses and Tests

1. Dependency versions/checksums changed: rejected for the captured tidy diff;
   all 268 removed/added lines contained identical text.
2. PowerShell counts empty Git output as dirty: rejected by the extracted helper
   on the actual clean repository. Native status output had zero entries.
3. Plain tidy guarantees canonical checksum bytes: rejected by installed Go
   source, which skips writing unchanged checksum entries; its diff path renders
   canonical LF output separately.
4. Every pasted dirty refusal came from the tracked backup deletion: inconclusive;
   that deletion was observed at one point, but historical invocation status was
   not captured by the original script.

## Findings

- Root cause (or best current explanation): distinct clean-source and module
  freshness gates with insufficient diagnostics and inconsistent byte-level
  hygiene under Windows checkout conversion.
- Contributing factors: destructive preparation preceded compilation; publication
  helpers did not distinguish unknown lookup state or freeze identity/destination.
- Durable decision required: ownership-based output replacement and the narrow
  final generated-output exception change operational behavior.

## Decision Linkage

- ADR created: ADR-0242, Release Preparation Safety.
- Decision delta summary: preserve source/module gates, constrain replacement,
  bind publication, restore caller state, and permit governed Codex release edits.

## Verification and Monitoring

- Commands: `scripts/test-release-identity.ps1` and
  `scripts/test-release-safety.ps1` under PowerShell 7 and Windows PowerShell 5.1;
  `go mod tidy -diff`; isolated real package-only build and ZIP/metadata checks;
  workflow/troubleshooting checks; generated-map freshness and `git diff --check`.
- Observed fixture results: 100 safety cases passed with no skips on each of
  PowerShell 7.6.6 and Windows PowerShell 5.1; release-identity and workflow
  fixtures passed on both. A separate one-drive fixture correctly reported the
  unavailable external-drive check as skipped. Actual Git 2.51.0 lookup returned
  0 for a present tag, 2 for absence, and 128 for an execution failure.
- Observed package results: two real PowerShell 7 package-only builds verified
  unchanged-output replacement; a final Windows PowerShell 5.1 build wrote its
  ZIP on a different drive. Both engines produced 36 allowlisted ZIP entries;
  archived executable hashes matched `binaries.json`, stamped version/tag/commit
  matched the captured identity, private ownership metadata stayed out of the
  ZIP, and invocation scratch/rollback artifacts were cleaned.
- Signals to monitor: explicit Git-path refusal, ownership-content refusal,
  lookup error versus absence, and reported recovery paths.
- Rollback triggers: a fixture permits unrelated data deletion, hides source
  edits, publishes to another repository/commit, or loses both prior output copies.
- Live publication is not part of this validation; retain Monitoring until a
  separately authorized release confirms the operational path.

## References

- Related ADRs: ADR-0076, ADR-0078, ADR-0221, ADR-0236, ADR-0242.
- Related docs: `scripts/README.md`, `README.md`, `customgpt/troubleshooting-index.md`.
