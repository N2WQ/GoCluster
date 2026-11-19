# Call Correction v2 Scope Ledger v9: Integration And Publication

Status: authorized by exact `Approved v9` on November 19, 2026.
Main baseline: `6a072b7bc8d118392d1aab0ce8eb6fd5827c7926`.
Correction source: `9523dbbc4706a22122b380281b2358b2ea22cee5`.
Common ancestor: `3fd37c6cb48d2028756ca18184d4f18a8dc95bb0`.

## Approved scope

Integrate the complete correction branch into an isolated main-based checkout
using a squash commit, preserving main's five newer commits. Keep the original
correction branch/history. Preserve main's CustomSCP, configuration path identity,
shutdown and Telnet behavior. Resolve mechanical conflicts only; a new behavior
choice requires reassessment. Renumber the correction ADRs to unused 0360/0361
and update corresponding references without changing main's 0358/0359 records.
The same mechanical index reconciliation assigns correction's duplicate
TSR-0137 to unused TSR-0139, preserving main's CustomSCP TSR-0137.

Keep C and the legacy Bayesian bonus disabled and B replay-only. Run integrated
ordinary/race/static and connected CustomSCP checks, inspect both parent histories,
update affected documentation, and generate/check maps after final changes.
Advance primary main by fast-forward only, preserve unrelated runtime files,
push main and the original correction branch without force, and verify remote
commit IDs. Recheck concurrent local/remote movement before publication.

This publishes source only. It does not deploy, restart production, activate a
method, install a model, waive failed CPU/reference qualification, or reopen
November 17+ archives. No new fitting, optimization or evaluation campaign.

## Risk and validation disposition

Main and correction source overlap in configuration, cluster bootstrap/runtime,
contracts, indexes, support routing and maps. A green merged suite could silently
omit main behavior or miss connected persistence. The separate read-only scope
challenge required explicit parent-source comparison and ordinary/race connected
CustomSCP regression; both refinements were adopted. No remaining scope blocker
was identified. This is not an independent scientific or RF-truth review.

Postapproval contract-to-check matrix:

| Contract | False-green case | Required distinguishing evidence |
| --- | --- | --- |
| Preserve both branches | Conflict resolution drops main fsutil, SCP stats, shutdown or Telnet work | Compare integrated source against both histories and inspect the overlapping functions/config |
| Preserve correction and SCP consumers | Independent fixtures pass without producer-to-consumer persistence | Native full ordinary/race lanes plus `scripts/test-customscp-regression.ps1` ordinary and `-Race` |
| Experimental paths remain inactive | Parser accepts new settings while shipped values activate them | Inspect effective configuration and existing config/live/replay tests; C/legacy Bayes false, no live B loader |
| Integrated code remains sound | Branch-only passes reused for changed combined tree | Fresh full Windows ordinary/race, vet, Staticcheck, lint; targeted reruns after relevant fixes |
| Documentation remains navigable | Duplicate ADR IDs or stale TSR links survive compilation | Narrow ADR renumbering/reference audit, troubleshooting-record check, regenerated/fresh maps and final diff |
| Primary and remote publication preserve work | Runtime data overwritten or concurrent main discarded | Record runtime-file hashes, inspect dirty paths, verify expected refs, fast-forward only and compare local/remote SHAs |

The detailed gate is adequate for this integration scope. Existing tests/runners
provide the necessary seams; no new test framework is authorized. Functional or
algorithmic fixes beyond mechanical integration require revised scope.

Execution: integration and selected local validation completed; see the
[scope-to-evidence mapping](call-correction-v2-v9-validation.md). Publication
preserves the unchanged qualification and activation boundaries above.

Exact authorization required before implementation: Approved v9
