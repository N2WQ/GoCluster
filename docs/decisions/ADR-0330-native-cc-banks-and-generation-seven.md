# ADR-0330: Native CC Banks and Generation Seven

- Status: Accepted
- Date: 2026-10-29
- Decision Origin: Design

## Context

CC was a command dialect over the GO Boolean filter. Familiar CC categories
could not be inspected or edited reliably without interpreting arbitrary GO
expressions. The owner selected independent banks, equivalent starting defaults,
and a fresh start that also clears old named presets. The exact approved
contract is [Scope Ledger v4](../cc-dialect-v4-scope-ledger.md).

## Decision

Keep one GO Boolean bank and one native CC bank per full login identity. Each
bank owns its six delivery switches. Shared preferences and selected dialect
remain in the same profile. Selecting a dialect activates its saved bank.

CC owns explicit modes and human/skimmer sources, four geography categories with
optional native-band overrides, and DXBM's fixed frequency slots. Commands,
native YAML, inspection and matching use the same state. Native modes apply to
both report sources. Frequency classification uses the reported frequency and
the fixed CC table. Per-band terminal syntax and CC User integration remain
unverified; per-band geography initially uses GoCluster YAML.

Generation seven stores both banks, independent tagged preset associations and
fixed reset/acknowledgement metadata. Full schema-seven CONFIG restores both
banks; ordinary filter and PATCH edits own only the selected bank. Schema six
remains a bounded legacy GO/settings interface and is not a complete backup.

The one-time upgrade discards all earlier profile filters and six switches,
starts both banks from current operator defaults, and clears pre-seven presets.
It preserves configured shared preferences and selected dialect. Each eligible
reset replacement requires admission and verification of its original backup.
Ordinary saves and notice acknowledgements retain their established persistence
contracts. Library reset and its completion marker share one replacement; later
presets must survive profile/notice retries from every SSID.

Completed resets and notification are separate. A bounded writer receipt after
complete flush permits a strict, current-owner metadata-only acknowledgement.
An acknowledgement failure may repeat the historical notice, never the reset.
Waiting holds no preparation reservation or configuration lease.

Profile/preset byte and decode-work bounds remain independent. Native preparation
and wrapped exports must fit their approved limits before publication or reset.
Process-wide input/output reservations bound concurrent configuration work and
queued responses through decode and writer flush/discard respectively. Matching
uses immutable prepared generations outside client publication locks.

All preset verbs, notice acknowledgements and final saves acquire the bounded
preparation owner before storage/capture.
A closed current owner waits for its writer's mandatory buffer/reservation drain
before preparing a final save; logging and optional reporting occur after that
completion signal. The fixed per-connection signal adds no background worker.
Preparation-only ownership permits no enqueue, including after socket closure.

## Alternatives considered

1. One common Boolean filter: rejected because CC edits would need to interpret
   or replace complex GO expressions.
2. Translate saved GO expressions into CC categories: rejected because the
   representations are not equivalent.
3. Preserve old filters or presets during migration: rejected by the owner's
   explicit fresh-start decision.
4. Repeat library reset when an SSID still needs profile/notice work: rejected
   because it could erase presets created after the first completed reset.

## Consequences

### Benefits

- Predictable native CC inspection and editing without changing the GO bank.
- Complete two-bank backups and tagged presets with announced dialect switches.
- Retryable migration and notices with independently evidenced completion.

### Risks

- First upgrade intentionally changes selections and removes old presets.
- Switching banks can change the spot stream; existing automatic reading pauses
  still suppress spots without replay.
- Compatibility choices identified in the ledger are GoCluster behavior, not
  claims of verified native CC or CC User equivalence.

### Operational impact

Deploy the updated executable and its matching taxonomy/HELP assets together.
No live upgrade or deployment is authorized by implementation approval. Rollback
requires stopping writers and restoring a matching old executable, profiles and
shared library from pre-upgrade backups. Preserve the current files first;
restoring old backups loses later changes across affected SSIDs.

## Links

- Approved contract: [Scope Ledger v4](../cc-dialect-v4-scope-ledger.md)
- Implementation and validation: [Execution record](../cc-dialect-v4-execution.md)
- Tests: `filter/cc_*_test.go`, `filter/upgrade_v7_test.go`,
  `telnet/configuration_v7_integration_test.go`, `telnet/reset_notice_v7_test.go`
- Supersedes the shared CC/Boolean ownership and generation/reset clauses of
  ADR-0317, ADR-0318, ADR-0319, ADR-0320 and ADR-0329; their GO predicate,
  source, exact-value, transaction and admission contracts continue where
  compatible. Refines ADR-0238/0244 for tagged presets and dual associations.
- Related TSRs: none; this work originated in a product compatibility design.
