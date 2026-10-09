# ADR-0271: Shared Station Reputation

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Design

## Context

Login used the full numeric-SSID identity for reputation history and state,
while manual DX checked the baseline callsign it displays. Login also reset
active posting counts and rephased the window. A shared key alone would leave
reconnects replenishing the allowance and expose shared history reads/writes.

## Decision

Normalize RecordLogin and Check to the existing baseline own-call identity.
Keep full-SSID configuration/session ownership and baseline manual spot display.
Reuse an existing baseline history, or ordinary new-station probation when it
is absent. Preserve old SSID history files without merging or migrating them.

Advance existing elapsed windows before a login transition. A benign reconnect
preserves the current window, counts, limits and applicable deadlines after
that normal progression. Risk triggers retain their existing roles: new-station
restrictions may apply initial wait and starting caps; extra delay affects ramp.
Tighten caps and take later deadlines without resetting spent counts/window.
Keep lastSeen nondecreasing even if a delayed operation has an older timestamp.

A penalty can demote per-band maturity while a total-cap upgrade is scheduled.
Require maturity before applying that upgrade, retaining a later old deadline
as a floor. On reaching maturity, take the later floor or the configured delay.
Do not let an old pending upgrade immediately undo newly tightened restrictions.

Each existing call shard owns a fixed login-only mutex, separate from its
in-memory state mutex. Enrichment occurs before both locks. The login mutex
serializes baseline history load/evaluation/touch and state publication. Hold
the state mutex only for bounded in-memory work; checks and sweeping need no
login mutex. Re-fetch current ownership after history work so eviction cannot
cause a retired pointer to be resurrected. Store is Gate-owned in current
production callers; its public API and existing failure behavior remain intact.

Retain existing TTL/sweep eviction, bounded history and prefix/cache ownership.
The state sweep threshold is per shard and is not an insertion-time hard cap.
Counters remain runtime-only; eviction or restart can discard them. This
decision does not add counter persistence, migration or a new callsign registry.

## Alternatives considered

1. Share only the key. Rejected because login would still replenish counts.
2. Serialize history under the state mutex. Rejected because posting checks
   would wait for disk I/O.
3. Move history and publication into a Store callback transaction. Viable,
   but requires extra Store plumbing and callback/lock-order contracts. Fixed
   shard login ownership is sufficient with current Gate-owned Store access.
4. Return before/after history and publish after releasing its serialization.
   Rejected because a newer history update could publish before an older one.
5. Persist counters or merge old SSID histories. Outside the selected policy.

## Consequences

Numeric SSIDs share one active station allowance and history while their
preferences stay separate. Login I/O may serialize other logins in its fixed
shard; it does not acquire the posting state lock until publication. This is
an ownership guarantee, not a production latency or concurrent-capacity claim.
Support should explain active runtime continuity and the restart/eviction limit.

## Links

- Refines baseline identity: [ADR-0115](ADR-0115-dx-numeric-ssid-canonicalization.md)
- Authority: [Scope Ledger v3](../command-fix-scope-ledger-v3.md), checkpoint 05a
- Evidence: [execution record](../command-fix-execution.md#checkpoint-05a-shared-station-reputation-without-reconnect-replenishment)
- Implementation: `reputation/gate.go`, `reputation/login_restrictions.go`
- Validation: `reputation/login_restrictions_test.go`, actual checkout runner
- Supersedes / superseded by: none; configuration ownership remains unchanged.
