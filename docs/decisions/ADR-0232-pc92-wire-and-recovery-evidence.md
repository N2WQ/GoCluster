# ADR-0232: PC92 Raw Identity and Continuous Recovery Evidence

- Status: Accepted
- Date: 2026-10-01
- Decision Origin: Troubleshooting chat

## Context

Re-audit of 2c06079 falsified the equivalence between login normalization and raw
PC92 validation, generic metadata omission and K omission, and sampled headroom
and a continuous recovery interval. The owner approved v12 after resolving CTY
retention. Prior resource, policy and deadline selections remain controlling.
Acceptance of this decision does not assert implementation qualification.

## Decision

- Validate received raw identities before stable canonical mapping. Origin
  receives no padding/case/slash/SSID repair; entry call portions permit trailing
  ASCII spaces within the supported printable envelope. Keep local encoding and
  login normalization, literal authentication and original transit payload.
- Explicit K subject-node omitted/zero numeric values replace prior values
  with literal zero. Preparation, commitment, final charge and replacement peak
  use the same effective node metadata. Preserve other actions, relationship
  metadata, absent IP and old projection ownership; no SQL schema change.
- Retain one bounded failure episode per configured peer, with cause and
  generation. Mailbox failures require mailbox capacity; new authority requires
  all applicable resources; alternate ingress requires its missing observations.
  A refused wire is only a capacity witness and is never automatic replay.
- Track every mailbox charge-class transition and exact graph feasibility after
  whole transactions retire. Cache availability follows original admission age
  and strict 600-second expiry. True-to-true health preserves its interval;
  interruption resets it. Final clearing rechecks generation and mailbox
  interval under manager then queue locks. All gate reasons remain independent.
- Qualification records capacity facts and actual gate transitions, and rejects
  missing, late, stale, canceled or overflowed observations. One continuous
  healthy second precedes reopening; observation is due within the next second.
  A five-second recovery exchange allowance cannot replace this obligation.
- The owner retained the separately documented CTY refresh. Future qualification
  pins exact consumed source/reference/assets and reports old runs as historical.

## Alternatives considered

1. Normalize before raw validation or globally tighten login identity: conflates
   distinct boundaries and can either grant malformed authority or break locals.
2. Change all empty metadata merging: changes A/C/D and relationship behavior.
3. Tick-only headroom or unconditional reset on mutation: misses interruptions
   or indefinitely delays recovery during healthy unrelated traffic.
4. Additional graph certificates, reverse indexes or retained plans: outside
   v12. Exact checks must pass the early complete service-cost gate before long
   qualification; failure requiring such machinery needs revised scope.

## Consequences

### Benefits

Receiver-compatible identity and numeric metadata have distinguishing oracles.
Recovery timing reflects the resources that actually caused closure and cannot
pass through late sampling or a check that mutates its observed state.

### Risks

Exact graph checks across 64 blockers can be expensive. The approved fixed-state
addition is at most 32 KiB inside existing bounds, with one graph scratch owner.
These bounds and service cost require evidence. Enabled-SQLite, context backing,
retirement ownership and full workload acceptance remain separate open proofs.

The approved experiment subsequently **failed** its sustained necessary
condition: 63 blocked peers caused exact re-evaluation to fill the live input
mailbox and disconnect its source. The zero-blocker control passed. V12's stop
condition is active; this ADR preserves the approved decision and negative
evidence, and does not authorize certificates, retained plans or another
recovery architecture. A revised decision/scope must precede that work.

### Operational impact

Malformed wire aliases that were previously repaired now reject atomically.
K can visibly clear prior version/build to zero. Reconnect alone cannot fix a
capacity gate. Membership's one-second promise is queue admission, not a remote
processing acknowledgment. No local users, refresh policy or database driver
are changed by this decision.

## Links

- Related issues/PRs/commits: Approved v12 on `p92`, baseline 2c06079; no deployment.
- Related tests: v12 wire, K, recovery and qualification regression families.
- Related docs: [v12 ledger](../pc18-pc92-scope-ledger-v12.md),
  [validation](../pc92-v12-validation.md), [CTY provenance](../cty-refresh-2c06079.md).
- Related TSRs: [TSR-0035](../troubleshooting/TSR-0035-pc92-qualification-accounting.md).
- Supersedes / superseded by: refines ADR-0231's raw identity clause and
  ADR-0230/0231 admission recovery/evidence. Other clauses remain effective.

Admission-recovery clauses are superseded by [ADR-0233](ADR-0233-pc92-controlled-retries-and-peer-cap.md) under Approved v14. Other clauses and historical evidence remain effective.
