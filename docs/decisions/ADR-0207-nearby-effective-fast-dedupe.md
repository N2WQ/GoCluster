# ADR-0207: NEARBY Effective Fast Dedupe

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context
`SET DEDUPE` is a saved per-callsign telnet preference. The shipped default is
`SLOW`, which is useful for broad feeds because it suppresses repeated spots
from an entire DE CQ zone.

`PASS NEARBY ON` changes the user's intent. While the nearby filter has usable
grid-backed H3 cells, the user is asking for a narrow local-area feed. Keeping
the saved `SLOW` secondary dedupe lane during that filter can hide useful local
repeats before the user sees them.

Existing decisions constrain the fix:

- ADR-0138 keeps saved per-callsign dedupe policies authoritative.
- ADR-0148 keeps archive, peer, ring-buffer, and telnet on the final stabilized
  fanout boundary while preserving telnet `FAST`/`MED`/`SLOW` controls.
- ADR-0186 treats secondary dedupe windows as operator-owned config values.

## Decision
When a telnet client's `NEARBY` filter is enabled and has valid cached fine and
coarse user H3 cells, telnet broadcast admission uses the least-suppressive
available secondary dedupe policy by resolving `FAST` through the existing
enabled-policy fallback path.

This is an effective runtime policy only:

- The saved `SET DEDUPE` value is not changed by `PASS NEARBY ON` or
  `PASS NEARBY OFF`.
- Stored `NEARBY` state without usable grid-backed cells remains inactive and
  does not change the effective dedupe policy.
- `SHOW DEDUPE` reports the saved policy and the temporary `NEARBY` effective
  policy when they differ.
- `SET DIAG DEDUPE` uses the same effective policy for its compact key and
  policy token so troubleshooting matches broadcast admission.
- Primary dedupe, secondary key shapes, secondary windows, archive/peer fanout,
  ring-buffer history, H3 matching, self-spot bypass, queues, and command syntax
  are unchanged.

## Alternatives considered
1. Require users to run `SET DEDUPE FAST` manually before `PASS NEARBY ON`.
   Rejected because it makes the local-area filter easy to misconfigure and
   leaves the user responsible for restoring their broader-feed policy later.
2. Persist `FAST` when `PASS NEARBY ON` is accepted.
   Rejected because `PASS NEARBY` should not rewrite the saved dedupe
   preference, and persisted mutation would surprise users after `NEARBY` is
   turned off or becomes inactive.
3. Bypass secondary dedupe entirely while `NEARBY` is active.
   Rejected because it would remove bounded repeat suppression and increase
   per-client traffic more than the least-suppressive configured lane requires.

## Consequences
### Benefits
- `PASS NEARBY ON` shows more relevant local spots without requiring a separate
  `SET DEDUPE FAST` command.
- Saved dedupe preferences remain deterministic and reversible.
- Diagnostics and `SHOW DEDUPE` expose the effective policy used for telnet
  broadcast admission.
- Operators can still disable `FAST`; the existing fallback then selects the
  next available lane.

### Risks
- NEARBY users can receive more repeat spot lines than they did under saved
  `MED` or `SLOW`.
- Broadcast workers may perform filter checks for more candidate repeats for
  NEARBY clients because `FAST` admits more spots before per-client filters run.

### Operational impact
- User-visible telnet behavior changes only while usable `NEARBY` is in effect.
- No YAML, schema, config default, archive, peer, history, primary dedupe,
  secondary key, or secondary window change.
- If `FAST` is disabled, the effective NEARBY lane falls back through the
  existing enabled-policy order.

## Links
- Related issues/PRs/commits: current working tree
- Related tests: `telnet/dedupe_policy_test.go`,
  `telnet/diag_command_test.go`, `telnet/server_filter_test.go`,
  `commands/processor_test.go`
- Related docs: `README.md`, `telnet/README.md`,
  `customgpt/troubleshooting-index.md`, `commands/processor.go`
- Related TSRs: none
- Supersedes / superseded by: none
