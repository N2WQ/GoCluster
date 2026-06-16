# ADR-0186: Prod Secondary Dedupe Window Alignment

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Design

## Context

Production has been running secondary telnet dedupe windows one second shorter
than the checked-in example config:

- FAST: 179 seconds
- MED: 359 seconds
- SLOW: 479 seconds

The current production config update keeps those values rather than moving prod
to the checked-in 180/360/480 values. Leaving the repo examples at a different
policy would make future config syncs show intentional drift every time and
would make the shipped policy less representative of the active production
node.

## Decision

Set the checked-in `data/config/dedupe.yaml` secondary windows to match
production:

- `dedup.secondary_fast_window_seconds: 179`
- `dedup.secondary_med_window_seconds: 359`
- `dedup.secondary_slow_window_seconds: 479`

This changes only operator-owned policy values. It does not change primary
dedupe, secondary dedupe key semantics, per-user `SET DEDUPE` choices, archive
or peer fanout ownership, queue behavior, or fallback behavior when a selected
policy is disabled.

## Alternatives considered

1. Move prod to 180/360/480.
   - Rejected by operator choice; production keeps the existing one-second
     shorter windows.
2. Keep repo at 180/360/480 and document prod as local drift.
   - Rejected because the drift is intentional and small enough that keeping it
     split would add recurring config-review noise without improving safety.
3. Add separate production overlay documentation only.
   - Rejected because the checked-in example is already treated as the source
     for current policy sync decisions.

## Consequences

### Benefits

- Checked-in example policy now matches the production node for secondary
  dedupe windows.
- Future prod-vs-repo config reviews no longer report these three values as
  drift.

### Risks

- New deployments using the checked-in config will use one-second shorter
  secondary dedupe windows than before.
- Any docs or tests that quote exact shipped secondary windows must stay aligned
  with the YAML values.

### Operational impact

The effective secondary dedupe windows for FAST, MED, and SLOW are 179, 359,
and 479 seconds. Operators who want round-number windows can still set their
private config back to 180, 360, and 480.

## Links

- Related config: `data/config/dedupe.yaml`
- Related tests: `config/dedup_config_test.go`
- Related docs: `data/config/README.md`
- Related ADRs: ADR-0138, ADR-0148
- Related TSRs: none
