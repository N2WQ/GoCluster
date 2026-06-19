# TSR-0024: Custom SCP Test Time Horizon

- Status: Resolved
- Date opened: 2026-05-23
- Status date: 2026-05-23

## RCA Summary

- What happened: Custom SCP retained-state tests failed after fixed April 2026
  observations aged beyond the configured 30-day horizon.
- Why: The tests used absolute timestamps for data that was supposed to be
  fresh relative to `time.Now().UTC()`, while production correctly drops stale
  observations before mutating retained state.
- What fixed it: ADR-0136 changed tests to use current-relative UTC fixtures
  that preserve intended fresh/stale ordering inside the active horizon.
- How we know: Targeted `go test ./spot` reproduced empty retained-state
  failures, and source inspection showed `recordObservation` returning early
  for stale `seenAt` values.
- Operator/support answer: This was validation-only fixture drift, not a
  production retention regression; production horizon behavior stayed correct.

## Trigger
`go test ./spot` failed in Custom SCP retained-state tests after fixed April
2026 observation fixtures aged past their configured 30-day horizon.

## Symptoms and impact
The failing tests expected retained Custom SCP entries, spotters, interner refs,
and expiry-index cardinality, but `recordObservation` retained no state. This
blocked full repository validation while production horizon behavior remained
correct.

## Hypotheses tested
1. Pebble persistence or WAL replay failure.
2. Spotter slice, interner, or max-key eviction regression.
3. Test fixture timestamps aged beyond `HorizonDays`.

## Evidence
- Targeted `go test ./spot -run 'TestCustomSCP(...)' -count=1 -v` reproduced
  four empty-state failures.
- `spot/custom_scp_store.go` computes the observation cutoff from
  `time.Now().UTC()` and returns before mutation when `seenAt` is stale.
- The failing tests used April 11-12, 2026 observations with 30-day horizons;
  on 2026-05-23 those observations are older than the active horizon.
- 2026-06-19 recurrence: `go test ./internal/cluster -run
  TestFormatRecentSupportByBandLinesIncludesCustomSCPStaticCalls -count=1 -v`
  reproduced the same fixture-drift class in the dashboard support-summary
  test. The fixture used fixed April 2026 observations with a 60-day horizon;
  those observations aged out before Custom SCP retained them.

## Root cause or best current explanation
The tests used absolute dates for data that was meant to be fresh relative to
the process clock. Once the calendar advanced past the configured horizon, the
production stale-observation guard correctly dropped the fixtures.

## Fix or mitigation
Use current-relative UTC test fixtures inside the active horizon while
preserving each test's intended ordering and stale/fresh relationships.

## Why an ADR was or was not required
- No durable production decision changed.
- A lightweight ADR stub records that the fix is test-only and preserves the
  existing Custom SCP horizon contract.

## Links
- Related ADRs: ADR-0136
- Related issues/PRs/commits: -
- Related tests: `spot/custom_scp_store_test.go`
- Related docs: `docs/decision-memory.md`
