# ADR-0136: Custom SCP Test Time Fixtures

- Status: Accepted
- Date: 2026-05-23
- Decision Origin: Troubleshooting chat

## Context
Custom SCP retained-state tests used fixed April 2026 observation timestamps for
fixtures intended to be fresh under `HorizonDays`. By May 23, 2026 those
fixtures had aged out, so production code correctly rejected them before
retaining entries, spotters, expiry items, or interner refs.

## Decision
No durable production decision change. Custom SCP tests that exercise fresh
runtime observations should use current-relative UTC fixtures safely inside the
active observation horizon. Production `recordObservation` horizon enforcement
continues to use the process clock and remains unchanged.

## Alternatives considered
1. Inject a test clock into `CustomSCPStore`: rejected as unnecessary production
   surface for a test fixture bug.
2. Increase test `HorizonDays`: rejected because it weakens what the tests prove
   and can fail again later.
3. Use current-relative test timestamps: chosen because it preserves the runtime
   contract while keeping stale/fresh relationships explicit.

## Consequences
### Benefits
- Custom SCP tests remain deterministic across calendar time.
- Existing retained-state bounds and stale-observation behavior are preserved.

### Risks
- Tests still depend on wall-clock execution, but using a one-hour-old UTC base
  leaves a large margin inside 30-day and 60-day horizons.

### Operational impact
- None. Test-only change; no runtime, config, protocol, or operator-visible
  behavior changed.

## Links
- Related issues/PRs/commits: -
- Related tests: `spot/custom_scp_store_test.go`
- Related docs: `docs/troubleshooting/TSR-0024-custom-scp-test-time-horizon.md`
- Related TSRs: TSR-0024
- Supersedes / superseded by: -
