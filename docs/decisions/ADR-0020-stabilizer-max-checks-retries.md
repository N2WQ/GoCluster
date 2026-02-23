# ADR-0020 - Multi-Check Telnet Stabilizer Retries With Bounded Re-Delay

Status: Accepted
Date: 2026-02-23
Decision Origin: Design

## Context
- ADR-0013 introduced a telnet-only stabilizer with one delayed check before timeout action.
- Operators requested faster early release when correction converges quickly, while still holding unresolved risky spots longer.
- The implementation must keep bounded resources and preserve fail-open behavior under queue pressure.
- Archive/peer output contracts must remain unchanged.

## Decision
- Add `call_correction.stabilizer_max_checks` to configure how many delayed-check cycles run before timeout action.
- `stabilizer_max_checks` includes the first delayed check; `1` preserves legacy behavior.
- Keep one bounded scheduler goroutine and bounded pending queue (`stabilizer_max_pending`); re-delays reuse the same bounded queue.
- Retry only when the spot is still risky and confidence is not `P`, `V`, or `C`.
- If a retry cannot be enqueued because the stabilizer queue is full, fail open and release immediately (do not suppress).
- Keep timeout behavior unchanged after checks are exhausted: apply `stabilizer_timeout_action` (`release` or `suppress`).
- Keep stabilizer telnet-only. Archive/peer behavior remains unchanged.

## Alternatives considered
1. Keep single delayed check only
   - Pros: simplest behavior.
   - Cons: cannot combine short delay with longer hold for unresolved spots.
2. Increase one-shot `stabilizer_delay_seconds`
   - Pros: no new knob.
   - Cons: penalizes quickly corrected spots with unnecessary latency.
3. Retry all risky non-`P` spots (including `V`/`C`)
   - Pros: more aggressive cleanup.
   - Cons: holds already-validated/confident outcomes longer without proportional benefit.

## Consequences
- Benefits:
  - More tunable latency/cleanliness tradeoff for telnet output.
  - Backward compatibility when `stabilizer_max_checks=1`.
  - Bounded-state model is preserved.
- Risks:
  - Higher queue occupancy when `stabilizer_max_checks>1`.
  - More delayed processing cycles per unresolved risky spot.
- Operational impact:
  - New YAML knob in `data/config/pipeline.yaml`.
  - Existing stabilizer counters remain valid; `held` may increase when re-delays occur.
  - No wire-format/protocol changes.

## Links
- Supersedes behavior from:
  - `docs/decisions/ADR-0013-call-correction-telnet-stabilizer.md`
- Code:
  - `config/config.go`
  - `main.go`
  - `stabilizer.go`
- Tests:
  - `config/call_correction_stabilizer_test.go`
  - `main_test.go`
  - `stabilizer_test.go`
- Docs:
  - `README.md`
  - `data/config/pipeline.yaml`
  - `docs/decision-log.md`
