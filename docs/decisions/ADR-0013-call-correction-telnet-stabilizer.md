# ADR-0013 - Telnet Stabilizer for Risky Call-Correction Output

Status: Accepted
Date: 2026-02-17
Decision Makers: Cluster maintainers
Technical Area: main/telnet fan-out, spot/correction
Tags: call-correction, telnet, dedupe, bounded-state

## Context
- Call-correction can emit an initial call that is later corrected again as more corroboration arrives.
- Telnet clients and downstream logger bandmaps can retain the first/busted call for a long dwell time, even after correction converges.
- Operators requested a deterministic way to reduce busted calls seen by humans without blocking ingest and without changing archive/peer contracts by default.
- Existing secondary dedupe is shared across telnet/archive/peer in MED policy, so introducing telnet-only delay risks changing archive/peer dedupe timing if state is shared.

## Decision
- Add a telnet-only stabilizer gate controlled by `call_correction.stabilizer_*`:
  - `stabilizer_enabled`
  - `stabilizer_delay_seconds`
  - `stabilizer_timeout_action` (`release` or `suppress`)
  - `stabilizer_max_pending`
- Risk definition for stabilizer delay:
  - correction-eligible mode, non-beacon,
  - not recently heard on same `(call, band, mode)` using recent-on-band store admission,
  - and not `P` confidence (P passes immediately).
- Hold risky telnet spots for configured delay, then re-evaluate:
  - re-run call correction once,
  - apply known-call floor and license gate,
  - if still risky: follow timeout action (`release` or `suppress`).
- Keep scheduler resources bounded:
  - single scheduler goroutine with heap/timer,
  - bounded pending queue (`stabilizer_max_pending`),
  - overflow policy is fail-open immediate release.
- Preserve archive/peer behavior by splitting archive/peer MED secondary dedupe state from telnet MED dedupe when stabilizer is enabled.
- Add stabilizer observability counters:
  - held, released immediate, released delayed, suppressed timeout, overflow release.

## Alternatives Considered
1. Fixed global delay for all telnet spots
   - Pros:
     - Simple to reason about.
   - Cons:
     - Unnecessary latency for high-confidence/non-risky spots.
2. Require two independent corroborators before any telnet output
   - Pros:
     - Strong busted-call suppression.
   - Cons:
     - Can suppress useful sparse-band activity and increase false negatives.
3. Suppress risky spots only (no delayed release option)
   - Pros:
     - Maximum cleanliness for visible bandmaps.
   - Cons:
     - Potentially hides valid weak/sparse activity; less operator flexibility.

## Consequences
- Positive outcomes:
  - Fewer busted calls visible to telnet clients and logger bandmaps.
  - Deterministic operator control (`release` vs `suppress`) for timeout behavior.
  - Archive/peer dedupe timing remains stable under telnet-only delay.
- Negative outcomes / risks:
  - Added memory/CPU overhead for delayed queue management.
  - Added telnet latency for risky spots.
  - In `suppress` mode, some legitimate low-corroboration calls may be dropped.
- Operational impact:
  - New YAML knobs in `data/config/pipeline.yaml`.
  - Stats/dashboard include stabilizer counters.
  - No protocol wire-format changes.

## Validation
- Unit tests for:
  - stabilizer defaults and timeout-action validation (`config`).
  - bounded stabilizer queue and delayed release ordering (`main` stabilizer tests).
  - stabilizer counters (`stats`).
  - risky-gate behavior including `P` pass-through (`main_test`).
- Required checker suite:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`

## Rollout and Reversal
- Rollout plan:
  - Start with `stabilizer_enabled: true`, `stabilizer_delay_seconds: 5`, `stabilizer_timeout_action: release`.
  - Observe stabilizer counters and telnet-visible busted-call rate.
- Backward compatibility impact:
  - Telnet timing/visibility changes for risky spots only.
  - Archive/peer output contracts preserved.
- Reversal plan:
  - Set `stabilizer_enabled: false` to disable stabilizer without code rollback.

## Links
- Code:
  - `main.go`
  - `stabilizer.go`
  - `config/config.go`
  - `stats/tracker.go`
- Tests:
  - `stabilizer_test.go`
  - `main_test.go`
  - `config/call_correction_stabilizer_test.go`
  - `stats/tracker_stabilizer_test.go`
- Docs/config:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/OPERATOR_GUIDE.md`
  - `docs/decision-log.md`
