# Phase 2 Architecture Note - Signal Resolver (Shadow Mode)

Status: Implemented (shadow mode, no cutover)
Date: 2026-02-23
Scope Ledger: v4 (approved)
Decision refs (target): TSR-0003, ADR-0022
Runbook: `docs/call-correction-phase2-shadow-runbook.md`

## 1) Intent

Phase 2 introduces a bounded signal-level resolver in **shadow mode** to evaluate a single canonical state per `(band, mode, frequency-key)` without changing user-visible correction behavior yet.

This phase is deliberately observational: existing correction/stabilizer/glyph behavior remains authoritative.

## 2) Non-goals (Phase 2)

- No correction cutover to resolver output.
- No stabilizer cutover to resolver output.
- No confidence glyph semantic changes.
- No new YAML knobs unless explicitly approved later.

## 3) Requirements and Edge Cases

### Functional

- Ingest pre-correction evidence snapshots from output pipeline paths.
- Maintain per-signal resolver state with bounded memory:
  - `winner`, `runner_up`, `margin`, `state`.
  - `state in {confident, probable, uncertain, split}`.
- Preserve distance parity with current correction path:
  - CW uses Morse-aware distance when configured.
  - RTTY uses Baudot-aware distance when configured.
  - Other supported modes use plain distance.
- Emit disagreement metrics against current correction outcomes.

### Non-functional

- Ingest hot path must stay non-blocking and fail-open under resolver backpressure.
- Resolver CPU work must be bounded under burst.
- Resolver state and queues must be bounded.
- Deterministic evaluation and tie-break behavior.

### Operational edge cases

- Burst traffic and reconnect churn.
- Queue saturation in shadow ingest.
- Key churn across wide frequency ranges.
- Sparse evidence and fast winner churn.
- Shutdown while queue has pending snapshots.

## 4) User Impact and Determinism

- **Expected user-visible behavior change**: none in Phase 2.
- Resolver output is shadow-only and observational.
- Determinism is preserved by:
  - single-owner resolver goroutine,
  - deterministic sort/tie-break rules,
  - fixed cadence policy per key.

## 5) Architecture

### 5.1 Components

1. `SignalResolver` (new, single-owner state machine)
2. `ResolverEvidence` snapshot type (immutable, pre-correction call identity)
3. non-blocking ingest adapter from pipeline to resolver input queue
4. disagreement observer that compares shadow resolver state with current correction decisions

### 5.2 Concurrency model

- One resolver goroutine owns all mutable resolver state.
- Producers only enqueue snapshots through a bounded channel.
- No lock-sharing on resolver internal maps from pipeline goroutines.

### 5.3 Evaluation cadence

- Event-driven with per-key rate limiting:
  - mark key dirty on each accepted snapshot,
  - evaluate immediately but not more than once per key every `500ms`.
- Periodic sweep ticker (for stale cleanup and pending hysteresis transitions):
  - default `1s` sweep in shadow mode.

Rationale:
- keeps low latency for fresh evidence,
- bounds repeated evaluation under high-rate bursts.

### 5.4 Key granularity

Phase 2 draft choice: derive effective frequency-key from the same per-spot effective tolerance logic used by current correction path (band/state/mode aware), then normalize to stable key buckets.

Rationale:
- better apples-to-apples disagreement data in shadow mode than fixed global bins.

### 5.5 Evidence and scoring inputs

- Candidate support by unique spotters (bounded per candidate).
- Spotter overlap metrics between top candidates.
- Mode-aware string distance parity with current path.
- Existing validity signals used for comparison/labeling only in shadow path.

Phase 2 keeps thresholds internal (constants), not YAML, until disagreement data proves operator tuning need.

### 5.6 Split detection

- Extract shared split/overlap helper so both:
  - current Phase 1 ambiguity guard, and
  - resolver split state detection
  rely on one implementation.

### 5.7 Hysteresis

- New key with no prior winner: first winner can be set immediately.
- Winner transitions require sustained margin across `N=2` consecutive evaluation windows.
- Prevents flap without delaying initial convergence.

## 6) Resource bounds

Initial Phase 2 defaults (internal constants):

- input queue size: `8192`
- max active keys: `6000`
- max candidates per key: `8`
- max reporters per candidate: `32`
- key TTL (inactive eviction): `10m`

If caps are hit:
- drop new shadow evidence for affected path,
- increment explicit drop counters,
- keep current correction path unaffected.

## 7) Integration sequencing (critical)

### Primary output path

- Capture resolver evidence snapshot in `processOutputSpots` **before** calling `maybeApplyCallCorrectionWithLogger`.
- Do not feed resolver from mutated/corrected call fields.

### Delayed stabilizer release path

- Also capture pre-correction snapshot before delayed re-check correction call.
- Use same non-blocking enqueue semantics as primary path.

### Explicit non-blocking contract

- Queue full => drop shadow snapshot and increment counter.
- Never block ingest/output goroutines.

## 8) Failure modes and shutdown

- Resolver panic isolation: recover and log with bounded restart policy (if used).
- Shutdown order:
  1. stop new enqueue (context cancellation),
  2. drain resolver queue within bounded deadline,
  3. stop resolver goroutine and publish final counters.

## 9) Observability plan

Shadow metrics/counters:

- resolver state counts: confident/probable/uncertain/split
- shadow ingest drops (queue full)
- disagreement classes:
  - resolver `split` while current path corrected,
  - resolver confident with different winner than current correction,
  - resolver uncertain while current path corrected with high confidence,
  - overall agreement rate

## 10) Test plan (Phase 2)

- Unit tests for resolver:
  - deterministic ranking/tie-break,
  - split detection,
  - hysteresis transition behavior,
  - bounds and TTL eviction.
- Integration tests for feed sequencing:
  - snapshot pre-mutation correctness,
  - non-blocking/fail-open enqueue under saturation.
- Race tests for resolver and pipeline integration.
- Benchmarks:
  - resolver eval micro-bench,
  - hot-path benchmark guardrail before/after shadow integration.

## 11) Alternatives considered

1. timer-only evaluation
- simpler CPU envelope
- higher decision lag, less responsive

2. fixed global frequency bins
- simpler
- worse disagreement fidelity against current per-band/state tolerance behavior

3. immediate Phase 3 cutover
- faster architecture migration
- too risky without shadow disagreement data

## 12) Resolution Notes

- Hysteresis window count confirmed: `N=2`.
- Resolver constants remain internal (no new YAML knobs in Phase 2).
- Core resolver is implemented in `spot/signal_resolver.go`.
- Shadow ingest/observer adapters are implemented in `main.go`.
