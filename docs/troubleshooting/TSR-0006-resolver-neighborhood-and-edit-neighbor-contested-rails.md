# TSR-0006 - Resolver Neighborhood Competition and Contested Edit-Neighbor Rails

Status: Resolved
Date Opened: 2026-02-26
Date Resolved: 2026-02-26
Owner: Cluster maintainers
Technical Area: main output pipeline, internal/correctionflow, telnet suppression, cmd/rbn_replay, config
Trigger Source: Chat request
Led To ADR(s): ADR-0033
Tags: resolver-primary, neighborhood, stabilizer, suppression, replay-parity

## Triggering Request

- Request date: 2026-02-26
- Request summary: implement P2 from approved resolver-only ledger (A2, A3, A5, A6, A8, A10) to reduce similar-call leakage and keep replay/main behavior aligned.
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact

- What failed or looked wrong?
  - Similar calls differing by one character still leaked through under timing and bucket-boundary conditions.
  - Resolver-primary decisions and replay analytics risked drift without shared neighborhood selection logic.
- User/operator impact:
  - Duplicate/conflicting variants in telnet output reduced trust in corrected call stream.
  - Replay evidence was less actionable if it did not classify decisions with the same policy path as runtime.
- Scope and affected components:
  - resolver-primary apply path (`main.go`),
  - shared correctionflow policy helpers,
  - stabilizer delay policy,
  - telnet family suppressor,
  - replay AB metrics and runbook output,
  - config schema and defaults/sanitization.

## Timeline

1. 2026-02-26 - P2 implementation approved in scope ledger v5.
2. 2026-02-26 - Added neighborhood snapshot selection + adaptive min-reports parity in resolver-primary path and replay.
3. 2026-02-26 - Added contested edit-neighbor delay/suppression rails, config gating, replay/runtime counters, and deterministic tests.

## Hypotheses and Tests

1. Hypothesis A - Adjacent resolver buckets can produce winner forks that require neighborhood competition.
   - Evidence/commands: implemented `SelectResolverPrimarySnapshot(...)` in `internal/correctionflow/shared.go`; validated with neighborhood winner/split tests in `internal/correctionflow/shared_test.go` and `main_test.go`.
   - Outcome: Supported.
2. Hypothesis B - Family-only stabilizer/suppressor rails miss substitution-class errors without resolver-contested edit-neighbor policy.
   - Evidence/commands: added `edit_neighbor_contested` stabilizer reason and resolver-contested edit-neighbor suppression; validated via `internal/correctionflow/stabilizer_test.go`, `main_test.go`, and `telnet_family_suppressor_test.go`.
   - Outcome: Supported.
3. Hypothesis C - Safe rollout requires explicit feature gates and replay/runtime observability for neighborhood/edit-neighbor behavior.
   - Evidence/commands: added config knobs and sanitization in `config/config.go`; replay counters and compare output updates in `cmd/rbn_replay/*`; full checker suite pass.
   - Outcome: Supported.

## Findings

- Root cause (or best current explanation):
  - Resolver-primary lacked explicit adjacent-bucket competition and contested edit-neighbor safety rails, allowing one-character variants to escape in boundary/timing windows.
- Contributing factors:
  - Shared policy parity between runtime and replay was incomplete for neighborhood selection and contested edit-neighbor reasons.
- Why this did or did not require a durable decision:
  - Required a durable decision because it changes shared resolver policy behavior, config contracts, and observability semantics across runtime and replay.

## Decision Linkage

- ADR created/updated:
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
- Decision delta summary:
  - Added resolver neighborhood winner/split selection as a shared helper for main and replay.
  - Added resolver-contested edit-neighbor stabilizer delay and telnet suppression rails.
  - Added rollout feature gates and replay/runtime telemetry for these decisions.
- Contract/behavior changes (or `No contract changes`):
  - Config contract changed (new `call_correction` knobs).
  - New/updated decision-reason and replay metric labels.
  - Runtime behavior changes only when new feature gates are enabled; adaptive min-reports parity in resolver-primary applies immediately.

## Verification and Monitoring

- Validation steps run:
  - `go test ./internal/correctionflow ./cmd/rbn_replay ./config -count=1`
  - `go test . -count=1`
  - `go test ./... -count=1`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./... -count=1`
- Signals to monitor (metrics/logs):
  - resolver decision reasons: `resolver_neighbor_conflict`, `resolver_applied_neighbor_override`,
  - stabilizer delay reasons including `edit_neighbor_contested`,
  - replay AB resolver counters: `neighborhood_used`, `neighborhood_winner_override`, `neighborhood_conflict_split`,
  - telnet suppression behavior for contested edit-neighbor pairs.
- Rollback triggers:
  - increased false suppressions/delays for valid calls, or resolver apply-rate regression after enabling neighborhood/edit-neighbor flags.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
  - `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md`
  - `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md`
- Related docs:
  - `docs/call-correction-resolver-scope-ledger-v5.md`
  - `docs/rbn_replay.md`
  - `docs/OPERATOR_GUIDE.md`
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
