# ADR-0060 - Source-Aware FT Burst Clustering for Live Corroboration

Status: Accepted
Date: 2026-04-08
Decision Makers: Cluster maintainers
Technical Area: main output pipeline, FT confidence, stats, docs
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0017
Tags: ft8, ft4, ft2, confidence, burst-clustering, output-pipeline

## Context

- ADR-0057 introduced bounded FT corroboration in the main output pipeline with count-only `?`/`S`/`P`/`V` semantics and `V`-only `custom_scp` admission.
- ADR-0058 fixed PSKReporter FT frequency semantics by canonicalizing operational frequency to dial values while preserving observed RF separately.
- ADR-0059 then replaced the original short arrival hold with slot-aware grouping, using source time where available and arrival-time fallback for RBN FT8/FT4.
- Live validation after rebuilding and restarting the runtime still showed no practical FT `P`/`V` output under the slot-aware model:
  - an 8 second `localhost:8300` probe produced `?=387`, `S=124`, `P=0`, `V=0`
  - large exact `DX + mode + canonical dial` FT groups were present but still fragmented, including:
    - `JA4LXY|FT8|21074.00` with 26 spotters over 14.6 seconds
    - `4X1NX|FT8|21074.00` with 23 spotters over 7.6 seconds
    - `SV1JMC|FT8|18100.00` with 18 spotters over 13.6 seconds
- A replay-style simulation over a captured live telnet sample showed that bounded arrival-burst clustering would materially improve practical promotion quality on the same traffic, yielding non-zero `P` and `V` output without changing frequency semantics.

## Decision

Accepted refinement (2026-10-11): [ADR-0283](ADR-0283-ft-admission-expiry.md)
enforces the same exclusive expiry at admission and drain. Arrivals at or after
the earlier quiet/hard deadline start a new burst; all due groups retire before
admission, without changing timing values, thresholds or pending limits.

- Keep the bounded main-loop FT corroboration architecture from ADR-0057 and the canonical dial-frequency model from ADR-0058.
- Supersede ADR-0059's slot-identity grouping with source-agnostic active burst clustering:
  - key FT corroboration by normalized DX call, exact FT mode, and canonical dial frequency
  - extend one active burst while corroborators continue arriving within the mode-specific quiet gap
  - release the burst at `min(last_seen + quiet_gap, first_seen + hard_cap)`
- Enforce the architectural boundary from ADR-0057 in the live pipeline: FT modes must bypass resolver/temporal placeholder confidence assignment so the FT corroboration stage always sees a blank confidence field on entry.
- Use mode-specific defaults tuned to observed live dispersion:
  - FT8: `quiet_gap = 6s`, `hard_cap = 12s`
  - FT4: `quiet_gap = 5s`, `hard_cap = 10s`
  - FT2: `quiet_gap = 3s`, `hard_cap = 6s`
- Allow cross-source corroboration between PSKReporter and RBN-digital when the burst key matches and the reporter calls are distinct.
- Preserve existing FT glyph meanings and floors:
  - `?` = one unique reporter without static/recent promotion
  - `S` = one unique reporter with static/recent promotion
  - `P` = exactly two unique reporters in the same burst
  - `V` = three or more unique reporters in the same burst
- Preserve fail-open bounds for pending FT state, but record new FT burst observability in tracker/dashboard output:
  - active burst count
  - released burst count
  - overflow releases
  - average released burst span by FT mode
- Do not add new operator config knobs in this revision; timing remains code-bound until live evidence justifies a stable config surface.

## Alternatives Considered

1. Keep slot-aware grouping and continue tuning slot math.
   - Rejected because live evidence showed the source-time anchor itself was not reliable enough for practical corroboration.
2. Use one larger fixed per-mode hold without burst extension.
   - Rejected because it would still release based on first arrival only and would merge or split bursts less predictably than quiet-gap extension.
3. Return to the original short arrival-based hold.
   - Rejected because it had already failed live validation after frequency semantics were fixed.

## Consequences

### Benefits
- Live FT `P`/`V` formation now follows observed arrival dispersion instead of inferred slot identity.
- PSKReporter and RBN-digital can corroborate each other using the same canonical dial-frequency key.
- The new FT burst stats make post-deploy tuning observable instead of guesswork.

### Risks
- First FT spots in a burst can now wait as long as the mode hard cap before fan-out.
- Consecutive transmissions from the same DX on the same dial can merge if the quiet-gap or hard-cap defaults are too loose.
- Pending FT state remains in memory longer, so fail-open bounds remain mandatory.

### Operational impact
- Slow clients remain unaffected because the burst hold still happens before archive/telnet/peer fan-out.
- Overload still degrades by reducing promotion quality rather than blocking the single-owner output pipeline.
- The dashboard/overview now exposes FT burst counts and average burst spans for operator tuning.

## Validation

- Added/updated tests:
  - `ft_confidence_runtime_test.go`
  - `stats/tracker_ft_test.go`
- Live validation:
  - after rebuild/restart, a 20 second `localhost:8300` probe produced non-zero FT corroboration output with `?=46`, `S=261`, `P=188`, `V=1414`
- Commands:
  - `go test . ./stats -run "TestOutputPipelineFT|TestFTConfidenceController|TestBuildFTConfidence|TestTrackerFTBurstCounters"`
  - `go test . -run ^$ -bench "Benchmark(OutputPipelineFTHoldAndRelease|FTConfidenceControllerObserveAndDrain)$" -benchmem`
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`

## Rollout and Reversal

- Rollout plan:
  - Deploy normally, then validate against live `localhost:8300` output for non-zero FT `P`/`V` counts and sane burst stats.
- Backward compatibility impact:
  - FT `P`/`V` now mean corroboration within one bounded arrival burst rather than one inferred transmission slot.
  - The overview adds FT burst observability lines.
- Reversal plan:
  - Revert to ADR-0059 slot-aware timing or ADR-0057's original short-hold model if burst merging proves operationally worse than sparse promotion.

## References

- Related ADR(s):
  - `docs/decisions/ADR-0057-ft-confidence-corroboration.md`
  - `docs/decisions/ADR-0058-pskreporter-ft-frequency-canonicalization.md`
  - `docs/decisions/ADR-0059-slot-aware-ft-confidence-timing.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0017-ft-confidence-slot-anchor-fragmentation.md`
- Docs:
  - `README.md`
