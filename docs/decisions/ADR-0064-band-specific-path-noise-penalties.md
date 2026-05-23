# ADR-0064: Band-Specific Path Noise Penalties

- Status: Superseded
- Date: 2026-04-20
- Decision Origin: Design

## Context
Path reliability previously applied one scalar `SET NOISE` penalty per user
noise class across every band. That was too coarse for HF operation: low-band
local noise can dominate receive margin, while 10m and 6m usually have much
lower absolute external noise and are more often limited by receiver/system
noise or local interference events.

The scalar YAML key also made it impossible to tune receive-side penalties by
band without changing code.

## Decision
Path reliability uses a required `noise_offsets_by_band` table keyed by noise
class and band. The table is a P.372-17-informed operational receive-penalty
model, not a literal absolute radio-noise table.

The shipped table anchors larger penalties on 160m/80m and tapers them toward
10m/6m. `SET NOISE` continues to persist only the user's class; glyph rendering
and PATH filtering resolve the numeric penalty from `class + band` at prediction
time.

The old scalar `noise_offsets` YAML key is rejected with a clear configuration
error. Persisted user records do not need migration because they store only
`noise_class`.

## Alternatives considered
1. Keep scalar per-class penalties. Rejected because it keeps low-band and
   high-band receive penalties coupled.
2. Use strict P.372 class deltas versus quiet rural on every band. Rejected
   because it leaves high-band penalties counterintuitively large for this
   empirical spot-derived glyph model.
3. Add per-spotter or per-user calibrated noise/station models. Deferred
   because spotter noise and station capability are not available in the live
   feed.

## Consequences
### Benefits
- Low-band glyphs can be more conservative for noisy users without over-penalizing 10m/6m.
- The config file can be tuned per band without code changes.
- PATH filters and displayed glyphs use the same receive-side penalty resolver.
- Propagation reports serialize the same band-specific model context used at runtime.

### Risks
- Existing scalar `noise_offsets` configs must be replaced before startup.
- The shipped table is operationally calibrated rather than a full noise-temperature model.
- Historical comparisons across the deployment boundary will show changed glyph distributions.

### Operational impact
- Telnet command syntax is unchanged.
- Saved user records remain compatible.
- Slow clients, broadcast queues, reconnect handling, and shutdown behavior are unchanged.

## Links
- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`, `pathreliability/noise_test.go`, `telnet/path_settings_test.go`
- Related docs: `data/config/path_reliability.yaml`, `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`
- Related TSRs:
- Supersedes / superseded by: Superseded by ADR-0124
