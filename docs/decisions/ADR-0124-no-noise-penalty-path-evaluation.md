# ADR-0124: No-Noise-Penalty Path Evaluation

- Status: Superseded
- Date: 2026-05-08
- Decision Origin: Design

## Context
Path reliability had a required `noise_offsets_by_band` table that applied a
receive-side penalty by `SET NOISE` class and band at glyph/filter time. Field
review of path diagnostics raised a simpler hypothesis: incoming SNR evidence
already contains the receiving station's noise environment, so applying a broad
band-noise penalty to the whole bucket may add an assumption that is not present
in the observed data.

We want to test that hypothesis without changing the user command surface,
saved user records, YAML schema, or startup validation.

## Decision
Keep the `noise_offsets_by_band` table required and preserve all existing noise
classes and band keys, but set the checked-in and in-memory default penalties to
0 dB for every class and band.

`SET NOISE` continues to store a user's selected noise class, and the runtime
still resolves `class + band` through the same lookup path. With the checked-in
configuration, that lookup has no effective receive-side adjustment.

## Alternatives considered
1. Keep the band-specific nonzero table from ADR-0064. Rejected for this
   evaluation because it may adjust already-observed SNR evidence with a broad
   receiver-noise assumption.
2. Remove `SET NOISE` and the YAML table. Rejected because it would add schema,
   command, help, persisted-user, and support churn before the diagnostic
   evidence proves the command is no longer useful.
3. Replace the table with a per-user or per-station calibrated model. Deferred
   because the live spot stream does not yet contain enough station-specific
   receive-noise evidence to support that model safely.

## Consequences
### Benefits
- Active glyphs and PATH filters are easier to interpret against observed SNR.
- Schema, command, help, and saved user compatibility are preserved.
- Future logs can compare path behavior across the no-penalty boundary without
  also changing parser or command semantics.

### Risks
- `SET NOISE` is behaviorally inert while the checked-in penalties remain zero.
- Historical glyph distributions are not directly comparable across this
  deployment boundary.
- Some noisy local stations may see more optimistic receive-side glyphs if the
  removed penalties were compensating for their actual environment.

### Operational impact
- Telnet command syntax is unchanged.
- Saved user records remain compatible.
- Slow clients, broadcast queues, reconnect handling, and shutdown behavior are
  unchanged.

## Links
- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`, `pathreliability/noise_test.go`, `telnet/path_settings_test.go`
- Related docs: `data/config/path_reliability.yaml`, `README.md`, `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`, `telnet/README.md`
- Related TSRs:
- Supersedes / superseded by: Supersedes ADR-0064; superseded by ADR-0127
