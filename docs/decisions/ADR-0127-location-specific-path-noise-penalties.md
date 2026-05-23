# ADR-0127: Location-Specific Path Noise Penalties

- Status: Accepted
- Date: 2026-05-09
- Decision Origin: Design

## Context
Path reliability previously moved from scalar `SET NOISE` penalties to a
band-specific `noise_offsets_by_band` table, then temporarily set that table to
0 dB everywhere while evaluating whether observed SNR evidence should stand
without an additional broad receive-noise adjustment.

That shape left two problems: the checked-in schema still carried a large
band-specific table even when band-specific penalties were no longer wanted,
and `SET NOISE` became behaviorally inert while the values were all zero.

The earlier scalar checked-in values were:

- `quiet: 0`
- `rural: 4`
- `suburban: 12`
- `urban: 17`
- `industrial: 20`

## Decision
Path reliability uses a required scalar `noise_offsets` table keyed only by
noise class. The configured penalty is subtracted from the DX-to-user receive
side at prediction time and applies equally on every band.

The obsolete `noise_offsets_by_band` YAML key is rejected at startup with a
clear configuration error. `SET NOISE` command syntax and saved user records are
unchanged because saved records store only the selected noise class.

Propagation-report model context exposes the same scalar `noise_offsets` table
instead of the removed band-specific table.

## Alternatives considered
1. Keep `noise_offsets_by_band` with repeated scalar values per band. Rejected
   because it preserves the removed schema and adds unnecessary config surface.
2. Keep the no-noise all-zero table from ADR-0124. Rejected because the desired
   behavior is location-specific receive penalties, not an inert command.
3. Remove `SET NOISE` entirely. Rejected because persisted user compatibility
   and operator command semantics remain useful with scalar penalties.

## Consequences
### Benefits
- The checked-in config matches the intended scalar location model.
- Runtime lookup becomes a single class map lookup instead of class+band lookup.
- `SET NOISE` again has a direct effect on active glyphs and PATH filters.
- Reported model context is smaller and easier to interpret.

### Risks
- Existing deployments with `noise_offsets_by_band` must migrate to
  `noise_offsets` before startup.
- Nonzero receive penalties make noisy classes more conservative on every band.
- Propagation-report JSON consumers must read `noise_offsets` instead of
  `noise_offsets_by_band`.

### Operational impact
- Telnet command syntax is unchanged.
- Saved user records remain compatible.
- Slow clients, broadcast queues, reconnect handling, and shutdown behavior are
  unchanged.

## Links
- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`, `pathreliability/noise_test.go`, `telnet/path_settings_test.go`, `internal/propreport/report_test.go`
- Related docs: `data/config/path_reliability.yaml`, `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`, `telnet/README.md`
- Related TSRs:
- Supersedes / superseded by: Supersedes ADR-0124
