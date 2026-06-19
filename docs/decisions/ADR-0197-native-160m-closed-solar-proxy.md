# ADR-0197: Native 160m Closed Solar Proxy

- Status: Accepted
- Date: 2026-06-19
- Decision Origin: Design

## Context

ADR-0196 added native 160m solar-darkness fallback only for the gap where p50
evidence is insufficient and VOACAP has no usable current-hour result. The
first fallback emitted only `LOW` or `UNLIKELY`, which left daylit or
low-darkness 160m paths indistinguishable from ordinary blank insufficiency.

Operators need `CLOSED` to remain directly filterable for closed fallback spots
while preserving the high-level precedence order: sufficient p50 wins, then
usable VOACAP fallback, then native 160m fallback, otherwise blank
insufficiency.

## Decision

Extend native 160m fallback so it can emit only `CLOSED`, `LOW`, or `UNLIKELY`.
It still never emits `HIGH` or `MEDIUM`, never replaces sufficient p50, and
still yields to any usable current-hour VOACAP result.

Add required YAML setting
`path_reliability.native_160m_fallback.closed_max_civil_dark_fraction`, shipped
as `0.25`. Native 160m classification is:

1. `civil_dark_fraction <= closed_max_civil_dark_fraction`: emit `CLOSED`.
2. `civil_dark_fraction >= low_min_civil_dark_fraction`: emit `LOW`.
3. `civil_dark_fraction >= unlikely_min_civil_dark_fraction`: emit `UNLIKELY`.
4. Otherwise leave the path blank as insufficient.

Native 160m `CLOSED` is a solar-darkness proxy, not a VOACAP SNR result. It
maps to `filter.PathClassClosed`, remains compatible with `UNLIKELY` PATH
filters, and is directly selectable with `PASS/REJECT PATH CLOSED`. R/G solar
weather overrides must not replace a final `CLOSED` path class.

Add `n160c|dNN` and `bn160c|dNN` diagnostics, final
`native160_closed` path-prediction counters, native fallback `closed` and
`dark_le_closed` counters, propagation-report parsing, operator docs, support
cards, and generated code-map refresh.

## Alternatives considered

1. Keep daylit native 160m paths blank. Rejected because it hides a useful
   operator distinction between "no fallback emitted" and "native 160m solar
   proxy says closed."
2. Use VOACAP closed SNR semantics for native 160m. Rejected because native
   fallback has no SNR model; it only observes path civil-dark fraction.
3. Add a new PATH class distinct from `CLOSED`. Rejected because operators
   already have filter semantics for closed fallback spots and `UNLIKELY`
   compatibility.

## Consequences

### Benefits

- Keeps the p50 -> VOACAP -> native 160m -> blank hierarchy intact.
- Makes low-darkness native 160m fallback spots directly filterable as
  `CLOSED`.
- Preserves operator compatibility for `PASS/REJECT PATH UNLIKELY`.
- Adds reason-level counters for the native closed bucket without changing
  VOACAP worker or cache semantics.

### Risks

- Native 160m `CLOSED` can be mistaken for VOACAP SNR closure unless diagnostics
  and docs keep the distinction explicit.
- The shipped `0.25` closed threshold is an experiment parameter and may need
  calibration from on-air evidence.
- The blank middle band between closed and unlikely can still surprise users
  who expect every evaluated native candidate to emit a glyph.

### Operational impact

- `Path predictions (5m)` gains `native160_closed`.
- `Native 160m fallback (5m)` gains `closed` and `dark_le_closed`.
- `SET DIAG PATH` can show `n160c|dNN` or `bn160c|dNN`.
- `PASS/REJECT PATH CLOSED` applies to VOACAP closed fallback and native 160m
  low-darkness `CLOSED`.

## Links

- Related issues/PRs/commits: experiment branch `experiment/160m-native-heuristic`
- Related tests: `pathreliability/native160_fallback_test.go`; `pathreliability/config_test.go`; `telnet/server_prediction_stats_test.go`; `telnet/diag_command_test.go`; `internal/propreport/report_test.go`
- Related docs: `README.md`; `data/config/PATH_PREDICTIONS.md`; `pathreliability/README.md`; `docs/OPERATOR_GUIDE.md`; `customgpt/support-cards/path-reliability.md`
- Related TSRs: -
- Supersedes / superseded by: Supersedes ADR-0196
