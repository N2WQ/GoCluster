# ADR-0201: Native 160m Endpoint Daylight Gate

- Status: Accepted
- Date: 2026-06-20
- Decision Origin: Design

## Context

ADR-0197 made native 160m fallback emit `CLOSED`, `LOW`, or `UNLIKELY` from
whole-path civil-dark fraction when p50 evidence is insufficient. That was still
too permissive for paths whose great-circle average looked partly dark while
one endpoint was in daylight.

For 160m, endpoint darkness is a hard practical constraint. Daylight at either
end can dominate the operator outcome because local D-layer absorption near the
transmitting or receiving end is not rescued by a darker middle path. Operators
also need daylight endpoint decisions to be obvious in diagnostics instead of
appearing as a high civil-dark fraction such as `n160c|d82`.

## Decision

Keep the native 160m fallback behind the same precedence gate: sufficient p50
wins, then any usable current-hour VOACAP fallback result, then native 160m,
otherwise blank insufficiency. The shipped runtime VOACAP band list does not
enable VOACAP prediction for 160m, but the precedence rule remains explicit for
future configuration safety.

Native 160m classification now checks endpoint solar state before whole-path
civil-dark fraction:

1. If either endpoint is above the horizon, emit `CLOSED`.
2. Else if either endpoint is below the horizon but still above civil twilight,
   emit `UNLIKELY`.
3. Else both endpoints are civil-dark; use the existing whole-path
   civil-dark-fraction thresholds from ADR-0197:
   `closed_max_civil_dark_fraction`, `unlikely_min_civil_dark_fraction`, and
   `low_min_civil_dark_fraction`.

Do not change the shipped threshold values in this ADR. The change is the order
of evidence: endpoint darkness is required before the whole-path score can
upgrade a native 160m fallback to `LOW`.

Add endpoint reason diagnostics:

- `uD`, `xD`, `bD`: user, DX, or both endpoints daylight, attached to
  `n160c`/`bn160c`.
- `uT`, `xT`, `bT`: user, DX, or both endpoints in civil twilight, attached to
  `n160`/`bn160`.

Keep `dNN` as the whole-path civil-dark fraction so operators can see both the
endpoint veto and the secondary path score.

Add native fallback aggregate counters:

- `endpoint_daylight_closed`
- `endpoint_twilight_unlikely`

Keep the existing `dark_le_closed`, `dark_ge_50`, `dark_ge_75`, and
`dark_ge_90` buckets as path-fraction telemetry.

## Alternatives considered

1. Lower only the existing civil-dark fraction thresholds. Rejected because a
   lower threshold still lets a partly dark path rescue a sunlit endpoint.
2. Add more whole-path waypoints. Rejected because more samples improve the
   average but do not make endpoint darkness a required condition.
3. Treat civil twilight endpoints as `CLOSED`. Rejected for now because 160m
   grayline operation can be marginal rather than impossible; `UNLIKELY` is the
   safer operator-facing cap.

## Consequences

### Benefits

- Daylight endpoint paths such as same-region daytime 160m routes become
  no-brainer `CLOSED` decisions instead of possible `LOW` results.
- Endpoint twilight can no longer become `LOW` only because the rest of the
  path is dark.
- Diagnostics show why native 160m closed or unlikely was emitted.
- Existing path-fraction thresholds remain available as the secondary score for
  both-dark endpoints.

### Risks

- Endpoint state can produce `CLOSED` even when `dNN` is high, so support docs
  and diagnostics must explain that endpoint state wins.
- The daylight horizon cutoff is intentionally strict; near-horizon numerical
  cases may move between daylight and twilight at exact boundary times.
- The native model remains a solar-darkness proxy, not an SNR or probability
  model.

### Operational impact

- `SET DIAG PATH` can show endpoint reason tokens such as `n160c|uD|d82` or
  `n160|xT|d54`.
- `Native 160m fallback (5m)` gains `endpoint_daylight_closed` and
  `endpoint_twilight_unlikely`.
- `Path predictions (5m)` final class counters are unchanged: emitted native
  decisions still appear as `native160_closed`, `native160_low`, and
  `native160_unlikely`.
- `PASS/REJECT PATH CLOSED` continues to apply to native 160m daylight-closed
  decisions because they still map to `filter.PathClassClosed`.

## Links

- Related issues/PRs/commits: -
- Related tests: `pathreliability/native160_fallback_test.go`; `telnet/diag_command_test.go`; `telnet/server_prediction_stats_test.go`
- Related docs: `README.md`; `docs/OPERATOR_GUIDE.md`; `data/config/PATH_PREDICTIONS.md`; `pathreliability/README.md`; `customgpt/support-cards/path-reliability.md`
- Related TSRs: -
- Supersedes / superseded by: Supersedes ADR-0197 native 160m whole-path-only classification order
