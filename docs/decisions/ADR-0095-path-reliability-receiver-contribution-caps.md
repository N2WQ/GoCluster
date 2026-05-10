# ADR-0095: Path Reliability Receiver Contribution Caps

- Status: Accepted
- Date: 2026-04-30
- Decision Origin: Design

## Context

Path reliability already requires selected evidence to be fresh, above
`min_effective_weight`, and above `min_observation_count`. The observation count
was still raw selected report count, so one high-volume receiver could make a
bucket look better-sampled than it really was.

The operator goal is more trustworthy predictions at a reasonable resource
cost. Exact per-bucket unique receiver sets would add unbounded or large
retained state at the current UI scale of roughly 90k fine and 20k coarse
buckets.

## Decision

Add bounded receiver contribution caps inside each path bucket. Fine buckets
track `receiver_fine_slots` receiver identities; coarse buckets track
`receiver_coarse_slots`. The shipped values are 4 and 8. One receiver can add at
most `receiver_max_effective_count: 5` accepted reports and
`receiver_max_effective_weight: 5.0` accepted weight to a bucket's capped trust
evidence.

The shipped `receiver_contribution_mode` is `shadow`. In shadow mode, existing
raw-count glyph and PATH-filter behavior remains unchanged, while diagnostics
and five-minute logs expose where capped evidence would have blocked the
prediction. `enforce` switches the count and weight gates to capped evidence.
`off` disables capped tracking.

Receiver state is owned by the bucket object and is deleted by the existing
bucket purge and map compaction paths. There are no new maps, side tables,
interners, goroutines, queues, or persistence records.

## Alternatives considered

1. Exact unique receiver sets per bucket. Rejected because exact sets scale with
   historical receiver cardinality per bucket and add more heap than the current
   trust problem needs.
2. A small fresh-window unique receiver sketch. Deferred because it is good
   observability but does not directly cap the count/weight gates without more
   policy complexity.
3. Enforce capped evidence immediately. Deferred because shadow mode lets the
   operator measure false blocks before changing displayed path behavior.
4. Track 8 slots in every bucket. Rejected for v1 because fine buckets dominate
   active cardinality; 4 fine and 8 coarse slots give the coarse fallback more
   diversity at lower heap cost.

## Consequences

### Benefits

- A single receiver can no longer supply unlimited capped trust evidence.
- `SET DIAG PATH` can show capped versus raw selected counts as
  `n<capped>/r<raw>`.
- `Path predictions (5m)` exposes `cap_limited` and `cap_would_block` counters.
- Shadow mode preserves current operator-visible glyph behavior until the cap
  effect is understood.

### Risks

- The fixed slots are an approximation, not an exact all-time unique receiver
  set. When slots are full, the weakest/oldest slot can be reused.
- Enforce mode will produce more `INSUFFICIENT` results on sparse paths and
  paths dominated by one receiver.
- Fine buckets have only four slots by default; coarse evidence carries the
  broader eight-slot diversity fallback.

### Operational impact

- Startup now requires the new receiver-cap YAML keys in
  `path_reliability.yaml`.
- Normal path glyphs and PATH filters are unchanged in the shipped `shadow`
  mode.
- `SET DIAG PATH` and file-only `Path predictions (5m)` logs gain receiver-cap
  observability.
- Slow-client handling, broadcast queues, reconnect handling, peer behavior,
  archive format, and shutdown behavior are unchanged.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/receiver_test.go`, `pathreliability/config_test.go`, `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`, `internal/propreport/report_test.go`
- Related docs: `data/config/path_reliability.yaml`, `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`, `customgpt/common-questions.md`, `customgpt/operator-guide-index.md`
- Related TSRs:
- Supersedes / superseded by: ADR-0129 supersedes the shipped slot ceiling and
  enforcement-mode portions of this decision.
