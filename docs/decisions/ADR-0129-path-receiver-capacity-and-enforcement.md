# ADR-0129: Path Receiver Capacity And Enforcement

- Status: Accepted
- Date: 2026-05-10
- Decision Origin: Design

## Context

ADR-0095 added bounded per-bucket receiver contribution caps with four fine
slots, eight coarse slots, and shipped `shadow` mode. Later review of the path
reliability method found that shadow mode preserves raw concentrated evidence
for operator-visible glyphs and PATH filters. A single receiver could therefore
carry enough raw reports to make a path look usable even when capped evidence
would not pass.

The p50 histogram makes SNR scoring more robust than averaging, but it does not
solve receiver concentration. A histogram weighted mostly by one receiver still
represents that receiver more than the broader path population.

## Decision

Raise the compiled receiver slot ceilings to six fine slots and twelve coarse
slots. Ship `path_reliability.yaml` with `receiver_fine_slots: 6`,
`receiver_coarse_slots: 12`, `receiver_contribution_mode: enforce`, and the
existing per-receiver caps of five accepted reports and five effective weight
units.

The maximum active capped sample capacity is therefore thirty observations in a
fine bucket and sixty observations in a coarse bucket. YAML may select smaller
values, but the compiled ceiling remains the retained-state guardrail.

## Alternatives considered

1. Keep four fine and eight coarse slots. Rejected because enforce mode plus a
   20-sample floor leaves too little fine-bucket diversity headroom.
2. Make slot limits fully YAML-driven. Rejected because one config edit could
   multiply retained heap and per-update scan cost across all path buckets.
3. Increase `receiver_max_effective_count` instead. Rejected because it allows
   each receiver to dominate more of the evidence instead of increasing source
   diversity.

## Consequences

### Benefits

- Enforce mode gates operator-visible glyphs and PATH filters on capped
  receiver evidence.
- Six fine slots allow a 20-sample floor to pass without requiring every
  retained receiver to reach the five-report cap.
- Twelve coarse slots give regional fallback evidence more receiver diversity.

### Risks

- Each active bucket can retain more receiver slot state than ADR-0095 allowed.
- Receiver slot selection scans more entries per update, so the path store
  hot-path benchmark must be checked.
- Sparse or single-receiver-dominated paths can show more insufficient glyphs.

### Operational impact

- Startup accepts `receiver_fine_slots` up to 6 and `receiver_coarse_slots` up
  to 12.
- The checked-in path config uses enforce mode, so capped count and capped
  weight gate path glyphs and PATH filters.
- `SET DIAG PATH` remains the operator tool for seeing capped/raw count splits
  and low-count/low-weight reasons.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`,
  `pathreliability/receiver_test.go`, `pathreliability/snr_histogram_test.go`,
  `pathreliability/store_bench_test.go`
- Related docs: `data/config/path_reliability.yaml`,
  `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `customgpt/common-questions.md`,
  `customgpt/operator-guide-index.md`
- Related TSRs:
- Supersedes / superseded by: supersedes the shipped slot ceiling and
  enforcement-mode portions of ADR-0095; superseded by ADR-0130 for receiver
  count-decay semantics and by ADR-0135 for the shipped cap value.
