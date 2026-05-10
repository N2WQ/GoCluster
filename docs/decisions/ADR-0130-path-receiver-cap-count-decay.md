# ADR-0130: Path Receiver Cap Count Decay

- Status: Accepted
- Date: 2026-05-10
- Decision Origin: Troubleshooting chat

## Context

ADR-0129 shipped receiver-cap enforcement for path reliability. The capped
weight lane and capped SNR histogram decayed with the bucket half-life, but the
per-receiver count cap used a lifetime `uint32` slot counter. After a receiver
reached `receiver_max_effective_count`, later reports from that receiver were
blocked forever inside that retained bucket even after the older capped weight
and histogram evidence had decayed.

That made enforce-mode p50 path scoring too conservative on buckets where old
weak reports filled the receiver count cap before newer stronger reports
arrived.

## Decision

Treat receiver capped count as decayed effective evidence, aligned with capped
weight and capped SNR bins.

Per-receiver slot count and bucket capped count decay on the same elapsed-time
clock as capped weight. A new report is admitted fractionally by the smaller of
remaining receiver count capacity and remaining receiver weight capacity. The
accepted fraction updates capped count, capped weight, and the capped SNR
histogram together.

Public `Sample` and telnet diagnostics keep integer counts by flooring the
decayed effective count with a small floating-point epsilon. Raw selected count
remains the non-decayed report count used by `shadow` and `off` modes.

## Alternatives considered

1. Remove `receiver_max_effective_count` and rely only on capped weight.
   Rejected because one receiver could still satisfy the observation floor by
   itself.
2. Reset the lifetime slot count after a fixed wall-clock age.
   Rejected because count would no longer align with the configured half-life
   and capped histogram decay.
3. Keep lifetime count and recommend `shadow` mode.
   Rejected because it preserves a known enforce-mode correctness bug and makes
   capped p50 stale by construction.

## Consequences

### Benefits

- Old receiver evidence can decay out of the cap, letting newer evidence affect
  enforce-mode p50.
- One receiver still cannot satisfy the default sample floor by itself.
- Capped count, capped weight, and capped SNR bins now describe the same
  decayed evidence lane.

### Risks

- `n<capped>` in `SET DIAG PATH` is now a floored effective count, not a
  lifetime accepted-report count.
- Fractional admission is less intuitive than whole-report admission, but it
  keeps capped count and capped weight coherent when one cap has partial room.
- Historical diagnostics before this change are not directly comparable with
  diagnostics after this change.

### Operational impact

- No config keys or telnet command syntax change.
- `receiver_max_effective_count` keeps the same shipped value but now means a
  decayed effective observation cap per receiver per bucket.
- Enforce-mode path glyphs and PATH filters can become less artificially
  conservative where newer capped evidence replaces old weak evidence.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/receiver_test.go`,
  `pathreliability/snr_histogram_test.go`
- Related docs: `data/config/path_reliability.yaml`,
  `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `customgpt/troubleshooting-index.md`
- Related TSRs: TSR-0022
- Supersedes / superseded by: supersedes the lifetime receiver count semantics
  in ADR-0129.
