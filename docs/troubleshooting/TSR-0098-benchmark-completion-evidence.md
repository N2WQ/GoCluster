# TSR-0098 - Benchmark Completion Evidence

Status: Resolved
Date Opened: 2026-10-15
Date Resolved: 2026-10-15
Owner: GoCluster maintainers
Technical Area: Native Windows performance qualification controllers
Trigger Source: Chat request
Led To ADR(s): none
Tags: benchmark, test2json, powershell, qualification, provenance

## RCA Summary

- What happened: S5's first baseline completed one million operations and
  passed, but its controller rejected the missing JSON `bench` action. An
  earlier launch had rejected a correct detached checkout before measurement.
- Why: chatty Go 1.27 benchmark logs go directly to stdout; `--- BENCH` is
  emitted only when buffered logs remain. Separately, native empty output can
  preserve PowerShell's AutomationNull sentinel through a string cast.
- What fixed it: versioned checkers validate actual parent/child RUN events,
  complete work and metrics, and final package PASS; joining native branch
  output produces a comparable empty string. Original failure artifacts remain.
- How we know: actual Go output, installed Go source, mutation controls and
  frozen source/tool/binary identities. The original observation was adopted
  without rerunning it; all remaining measurements and profiles completed.
- Operator/support answer: these were local evidence-checker failures. They
  did not demonstrate a production latency problem or require firewall changes.

## Triggering Request

- Request date: 2026-10-15.
- Request summary: accept S4's stated performance exception, then finish S5-S7
  serially with their own unchanged acceptance gates.
- Request reference: Approved v4 and the first-wave execution record.

## Symptoms and Impact

The original controller stopped before builds on its detached-branch check.
Its replacement then reached a successful baseline result: 1,000,000 operations,
2111 ns/op, 917 B/op and 5 allocs/op. The postprocessor expected a `bench`
event that the actual verbose output did not contain. No candidate observation
or numerical gate verdict existed at that point. S6 remained blocked.

## Timeline

1. 2026-10-15 - preserve the rejected branch preflight; verify both checkout
   identities and correct native empty-output handling in a new controller.
2. 2026-10-15 - preserve the completed baseline and subsequent schema rejection.
3. 2026-10-15 - verify the actual Go stream and chatty logging source; challenge
   the continuation and strengthen exact fixture attribution.
4. 2026-10-15 - adopt the existing observation, execute 95 remaining scored
   observations and eight separate profiles, then verify all 104 raw results.

## Hypotheses and Tests

1. The baseline benchmark failed or did incomplete work.
   - Evidence: exact parent/child RUN, receipts for calibration 1 and measured
     1,000,000, a complete result split across output events, and package PASS.
   - Outcome: Rejected.
2. Every successful verbose benchmark must emit a `bench` action.
   - Evidence: installed Go `testing.go` chatty logging and `benchmark.go`'s
     conditional buffered-log report; actual saved JSON has no such action.
   - Outcome: Rejected.
3. The existing observation can be preserved through a checker-only repair.
   - Evidence: immutable log/invocation/manifest/stop hashes, unchanged saved
     binaries and timestamp, exact invocation controls and final raw audit.
   - Outcome: Supported, with the interruption disclosed below.

## Findings

- Root cause: the completion checker assumed buffered benchmark logging;
  the actual run used chatty logging. The earlier branch check also assumed
  that casting native empty output always creates a regular empty string.

The checker must validate the actual framework stream, including fragmented
result lines. Every present Test identity must equal the selected parent or
child. Work receipts must remain ordered, and missing/duplicate results,
failures, skips and trailing activity must fail qualification.

The continuation kept all twelve balanced pairs per fixture and every original
limit. The first baseline-to-candidate gap was 614.6226298 seconds because of
the checker repair. This is an interpretation limitation; no pair-gap limit
had been specified. No observation was replaced or discarded.

No original exit-code receipt was invented. The old frozen controller reaches
the known rejection only after checking exit zero and the stable executable
hash. That exit evidence is explicitly a source-grounded inference.

## Decision Linkage

- ADR created/updated: none for the checker repair.
- Decision delta summary: no runtime architecture or product policy changed.
- Contract/behavior changes: No contract changes to workload populations,
  timestamp, operation counts, ordering, numerical guards or runtime behavior.
  ADR-0307 separately records the approved report-presence repair.

## Verification and Monitoring

- Final checker: 104 schema/source/metric expectations, including 17 mutations
  of the real stream; 36 adoption/freshness controls; actual Git branch checks.
- Lead-owned raw audit: all 96 scored observations and eight profiles verified.
  Highest paired median ratio was 1.00506233612032 against the fixed 1.10 limit.
  Nonzero fast/compatibility controls retained 5/8 allocs per operation and
  917/1081 bytes per operation respectively in both variants.
- CPU/allocation/live-heap profiles use the standard Go testing profile window,
  including fixture setup. Sampled allocation totals differ from retained
  heap. They do not establish production delay or sustained-load behavior.
- Signals to monitor: exact stream identity and complete work; preserve any
  future failed or inconclusive evidence instead of retrying for a better score.
- Rollback triggers: failure of a nonwaived slice gate stops the next slice.

## References

- Issue(s): owner implementation conversation, Approved v4.
- PR(s): none; local branch work.
- Commit(s): S4 parent `258e4a8`; S5 checkpoint is in the execution record.
- Related ADR(s): [ADR-0307](../decisions/ADR-0307-pskreporter-report-presence.md).
- Related docs: [first-wave execution](../path-reliability-first-wave-execution.md).
- Local evidence: `.tmp/path-reliability-first-wave/s5/performance-v2/` retains
  the original observation and failure; `performance-v3/accepted-controls.json`
  and `performance-v3/final-raw-verification.json` retain completion and audit.
