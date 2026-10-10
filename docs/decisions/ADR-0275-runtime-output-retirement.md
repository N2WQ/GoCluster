# ADR-0275: Runtime Output Retirement

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

The runtime must retain downstream resources while the output pipeline processes
completed human-comment classification, temporal/FT holds and released stabilizer
envelopes. Dedup Stop does not close output. Classifier worker retirement is a
component boundary; it does not join output processing or delayed fanout.

Approved v3 distinguishes bounded classifier grace from overall shutdown and
defines delivery as normal processing plus an attempted existing handoff. It
does not guarantee network receipt or prevent intended queue drops/suppression.

## Decision

- Runtime installs a private pipeline stop/completion handle before launch.
  Normal shutdown and partial-startup close share one complete retirement owner;
  primary dedup Stop is invoked once without closing its producer-owned output.
- The first pipeline retirement request starts the existing classifier grace
  budget. Repeated requests do not extend it. One owned timer worker invokes
  terminal classifier Stop at expiry or early drain completion and is joined.
  It never applies results or performs fanout.
- The output loop remains the sole classifier processing consumer. Ordinary
  receives give a published stop priority. Both ordinary and grace result paths
  check processing admission after receive and before ApplyResult, because a
  delayed select can choose ready data after expiry.
- A physically received expired result remains consumer-owned and is discarded
  before spot mutation. Once admitted within grace, normal processing and
  handoff finish even after expiry. Terminal Stop cannot revoke that ownership.
- Cancel unfinished HTTP work at grace expiry while retaining downstream owners
  until admitted processing completes. Grace is not an overall shutdown deadline;
  synchronous downstream callbacks must return and are never abandoned.
- After classifier drain, force-retire temporal then FT holds, including holds
  created by late results or temporal release. Use actual UTC evaluation time,
  select future-due temporal entries too, and stop/clear both timers.
- Preserve the temporal decoder's existing score/margin/evidence/fallback rules.
  Force terminates waiting; it does not waive gates or imply correction accuracy.
  Age is actual evaluation minus observation time, not a synthetic future age.
- Fence terminal stabilizer admission/retry, retire every unreleased input/heap
  reservation as its existing shutdown drop, and preserve released ownership.
  Join both scheduler and release consumer before reporting pipeline completion.
- Peer/telnet/archive/grid/store dependencies stop only after this completion.
  Existing normal suppression, secondary dedupe, retry limits, timeout actions,
  queue overflow and handoff behavior remain authoritative.

## Alternatives considered

1. Wait for dedup output closure or close it externally. Stop does not close it;
   consumer closure lacks the broader producer/sender join contract.
2. Cancel the global runtime context before grace. This can retire dependencies
   before processing and peer withdrawal own their bounded work.
3. Drain/process/Stop sequentially. A blocked admitted callback postpones HTTP
   cancellation beyond grace. One owned expiry timer separates the boundaries.
4. Use AfterFunc with callback cancellation bookkeeping. Viable, but an explicit
   timer owner has a direct early-completion and join contract here.
5. Add 24 hours or force all classifications to fallback. The former contaminates
   age and cannot cover every accepted lag; the latter changes selected model policy.
6. Force-deliver unreleased stabilizer holds. This changes existing shutdown-drop
   behavior and normal final-gate timing without an approved product need.

## Consequences

### Benefits
- Runtime completion reflects pipeline and released fanout ownership.
- HTTP cancellation stays bounded independently of admitted processing.
- Late holds retire and temporal metrics use truthful elapsed time.

### Risks
- A synchronous downstream callback can extend overall shutdown; abandoning it
  would violate ownership. The classifier's cancellation bound remains separate.
- Prompt HTTP worker retirement relies on the client's cancellation contract.
- Terminal stabilizer admission gains a short mutex; its capacities remain fixed.

### Operational impact
- No schema, protocol, configuration key/default, scoring or archive format change.
- Existing classifier grace is clamped to 500ms through five seconds; this is
  not a promise that the whole process stops within five seconds.
- Attempted handoff can still be suppressed or dropped by existing bounded gates.
- A separate non-steered review established causal timing/force semantics and
  independently derived boundary vectors. No held-out truth/calibration evidence
  establishes correction accuracy, probabilities or optimal parameters here.

## Links
- Related issues/PRs/commits: approved v3 scope ledger, checkpoint 20b.
- Related tests: `internal/cluster/main_runtime_output_retirement_test.go`,
  `output_pipeline_http_retirement_test.go`, `output_pipeline_grace_admission_test.go`,
  `temporal_retirement_test.go`, `stabilizer_retirement_test.go`.
- Related docs: `docs/command-fix-execution.md`, `data/config/README.md`.
- Related TSRs: [TSR-0069](../troubleshooting/TSR-0069-output-pipeline-retirement.md).
- Supersedes / superseded by: none; refines runtime ownership alongside
  [ADR-0274](ADR-0274-terminal-classifier-retirement.md), retirement timing in
  [ADR-0038](ADR-0038-fixed-lag-temporal-decoder-main-replay-parity.md), and lifecycle
  completion for [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md).
