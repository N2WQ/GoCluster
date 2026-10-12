# ADR-0297: Core Accepted Observation Learning

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Troubleshooting chat

## Context

History represents accepted receiver observations. Previously, immediate
stabilized learning depended on secondary output eligibility, unlike delayed
and non-stabilized acceptance. Routing and congestion could therefore change
future method support under the same core decision. The owner selected one
consistent core-acceptance boundary.

## Decision

Learn eligible observations once after core validation and temporal/FT/stabilizer
resolution. Immediate, overflow, delayed and non-stabilized accepted paths
record before secondary output gates. Use one wall UTC acceptance clock for the
existing general history, FT history and WhoSpotsMe recorder group. Source-time
admission remains store-specific; this does not replace observation timestamps.

Pending, reheld, suppressed and discarded work returns before recording. Learn
the final accepted identity: an accepted correction teaches the replacement;
a rejected alternative teaches the retained identity when it is accepted.
Preserve source/mode/SNR/cell/beacon/test rules of each store, especially
CustomSCP's V-only admission. Do not introduce uniform new exclusions or an
already-learned registry.

Secondary FAST/MED/SLOW thinning, family hiding, queue admission/refusal and
client receipt do not govern learned call history or WhoSpotsMe. Family display
witnesses still require actual successful lane admission under ADR-0295.
Ring/archive/peer/telnet fanout and their metrics remain at existing boundaries.

Preserve shutdown ownership and ordering. Unreleased discarded work learns zero
times. Already transferred stabilizer envelopes and forced temporal/FT releases
may learn once per observation if they reach acceptance, including after stop.
Releases rejected, suppressed or discarded before core acceptance learn zero.
Output refusal after acceptance still learns. Add no blanket stop
guard and no worker, queue, retained map or capacity change.

## Alternatives considered

- Output eligibility as the learning boundary makes routing/pressure part of
  the model's evidence. Rejected for the selected accepted-observation meaning.
- Queue admission or client receipt adds destination-specific meaning and, for
  receipt, tracking/coordination not required by this bounded method.
- A duplicate registry treats a flow-ownership defect as retained state. Clear
  accepted call sites and occurrence oracles establish once-only behavior.

## Consequences and operational impact

Immediate stabilized observations rejected by secondary output now learn when
store-eligible; other accepted paths use the same boundary. This can change
future historical support. CustomSCP eligibility is not broadened. Output and
family display admission remain unchanged. WhoSpotsMe counts observations,
while distinct-receiver histories can overwrite repeated receiver timestamps.
Local contract tests do not establish calibrated identity accuracy or load limits.

## Links and supersession

- Clarifies/narrowly supersedes any learned-history admission interpretation of
  [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md) and
  [ADR-0295](ADR-0295-fresh-admitted-family-display.md). Their shared saved-output
  gates and successful family-lane witnesses remain accepted.
- [ADR-0296](ADR-0296-legacy-history-clock-and-capacity.md) still owns legacy/FT
  timestamp eligibility and actual-owner capacity.
- Troubleshooting: [TSR-0090](../troubleshooting/TSR-0090-accepted-observation-learning-boundary.md).
- Authority and evidence: [ledger](../call-correction-followup-scope-ledger-v2.md)
  and [execution record](../call-correction-followup-execution.md).
