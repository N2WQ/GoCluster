# TSR-0014 - Local Self-Spot Delay and Confidence Semantics

Status: Resolved
Date Opened: 2026-03-27
Date Resolved: 2026-03-27
Owner: Cluster maintainers
Technical Area: commands, main output pipeline, telnet delivery, custom_scp, docs
Trigger Source: Chat request
Led To ADR(s): ADR-0056
Tags: self-spot, stabilizer, temporal, confidence, custom_scp

## RCA Summary

- What happened: Local operator self-spots could appear delayed or
  out-of-sequence in telnet output and were not guaranteed to carry `V`
  confidence.
- Why: Runtime treated local self-spots like ordinary local manual spots, so
  resolver, temporal hold, and stabilizer paths could delay them before fan-out.
- What fixed it: ADR-0056 made non-test local `DX` self-spots
  operator-authoritative: they bypass resolver mutation, temporal hold, and
  telnet stabilizer delay, are forced to `V`, and can enter `custom_scp` through
  the existing V-only rail.
- How we know: Code tracing followed `handleDX()` through the output pipeline,
  and targeted `spot`, `main`, and `peer` tests validated the bypass,
  confidence, custom SCP, and peer-publish behavior.
- Operator/support answer: For local self-spot delay reports, distinguish
  operator self-spots from other local manual spots; self-spots should be
  immediate and `V` after ADR-0056.

## Trigger
- Request date: 2026-03-27
- Request summary: operator reported delayed/out-of-sequence local self-spots on 10m SSB and requested that self-spots be immediate, peer-published, shown to telnet users without delay, emitted as `V`, and admitted to `custom_scp`.

## Symptoms and Impact
- What failed or looked wrong?
  - Local self-spots were entering the same resolver/temporal/stabilizer path as other local manual voice spots.
  - The resulting telnet timing could lag the command entry time and display an earlier timestamp later in the stream.
- User/operator impact:
  - Operator-visible delay for self-spots that were intended to represent direct operator authority.
  - Confusion over confidence semantics because self-spots were not guaranteed to carry `V`.
  - Missed `custom_scp` admission for self-spots unless some other path produced `V`.
- Scope and affected components:
  - `commands/processor.go`
  - `output_pipeline_stages.go`
  - `main.go`
  - `output_pipeline_delivery.go`
  - `spot/custom_scp_store.go`
  - operator docs

## Timeline
1. 2026-03-27 - Operator report described delayed local self-spots and out-of-sequence timestamps.
2. 2026-03-27 - Code inspection traced the local `DX` command path, temporal hold, telnet stabilizer, peer publish, and `custom_scp` admission rails.
3. 2026-03-27 - Implemented a narrow local self-spot predicate and runtime bypass, added regression tests, and documented the operator-visible contract in ADR-0056.

## Hypotheses and Tests
1. Hypothesis A - the delay was caused by local telnet backlog or queue buffering.
   - Evidence/commands: inspected telnet broadcast path, writer batching, and queue behavior; existing queues are bounded fail-open/drop paths rather than indefinite buffering.
   - Outcome: Rejected as the primary explanation for this class of report.
2. Hypothesis B - local self-spots were entering normal resolver/temporal/stabilizer rails because they were indistinguishable from other local manual spots.
   - Evidence/commands: traced `handleDX()` into `processSpotBody()` -> `applyResolverStage()` -> `resolveDeliveryPlan()`.
   - Outcome: Supported.
3. Hypothesis C - forcing local self-spots to `V` and bypassing runtime delay rails would preserve peer/telnet queue ownership while enabling `custom_scp` admission through the existing V-only path.
   - Evidence/commands: implemented predicate + runtime guards; added targeted tests in `spot`, `main`, and `peer`.
   - Outcome: Supported.

## Findings
- Root cause (or best current explanation):
  - Local self-spots were not treated as a distinct operator-authoritative class inside the runtime output pipeline, so SSB/USB self-spots could be delayed by resolver-adjacent temporal/stabilizer behavior before telnet fan-out.
- Contributing factors:
  - Existing `custom_scp` admission intentionally required `V`, but self-spots were not guaranteed to become `V`.
  - Peer publishing was already immediate for local `DX` command spots, so the confusing behavior was mainly on the telnet/confidence side.
- Why this did or did not require a durable decision:
  - Required a durable decision because it changes operator-visible timing and confidence behavior for a shared runtime path and alters how `custom_scp` learns from local self-spots.

## Decision Linkage
- ADR created/updated:
  - `docs/decisions/ADR-0056-local-self-spots-operator-authoritative-v-path.md`
- Decision delta summary:
  - Local non-test `DX` command self-spots are operator-authoritative runtime input.
  - They bypass resolver mutation, temporal hold, and telnet stabilizer delay in the runtime main pipeline.
  - They are forced to `V`, and correction-eligible self-spots therefore enter `custom_scp` through the existing V-only admission rail.
- Contract/behavior changes:
  - User-visible telnet timing changes for local self-spots: immediate instead of delay-held.
  - Confidence behavior changes for local self-spots: explicit `V`.
  - No peer protocol or queue contract changes.

## Verification and Monitoring
- Validation steps run:
  - `go test ./spot ./peer . -run 'Test(IsLocalSelfSpot|EvaluateTelnetStabilizerDelayLocalSelfSpotPassesThrough|LocalSelfSpotResolverStageForcesVAndAdmitsCustomSCP|ShouldPublishLocalSelfSpotWhenForwardingDisabled)' -count=1`
- Signals to monitor (metrics/logs):
  - Stabilizer held/immediate counters for self-spot workloads should move toward immediate release.
  - `custom_scp` membership growth may increase modestly where operators self-spot.
  - Peer publish behavior for local `DX` spots should remain unchanged.
- Rollback triggers:
  - Evidence that operator self-spots should not be treated as authoritative `V` input.
  - Unwanted `custom_scp` reinforcement from self-spot activity in live use.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): pending
- Related ADR(s):
  - `docs/decisions/ADR-0043-custom-scp-v-only-admission.md`
  - `docs/decisions/ADR-0046-stabilizer-delay-eligibility-unknown-s-p-only.md`
  - `docs/decisions/ADR-0049-peer-publish-local-human-only.md`
- Related docs:
  - `README.md`
  - `docs/OPERATOR_GUIDE.md`
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
