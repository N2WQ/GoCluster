# TSR-0012 - PC92 Hop Suffix Amplification and Overlong Diagnostics Gap

Status: Resolved
Date Opened: 2026-03-05
Date Resolved: 2026-03-05
Owner: Codex
Technical Area: peer/protocol, peer/manager, peer/reader
Trigger Source: Chat request
Led To ADR(s): ADR-0050
Tags: peering, pc92, loop suppression, observability

## RCA Summary

- What happened: Relayed `PC92` topology frames could grow repeated hop suffixes
  such as `^H95^H94...`, increasing overlong-line risk and noisy topology
  churn.
- Why: Local parse/encode appended a new hop without canonicalizing existing
  hop fields, and raw hop-bearing dedupe keys allowed each route variant
  through.
- What fixed it: ADR-0050 made forwarding canonicalize to one trailing hop,
  dedupe `PC92` semantically before topology enqueue, and emit bounded
  reason-coded overlong diagnostics.
- How we know: A local repro showed suffix accumulation; peer parser/encoder,
  keying, handle-frame tests, and hop-suffix fuzzing validated the fix.
- Operator/support answer: For peer "long file" or overlong `PC92` reports,
  inspect relayed lines for stacked hop markers and confirm semantic `PC92`
  dedupe plus canonical hop encoding are active.

## Triggering Request
- Request date: 2026-03-05
- Request summary: Review peering code for root cause of remote "long file" errors showing `PC92` lines with repeated hop markers (`^H95^H94^H93...`).
- Request reference (chat/issue/link): Chat request in this implementation thread.

## Symptoms and Impact
- What failed or looked wrong? Peer-side diagnostics showed unusually long `PC92` lines with stacked hop markers and repeated near-duplicate topology records.
- User/operator impact: Increased risk of oversized-line drops, topology queue churn, and noisy/misleading diagnostics during peering storms.
- Scope and affected components: `peer.ParseFrame`/`Frame.Encode`, `peer.Manager.HandleFrame` `PC92` flow, `peer` overlong logging path.

## Timeline
1. 2026-03-05 08:2x - Root-cause review traced inbound `PC92` handling and forwarding path.
2. 2026-03-05 08:3x - Local repro confirmed `Frame.Encode` appended hop suffixes without removing existing suffixes.
3. 2026-03-05 08:4x - Fixes implemented and validated with peer unit tests and parser fuzzing.

## Hypotheses and Tests
1. Hypothesis A - Remote peer generated malformed stacked hops.
   - Evidence/commands: inspected local forwarding path and reproduced from a normal single-hop frame.
   - Outcome: Rejected as primary cause.
2. Hypothesis B - Local `Frame.Encode` appends new `Hn` without canonicalizing existing hop fields.
   - Evidence/commands: `go run tmp repro` showed `...^H95^` -> `...^H95^H94^`; repeated encode produced cumulative suffix growth.
   - Outcome: Supported.
3. Hypothesis C - Dedupe and enqueue ordering amplified looped `PC92` variants.
   - Evidence/commands: code inspection showed `pc92Key` used raw frame text (hop-sensitive) and topology enqueue occurred before dedupe in `HandleFrame`.
   - Outcome: Supported.

## Findings
- Root cause (or best current explanation): Hop suffix canonicalization was missing in parse/encode, causing suffix accumulation (`H95,H94,...`) across relays; hop-sensitive raw dedupe allowed each variant through.
- Contributing factors: `PC92` dedupe key used raw wire text, and overlong diagnostics lacked reason-coded summaries.
- Why this did or did not require a durable decision: This changes protocol-forwarding and observability contracts across peering and needed explicit long-term rationale.

## Decision Linkage
- ADR created/updated: ADR-0050
- Decision delta summary: Canonical single-hop forwarding, semantic hop-insensitive `PC92` dedupe before topology enqueue, and bounded reason-coded overlong diagnostics.
- Contract/behavior changes (or `No contract changes`): Peering forwarding contract now guarantees one trailing hop token; `PC92` duplicate suppression is semantic (not raw-text/hop-sensitive); overlong diagnostics expose reason/limit fields.

## Verification and Monitoring
- Validation steps run:
  - `go test ./peer -run "Test(ParseFrame|Encode|PayloadFields)"`
  - `go test ./peer -run "Test(PC92Key|HandleFramePC92)"`
  - `go test ./peer`
  - `go test ./peer -run ^$ -fuzz FuzzParseFrameHopSuffix -fuzztime 15s`
- Signals to monitor (metrics/logs):
  - Absence of stacked hop suffixes (`^H\d+\^H\d+\^`) in relayed `PC92` lines.
  - Overlong summaries in logs with reason buckets (`pc92_max_bytes`, `max_line_length`).
  - Reduced `Peering: dropping PC92 ... topology queue full` frequency under loop conditions.
- Rollback triggers: Any interoperability issue from canonicalized hop suffix behavior with legacy peer implementations.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0050
- Related docs: `README.md`, `data/config/peering.yaml`
