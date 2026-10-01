# ADR-0050 - Peering Hop-Suffix Canonicalization, Semantic PC92 Dedupe, and Overlong Diagnostics

Status: Accepted
Date: 2026-03-05
Decision Makers: Core maintainers
Technical Area: peer/protocol, peer/manager, peer/reader
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0012
Tags: peering, protocol contract, loop suppression, observability

## Context
- Peer diagnostics showed relayed `PC92` lines accumulating hop markers (`^H95^H94^H93...`), inflating line size and churn.
- Existing dedupe for `PC92` was keyed by raw wire text, so otherwise identical looped frames with decremented hop were treated as unique.
- Topology enqueue happened before dedupe, so looped `PC92` variants could consume bounded topology queue capacity.
- Oversized line handling dropped data safely but lacked reason-coded, bounded operator diagnostics.

## Decision
- Later refinement: [ADR-0230](ADR-0230-pc18-pc92-authority-and-bounds.md)
  replaces PC92 application ordering with manager-owned validation, shared
  freshness and transactional authority admission. Hop-insensitive keys and
  bounded overlong diagnostics remain effective.
- Canonicalize hop suffix handling in peer protocol parse/encode:
  - Strip trailing hop suffix runs at parse time.
  - Store one effective hop value (rightmost numeric hop token).
  - Encode with exactly one trailing `^Hn^` token when `hop > 0`.
- Change `PC92` dedupe semantics to be hop-insensitive and payload-based:
  - Build key from canonical payload fields (`origin`, `timestamp`, `record type`, hashed entries).
  - Apply dedupe before topology enqueue and forwarding for `hop > 1`.
- Harden overlong diagnostics while keeping drop semantics unchanged:
  - Add `reason` and `limit` metadata to `ErrLineTooLong`.
  - Emit periodic reason-bucket summaries.
  - Keep sample logging best-effort and rotate `logs/peering_overlong.log` at bounded size.

## Alternatives Considered
1. Keep raw-frame forwarding and only increase `max_line_length`/`pc92_max_bytes`
   - Pros:
     - No protocol parser changes.
   - Cons:
     - Preserves amplification bug and only postpones failure.
2. Keep hop-sensitive dedupe but increase dedupe TTL/capacity
   - Pros:
     - Minimal code churn.
   - Cons:
     - Does not collapse hop-variant duplicates; queue churn remains.
3. Disconnect peers on first overlong line
   - Pros:
     - Fast containment of abusive inputs.
   - Cons:
     - Too aggressive for transient/malformed bursts; harms resilience.

## Consequences
- Positive outcomes:
  - Deterministic single-hop forwarding contract.
  - Loop suppression for semantic duplicates independent of hop value.
  - Reduced topology queue pressure from looped `PC92` variants.
  - Better operator diagnostics for overlong drops with bounded log growth.
- Negative outcomes / risks:
  - Slight behavior change for malformed frames that relied on legacy raw hop-field retention.
  - Additional hashing work in `pc92Key` (bounded and low overhead).
- Operational impact:
  - Easier triage using reason-coded overlong summaries.
  - Lower probability of remote long-line errors caused by stacked-hop amplification.
- Follow-up work required:
  - Monitor interoperability with diverse DXSpider peers and adjust parsing strictness only if necessary.

## Validation
- Added/updated tests:
  - `peer/protocol_test.go` (single-hop, stacked-hop, malformed trailing hop handling, canonical encode).
  - `peer/keys_test.go` (hop-insensitive `PC92` dedupe key behavior).
  - `peer/manager_test.go` (`PC92` duplicate suppression before topology enqueue/relay).
  - `peer/reader_test.go` (reason-coded overlong classification).
  - `peer/overlong_test.go` (bounded sample log rotation).
  - `peer/protocol_fuzz_test.go` (hop-suffix parser roundtrip invariants).
- Executed validation commands:
  - `go test ./peer -run "Test(ParseFrame|Encode|PayloadFields)"`
  - `go test ./peer -run "Test(PC92Key|HandleFramePC92)"`
  - `go test ./peer`
  - `go test ./peer -run ^$ -fuzz FuzzParseFrameHopSuffix -fuzztime 15s`
- This decision would be invalidated if canonical hop handling causes verified protocol incompatibility with required production peers.

## Rollout and Reversal
- Rollout plan:
  - Deploy as normal with current peering config.
  - Monitor peering logs for overlong summary rates and topology queue drops.
- Backward compatibility impact:
  - Forwarded wire format remains `^Hn^` but no longer preserves malformed stacked hop suffixes.
- Reversal plan:
  - Restore legacy parse/encode behavior and raw-text `PC92` dedupe key; mark this ADR superseded.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0047, ADR-0049
- Troubleshooting Record(s): TSR-0012
- Docs: `README.md`, `data/config/peering.yaml`
