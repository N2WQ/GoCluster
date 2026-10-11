# ADR-0284: Original-Content Peer Publication

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Native PC11/PC61/PC26 transit already owns and relays validated original fields
independently of local corrections. Local human/manual publication instead used
the final locally processed spot. A successful correction could therefore change
the callsign published to peers, and formatter trimming, sanitization and
one-decimal frequency rounding could rewrite other accepted source fields.

The owner selected original input for all peer spot output, including local DX
commands, exact representable comments and refusal of unrepresentable output.
Local correction remains useful for local display and history.

## Decision

Local publication owns a compact immutable record of the accepted source `Spot`
after primary normalization and completed toxicity classification, before local
output preparation, correction or frequency averaging. This boundary preserves
accepted semantic fields, not literal raw command-line spelling or precision
already changed by command parsing. Classification may annotate toxicity but
does not change publication fields before capture.

Carry the source record with existing output, temporal, FT and stabilizer owners
through release, retry, overflow and shutdown. Never regenerate it from the
mutated local spot. Do not add a provenance map, queue, worker or persisted
field. Local display, ring history and archive continue to consume the locally
processed result.

Preserve exact representable comment bytes without blank-mode fallback,
trimming, delimiter replacement or sanitization. Empty comments, carets and
prohibited bytes refuse the destination variant. Preserve captured finite
numeric frequency without additional one-decimal rounding and preserve valid
captured IP text. Retain existing UTC date/minute encoding, local origin, hop and
sentence framing.

Validate each existing modern PC61 or legacy PC11 variant against its grammar
and size limit. Do not invent missing fields, truncate payloads or add a
modern-to-legacy fallback. Independently valid legacy publication can proceed
when a modern variant is unrepresentable. Refusal does not imply local delivery
failure or create an automatic retry.

Preserve native received-frame relay, source eligibility, forwarding enablement,
shared post-stabilizer MED admission, suppression, dedupe, queue limits and
delivery timing. Received peer spots remain excluded from the local publisher,
preventing duplicate relay. PC11/PC61 carry original content, not local
confidence percentages or rejection reasons.

This partially supersedes only ADR-0148's final peer content-sharing contract.
Its shared final fanout timing and delivery gates remain accepted.

## Alternatives considered

1. Publish the corrected shared spot: violates the selected forwarding contract.
2. Clone the entire spot at final fanout: captures corrected content too late and
   retains unrelated local metadata.
3. Recover source fields from a global identity map: adds boundedness, collision
   and retirement concerns without an ownership need.
4. Sanitize invalid originals or fall back from modern PC61 to PC11: rewrites
   input or changes destination compatibility policy without authorization.
5. Capture literal DX command text: changes the existing parser boundary and
   expands scope beyond preserving accepted publication fields.

## Consequences

### Benefits

- Peer recipients receive accepted original content while local users retain
  locally corrected output.
- Delayed publication has explicit immutable ownership without new retained
  registries or asynchronous workers.
- Refusal is separate from payload rewriting.

### Risks

- Blank local comments previously filled with inferred mode now refuse peer
  publication. Invalid comments or IP fields can also reduce peer output while
  local output remains valid.
- Compact capture and encoding add bounded per-input work; allocation and cap
  checks are required before making performance claims.
- GoCluster field validity and byte fidelity do not prove acceptance by every
  downstream receiver.

### Operational impact

- No YAML, archive schema, normal spot format or PC11/PC61 field-layout change.
- Existing delivery suppression remains in force; this decision does not repair
  family-suppression behavior or enable skimmer republication.
- Validation must observe local corrected delivery and original peer receipt
  separately, including held paths and destination-specific refusal.

## Links

- Related issues/PRs/commits: approved evidence scope v2, B0.
- Related tests:
  [connected local correction and peer delivery](../../internal/cluster/peer_original_delivery_test.go),
  [held-path original-content delivery](../../internal/cluster/peer_original_hold_delivery_test.go),
  [local publication grammar and variant refusal](../../peer/local_spot_publication_test.go),
  [native relay contract](../../peer/spot_relay_contract_test.go) and
  [native receiver interoperability](../../peer/spot_relay_interop_test.go).
- Related docs: [domain contract](../domain-contract.md#original-content-local-peer-publication),
  [peer behavior](../../peer/README.md#publishing-rules),
  [support card](../../customgpt/support-cards/peer-bulletin-dedupe.md#locally-published-spots).
- Related TSRs: none; this is a selected forwarding policy, not new incident
  evidence.
- Supersedes / superseded by: partially supersedes
  [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md) for peer content
  only. Preserves its timing/gates and native relay under
  [ADR-0239](ADR-0239-peer-original-validation-and-relay.md),
  [ADR-0240](ADR-0240-peer-dxspider-date-admission.md) and
  [ADR-0241](ADR-0241-peer-frame-payload-and-comment-framing.md).
