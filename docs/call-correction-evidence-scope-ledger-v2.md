# Call-correction evidence scope ledger v2

Status: Approved by the user's exact `Approved v2` on 2026-10-11.
Baseline: `3fbf87ff7ed379f72b3ce1479888a7f918e8f29f`.
Implementation branch: `fix_correction_evidence`.

This records the approved conversation ledger, superseding the unapproved
Batch 1-3 working draft. One approval authorizes the sequence below, one fully
validated patch at a time. Earlier P1-P5 and CustomSCP repairs remain intact.
Push, merge, deployment and external live-cluster testing are separate actions.

## Objective and selected policy

Repair historical qualification, numerical integrity and correction authority
after known evidence loss. Preserve original peer-publication content separately
from local correction. Support percentages are evidence shares, not calibrated
correctness probabilities. Fresh observations inside the existing key/window do
not prove association with the original transmission episode.

Direct user selections govern:

- Each historical receiver and cell qualifies only through its own observation's
  age score and that consumer's requirements. Future observations are retained
  but excluded at query time; existing inclusive stored-second boundaries stay.
- Any family member may qualify only by passing every applicable gate on its
  own. No pooled qualification. Impossible enabled requirements fail validation.
- Future learned static membership cannot qualify recognition or the S floor.
- Preserve finite reliability clamping and valid model results; reject nonfinite
  weights, negative counts, nonfinite/nonascending edges and overflowing totals.
  Invalid explicitly configured assets fail startup/replay. Missing, nonfinite
  or out-of-domain SNR omits model contribution; valid bin boundaries stay.
- Candidate/receiver-cap loss blocks correction and current-consensus promotion
  and withholds the percentage while discarded evidence could be eligible.
  Independent static/history recognition remains separate.
- Loss protection is generation-scoped. Window changes may reactivate retained
  loss metadata. A loss-affected generation retires at 10 minutes of process-clock
  idle time, even if discarded evidence could remain eligible.
- Retain already accepted B; block further corrections and withdraw its current-
  consensus confidence at a subsequent applicable evaluation. No retroactive
  reversal or new post-acceptance gate.
- Preserve original input for all peer spot output, including local DX commands.
  Preserve exact representable comments; refuse unrepresentable publication.

## Sequential patches

### B0 - Original-content peer publication

Received PC11/PC61/PC26 transit preserves validated original fields, subject to
existing hop decrement and required legacy conversion. Locally generated peer
publication preserves the accepted source Spot after primary normalization,
before local output preparation, correction and frequency averaging. Literal DX
command-line whitespace, case and pre-parser precision are outside this boundary.

Create a compact, owned, immutable publication record with only source fields
needed by peer publication. Capture after toxicity completion, before output
preparation; toxicity leaves primary-normalized publication fields unchanged.
Carry the record through existing temporal, FT and stabilizer hold, retry,
overflow-release and shutdown owners. Never recapture corrected fields. No new
provenance map, queue, worker, shared Spot field or persisted provenance.
SnapshotForAsync is not an isolation copy.

Preserve exact permitted spaces/tabs/comments and numeric frequency without the
formatter's additional one-decimal rounding. Remove inferred-mode substitution,
trimming and sanitization. Refuse empty/unrepresentable comments, invalid exact
IP and unrepresentable destination variants. Do not invent/canonicalize IP or
add modern-to-legacy fallback. Preserve origin/hop, UTC minute wire encoding,
source exclusions, admission, dedupe, suppression, queue and timing gates.

Checks: actual local TCP B versus actual peer TCP original A and complete
original payload; ring/reopened archive B. Immediate, temporal, FT, stabilizer
hold/retry, overflow, suppression, blocked-toxicity reentry, variant refusals and
frame-size boundaries. Receiving native parser and forced mutation-before-relay
proof complement pipeline integration. Independent local/peer fixture oracles;
deterministic publication barriers for absence checks.

### B1.1 - Receiver-level historical freshness

Qualify receivers individually by existing age score; count cells only from
qualifying observations. One refreshed receiver must not refresh older support.
Preserve age tiers, inclusive stored-second boundaries, newest-observation
semantics, retention, cleanup, admission and persisted encoding. Exclude future
observations without deleting them.

Checks: old 54/55-day observations plus one fresh report yield one qualifying
receiver/cell at minimum score 5 while three remain retained. Every age boundary,
future timestamps, refresh ordering, shared cells and delivered consequences.
Compare persisted bytes before decoding; independently check reopen and delivery.
Preserve real private-copy/concurrent-reader checks including normalized inputs.

### B1.2 - Consumer-specific history requirements

One canonical store owner, explicit consumer query requirements. Remove stricter
consumer coupling; apply each resolver, stabilizer, floor and applicable bonus
consumer's requirements plus relevant common minima. Preserve recording,
cleanup/shutdown ownership. Supply existing static horizon to replay construction.

Checks: unequal requirements, initialization/query order independence, changing
stabilizer requirements without silently changing resolver/FT, delivered results
and runtime/replay wiring. Historical replay clock redesign is deferred.

### B1.3 - Independent family-member qualification

Check all relevant members against all gates before selecting a qualifying
member. A higher-scoring failure cannot hide another independent success.
Never pool scores, receivers or cells across members to qualify.

Checks: highest-scoring failure/other success, reversed order, exact precedence,
static misses and collectively sufficient but individually failing members.
This is historical qualification, not delivery suppression.

### B1.4 - History capacity validation

Reject enabled consumed effective requirements above the store receiver cap;
direct impossible queries return no support. Include applicable common minima,
correction/stabilizer/floor/bonus consumers. Disabled unused requirements do not
block startup. Do not apply this capacity to unrelated FT storage. No cap/schema
change.

Checks: equality success, above-cap actionable configuration/startup/replay
failure, disabled-unused controls and direct impossible lookup.

### B1.5 - Future static-membership exclusion

Future learned static membership cannot provide recognition or S floor until
the query clock reaches its stored timestamp. Retain it normally; distinguish
retained counts from eligible membership.

Checks: before/equality/after; lookup does not change persisted bytes; reopened
agreement and actual delivered S-floor behavior.

### B2.1 - Numerical and asset integrity

Reject nonfinite reliability values, negative model counts, nonfinite or
nonascending edges and overflowing totals. Preserve valid finite clamping and
model outputs. Explicit invalid assets fail applicable startup/replay rather
than unit-weight or model-disabled substitution. Preserve disabled behavior and
unrelated text parsing. Refuse direct nonfinite evidence weights before scoring
or fallback. Verify partial-startup resource cleanup.

Checks: loaders/config/direct inputs/startup/replay for every invalid class,
valid positives, overflow boundaries and independent arithmetic/model vectors.
Weights 900+950 for A versus 1000 for B produce A=64% even across repeated buckets.
This establishes numerical integrity, not calibration.

### B2.2 - SNR presence and domain handling

Carry report presence into evidence. Missing SNR differs from reported zero.
Missing, nonfinite and out-of-domain SNR adds no confusion contribution.
Preserve valid (lower, upper] bins and existing representative-report policy.
Check CSV representability before integer conversion; preserve valid rounding.

Checks: missing/reported zero, edges, nonfinite/out-of-domain values, conversion
limits, runtime/replay agreement and delivered valid-input behavior. No new
per-receiver SNR aggregation algorithm.

### B3.1 - Cap-loss authority and retirement

Record actual candidate/receiver-cap discards with constant-size metadata in the
current key generation; no discarded identity log or tombstone history.
While discarded observations could remain eligible, retain the entering identity,
block corrections/current-consensus promotion and withhold percentage. Static/
history support remains independent. Conservative timestamp bounds may protect
longer than exact discarded-row tracking.

Carry loss through rowless/compact publications, neighborhood selection,
temporal proposals and existing stabilizer evaluations. Inspect admitted
neighborhood evidence before replacement comparability filters. Publish processed
loss promptly without ordinary evaluation throttling or hysteresis advancement.
Shrink can deactivate; expansion can reactivate while the generation lives.

Use process elapsed idle time independently of observation/query timestamps.
At idle >=10 minutes a loss-affected generation loses authority. Owner admission
of a report to the key extends idle time; enqueue alone/rejected reports do not.
Arrival at/after retirement starts a new generation. Delayed physical sweeping
cannot preserve authority; owner processing/sweeping removes state/publication
references. Active loss prevents premature empty cleanup until hard retirement;
inactive loss permits legitimate cleanup. Continuous accepted traffic may keep a
generation live, bounded by existing key capacity and constant per-key metadata.
At runtime's 6000-key cap keep existing refusal and expose pressure; do not evict
quarantined keys silently. Retired proposals have no authority; new-generation
evidence must independently pass gates.

Checks: both eviction causes, empty populations, immediate publication, window
shrink/expansion, future discarded timestamps, exact idle deadline, accepted/
rejected activity, post-retirement arrival, stale proposals, recovery, cap refusal,
concurrent immutable readers and delivered output with deterministic clocks.

Queue-full, whole-key admission, FT loss and cross-restart loss protection are
deferred. No ingress barrier, already-emitted reversal or new post-acceptance gate.

### B3.2 - Diagnostic meaning and output assertions

Use existing bounded surfaces with fixed reasons and explicit outcomes/bases:

| Surface | Assertion |
| --- | --- |
| Normal local TCP | Local emitted identity/glyph |
| DIAG CONF TCP | That identity's percentage or --% |
| PC61/PC11 | Original fields; no local glyph/percentage/rejection reason |
| Ring | Local identity and existing confidence availability |
| Reopened archive | Persisted local identity/glyph, no invented provenance/percentage |
| Pipeline correction-event stream/log fallback | Corrected/rejected/retained outcome, loss reason, support basis |
| Resolver/pressure/gate summaries | Fixed loss/recovery/retirement/cap-pressure counters |
| Replay diagnostics | Named reasons/counters in existing artifacts |

Adjust event label/help to avoid implying retained/rejected events are corrections.
No per-call registry or normal spot/peer/archive fields. Preserve accepted-only
temporal commit/history accounting and retry deduplication. History recognition
is distinct from current support; percentages are support shares.

## Deferrals and validation

Bounded family delivery-suppression repair is explicitly deferred; its reach
includes several destinations. B1.3 does not repair it. Wider signal association,
geographic origin, replay clock redesign and model tuning/calibration are deferred.
No new independent scientific assessment occurred in this ledger revision.

Every patch establishes a behavioral red counterexample where applicable, runs
targeted checks, full ordinary suite, vet, Staticcheck and lint. Add full race for
shared-state/ownership/publication/lifecycle (required B0/B3.1), connected CustomSCP
producer-to-consumer ordinary/race as affected, protocol/parser fuzzing and bounded
cost checks. Preserve allocation contracts; no production capacity claim from
local benchmarks. Fresh diff/quality review, affected docs/support routes/maps,
exact command evidence and patch-to-code/test traceability precede local commit.
Next patch starts only after the checkpoint passes.

Use existing stable Windows executables with scripts/test-windows.ps1 and
scripts/test-customscp-regression.ps1 serially. Ordinary/race reuse paths. No
firewall preparation/rule change, new listening executable or user interaction.
If a required check cannot run or material new scope/policy is needed, stop before
the next patch and report the concrete blocker. No routine checkpoint approvals.
