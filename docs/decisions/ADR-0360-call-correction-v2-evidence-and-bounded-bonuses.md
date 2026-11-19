# ADR-0360: Call-Correction V2 Evidence And Bounded Bonuses

- Status: Accepted
- Date: 2026-11-17
- Decision Origin: Design and follow-up implementation; approved v3 scope

## Context

Candidate-wide representative SNR allowed one receiver's report to stand for
other receivers. Raw slash spellings fragmented a semantic candidate's support
and state. Repeated history reads could disagree within one gate. The bounded
Bayes advantage rail also inherited report-short eligibility, excluding raw ties
that already met the report floor.

The owner retained the existing correction method, RF-association assumptions,
denominators and ordinary guards. The objective includes more reference-supported
one/two-character corrections, measured alongside wrong renames. Implemented
reachability is distinct from empirical qualification or RF-truth evidence.

## Decision

### Receiver evidence and fresh authority

The resolver remains the sole mutation owner. Each receiver/candidate observation
owns its timestamped SNR presence/value; existing receiver references own
reliability. A newer timestamp
supersedes older metadata. Equal-time numeric SNR or presence disagreement joins
to unknown, absorbing older/equal-time receipts until a strictly newer report
replaces it. A newer missing report stays missing; there is no newest-present
fallback. Preserve present zero and `(lower, upper]` model bins.

Only the eligible top cohort tied in raw and weighted support supplies both
confusion hypotheses and observations. Normalize both sides to canonical voting
identity; self-matches contribute zero. Missing/conflicted/out-of-domain SNR and
zero receiver weight omit optional confusion contributions, while support and
denominators survive. Reuse pair alignment and integer SNR-bin reductions within
bounded evaluation scratch, without a lifetime pair cache. These scores remain
ranking potentials, not observation probabilities.

Harmonic suppression requires present SNR on both sides; missing reports do not
seed fundamentals. Preserve its cross-receiver/mode comparisons. Its existing
recency retention has no hard total entry cap; the presence repair adds no
registry or extra admissions, and resolver caps do not bound harmonic state.

Publish immutable raw provenance and derived canonical receiver unions. A fresh
projection filters the published rows at its inclusive release clock, excludes
future rows and does not resurrect superseded alias rows. Recompute optional
ordering from surviving receiver SNR. The projection may retain a still-leading
previously settled identity or revoke authority; it never settles a new winner
or advances hysteresis. All-fresh reads reuse their publication; compact temporal
state discards receiver generations.

### Canonical identity and display

Derive canonical populations from the existing raw owner entries, without
replacing owner admission or retaining a separate variant registry. First-party
owners keep 16 raw spellings per key and 64 receiver rows per spelling. A
canonical union can therefore exceed 64 receivers, up to 1,024 incidences per
key and 5,120 across five admitted neighborhood keys. These are component
evidence bounds, not a whole-process memory ceiling.

Distinct receivers vote once per semantic candidate and once in the overall
denominator, retaining overlapping candidate support and dissent. Display an
eligible observed spelling by support, recency, then lexical order; never
fabricate an observed canonical spelling. Stable, pending, temporal and
accounting identities are semantic. Alias/display changes alone are not
corrections or transitions. One alias's removal preserves surviving identity
support; final-alias removal revokes its state. Fresh mutation resolves the
identity to the current observed display before CTY and licensing.

The independent slash threshold retains its fallback of two. Precedence affects
replacement eligibility, not bare-call dissent in the denominator or ordinary
gates. Preserve nomination by every candidate member of an admitted settled
population. Existing ambiguity/frequency witness scope and literal successful
delivery authority for family display remain.

### Decision-local history and bounded bonuses

One gate captures first-read historical counts for at most eight normalized
alias lookup keys: raw, vote, base and display for two identities. Reuse counts,
including zero, across that gate's qualifications. Keep existing alias/backend
dispatch and consumer requirements. Different keys can observe different store
publications; this is not a global atomic snapshot. Each later immediate,
temporal or stabilizer retry captures afresh. No retained history cache is added.

Retain the capped Bayesian-style weighted-support/history heuristic. The report
rail adds at most one missing report and does not stack another report after
ordinary recent +1 fills the floor. The advantage rail independently considers
raw-support ties, including full-report populations. Both retain usable stronger
winner history, the effective receiver minimum, candidate validation when
configured and contested-neighbor disallow. Advantage additionally retains its
weighted-delta, extra-confidence and distance-2 subject-validation checks.

One config-owned structural rule selects cells with the optional complete
`bayes_bonus.enabled_distances_by_mode` map. Omitted/nil preserves existing
CW/RTTY/USB/LSB distance-1/2 eligibility within `max_edit_distance`; `{}`, omitted
modes and empty mode lists select no corresponding cells. Master false disables
all cells. Normalize mode case/space; reject effective YAML null, unsupported
modes/distances and normalized duplicates. Direct-Go nil mode slices mean empty.
Preserve nil versus empty through mapping and typed YAML serialization.

A later explicit mask replaces the entire earlier mask; omission preserves the
earlier definition. Other maps retain their existing deep merge, and validation
remains final-effective. Live, replay, evaluator, diagnostics and both history
capacity validators share eligibility. Capacity follows selected structural
distances, including zero-weight or score-unreachable enabled paths. Preserve
recent +1 at distance zero, truncation, stabilizer and other consumers. Existing
scalar defaults remain bootable; study search bounds do not become blanket
startup scalar restrictions.

Ordinary confidence, advantage, distance/family, ambiguity/frequency, CTY/license
and accepted-learning contracts remain. Ingestion stays asynchronous and
nonblocking. Completed retained report joins at a defined clock/state do not
promise current-receipt inclusion or deterministic already emitted live outputs.
No persistence schema, new settlement authority or generative RF model is added.

## Alternatives considered

- Replace raw owner identities or add a variant/SNR registry: unnecessary new
  admission, provenance and lifecycle responsibilities; derive bounded views
  from the existing owner instead.
- Add a global atomic history view or lifetime memoization: stronger semantics
  and retention than one gate requires; use eight decision-local slots.
- Replace the bounded heuristic with a full generative model or lower ordinary
  guards: not the selected method. Evaluate the retained rails and exact cells
  without claiming calibrated probabilities.

## Consequences

Receiver accounting, metadata joins, semantic state and historical qualifications
have explicit bounded ownership. Optional scoring cannot borrow another
receiver's SNR or preserve expired SNR as fresh ranking evidence. Separate
report/advantage rails can reach additional cases while keeping their guards.
These are runtime contracts, not measured field-accuracy or latency claims.

Legacy score-unreachable thresholds remain compatible; arbitrary custom settings
are unqualified. Activation requires evidence for the exact complete cell map
and scalar vector, including incremental supported corrections and wrong renames.
Constructed tests seed real receiver cells and V-only history. Current archive
CSV replay supplies no DE grid; H3 tables alone do not provide receiver location.
No location proxy, empirical activation or study qualification is selected here.

The master off switch or explicit empty mask disables Bayes cells on the
supporting executable. Older executables can ignore the new mask key. Evidence
repairs have their own implementation boundaries; disabling Bayes does not undo
canonical accounting or receiver SNR joins.

## Follow-up: approved v4 CTY input scenario

The no-location-proxy clause above preserves the original v3 decision boundary.
After separate scientific review and scope/test challenge, the owner selected
the additive [v4 study-input amendment](../call-correction-v2-scope-ledger-v4.md)
with exact `Approved v4` on 2026-11-17.

Use frozen CTY representative receiver locations only in the declared
`cty_representative_v1` study scenario, identically across shipped, repaired-off
and repaired-on arms. Schema-2 plans/reports bind CTY/H3 roles and hashes;
per-row derived provenance and complete focal-A known/unknown coverage are
paired and reconciled. Unknown rows remain in the population. Missing/corrupt
assets are setup failures before archive parsing, not unknown measurements.
Ordinary replay/server behavior, raw references and history minima remain.

The owner accepted this input uncertainty separately from RBN reference-label
uncertainty. CTY proxies can misstate geographic diversity; common imputation
does not cancel nonlinear eligibility errors. Conditional CSV-to-history tests
do not establish historical station sites, independence, live enrichment timing,
RF truth or empirical activation. Exact-combination qualification remains
required. [TSR-0139](../troubleshooting/TSR-0139-study-geography-and-replay-clock-fixtures.md)
preserves the readiness finding and the separate test-only sample-clock failure.

## Qualification and first-party activation

The frozen complete CW/2 candidate failed untouched November 15/16 confirmation.
A separate empirical reviewer independently recounted all six streams before
opening generated summaries, then passed all 32 reconciliation checks. There
were 79 additional normalized episodes but only two jointly strong, coverage
9/79 and 22/79, and new contradiction flags 1/3. Positive supported nets +11/+23
do not waive zero-contradiction, sample/date or coverage floors. These are
RBN-reference outcomes under the accepted A-only/CTY scenario, not RF truth.

First-party configuration therefore explicitly selects master false and an
empty complete mask. Ordinary gates, confusion ordering and recent +1 remain
active. The repaired heuristic stays implemented; no active combination is
qualified. Compatibility scalar defaults remain loadable. Future activation
requires fresh untouched confirmation for the exact complete settings, with no
same-holdout masking or threshold adjustment. Operators must apply both off
fields to their active/private configuration directory.

One matched-settings capacity capture passed after a bounded derivation repair,
with all actual selected-candidate tuning streams unchanged. The subsequent
actual Bayes-off release capture fails the frozen typical CPU gate despite
complete loss-free delivery. The cause of the measurement discrepancy is
unresolved; the earlier matched pass does not qualify the final release.
Canonical read merging
reuses sorted immutable populations; source nomination is separate from receiver
counts. Small total-population reductions use head scans; larger cohorts use a
local cursor heap with equal-key batching. Source rows and all source frequency
witnesses remain immutable. No retained registry/cache or extra owner is added.
[TSR-0138](../troubleshooting/TSR-0138-neighborhood-read-amplification-and-recovery-coverage.md)
records measured failure diagnosis, adversarial shape checks and the separate
recovery-coverage refinement. The
[qualification record](../call-correction-v2-qualification.md) retains actual
cost increases, input limits and final-state evidence.

## Links

- Supersedes only candidate-representative SNR and equal-time receipt-selection
  clauses of [ADR-0291](ADR-0291-correction-snr-presence-and-domain.md); its
  presence/domain and CSV conversion decisions remain.
- Implements/refines [canonical grouping](ADR-0004-call-correction-slash-precedence.md),
  [family policy](ADR-0014-call-correction-family-policy.md),
  [receiver ownership](ADR-0280-resolver-receiver-evidence-ownership.md) and
  [state lifetime](ADR-0120-resolver-correction-state-hardening.md).
- Extends the compatible bounded-rail contract of
  [ADR-0044](ADR-0044-resolver-bayes-capped-gate-bonus.md).
- Authority and execution: [approved v3 scope](../call-correction-v2-scope-ledger-v3.md),
  [execution record](../call-correction-v2-execution.md).
- Current contracts: [spot confidence](../../spot/README.md),
  [mask configuration](../../data/config/README.md#call-correction-bayes-cells),
  [replay evidence limits](../rbn_replay.md#bayes-evaluation-and-evidence-limits).
- Related constructed regressions:
  [SNR joins](../../spot/call_correction_v2_snr_test.go),
  [canonical evidence](../../spot/call_correction_v2_canonical_test.go),
  [history capture](../../spot/correction_history_test.go),
  [Bayes rails](../../spot/correction_bayes_rails_test.go),
  [config and capacity](../../config/call_correction_bayes_test.go),
  [actual replay](../../cmd/rbn_replay/correction_bayes_reachability_test.go).
