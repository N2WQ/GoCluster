# Call correction follow-up scope ledger v2

Approved by the owner with exact `Approved v2` on 2026-10-11 (response
annotation). Inspected baseline: `96c0a84a19110fa248eddab182e622b174b73082`,
`fix_temporal_family_delivery`. Execution branch: `fix_call_correction_followup`
in `C:\src\gocluster`. This self-contained ledger supersedes the proposed v1;
it includes the maintenance-clock amendment and both final checker refinements.

## Objective and authority

Make correction, displayed support and learned history consistently represent
eligible accepted evidence. Execute S1, S2, S3 and S4 sequentially. Each receives
targeted checks, complete selected validation, fresh final review, documentation,
and a local checkpoint commit before the next starts. Failures remain visible;
do not skip a required check or proceed with a failed checkpoint.

Authority includes branch creation, these changes and their necessary tests,
docs, ADR/TSR records, generated maps, ignored validation artifacts and four
local checkpoint commits. No push, merge, deployment, live feeds, production
database mutation, firewall changes, workflow changes or threshold recalibration.
In-scope fixes proceed normally; material expansion requires revised exact
approval. Logging repairs require separate authority.

## Selected policies

- Learn eligible observations once after core acceptance and stabilizer
  resolution, regardless of secondary dedupe, family hiding, lane admission,
  queue admission/refusal or client receipt.
- Do not learn while pending/reheld, after suppression or after shutdown
  discards work. A transferred or forced shutdown release that reaches core
  acceptance can learn once. Preserve existing retirement ownership/order.
- Preserve every store's admission rules, especially CustomSCP's V-only
  admission; legacy history does not acquire a V-only rule.
- Positive weighted total plus known zero candidate weight yields available
  0%. A positive ratio floored to zero also yields available 0%.
- A genuine all-zero weighted population yields unavailable percentage and
  zero temporal decoder support contribution, including absent candidates.
- Deliberately unweighted evidence uses distinct-receiver coverage. Missing
  eligible call evidence and protected loss yield unavailable percentage.
- Missing reliability tables/entries retain the numerical unit-weight default;
  they do not declare an unweighted or unavailable population.
- Reject future legacy/FT observations before they can alter valid history.
- Reject enabled consumed impossible requirements at startup with a clear
  setting/effective requirement/store/capacity error. Never lower a request.

Percentage is a support share, not calibrated probability of correct identity.
Independent historical recognition retains its separate meaning. Existing C/B
overrides, S-floor precedence and positive-support family hiding remain intact.

## S1: stabilizer selected-loss propagation

Initial and retry evaluations currently use exact snapshots although shared
selection can identify neighborhood loss. Temporal terminal-abstain and
overflow-abstain also skip the ordinary correction wrapper.

Carry current selected loss into initial evaluation on all these paths. Use a
compact request-local scalar; do not save receiver rows or a sticky loss flag
in delayed envelopes. Reuse the initial correction selection, adding no second
neighborhood union. Retry uses its fresh eligible selection at the existing
decision clock. Overlay loss even with absent exact evidence, preserving other
exact-bucket state/low-P inputs and ordinary ambiguity policy.

Preserve reason-specific budgets/counters, equality/exhaustion, both timeout
actions, overflow fail-open, SELF/beacon/FT bypass, temporal proposal/accept/
handoff accounting, original peer tuples, queue bounds and scheduler joins.

Tests must exercise actual candidate/receiver-cap loss, healthy and absent
exact buckets, independent history S, initial holding, actual scheduler retry,
recovery/window expiry, reason budgets, exhaustion, timeout release/suppress,
overflow, no fanout/learning while held and both temporal abstention paths.
Healthy controls must detect accidental broadening of low-P/state policy.
Verify matching runtime/replay policy inputs without describing replay's proxy
as an actual scheduler. Check normal/DIAG TCP, ring, reopened raw archive and
original peer output where applicable. Record wiring learning as a TSR;
existing loss and stabilizer parity decisions govern this repair.

## S2: legacy/FT history admission, queries, capacity and maintenance

RecentBandStore currently lowers impossible Boolean minima to receiver capacity,
counts future timestamps and lets future admission overwrite/prune/evict valid
evidence. Preserve the effective nonpositive minimum of two; above-cap Boolean
requests return false while count queries retain partial-count semantics.

Use an explicit applicable admission clock: wall UTC for live recording,
evaluation time for replay/reverse-engineering. Zero observation timestamps
resolve to that clock. Reject observationTime > admissionNow before pruning,
overwriting, eviction, normalization-state mutation or retained counters.
Count the inclusive interval [queryNow-window, queryNow] at time.Time precision.
A backward query excludes later observations without deleting them solely for
being later; existing lower-pruning remains, with no historical reconstruction
claim. Preserve valid-admission lower cleanup/eviction behavior.

### Owners and startup checks

| Owner | Construction predicate | Receivers/key |
| --- | --- | ---: |
| Live general legacy | correction enabled, CustomSCP disabled, bonus or stabilizer enabled | 64 |
| Replay general legacy | same general selection | 64 |
| Reverse-engineering legacy | correction enabled, bonus or stabilizer enabled, including CustomSCP selected | 64 |
| Separate live FT legacy | bonus enabled, including correction globally disabled | 16 |

Use config-owned pure checks after normalization at actual startup ownership
boundaries, before relevant resolver/history owners or replay scratch deletion.
Keep generic YAML loader's existing CustomSCP check. New owner-specific checks
cover normalized YAML and direct constructors; do not impose an unused FT cap
on replay direct configurations or infer reveng's backend from CustomSCP alone.
Earlier startup services/model/adaptive initialization remain outside this fence.
Validate consumed common, stabilizer/edit-neighbor and applicable resolver
validation/Plus1/Bayes minima with existing enable predicates and raw defaults.
Disabled/unused settings do not reject startup. No new YAML keys, schema or
defaults; update affected setting explanations only.

### Maintenance clock

Legacy replay maintenance must not expire observations still eligible at the
evaluation clock solely because machine wall time is later. Select explicit-time
maintenance driven by replay's existing evaluation owner; a background clock
callback was considered but adds clock publication/synchronization and
asynchronous ownership without a required benefit here.

Expose only the concrete legacy store's needed explicit-time maintenance seam,
reusing its existing sweep/deletion coupling and maintenance interval. Replay
does not start wall-time legacy periodic cleanup. At most one scalar deadline
tracks cadence in evaluation time. Initialize from replay time, never a wall
fallback; sweep once when due without replaying a burst of missed ticks.
Cover ordinary progression, sampling/gaps and terminal temporal evaluation.
Keep live legacy and CustomSCP maintenance contracts intact. Add no worker,
timer, queue, side table, history version or FT cleanup goroutine.

Existing shard/key/receiver caps and active-call reference deletion remain.
Dedicated FT stays bounded through writes/reads/eviction. Retained statistics
remain cardinality snapshots, distinct from fresh eligible support counts.

### Falsifiable evidence

- Cap one/default two fails; exact cap succeeds; above cap fails. Raw and
  normalized counts agree and preserve partial evidence.
- Admission/query equality and +/-1ns, inclusive cutoff and cutoff +/-1ns.
- Future same/new receiver and new-key admission under full receiver/shard
  pressure preserves valid observations, lastSeen and reference counts.
- Backward exclusion followed by later visibility; bounded insert/expire/evict
  churn and concurrent recording/query/cleanup.
- Actual runtime CW/RTTY/FT history consequences and delivered glyph/identity
  through existing normal/DIAG/ring/archive/peer fixtures where fields exist.
- Before-wall replay fixture invokes actual replay maintenance dispatch,
  preserves in-window support, then advances beyond the window and expires it.
  Inspect untouched keys/active references before queries so query pruning
  cannot disguise missing maintenance. Cover cadence and large advances.
- After-wall admission fixtures catch accidental wall-clock comparisons:
  equality accepted; evaluationNow+1ns rejected.
- Zero timestamps in both clock directions: inspect exact normalized
  admissionNow, and prove eligibility at admissionNow+window and expiry at
  that boundary+1ns. Immediate support alone is not sufficient evidence.
- Startup errors name setting and capacity; owners remain absent and replay
  scratch untouched. Cover disabled/unused gates, FT with correction disabled,
  and reveng legacy >64 even with CustomSCP selected.
- Repair FT fixtures' synthesized future times with explicit clocks, adding no
  skew tolerance. Benchmark raw/normalized queries (16/64), reject/existing/
  full-cap/churn admission and maintenance due/not-due cost.

Add ADR for legacy/FT admission, capacity and replay maintenance ownership;
add TSR for the diagnostic lesson. Do not rewrite CustomSCP's separate history
timestamp/storage policy.

## S3: acceptance-boundary learning

Move existing general history, FT history and WhoSpotsMe recording to the
consistent accepted boundary across immediate, overflow, delayed and
non-stabilized paths. Preserve final accepted identity, all store-specific
source/mode/SNR/cell/beacon/test admission rules, fanout and metric ownership.
Do not add an already-learned registry. Pending/reheld/suppressed/discarded
paths return without learning; accepted releases record once before secondary
output eligibility. Family display history remains based on admitted lanes,
separate from learned call history.

Through actual loaded-config construction and scheduler paths, prove equal
eligible support under identical acceptance decisions; include all lanes
denied, one lane passing, family hidden and queue refusal. Pending/retries
record zero, release once, suppression zero. Accepted corrections learn the
accepted identity; rejected alternatives learn the retained identity.
Check future S effects and CustomSCP raw/disk/reopen/static readbacks.

Distinct-receiver stores overwrite timestamps and can hide duplicates. Use
WhoSpotsMe's per-occurrence count as an additional oracle. Each fixture
configuration first proves a successful exactly-one control with valid call,
band, reporter ADIF/continent, applicable exclusions and query clock through
the actual recording path; isolate the control from negative measurements.

Shutdown tests distinguish discarded pending work (zero), transferred
stabilizer envelopes and forced temporal/FT releases that reach acceptance
(once), and releases later suppressed/discarded (zero). Preserve current
retirement/ownership/order; add no blanket no-learning-after-stop guard.

Add acceptance-boundary ADR narrowly superseding conflicting learning clauses
of ADR-0148/ADR-0295 without changing family display history; record the TSR.
Benchmark bounded accepted recording overhead without capacity claims.

## S4: percentage basis and all-zero decoder support

Publish genuine weighted totals instead of inventing unit weights for zero.
Use compact scalar basis metadata, surviving owner/loss publication, Compact,
fresh projection, neighborhood synthesis and temporal copies. Recognize
existing positive weighted snapshots; explicitly identify deliberately raw
evidence, and fail closed on ambiguous zero/mixed metadata. Add no config switch,
persistence/schema change or state registry.

Require eligible call evidence before computing a percentage; a zero-support
ranked row cannot recover evidence from stale winner fallback. Preserve integer
flooring. Known weighted zero and rounded zero are available, all-zero/missing/
loss unavailable, explicit raw coverage uses receivers. Diagnostics must label
known zero share=0% basis=weighted. Runtime/replay/reveng consumers and temporal
classification agree; zero with positive total is low/uncertain, unavailable
coverage supplies no numerical confidence authority.

Genuine all-zero weighted events provide zero decoder support for present and
absent candidates, including subject present/absent. This intentionally changes
existing low-cardinality scores. Keep positive-total smoothing, thresholds,
stay/switch, transitions, priors and limits unchanged; test transitions/priors
separately. Fresh release guards must not manufacture current confidence from
unavailable coverage.

Family comparison/hiding still requires positive usable support. Zero display
does not authorize hiding. Accepted C/rejected B emitted-call share and glyph
precedence remain; S floor applies only at its existing unknown-glyph boundary.
Mixed zero A2/B1 minimum-report rejection yields A/P0, not A/V66, with configured
low-P stabilization and CustomSCP V-only refusal as consequences.

Golden evidence includes mixed 0/0 versus 1000 (0% available); absent call and
ranked raw-zero stale fallback unavailable; A1milli/total101000 (0% available);
all-zero total unavailable; explicit raw 2/3=66%; positive 900+950 versus 1000
(64%/35%); nonexclusive overlapping receiver union; 25%/51% threshold boundaries.
Exercise actual admission/publication, Compact/projection/neighborhood,
recovery/loss, decoder N=1/2/10 present/absent and subject fallbacks, immediate/
retry/temporal normal/DIAG/ring/diagnostic/archive/peer output where applicable.
No percentage peer/archive fields or new replay per-row artifact are added.
Benchmark metadata/helper/classifier/compaction cost; no calibration/accuracy
or production throughput/p99 claim. Add basis ADR refining ADR-0027/ADR-0293,
TSR, and user/operator explanations.

## Validation and checkpoint discipline

Before S1 edits, run the historical logging failures ordinary/race from the
canonical cluster executable. If failure recurs, retain evidence and stop;
read-only diagnosis is allowed, logging code/fixture repair requires separate
authority. Never exclude or silently waive logging failures. A later passing
run does not erase earlier failure evidence.

For each patch run targeted ordinary/race regressions first, verify exact test
selection is nonempty, then the complete selected lane:

```powershell
pwsh -NoProfile -File scripts/test-windows.ps1 -Timeout 10m
pwsh -NoProfile -File scripts/test-windows.ps1 -Race -Timeout 10m
go vet ./...
staticcheck ./...
golangci-lint run ./... --config=.golangci.yaml
pwsh -NoProfile -File scripts/test-customscp-regression.ps1
pwsh -NoProfile -File scripts/test-customscp-regression.ps1 -Race
```

Refresh package/tool inventory; include build-only packages. Existing baseline
has 70 packages (53 tests, 17 build-only). Ordinary/race reuse canonical
`.tmp/windows-tests/<full-import-path>/package.test.exe`, compile with test -c
and execute directly, count=1/paniconexit0, shared lock and no stale binary after
build failure. Listening tests use existing approved paths. Tagged qualification
builds execute at the same canonical paths under the same lock; include tagged
vet/lint for materially affected sources. No firewall changes.

Include applicable config, support-comment, support-agent docs, ADR/TSR, links,
allocation/cost and final review checks. Before each slice's Go mutation perform
its detailed test-strategy review and disposition material findings. Fresh
final Go quality/scope verification may be lead-owned and is not independent.
After source/docs, generate codemap as final write, check maps and whitespace,
then directly inspect the final diff. Rerun affected checks after review fixes;
rerun complete lane if broader evidence is invalidated. Commit only passed
checkpoints; no next patch with failed/missing required evidence.

Keep logs and checks.json under `.tmp/call-correction-followup/prerequisite/`
and S1/S2/S3/S4; tracked execution record maps every item to implementation,
tests/results and checkpoint commits. Historical review checks are supporting
evidence, not current complete checkpoint results.

## Review disposition and claim boundaries

Preapproval scope challenge dispositioned temporal skip-wrapper loss capture,
after-wall replay admission falsifiers, actual reveng backend and explicit
all-zero decoder scoring. V2 adds replay maintenance clock, untouched-key
expiry evidence, eligible WhoSpotsMe controls, accepted shutdown release
distinction and present/absent all-zero neutrality. Final checker refinements
add exact zero-time normalization and this self-contained consolidation.

An earlier independent numeric reviewer in a separate unsteered context derived
weighted versus raw coverage, defined zero versus undefined zero-total and
25%/51% boundaries. It received no preferred implementation/ledger or derived
expected values. It selected no smoothing/storage/glyph policy. Maintenance
amendment review was lead-owned, with no fresh independent scientific review.
Local verification does not establish field accuracy, production-load behavior,
firewall permission beyond user approval, or Linux parity.

Exact authorization recorded for implementation: Approved v2
