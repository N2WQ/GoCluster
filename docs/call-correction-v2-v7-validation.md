# Call Correction v2 v7 construction evidence

Authority: exact `Approved v7`, November 18, 2026, in the isolated
`call-correctionv2` checkout. This records construction and falsifiability;
production qualification remains unchanged.

## F0 detailed gate and implementation

A separate read-only test-strategy reviewer inspected the approved contract,
current evidence/selection code and the proposed request-local design before
implementation. Its detailed matrix found no material scope or normative gap.
The following checker refinements were accepted before construction. This was
an independent test-plan challenge, not RF calibration.

| Failure mechanism / stimulus | Observable distinguishing evidence | Checker and owner |
| --- | --- | --- |
| Favorable merged frequency hides other source rows; adjacent chains or frequency hulls fabricate association | Original source frequencies partition; chains stay joined; an empty 240-Hz hole does not attach the focal query | `TestCCV2F0RawFrequencyComponents`, correctionflow |
| Decimal/representable threshold error or closest-of-two selection | 150/250-Hz gaps, 100-Hz attachment and 60/120-second time windows tested below/at/above; adjacent binary64 values distinguish widening; two attachments fail | `TestCCV2F0AssociationBoundaries`, `TestCCV2F0RepresentableBoundaries`, correctionflow |
| Receiver aliases or transitive exclusion erase dissent | Only two distinct shared labels qualify; A–B and B–C evidence does not exclude C from A | `TestCCV2F0DistinctSharedWitnesses`, `TestCCV2F0ExclusionNotTransitive`, correctionflow |
| Unknown/conflicted/spanning-identity rows disappear | Unknown members stay; literal raw conflict survives favorable newer cross-source evidence; spanning aliases cannot nominate | `TestCCV2F0UnknownDissentAndRawConflict`, `TestCCV2F0SpanningIdentityUnresolved`, correctionflow |
| Sum of candidate votes masquerades as receiver union; unavailable weights regain raw confidence | Literal overlapping union is three receivers with max weights totaling 2600; weighted zero unavailable; deliberate raw distinct | `TestCCV2F0EvidenceOwnership`, spot; `TestCCV2F0GroupSupportBasis`, correctionflow |
| A merged/group count creates settled authority | Below-winner current settled support can nominate; uncertain/split/unmatched sources cannot; ordinary protected state and source loss block | `TestCCV2F0MatchingSourceAuthority`, `TestCCV2F0ProtectedAuthority`, correctionflow |
| Pending endpoints or saved lifecycle counts are misused | Applicable pair blocks; unrelated/missing endpoint does not; same-generation changed rows recompute group counts | `TestCCV2F0PendingApplicabilityAndFreshInvocation`, correctionflow |
| Bounds are applied after folding or inconvenient data is truncated | 80 raw slots/5120 incidences accepted, 81/5121 refused; 16 components accepted, 17 refused; more than eight nominees exposed before rank/gate beam | `TestCCV2F0RawBounds`, `TestCCV2F0ComponentOverflowAndBeam`, correctionflow; `TestCCV2F0StudyEvidenceBounds`, spot |
| Import changes captured aliases/identity/support/total/origin | Four complete frozen initial-selection DTOs import; independent corruptions are rejected | `TestCCV2F0ImportCapture`, correctionflow |
| Borrowed mutable data or map order changes outputs/public evidence | Mutated inputs/headers and concurrent filters cannot mutate source; reverse row order stable; ordinary population intact; unread/missing/present-generation-zero distinct | `TestCCV2F0EvidenceOwnership`, spot; `TestCCV2F0DeterminismAndIsolation`, correctionflow; source review of scalar output |
| Every real example merely falls back or frequency separation is treated as RF truth | DL3 assessed with current support; R9 time-only gap unpromoted; N2 conflict protected; same-frequency reference control unseparated | `TestCCV2F0FrozenWitnessControls`, correctionflow |

The pure core lives in `internal/correctionflow/frequency_proposals.go` and
`frequency_import.go`; the raw import/filter remains beside canonical joins in
`spot/resolver_evidence_filter.go`. `CorrectionCandidateDistance` delegates to
the existing primary gate's canonical distance, preserving portable aliases.
No live caller, resolver owner, queue, timer or saved receiver state was added.

The frozen fixture is 41,854 bytes, SHA-256
`48a0f58f0e86e4b9670721d9d8a52a93d131659dc522be9cdd1af74d95f7907a`.
The existing independently reviewed typed E2 reader verified all 621 November 7
trace frames, with trace SHA-256
`139bac19958dfcd38c7dc7316b15831e407bdd51d4b5c93bcaf4157dfb8e78a7`,
before complete initial DTOs for ordinals 174575, 432308, 480813 and 520537 were
extracted. No source rows were selected away within those DTOs.

DL3KWF has 14 current receivers and 13,445 weight units in the assessed group.
The retained one-receiver HA3NU dissent uses DK0KK, already one of those fourteen;
its presence does not create a fifteenth receiver. The group share is therefore
100%, although overlapping candidate shares are not a categorical probability.
The ordinary public population is untouched. R9AAB remains 4/15 receivers and
3,864/14,455 weight units, 26%, with no frequency exclusion. N2QLV remains
protected by ordinary split authority. These are gate inputs, not applied
corrections or RF-truth findings.

A separate source review found three defects during construction: a threshold
epsilon, borrowed witness-key string backing, and an incorrect negative-distance
sentinel. They were fixed and rechecked. Threshold comparisons now convert each
frequency to Hz before exact comparison, with no widened threshold. Native
fixtures also caught normalized-away `/P` aliases and a mistaken fifteenth-
receiver expectation; actual portable aliases and a direct frozen-row union
check corrected those oracles before passing checks.

Native ordinary `scripts/test-windows.ps1 -Packages @('./spot','./internal/correctionflow')`
passed 1,563 cases, zero skips, including 27 new F0 cases. Targeted checks ran
through the same runner. An initial `pwsh -File ... -Packages a,b` invocation
passed a single comma-containing path and failed before tests; the PowerShell
array invocation above is the executed evidence. Final race/source and later
unit evidence are recorded below as executed; the complete final lane remains F4.

The same complete two-package native race lane passed 1,563 cases, zero skips.
The reviewer rechecked all three source fixes and found them applied. The lead
followed raw-source admission, partition, pairwise exclusion, unresolved dissent,
canonical union, authority/pending/tie and scalar-output paths before proceeding
to F1. F0 establishes a reachable bounded proposal core, not consumer integration.

## F1 detailed gate, fit and ranker

The same separate reviewer challenged the concrete fixed-table design before F1
code. The accepted matrix covers independently derived posterior means and
single-path log weights; suffix-forward M/S/D/I ties; asymmetric direction and
source-gap stops; focal SNR interval edges/missing/zero; 49/50 character/gap
floors; whole-decision fallback including candidate nine; rank ties; exact JSON
dimensions before fixed-array decoding; malformed operative facts/counts;
complete-source/reference accounting; late publication failures; and bounded
immutable concurrent scoring. These were checker refinements, with no scope or
normative gap. The mathematical event contract had earlier separate non-steered
review; this construction review is not RF calibration.

`ordinal_counts.go` and `ordinal_model.go` implement private fixed numeric
tables. `study_ordinal_asset.go` reuses the strict wire preflight with exact
2-source/6-cell/7-edge/37-symbol/38-outcome dimensions. A regular-file handle is
size/read bounded to 4 MiB, its actual bytes are hashed, and errors including
close are propagated. No runtime model installation, cache or worker is added.

The independent rational fixture has A opportunities 76, outcomes A38/B37/del1,
one inserted A and 97 stops. It verifies emission means 1445/2926, 1407/2926,
39/2926; insertion A19/37; and continuation 1/50. Literal paths include `AA→A`
MD, `A→AA` MI, `AB→BA` SS and `ABAB→BABA` DMMMI. Exact matches retain match and
stop terms. Unexposed source characters force geometry fallback for all nominees
before beam selection; an unseen observed/inserted symbol keeps its prior.

The full fit used complete hash-bound November 7/14 terminal strong-agree R1/R2
pairs, including unchanged calls, not the selected 1,107 diagnostics. All-mode
counts reconcile: 1,340,516 focal receipts; 138,330 accepted pairs; 137,115 exact;
M/S/D/I 672,791/2,274/582/340. The CW asset contains 124,035 pairs, 122,871 exact;
M/S/D/I 607,763/2,155/552/321; 610,470 source characters, 610,239 observed
characters, 734,505 stops. There are 806 event-census and 1,344 character-census
comparisons. Missing/outside-SNR and unsupported/overlength accepted counts are
explicitly zero. Only A reaches the floor in the lowest CW bin; slash remains
sparse in (0,6]. No posterior uncertainty or RF probability follows from this.

Final asset: `.tmp/call-correctionv2/v7-ordinal-fit-v3-model.json`, 21,373 bytes,
SHA-256 `968caf263f3bf44c13509ded8b5e48faf0bd39b1d7cc42dcd9d1e6c0a45e5e1a`.
The identical native fixture is `cmd/rbn_replay/testdata/ordinal_fitted_v7.json`.
Final receipt SHA-256 `225ddb06f3b1171f8d90c31590538661683ced86edffe3279fd6658959f127c7`;
COMPLETE SHA-256 `575f0fdded0d2d6105c052ad2e07034d7460aa38eadde06fdba7f7cd8dcfdc35`.
`scripts/fit-call-correction-ordinal.py --selfcheck` and the full `--out-prefix
.tmp/call-correctionv2/v7-ordinal-fit-v3` invocation passed; the lead independently
reread final bindings. Earlier v1/v2 fit bytes were identical but their failure
checks were refined; v3 is the current authority.

Source review caught impossible exact-pair metadata and a late-failure COMPLETE
publication gap. Go/Python now validate nonexact/edit feasibility. Publication
uses a verified pending file and rename last, revoking a final marker if its
last verification fails. Seven injected write/flush/fsync/close/pending-readback/
rename/final-readback faults left no final COMPLETE. Nine refusal checks also
passed. Eighteen owned fit evidence files total 348,002 bytes, including retained
predecessor receipts and diagnostic pending files. A corrected hand-summed CW
total was replaced by direct frozen character-census reconciliation. No source
rows or difficult strata were dropped to obtain a result.

`TestCCV2F1*` covers the numeric/availability/model checks in correctionflow and
wire/file/fitted-asset checks in replay; the fuzz corpus includes a full valid
asset and a near-valid nested dimension change. The separate reviewer rechecked
all material fixes and closed its source findings. Complete native ordinary
correctionflow/replay checks passed 1,230 cases, zero skips. `go vet` passed both
packages. Race/fuzz evidence follows when completed; this is not a correction-
gain verdict.

The complete native race lane also passed 1,230 cases, zero skips. The stable
replay test image was compiled with `go test -c -fuzz=FuzzCCV2F1AssetWire`, then
run with a literal PowerShell argument array for a bounded 15-second, two-worker
fuzz pass: five seeds, 12 executions, PASS. This limited fuzz throughput is not
exhaustive decoder assurance; exact malformed-shape cases supply the principal
parser evidence. An initial bare dotted-flag PowerShell invocation failed at
argument parsing before fuzzing; the literal-array invocation is the run above.
All F1 material review findings are closed; dependent F2 construction has begun.

## F2 detailed gate before integration

The lead applied the post-approval test-strategy method to the actual replay
normal/failure/release paths. This is a lead-owned gate, not an independent
review. A separate read-only source walk mapped all v6 owner/writer/copy/cleanup
and legacy-reader hooks. No new product interpretation or owner state is needed.
Use an artifact-only composition of the existing diagnostic file/copy/scratch
helpers in a separate method context; do not install a fake selected capture as
`r.diagnostics`. Existing diagnostic DTOs/selection semantics remain unchanged.

| Failure mechanism / stimulus | Required observable result | Planned checker/owner |
| --- | --- | --- |
| Ordinary success conflicts with stronger extra proposal; ordinary replacement fails license/CTY | Ordinary mutation/suppression and chosen-replacement rejection are terminal; no second ordinary execution | `TestCCV2F2OrdinaryFirst`, replay |
| Gate rejection versus first allowed proposal's license rejection | Failed gate may advance, at most eight; first allowed candidate alone validates; no post-validation runner-up | `TestCCV2F2GateAndLicenseBoundary`, replay |
| Incoming subject itself is unlicensed | Valid supported rescue occurs before later final subject license check; emitted replacement is still validated | `TestCCV2F2SubjectLicenseRescue`, replay |
| Group share is high but ordinary population differs | Gate receives group counts; ordinary public percent, family/stabilizer and temporal evidence remain ordinary; accepted mutation C never V-only learning | `TestCCV2F2DualEvidence`, replay |
| Enqueued focal report is not yet published | No forced Step/drain; subject query creates no vote and missing current nominee cannot repair | `TestCCV2F2CurrentPublishedMatch`, replay |
| Temporal abstain/bypass/overflow or Apply guard failure | Existing routing preserved, no extra alternate proposal; Apply may assess only the already-permitted identity | `TestCCV2F2TemporalAuthority`, correctionflow/replay |
| Excluded source appears/disappears/reuses key; unchanged generation has different rows/SNR/frequency/clock eligibility | Saved scalar read/presence/lifecycle vector protects identity; current eligible sources always recompute association/counts and observed display | `TestCCV2F2ReleaseFreshness`, replay |
| Candidate nine unavailable to Bayes, expensive history queries or beam overflow | Rank all eligible bounded candidates before eight trials; whole-decision geometry fallback; at most 64 added existing alias queries | `TestCCV2F2BeamAndHistory`, replay |
| CLI unknown/missing/null/duplicate settings, explicit false, invalid model or cell map | Strict requested method plan/ref/model admitted before archive content; complete fixed 16-grid only; disabled mode is explicit and still experimental | `TestCCV2F2MethodPlan`, replay |
| Writer/copy/rename/close/cleanup/integrity/budget failure | Existing cumulative 4 GiB owner and finite inventory cover every evidence/asset write; scratch separately measured; error prevents COMPLETE | `TestCCV2F2ArtifactLifecycle`, replay |
| Legacy reader sees experimental marker false/null/malformed | Marker presence refused before value/typed decode at report read boundaries | `TestCCV2F2LegacyRefusal`, replay |
| Disabled method changes downstream state or predictions | Complete frozen 2,364,930 decision rows byte-identical before interpreting new grid outcomes; unchanged clocks/history/reference rules | F3 frozen full-stream parity, replay |

These checks cover meaningful distinct boundaries rather than copying the core
algorithm. Source completeness must verify actual normal, validation failure,
release and finalization paths before F3. Frozen examples alone cannot prove
net gains or safe RF separation; the complete paired grid/confirmation remains
required. No live wiring, YAML/default or new history/queue/worker is authorized.

### F2 executed evidence and source completion

The separate source reviewer traced normal correction, gate rejection, final
replacement validation, temporal confirmation/final licensing/learning and
artifact failure paths. It found no material integration finding. The lead
verified its material conclusions against current source. This is source review,
not independent RF validation or full-stream parity evidence.

The native targeted F2 lane passed 69 cases, zero skips. Complete ordinary and
race correctionflow/replay lanes each passed 1,299 cases with zero skips. The
subsequently added actual-study-run fixture passed four ordinary and four race
cases: ordinary, explicit disabled and enabled runs all completed through actual
asset snapshot/parser/observer/finalization paths. The disabled run's complete
decision bytes match ordinary; both experimental runs carry explicit markers,
valid owned evidence reports and observed scratch/backend state. Full four-date
parity remains F3's prerequisite. Vet of both packages and normal Git diff checks
passed. A diff check with `core.autocrlf=false` was invalid for existing CRLF
files and produced line-ending false positives; the normal check was rerun.

Actual publication fixtures distinguish ordinary success/replacement rejection
from extra rescue, failed gates from terminal CTY/license validation, and final
subject licensing from a replacement's licensing. Ordinary selected support
gives 66% while the extra gate receives 100%, with the ordinary selection and
existing replay Spot percentage untouched. Replay still does not publish live
wire percentages. Actual C output does not learn in Custom SCP; identical
metadata with V does. The actual nine-candidate consumer uses a focal 6-dB
measurement while receiver rows contain 18 dB: the ninth sparse slash candidate
forces whole-decision geometry before eight trials. Eight gate calls retain the
existing eight-alias per-gate bound, at most 64 added queries.

Held-release checks restrict an extra proposal to the already permitted decoder
identity. A failed existing release guard, abstain, bypass and overflow add no
rescue. Actual decoder hold inputs and emitted decisions match with the method
disabled/enabled. Same-generation frequency, receiver SNR and clock-expiry
changes are reread; missing-source creation and actual expired-key recreation
invalidate saved lifecycle vectors. Queued observations stay unpublished until
the existing owner drains them.

Construction caught test-oracle defects rather than changing rules to fit them:
the resolver recomputes bucket keys from frequency, so 14020.3 kHz did not share
the 500-Hz bucket centered at 14020.0; fixtures use a genuine 200-Hz same-bucket
separation. A second same-clock sweep had not published a newly added candidate;
the fixture now publishes the complete population once. Slash pairs do not enter
truncation history rules, so that query-bound probe uses actual bare truncation.
Process-idle retirement applies to loss-affected keys; key-reuse testing now
proves physical expiry/removal before recreation. A tiny run initially used a
placeholder metadata path and was corrected to pass its actual preloaded raw
day, matching the existing lifecycle fixtures. All affected checks were rerun.

Frozen fixture files now explicitly use LF in Git attributes so Windows checkout
cannot change their raw-byte SHA bindings. The pure study evidence constructor
also normalizes copied instants to UTC without retaining caller location tables;
the affected 27-case F0 lane passed again.

## F3 detailed gate before complete-grid execution

The lead applied the post-approval test-strategy method before constructing the
common evaluator; a separate read-only reviewer then challenged its actual
source and fixtures. Complete captures, immutable raw-ID pairing and unchanged
scorer/reference procedures are required. Old selected diagnostic cases and
old paired files without reference covariates cannot substitute for predictions.
This is source/test review, not an independent RF oracle.

| Failure mechanism / stimulus | Required observable result | Checker |
| --- | --- | --- |
| Disabled stream compared with itself or an arbitrary caller-selected original | Bind all four original E1 byte hashes and row counts in the production reader; verify disabled capture COMPLETE/manifest/source/inventory | F3 parity authority and fixture refusal cases |
| Missing, duplicate, extra or mismatched incoming IDs/covariates | Reject before producing a complete comparison; pair every focal input, including unknown/outside/suppressed outcomes | Existing complete-stream/archive binding readers plus F3 fixtures |
| Gate allowance counted as an actual correction, or only common outputs retained | Count actual applied delivered receipts; retain complete status pairs, added/lost/common/retargeted/retimed and overlaps | F3 literal accounting cases |
| Rounded stability hides a decline, or inherited reference floors substitute for the objective | Use exact fractions, undefined zero denominators, separate objective/reference verdicts; both required for confirmation | F3 fraction and combined-verdict cases |
| Retiming changes reference assessability for the same target | Preserve inherited new-target rename predicate; separately report newly assessable same-target disagreements | F3 retiming/reference boundary cases |
| Geometry/Bayes differ in immutable input facts or ordinary configuration | Reject mismatches; evaluate the otherwise identical pair and explicit roles without relabeling legacy arms | F3 binding and attribution cases |
| Artifact quota, write, flush, sync, close or integrity failure | Run-owned cumulative 4 GiB bound; no truncation; failed run has no final COMPLETE | Existing owner plus F3 actual writer/failure cases |
| Freeze uses dirty or unrelated source, a modified executable, or an incomplete original | Refuse preparation; four controls precede the fixed sixteen-by-four active runs; FREEZE published last | Preparation self-check and actual post-commit verification |

The initial source review found that equal caller-supplied parity hashes did not
prove identity with E1. Production now requires the independently verified four
original hashes and row counts; only private synthetic fixtures have a separate
authority seam. The reviewer also identified a report conflating the primary
objective with the strong-reference rename gate. The fields are separate, with
eligibility their conjunction. Retimed same-target disagreements remain visible
without changing v3's inherited new-rename policy. The lead verified these
dispositions against the controlling ledgers and source.

The F3 targeted lane passed 31 cases, zero skips. The complete ordinary
correctionflow/replay lane passed 1,334 cases, zero skips; vet passed both
packages. The preparation script's lead-run self-check passed the literal
sixteen-candidate/four-date/68-command grid, stream/publication positive controls
and nineteen refusal cases. Actual preparation and prediction execution follow
the clean-source freeze. No candidate outcome has yet been used for selection.
The complete race lane also passed 1,334 cases, zero skips. The owner then
explicitly resolved the tertiary tie-break: count each affected incoming ID
once. Common metrics retain category counts, overlaps and that union; a separate
bounded selector will compare the complete sixteen-candidate results under the
agreed ordering. No candidate outcomes informed this clarification.

Full-repository vet and Staticcheck passed. Full lint initially reported twenty
two mechanical issues: unchecked latching, large range-value copies and three
equivalent control/expression simplifications. These were fixed without changing
thresholds or ordering. Separate source review checked that the new pointers
neither mutate caller-owned evidence nor escape, and that model tables remain
owned copies. Full lint then passed with zero issues.

The new method/evaluation wire fuzz target passed a native 15-second two-worker
run: four seeds, eight executions. Throughput was limited; literal malformed
wire cases remain the main parser evidence. The full ordinary lane's earlier
packages passed, but the existing cluster integration package reached the
selected three-minute whole-package timeout while starting a numeric-delivery
subcase. No assertion failure preceded that timeout. That package and the
remaining packages are rerunning under the runner's normal ten-minute limit;
the interrupted whole lane is not reported as passed.

### F3 source-completion gate for the frozen experiment

The separate selector reviewer found one missing consistency guard: per-date
strong-disagreeing rename counts could be hidden by pooled zeros. Both R1/R2
selection-operative counters must now pool exactly. The lead-run selector
selfcheck passed its native-shaped positive case and both hidden-harm refusals,
five literal ordering cases, two exact declines and seven publication failures.
The reviewer rechecked the fix and closed its material finding. The additional
wire fuzz assertions directly check admitted collection sizes; deterministic
refusal cases remain the operative-field oracle.

The lead's source-completion review traced the actual CLI, bounded loading,
ordinary-first gates, terminal validation, temporal freshness, actual C output,
learning, artifact failure/cleanup and complete-stream evaluation paths. These
paths are implemented rather than represented only by passing helper tests.
F3 may freeze and execute the declared grid; complete final lane/experiment
results and production qualification are still outstanding. A read-only actual
preparation check verified all 35 frozen template files and all four authoritative
original streams/counts. Native preparation will repeat this under clean HEAD.
The crawler-entry check passed 24 support-critical files after adding the touched
pending-owner header. Local target existence checks passed 341 Markdown file
references using the generator's repository-root convention for code-map links;
this does not claim external URL or rendered-anchor validation. Both frozen
staged fixture blobs retain their declared raw-byte hashes.
