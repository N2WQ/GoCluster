# Call Correction v2 Scope Ledger v5

Status: Approved by the owner with exact `Approved v5`, 2026-11-18.
Branch: `call-correctionv2`; starting checkpoint `c1414d54`.

This records the approved conversation ledger and its review dispositions.
It supplements [v3](call-correction-v2-scope-ledger-v3.md) and
[v4](call-correction-v2-scope-ledger-v4.md). It authorizes evidence collection,
analysis and a resulting design, not a production correction rewrite, model
activation, deployment, merge or push. Original activation requirements and
failed qualification verdicts are not waived.

## Objective and resolved behavior

Increase correction count with improved or unchanged accuracy, estimated using
the existing replay follow-on stability method. The owner selected:

- A replacement must have at least one current matching report before active
  correction. History-only proposals may be investigated offline.
- Separately supported calls may compete despite an unrelated bucket winner
  when evidence distinguishes their signals. Spelling similarity alone cannot
  justify removing dissent.
- CPU, heap and allocation optimization is deferred. Bounded resources and
  truthful qualification remain requirements.

The requested estimator uses the checked-in replay settings: 15 minutes, three
raw follow-on rows and 500 Hz. Preserve its existing inclusive endpoints,
same-second evidence, literal normalization, row-count interpretation and
day-end behavior. More corrections plus a nondecreasing exact stability
fraction is a descriptive replay result, not demonstrated RF accuracy. Keep
full reference/unknown and date/mode diagnostics alongside it. Compare integer
counts rather than rounded display percentages; a zero denominator is undefined.

## Approved units

| Unit | Executable scope | Required result |
| --- | --- | --- |
| E0 | Reproducible audit of existing shipped/OFF/ON predictions, preserving current stability mechanics; per-applied receipt and full immutable-ID pairing. | Reconciled old 60/2/1000 counts before new 15/3/500 scoring; common, added, lost, retargeted and retimed outcomes, with exact counts and provenance. No new predictions. |
| E1 | Bounded replay diagnostics and small read-only shared selection/temporal diagnostic interfaces. | Capture actual consumed evidence, executed branches, candidate/window membership and provenance. Missing observations remain unknown; unexecuted branches are not evaluated for explanation. |
| E2 | Exposed-only association and Bayesian-data feasibility analysis. | Diagnose 75 identified nearby misses plus frozen controls; compare retained observations with causal raw history; account for matches/error opportunities, canonical aliases, modes/SNR and unknown labels. Findings may be insufficient or inconclusive. |
| E3 | Disposition findings and prepare a subsequent method scope when supported. | A concrete implementation design, or precise missing-information evidence and a bounded alternative. Obtain separate mathematical review and independent reference cases if a model contract can be justified. |

Execute narrow units sequentially without another permission request unless
evidence requires work outside this boundary. Dependent work stops on a failed
feasibility premise. No unit authorizes a trained production posterior asset,
history-only active correction, invented receiver votes, loss-veto bypass or
relaxation of ordinary guards/history/licensing requirements.

## Architecture and preserved contracts

The preferred future ownership direction is a request-specific evaluator over
the existing resolver's bounded publications. This is not an approved concrete
signal-association algorithm. If retained observations cannot support the
necessary distinction, report that result before proposing tracks or new
retention. No new owner, queue, timer, ingestion barrier, persistence schema or
temporal objective is authorized here.

E1 may add diagnostic-only interfaces in `internal/correctionflow` and their
replay callers. Existing runtime entry points remain diagnostics-disabled.
Preserve selection order, clock reads, settlement, gates and ordinary outputs;
disabled diagnostics must add no allocations relative to the frozen baseline.
Capture the actual consumed lookup results once. Do not substitute a second
latest-publication lookup, step/drain the resolver, or change clocks to make a
trace convenient.

Diagnostic collections are bounded owned copies, including nested alias lists.
No resolver snapshot/view pointer, borrowed backing array, receiver generation,
pending-state reference or component-lifetime observer escapes through them.
No external callback, formatting or I/O occurs while holding the temporal
mutex. Diagnostic failure is separate from a normal correction/fallback result;
the study must fail rather than substitute a different decision.

Preserve five-key generation witnesses as a vector. Different keys need not
have equal generations. Bind temporal event/request IDs to raw archive IDs
through observed mappings; call/time equality is not provenance. Preserve
baseline public percentages, glyphs, family/stabilizer behavior and V-only
learning admission. A future proposal population must not silently redefine
those consumers.

## Evidence and bounds

Use already-exposed November 7, 14, 15 and 16 inputs for analysis. Reserve
November 17 and later uninspected contents until an actual candidate and
evaluation contract are frozen. Hypothetical analysis outputs are not applied
correction receipts or a qualified runtime method.

- Freeze selection before capture: all 75 known nearby repaired-OFF misses plus
  stratified original-correct, split, unavailable-snapshot, mode/SNR and near/far
  controls; at most 512 selected incoming IDs per date and 64 detailed records
  per selected ID. This sample is not a population recall denominator.
- Existing five-key input limits remain 80 raw spellings and 5,120 raw receiver
  incidences. Canonical groups retain union semantics and may exceed 64
  receivers. Raw and canonical serialization can repeat incidences.
- Freeze the studied temporal configuration at eight observation candidates
  and 32 events, allowing actual winner/subject/fallback extras. The wanted-event
  join is separately bounded by 512 times 32, or 16,384 IDs. Stream at most one
  actual request mapping per Observe attempt, bounded by the daily row limit.
- Existing raw-day/scorer/comparison state remains day-owned, separately
  accounted and released between dates: at most 8,000,000 raw rows and 512 MiB
  decoded CSV input. Delayed terminal streams require ID pairing, not position.
- The approved literal run-output limit is 4 GiB, with 8 MiB per diagnostic
  record, 1,024 bytes per label and 64 KiB per encoded row. Enforce structural
  and byte limits before large allocations. Stream bounded records; do not
  marshal an unbounded population before checking size.
- Budget, duplicate/missing provenance and integrity failures invalidate the
  study. No silent truncation or discarded difficult cases. A failed run must
  not publish COMPLETE; a FAILED marker is best-effort when storage permits.

## Reviews and validation

A separate non-steered scientific review derived the estimator and model-claim
limits. A separate design comparison evaluated request-local evaluation,
owner-published tracks and joint inference. Independent scope challenges of
the initial and materially revised ledger required direct consumed-evidence
capture, owned copies, observed temporal mappings, vector provenance, explicit
unknowns, pre-change parity and bounded output; those findings are adopted.

The detailed post-approval falsifiability review distinguishes E0 checker
refinements from the material E1 budget gap recorded below. E0 refinements are
adopted before implementation:

- Bind each decision payload, family, focal eligibility and ordinal to the
  actual parsed archive, not only to its self-hash. Validate frozen plan,
  report, manifest, decision and archive bindings and complete three-arm input.
- Exercise literal scorer vectors for inclusive time/frequency boundaries,
  repeated rows, aliases/SSID, decision versus arrival time, EOF/day end, zero
  denominators and rounded-percentage ties. Preserve A-only raw beacon evidence.
- Exercise reordered, duplicate, missing and forged records and all paired
  outcome categories. Reconcile every old integer total before new scoring.
- Exercise bounded malformed-input/fuzz seeds and actual CLI isolation: no
  fallback into ordinary replay, downloads, prediction or database creation.
- Exercise short writes and flush/sync/close/finalization failures; completion
  follows successful finalization. Preserve partial failed evidence and old files.

For E1, add ownership-mutation, consumed-read/early-return/cutoff, generation,
temporal lifecycle/mapping, concurrent/race and disabled-allocation checks.
Compare against frozen pre-change evidence as well as modified tracing OFF/ON.
Include available status/reason/gate/identity/timing/temporal/counter/learning
and cleanup semantics. Legacy-unlogged fields remain unavailable and require
consumer fixtures/source review, not invented empirical parity claims.

Use targeted native ordinary/race package checks, then one complete final native
ordinary/race lane, vet, Staticcheck and lint. Perform the substantial Go source
review, applicable support/comment/config checks, code-map generation/check and
final-diff review. These are planned obligations, not claimed passing checks.
No CPU optimization or profiling campaign is authorized by this ledger.

## Post-approval material finding: E1 storage ceiling

The detailed review established that replay creates `custom_scp_runtime` inside
its output directory. Pebble writes do not pass through the report writer;
logical key/receiver limits do not prove peak WAL/SST/compaction disk bytes.
Some database writes are best-effort. A report-byte quota or directory polling
cannot enforce the approved literal 4 GiB all-run filesystem ceiling.

This is a material scope/guarantee gap, not a checker refinement. E1 dependent
implementation is stopped pending the owner's budget decision and an amended,
reviewed scope. E0 creates no database and can proceed independently under the
literal output ceiling. Do not silently narrow the guarantee or add a quota
filesystem/persistence failure channel under this approval.

The practical amendment offered to the owner would cap evidence files and
frozen asset copies, retaining the inherited database limits and reporting its
measured scratch usage separately. That interpretation is not selected by this
document. The unchanged alternative requires an explicitly approved strict
filesystem-quota design. Original activation and resource gates remain unmet.

The owner subsequently selected “Cap evidence and assets; report scratch usage
separately.” The [approved v6 amendment](call-correction-v2-scope-ledger-v6.md)
records that policy and its implementation/validation boundary. The original
v5 guarantee is not silently changed: the owner subsequently provided exact
`Approved v6`, authorizing that reviewed E1 amendment.

## Implementation checkpoint

E0 is implemented and its actual four-date audit and selected validation lanes
have completed; the [execution record](call-correction-v2-execution.md#approved-v5-reproducible-receipt-audit-and-preliminary-method-evidence)
records source boundaries, full package/case reconciliation, original race
timeout and successful continuation. E2's exposed-only accounting and case
selection are preliminary, not a feasibility verdict. E1's shared diagnostic
unit is implemented and has passed its targeted checks; the [method record](call-correction-v2-method-investigation.md#shared-diagnostic-checkpoint)
states their boundaries. Replay storage/wiring is also implemented and locally
checked; the [integration checkpoint](call-correction-v2-method-investigation.md#replay-integration-checkpoint)
separates those results from the remaining full lane and actual captures.
V6 and the shared-unit construction/test gate were satisfied before code;
each later unit retains its detailed gate. E3 has produced the bounded amendment
and construction planning, not an active correction method.

E1 subsequently completed its final native ordinary/race/static lanes and all
four exposed diagnostic captures. Exact complete decision-stream parity and
external cumulative artifact accounting passed; the [capture reconciliation](call-correction-v2-method-investigation.md#completed-exposed-capture-reconciliation)
records the proof and limits. E2 interpretation and E3's successor method scope
remain; no new active method follows from capture completion alone.
