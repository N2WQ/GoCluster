# Call Correction v2 Scope Ledger v6

Status: Approved by the owner with exact `Approved v6`, 2026-11-18.
Base authority: [approved v5](call-correction-v2-scope-ledger-v5.md).

## Objective and owner decision

Resume the approved bounded diagnostic replay and method investigation without
adding a database-quota subsystem. The owner explicitly selected:
“Cap evidence and assets; report scratch usage separately.” This resolves the
budget policy; it does not by itself authorize this revised implementation scope.

The benefit is progress on the retained-evidence and Bayesian feasibility
questions within the existing correction method. The tradeoff is that 4 GiB no
longer bounds the scratch database or total disk usage. Its existing logical
key/receiver limits remain, and its final file bytes are measured separately.
That final measurement does not establish peak file bytes, compaction/WAL
high-water usage, allocated filesystem blocks or protection against disk-full.
Inherited replay cleanup does not observe the Custom SCP close error; this
amendment does not add database durability or error-free shutdown claims.

## Exact amendment

Replace only v5's literal combined all-run output guarantee for E1 diagnostic
replay with the following boundary:

- One shared 4 GiB budget covers all first-party run-owned evidence files and
  frozen asset copies for a diagnostic invocation. Writers or dates within one
  invocation do not receive separate budgets. Original frozen inputs and the
  existing scratch database are outside this artifact budget.
- Count ordinary replay/study evidence as well as new diagnostics: decision,
  request-mapping and trace streams; retained capture logs; runbook/sample output; interval/threshold
  CSV; gate/manifest/history/report/aggregate files; completion/failure markers;
  frozen model/geography/license copies and any additional run-owned license
  snapshots; temporary copies and partial writes. Renaming a temporary file
  does not charge its bytes twice. Failed writes may be conservatively charged
  for their full requested length.
- Debits are cumulative for every artifact write/copy and are not refunded
  when cleanup removes a license snapshot or temporary copy. Report charged
  bytes and reserved capacity separately from retained regular-file bytes.
  Label each accounting stage explicitly, including whether it precedes its
  own report/COMPLETE write; final external reconciliation includes those files.
- Enforce the shared budget while writing or copying, before bytes can exceed
  the allowance. A pre-copy size check alone is insufficient. Retain v5's
  structural preflight, 8 MiB record, 1,024-byte label and 64 KiB encoded-row
  limits, with reserved metadata/failure capacity. No silent truncation or
  omitted difficult records may make a run appear complete.
- Only the inherited `custom_scp_runtime` database files are excluded from this
  artifact budget. Preserve its configured key/receiver limits, construction,
  persistence behavior, cleanup and existing best-effort disk-error policy.
  Add no quota filesystem, compaction tuning, write barrier, checkpoint,
  persistence-error channel or new database option.
- After the existing replay/database cleanup completes, measure final regular
  file bytes and file count beneath that run's scratch directory using bounded
  streaming traversal without following links. Record the measurement stage,
  configured key/receiver limits, presence and measured final bytes separately
  from artifact bytes. Disabled scratch is explicitly not applicable. An
  expected missing/unreadable/unsupported tree or failed measurement invalidates
  the diagnostic study; it is not reported as zero or silently ignored.
  Label the stage as post-existing-cleanup, with inherited unobserved database
  close errors explicit; do not present it as successful durable closure.
- Freeze traversal limits: batches of at most 256 directory entries, depth at
  most 16 below the root, paths at most 1,024 bytes, and at most 1,048,576 visited
  entries/directories plus one overflow probe. A depth-first walk owns at most
  17 open directory handles. Do not materialize or sort an entire directory.
  Any bound or close failure invalidates measurement; report no partial total
  as complete. Determine applicability from the backend actually constructed,
  including disabled correction or a legacy backend, rather than one config
  flag alone.
- Measurement runs after cleanup, outside correction/resolver/temporal locks.
  Add no sampler goroutine, timer, component-lifetime observer or I/O callback
  under a lock. No peak-disk claim follows from a final measurement.
- Budget, copy, measurement, integrity and finalization failures are diagnostic
  failures, separate from correction outcomes. The study returns an error and
  cannot publish COMPLETE. Preserve the original failure when cleanup or a
  best-effort FAILED marker also fails. Partial files remain usable only for
  troubleshooting, never qualification.
- Add only diagnostic-run writer/copy hooks and a run-owned failure latch where
  existing helpers ignore an artifact write or Close error. Cover runbook
  `Fprintf`, interval/history/index finalization and both `.study-assets` and
  `.license-inputs` copies. Check latched errors before completion. Ordinary
  non-diagnostic behavior and the inherited database error policy are unchanged;
  this does not authorize a general I/O refactor or a new component observer.

E0's completed audit creates no scratch database and retains its literal 4 GiB
run-output contract. Its original inputs and new completed artifacts are not
rewritten by this amendment. Validation/build artifacts outside a captured run
remain validation evidence, not extra unaccounted replay-output writers.

## Unchanged authority and boundaries

All v5 E1–E3 content and limits remain controlling: actual consumed-evidence
capture, owned flattened copies, five-key witness vectors, observed temporal
ID mappings, explicit unexecuted branches, no extra lookups/rules/clock changes,
disabled-diagnostic parity, selected-case bounds and causal separation.

The owner still requires a current matching report for active replacement and
permits separately supported proposals only when evidence distinguishes signals.
No active candidate generator, production model asset/installation, correction rewrite,
new state owner or activation is authorized. The investigation may conclude
insufficient or inconclusive. Existing settings, ordinary gates, history and
licensing requirements, V-only learning and public confidence/family/stabilizer
behavior remain. CPU/heap/allocation optimization is deferred; original failed
qualification and activation gates are not waived. November 17 contents remain
reserved until an actual method and confirmation contract are frozen.

No merge, push, deployment or service restart is authorized. Execute the
remaining narrow units sequentially after E0 closeout. If the writer inventory
or actual lifecycle reveals another material scope/guarantee gap, stop the
dependent work and revise the scope before crossing that boundary.

## Validation and scope challenge

Before E1 implementation, disposition the amendment's separate scope challenge
and detailed test-strategy review alongside v5's existing matrix. Required
additional evidence is:

- Inventory every first-party file creation/copy/append/finalization path in
  diagnostic execution. Demonstrate that only the declared scratch subtree is
  excluded and that preserved ordinary/non-diagnostic paths keep their behavior.
- Exact-limit and one-byte-over tests across multiple artifact writers and
  asset/license copies, including a source that grows after preflight, short
  writes and escaping/multibyte labels. No output may exceed its shared cap.
- Verify copied bytes/hashes and owned paths; changed input, copy failure,
  budget failure or late finalization failure must prevent COMPLETE.
  Inject failures through helpers that ordinarily ignore their write/Close
  result, proving that the diagnostic latch still prevents completion.
- Literal scratch-file totals after cleanup; explicit disabled, missing,
  unreadable, unsupported topology, counting/overflow and directory-close
  failures. Failure must not alter a correction, become fallback, or publish
  completion with an invented zero measurement.
- Reconcile a real exposed diagnostic run's artifact bytes and separate post-cleanup
  scratch measurement with its manifest and source/config/asset bindings.
  Retain frozen pre-change and tracing-disabled/enabled semantic checks; no
  legacy-unlogged fields are invented.
- Reuse v5's ownership, cutoff, provenance, concurrency/race and allocation
  checks plus the selected ordinary/static/fuzz/source-review lanes. No new
  production performance or RF-accuracy claim is authorized by these tests.

A separate read-only reviewer traced the writers, copy paths and cleanup-to-
completion sequence and found no material authority conflict. The lead verified
and adopted its three refinements: cumulative staged accounting, the finite
traversal limits above and the complete writer inventory with diagnostic-only
error latching. The amendment-specific seven-row test matrix covers shared
budget, bounded copies, ignored artifact errors, measurement stage/bounds/
applicability and accounting stages. This is scope/falsifiability review, not
an implementation pass or new scientific qualification. The separate reviewer
rechecked the final revised wording and found no new material scope issue.

The only new implementation authority is artifact-budget coverage and separate
final scratch accounting described above. Detailed construction and targeted
checks remain subject to the post-approval test-strategy gate before E1 code.

## Implemented amendment checkpoint

The amended E1 implementation and final selected functional validation are
complete. All four exposed captures passed complete decision-stream parity,
actual trace-call coverage and separate external final accounting. Each stayed
within the cumulative evidence/assets cap; post-existing-cleanup scratch bytes
were measured and reported separately. The [capture reconciliation](call-correction-v2-method-investigation.md#completed-exposed-capture-reconciliation)
binds exact source, plan, executable, bytes and proof. No peak disk, durable
database closure, RF-accuracy, active-method or resource qualification is inferred.
E2/E3 remain under their original boundaries.

Exact authorization required before implementation: Approved v6
