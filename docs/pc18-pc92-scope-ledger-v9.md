# PC18/PC92 approved v9 execution record

The user authorized **Approved v9** on 2026-10-01 and explicitly selected
keeping SQLite while cluster behavior is qualified. V9 authorizes the isolated
persistence feasibility experiment below. It does not authorize replacing the
production driver or migrating to Pebble.

Work remains on `p92`. The production baseline was clean at
`0d8a728354cc423f3d7929208aac2b72b05f9d5f`. No production Go source,
dependency, configuration, schema, database, or running process was changed.
This record follows [v8](pc18-pc92-scope-ledger-v8.md) and does not supersede
the inherited D1-D8/S01-S14 protocol decisions or Q1-Q6 acceptance requirements.

## Outcome

**The isolated v9 slice is complete with a negative feasibility result.**
The candidate driver was rejected under the approved boundaries. The current
production `modernc.org/sqlite v1.36.1` remains in place. Full PC18/PC92
qualification and the complete 480 MiB allocation proof remain open.

The [evidence report](pc92-persistence-feasibility-v9.md) records the exact
versions, tests, measurements, reproduction command and limitations.

## Approved scope and disposition

| Item | Approved boundary | Implementation and validation disposition |
| --- | --- | --- |
| P01 | Isolated temporary module using ncruces/go-sqlite3 v0.35.6 and go-sqlite3-wasm/v6 v6.3.35304, with modernc v1.36.1 as the current-driver reference. | Pinned module copy and probe module outside the repository; module inventory, source hashes, build and vet recorded. No production dependency replacement. |
| P02 | Provisional 10 MiB aggregate persistence allowance: 8 MiB engine and 2 MiB host/other storage, including backing, native/WAL mappings and overlapping generations. Existing projection snapshots retain their 36 MiB reservation. | Early ownership and fallback-allocation gates failed. The complete host/aggregate proof was not attempted after rejection. This provisional allowance does not establish available production headroom. |
| P03 | If the candidate survives early gates, trial one serialized connection, streaming rows, a deadline beginning before serialization wait, and retirement of an exhausted owner before replacement. | Not implemented after the earlier hard gate. Existing production workers and connection ownership remain unchanged. |
| P04 | First reproduce stock failures. Permit only local experimental repairs to partial initialization/open cleanup, known allocation-error/panic containment, retirement and accounting/fault hooks. | Reproduced failed-open retention and the typed allocation-exhaustion panic. Added observation and optional-API availability seams only. No repair was applied because fallback allocation and WAL compatibility require work outside the approved boundary. |
| P05 | Preserve accepted files, diagnostic rows, additive schema, transaction atomicity, plain-path/pragma DSN behavior, pragma ordering and reopen semantics. | Ordinary WAL controls passed for modernc and the candidate native backend; the candidate's existing fallback failed with IOERR_SHMMAP. The full compatibility/atomicity matrix was not run. |
| P06 | Retain reproducible evidence and report feasible, rejected or unverified. A negative result is a valid end to this slice. | Rejected; source/binary/runner manifests and observed results retained. No production integration or broader engineering follows from this verdict. |

The provisional 10 MiB allowance is not a resource-limit increase. Remaining
metadata, context and retirement ownership still require a complete inventory
before the original 480 MiB ceiling can be established.

## Detailed test review and findings

A post-approval test-strategy review preceded experimental code. A separate
reviewer used inherited context; this was not an independent non-steered review.
The lead retained scope and verdict authority. Accepted refinements included:

- Measure Windows native extents directly; Go MemStats and process exit cannot
  establish live ownership or release.
- Count reserved and committed address regions without double counting. Verify
  simultaneous failed-open extents are disjoint and still live after the last
  failure. Keep observer state scalar so it cannot retain engine wrappers.
- Use a successful open/close control and an allocation/release observer control.
- Identify the candidate's exact known OOM panic; do not mask unknown panics or
  claim that test recovery constitutes production containment.
- Force only the existing unavailable-API branch, then measure actual backing
  capacity and overlapping growth. Distinguish this backend sequence from a
  measured SQL workload peak.
- Run equivalent modernc/native WAL controls before attributing fallback failure
  to compatibility. Do not infer a complete DSN/atomicity matrix from that check.

The fresh final review checked the final source, manifests, controls and
negative-result interpretation. It verified 290 source hashes and the precise
two modified/two added candidate files. It also used inherited context and is
not described as independent evidence.

## Conditional validation and explicit stop

A surviving candidate would require the complete source/OS ownership bound,
fault coverage across lifecycle phases, exact old-or-new transactional content,
and real SQL/lock cancellation. The approved later phases also include:

- 1,000 cycles: 250 each of successful lifecycle, partial-open failure,
  allocation exhaustion and cleanup failure.
- A 30-minute run with two Go processors, GOGC 50 and GOMEMLIMIT 1536 MiB;
  one projection and one legacy request per second, bounded coalescing,
  three ten-minute full-count/max-admitted-metadata/alternating-generation
  phases, healthy commits from both classes every five seconds and latest
  snapshot within five seconds of drain.
- Windows and Linux amd64 execution.

**These later phases were not run.** The existing fallback's allocation behavior
and ordinary WAL failure already reject this candidate within v9. Repairing an
allocator, VFS or generated engine, adding a custom allocator or production
helper, making broad API repairs, or narrowing supported platforms is outside
the approved scope. The negative-result exit therefore completes the experiment
without pretending the conditional tests passed.

## Validation and documentation closeout

The isolated probe compiled and passed vet. The adversarial test binary exited
1, with four expected candidate failures and a passing observer-control test.
The evidence runner exited 0 because it verified all rejection conditions and
preserved the failing test result; this is not a passing candidate test suite.

Repository changes are ordinary Markdown evidence/status documentation.
Documentation links, consistency, the final diff, the troubleshooting-record
checker and whitespace checks form the repository validation lane. The final
documentation review checked eight Markdown files and 29 local links with no
missing targets or production-file changes. The troubleshooting-record checker
and git diff --check passed. No new production Go validation result is claimed.

Support-agent documentation impact: **Required**. The peer support card now
distinguishes this rejected experimental driver from the current production
driver and preserves the outstanding allocation-proof limitation.

[TSR-0035](troubleshooting/TSR-0035-pc92-qualification-accounting.md) retains the
durable ownership and checker lessons.
[ADR-0230](decisions/ADR-0230-pc18-pc92-authority-and-bounds.md) retains its existing
SQLite projection decision and links the evidence; no new architecture decision
or supersession was made. No commit, push or deployment is included.
