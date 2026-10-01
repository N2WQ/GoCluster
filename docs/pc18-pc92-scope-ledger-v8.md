# PC18/PC92 approved v8 execution record

The user authorized **Approved v8** on 2026-10-01. Work remains on `p92`.
V8 inherits every decision, protocol requirement, resource limit, workload and
acceptance threshold in [v6](pc18-pc92-scope-ledger-v6.md) and
[v7](pc18-pc92-scope-ledger-v7.md). Implementation and qualification are in
progress; this record does not claim compliance or authorize commit, push or
deployment.

The user explicitly selected the allocation boundary on 2026-10-01: the
480 MiB ceiling includes all owned protocol data, backing allocations and
overlapping active generations. Go runtime stacks, GC overhead and unchanged
configuration are reported separately. This records the accounting decision;
it does not claim the complete bound has passed qualification.

## Approved shared-parser amendment

Bound spot-comment parsing without changing existing results or admission:

- Replace accumulated scanner match lists and their index with a streaming
  scanner that preserves current transitions, output precedence and complete
  results, including existing Unicode byte-offset behavior.
- Count the existing space/tab-delimited tokens and allocate their backing
  storage once.
- Include the complete temporary comment-parser working set in peer parsing
  reservations for PC11, PC61 and PC26, including invalid-call diagnostics.
- Document allocation ownership and the effect on other shared-parser callers.

No new input restriction, taxonomy change, correction-policy change, resource
limit increase or default change is authorized. The existing 8 MiB shared
peer parsing/reader scratch budget remains binding.

The verified pre-change parser retained 13,592,432 bytes of match/index storage
for an accepted single `A` alias and a 65,000-byte all-`A` comment. Total
allocation is a different measurement and does not establish live ownership.
The earlier extrapolation involving 512 overlapping aliases was disproved by
the actual scanner and withdrawn. The frozen parser SHA256 is
`26B0F348FE844938258A9AA94B2F7B077246641B5D882F091DEEB7D936A4B5E6`.

## Detailed test review dispositions

The post-approval review occurred before shared-source mutation. A separate
worker used inherited context; this was not an independent non-steered review.
The lead accepted these refinements:

- Freeze both the original parser and tokenizer. Compare every result field
  against that oracle over deterministic cases, default/custom taxonomies and
  the supported 512-alias maximum. Do not share replacement parsing logic with
  the reference.
- Explicitly exercise width-changing Unicode, invalid UTF-8, original-byte
  spans, scanner output suppression, punctuation, time-prefix peeling and
  DB/WPM/BPS precedence. Preserve the initial scanner snapshot for the global
  cursor and current-snapshot visibility for each fallback scan.
- Check exact token count/backing capacity at maximum supported input. Derive
  a conservative allocation table including rounding and temporary copies;
  distinguish cumulative allocation from simultaneously owned storage.
- Exercise PC11/PC61/PC26 header forms, extra caret fields and invalid-call
  callbacks. Include callback normalization scratch in the peer lease, while
  identifying retained application logging state under its existing owner.
- Verify lease cancellation, deadlines, errors, shutdown release and progress
  of the 288 KiB reader lease. Run bounded differential fuzzing, race checks,
  comparable original/replacement benchmarks and allocation/CPU profiles.

Complete the final Go validation lane and rerun affected runtime qualification
on the final relevant source. Existing v7 qualification failures remain failures
until replaced by passing evidence; diagnostic runs cannot satisfy required
durations or change their thresholds.

## Observed targeted results

The shared parser and peer reservation changes are implemented. The full spot
package passed normally (2.271 s) and under the race detector (6.387 s).
The frozen-original differential covered 12,288 deterministic cases and a
30-second fuzz run with 19,320 executions. Explicit cases cover Unicode spans,
keyword precedence, taxonomy visibility, malformed UTF-8 and exact 32,768-token
backing. The cursor plus fallback check allocated zero objects. Spot lint
reported zero issues.

Maximum-size parser tests measured cumulative allocation from 133,896 bytes
for one repeated alias to 5,915,200 bytes for dense malformed UTF-8. These are
component allocation observations, not a proof of the complete transport pool.
The peer charge boundary, actual parser consumer, bad-call callback, concurrent
lease, fixed deadline, Stop cancellation and reader-headroom checks passed
normally (1.264 s) and under the race detector (2.639 s).

Matched three-run microbenchmarks on Go 1.26.4/windows amd64 measured ordinary
comment medians of 5,791 ns/op before and 3,743 ns/op after, with 4,648 to
1,000 B/op and 21 to 13 allocations/op. Paired CPU/allocation profiles show
the accumulated matcher/index storage has been removed. Evidence is under
`%TEMP%\gocluster-v8-comment-parser-20261001`; these measurements used the
machine's default 20 processors and do not establish the qualified two-processor
runtime latency. The final full validation lane and affected long qualification
remain outstanding.

## Allocation and lifecycle review corrections within S04/S11

Review of Go 1.26 map growth showed that a live-key limit alone does not prove
the complete retained bucket/directory bound under hash skew and churn. No
480 MiB overrun was observed; this was a missing proof. The implementation uses
peer-private exact-key indexes with explicit bucket backing for the changed
protocol owners. Existing limits, TTL, no-eviction policy, authority and atomic
C admission remain controlling. This is an internal correction to the approved
S11 bound, not a new protocol feature or an increased resource allowance.

The lead reviewed this correction with inherited-context design and test
workers; it was not an independent non-steered review. Required checks include
forced collisions against a map oracle, delete-current iteration, bounded
growth/compaction overlap, exact TTL, expiry/index agreement, atomic refusal,
race checks and complete allocation derivation. The common index passed normal
and race checks; its 30-second differential fuzz run completed 143,858 executions.
Cache and graph component results are recorded in the allocation evidence; full
qualification remains required on the final source.

A second correction keeps the combined 64+128 transport reservation until Run
finishes terminal callbacks and releases ownership. Registry removal alone is
insufficient: a slow callback can outlive the reusable registry identity.
Admission reserves both pending and lifetime credits before production dialing
or session construction. A real-session regression retired 192 successive
owners behind a blocked reporter, refused another owner, then released and
refilled all credits. This and cancellation before Run passed normally and
under the race detector. These are local lifecycle results, not a long-running
leak qualification.

The review also found that retrying the complete startup A/K pair after K's
timestamp refusal could accumulate duplicate A records in pending output.
Startup retry must retain its completed A phase and resume at K. This preserves
the intended initial exchange and is necessary for the pending-control backing
proof. Targeted phase, failure and ownership tests plus the final integration
lane are required before this correction is considered validated.
