# Call correction delivery scope ledger v1

Approved by the owner's exact `Approved v1` on 2026-10-11. Inspected baseline:
`8890e798cf7c70b8decdc8b10ce8565e4f7f8c3b`, branch `fix_correction_evidence`.
Execution branch: `fix_temporal_family_delivery`.

## Authority and sequence

Execute T0, T1, F1 and F2 sequentially. Each receives targeted checks, complete
validation, final review and a local commit before the next production patch.
Preserve unrelated profiling work, including overlapping documentation hunks.
No merge, push, deployment or live feeds are authorized. A material scope or
policy change requires revised approval. Routine fixes inside this scope are
autonomous. Tests use the established Windows paths without firewall changes.

## Selected display policy

- Compare related calls using the SAME fresh eligible receiver population;
  count each receiver once in totals and retain competing-call dissent.
- Greater existing whole percentage wins, irrespective of specificity.
- A usable whole-percent slash/base tie favors the slash member. Other ties
  provide no hiding authority. This supersedes the earlier general tie choice
  only for slash/base pairs; it is a product preference, not RF truth.
- Missing, stale, split, uncertain or known incomplete evidence provides no
  hiding authority. Global selection split/uncertainty/loss vetoes hiding.
- A separate usable stronger rival can hide incoming output even when another
  rival's comparison is tied or unavailable; evaluate all before mutation.
- Decide on arrival, add no wait, and never retract earlier TCP output.
- Protect FAST, MED and SLOW separately using previously admitted ordinary
  broadcast lanes. Existing configuration keys remain active and supported.
- Family hiding is telnet-only. Otherwise eligible local history/archive and
  original-input peer publication retain their existing destination gates.

## T0: deterministic toxicity-summary test

Test-only: receive/apply the result, then `Stop()`/join workers before exact
terminal Stats assertions: SafeBypass=1, AICalls=1, Toxic=1, CacheEntries=1;
Pending, ResultBacklog and other counters zero. Keep summary redaction checks.
Do not change production pending accounting, publication or shutdown behavior.
Prior diagnostic repeats observed five ordinary and two race assertion failures
per 1,000, with no race-detector report. Receipt does not synchronize a producer's
deferred decrement. Repeat the repair 1,000 times ordinary/race and retain
blocked-publication and received-result ownership controls, then full validation.

## T1: temporal final-request retirement

Under the existing decoder mutex, retire known terminal requests consistently,
including event_evicted/missing-key/apply/fallback. Delete key state iff no
same-key request remains; defer retains ownership. Reuse the bounded pending
scan, with no index/collector. Preserve independently bounded accepted identity
history, path-switch accounting, confirmations and sequence.

First add a baseline-compatible deterministic regression: events/key=1,
active keys=1, lag=1s, max wait=5s; an older tied request defers, a newer request
evicts its event and completes, then the old request retires. Pending/heap must
be empty and a fresh key admitted. Require baseline assertion failure, not
compilation/setup failure. Reverse retirement order preserves the newer request.
Cover force, repeated evaluation, fallback actions, churn, caps and concurrency.

Actual runtime delivery uses supported overflow_action=abstain: defective
capacity denial retains A immediately; repair admits/holds then releases B with
fresh B3/A1 equal-weight evidence (75%). Assert admission/deadlines/exact counters
and normal/DIAG TCP, sealed ring, stored archive bytes before decoding, reopened
archive, and original-input peer A. Resolver fallback with identical output is
not proof. Replay's real hold/drain path is a separate limited oracle. Primary
production boundary is temporal_decoder.go; unexpected runtime/replay production
changes outside this contract require renewed scope review.

## F1: telnet-only family destination isolation

Shared secondary flags still govern ring (any lane) and archive/local peers
(MED). Family hiding only gates ordinary telnet, preserving SELF and native
original-frame transit. After actual stabilizer ownership resolves, family
hiding cannot prevent recent-band/CustomSCP/WhoSpotsMe/FT history admission when
base normal fanout is admitted. Preserve all-secondary-denied behavior, holds,
risky timeout suppression and once-only release.

Use loaded config and production newOutputPipeline, not manual fixtures that
omit suppressor construction. Establish baseline failure and repaired actual
TCP, ring, persisted/reopened archive and original-content PC61 preservation.
Keep MED refusal tests, source exclusions/no-telnet controls and immediate,
overflow/delayed history readbacks. Old telnet arbitration remains only through
this independently validated checkpoint; F2 replaces it.

## F2: fresh percentages and admitted per-lane history

Use one existing final-literal-call-anchored fresh resolver selection per
arbitration (configured exact/neighborhood, at most five keys). Both identities
use its same denominator, dissent and receiver union. Require usable authority,
both positive supports and compatible score basis: both weighted, or raw when
no weighted total. Mixed basis/missing support provides no authority. Do not
change confidence helper arithmetic, glyphs, markers or correction gates.

Preserve configured relation classifier, truncation controls, eligible modes,
frequency tolerance, cache bounds and switches. Enabled distance-one neighbors
require both identities in fresh usable evidence; cached raw contested flags
cannot authorize hiding. Preserve normalized literal identity for evidence
lookup and canonical identity for relationships; do not pool slash aliases.
Repeated calls recheck competing history; compare before changing entries.

The existing family mutex serializes arbitration, NONBLOCKING broadcast queue
admission and history commitment across ordinary and stabilizer consumers.
TryBroadcastSpotOwned reports admission; retain existing void APIs, immutable
payload reuse, stage markers, drops/logs and nonblocking behavior. Existing
temporal handoff attempts include refused publication, and lastOutput retains
its existing attempted-broadcast semantics. Queue success is not client receipt.

Each capped entry retains only compact per-lane identity/time/frequency witnesses,
no snapshots/rows/full spots. Record only successful ordinary handoffs; failed
or hidden lanes neither refresh incoming history nor delete competitors (normal
expiry housekeeping still applies). FAST never refreshes MED/SLOW. Preserve the
inclusive window boundary, global max_entries count and LRU eviction; eviction
conservatively removes associated protection. SELF-only output teaches no
ordinary history. Partial masks use existing broadcast SELF once; direct SELF
is only the no-ordinary-lane case, with no queue-refusal fallback.

Test stronger base/shorter, slash ties, other ties, both arrival orders, stale/
future/split/uncertain/loss, weighted/overlap/repeated receivers, multiple rival
insertion orders, independent lane expiry/frequencies/repeated calls, deterministic
full-queue refusal followed by actual TCP, concurrent main/stabilizer handoffs,
caps/churn/settings/SELF and destination gates. The behavioral baseline fixture
must compile against pre-F2 APIs; new API tests are repaired-state checks.

## Independent oracle and acceptance vectors

A separate non-steered mathematical reviewer received neutral variables and
objectives, not preferred design or implementation-derived expected values.
The reviewer established ratio comparability, overlapping support and claim
limits; the owner separately selected population/tie policies. Dimensionless
reliability milli-units use exact integer truncation. Healthy shared-population
vectors (global safety vetoes still override):

| Inputs | Expected | Failure detected |
| --- | --- | --- |
| A weights 900+950; B 1000 | A64%, B35% | Unequal weighting |
| One receiver A twice in buckets; B and dissent once each, equal weights | A33%, B33% | Duplicate corroboration |
| Three equal receivers: A, both, B | Both66% | Treating overlapping shares as probabilities |
| Base334; slash330; dissent336 | Both33%; slash preferred | Whole-percent tie policy |
| A500; B499; dissent1 | A50%, B49% | Integer boundary |
| Absent identity/mixed score basis | No hiding authority | Manufactured/mixed evidence |

No tolerance applies to exact arithmetic/categorical expectations. These checks
do not prove RF identity, partial copying or calibrated correctness.

## Complete validation after EACH patch

Sequentially, checking each exit independently:

```powershell
& .\scripts\test-windows.ps1
& .\scripts\test-windows.ps1 -Race
go vet ./...
staticcheck ./...
golangci-lint run ./... --config=.golangci.yaml
& .\scripts\test-customscp-regression.ps1
& .\scripts\test-customscp-regression.ps1 -Race
```

Established .tmp/windows-tests paths only; no firewall edits, temporary listening
executables, live cluster or permission prompts. Add relevant YAML comment-only,
crawler comment/TSR/documentation checks. Final write: codemap generate -all,
then check -all and final diff review. High-risk Go receives quality review and
fresh verification. F2 detailed contract-to-test review precedes implementation.
Targeted cost benchmarks cover terminal pending capacity, dense family history,
fresh/partial-expiry/capped populations and concurrent admission. Avoid per-entry
projection/allocation; preserve all-fresh LookupEligible's zero allocations and
report inherited neighborhood costs separately. No production performance claim.

Persist execution evidence and affected documentation/decision links; partially
supersede affected ADR-0014/0033/0148 family-display clauses. Keep YAML schema,
defaults and configured values. Wider association, RF calibration, resolver
queue/whole-key/FT loss and per-client receipt history remain deferred.

Exact authorization required before implementation: Approved v1
