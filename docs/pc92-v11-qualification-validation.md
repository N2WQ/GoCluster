# PC92 v11 qualification evidence contract

This records the R09 checker and observer changes authorized by `Approved v11`.
The inherited traffic, populations, durations, resource ceilings, and required
final-source profiles remain unchanged. Audit correction evidence and overall
PC18/PC92 acceptance are separate results.

This is the slice-handoff record. Later full Q5/Q6 executions, runtime preflight
failures, Q4 fixture sequencing correction and final-state checks are recorded
in the [v11 implementation record](pc92-v11-closeout.md).

## Authoritative artifacts

The runtime, Q4, Q5, Q6, and cache wrappers use one common build/execute/finalize
sequence. They require a fresh evidence directory outside the source tree,
build one retained executable, and execute that exact path. Each run has an
identifier, executable SHA-256, build/test arguments, runtime settings, Go build
environment, expected case names, and monotonic process duration.
The recorded build environment includes CC/CXX. Supplied Perl DLL directories
enter PATH only for retained executable execution, after build and environment
capture, so a bundled GCC cannot silently change the selected build compiler.

`verdict.json` is the only authoritative verdict. It starts with
`status=incomplete`, and every acceptance flag false. The final record is
replaced atomically. A killed wrapper therefore cannot leave an initial positive
verdict. Build failure, process failure, missing executed cases, incorrect
profile/duration, stale/malformed observations, changed source or binary, and
reference mismatch all produce failure. Existing evidence directories are
refused without overwriting their old verdict.

`measurement_passed` records whether this run's measured checks passed.
`provenance_passed` records source/binary association. `profile_accepted` can be
true only for a complete named profile, after both checks; it concerns that
profile's bounded evidence claim. Diagnostic profiles remain unaccepted.
`qualified` and `overall_accepted` remain false while the complete allocation
proof and required final-source qualification remain open. Cache-only evidence
does not establish runtime or whole-subsystem acceptance.

Go runtime/Q4 reports contain `RunID` and `MeasurementPassed`; these are
provisional observations, not qualification verdicts. An old report advertising
`Qualified=true` is rejected instead of coexisting with a contradictory final
verdict. Q4 retains its explicit open allocation-proof failure.

Input manifests compare pre-build, post-build, and post-execution content and
membership of tracked/untracked repository inputs. Runtime/Q4 additionally hash
their ignored CTY/H3 assets and configured confusion/reliability model paths,
including missing optional assets. Q6 includes the pinned receiver Perl tree,
prefix data, interpreter, and explicitly supplied libraries/DLL directories.
The reference pin is checked before and after execution. Go module manifests,
toolchain version and build settings are retained; this is workspace/build
association, not a claim to detect every transient edit-and-restore operation.

## Clock and output observers

Qualification-only immutable clock settings are atomically replaced outside the
protocol actor. The returned monotonic application instant precedes replacement
and conservatively starts the timing interval. Historical setup offsets retain
their existing authority-time meaning. Frozen-clock faults affect authority
UTC; payload expiry, handshake deadlines, I/O deadlines, and evidence durations
retain real elapsed time. Ordinary builds have no mutable clock settings.

Q6 regression, quiet frozen-clock, and lifecycle-loaded frozen-clock cases
require both external PC9x closures within five seconds of fault application.
The loaded case uses four bounded synchronous snapshot callers; applying the
fault does not depend on those requests being serviced. Clock recovery has one
second of stable progress plus at most the following second for observation.
The erroneous five-second minimum and eight-second allowance are removed.

External reader termination records its time and reason. Remote EOF/reset can
establish remote closure. Local close, overflow, oversized output, unrelated
reader error, and late observations cannot. Snapshot requests use the remaining
absolute deadline, and predicates cannot succeed after that deadline. The
bounded external observer holds at most 256 wire events per peer; overflow
invalidates evidence and is never treated as successful peer closure.

The tagged publication callback observes successful outbound control-queue
admission, not wire reception or receiver processing. It is installed as an
immutable callback and must remain bounded and nonblocking. Retained observer
data belongs to the external qualification driver, separately from production
protocol allocations. The one-second membership deadline includes recovery;
matching C/A ordering remains independently checked at the receiver.

## Detailed falsifiability mapping

| Failure mechanism | Stimulus and distinguishing observation | Checker / owner |
| --- | --- | --- |
| Fault application waits behind the actor | Fill lifecycle queue with no consumer; fault changes UTC immediately without changing queue occupancy | `TestQualificationClockFaultBypassesActorQueue`, peer tagged tests |
| Authority offset changes payload TTL | Admit at controlled authority time and query the exact 600-second elapsed boundary | `TestQualificationClockSeparatesAuthorityAndPayloadExpiry`, peer tagged tests |
| Observer failure masquerades as remote closure | Real monitor reads EOF, two events into a one-event queue, and an oversized record; reason/time/error flag distinguish them | `TestQ6ObserverTerminalReasons`, peer tagged tests |
| Late successful predicate/event passes | Test before/exact/after deadline and earlier event checked late | `TestQ6DeadlineRejectsLatePredicateAndWire`, peer tagged tests |
| Unsafe clock exceeds external deadline | Apply regression and freeze, including sustained lifecycle pressure; independently observe closure/refusal and ordered recovery | Q6 clock cases, both periodic configurations |
| Wrapper never executes selected tests | Exit-zero mocked process omits one required case | All five wrapper behavioral fixtures; exact `missing_case` failure |
| Wrapper reuses prior success | Repeat a positive wrapper invocation with the same output path; stale report run identifiers | All five fresh-output fixtures; runtime/Q4 run-ID fixtures |
| Source changes are merely recorded | Change, add, delete inputs during execution; change during build; mutate ignored runtime assets or receiver input | All five manifest fixtures plus runtime/Q4 asset and Q6 reference fixtures |
| Binary association changes | Mock process replaces its executable pathname after execution starts | All five binary-change fixtures |
| Process failure leaves a positive verdict | Emit successful provisional output, then exit nonzero | All five process-failure fixtures |
| Malformed/wrong-profile reports pass | Invalid JSON, array instead of object, wrong profile, failed measurement, or positive old qualification flag | Runtime/Q4 observation fixtures |
| PowerShell coercion accepts malformed field types | Array-valued profile/phase, string/boolean/fractional/negative failure counts, and scalar/non-string Q4 evidence collections | `test-pc92-qualification-observation-types.ps1`; positive null/empty arrays and numeric zero distinguish valid output |
| Perl DLL setup silently switches build compiler | Fake build process rejects DLL PATH; retained fake test requires it | Q6 wrapper behavioral fixtures; CC/CXX captured before runtime PATH change |
| Shortened run becomes full qualification | Full-profile case output with a short actual process duration | All five `short_duration` fixtures |
| Checker always fails | Complete positive fixtures exercise each real wrapper; direct full-duration finalizer positive control | Shared behavioral fixture, mock-only |
| Complete ownership remains unproven | Valid measurements while explicit overall dependencies remain open | Every final verdict retains `overall_accepted=false` and open evidence |

The parent implementation owns deadline/commit linearization, replay readiness
and retirement, one-second publication under combined load, recovery generation
bounds, controller/maintenance fairness, continuous clock-health detection, and
changed allocation inventories. Their targeted tests and actual qualification
runs are required in addition to these checker tests. Passing this matrix does
not constitute execution of those workloads.

## Combined scheduler diagnostic and remaining evidence

`TestPC92SchedulerCombinedMembershipAndMaintenance` prepopulates 4,096 nodes,
65,536 users, 131,072 typed edges, 262,144 ingress records, 16,384 freshness
records and all four caches at their cardinality limits. It supplies 1,000 local
users and 64 actual bounded queues with net.Pipe writers. It reserves a final
candidate with 256 staged records and queues full topology projection capture.
Four synchronous lifecycle producers compete with scheduled 100 PC92/second
(45 A, 45 D, two 8,000-member C, eight K) and 100 PC93/minute input. These are
the producer schedules; this short test does not establish sustained rates.

The first observed successful recovery C admission changes a prebuilt immutable
external membership provider: one withdrawal, one new user and one IP change.
Its monotonic eligibility timestamp precedes publishing that provider revision.
Each of 64 recipients must receive its original immutable C/A pair before
catch-up, and all records establishing the new state must be admitted within
one second. A later complete current C plus matching A can satisfy catch-up;
C alone cannot satisfy metadata convergence. The bounded observer fails on
overflow. Starting with 96 UTC slots consumed near rollover exercises progress
across the 100-slot boundary.

The cache cohort is aged before actor ownership. Read-only heap inspection
measures controller cleanup without invoking cleanup itself. This establishes
short service behavior under synchronized expiry, not a real 600-second soak.

| Requirement | Evidence supplied here | Remaining evidence |
| --- | --- | --- |
| One-second membership admission including recovery | All 64 queues, 1,000 local users, immutable older C/A, withdrawal/join/IP changes, real actor contention | Sustained declared workload, actual user/socket producer path, spot forwarding mix and final-source runtime qualification |
| Maintenance fairness and one-second expiry cleanup | Full prepopulated caches, aged synchronized expiry, full topology and competing lifecycle/input work | Real 600-second retention/cleanup cycles, repeated spikes, long-run occupancy and recovery |
| Replay and topology projection coexist with publication | 256-record replay ready at commitment; normal diagnostic completed replay and retained a full projection | Long-run replay/live ordering, retirement ownership and SQLite/native memory proof; race diagnostic may end before replay/projection completes |
| Clock close/gate/recovery | Actual external peer observations for regression and quiet/loaded freeze, both periodic configurations, actual pinned receiver replay | Full Q6 repetitions, all other faults and receive-only profile, final-source wrapper provenance |
| No false qualification from runner/oracle defects | Five actual wrappers exercised with mock processes plus typed report negatives | Actual complete profile executions; mock durations and fixtures cannot supply workload evidence |
| Overall 480 MiB acceptance | Explicitly remains open | Enabled SQLite, context backing and retiring generations ownership proof plus required final-source profiles |

## Commands and observed results

All execution paths below are local test evidence, not full qualification.

- `scripts/test-pc92-qualification-contract.ps1` passed 64 behavioral fixtures,
  including all five actual wrapper entrypoints, retained-binary association,
  negative verdict/provenance cases and the compiler PATH separation. Mock-only
  artifacts: `D:\codex-gocluster-v11-20261001\tmp\pc92-wrapper-fixtures-ed23f40785e348a38a69ac44db4ec6d5`.
- `scripts/test-pc92-qualification-observation-types.ps1` passed 33 additional
  typed observation positive/negative fixtures. It also exposed and corrected
  null Q4 failure/evidence collections being counted as one PowerShell item.
  Mock-only artifacts: `D:\codex-gocluster-v11-20261001\tmp\pc92-observation-fixtures-358bf0fbcbcf43ea98ae2595c7b5defd`.
- With `GOMAXPROCS=2`, `GOGC=50`, `GOMEMLIMIT=1536MiB`, tagged combined scheduler
  and clock/observer/deadline tests passed (`ok dxcluster/peer 1.750s`). All 64
  recipients converged within 219.8529 ms; synchronized aged-cache cleanup took
  141.5674 ms. The short interval admitted 22 PC92 inputs, completed staged replay
  and retained a 15,253,600-byte projection. Log:
  `D:\codex-gocluster-v11-20261001\evidence\r09-r07-targeted-final-2p.txt`.
- The same selected tests with race detection passed after final test edits
  (`ok dxcluster/peer 8.030s`): worst membership 164.9374 ms and cleanup 932.5571 ms.
  That instrumented run ended before replay and projection capture completed;
  it does not supply their completion evidence. Log:
  `D:\codex-gocluster-v11-20261001\evidence\r09-r07-targeted-race-final-2p.txt`.
- Actual Q6 preflight selection
  `^TestPC92QualificationQ6Faults$/zero=(false|true)/repeat=1/(clock|publication)`
  passed all eight cases in 44.879 seconds at the same 2P/GC/memory settings.
  Maximum observed clock-fault closure was 4.1987351 seconds from external
  application; maximum gate recovery was 1.6940773 seconds. Fresh configured
  peer identities tested global admission gating. Captured recovery C/A was
  replayed through the actual pinned DXSpider receiver and checked for IP
  convergence. Log:
  `D:\codex-gocluster-v11-20261001\evidence\r09-q6-gates-preflight-2p.txt`.
- All ten qualification PowerShell scripts parsed successfully, and the scoped
  tracked diff passed `git diff --check`. Tagged peer lint after correcting the
  new scheduler test still reports ten existing tagged findings (errorlint,
  noctx, staticcheck and unparam); it is not a clean lint pass. The broader
  peer/cluster tagged run also reports existing cluster findings, including
  G703 paths outside this scope. Logs:
  `D:\codex-gocluster-v11-20261001\evidence\r09-targeted-peer-tagged-lint-final.txt`
  and `D:\codex-gocluster-v11-20261001\evidence\r09-targeted-tagged-lint.txt`.
- The initial behavioral fixture run on C: stopped on disk exhaustion. Its
  expected test-failure case instead encountered build failure, and the fixture
  correctly rejected that wrong reason. This is failed environment evidence,
  not a qualification or checker pass.

For the space-constrained host, each validation command sets only its process
environment: `GOCACHE=D:\codex-gocluster-v11-20261001\go-cache` and
`GOTMPDIR`, `TEMP`, `TMP` to `D:\codex-gocluster-v11-20261001\tmp`.
Machine/user defaults are unchanged. Mock executables use immutable hardlinks
inside their isolated fixture directory to avoid one copied image per case.

No shortened diagnostic run in this document substitutes for full Q1-Q6,
the shipped-Q1 repeat, the final repository lane, or the complete 480 MiB
owned-allocation proof. The actual receiver evidence above is limited to the
captured preflight recovery exchanges.
