# Shared Logging: Implementation Evidence

## v7 status

The owner approved v6 to restore the earlier daily file handling and use
fixed-queue asynchronous delivery for file, console and dashboard logs. Peer
logging remains in the cluster executable. File records over 2 KiB are dropped;
dashboard messages retain their configured shortening. Logs are disposable:
the design omits loss and recovery warnings, and reports may use
incomplete logs without warning.

Implementation and final validation are in progress. The approved 4 MiB peer
and 64 MiB total limits apply to logger queues and retained state, including
stalled owners; buffers inside Windows file operations are excluded. The
remaining bounds have not yet been proved. The current PC92
480 MiB qualification also remains open. Do not treat earlier test results as
validation of the v6 implementation.

The owner approved v7 with that accounting boundary. Implementation and
evidence for the revised boundary are not yet complete.

Later review fixes wait for dashboard retirement within the shared shutdown
deadline, keep admissible records when a deduplication summary does not fit,
discard the active log instead of merging into an existing archive, and notify
the report scheduler when rotation succeeds but the new file cannot open. The
dashboard retains its 512-KiB queue ceiling, as the owner confirmed; a whole
message that cannot fit the bounded queue can be dropped. Focused Windows race
checks passed for those changes, including a real dashboard, archive-delete
retry and failed new-file open. Configuration now rejects
duplicate cleaned folder names, including Windows case variants; focused
configuration race checks passed. UI configuration normalization no longer
retains oversized input backing. Windows and Linux file workers check physical
folder and active-file identities before cleanup or writes; conflicting
destinations trigger a configuration error. Operations use the verified open
directory, so retargeting a folder link cannot redirect rotation or cleanup.
Rotation-triggered propagation reports acquire a bounded archive snapshot
before retention cleanup. Focused Windows and Linux race checks cover these
cases. Other operating systems fail
closed when physical identity is unavailable.

On Windows amd64, the current fixed queues, grouped buffers and slot metadata
occupy 14,930,960 bytes. The two peer slots account for 536,896 bytes, and
the peer deduper's fixed-table allowance is 1,089,536 bytes. These figures do
not include all live path, filesystem, sink and adapter backing. In particular,
Windows filesystem path expansion and directory enumeration can retain
native-sized Go buffers during a stalled operation. The remaining in-scope
logger-owned state has not yet been fully accounted for under the owner's revised
boundary. Peer `ChargedBytes` is therefore unavailable, so
the historical PC92 qualification fails closed instead of certifying a bound.

The local `data/logs` samples inspected for sizing contained 134,194 lines.
The largest was 832 bytes; the longest system line was 369 bytes. No peer log
sample was present. These observations support the selected 2 KiB record
limit but do not establish a worst-case message or memory bound.

The native Windows ordinary lane passed 12,164 cases, with 93 skips, across
74 packages before the final linked-archive and conflict-ordering fixes; the
affected final ordinary selection also passed. A later Windows race lane
exposed a conflict-release race and was stopped after that failure. The fix
preserves conflict identity and latches it across runtime generations; the
affected Windows race selection passed 100 repetitions. The full Windows race
lane then passed 12,170 cases, with 93 skips, across 74 packages. Focused Linux
race checks and the real-dashboard Windows race checks also passed. These
checks used `scripts/test-windows.ps1` for the full lanes and `.tmp/v7-final`
for temporary files and logs.

Focused Windows ordinary and race checks passed for the shared queues, peer
diagnostics, report input, daily sink, retention, rotation collisions, report
hook, runtime ownership and the real dashboard integration. The actual main
executable test connected a local peer and checked logging without a companion.
The launcher, isolated-build and release safety fixtures passed; release safety
skipped one unavailable second-drive case. Synthetic PC92 wrapper and finalizer
fixtures passed, but are not production qualification. `go vet`,
Staticcheck and golangci-lint passed; the generated code-map check passed.
Temporary files and caches were under `.tmp`. The full Windows ordinary lane
passed 12,092 cases across 73 packages, with 93 skips. Its cluster binary was
built before a later report-collision fix, and the final-state rerun has the
limitations below. The Windows race lane passed 10,819 cases across the other
72 packages, with 85 skips; the separate final-source cluster race run passed
with 448 top-level cases and eight top-level skips.
One earlier full cluster run used
a five-minute package timeout and timed out; its output did not establish which
test was active, and it did not validate the later source changes. A subsequent
direct cluster rerun omitted the runner's required PowerShell identity and
failed diagnostic-wrapper tests; it was stopped and restarted with that
identity set to the actual PowerShell 7 executable. Three PSK Reporter tests
in the restarted run also require the stable runner binary path; they failed
before execution from the alternate path and passed on the final source when
rebuilt and rerun at the required `.tmp/windows-tests` path. The remainder of
that direct final-source cluster run completed without other failures; this was
not a single clean full-cluster invocation.

## Historical v4/v5 evidence

The following sections describe the earlier implementation and are retained as
historical evidence. Their design, source paths, resource figures and warning
behavior do not describe the approved v6 replacement.

The owner approved v4 to move peer logging into the main executable and use
the same bounded admission and failure handling for runtime logging, then v5
to raise the peer reservation limit to 4 MiB. This note records implementation
evidence and remaining acceptance gaps.

### Scope and checks at v5

| Approved behavior | Implementation and evidence |
| --- | --- |
| Common logger for files, console and dashboard | `internal/unifiedlog` owns bounded record admission; `internal/cluster/unified_log_*` connects the runtime outputs. Runtime, dashboard and console stall tests exercise the shared owners. |
| Peer logging in the main executable | `internal/peerdiag/producer.go` sends typed records to the common coordinator. `TestUnifiedV3MainExecutablePeerLoggingWithoutCompanion` builds and runs the actual executable, connects a local peer and checks the resulting log without a companion. |
| Normal filenames and disposable collisions | `internal/logfiles` provides daily and size rotation, refuses overlapping destinations, and records verified active-file discard events. Group sink, alias, retention and separate-process snapshot tests exercise the corresponding cases. |
| Recovery warnings and reports | The common warning ledger tracks dropped and unconfirmed records separately from file-discard events. `internal/propreport` reports known loss and unverifiable completeness; warning and report tests cover both marker versions and unknown discarded-record counts. |
| Bounded shutdown | Runtime, report-worker and owner tests exercise the shared deadline, stuck outputs and retained ownership. A timeout is not treated as proof that an operation ended. |
| Packaging and qualification | The launcher and release fixtures cover the single executable. Historical companion receipts retain their original checks. The current PC92 480 MiB qualification is open. |

Maintenance availability notices repeat while a condition persists, but their
ordinary queue admission is not a write receipt. If their output fails after
admission, a particular availability or recovery notice can be lost. Actual
record-loss warnings use the separate receipt-backed loss ledger.

The old synchronous sink and fanout constructors remain for legacy tests, but
the live runtime does not call them. Those tests do not establish the current
rotation or failure behavior. A later cleanup should move any still-useful
assertions onto the shared logger before removing the unused machinery.

Dashboard messages may use multiple 2 KiB logger records, admitted together, so
the configured pane shortening still applies. The file destination applies the
2 KiB whole-message limit independently. The tagged
`TestUnifiedRealDashboardGroupedMessagesAndFileDrops` runs the actual dashboard
on a simulated screen alongside a physical file destination. It checks size
boundaries, UTF-8, CRLF, rendered output, truncation and file-loss counts in
ordinary and race builds on Windows and WSL.

### Resource acceptance at v5

The registry refuses reservations above 64 MiB overall or 4 MiB for its peer
accounting. Fixed queue and processor sizes, reservation arithmetic, stalled
owners and positive retirement witnesses have targeted tests. On Windows
amd64 with Go 1.27.1, a representative peer fixture reserves 3,637,248 bytes,
leaving 557,056 bytes within the 4 MiB limit. Windows path discovery keeps
expanded paths in its reserved fixed UTF-16 workspace.

These checks do **not** establish that the reservation declarations cover all
actual live logger-owned backing. Windows file operations now reuse a reserved
UTF-16 input workspace, but simultaneous path copies, native conversions,
channels, closures and errors across discovery, rotation and retirement still
need a complete source-backed inventory. The fixed coordinator shell is charged
to the peer reservation regardless of which category owns it. Caller formatting,
goroutine stacks, runtime allocator overhead, OS buffers, report read buffers and
dashboard-owned row storage are outside the approved logger-buffer accounting.
The 4 MiB and 64 MiB actual backing requirements remain unverified, so this
implementation is not ready for deployment on the current evidence.

#### v5 inventory boundaries

- Each dashboard pane reserves 2,162,688 bytes for its logger-side sink and
  transient copies, plus 565,248 bytes for its queue and control allowance.
  The system pane reserves another 1,048,576 bytes for its two independent
  bounded row buffers. The six panes therefore reserve 17,416,192 bytes in
  total. Their fixed metadata allowance still needs a complete live-object
  inventory; dashboard-owned retained rows are accounted separately.
- The file destination inventory includes the fixed discovery workspace,
  the reusable Windows native input buffer, and measured `os.File` wrappers.
  It does not yet cover every simultaneous path, error, channel and closure
  allocation across discovery, file operations, failure and retirement on both
  supported platforms.
- Each destination now reserves 262,144 bytes for controls and an additional
  128 times its path length plus fixed slack for path-sized backing. These are
  conservative allowances, not proved maxima. Exceptionally long paths can be
  refused before acquisition if the reservation would exceed a budget.
- On Windows amd64, the measured `os.File` wrappers and selected route objects
  use only part of the control allowance. Discovery channels, native-call
  values, closures and error objects also consume it; their maximum coexistence
  has not been proved.
- A retired coordinator can still be referenced by a release or worker tail
  after its state charge is discharged and a successor is admitted. Retired
  dashboard adapters can also remain reachable through old runtime references.
  The number and live backing of these overlapping generations are not yet
  proved to fit the approved limits.
- Runtime configuration, warning supervision, router retirement and callback
  captures also need a complete simultaneous-live inventory against their
  declared reservations. A passing reservation counter or heap sample cannot
  substitute for that bound.
- Configuration construction temporarily borrows the caller's existing
  configuration, including its original path backing. The completed logging
  mapping clones bounded paths and does not retain those original buffers.
  Warning formatting can allocate beyond its initial scratch buffer; its peak
  live backing is still part of the open logger inventory.

### Historical validation runs

The v5 targeted Windows race selection passed 15 checks across the common
logger, file coordinator and cluster, including the literal 4 MiB and 64 MiB
admission boundaries, both enabled peer files, stalled ownership, all enabled
file categories and six dashboard owners. WSL race checks passed the common
logger boundary tests and the dashboard/configuration stress tests. These tests
exercise behavior; reservation counters and heap samples do not prove the
remaining actual-live-backing bound. The historical Q4 ownership test still
accepts 3 MiB and rejects 3 MiB plus one byte; its qualification fixtures passed
without changing that gate. A full v5 validation lane was not run because the
required backing proof remains open.

The preceding v4 validation was:

- The stable Windows ordinary runner checked 73 packages: 12,300 cases passed
  and 93 were skipped. The Windows race runner completed all packages before
  `spot`, then failed `TestCWSpeedDisplayNeverTruncatesLabel`: its search for
  `22` also matched the UTC time `1922Z`. The same test passed 30 consecutive
  runs after the minute changed. A race rerun of `spot` and the six remaining
  packages passed 3,621 cases with four skips. This is a known unrelated test
  failure; the first full race invocation was not clean.
- WSL ordinary and race checks passed for the shared logger, file coordinator,
  peer diagnostics, propagation reports, UI, cluster and peer packages. The
  tagged real-dashboard integration passed ordinary and race checks on Windows
  and WSL. An initial WSL attempt placed Linux file fixtures on the mounted
  Windows filesystem, where identity checks failed; reruns used Linux-native
  fixture storage. The failed setup also exposed a test-cleanup race, which was
  fixed and retested.
- `go vet ./...`, Staticcheck 2026.2.1, golangci-lint 2.14.0 and the code-map
  check passed. Launcher and isolated-build fixtures passed; release safety
  passed 118 fixtures with one skipped because a second drive was unavailable.
  The PC92 qualification contract and finalizer fixtures passed 63 and 47
  cases respectively. These synthetic fixtures do not establish the open
  480 MiB production qualification.
