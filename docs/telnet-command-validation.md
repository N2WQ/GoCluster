# Live telnet command regression

`scripts/test-telnet-commands.py` tests a running, owner-authorized cluster.
It uses schema 7 to capture both GO and CC filter banks and shared settings,
and checks the captured configuration after restoration and reconnect. The
legacy transport classes are also used by the isolated command-fix runner.

## Setup and execution

Use Python with PyYAML 6.0.3. The output directory must not exist. Choose two
adjacent numeric SSIDs that are not active; an existing login with the same
identity may be displaced. Check existing profile ownership before choosing
SSIDs. Login may migrate a base call's old preset library. Test profiles and
preset associations can remain after cleanup, and labeled test spots can remain
in the archive.

```powershell
python -m venv .tmp/telnet-test-venv
.tmp/telnet-test-venv/Scripts/python.exe -m pip install PyYAML==6.0.3
.tmp/telnet-test-venv/Scripts/python.exe -B scripts/test_telnet_commands_test.py
.tmp/telnet-test-venv/Scripts/python.exe -B -u scripts/test-telnet-commands.py `
  --host 127.0.0.1 --port 8300 --call LZ1XA --ssid-start 78 `
  --history-wait-seconds 65 --history-timeout-seconds 120 `
  --cc-shortcuts CW,RTTY,FT8,FT4,FT2,PSK `
  --output .tmp/telnet-command-new-run
```

Use ordinary Python; optimized mode is refused because it disables assertions.
The optional history wait allows configured new-call probation to expire before
posting; a rejected spot blocks history coverage and is not counted as a pass.
The separate history timeout allows held spots to reach the archive. The suite
follows work-limited history pages within a bounded time and page budget and
requires exact rows or exhausted status. Supply
`--cc-shortcuts` from the running server’s loaded taxonomy to check each
configured mode shortcut; without it, shortcut coverage is representative.
The suite bounds socket buffers and deadlines, records wire failures separately,
and uses a fresh connection and separate cleanup budget for restoration.

## Coverage and evidence

The required inventory in the script covers representative GO numbered rules,
NEARBY and retired syntax; native CC geography, DXBM, modes, sources and
controls; dialect switching; settings; HELP and read commands; history; presets;
schema 6 compatibility and schema 7 reads and writes; malformed uploads; and
logout. Missing or interrupted required cases are reported as unexecuted, not
as passes. Individual command cases are representative and do not establish
all combinations or live spot-filtering behavior.

A correlated machine readback follows human commands. Tests check saved state
and revisions where applicable; an acknowledgement alone is not evidence that
an edit took effect. Offline fixtures exercise malformed frames, echo, identity,
two-bank restoration, cleanup errors and false-success responses. Observed
live results and their limitations are in the output directory.

## Ownership, restoration and output

The suite refuses incomplete schema 7 backups, temporary defaults, and a
pre-existing preset association in either bank. It saves the two-bank snapshots
in `baselines.json`. Cleanup restores the captured writable configuration,
restores diagnostics, ends the test pause, deletes only test-owned preset names, and
checks configuration after reconnect. Any cleanup error is reported separately
and causes a failing exit. A lost connection or process termination may prevent
cleanup; inspect the saved baseline and result before retrying.

CONFIG uploads do not restore preset association references. A test-owned
profile may therefore retain a reference to a deleted test preset. Results
record residual profiles and associations; do not treat configuration equality
as restoration of every profile field. Login bookkeeping and migration effects
also remain outside the writable configuration backup.

`transcript.bin` contains decoded application bytes, and `results.json` records
required cases, failures, wire errors, cleanup outcomes, and test identities.
A live pass does not prove production latency, scientific accuracy, filtering
recall, or delivery under load.

## COMMENT CPU and literal-input regression

[TSR-0046](troubleshooting/TSR-0046-comment-matching-cpu-and-history-input.md)
records the repeated-prefix CPU defect and generic history phrase trimming.
Capture the old baseline after adding fixtures but before production edits;
the invalid-ending goldens should fail on old code. `-run '^$'` keeps those
expected failures out of benchmark runs. Repeat the same benchmark fixtures
and toolchain/runtime settings on corrected code:

```powershell
go test ./filter ./commands ./telnet -run '^$' -bench 'Comment' -benchmem -benchtime=100ms -count=7
go test ./filter ./commands ./telnet ./peer -run 'Comment|TestGenericHistoryComment' -count=1
go test ./filter -run '^$' -fuzz '^FuzzMatchCommentPhrase$' -fuzztime=30s -parallel=4
go test ./commands -run '^$' -fuzz '^FuzzHistoryCommentRemainder$' -fuzztime=30s -parallel=4
```

Include both repeated-prefix/suffix and periodic negatives, overlapping and
final-position positives, phrase lengths 1/63/64, arbitrary stored comment
bytes, both maximum lists and cheap band/mode/archive-identity rejection.
65,500 bytes is a synthetic matcher/fanout stress size. The peer fixture derives
the actual maximum stored comment from each frame family's 65,536-byte envelope
and checks that the full comment and tail phrase survive production admission.

Require zero matcher/normalized-filter allocations and at least 10x improvement
for the specified 32-reject repeated-prefix cases at 1,024/65,500 bytes. Existing
passing/no-COMMENT fixtures allow at most 25% regression. Separately report
absolute old/new times for a first REJECT match and short PASS miss: deferring
COMMENT adds ordinary-filter work, with the new total bounded by 1.25 times
the measured new ordinary-only plus isolated-matcher component sum. A missed
budget requires disposition rather than quietly weakening the criterion.

Capture separate old/new fanout CPU profiles for one and eight clients. This
example records the corrected eight-client case; use the matching filenames
and fixture suffix for the other cases:

```powershell
go test ./telnet -run '^$' -bench '^BenchmarkCommentFanout/clients-8$' -benchmem -benchtime=5s -count=1 -cpuprofile=.tmp/comment-v2/new-fanout-8.pprof -o .tmp/comment-v2/new-telnet.test.exe
go tool pprof -top -cum .tmp/comment-v2/new-telnet.test.exe .tmp/comment-v2/new-fanout-8.pprof
```

Create the output directory first. The fanout guard checks exact delivery count,
spot identity, empty backlog and unchanged drops, with a separate self-bypass
control. Its logs include completed calls/envelopes from calibration runs for
normalizing whole-profile CPU samples. Compare absolute operation time and
caller/matcher cumulative CPU per completed call; CPU percentages alone cannot
establish success. Delivery envelopes retain their existing allocations.
Record platform, toolchain, runtime settings, sample counts and budget arithmetic.

These local fixtures/profile checks support the correction, not production
latency guarantees. Production acceptance needs representative steady/burst
runtime profiling, admission latency and filter-writer wait measurements,
alongside drop/backlog checks against the deployment's capacity budget.

## Observed validation

On October 29, 2026 (America/New_York), the final expanded live run
`.tmp/telnet-v7-lz1xa-08` completed 1,318 commands and passed all 100 required
groups, with no wire or cleanup errors. It covered both dialects, the six
configured CC mode shortcuts, schema 6 and 7 writes, preset overwrite, and
history continuation across work-limited pages. The writable configurations of
LZ1XA-82 and LZ1XA-83 were restored after reconnect. Test preset associations
remain on those profiles; labeled test spots may remain in the archive.

On October 29, 2026 (America/New_York), `.tmp/telnet-v7-lz1bb-04`
recorded 1,273 commands and 93 passing required groups, with no failed,
blocked, unexecuted, wire or cleanup results. This includes both labeled
history rows, both dialects, and the six configured CC mode shortcuts. The
writable configurations of LZ1BB-97 and LZ1BB-98 were restored and verified
after reconnect; references to the deleted test preset remain on those profiles.

On October 29, 2026 (America/New_York), `.tmp/telnet-v7-lz1bb-03`
recorded 49 passing required groups, one blocked history prerequisite, one
unexecuted dependent group, and no wire or cleanup errors. Both test spots
were queued, but the history query did not establish both labeled rows. The
run used LZ1BB-94 and LZ1BB-95; their writable configurations were restored,
and test preset references remain on those profiles. Subsequent history-mode
setup changes are not qualified by that run.

On October 7, 2026 (America/New_York), the final complete run against
`dxc.n2wq.com:8300` reported release `261008r4c40`, build version `261008`,
Go `go1.27.1`. It executed 792 commands including barriers and cleanup, with
305 passing semantic cases, zero wire failures and zero cleanup failures.
Evidence is in `.tmp/telnet-command-run-3/results.json`, `baselines.json` and
`transcript.bin`. The two test profiles were `VA3UXA-74368` and `VA3UXA-74369`;
their writable configuration was restored and verified after reconnect. Owned
presets were deleted. Test profiles/deleted-preset references and admitted test
spots may remain remotely. Earlier attempts also restored configuration and
removed owned presets; their profile records were `VA3UXA-35024/35025` and
`VA3UXA-82388/82389`.

Earlier discovery observed CRCRLF; it did not recur in the final remote run.
The harness records that condition as a failure if encountered again. Initial
runs corrected checker assumptions about already-selected dialect replies,
early YAML-error request IDs, echo-equal-to-response handling, DXBM grammar,
RST closure and using retained rows for history paging. None required a
production change. Raw evidence from those runs remains in separate directories.

Local `go test ./telnet ./commands ./filter ./archive -count=1 -timeout=180s`
passed on checkout `2948da4cbee8b63bd1651272ec3f071f1c216ef7`. These are relevant
package tests, not a claim that the full repository suite or that exact commit
ran on the remote server. Offline Python fixtures cover transport, echo,
correlation and failure cleanup. The script-only lane also checks syntax and
diff whitespace; code-map generation/check is run after documentation updates.

That October 7 live run predates the COMMENT/schema-4 feature and does not
validate its behavior or performance. It also predates history BAND/MODE
selections; it does not qualify the newer schema 7 harness or its live behavior. No live execution is implied by offline
collector fixtures or local Go tests.

## Local command-fix regression runner

`scripts/test-command-fixes.py` builds this checkout and starts isolated local
servers. It requires no external cluster or manual server setup. Run with the
pinned test environment:

```powershell
.tmp/telnet-test-venv/Scripts/python.exe -B -m unittest discover -s scripts -p test_command_fixes_test.py
.tmp/telnet-test-venv/Scripts/python.exe -B scripts/test-command-fixes.py
.tmp/telnet-test-venv/Scripts/python.exe -B scripts/test-command-fixes.py --race
```

The runner requires native Windows and uses the existing stable telnet test executable and exclusive
runner lock. It refuses optimized Python, cross compilation, output junctions,
stale builds, missing required cases, framing errors and incomplete cleanup.
Each run retains source/config identity, results, child logs and socket transcripts
under `.tmp/command-fixes`. Both telnet transports and both dialects are covered;
fresh child processes verify saved preferences and actual archive data.

The main profiles load private MODE/EVENT names and a mode alias containing
underscores. Human commands reach the real reader, canonical publication,
immediate saved records and archive filtering; fresh children restore the same
rules. Seeded spots carry explicit mode/event metadata. This qualifies command
and filter behavior, not event inference or model quality. Reader controls keep
login and recognized machine headers restricted, with literal editing, echo
and byte-boundary checks. They do not qualify early machine-error wire framing.

The same private taxonomy maps COMMENT to CW. Actual count-one history queries
distinguish the mode alias from a later literal COMMENT clause, reject newer
wrong-mode and wrong-spacing rows, preserve continuation after refused input,
and continue to the second matching row. Complete CRLF pages, tokens and a
correlated configuration barrier prevent hidden suffixes from passing. Reader
and history-owner controls also cover absent/hidden aliases, editing and exact
cursor/settings/disk preservation. This is local command evidence, not a
production archive or model-quality qualification.

CC profiles read complete HELP SET/FILTER, HELP UNSET/FILTER and HELP FILTERS
responses through the automatic read-pause footer, then require a correlated
configuration barrier. Independent category and grammar assertions reject
unsupported COMMENT aliases and blanket inline flags. Advertised commands
check intended live settings and immediate stored rules; refused forms preserve
configuration, revision, preset and exact stored bytes. Fresh children restore
the accepted settings before repeating the profile. Composed Go controls cover
all 18 ordinary CC list families and special owners; these are local checkout
checks, not evidence about a deployed server.

Both dialects also exercise independently inventoried HELP aliases and argument
topics, including SH forms, settings, presets, framed YAML, shared CC fallback
and loaded shortcut modes. Canonical headers, usage and visible aliases must
agree; prefix collisions and unsupported heads must stay unknown. Each response
is consumed through a source-verified terminal and its conditional pause footer
before a strict correlated CONFIG barrier. Live configuration, revision, preset
association and exact stored bytes stay unchanged, with fresh children proving
saved ownership. Isolated Go taxonomy controls own fixed-shortcut overlap and
disabled-mode vectors; this does not qualify every deployment vocabulary.

Filter discovery cases check an independent 29-family inventory, action-aware
bare/unknown replies, shared DXBM guidance and complete state usage in both
dialects. Short feedback stays below the fixture's automatic-pause threshold;
each response precedes a strict CONFIG barrier. Refusals preserve preferences,
revision, runtime readbacks, preset ownership, exact bytes and file identity,
mtime and size. Composed Go owner checks additionally prove zero commit calls;
the actual fixture does not expose that internal count. Complete HELP receipts
check DX intake/admission wording, all 73 mailing codes and MODE-specific rules.
Fresh children repeat the saved-state controls. These checks do not establish
station location, spot delivery or production behavior.

SHOW HOLD cases require whole tokens, exactly two tokens, case and repeated
ASCII spaces. Extra arguments receive complete usage; nearby heads and SH/slash
aliases stay unsupported. Both dialects preserve full saved settings, preset
association, revision, runtime projections and file identity/mtime/size. An
admitted archived spot produces one actual suppressed count, then RESUME permits
a distinct fresh live row without replay. Fresh children repeat the checks.
Public remaining seconds round up; measured send/receive intervals constrain
the countdown without claiming exact deadline, cutoff or epoch visibility.
Go controls own those exact state checks, TAB refusal on both real codecs,
threshold-one automatic-pause bypass and gated writer completion. The main
checkout fixture retains threshold ten and does not establish bypass alone.

Coverage grows with each approved fix. The initial fixture does not certify
production dedupe, classifier or runtime shutdown. See the
[execution record](command-fix-execution.md) and
[approved v3 scope](command-fix-scope-ledger-v3.md).
