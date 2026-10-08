# Live telnet command regression

`scripts/test-telnet-commands.py` replaces the ad hoc October 3, 2026 live
preset test (38 commands, conversation `01a10480-fa10-76c3-ad00-6374493f3d99`).
The original evidence remains in `.tmp/preset-live-current.log` and `.json`.
The current harness covers that preset sequence and the subsequent command
families found in current dispatch, HELP, source tests and ADR-0243 through
ADR-0261, plus history BAND/MODE selections. The current harness requires
schema 4 to snapshot and restore comment preferences, including hidden-field
preservation checks for older schemas.

Run only against an owner-authorized server. It creates numeric SSIDs under the
supplied base call, changes their preferences, creates/deletes uniquely named
presets, reconnects, sends two short labeled human spots and deliberately closes
malformed/oversized/incomplete/expired uploads. Those spots may be admitted,
filtered or deduplicated by the live pipeline. Their archive lifetime belongs to
the server; this script does not remove archived records.

## Setup and execution

Python 3.14 no longer supplies the old test's `telnetlib`. The harness uses a
bounded standard-library socket client and PyYAML 6.0.3 in an isolated environment:

```powershell
python -m venv .tmp/telnet-test-venv
.tmp/telnet-test-venv/Scripts/python.exe -m pip install PyYAML==6.0.3
$env:PYTHONDONTWRITEBYTECODE = '1'
.tmp/telnet-test-venv/Scripts/python.exe scripts/test_telnet_commands_test.py
.tmp/telnet-test-venv/Scripts/python.exe -u scripts/test-telnet-commands.py `
  --host dxc.n2wq.com --port 8300 --call VA3UXA `
  --output .tmp/telnet-command-new-run
```

The output directory must not exist. Use ordinary Python; optimized Python
(`-O`, `-OO`, `PYTHONOPTIMIZE`) is refused because it disables assertions.
Default command timeout is 12 seconds; the test budget is 900 seconds. Upload
expiry explicitly waits up to 36 seconds. Cleanup has a separate 120-second
budget. Connection buffers and per-response recovery are capped at 262,144
bytes; strict application responses still fail above 65,536 bytes.

## Coverage and evidence

| Contract | Live stimulus and falsifiable observation | Deterministic supplement |
|---|---|---|
| Telnet framing and correlation | Split-capable IAC decoding; complete YAML markers, resource/version/request ID, raw CRLF and final byte count | Offline split, EOF, timeout, malformed marker, duplicate-key, identity and echo fixtures |
| Human readbacks | Both dialects; overview, FULL, 27 categories and CONF/PC93 aliases; headings, representative values, 78 printable-ASCII columns, delivery hold | Existing configuration human/readback tests |
| Human filter commands | PASS/REJECT and CC mutations; barrier GET verifies maps, lists and toggles; mixed-invalid lists preserve configuration/revision | State, MINSNR, canonical DXCC and GRID2 package tests |
| Settings and diagnostics | GRID, NOISE, PATHSAMPLES, SOLAR, DEDUPE, DIAG, DIALECT; exact configuration/status after commands | Existing command/settings tests |
| Pause | Default, 1/300 boundaries, invalid durations, SHOW HOLD, RESUME; state and remaining duration | Existing manual/delivery-timed pause tests |
| Machine schema versions | All GET resources in schemas 1/2/3/4; old shapes and hidden-field preservation | State/MINSNR/comment projection and transaction fixtures |
| Comment rules/search | Literal punctuation/repeated spaces, idempotent additions, opposite moves, removal/clears, 64/65-byte phrases; labeled archive query | Matching truth tables, 32/33-entry limits, snapshots, concurrent invalidation, storage and parser fuzz fixtures |
| PUT/PATCH | Unchanged and changed PUT for every writable resource; PATCH collection replacement and omission preservation; fresh revisions | Existing machine transaction/persistence-failure tests |
| Validation and conflicts | VALIDATE changes neither revision nor configuration; stale writes and malformed values reject atomically | Existing machine schema/transaction tests |
| Presets | Case normalization, ordered listing, cross-SSID sharing, exact new-field round trips, overwrite, invalid inputs, CC/NEARBY and reconnect, association/modified state, deletion without preference changes | Existing preset ownership, disk failure and transaction tests |
| History | Exact-call rows; singleton/list BAND/MODE, category AND, COMMENT-last punctuation, MODE UNKNOWN, saved-filter narrowing; selected NEXT and invalid-request preservation; count bounds and GO/CC aliases | Commands/telnet history tests cover matching before counting, parser/fuzz boundaries, immutable selections, self exceptions, cutoff/work limits and concurrent invalidation; offline harness fixtures reject ignored categories, union matching, preference mutation and lost selections |
| Terminal uploads | Bad header, over-limit body, missing end marker and absolute expiry; FIN or RST required; reconnect configuration unchanged | Existing terminal framing/deadline/session tests |
| General commands | HELP/H, BUILD, OWN, DXCC, WHOSPOTSME, PROP, DX syntax and BYE/QUIT/EXIT | Existing processor/help tests |

Every human command is followed by a uniquely correlated GET barrier, so the
collector does not stop merely because a substring appeared in a partial
response. Negotiated input echo is removed once and cannot satisfy response
assertions. YAML and human CRCRLF anomalies are recorded as failures before
tolerant semantic decoding; they cannot produce a clean exit.

Command acknowledgement and readback establish parsing and configuration
continuity. They do not alone prove live spot filtering, scientific prediction,
latency, throughput or delivery under overload. Local state/SNR/history fixtures
provide the deterministic evidence for those filtering contracts; no performance
or scientific-model improvement is claimed. A missing usable live history page
is a failed coverage condition, not an empty-history PASS.

The same two labeled DX stimuli establish history-selection evidence: 10m/CW
and 15m/FT8, with explicit mode tokens and exact per-run labels. Their target is
CTY-valid and differs from the authorized base call (K1ABC for VA3UXA tests,
otherwise VA3UXA), so history's existing self exception cannot hide saved-filter
failures. Both labeled archive rows must be present; admission/deduplication
that loses either row fails coverage. Tests compare the exact callsign,
frequency and label identities, rather than acknowledgements or nonempty
output. No additional DX stimuli are sent.

Raw ingress has two distinct contracts: ordinary DX input retains the command
reader's safe character list, while recognized COMMENT prefixes admit printable
ASCII in their phrase. Submitted DX fixtures therefore use `up-5?`; `:` and `!`
would be rejected before DX dispatch. The matching history query uses `up-5?`,
and its negative punctuation query uses `up-5!`, which is legal after SHOW
COMMENT but differs from the archived phrase. Collector-only fixtures bypass
raw ingress; actual Go reader fixtures validate both emitted DX command orders.

Selections accept comma/space lists, OR within each list and AND across BAND,
MODE and COMMENT. Crossed 10m/FT8 selection and a mismatched literal punctuation
suffix must return an exhausted empty search. Saved band/mode blocks must hide
otherwise selected rows, and each search must preserve configuration/revision.
A count-one combined selection supplies a NEXT token; missing/invalid/repeated
categories and ALL/NONE are rejected before that token returns the remaining
labeled row. MODE UNKNOWN is valid; BAND UNKNOWN is unsupported and rejected.
GO rejects slash aliases; accepted aliases in GO/CC return the
same exact selected identities. Offline synthetic-session defects prove the
collector rejects false-green responses; they do not prove server behavior.
These exact tests require the selected labeled rows within the page work
budget and exhausted status for empty searches. A timeout, unreadable-record
warning or incomplete page instead fails coverage; the harness does not treat
that bounded-search limitation as proof that no matching records exist.

## Ownership, restoration and output

The main callsign is used only for GET and BUILD discovery. Its persisted
preferences are checked unchanged. Ordinary login bookkeeping still occurs.
Mutation uses numeric SSIDs. The harness refuses protected temporary defaults or
an existing preset association before modifying a test profile. It snapshots
each profile's exact schema-4 configuration before changes and saves snapshots
to `baselines.json`.

Cleanup runs in `finally`, closes active test sockets and uses fresh connections
to PUT the captured configuration, restore diagnostics, RESUME, delete only
test-owned names and verify configuration after another reconnect. Preset names
are checked for collisions/capacity before SAVE and reserved for cleanup before
sending: a lost acknowledgement may still have committed a preset. Restoration
failure is reported separately and causes nonzero exit. Hard process termination
or network loss can prevent cleanup; use the snapshot and ownership list to
recover rather than assuming cleanup succeeded.

The remote command API cannot delete user records or clear an applied preset's
reference. Test SSID records and their deleted-preset associations can remain,
even after exact writable configuration is restored. These are reported as
residual test profiles; no main-account preset association is changed.

`transcript.bin` retains Telnet-decoded application bytes before newline
normalization, with case/callsign labels. It excludes the Telnet negotiation
control bytes. `results.json` records remote BUILD identity, semantic cases,
wire failures, cleanup failures, test names and residual profiles. Its command
count includes GET barriers and cleanup, so it is not comparable directly with
the original 38-command count. Any semantic, wire or cleanup failure, or remaining
owned preset, produces nonzero exit.

The remote BUILD display is evidence of the observed deployment, not proof that
the local checkout's commit ran. Local tests validate the checkout separately.
Raw transcripts may contain server metadata; keep them local when sharing only
the relevant failure excerpts is sufficient.

The entrypoint keeps transport, the command matrix and cleanup together so the
whole authorized mutation/restoration sequence can be reviewed in one place.
Its length comes from explicit command cases; it adds no general testing framework.
This document owns harness prerequisites and observed evidence. Operator/support
documentation and the durable decision for runtime BAND/MODE commands belong to
the accompanying implementation.

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
selections; the extended harness requires a separate owner-authorized live run
to establish deployed behavior. No live execution is implied by offline
collector fixtures or local Go tests.
