# Live telnet command regression

`scripts/test-telnet-commands.py` replaces the ad hoc October 3, 2026 live
preset test (38 commands, conversation `01a10480-fa10-76c3-ad00-6374493f3d99`).
The original evidence remains in `.tmp/preset-live-current.log` and `.json`.
The current harness covers that preset sequence and the subsequent command
families found in current dispatch, HELP, source tests and ADR-0243 through
ADR-0261. The current harness requires schema 4 to snapshot and restore comment
preferences, including hidden-field preservation checks for older schemas.

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
| History | Exact-call rows, retained-history NEXT, invalid tokens and replay, count bounds and GO/CC aliases | Existing archive and commands/telnet history tests cover cutoff, work limits, state/SNR matching and concurrent invalidation |
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
Support-agent documentation impact: not required; this is a developer test tool
and changes no supported command behavior. No architecture decision changed.

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
