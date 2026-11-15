# Private flood evidence and offline comparison

The `flood_replay` tool imports an existing capture and compares hypothetical
reporting-station allowances. It does not connect to peers, change the server
configuration, or select operational limits. The replenishing allowance is an
offline experiment; it is not selectable by the server.

## Build and exercise the generated examples

From the repository root in PowerShell:

```powershell
go build -buildvcs=true -o .tmp/flood-v2-validation/flood_replay.exe ./cmd/flood_replay
./.tmp/flood-v2-validation/flood_replay.exe synthetic -output .tmp/flood-v2-validation/generated-example
./.tmp/flood-v2-validation/flood_replay.exe compare -input .tmp/flood-v2-validation/generated-example -threshold 2 -window-seconds 10 -actor-cap 8
```

Output directories and optional comparison output files must be new. The
example's T=2, W=10 seconds and actor cap 8 are small test parameters, not
recommended human posting limits. Generated events cover bursts, continued
excess, the exact window cutoff, idle and slower recovery, known copies, and
independently genuine repeats with identical observable fields.

Generated reports include fixed phase summaries and first resumed-post times;
the phase labels come from the generator. Real traffic is not classified into
legitimate/flood phases automatically.

The baseline calls the actual flood controller in an explicitly hypothetical
DECALL-only suppress scenario with a synchronous accepting sink. The other
rails and downstream pressure are excluded from this experiment. The rolling
window includes its exact cutoff and charges refused attempts. The candidate
starts full, permits burst T, replenishes at T/W, and debits successful
allowances only. Both retain separate source partitions, native exemptions,
per-source actor caps and fail-open capacity overflow. Neither runs periodic
actor cleanup; this is a finite workload comparison, not a live cardinality or
whole-process resource qualification.

## Collector dependency and lab check

The existing single-session Python recorder is owned outside this branch:

`C:\src\gocluster-cc-user\scripts\telnet-recording-proxy.py`

Required SHA-256:

`274A81A5D1CCBB1292D8EBDEAF7ACB5FCF23ACC07920508AA0E427EDA6A9B490`

It is not included in this branch or automatically deployed. A private ignored
snapshot of that exact file is required for the lab and later collection.
Verify source-before, snapshot and source-after hashes; do not edit the other
agent's file. A different hash requires a fresh review rather than silently
accepting a changed collector.

The required lab check uses only owned loopback endpoints, no real login or
external node. It checks the recorder exit status and half-close, exact receiver
bytes, footer counts, PC11/61/26 conversion, duplicated arrivals, and the
comparison. Python must be available on PATH.

```powershell
$env:GOCLUSTER_FLOOD_RECORDER_PATH = "$PWD/.tmp/flood-v2-validation/recorder/telnet-recording-proxy.py"
$env:GOCLUSTER_FLOOD_LAB_DIR = "$PWD/.tmp/flood-v2-validation"
./scripts/test-windows.ps1 -Packages ./internal/floodcorpus -Run '^TestRecorderLoopbackCaptureToComparison$' -RequireCases 'dxcluster/internal/floodcorpus::TestRecorderLoopbackCaptureToComparison'
```

Without the recorder variable the lab test reports SKIP; that is not collection
evidence. The caller must explicitly require the case as above. Raw lab files
remain private and ignored when the lab directory variable is used.

## Later collection from an approved peer connection

Collection is a separate operator action after the production host and peer
connection have been approved. This runbook does not provision production,
create peers or connect to an external host.

The collector listens on loopback for one client, then connects to one supplied
numeric IPv4 address and port. For an outbound cluster peer, the cluster is the
client and the approved remote peer is upstream; `s2c` is the cluster-directed
input. Keep the real expected remote callsign and existing peer protocol
settings. This arrangement needs to be included in the operator's peer setup;
the tool does not rewrite that configuration. It sends no generated login or
maintenance commands and forwards the client's bytes unchanged.

Use a new local private capture file and the hash-verified recorder. Its default
limits are 600 seconds and 32 MiB of journal output. It has one session, fixed
pending blocks per direction, and no reconnect or rotation. Synchronous disk
recording can slow the connection; this is not passive timing telemetry.

A persistent connection normally reaches a time/size limit and produces a
**partial** capture. Normal complete EOF requires both directions to end and
drain before the limit, with a successful recorder exit. Ctrl-C or a time/size
limit is not a complete session. Preserve the footer and exit status. A missing,
corrupt or gapped footer cannot be imported as valid evidence.

Import a saved journal, explicitly naming the cluster-directed direction and a
corpus-local transport alias:

```powershell
./.tmp/flood-v2-validation/flood_replay.exe import -input .tmp/flood-v2-validation/peer-capture.jsonl -output .tmp/flood-v2-validation/peer-corpus -direction s2c -transport peer-A
```

The importer validates the whole journal first, then rehashes a second pass.
Only the selected direction's locally send-accepted prefix is decoded. Recorded
but unsent trailing bytes remain in the raw journal and are counted in the
manifest; they are not replayed as cluster arrivals. Send acceptance does not
prove the cluster consumed those bytes. A manifest is written last, after the
event file closes successfully; failed imports do not publish a complete run.

## Interpret the evidence

- Raw capture, converted events and independent labels are private. They can
  contain login credentials, callsigns, IPs and comments. Keep them out of Git
  and public reports. Console/comparison output uses aggregate fields only.
- Events retain the decoded native sentence as base64, including byte-sensitive
  original fields. This is after Telnet/framing removal; the raw journal is the
  transport-byte evidence. Equality never supplies a copy label.
- Receipt nanoseconds are the last recorder read contributing to the sentence.
  Frames in one read share time. Selector behavior, disk stalls and chunking
  affect timing; separate captures do not establish an exact global arrival
  order. Claimed wire time is a different field.
- The private manifest preserves the recorder's starting UTC anchor and final
  observation offset. Those identify the recorded interval with the same clock
  caveats; they do not create exact clock alignment between connections.
- The importer checks original peer fields with native code and age filtering
  disabled. It does not reproduce live handshake, CTY, license or age admission.
  Replay therefore assumes these arrivals would be eligible. Live validation
  exclusions are needed before calibrating real flood charges.
- Limits are 32 KiB per recorder record, 16 KiB per captured data block, 256 KiB
  per event, 256 MiB per input/event file, and 100,000 events. Oversize/corrupt
  inputs fail rather than becoming silently truncated complete data.
- Comparisons reject incomplete captures by default. An explicit
  `-allow-partial` enables a labelled sensitivity experiment; it does not make
  the capture calibration-ready. No interior-window extractor is implied.
- Implementation revision and dirty status come from embedded Go build
  information. Missing provenance is reported as unavailable/null, never clean.
- Comparison output includes the exact event-file SHA-256. Retain its private
  manifest and raw journal with the report to preserve timing and capture
  provenance; a report alone is not the complete evidence unit.
- Independently supplied `known_post`/`known_copy`, logical ID and provenance
  labels can enable the synthetic oracle. It charges the first observed logical
  post once within each trusted source partition, without selecting a practical
  equivalence rule. Recovery counts compare logical delivery, not merely copy
  volume. Unknown truth yields null harm/ambiguity fields.

No algorithm winner, suitable threshold, live deployment, remote compatibility,
or production CPU/latency ceiling follows from these experiments.
