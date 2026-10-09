# GoCluster Operator Guide

This guide is for running a GoCluster node and connecting to it as a telnet
DX-cluster user. For implementation details, use the package READMEs linked
from the repository root.

## Get A Binary

The current ready-to-run release asset is Windows amd64:

```text
https://github.com/N2WQ/GoCluster/releases/latest
gocluster-windows-amd64.zip
```

Download that asset, not GitHub's automatic source-code archives. Extract it
and open `ready_to_run/`.

Linux operators currently build from source:

```sh
GOOS=linux GOARCH=amd64 go build -trimpath -o gocluster .
GOOS=linux GOARCH=amd64 go build -trimpath -o peerdiag ./cmd/peerdiag
```

## Configure A Real Node

The packaged and checked-in `data/config` directory is public example config.
For a real node:

1. Copy the whole directory to a private complete directory, for example
   `data/config.local`.
2. Edit the private copy.
3. Start the server with `DXC_CONFIG_PATH` pointing at that directory.

Review normal deployment/runtime files before first run:

- `app.yaml`: server node ID, `headless` or `tview-v2` local UI mode, and logging paths.
- `runtime.yaml`: telnet port, default filters, buffers, and Go runtime controls.
- `ingest.yaml`: RBN, PSKReporter, DXSummit, and human/manual ingest settings.
- `peering.yaml`: only if this node connects to peer clusters.
- `reputation.yaml`: only if IPinfo/Cymru reputation enrichment is enabled.
- `solarweather.yaml`: only if solar/geomagnetic path overrides are enabled.
- `data.yaml`: CTY, FCC, ISED, H3, skew, and runtime data paths.
- `spot_taxonomy.yaml`: only when changing supported modes, events, or
  PSKReporter mode routing.

Do not retune `pipeline.yaml`, path thresholds, solar override gates, or
mode-inference calibration as normal setup. Use `data/config/README.md` for the
ownership class before editing a YAML file.

Keep real callsigns, peer hosts/IPs, passwords, and service tokens out of the
public example config and out of shared archives.

At minimum, replace the public placeholder identity before connecting a real
node: change `server.node_id` in `app.yaml` from `N0CALL-1`, change the RBN
login callsigns in `ingest.yaml` from `N0CALL-1`, and update any private
upstream telnet `host` and login fields you enable. If peering is enabled,
also replace peer hosts, login callsigns, and passwords in `peering.yaml`.

For human/upstream telnet ingest, use the ordered `human_telnet` list in
`ingest.yaml`. You may define zero to 64 complete entries. Keep each name
unique without regard to case, and keep every field present even on disabled
entries. Each enabled entry owns an independent connection and bounded retry
lifecycle, so an unavailable server stays red and retrying without holding up
the other upstreams. The older single-map shape still loads as one entry.

## Run On Windows

From the extracted `ready_to_run` directory:

```pwsh
$env:DXC_CONFIG_PATH = "data/config.local"
.\gocluster.exe
```

From a source checkout:

```pwsh
$env:DXC_CONFIG_PATH = "data/config.local"
go run .
```

To compile from source on Windows:

```pwsh
go test ./...
go build -trimpath -o gocluster.exe .
go build -trimpath -o peerdiag.exe ./cmd/peerdiag
```

## Run On Linux

Build from the repository root with Go `1.26+`:

```sh
go test ./...
GOOS=linux GOARCH=amd64 go build -trimpath -o gocluster .
GOOS=linux GOARCH=amd64 go build -trimpath -o peerdiag ./cmd/peerdiag
```

Install both executables and the required runtime data together, for example under
`/opt/gocluster`. Keep a complete private config directory at a stable path
such as `/opt/gocluster/data/config.local`.

Runtime data commonly needed beside the binary includes `data/cty`, `data/h3`,
`data/peers/topology.db`, and `data/skm_correction/rbnskew.json` when those
inputs are used by your config.

For unattended service operation, set `ui.mode: headless` in the private
`app.yaml`.

Create the service account, install directory, binary, config, and runtime
data, then assign ownership to the service user:

```sh
sudo useradd -r -s /bin/false gocluster
sudo mkdir -p /opt/gocluster
sudo cp gocluster peerdiag /opt/gocluster/
sudo cp -R data /opt/gocluster/
sudo chown -R gocluster:gocluster /opt/gocluster
```

Save this unit file as `/etc/systemd/system/gocluster.service`:

```ini
[Unit]
Description=GoCluster DX Cluster
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=gocluster
Group=gocluster
WorkingDirectory=/opt/gocluster
Environment=DXC_CONFIG_PATH=/opt/gocluster/data/config.local
ExecStart=/opt/gocluster/gocluster
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and inspect the service:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now gocluster
sudo systemctl status gocluster
journalctl -u gocluster -f
```

The interactive local console requires the process to run in a real terminal.
For console inspection, stop the service, edit `app.yaml` in the private config
directory, change `ui.mode` to `tview-v2`, then run the binary manually:

```sh
sudo systemctl stop gocluster
cd /opt/gocluster
DXC_CONFIG_PATH=/opt/gocluster/data/config.local ./gocluster
```

On the Overview page, the Caches & Data Freshness footer includes CTY, FCC, and
skew dates plus `VOACAP SSN: <integer|n/a>`. The integer is the rounded current
SSN generation used for VOACAP forecast cache keys and deck generation; `n/a`
means the runtime has no initialized VOACAP SSN generation.
The Path Predictions panel also shows the same fallback snapshot as
`VOACAP cache: <cache> (C) / <delay> (D) / <inflight> (I) / <queue> (Q)`
before the H3 path-pair counts. These are the existing in-memory fallback cache
entries, delayed lookups, inflight jobs, and queued jobs from the current
process.

VOACAP fallback is a Windows-only runtime feature because it launches the
Windows VOACAP engine. Linux startup skips VOACAP validation and logs that the
fallback is disabled, so the cluster can still run with path reliability,
native 160m fallback, and ordinary p50 predictions.

After inspection, set `ui.mode` back to `headless` before returning to
unattended service mode.

## Connect And Use Commands

Connect to the configured telnet port from `runtime.yaml`:

```text
telnet localhost 8300
```

Log in with your callsign. Useful first commands:

- `HELP`: show commands grouped by task, filter categories and practical examples.
- `HELP <command>`: show command-specific help.
- `HELP PASS` / `HELP REJECT`: show detailed filter syntax, values and examples.
- `HELP FILTERS`: show full filter rules and supported values.
- `HELP SYMBOLS`: explain confidence and configured path reliability symbols.

- `SHOW MYDX` or `SHOW DX`: show filtered spot history.
- `SHOW DXCC <call>`: look up DXCC/ADIF and zones.
- `SHOW PROP <call|prefix|grid> [band] [mode]`: show hourly
  propagation outlook from your grid to a target.
- `SHOW OWN`: show your login call and baseline own call.
- `WHOSPOTSME [band]`: show recent spotter countries for your baseline call.
- `PAUSE [seconds]`: pause live spots for 30 seconds by default, or 1-300 whole seconds.
- `SHOW HOLD`: show manual or automatic read-pause status.
- `RESUME`: end the read pause and resume live spots immediately.
- `SET GRID <grid>`: set your 4-6 character Maidenhead grid.
- `SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL`: set receive noise class.
- `SET PATHSAMPLES <count|DEFAULT>`: require more path samples than the cluster default, or clear your personal override.
- `SET DIAG OFF|DEDUPE|SOURCE|CONF|PATH|MODE`: replace spot comments with compact per-session diagnostics.
- `SET SOLAR 15|30|60|OFF`: opt into or stop periodic solar summaries.
- `DIALECT`, `DIALECT LIST`, `DIALECT <go|cc>`: show or switch command dialect.
- `SHOW FILTER`: show filter selections and restrictions.
- `SHOW FILTER FULL` or `SHOW FILTER <category>`: show complete effective PASS/REJECT selections and ON/OFF switches.
- `SHOW SETTINGS`: show configured choices, effective settings, and session status.
- `PASS <type> <list>`: allow matching spots.
- `REJECT <type> <list>`: block matching spots.
- `RESET FILTER`: restore default filters.
- `SAVE PRESET <name>`: save filters and preferences to a named snapshot.
- `LIST PRESET`: list the snapshots shared by your numeric SSIDs.
- `LOAD PRESET <name>`: load a snapshot and save the current SSID's defaults.
- `DELETE PRESET <name>`: remove a snapshot while keeping current settings.
- `PASS NEARBY ON|OFF`: toggle nearby local-area filtering.
- `SHOW DEDUPE`: show your dedupe policy.
- `SET DEDUPE FAST|MED|SLOW`: change your dedupe policy.
- `DX <freq> <call> <comment>`: post a local human spot.
- `BYE`: disconnect.

In the CC dialect, use `HELP SET/FILTER` and `HELP UNSET/FILTER` for ordinary
list filters. The overview uses CC spellings; COMMENT, MINSNR and NEARBY keep
their shared PASS/REJECT syntax. YAML client commands are omitted from the
overview but remain available, including their command-specific help.

The top-level repository README contains the generated default `HELP` output.
That block is checked against the command processor in tests.

Long command output can temporarily pause live spot lines so users have time to
read the response. The shipped config starts that pause at `10` rendered rows
for `30` seconds. Missed live spots are not buffered or replayed; control
traffic such as bulletins, talks, command replies, errors, and close messages
continues.

Users can also start the same pause with `PAUSE [seconds]`. Manual pausing works
even when automatic read pause is disabled and always defaults to `30` seconds.
A new `PAUSE` sets a fresh duration; automatic pauses can extend an active pause
but cannot shorten it. `SHOW HOLD` reports either kind of pause. Replies to
`PAUSE`, `SHOW HOLD`, and `RESUME` do not trigger another automatic pause.

### Read Your Filters And Settings

`SHOW FILTER` gives an aligned overview of passing selections. BAND, MODE,
SOURCE, EVENT, PATH, CONFIDENCE and DX/DE continents show names and useful
explicit exclusions, wrapping across lines. Unrestricted categories show
`All`; an explicit block can show `All except 80m`. Disabled choices are not
enumerated. Long callsign, DXCC, grid and zone lists use counts. DXCC labels use
unambiguous canonical CTY prefixes in overview, FULL and category views. Shared
entities display all such prefixes: IT9, I and IG9 all refer to ADIF 248.
Conflicting labels are omitted; valid alternatives remain. Rules with no usable
label display `Unknown DXCC (12345)`; YAML retains numeric ADIF keys. Geography rules
and inclusion switches are grouped for reading. For example:

```text
User          N2WQ-1
Preset        CONTEST (modified)

Bands         Only 20m, 40m
Modes         CW, FT8; unknown modes hidden
Sources       All (HUMAN, SKIMMER)
Events        POTA; block WWFF; untagged included
Confidence    All
Path          All
DX geography  Suspended by NEARBY; rules retained
DE geography  Suspended by NEARBY; rules retained
DX calls      Only 12 patterns; 3 blocked
DE calls      All
Nearby        On; grid FN31PR
Include       Beacons: Off | WWV: On | WCY: On | Announce: On
              Self: Off | Toxic: Off

Detailed selections: SHOW FILTER FULL
One category: SHOW FILTER <category>

Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

`SHOW FILTER FULL` lists complete effective selections; `SHOW FILTER BAND` limits the
detail to one category. In the CC dialect, `SHOW/FILTER` and `SH/FILTER`
support the same arguments. An example response to `SHOW FILTER EVENT`
shows effective event selections, including active false-valued keys:

```text
User          N2WQ-1
Preset        CONTEST (modified)

Events
  PASS: POTA, SOTA
  REJECT: WWFF
  Untagged spots are always included.

Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

The overview describes matching behavior; FULL/category shows complete
effective PASS/REJECT selections with ALL/NONE and ON/OFF switches. Ordinary
false entries are omitted; REJECT wins over PASS. Use GET YAML FILTER for stored
flags, false entries and defaults. Ordinary string/integer categories can remain
restrictive with a nonempty allow map even when `allow_all=true`. EVENT instead matches
key presence, including keys stored as `false`, and ignores allow restrictions
when `allow_all=true`. Untagged spots always pass its check. When editing YAML,
remove an EVENT key from its replacement map to remove that rule. Setting the
value to `false` keeps the key present. PATH retains its UNLIKELY/CLOSED rules.

Human readbacks use at most 78 printable ASCII characters per line, followed
by CRLF. Exact strings use quoted ASCII escapes; long values use complete
quoted pieces joined with `+`. Spaces inside quotes are retained, and neither
the marker nor continuation indentation is part of the value. Escapes are not
split. Callsign patterns retain supplied order; map keys use stable ordering.
See [lossless wrapping examples](../telnet/README.md#human-configuration-readbacks).

`SHOW SETTINGS` shows the configured choice and the behavior the server uses.
For example, an empty configured GRID can use a callsign lookup. Active NEARBY
can temporarily use FAST dedupe while the configured choice remains SLOW. A zero
PATHSAMPLES value uses the active station/beacon minimums; zero SOLAR means Off.
Pause and diagnostic status appear under Session only:

```text
User          N2WQ-1
Preset        CONTEST (modified)

Dialect       GO
Grid          FN31PR
Noise         SUBURBAN
Dedupe        SLOW; effective FAST while NEARBY is active
Path samples  DEFAULT; stations 21, beacons 11
Solar         Every 30 minutes

Session only
Diagnostics   Off
Live spots    Paused for reading; at least 30s after delivery
              135 spots suppressed

Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

Effective path minimums reflect restored runtime state. With server floors
of 21/11, reconnect leaves a saved personal minimum of 15 inactive:

```text
Path samples  15 configured; effective stations 21, beacons 11
              Override inactive: not above station minimum 21
```

An active minimum of 30 gives stations 30 and beacons 30. Production output
reads the loaded predictor configuration rather than using these example
numbers, and identifies disabled or unavailable prediction explicitly.
NEARBY readback distinguishes enabled from usable. Enabled NEARBY keeps ordinary
location rules suspended when user cells are unavailable; DX spots on the
affected bands fail NEARBY matching instead of falling back to those rules.

Every human filter/settings readback pauses live spots as soon as the request
is accepted, including time spent preparing and sending the reply. After the
server finishes its write and flush, users get the full configured positive
reading interval, or 30 seconds when that duration is zero. The row threshold,
including zero, does not disable these readback pauses. Other automatic
responses retain the ordinary row threshold and zero-disable settings.

A longer active pause remains in force. A later valid PAUSE or RESUME takes
precedence over a reply still waiting to finish; invalid pause arguments have
no effect. A new human readback starts a fresh reading pause. SHOW HOLD can
report a reply still being delivered with no finite countdown yet. Terminal
rendering time is outside the server's delivery measurement.

Each new readback has a 65,536-byte final response limit, including line
endings and footers. If the complete response does not fit, users receive an
explicit error. A human size error still pauses for reading. Use a smaller
filter category when FULL is too large. A valid larger preset can still LOAD
within the existing 256 KiB preset limit even when FULL or YAML inspection
returns a size error.

### Understand Preset Status And Reconnect Warnings

Successful LOAD or SAVE associates the preset name with the current full login
callsign, including its SSID. The reference is the snapshot successfully
applied or saved. Editing filters or preferences adds `(modified)`; restoring
those values clears it. Callsign pattern order does not count as a change, but
adding or removing an occurrence does. Pauses, diagnostics, and temporary NEARBY dedupe
behavior leave this status unchanged.

Reconnect restores the preferences, preset name, and reference together.
Deleting or overwriting a library preset keeps the session's applied snapshot
and reference. Failed LOAD retains the previous configuration. If SAVE writes
the named snapshot but cannot save its association, the reply says:

```text
Saved preset CONTEST, but could not persist its association for W1ABC-1.
Previous preset association and baseline retained.
```

The named snapshot was saved, and the previous association remains recoverable
after reconnect. Reconnect also continues with a warning when it restores the
configuration but cannot save the new login timestamp/IP.

An unreadable or unsupported saved user record causes a different warning:
the session uses temporary defaults, and its original record stays protected.
Human changes remain temporary. SAVE PRESET is rejected before changing the
preset library, and YAML PUT/PATCH are rejected. Readbacks and YAML validation
remain available. Repair or restore the saved record before reconnecting to
resume normal persistence.

### Configure Through A YAML Client

Clients can use these commands in either dialect:

| Command | Practical use |
| --- | --- |
| `GET YAML FILTER` | Read every exact filter value. |
| `GET YAML SETTINGS` | Read preferences and current behavior. |
| `GET YAML CONFIG` | Read filters and settings together. |
| `GET YAML CAPABILITIES` | Discover fields, available choices, and limits. |
| `PUT YAML FILTER|SETTINGS|CONFIG` | Replace the complete selected resource. |
| `PATCH YAML FILTER|SETTINGS|CONFIG` | Change supplied fields and keep omitted fields. |
| `VALIDATE YAML CONFIG` | Check a complete proposal without changing or saving it. |

GET accepts an optional identifier, for example `GET YAML SETTINGS ID Noise-1`.
Identifiers contain 1-32 ASCII letters, digits, or hyphens, and preserve case.
The server assigns one when it is omitted. One example complete reply is:

```yaml
---
schema_version: 1
request_id: Noise-1
resource: SETTINGS
revision: example-session-0
configuration:
  dialect: ""
  grid: ""
  noise_class: ""
  dedupe_policy: ""
  path_min_observation_count: 0
  solar_summary_minutes: 0
status:
  configured:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
  effective:
    dialect: go
    grid: FN31
    grid_derived: true
    noise_class: QUIET
    dedupe_policy: FAST
    path_min_observation_count: 0
    solar_summary_minutes: 0
    nearby_active: false
  session:
    callsign: W1ABC-1
    diagnostic_comments: "OFF"
    pause_active: false
    pause_pending_delivery: false
    pause_remaining_seconds: 0
    suppressed_spots: 0
    temporary_defaults: false
  server:
    default_dialect: go
    default_dedupe_policy: FAST
    default_noise_class: QUIET
    path_min_observation_count: 0
    auto_read_pause_min_rows: 10
    auto_read_pause_seconds: 30
  preset:
    associated: false
    name: ""
    modified: false
...
```

Copy the writable `configuration` into a request and use the actual revision
returned by GET. FILTER and SETTINGS put their fields directly under
`configuration`; CONFIG contains `filters` and `settings`. The separate
`status` sections are read-only, and the stored preset reference is private.

For example, a complete settings replacement is sent as:

```text
PUT YAML SETTINGS
---
schema_version: 1
request_id: edit-1
if_revision: example-session-0
configuration:
  dialect: "go"
  grid: "FN31"
  noise_class: URBAN
  dedupe_policy: FAST
  path_min_observation_count: 0
  solar_summary_minutes: 0
...
```

PUT requires every writable field in the selected resource. A missing field
returns an error without resetting other settings. PATCH keeps omitted fields:

```text
PATCH YAML SETTINGS
---
schema_version: 1
request_id: edit-2
if_revision: example-session-1
configuration:
  noise_class: RURAL
...
```

A supplied list or rule map replaces that whole list or map. PATCH can change
one rule-set member while preserving the other members. Use CONFIG to change
GRID and its dependent NEARBY filter together. Explicit `false`, zero, empty
default strings, and toggle `DEFAULT` values survive read/edit/write cycles.

Writes validate the resulting configuration, save it atomically, and then
install it. Validation or persistence failure leaves the live and saved
configuration unchanged. A recognized choice that the server has disabled,
such as SLOW dedupe, is rejected. An unchanged successful PUT still repairs
durability if an earlier human change failed to save; its revision stays the
same. Successful replies contain `applied: true` and `persisted: true`.
VALIDATE replies contain `valid: true`, `applied: false`, and `persisted: false`.

PUT/PATCH require the matching GET revision. After an edit conflict, reconnect,
or server restart, GET again before retrying. Pause, diagnostics, and login
timestamps do not change the configuration revision. Ordinary YAML writes keep
the preset reference and may change `(modified)`. If the connection breaks
during a write, reconnect and GET to discover the outcome.

Upload one ordinary YAML document between standalone `---` and `...` lines.
The body limit is 65,536 bytes, counting its received LF or CRLF line endings;
the marker lines are separate. The complete upload must arrive within 30
seconds. Oversized, incomplete, expired, or unreliable framing closes the
connection without applying the proposal. Rejected PUT/PATCH/VALIDATE headers
also close it, so leftover payload cannot execute commands. A fully received
document with invalid syntax or values gets a framed error and keeps the
connection open. Use plain values and collections; anchors, aliases, merge
keys, custom tags, nulls, unknown fields, and duplicate interpreted keys are
rejected.

Every YAML reply is one complete document with CRLF line endings and the same
65,536-byte response limit. New machine writes must leave a complete CONFIG
readback within that limit, including reserved metadata. A small PATCH of an
already oversized human configuration can fail until that configuration is
reduced. A size error contains `error.code: response_too_large` instead of a
truncated success. YAML commands and their errors have no pause effects or
human footers. Existing pauses still suppress ordinary live traffic and can
naturally increase their counters.

See [the telnet protocol guide](../telnet/README.md) for the full field schema,
framing contract, error codes, and limits.

### Login Identity And Diagnostics

Numeric SSIDs on the spotted DX call are removed regardless of ingest source.
For example, a new DX call of `K1ABC-2` is materialized and displayed as
`K1ABC`. Telnet login identity remains the full login call, while manual spots,
`SHOW OWN`, self-spot matching, and `WHOSPOTSME` use the baseline call when a
login has a numeric SSID. Existing archive files are not rewritten; old stored
rows can still contain a numeric SSID.

`SET DIAG MODE` is useful when the displayed mode is surprising. It shows
`<mode>|<provenance>`, where blank modes are shown as `--`. Provenance tokens
are `SRC` source explicit, `CMT` comment explicit, `EVD` recent evidence, `FQ`
digital frequency, `RCW` regional CW default, `RVO` regional voice default,
`RMIX` regional mixed blank, `RUNK` regional unknown blank, and `UNK` unknown.

### Reading `SET DIAG PATH`

`SET DIAG PATH` replaces the normal spot comment with the path-reliability data
used for that spot. The mode/report and fixed tail columns are preserved.

The compact format omits the path class glyph because the normal path
column already shows it when path display is enabled:

```text
n<count>|w<weight>|a<age>
```

When receiver contribution caps reduce the diagnostic evidence, raw selected
count is shown first, capped effective count is shown after `/c`, and live
attributed receivers are shown after `/rx`:

```text
n<raw>/c<capped>/rx<receivers>|w<weight>|a<age>
```

Insufficient evidence is shown as:

```text
n<count>|<reason>
n<count>|<reason>|v<voacap-reason>
```

VOACAP fallback results are shown as:

```text
vcap|<snr>|h<hour>|s<ssn>
valn|<p50>/<snr>h<hour>s<ssn>
vup|<p50>/<snr>r<rel>s<ssn>
vop|<snr>r<rel>h<hour>s<ssn>
```

Beacon RX-only decisions add `brx|` to bucket diagnostics. Beacon VOACAP
fallback uses the same compact shapes with `bvcap`, `bvaln`, `bvup`, or `bvop`
prefixes.

- `n<count>` is the raw selected observation count behind the displayed path
  decision in every receiver-cap mode. It is a sample-size clue, not a
  confidence percent.
- `n<raw>/c<capped>/rx<receivers>` means receiver contribution caps reduced
  the diagnostic evidence. The raw count is the sample floor; the capped count,
  receiver count, and capped weight explain receiver-cap trust evidence.
- `w<weight>` is the rounded effective weight after decay and path selection.
  Fine and coarse are overlapping evidence layers, so blended scalar weight uses
  the larger layer instead of adding both. It is not SNR or dB. Weight is an
  evidence-strength gate, not the displayed path class itself.
- `a<age>` is the effective age of the selected evidence. Bare numbers are
  seconds; `m` and `h` mean minutes and hours. Blended fine/coarse age uses
  local fine mass plus the coarse regional complement; old local evidence can
  make a direction stale even when coarse regional evidence was recently
  refreshed.
- `vcap|<snr>|h<hour>|s<ssn>` means the optional VOACAP closed fallback
  supplied the result. `<snr>` is the selected hour's rounded bidirectional
  FT8-equivalent SNR after receive-side noise penalty, `h<hour>` is the
  selected UTC forecast hour, and `<ssn>` is the rounded EWMA SSN generation
  used for the run. `PASS/REJECT PATH CLOSED` targets these VOACAP closed
  fallback spots; `UNLIKELY` PATH filters still include them for compatibility.
  The runtime SSN monitor persists NOAA validators, the last observation, EWMA,
  and the current rounded SSN generation at
  `voacap_fallback.ssn_state_path`; a restart can reuse that SSN baseline when
  the state file is present. Completed VOACAP hourly forecast windows persist
  in the per-node Pebble cache at `voacap_fallback.forecast_cache_db_path`.
  On restart, records that still match the current cache schema, model
  generation, rounded SSN generation, forecast month, TTL, and current UTC hour
  hydrate the memory cache before workers start, so they bypass
  `voacap_fallback.delay_seconds`. Stale or malformed cache records are pruned;
  a missing/unavailable cache cold-starts normal delay/queue behavior.
- `valn|<p50>/<snr>h<hour>s<ssn>` means sparse bucket p50 evidence was
  insufficient by sample gates but aligned with the current-hour VOACAP class.
  `<p50>` is rounded for display so the diagnostic fits the fixed-width line.
- `vup|<p50>/<snr>r<rel>s<ssn>` means sparse p50 was insufficient, VOACAP
  mapped one class stronger, and the VOACAP request-SNR REL gate passed. REL is
  shown as percent and is not a direct HIGH/MEDIUM/LOW probability.
- `vop|<snr>r<rel>h<hour>s<ssn>` means there was no sparse p50, but cached
  current-hour VOACAP mapped to an open class and passed the REL gate.
- `n160c|uD|dNN` means native 160m fallback classified an insufficient 160m
  result as `CLOSED` because the user endpoint was in daylight. `xD` means DX
  endpoint daylight and `bD` means both endpoints. This is a solar-darkness
  proxy, not a VOACAP SNR result. Beacon receive-only native 160m diagnostics
  use the same suffixes with `bn160c`.
- `n160c|dNN` means no endpoint token was needed; native 160m `CLOSED` came
  from whole-path civil-dark fraction `NN`.
- `n160|xT|dNN` means native 160m fallback classified an insufficient 160m
  result as `UNLIKELY` because the DX endpoint was in civil twilight. `uT`
  means user endpoint twilight and `bT` means both endpoints.
- `n160|dNN` means native 160m fallback filled an insufficient 160m result as
  `LOW` or `UNLIKELY` from civil-dark path fraction `NN` after both endpoints
  were dark. Beacon receive-only native 160m diagnostics use `bn160|dNN`.
- `brx|...` means the spot was marked as a beacon and the path decision used
  only the DX-to-user receive leg. `bvcap`, `bvaln`, `bvup`, and `bvop` are the
  equivalent beacon VOACAP fallback diagnostics; their SNR and REL fields are
  receive-leg values, not bidirectional effective values.
- `none` means no usable selected sample existed.
- `lown` means selected samples existed, but their observation count was below
  the configured minimum.
- `lowr` means raw selected observations met the count floor, but receiver
  diversity was below the derived receiver gate.
- `loww` means selected samples existed, but their effective weight was below
  the configured minimum.
- `stale` means selected samples existed, but the selected evidence was too old
  for the band's freshness gate.
- `v*` suffixes on insufficient diagnostics explain VOACAP state for sparse or
  no-p50 candidates: `vq` queued, `vdly` delayed, `vinf` inflight, `vband`
  unsupported band, `vnbnd` empty/unknown band, `vugrd` invalid user grid,
  `vdgrd` invalid DX grid, `vucel` invalid user cell, `vdcel` invalid DX cell,
  `vbad` other invalid request, `vssn` SSN unavailable, `vcur` no current-hour
  cache record, `vqf` queue full, `vnr` worker not running, `vdis` disabled,
  `vun` unavailable, `vrel` open forecast blocked by REL or tier guards, `vnc`
  usable forecast that did not classify closed, and `vhit` ready cache hit with
  no emitted fallback.

The five-minute `Path predictions (5m)` propagation log uses the same reason
split: `no_sample`, `low_count`, `low_receiver`, `low_weight`, and `stale`.
`low_count` is the raw observation-count gate; `low_receiver` is the
receiver-diversity gate; `low_weight` is the decayed effective-weight gate; and
`stale` can increase when fine/coarse age drops an old local direction before
receive/transmit merge.
VOACAP fallback outcomes are counted separately as `voacap_closed`,
`voacap_aligned`, `voacap_sparse_upgrade`, and `voacap_open`.
Beacon spots add `beacon_rx`, `beacon_rx_insufficient`,
`beacon_rx_<reason>`, and `beacon_rx_voacap_*` counters to the same final
emission line.
Native 160m darkness fallback emissions add `native160_closed`,
`native160_low`, and `native160_unlikely` to the same line. These are
conservative CLOSED/LOW/UNLIKELY fills for insufficient 160m p50 when no usable
current-hour VOACAP result has precedence.

When the optional VOACAP fallback has activity, a separate
`VOACAP fallback (5m)` propagation log line explains the stage path:
`queued`, `success`, `failure`, `cache_hit`, `no_current_hour`, `delay_wait`,
`inflight`, `queue_full`, `not_running`, `ssn_unavailable`,
`invalid_request`, split invalid-request reasons (`invalid_unsupported_band`,
`invalid_empty_unknown_band`, `invalid_user_grid`, `invalid_dx_grid`,
`invalid_user_cell`, `invalid_dx_cell`), `closed`, `closed_no_p50`,
`closed_with_sparse_p50`, `closed_with_sparse_p50_class_*`, `aligned`,
`open_no_p50`, and `class_mismatch`, plus the REL-gated counters
`sparse_upgrade`, `open_no_p50_rel`, `rel_missing`, `rel_below_floor`, and
`rel_multi_tier`.
Use `Path predictions (5m)` to count final emitted glyphs.
Use `VOACAP fallback (5m)` to explain why a fallback lookup did or did not
emit.
Runtime VOACAP fallback decks select Method 20 below 7000 km and Method 30 at
and above 7000 km using the same Maidenhead grid-center endpoints written to
the VOACAP circuit. Cached records still reuse the existing fine path-cell
granularity, so near-threshold method reuse follows the same res-2 cache
boundary as other VOACAP fallback data.
When sparse or no-p50 candidates are present, a separate `Sparse p50 VOACAP
(5m)` line splits those candidates by p50 evidence (`no_p50`,
`very_low_count`), path kind (`beacon_rx`, `non_beacon`), cache/work state
(`cache_miss_total`, `cache_hit`, `queued`, `delayed`, `inflight`,
`invalid_request`, split invalid-request reasons, `ssn_unavailable`,
`no_current_hour`, `queue_full`, `not_running`, `disabled`, `unavailable`), and
outcome (`closed`, `aligned`, `sparse_upgrade`, `open_rel_pass`,
`open_rel_fail`, `not_closed`, `rel_missing`, `rel_below_floor`,
`rel_multi_tier`). It is diagnostic only; it does not change glyph decisions.
When native 160m fallback evaluates candidates, `Native 160m fallback (5m)`
reports `candidates`, `emitted`, class splits, `not_dark`, `unknown`,
`display_disabled`, endpoint daylight/twilight outcome counters,
`dark_le_closed`, and civil-darkness buckets `dark_ge_50`, `dark_ge_75`, and
`dark_ge_90`.
When sufficient p50 predictions can be compared against an existing current-hour
VOACAP cache record, a separate `VOACAP p50 compare (5m)` line reports cache
hits, cache misses, class agreement, stronger/weaker effective SNR, closed
VOACAP versus p50 class, and absolute SNR-delta buckets. The comparison is
cache-only: cache misses do not run VOACAP, start delay windows, or change
glyphs.
The shipped config writes these aggregate lines to `data/logs/propagation`,
not the system log.

The fixed-width cluster format may clip the right edge of a long diagnostic
comment to keep the grid, confidence, and time columns aligned. The leftmost
fields remain the important ones: count and effective weight or reason for
bucket results, and SNR plus selected UTC hour for VOACAP fallback results.

Example readings:

- `n18|w7`: 18 selected observations, rounded effective weight 7.
- `n0|none`: no usable selected sample.
- `n3|lown`: three selected observations existed, but not enough to emit a
  path class.
- `n0|none|vdly`: no usable selected path sample, and VOACAP is still in its
  configured delay window.
- `n2|lown|vrel`: very sparse p50 existed, VOACAP had a usable open forecast,
  but the REL or one-tier guard blocked an open fallback glyph.
- `n19/c5/rx1|lowr`: nineteen raw observations existed, but only one
  attributed receiver contributed capped evidence.
- `n19/c5/rx1|w3`: receiver caps reduced diagnostic evidence; raw count is
  shown first, capped effective count after `/c`, and attributed receiver count
  after `/rx`.
- `vcap|-34|h20|s112`: VOACAP fallback selected the 20:00 UTC forecast record,
  blended both directions, applied the user's receive noise penalty, rounded the
  effective FT8-equivalent SNR to -34, and used SSN generation 112.
- `valn|-15/-15h20s112`: sparse bucket p50 rounded to -15 dB and the 20:00
  UTC VOACAP forecast also mapped to that same path class.
- `vup|-19/-15r84s112`: sparse p50 rounded to -19 dB, VOACAP rounded to
  -15 dB, and VOACAP REL 84% passed the one-tier upgrade gate.
- `vop|-19r75h20s112`: no sparse p50 existed, but the 20:00 UTC VOACAP record
  rounded to -19 dB and REL 75% passed the open fallback gate.
- `n160c|uD|d82`: native 160m fallback classified an insufficient 160m result
  as `CLOSED` because the user endpoint was in daylight. `xD` means DX endpoint
  daylight and `bD` means both endpoints.
- `n160c|d12`: no endpoint token means native 160m `CLOSED` came from the
  whole-path civil-dark fraction.
- `n160|xT|d54`: native 160m fallback classified an insufficient 160m result
  as `UNLIKELY` because the DX endpoint was in civil twilight. `uT` means user
  endpoint twilight and `bT` means both endpoints.
- `n160|d82`: native 160m fallback filled an insufficient 160m result as
  `LOW` or `UNLIKELY` from an 82% civil-dark path fraction after both endpoints
  were dark. Beacon receive-only paths use `bn160|d82`.
- `n1|loww`: one selected observation existed, but the effective weight was
  below the minimum.
- `n32|w1`: large selected count but low rounded effective weight.

### Reading `SHOW PROP`

`SHOW PROP <call|prefix|grid> [band] [mode]` exposes the same rolling VOACAP
forecast window used by the fallback. Omitted mode defaults to CW. If an
explicit single-band request has no rows, or fewer rows than
`voacap_fallback.forecast_hours`, the command starts a refresh through the
existing fallback worker and waits briefly. All-band requests show cached rows
immediately while refreshing missing or partial bands in the background.

On Linux and other non-Windows builds the VOACAP fallback provider is not
started, so `SHOW PROP` reports that VOACAP fallback is disabled.

The target can be an explicit Maidenhead grid, a callsign found in the grid
store, or a CTY-derived prefix/callsign center. With no band, the command
queries all configured VOACAP fallback bands. With no mode, it uses `CW`.
Rows run from the current UTC hour through the configured
`voacap_fallback.forecast_hours` cache horizon, but only rows whose `REL`
prediction is `HIGH`, `MEDIUM`, or `LOW` are displayed.

```text
PROP FN31 -> JM77 target=IT9 source=cty-derived mode=FT8 band=20m noise=SUBURBAN ssn=112 hours=8
UTC  EFF  RX  TX  REL
18Z  <    -   <   LOW
```

- `EFF` is the merged bidirectional effective-path glyph.
- `RX` is the target-to-user receive-leg glyph after the user's `SET NOISE` penalty.
- `TX` is the user-to-target transmit-leg glyph.
- `REL` is the configured path class for the requested mode and merged path.
- Rows whose `REL` prediction is `UNLIKELY` or `CLOSED` are hidden. If every
  cached row is hidden, the command reports that there are no
  `HIGH`/`MEDIUM`/`LOW` rows in the current forecast window.
- Bucket p50 is intentionally not shown; sufficient bucket p50 remains
  authoritative for live spot glyphs.

## Logs And Health

System logs, propagation logs, optional dropped-call logs, and file-only event
logs are configured in `app.yaml`. Runtime file logs keep a stable active
filename derived from the configured directory name, such as `system.log` or
`propagation.log`, and completed UTC days archive as `DD-Mon-YYYY.log`.
Propagation logs live under `logging.propagation.dir`; they contain the path
prediction aggregates used by the daily propagation report. The file-only event
logs cover login attempt failures, reputation-gated spot drops, telnet client
lifecycle, ingest source lifecycle, and peer lifecycle. They do not add local UI
or console panes.
Under `systemd`, stdout/stderr also go to journald and can be tailed with:

```sh
journalctl -u gocluster -f
```

Common startup failures are usually config-path or config-content issues:

- `DXC_CONFIG_PATH` must point at a complete config directory, not one YAML file.
- Unknown YAML files fail startup. Extra YAML keys are logged as config
  warnings and ignored, except known removed migration keys, which still fail
  startup with a migration hint.
- Required startup YAML files, YAML-owned settings, and reference tables must
  be present. The loader reports all missing required files and settings it can
  find before aborting startup.
- When path reliability is enabled, `data.h3_table_path` must contain valid
  `res1.bin` and `res2.bin` H3 tables. Missing or malformed H3 tables fail
  startup because path predictions depend on those cells.
- On Linux and other non-Windows builds, `voacap_fallback.enabled: true` does
  not fail startup only because the Windows VOACAP engine is unavailable.
  Startup logs that the VOACAP fallback is disabled and continues without the
  VOACAP SSN monitor, worker, or forecast cache.
- Gridstore startup open failures are written to the system log. Corruption
  starts checkpoint recovery and runs temporarily without persistence; other
  open failures abort startup.
- The default config directory is `data/config` when `DXC_CONFIG_PATH` is not set.

The Overview `Ingest Sources` panel shows every enabled human/upstream server
as `HUMAN/<name>` in `ingest.yaml` order. Green means that server currently has
a TCP connection; red means disconnected/retrying. Each server contributes one
to the enabled count and, while connected, one to the connected count. The pane
shows at most ten rows but is scrollable, so entries beyond the visible window
remain discoverable.

For config loader details, see `data/config/README.md`.

### Canonical DXCC filter and history inputs

Use `PASS DXDXCC K,VE,248`, `REJECT DXDXCC IT9`, or `REJECT DEDXCC 3D2/R`.
Prefixes match canonical CTY labels exactly after trimming and uppercasing;
filter aliases/callsigns such as W6 and K1ABC are rejected. Invalid mixed lists
change nothing. Canonical input needs CTY; numeric-only filters retain their
existing positive-code behavior. IT9 selects the whole entity shared with I and
IG9, not Sicily alone. Use `REJECT DXCALL IT9*` for a callsign-prefix block.

`SHOW MYDX 3D2/R 10` and `SHOW DX K 10` search by entity. Canonical labels take
precedence over portable callsign parsing; existing callsign searches still work.
Numeric arguments remain counts. `SHOW DXCC` detail lookup is unchanged. See
[canonical DXCC prefixes](../telnet/README.md#canonical-dxcc-prefixes).

## US State And Canadian Province Filtering And Upgrade

Users can select `PASS DXSTATE CA,TX,ON,QC` or `REJECT DESTATE AA,AE,AP` using
the existing State fields. Unknown State fails an explicit PASS list and passes
a REJECT-only list. NEARBY suspends these rules until OFF. All 60 FCC and 13
Canadian province/territory codes are accepted. The source is a registered
address, which can differ from the station's operating location.

`fcc_uls.enabled` and `ised.enabled` control source-specific license rejection.
Turning either off keeps its downloads and enrichment active. Add the complete
required `ised` block from public `data.yaml` to private configurations before
upgrade; URL, archive/database/temp paths and UTC refresh time are explicit.
Canadian spot/login coverage includes ADIF 1, 211 and 252, routed by base call.
The two ISED archives publish together. Failed downloads/imports/swaps retain the
last good database, and exact source hashes keep pending updates retryable even
after HTTP 304 responses or restart. FCC availability remains independent.

ISED ordinary calls use club province when club data exists, otherwise personal
province. Active exact special calls use the listed trustee's province; active
prefix substitutions use an assigned ordinary call. Blank or conflicting
address evidence stays unknown. First/last event dates are inclusive UTC days;
Canadian cache entries revalidate at midnight. Prefix checks prove callsign
plausibility without proving residency or club/event eligibility.

Archive version 6 retains observed State; versions 2–5 remain unknown and are
never enriched on history reads. Preference disk version 3 and explicit machine
schema 3 also carry MINSNR; machine schemas 1/2 keep their layouts. Schema 1
writes preserve hidden State rules;
schema 2 exposes them. Stop writers and retain matching binary/data backups
before upgrade or downgrade: older binaries may reject Canadian codes in saved
profiles and archive rows. Recover failed refreshes by retrying with the last
good database in place. See [configuration](../data/config/README.md#canadian-ised-reference-data)
and [validation](canadian-state-validation.md).

## Per-Mode Minimum SNR

```text
PASS MINSNR CW,RTTY 10
REJECT MINSNR FT8,FT4 -10
PASS MINSNR FT8 ALL
REJECT MINSNR CW,RTTY NONE
SHOW FILTER MINSNR
```

Numeric PASS and REJECT set the same inclusive minimum. Human spots and spots
without SNR are exempt from MINSNR; other filters still apply. Zero and negative
values are thresholds, and a missing setting means disabled. Other modes remain
unchanged. ALL in the mode position must stand alone; numeric updates expand
currently supported modes, while reset operations clear active and dormant rules.
Removed modes retain inactive thresholds until their exact canonical mode returns.
RESET FILTER and PASS NOFILTER clear every minimum.

Presets, reconnects and modified status retain these settings. Changes invalidate
history continuation. Use GET YAML CONFIG SCHEMA 3 for exact `min_snr` values;
schemas 1/2 preserve hidden thresholds. PATCH replaces a supplied map and `{}`
clears it. Active and dormant rules share the finite entry/key-byte limits exposed
by schema 3 CAPABILITIES. See [the transport contract](../telnet/README.md#minimum-snr).

Literal comment filtering uses `PASS COMMENT <phrase>` and `REJECT COMMENT <phrase>`.
PASS phrases use OR, REJECT wins and other ordinary filters still apply. Use
`SHOW FILTER COMMENT`, `REMOVE PASS|REJECT COMMENT <phrase>` and
`RESET FILTER COMMENT [PASS|REJECT]` to inspect/remove/clear rules. Each list holds
32 phrases of at most 64 printable ASCII bytes; punctuation and repeated spaces
are literal. For archive searching use `SHOW DX [selector] [count] COMMENT <phrase>`;
the phrase is mandatory even for self-spots and is retained by NEXT. Rules persist
in presets/profiles and explicit machine schema 4. Old schemas preserve hidden
comments. See [the full comment contract](../telnet/README.md#comment-filters-and-searches).

New saves use disk configuration version 4. Older records acquire no minimums
and retain their previous rules, including version 2 States and version 3 minima.
Versions before 4 acquire empty comment rules. Back up profiles and
presets before upgrading; downgrade with a matching binary and data backup.
