# GoCluster DX Cluster

GoCluster helps amateur radio operators find stations on the air. It collects
**spots** (reports of a station heard at a particular frequency) and sends them
to your telnet client or logging program. You can choose which spots to see,
hide repeated reports, and view optional hints about radio conditions.

## On This Page

- [Connect and get started](#start-as-a-telnet-user)
- [Read a spot line](#read-a-spot-line)
- [Everyday commands](#common-telnet-commands)
- [Choose which spots you see](#filter-examples)
- [Save your preferences](#named-presets)
- [Understand confidence and path symbols](#confidence-tags)
- [Troubleshoot missing or surprising spots](#troubleshooting)
- [Advanced examples and command reference](#advanced-reference)
- [Run your own node](#running-a-node)
- [Operator and developer documentation](#deeper-docs)

## Start As A Telnet User

You need a telnet client or a logging program with a DX cluster connection.
For an example cluster, enter these connection settings:

| Setting | Value |
| --- | --- |
| Host / server | `cluster.n2wq.com` |
| Port | `8300` |
| Login | Your amateur radio callsign |

If you have a command-line telnet client installed, connect with:

```text
telnet cluster.n2wq.com 8300
```

Other clusters may use a different host or port; use the details supplied by
that cluster's operator. You do not need to download or run GoCluster to use
an existing cluster.

1. Connect and enter your callsign when asked.
2. Type `HELP` and press Enter to see the available commands.
3. Type `SHOW DX 10` to see up to ten recent spots that pass your filters.
4. Type `SHOW FILTER` to see your current filters, then `RESUME` when you are
   ready for live spots to continue.
5. Optionally, set your location with `SET GRID FN31PR`, replacing `FN31PR`
   with your own 4-6 character Maidenhead grid locator. A grid locator is the
   short location code used in amateur radio.
6. Type `BYE` when you want to disconnect.

Enter one command per line. In syntax such as `SET GRID <grid>`, replace the
angle-bracketed word with your value; do not type the brackets. Square brackets
in help mean an argument is optional. Example callsigns and grids below are
illustrative; use your own where appropriate.

**Reading pauses:** `SHOW FILTER` and `SHOW SETTINGS` temporarily stop live
spots so you can read the reply. The reply tells you about the pause. Type
`RESUME` to continue immediately. Missed live spots are not replayed; use
`SHOW DX` to look through recent history after your filters.

## Read A Spot Line

This illustrative line uses the shipped 76-character layout. Your cluster's
settings and available information may differ.

```text
DX de W1ABC:     14074.00  K1XYZ       FT8 -12 dB             > FN31 V 1830Z
```

| Part | Meaning |
| --- | --- |
| `W1ABC` | The reporting station, also called the spotter or **DE** station. |
| `14074.00` | Frequency in kilohertz (kHz): 14.074 MHz, on the 20m band. |
| `K1XYZ` | The station being reported, also called the **DX** station. |
| `FT8 -12 dB` | Operating mode and signal report, when available. **SNR** means signal-to-noise ratio. |
| `>` | An optional path hint for your location; see [path symbols](#path-reliability-tags). |
| `FN31` | The DX station's grid locator, when known. |
| `V` | Confidence in the reported callsign; see [confidence symbols](#confidence-tags). |
| `1830Z` | Time in UTC (18:30), rather than your local time. |

Comments can appear between the mode/report and the right-hand fields. A blank
path symbol means there is not enough usable evidence to rate the path.
Confidence describes support for the callsign; it does not promise that you
will hear the station or make a contact.

## Common Telnet Commands

| What you want to do | Command |
| --- | --- |
| Get help for a command | `HELP SHOW DX` |
| View recent spots | `SHOW DX 10` (`SHOW MYDX` is the same command) |
| View recent spots for a station | `SHOW DX K1ABC 10` |
| See all your filter categories | `SHOW FILTER` |
| Inspect one category or all details | `SHOW FILTER BAND` or `SHOW FILTER FULL` |
| See your grid, preferences, and session state | `SHOW SETTINGS` |
| Pause live spots for 60 seconds | `PAUSE 60` |
| Check or end a pause | `SHOW HOLD` or `RESUME` |
| Inspect or change repeated-spot suppression | `SHOW DEDUPE` or `SET DEDUPE FAST` |
| See countries reporting your own call | `WHOSPOTSME 20M` |
| Request a propagation outlook | `SHOW PROP IT9 20m FT8` (set your own grid first) |
| Receive solar summaries | `SET SOLAR 30` (every 30 minutes); `SET SOLAR OFF` to stop |
| Disconnect | `BYE` |

To post a station you have heard, use `DX <frequency_kHz> <callsign> [comment]`.
For example:

```text
DX 7001.0 K1ABC CW heard well
```

Frequency is in **kHz**, so enter `7001.0` for 7.001 MHz. Replace the example
with the station and frequency you actually heard. `HELP DX` explains the syntax.

## Filter Examples

Filters let you choose which reports to see. Different categories work
together: allowing a band does not bypass your mode, location, or other filters.
Use `SHOW FILTER` to confirm the result. Filter readbacks temporarily pause
live spots. Type `RESUME` when ready, or wait for the pause to expire.

`PASS` adds allowed values and removes those values from the blocked list.
`REJECT` adds blocked values and removes those values from the allowed list.
Values you do not name remain unchanged. In particular, `PASS MODE CW` does
not clear previously selected modes. `ALL` applies to the named category only.

Each recipe below is separate. Commands within a recipe are entered in order.
Recipes that start with `REJECT ... ALL` replace that category's selections.
Save a preset first if you want to restore your exact previous setup. Clearing
a category's restrictions allows all its values; it does not restore an older
selection.

### Show Only 20m And 40m

```text
REJECT BAND ALL
PASS BAND 20m,40m
SHOW FILTER BAND
```

Other filter categories still apply. To clear band restrictions, use
`PASS BAND ALL`.

### Show Only CW

```text
REJECT MODE ALL
PASS MODE CW
SHOW FILTER MODE
```

To clear mode restrictions, use `PASS MODE ALL`. `UNKNOWN` is the mode token
for reports whose mode is blank.

### Hide FT8 Reports

```text
REJECT MODE FT8
SHOW FILTER MODE
```

To allow FT8 again, use `PASS MODE FT8`; this adds it to the allowed modes.
Use `PASS MODE ALL` if you want every mode instead.

### Show Only Human Reports

```text
REJECT SOURCE ALL
PASS SOURCE HUMAN
SHOW FILTER SOURCE
```

`SKIMMER` is the automated-source category. To clear source restrictions,
use `PASS SOURCE ALL`.

### Choose Locations, Callsigns, Or Events

| Goal | Example | Clear this category's restrictions |
| --- | --- | --- |
| Select DX continents | `PASS DXCONT EU,AF` | `PASS DXCONT ALL` |
| Select DX countries/entities (**DXCC**) | `PASS DXDXCC K,VE` | `PASS DXDXCC ALL` |
| Select US states or Canadian provinces | `PASS DXSTATE CA,TX,ON,QC` | `PASS DXSTATE ALL` |
| Select callsign patterns (`*` is a wildcard) | `PASS DXCALL K1*,W1AW` | `PASS DXCALL ALL` |
| Select event families | `PASS EVENT POTA,SOTA` | `PASS EVENT ALL` |

These examples add to existing selections. `DX` filters refer to the spotted
station; `DE` filters refer to the reporting station. Country/entity filters
accept canonical prefixes or positive ADIF entity numbers. A prefix can cover
several related prefixes; for example, `IT9` selects the same entity as `I`
and `IG9`.

Event filters affect recognized event tags, such as POTA (Parks on the Air)
and SOTA (Summits on the Air). **Untagged spots still pass the EVENT category**,
even with `REJECT EVENT ALL`; other filters still apply. State/province filters
use registered addresses, which may differ from the operating location.

For more options, use `HELP PASS` or the [filter reference](telnet/README.md#filters).

### Focus On Nearby Reports

Set your actual grid before enabling this feature:

```text
SET GRID FN31PR
PASS NEARBY ON
SHOW FILTER
```

NEARBY keeps reports with either the DX station or the spotter in your nearby
area. The area is broader on 160m, 80m, and 60m than on other supported bands;
it is not a fixed distance radius.

While enabled, NEARBY suspends continent, zone, DXCC, grid, state, and province
filters for both DX and DE. Attempts to change those location filters are
rejected. Band, mode, and other filters still apply. Turn it off with
`PASS NEARBY OFF` to restore your saved location filters.

NEARBY survives reconnecting. If it is enabled but your grid cannot be used,
spots on affected bands are rejected. Check `SHOW FILTER`; set a usable grid
or turn NEARBY off. See [NEARBY details](telnet/README.md#grid-noise-and-nearby).

### Reset Your Filters

`RESET FILTER` restores the cluster's configured defaults for new users.
It changes your existing filters, including minimum-SNR rules; **it does not
necessarily enable every spot**. Save a preset first if you want to keep your
current setup.

## Named Presets

Your preferences normally save automatically and return when you reconnect
with the same full login callsign. `N2WQ` and `N2WQ-1` have separate saved
preferences. A numeric suffix such as `-1` is called an **SSID**.

A preset is a named snapshot of your filters and preferences:

| Action | Example |
| --- | --- |
| Save the current setup | `SAVE PRESET contest` |
| List saved setups | `LIST PRESET` |
| Apply a saved setup | `LOAD PRESET contest` |
| Delete a saved setup | `DELETE PRESET contest` |

Preset names are case-insensitive and display uppercase. Use 1-32 letters,
digits, or hyphens, starting with a letter or digit. Saving an existing name
replaces its snapshot. Deleting it leaves your current settings unchanged.

Your numeric SSIDs share the preset library. Loading a preset applies and
saves it for the current login identity; other connected sessions keep their
settings until they load it themselves. `(modified)` means your preferences
have changed from the saved or loaded snapshot. Temporary pauses and diagnostics
do not affect that marker and are not saved in presets.

If a reply warns that saving failed or that temporary defaults are in use,
do not assume your changes will survive reconnecting; contact the operator.
See [preset limits, persistence, and failure details](telnet/README.md#named-presets).

## Dedupe Policies

**Dedupe** hides repeated live reports so they do not overwhelm your feed.

| Policy | What to expect |
| --- | --- |
| `FAST` | More repeats; the shortest configured suppression window. |
| `MED` | A middle ground. |
| `SLOW` | Fewer repeats; a longer window and broader grouping by reporting area. |

Use `SET DEDUPE FAST`, `SET DEDUPE MED`, or `SET DEDUPE SLOW`, and check
`SHOW DEDUPE`. The shipped default is SLOW; the operator controls the available
policies and timing. If your chosen policy is disabled, the server selects an
enabled one and tells you. Usable NEARBY temporarily uses the least-suppressive
available policy without changing your saved choice.

See [dedupe details](telnet/README.md#dedupe-policies).

## Confidence Tags

These symbols describe support for the reported DX callsign:

| Symbol | Meaning |
| --- | --- |
| `?` | Little supporting evidence. |
| `S` | One current report with static or recent on-band support for the call. |
| `P` | Corroborated, below the strongest support level. |
| `V` | Strongly corroborated. |
| `C` | The call was corrected and the corrected call passed validation. |
| `B` | A suggested correction failed validation; the original call was kept. |

Use `PASS CONFIDENCE` or `REJECT CONFIDENCE` to filter these values.
For calculation details, see [spot confidence](spot/README.md).

## Path Reliability Tags

Path symbols are optional hints about radio conditions between your location
and the DX station. Set your actual grid with `SET GRID` and choose your local
receive-noise class with `SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL`.

| Symbol | PATH filter value | Meaning |
| --- | --- | --- |
| `>` | `HIGH` | Favorable path. |
| `=` | `MEDIUM` | Workable path. |
| `<` | `LOW` | Weak or marginal path. |
| `-` | `UNLIKELY` | Poor path. |
| `#` | `CLOSED` | Fallback indicates closed conditions. |
| blank | `INSUFFICIENT` | Not enough usable evidence to rate the path. |

The hints use recent reports and, when available, model fallbacks. They are not
a guarantee of a contact. A blank is not a prediction of a closed path.
Solar-weather overrides may display `R` for a radio blackout or `G` for a
geomagnetic storm; these do not replace INSUFFICIENT.

PATH filters use class names, not symbols: for example, `PASS PATH HIGH,MEDIUM`.
UNLIKELY rules also match CLOSED fallback results; a direct CLOSED rule can
select that class specifically. `SHOW PROP IT9 20m FT8` requests an hourly
outlook from your grid. Omitted mode defaults to CW; forecast availability
depends on the server, and UNLIKELY/CLOSED forecast rows are hidden.

See [path display](telnet/README.md#path-display) and the
[path calculation reference](pathreliability/README.md).

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Live spots stopped after a command | Run `SHOW HOLD`, then `RESUME`. Readbacks intentionally pause live spots. |
| No spots, or fewer than expected | Run `SHOW FILTER` and `SHOW SETTINGS`, then `RESUME`. Check band, mode, source, location, and path/confidence restrictions. |
| NEARBY is on but unavailable | Set your correct grid or use `PASS NEARBY OFF`. |
| Repeated reports are missing | Check `SHOW DEDUPE`; try `SET DEDUPE FAST` if you want more repeats. |
| A spot has a blank path symbol | Check your grid. Evidence or forecast support may be unavailable; blank means insufficient evidence. |
| Comments look like diagnostic codes | Use `SET DIAG OFF` to restore normal comments. |
| Preferences differ after reconnecting | Check the full login callsign, including the SSID, and any saving warnings. |
| Login is rejected | Check that you entered your actual callsign. Contact the operator with the exact reply if it remains rejected. |

`SHOW DX 10` checks recent stored history after your filters; it does not prove
that new reports are currently arriving. If filters and pauses do not explain
the problem, contact the cluster operator with the commands and replies you saw.

## Advanced Reference

The sections below provide optional detail. The linked package documents own
the complete command, persistence, and protocol explanations.

### Diagnostic Comments

`SET DIAG` replaces ordinary spot comments for your current session. Use
`SET DIAG SOURCE` to identify the source, `SET DIAG DEDUPE` for repeat-suppression
keys, `SET DIAG CONF` for calculated confidence, `SET DIAG PATH` for path
evidence, or `SET DIAG MODE` for mode provenance. Restore comments with
`SET DIAG OFF`. Long diagnostics can be clipped to fit the spot line.

See the [operator command guide](docs/OPERATOR_GUIDE.md#connect-and-use-commands)
and [path reference](pathreliability/README.md) for interpretation.

### Minimum SNR Filtering

`PASS MINSNR CW,RTTY 10` sets an inclusive minimum of 10 dB for those modes.
Numeric PASS and REJECT forms set the same minimum. Human reports and reports
without SNR are exempt; other filters still apply. Negative thresholds are valid.
Clear a mode's threshold with `PASS MINSNR FT8 ALL`, and inspect saved thresholds
with `SHOW FILTER MINSNR`. See the [minimum-SNR reference](telnet/README.md#minimum-snr).

### Toxic Comment Filtering

`REJECT TOXIC` hides human reports already classified as toxic; `PASS TOXIC`
allows them. This optional feature is disabled by default. Reports that have
not been classified, or whose classification is unavailable, still pass.
See [filter details](telnet/README.md#filters).

### History Searches

`SHOW MYDX 3D2/R 10` searches a canonical DXCC entity;
`SHOW DX K1ABC 10` searches one normalized station identity. Counts default to
50 and accept 1-250. Your filters and archive retention apply. Pages select
newest matches and display them chronologically. If a reply supplies
`SHOW DX NEXT H1...`, enter the exact returned command on the same connection
to continue. Reconnects and relevant preference changes require a fresh search.
See [history and continuation](telnet/README.md#archive-history-and-continuation).

New DX calls display without trailing numeric SSIDs: `K1ABC-2` becomes `K1ABC`.
Your login identity keeps its suffix, but own-call features ignore numeric
SSIDs. Use `SHOW OWN` to check the baseline call.

### Output Examples

These representative examples show detailed filter and settings readbacks.
Exact values depend on the active server configuration.

<details>
<summary>Expand filter, settings, own-call, and propagation examples</summary>

Confirm which baseline call is used for own-call features:

```text
> SHOW OWN
Own call: N2WQ
Login call: N2WQ-7
SSID handling: numeric SSIDs are ignored for own-call features.
```

Inspect and change your duplicate-suppression policy:

```text
> SHOW DEDUPE
Dedupe: SLOW (cqzone) (fast=on med=on slow=on)
> SET DEDUPE FAST
Dedupe policy set to FAST
```

Check your filter selections at once. Finite selections show their passing
names and useful exclusions, wrapping across lines. Long callsign, DXCC, grid
and zone lists show counts. This example uses the CONTEST preset with changes
and NEARBY enabled:

```text
> SHOW FILTER
User          N2WQ-1
Preset        CONTEST (modified)

Bands         Only 20m, 40m
Modes         CW, FT8; unknown modes hidden
Min SNR       None
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

With NEARBY off, the geography and callsign rows can look like this:

```text
DX geography  Continents: Only EU, NA | Zones: All | DXCC: All
              Grids: All
DE geography  Continents: All | Zones: Only 5, 8 | DXCC: All
              Grids: All
DX calls      All except 100 blocked patterns
DE calls      Only W1*, K1*; block W1XYZ
Nearby        Off
```

`SHOW FILTER FULL` displays complete effective PASS/REJECT selections. A
category shows only that part. ALL means unrestricted before listed exclusions;
NONE means no selections. Switches show ON/OFF. EVENT uses key presence, so a
stored `false` entry still applies:

```text
> SHOW FILTER EVENT
User          N2WQ-1
Preset        CONTEST (modified)

Events
  PASS: POTA, SOTA
  REJECT: WWFF
  Untagged spots are always included.

Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

`SHOW SETTINGS` separates your selections from the choices currently in use.
The following example uses server path minimums of 21 for stations and 11 for
beacons. NEARBY temporarily uses FAST dedupe while the saved choice remains
SLOW:

```text
> SHOW SETTINGS
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

The reading interval starts after the server finishes writing and flushing the
response. Session status is captured during preparation, so it does not claim
that the reading countdown has already started. Effective settings and path
minimums come from the active server and session state.

For example, reconnect does not activate a saved personal minimum of 15 when
the station minimum is 21. The beacon minimum remains 11:

```text
Path samples  15 configured; effective stations 21, beacons 11
              Override inactive: not above station minimum 21
```

An active stricter personal minimum of 30 applies to both:

```text
Path samples  30 (user minimum); stations 30, beacons 30
              Cluster minimums: stations 21, beacons 11
```

Human readbacks use at most 78 printable ASCII characters per line, followed
by CRLF. Exact strings use quoted ASCII escapes. A long value is split into
quoted pieces joined by `+`, without adding or removing characters:

```text
DX calls
  PASS: "  W1ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ c"
        + "af\u00e9\t\"Q\"\\end  "
  REJECT: NONE

A + joins quoted pieces of one value; no characters are added.
```

Spaces inside quotes belong to the value. Indentation, line endings and the
`+` marker do not. FULL/category omit inactive ordinary entries and show
effective PASS/REJECT selections; callsign patterns retain supplied order,
with REJECT taking precedence. Map keys use stable ordering. Use GET YAML FILTER
for exact stored flags, false entries and defaults.

See recent countries that have heard your baseline call:

```text
> WHOSPOTSME 20M
WHOSPOTSME 20M (last 10m):
  EU:  G(4) DL(2) F(1)
  NA:  K(3) VE(1)
```

Ask for a point-to-point propagation outlook. When a single-band request is
missing cache rows, it starts a VOACAP refresh and may wait briefly; all-band
requests show cached rows while refreshing missing bands in the background.
Omitted mode defaults to CW. Rows whose `REL` prediction is `UNLIKELY` or
`CLOSED` are hidden:

```text
> SHOW PROP IT9 20m FT8
PROP FN31 -> JM77 target=IT9 source=cty-derived mode=FT8 band=20m noise=SUBURBAN ssn=112 hours=8
UTC  EFF  RX  TX  REL
18Z  <    -   <   LOW
```

`EFF`, `RX`, and `TX` are mode-specific path glyphs. `EFF` is the merged
effective path, `RX` is the target-to-you receive leg after your `SET NOISE`
penalty, `TX` is the you-to-target transmit leg, and `REL` is the configured
class for the merged path. If all cached rows are `UNLIKELY` or `CLOSED`, the
command reports that there are no `HIGH`/`MEDIUM`/`LOW` rows in the current
forecast window.

Long command responses also append a short read-pause footer:

```text
Live spots paused for 30s after 14 output rows. Type RESUME to resume now.
Missed spots are not replayed.
```

`PAUSE [seconds]` lets you start the same pause manually. The duration defaults
to `30` seconds and accepts whole seconds from `1` to `300`, even when automatic
read pause is disabled. Repeating `PAUSE` sets a fresh duration; automatic
pauses can extend an active pause but cannot shorten it. Command replies and
other control traffic continue, and missed live spots are not replayed.

Human `SHOW FILTER`, FULL/category readbacks and `SHOW SETTINGS` suppress spots
from command acceptance through preparation, queueing and delivery. Their full
reading interval starts after the server completes its write and flush; terminal
rendering time is unknown. They use a positive configured pause duration, or
30 seconds when it is zero, and ignore the row threshold even when it is zero.
A longer existing pause is preserved. A later valid, processed `PAUSE` or
`RESUME` takes precedence over an earlier queued readback; another human
readback starts a fresh hold. Size-error responses follow this same policy.

</details>

### Client YAML Configuration

This protocol is for client software and advanced configuration tooling; ordinary
telnet users can use the commands above.

<details>
<summary>Expand the YAML protocol and complete SETTINGS response example</summary>

Clients use `GET YAML FILTER`, `GET YAML SETTINGS`, `GET YAML CONFIG` and
`GET YAML CAPABILITIES`. The first two read one resource; CONFIG captures filters
and settings together; CAPABILITIES describes the schema, choices and limits.
CAPABILITIES is read-only.
Optional `ID <id>` accepts 1-32 ASCII letters, digits or hyphens and preserves
case. Without an ID, GET assigns one.

This complete SETTINGS response is a separate example with no active pause:

```yaml
---
schema_version: 1
request_id: noise-Ab1
resource: SETTINGS
revision: 7faea7039a0b47a1bb8e462157b4c621-3
configuration:
  dialect: go
  grid: FN42
  noise_class: URBAN
  dedupe_policy: SLOW
  path_min_observation_count: 0
  solar_summary_minutes: 30
status:
  configured:
    dialect: go
    grid: FN42
    noise_class: URBAN
    dedupe_policy: SLOW
    path_min_observation_count: 0
    solar_summary_minutes: 30
  effective:
    dialect: go
    grid: FN42
    grid_derived: false
    noise_class: URBAN
    dedupe_policy: SLOW
    path_min_observation_count: 20
    solar_summary_minutes: 30
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
    default_dedupe_policy: SLOW
    default_noise_class: QUIET
    path_min_observation_count: 20
    auto_read_pause_min_rows: 10
    auto_read_pause_seconds: 30
  preset:
    associated: true
    name: CONTEST
    modified: true
...
```

Send `GET YAML SETTINGS ID noise-Ab1` to request that response shape. Only
`configuration` is writable. Preserve explicit `false`, zero, empty strings,
empty collections and `DEFAULT` selections when editing it; the `status`
sections, resource name and GET envelope are read-only. Use the returned
`revision` as `if_revision` in a new write envelope.

`PUT YAML FILTER|SETTINGS|CONFIG` replaces a complete resource and requires
every writable field. `PATCH YAML FILTER|SETTINGS|CONFIG` changes supplied
fields while retaining omissions. Supplied maps and lists replace their whole
collections. For example, change only the noise selection:

```text
PATCH YAML SETTINGS
---
schema_version: 1
request_id: noise-Ab2
if_revision: 7faea7039a0b47a1bb8e462157b4c621-3
configuration:
  noise_class: RURAL
...
```

The revision shown is illustrative: use the value from your own GET. GET again
after reconnect or a revision conflict. Validation or persistence failure leaves
both live and saved configuration unchanged; unavailable choices are rejected.
Even an unchanged PUT saves the configuration before reporting success, repairing
a failed earlier human autosave without resetting scheduling or session controls.
Machine writes retain the preset reference and may change `(modified)`.
`VALIDATE YAML CONFIG` checks a complete proposal without applying or saving it.

Every new human or YAML readback is limited to **65,536 final response bytes**,
including CRLF, framing and human footers. Responses are complete or return an
explicit error. New machine proposals must also fit a complete CONFIG response,
including reserved response metadata. Uploads allow 65,536 body bytes, excluding
markers but counting actual LF/CRLF bytes, with a 30-second deadline from header
acceptance. Oversized, expired, incomplete or unreliable uploads, including
malformed upload headers, close the connection. A fully received invalid document
gets a framed YAML error and keeps the connection open.

All machine success and error responses leave pause state unchanged and have no
human pause footer. Send one plain YAML document: aliases, anchors, merge keys,
custom tags, nulls and additional documents are rejected. Ordinary command
headers retain the configured line limit (128 bytes in the shipped config);
the framed body uses its separate limit.
See the [client protocol details](telnet/README.md#client-yaml-configuration).

</details>

### HELP

Use `HELP` on your connection for the commands supported by your active dialect.

<details>
<summary>Expand the default GO command list and filter rules</summary>

The section below mirrors the default `go` dialect `HELP` output from [`commands/processor.go`](commands/processor.go) using the shipped config in [`data/config`](data/config).

<!-- BEGIN DEFAULT_GO_HELP -->
```text
GoCluster help - GO dialect

Getting started:
  SHOW DX 10             Show 10 recent spots matching your filters.
  SHOW FILTER            See your current filters.
  SHOW SETTINGS          See your preferences and session settings.
  SET GRID FN31          Set your Maidenhead grid square.
  BYE                    Disconnect.

Type HELP followed by a command for details:
  HELP SHOW DX
  HELP PASS
  HELP REJECT
  HELP PASS COMMENT

Reading and posting spots:
  SHOW DX                Show recent spots matching your filters.
  SHOW MYDX              Same as SHOW DX.
  SH DX                  Short form of SHOW DX.
  DX                     Post a spot.
  SHOW DXCC              Look up a callsign or country prefix.
  WHOSPOTSME             Show recent spotter countries for your call.

Examples:
  SHOW DX 10             Show the latest 10 matching spots.
  SHOW DX K1ABC 10       Search for spots of K1ABC.
  SHOW DX 10 BAND 20     Search for 10 matching spots on 20m.
  DX 14025.0 K1ABC CQ    Post K1ABC on 14025.0 kHz with comment CQ.

Changing filters:
  PASS                   Allow selections.
  REJECT                 Block selections.
  SHOW FILTER            Show your current filters.
  SHOW FILTER FULL       Show complete filter selections.
  SHOW FILTER MODE       Show one filter category.
  RESET FILTER           Restore the cluster's default filters.

What you can filter:
  DX means the station being spotted; DE means the spotter.

  BAND                   Radio band, such as 20m or 40m.
  MODE                   Operating mode, such as CW, FT8 or USB.
  SOURCE                 HUMAN or SKIMMER reports.
  EVENT                  POTA, SOTA, IOTA, WWFF or LLOTA tags.
  COMMENT                A phrase anywhere in the spot comment.
  DXCALL                 Spotted callsign or pattern, such as K1ABC or W1*.
  DECALL                 Spotter callsign or pattern.
  DXCONT                 Spotted station's continent.
  DECONT                 Spotter's continent.
  DXZONE                 Spotted station's CQ zone (1-40).
  DEZONE                 Spotter's CQ zone (1-40).
  DXDXCC                 Spotted station's country prefix or ADIF number.
  DEDXCC                 Spotter's country prefix or ADIF number.
  DXSTATE                Spotted station's US state or Canadian province code.
  DESTATE                Spotter's US state or Canadian province code.
  DXGRID2                Spotted station's two-character grid, such as FN.
  DEGRID2                Spotter's two-character grid.
  CONFIDENCE             Callsign confidence symbols: ?, S, P, V, C, B.
  PATH                   HIGH, MEDIUM, LOW, UNLIKELY, CLOSED or INSUFFICIENT.
  MINSNR                 Minimum signal report in dB, selected by mode.

PASS and REJECT examples:
  Each example below is a separate change, not a combined recipe.

  PASS BAND 20,40        Add 20m and 40m to your band selections.
  REJECT BAND 80         Block 80m spots.
  PASS MODE CW,FT8       Enable CW and FT8; other modes stay unchanged.
  REJECT MODE FT8        Disable FT8.
  PASS SOURCE HUMAN      Allow human reports.
  REJECT SOURCE SKIMMER  Block automated skimmer reports.
  REJECT EVENT POTA      Block POTA-tagged spots.
  PASS COMMENT CQ        Require a comment containing CQ.
  REJECT COMMENT QRT     Block comments containing QRT.
  REJECT DXCALL W1*      Block spotted calls beginning with W1.
  REJECT DECALL W1ABC    Block reports from spotter W1ABC.
  PASS DXCONT EU         Add Europe to your DX continent selections.
  PASS DECONT NA         Add North America to your spotter selections.
  PASS DXZONE 14,15      Add DX CQ zones 14 and 15.
  PASS DEZONE 5          Add spotters in CQ zone 5.
  PASS DXDXCC DL         Add Germany to your DX country selections.
  PASS DEDXCC K          Add the United States to your spotter countries.
  PASS DXSTATE CT,MA     Add DX stations in Connecticut and Massachusetts.
  PASS DESTATE ON        Add spotters with Canadian province code ON.
  PASS DXGRID2 JO        Add DX stations in grid field JO.
  PASS DEGRID2 FN        Add spotters in grid field FN.
  REJECT CONFIDENCE ?    Block spots marked with confidence symbol ?.
  REJECT PATH LOW        Block spots classified as LOW path reliability.
  PASS MINSNR CW 10      Set a minimum CW signal report of 10 dB.

A complete recipe: only 20m/40m and only CW/FT8:
  REJECT BAND ALL
  PASS BAND 20,40
  REJECT MODE ALL
  PASS MODE CW,FT8
  SHOW FILTER

  This changes BAND and MODE only. Other filters still apply.
  Own-call spots have special exemptions; see HELP SHOW MYDX.

Filter rules to remember:
  Allowed selections do not generally replace your existing selections.
  Allowing moves named items to allowed; blocking moves them to blocked.
  Different filter categories must all match, except own-call exemptions.
  MODE changes only the modes you name.
  PASS BAND ALL allows every band; other filters still apply.
  REJECT EVENT ALL blocks tagged events, not untagged spots.
  COMMENT matches literal text without regard to letter case.
  COMMENT treats commas, quotes, * and ? as literal characters.
  COMMENT also treats ALL and NONE as literal phrases.
  Any allowed COMMENT phrase can match; a blocked phrase wins.
  Numeric PASS and REJECT MINSNR commands set the same minimum.
  MINSNR exempts human reports and reports without SNR.
  RESET FILTER restores cluster defaults, which may restrict spots.

Comment rules:
  PASS COMMENT           Add an allowed comment phrase.
  REJECT COMMENT         Add a blocked comment phrase.
  REMOVE PASS COMMENT    Remove an allowed phrase.
  REMOVE REJECT COMMENT  Remove a blocked phrase.
  RESET FILTER COMMENT   Clear all comment rules.
  SHOW FILTER COMMENT    Show your saved phrases.

Examples:
  PASS COMMENT CQ DX
  REJECT COMMENT QRT
  REMOVE PASS COMMENT CQ DX
  RESET FILTER COMMENT REJECT

Feature switches:
  Use PASS to enable or REJECT to disable:
  BEACON                 Beacon spots.
  WWV                    WWV bulletins.
  WCY                    WCY bulletins.
  ANNOUNCE               Announcements.
  SELF                   Your own spots.
  TOXIC                  Human spots already classified as toxic.

Examples:
  REJECT BEACON          Hide beacon spots.
  PASS ANNOUNCE          Allow announcements.
  REJECT TOXIC           Hide spots already classified as toxic.

Nearby filtering:
  SET GRID FN31          Set your location first.
  PASS NEARBY ON         Enable nearby filtering.
  PASS NEARBY OFF        Disable nearby filtering.

  NEARBY suspends location filters and retains their rules.
  Type HELP PASS NEARBY for details.

Preferences and propagation:
  SHOW SETTINGS          Show preferences and session settings.
  SET GRID               Set your Maidenhead grid square.
  SET NOISE              Set your receiving noise environment.
  SHOW PROP              Show an outlook to a callsign, prefix or grid.
  SET PATHSAMPLES        Set the minimum observations for path estimates.
  SHOW DEDUPE            Show duplicate-spot suppression settings.
  SET DEDUPE             Choose FAST, MED or SLOW duplicate suppression.
  SET SOLAR              Receive solar summaries or turn them off.
  DIALECT                Show or switch between GO and CC command styles.

Examples:
  SET GRID FN31
  SET NOISE SUBURBAN
  SHOW PROP DL 20 CW
  SET DEDUPE FAST
  SET SOLAR 30           Receive solar summaries every 30 minutes.
  SET SOLAR OFF          Stop solar summaries.

Saving and loading presets:
  SAVE PRESET <name>     Save your filters and preferences under a name.
  LIST PRESET            List your saved presets.
  LOAD PRESET <name>     Apply a preset and save this login's defaults.
  DELETE PRESET <name>   Delete a saved preset.

Examples:
  SAVE PRESET CONTEST
  LOAD PRESET CONTEST

Pausing live spots:
  PAUSE                  Pause live spots for 30 seconds.
  PAUSE 60               Pause for 60 seconds (maximum 300).
  SHOW HOLD              Show time remaining and spots suppressed.
  RESUME                 Resume live spots immediately.

  Filter and settings readbacks temporarily pause live spots.
  Type RESUME when ready, or wait for the pause to expire.
  Spots missed during a pause are not replayed.

Diagnostics:
  SHOW BUILD             Show server version and build information.
  SHOW OWN               Show your login call and own-call identity.
  SET DIAG               Select diagnostic information in spot comments.

More help:
  HELP PASS              Complete allow rules and examples.
  HELP REJECT            Complete block rules and examples.
  HELP FILTERS           Filter reference and supported values.
  HELP SYMBOLS           Confidence and path reliability symbols.
  HELP <command>         Detailed help for a command.

Other commands:
  HELP                   Show this help.
  BYE                    Disconnect (also QUIT or EXIT).

Syntax used in detailed help:
  <value> means required; [value] means optional.
  A|B means choose one. Do not type the brackets.
```
<!-- END DEFAULT_GO_HELP -->

</details>

## Running A Node

Telnet users connecting to an existing cluster can skip this section.
To operate your own node, download `gocluster-windows-amd64.zip` from
[GitHub Releases](https://github.com/N2WQ/GoCluster/releases/latest), extract it,
and follow the packaged `ready_to_run/README.md`. GitHub's automatic source
archives are not ready-to-run packages. See [download instructions](download/README.md).

For installation and service operation, use the [Operator Guide](docs/OPERATOR_GUIDE.md).
The published ready-to-run package is Windows amd64. Linux can be built from
source; Windows VOACAP execution is unavailable on non-Windows builds.

## Configuring A Real Node

Copy the complete public example directory `data/config` to a private directory
such as ignored `data/config.local`. Set `DXC_CONFIG_PATH` to that directory.
Replace the placeholder node and ingest login callsigns before connecting a
real node, and configure any upstreams or peers you enable. Keep passwords,
tokens, private callsigns, and endpoints out of committed example config.

Read [configuration ownership and loader rules](data/config/README.md) before
editing YAML. Normal setup does not require retuning pipeline or path-model
calibration. Follow the [node configuration steps](docs/OPERATOR_GUIDE.md#configure-a-real-node).

## Build And Service Notes

GoCluster builds from the repository root with Go `1.27.1+`. Follow the
[development and launcher instructions](scripts/README.md#development-and-launcher-validation)
and [environment guide](docs/ENVIRONMENT.md#development-tools-and-wsl).
Use the [release preparation guide](scripts/README.md#release-preparation)
for packaging and publishing, and the [Operator Guide](docs/OPERATOR_GUIDE.md)
for Windows startup or unattended Linux service operation.

Build identity uses the UTC date as `YYMMDD`; release builds also carry a
separate tag such as `261006r9abc` (date plus the last four commit-hash
characters). Same-day builds can share a product version. Use `SHOW BUILD`
or `--version` to identify the binary and available release/toolchain metadata.

## Operator Logs

Use the [logs and health guide](docs/OPERATOR_GUIDE.md#logs-and-health) and
[logging configuration reference](data/config/README.md) for file locations,
retention, and enabled event logs. Path diagnostics use the propagation log.
Keep the `peerdiag` companion beside the cluster executable for detailed peer
diagnostics; see [peer operations](peer/README.md#diagnostics-and-persistence).

## Repo Layout

The repo root now follows a simple ownership rule:

- `main.go` is the live binary entrypoint only.
- `internal/cluster` contains the live runtime implementation and cluster-local helpers.
- `cmd/` contains standalone tools and offline runners.
- `scripts/` contains build, release, profiling, validation, and developer helper scripts; use [`scripts/README.md`](scripts/README.md) before running or changing them.
- `data/` contains more than config: public example YAML in `data/config/`,
  private ignored config in `data/config.local/`, reference inputs such as CTY,
  FCC, ISED, H3, grids, beacons, and reputation/IPinfo data, plus runtime/local state
  such as users, logs, reports, diagnostics, peer topology, RBN data, SCP data,
  VOACAP runtime state, and skew/correction data. Treat committed
  example/reference data differently from ignored operator-local state.
- Domain packages such as `spot`, `peer`, `telnet`, `config`, and `pathreliability` remain reusable subsystems with their own tests and package-local docs.

Historical analysis notes and protocol reference material live under [`docs/archive/analysis`](docs/archive/analysis) and [`docs/reference`](docs/reference) rather than competing with the live binary at the repo root.

## Deeper Docs

Implementation-heavy material now lives next to the relevant code:

- [`commands/README.md`](commands/README.md) - HELP source of truth, dialects, and command/filter behavior
- [`telnet/README.md`](telnet/README.md) - login flow, output lines, dedupe, `NEARBY`, path display, and filter persistence
- [`spot/README.md`](spot/README.md) - confidence calculation, correction flow, and FT policy knobs
- [`pathreliability/README.md`](pathreliability/README.md) - path bucket math and shipped YAML tuning
- [`rbn/README.md`](rbn/README.md) - structural RBN parsing and comment handoff
- [`pskreporter/README.md`](pskreporter/README.md) - MQTT normalization, path-only modes, and FT frequency handling
- [`dxsummit/README.md`](dxsummit/README.md) - HTTP polling, DXSummit source markers, and HF/VHF/UHF scope
- [`peer/README.md`](peer/README.md) - peer forwarding, receive-only behavior, and control-plane details
- [`scripts/README.md`](scripts/README.md) - build, release, profiling, and workflow helper scripts
- [`data/config/README.md`](data/config/README.md) - YAML ownership, loader rules, and safe config editing boundaries
- [`data/h3/README.md`](data/h3/README.md) - H3 dataset notes

Additional operator references:

- [`docs/OPERATOR_GUIDE.md`](docs/OPERATOR_GUIDE.md)
