---
title: GoCluster DX Cluster
description: Connect to GoCluster, use numbered Boolean filters, and operate a bounded DX cluster.
---

GoCluster helps amateur radio operators find stations on the air. It collects
**spots** (reports of a station heard at a particular frequency) and sends them
to your telnet client or logging program. You can choose which spots to see,
hide repeated reports, and view optional hints about radio conditions.

> [!IMPORTANT]
> This checkout contains the Boolean-filter MVP, using schema 6 and a fresh
> isolated user store. It is not a production rollout or a claim that published
> release assets already contain this change. Recognized old profiles receive
> a one-time filter reset at login while retaining settings; old machine schemas
> remain refused. See the [upgrade contract](telnet/README.md#login-and-session-behavior).

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
   with your own 4 or 6 character Maidenhead grid locator. A grid locator is the
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
| View recent space weather bulletins | `SHOW WWV` or `SHOW WCY` |
| View retained space weather bulletins | `SHOW WWV FULL` or `SHOW WCY FULL` |
| See all your filter categories | `SHOW FILTER` |
| Inspect one category or all details | `SHOW FILTER BAND` or `SHOW FILTER FULL` |
| See your grid, preferences, and session state | `SHOW SETTINGS` |
| Pause live spots for 60 seconds | `PAUSE 60` |
| Check or end a pause | `SHOW HOLD` or `RESUME` |
| Inspect or change repeated-spot suppression | `SHOW DEDUPE` or `SET DEDUPE FAST` |
| See countries reporting your own call | `WHOSPOTSME 20M` |
| Request a propagation outlook | `SHOW PROP IT9 20m FT8` (set your own grid first) |
| Receive active solar summaries | `SET SOLAR 30` (every 30 minutes while active, on enabled nodes); `SET SOLAR OFF` to stop |
| Disconnect | `BYE` |

To post a station you have heard, use `DX <frequency_kHz> <callsign> [comment]`.
For example:

```text
DX 7001.0 K1ABC CW heard well
```

Frequency is in **kHz**, so enter `7001.0` for 7.001 MHz. Replace the example
with the station and frequency you actually heard. `HELP DX` explains the syntax.

Comments accept ASCII letters, digits, spaces, and `/ # @ - . , * ? _`.
Other punctuation and non-ASCII characters are rejected. The entire edited
command must fit the node's byte limit (128 by default), excluding its line ending.

## Filter Examples

Use numbered `PASS RULE` and `REJECT RULE` expressions to choose reports.
Any matching REJECT wins. Otherwise, any matching PASS qualifies. With no PASS
rules there is no personal PASS restriction; an empty set has no hidden base
filters. Separate controls, such as SELF and TOXIC, still apply.

Slots 0 through 9 identify rules, not priorities. Adding another PASS broadens
your selection rather than adding an AND condition. To require two conditions,
put both in one expression, such as `BAND=20m AND MODE=CW`. NOT binds before AND,
then OR; parentheses let you override that order.

New users receive an ordinary editable PASS rule in slot 0 containing the
configured default modes and sources. Users leaving the old filter system
receive the same starting rule while keeping their settings. Edit that rule when narrowing
the default selection. `SHOW FILTER RULE` displays the canonical expressions that are evaluated. Readbacks temporarily pause live spots;
type `RESUME` when ready.

A PASS rule without SOURCE allows both HUMAN and SKIMMER reports.
Use `AND SOURCE=HUMAN` for human reports only, or `AND SOURCE=SKIMMER`
for automated reports only. SOURCE follows ordinary AND/OR/NOT logic.
Neither PASS nor REJECT adds an implicit source restriction.

The shipped starting rule now selects both sources. Existing saved rules with
explicit `SOURCE=HUMAN` stay human-only, including previously generated defaults.
Saved expressions and presets are not rewritten. Source-less saved PASS rules
now also admit matching skimmer reports. Feed connections are unchanged.

Each recipe below is separate. Save a preset before using a recipe that begins
with RESET FILTER: it clears your rules, including any NEARBY terms. Old category
mutation commands are refused with guidance to `HELP RULE`.

### Show Only 20m And 40m

```text
RESET FILTER
PASS RULE 0 BAND=20m,40m
SHOW FILTER RULE
```

The comma list selects either band. Add `AND MODE=CW` to the same expression
if both the band selection and CW are required.

### Show Only CW

```text
RESET FILTER
PASS RULE 0 MODE=CW
SHOW FILTER RULE
```

Use `RESET FILTER RULE 0` to remove this rule. `UNKNOWN` is the known MODE token
for reports whose decoded mode is blank.

### Hide FT8 Reports

```text
REJECT RULE 9 MODE=FT8
SHOW FILTER RULE
```

This veto applies even when a PASS rule matches. Remove it with
`RESET FILTER RULE 9`; the remaining PASS rules still determine what qualifies.
Choose an unused slot if slot 9 already contains a rule you want to keep.

### Show Only Human Reports

```text
RESET FILTER
PASS RULE 0 SOURCE=HUMAN
SHOW FILTER RULE
```

`SKIMMER` is the automated-source category. To select both sources explicitly,
use `PASS RULE 0 SOURCE=HUMAN,SKIMMER`. Removing the last PASS also removes the
PASS restriction entirely.

### Choose Locations, Callsigns, Or Events

Each expression below can replace slot 0 with `PASS RULE 0 <expression>`.
Combine required selections with AND in that same expression.

| Goal | Expression |
| --- | --- |
| Select DX continents | `DXCONT=EU,AF` |
| Select DX countries/entities (**DXCC**) | `DXDXCC=291,1` |
| Select registered US/Canadian states or provinces | `DXSTATE=CA,TX,ON,QC` |
| Select callsign patterns | `DXCALL=K1*,W1AW` |
| Select event families | `EVENT=POTA,SOTA` |
| Select literal comment text | `COMMENT="CQ test"` |

`DX` refers to the spotted station; `DE` refers to the reporting station.
DXCC fields accept positive numeric ADIF identifiers and exact canonical CTY
labels, for example `DXDXCC=K,VE` or `DXDXCC<>K`. Labels need CTY and normalize
to numeric codes in responses, readbacks and saved rules. They select whole
entities, not callsign prefixes; numeric-only rules need no CTY.
Call patterns support an exact name, a single leading or trailing wildcard,
or a lone `*`.

EVENT is literal: `EVENT=POTA,SOTA` does not admit untagged spots.
Add `NONE` to include untagged spots. State/province values are registered
addresses, not proof of operating location. Unknown values fail comparisons,
including `<>`; NOT flips that result. Use `HELP RULE` or the
[filter reference](telnet/README.md#filters) for the full field contract.

### Focus On Nearby Reports

Set your actual grid, then replace rule 0 with the selection you want:

```text
SET GRID FN31PR
PASS RULE 0 MODE=CW AND NEARBY
SHOW FILTER
```

`NEARBY` is true when either the DX station or the spotter shares your H3 cell.
The cell is broader on 160m, 80m and 60m than on other supported bands; it is
not a fixed distance radius. Combine it with ordinary conditions:

```text
PASS RULE 0 MODE=CW AND (DXCONT=EU OR NEARBY)
```

This selects CW from Europe or with a nearby endpoint. Geography and other
conditions keep their meaning. Other PASS slots can broaden selection, and a
matching REJECT still vetoes it. There is no global NEARBY switch.

Missing proximity makes NEARBY false; NOT NEARBY consequently also accepts
unknown proximity. It does not prove a station is distant. An unrelated OR
branch can still qualify the spot. See [NEARBY details](telnet/README.md#grid-noise-and-nearby).

### Reset Your Filters

`RESET FILTER` empties all personal rules, including their NEARBY terms. It preserves
GRID, settings and separate controls and does not reinstall new-user defaults.
Use `RESET FILTER RULE <slot>` or `RESET FILTER RULE ALL` to clear selected rules. Save a preset first to retain the current setup.

## Named Presets

Your preferences normally save automatically and return when you reconnect
with the same full login callsign. `N2WQ` and `N2WQ-1` have separate saved
preferences. A numeric suffix such as `-1` is called an **SSID**.
For login, use one or two digits from `0` to `99`. `-01` and `-1` have
separate saved preferences. A longer or non-numeric SSID is rejected.

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
enabled one and tells you. NEARBY does not change your dedupe policy. Use
`SET DEDUPE FAST` explicitly if you want more local repeats.

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
| A NEARBY condition never matches | Check your saved grid and the full rule. Missing proximity is false; other OR branches may still pass. |
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

SNR comparisons use an integer report whenever present, including human reports.
To reject weak automated CW/RTTY reports while leaving human or missing reports
unaffected by this rule, use an unused slot:

```text
REJECT RULE 8 SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10
```

Remove it with `RESET FILTER RULE 8`. This does not bypass other rules.
See the [SNR reference](telnet/README.md#minimum-snr).

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

Inspect your complete expressions with `SHOW FILTER RULE`. For example, its
rule portion can show:

```text
PASS RULE 0 MODE=CW AND (DXCONT=EU OR NEARBY)
REJECT RULE 9 COMMENT="QRT"
```

Every saved rule retains its Boolean structure. `SHOW FILTER FULL` adds the
single prepared program's counts and separate delivery controls. Category
readbacks are unsupported. Human readbacks temporarily pause live spots;
use `RESUME` when ready.

`SHOW SETTINGS` separates your selections from the choices currently in use.
The following example uses server path minimums of 21 for stations and 11 for
beacons. Dedupe remains SLOW even when a rule contains NEARBY:

```text
> SHOW SETTINGS
User          N2WQ-1
Preset        CONTEST (modified)

Dialect       GO
Grid          FN31PR
Noise         SUBURBAN
Dedupe        SLOW
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

Login shows actual settings and numbered rules under "Default settings and
filters" for new accounts or "Current settings and filters" for returning
accounts. Upgraded users keep their settings and receive a short reset notice.
Rules are never flattened into independent mode/band/source lists.

Human readbacks use at most 78 printable ASCII characters per line, followed
by CRLF. Short rules are commands you can reuse:

```text
PASS RULE 0 MODE=CW AND BAND=20M
PASS RULE 1 MODE=FT8 AND BAND=40M AND SOURCE=SKIMMER
```

Long values use exact quoted ASCII pieces joined by `+`. Decode and concatenate
the pieces without adding spaces; display quotes and the `+` are not command
syntax. Use schema 6 YAML for rules beyond the command-line limit.
`SHOW FILTER FULL` adds preparation counts and separate-control detail. Category
readbacks are unsupported. `GET YAML FILTER SCHEMA 6` exposes exact saved rules,
individual controls and canonical expressions.

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

Human `SHOW FILTER`, RULE/FULL readbacks and `SHOW SETTINGS` suppress spots
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

Clients use `GET YAML FILTER SCHEMA 6`, `GET YAML SETTINGS SCHEMA 6`,
`GET YAML CONFIG SCHEMA 6` and `GET YAML CAPABILITIES SCHEMA 6`. The first two read one resource; CONFIG captures filters
and settings together; CAPABILITIES describes the schema, choices and limits.
CAPABILITIES is read-only.
Optional `ID <id>` accepts 1-32 ASCII letters, digits or hyphens and preserves
case. Without an ID, GET assigns one.

This complete SETTINGS response is a separate example with no active pause:

```yaml
---
schema_version: 6
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
  rules:
    configured_count: 0
    program: {nodes: 0, atoms: 0, values: 0}
    entries: []
...
```

Send `GET YAML SETTINGS SCHEMA 6 ID noise-Ab1` to request that response shape. Only
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
schema_version: 6
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
  HELP RULE
  HELP PASS
  HELP REJECT

Reading and posting spots:
  SHOW DX                Show recent spots matching your filters.
  SHOW MYDX              Same as SHOW DX.
  SH DX                  Short form of SHOW DX.
  DX                     Post a spot.
  SHOW DXCC              Look up a callsign or country prefix.
  WHOSPOTSME             Show recent spotter countries for your call.

Space weather bulletins:
  SHOW WWV               Show the latest five WWV bulletins.
  SHOW WCY               Show the latest five WCY bulletins.
  SHOW WWV FULL          Show retained WWV bulletins from the last 24 hours.
  SHOW WCY FULL          Show retained WCY bulletins from the last 24 hours.

Examples:
  SHOW DX 10             Show the latest 10 matching spots.
  SHOW DX K1ABC 10       Search for spots of K1ABC.
  SHOW DX 10 BAND 20     Search for 10 matching spots on 20m.
  SHOW DX 10 MODE CW     Search for 10 matching CW spots.
  SHOW DX 10 COMMENT CQ  Search stored comments for literal CQ.
  DX 14025.0 K1ABC CQ    Post K1ABC on 14025.0 kHz with comment CQ.

Changing filters:
  PASS RULE              Replace one slot with an allowing expression.
  REJECT RULE            Replace one slot with a vetoing expression.
  SHOW FILTER            See your current settings and rules.
  SHOW FILTER RULE       Same as SHOW FILTER.
  SHOW FILTER FULL       Show rules, controls and program counts.
  RESET FILTER RULE      Clear one slot or ALL; preserve settings.
  RESET FILTER           Clear personal rules; preserve settings.

What you can filter:
  Combine band, mode, source, geography, calls, comments and more.
  DX means the station being spotted; DE means the spotter.
  HELP RULE lists every field, supported value and expression limit.

Examples (separate full-rule replacements):
  PASS RULE 0 MODE=CW,FT8 AND SOURCE=HUMAN
  REJECT RULE 9 SNR<-15 OR COMMENT="test beacon"

A complete recipe: human-reported CW/FT8 on 20m/40m:
  RESET FILTER RULE ALL
  PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN
  SHOW FILTER

  This replaces all personal rules. Separate controls are unchanged.
  SELF ON can bypass personal rules; see HELP SHOW MYDX.

Filter rules to remember:
  Slots 0..9 identify rules, not priorities. Each edit replaces a full rule.
  Any PASS can qualify; any matching REJECT vetoes it.
  No active PASS means unrestricted except for matching REJECT rules.
  No rules means no personal restriction.
  New users start with an ordinary editable default PASS rule in slot 0.
  PASS without SOURCE allows humans and skimmers.
  Add AND SOURCE=HUMAN to select human reports only.
  To narrow it, replace/remove slot 0; adding another PASS broadens it.
  RESET FILTER clears rules, preserving separate controls/preferences.
  Reset does not reinstall default slot 0. See HELP RULE for full details.

Comment rules:
  PASS RULE 0 COMMENT="CQ DX"
  REJECT RULE 9 COMMENT="QRT"
  ASCII case-insensitive literal substrings, not regular expressions.
  Inside a quoted value, double a literal quote: COMMENT="CQ ""DX"""
  Edit/clear the numbered slot to replace/remove the whole rule.
  SHOW DX COMMENT remains a history query, not a saved rule edit.

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
  SET GRID FN31          Set your location.
  PASS RULE 0 NEARBY     Replace rule 0 with nearby selection.

  NEARBY combines with other conditions using NOT, AND and OR.
  It does not change geography conditions or duplicate suppression.
  Type HELP NEARBY or HELP RULE for details.

Preferences and propagation:
  SHOW SETTINGS          Show preferences and session settings.
  SET GRID               Set your Maidenhead grid square.
  SET NOISE              Set your receiving noise environment.
  SHOW PROP              Show an outlook to a callsign, prefix or grid.
  SET PATHSAMPLES        Set the minimum recent report units for paths.
  SHOW DEDUPE            Show duplicate-spot suppression settings.
  SET DEDUPE             Choose FAST, MED or SLOW duplicate suppression.
  SET SOLAR              Set the cadence for active solar summaries.
  DIALECT                Show or switch between GO and CC command styles.

Examples:
  SET GRID FN31
  SET NOISE SUBURBAN
  SHOW PROP DL 20 CW
  SET DEDUPE FAST
  SET SOLAR 30           Receive active solar summaries every 30 minutes.
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
  HELP RULE              Canonical Boolean grammar, fields and values.
  HELP PASS              Rule updates and separate enable controls.
  HELP REJECT            Rule updates and separate disable controls.
  HELP FILTERS           Navigate to rules and separate controls.
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

Run the executable with `--help` (`-h` or `-help` also works) to see usage and
configuration-directory guidance. Help and version return before configuration
loading or server startup. Start the configured node by running with no
arguments. Unexpected options or positional arguments print usage and exit 2.
A `--version`, `-version` or `version` token anywhere takes precedence over all
other arguments, preserving the existing version behavior.

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
