# Telnet Surface

This directory owns the telnet session layer: login flow, prompt handling, filter commands, client fan-out, and path display.

## Login And Session Behavior

- Clients log in with a callsign before commands are accepted.
- Login rejects command-like and mode-like tokens such as numeric-only port
  strings, all-letter words, `FT8`, `NOFT8`, or `SET/NOFT8` before any
  per-user state is loaded.
- Valid login tokens must include a concrete call-like identity segment such as
  `K1ABC`, `DL6LD`, `4U1UN`, `P5/N1K`, or `W6TEST-1`.
- When CTY data is loaded, login calls must resolve to a known CTY prefix.
- Base US calls (ADIF 291) must pass FCC validation; Canadian base calls (ADIF 1, 211, 252) must pass ISED assignment/plausibility checks when enabled and available. Portable location prefixes do not change the licensing source.
- Local TEST calls with CTY-valid US prefixes, such as `W6TEST` or `W6TEST-1`,
  bypass license validation but still require CTY validity.
- CTY or license data outages fail open so operators are not locked out by a
  reference-data refresh or missing local database.
- The greeting can include dialect, grid, noise, and dedupe status from config.
- Dialect choice and filter state are persisted per callsign.
- If `NEARBY` is active, the login greeting warns the user.

Restoration uses the full login call, including its SSID. Configuration,
associated preset name and its reference snapshot stay consistent through
replacement of an older connection. An old session cannot overwrite the new
session's record, including through a save already underway during reconnect.

If a valid record is restored but updating the login timestamp/IP fails, login
continues with those preferences and a warning. If the record itself is
unreadable or has an unsupported format, login uses protected temporary
defaults without overwriting that record. Human preference changes are
temporary; readbacks remain available. SAVE PRESET is rejected before any
library write, and LOAD PRESET and PUT/PATCH cannot commit configuration.
LIST/DELETE retain their separate preset-library behavior.

The handshake transcript tests in this package cover the visible login sequence.

## Spot Line Format

Spot lines are formatted in [`../spot/spot.go`](../spot/spot.go) and sent by the telnet layer.

Key operator-visible facts for the shipped config:

- `data/config/runtime.yaml` sets `telnet.output_line_length: 76`, so spot
  lines are `76` characters before CRLF
- mode starts at 1-based column `40`
- the fixed tail holds:
  - grid at column `65`
  - confidence at column `70`
  - time at column `72`
- the telnet layer normalizes line endings to CRLF

The formatter keeps the right-side tail stable by truncating comment text before it can push the grid, confidence, or time columns around.

Beacon spots with a blank source comment display `BEACON` in the comment field.
The mode, SNR/report text, path glyph, grid, confidence, and time columns stay
in their normal positions. This is a telnet and archive display convention; it
does not rewrite the comment forwarded to peers.
Blank `NCDXF B` source-class beacon comments display `NCDXF BEACON`.

## Filters

The telnet layer owns the live filter parser and persistence rules.

- `PASS` adds to the allow list and removes from the block list
- `REJECT` adds to the block list and removes from the allow list
- if a value appears in both, block wins
- `RESET FILTER` restores configured defaults for new users
- `PASS/REJECT MODE <list>` are deltas; modes not listed are unchanged
- `UNKNOWN` is the MODE token for blank-mode spots; use `PASS MODE UNKNOWN` or `PASS MODE ALL` to show them again after they are hidden

Path and confidence filters are operator-visible here:

- `PASS/REJECT CONFIDENCE` works with `?`, `S`, `C`, `P`, `V`, `B`
- `PASS/REJECT PATH` works with `HIGH`, `MEDIUM`, `LOW`, `UNLIKELY`, `CLOSED`, `INSUFFICIENT`
- `PASS/REJECT MODE` and `PASS/REJECT EVENT` validate against the active `spot_taxonomy.yaml`
- the shipped `PASS/REJECT EVENT` families are `LLOTA`, `IOTA`, `POTA`, `SOTA`, `WWFF`, or `ALL`
- `PASS TOXIC` shows spots classified as toxic; `REJECT TOXIC` hides only spots with the classifier status `TOXIC`

`CLOSED` is the optional VOACAP closed fallback class. It is filter-visible as
`CLOSED`, but remains compatible with `UNLIKELY`: existing
`PASS/REJECT PATH UNLIKELY` filters still include closed fallback spots. Direct
`PASS/REJECT PATH CLOSED` rules target only closed fallback spots.

EVENT filters are family-level. Standalone tokens such as `POTA` and acronym-prefixed references such as `POTA-1234` both match `POTA`; the reference stays in the comment and is not separately filterable. Spots with no recognized EVENT tag are not affected by EVENT filters, including `REJECT EVENT ALL`.

Toxic comment filtering is optional and fail-open. When the classifier is
disabled, unavailable, timed out, or has not seen a human comment yet, the spot
passes `REJECT TOXIC`. Routine comments that match the local ham-radio safe
gate are marked `SAFE_LOCAL` and also pass. The classifier receives only the
cleaned free-text comment, not callsigns, band, mode, source, IP, session data,
or archive records.

`SHOW FILTER` reports the current `TOXIC` toggle. The toggle applies to live
spots and archive-backed history queries. Local self-spot bypasses still honor
`REJECT TOXIC` once a spot is classified as `TOXIC`.

## Canonical DXCC Prefixes

DXCC filters accept exact canonical CTY prefixes (case-insensitive) as well as
existing positive ADIF numbers, including mixed comma/space-separated lists:

```text
PASS DXDXCC K,VE,248
REJECT DXDXCC IT9
REJECT DEDXCC 3D2/R
```

Canonical labels come from CTY's Prefix field, not its callsign lookup keys.
Letters-only and slash-bearing labels work; aliases and callsigns such as W6
and K1ABC are not canonical filter inputs. A canonical input requires loaded
CTY. Unknown or conflicting prefixes reject the entire command without changing
or saving any rules. Numeric-only commands retain their existing acceptance
without CTY membership verification. Standalone ALL and NEARBY restrictions
retain their current behavior.

Matching and persistence remain ADIF-based. IT9, I and IG9 select the same
entity, ADIF 248: REJECT DXDXCC IT9 blocks the whole entity, not just Sicily.
For a literal callsign-prefix block use REJECT DXCALL IT9* instead.

Every human SHOW FILTER view displays unambiguous canonical prefixes for known
entities. Short overview selections list all such prefixes, such as DXCC: All
except I, IG9, IT9; long previews count DXCC entities rather than expanded prefix
labels. FULL and DXDXCC/DEDXCC category views show effective PASS/REJECT
selections, for example `PASS: ALL` and `REJECT: I, IG9, IT9`. Inactive ordinary
entries and overridden flags are omitted.
Conflicting canonical labels are omitted; valid alternatives remain.
When none remain, each stored entity retains its own `Unknown DXCC (<number>)`
label. A missing CTY association, including unavailable CTY, also appears as
`Unknown DXCC (12345)`. Machine YAML and saved records continue to use numbers.
All human width, size, quoting and reading-pause limits remain in force.

SHOW DX and SHOW MYDX recognize canonical CTY labels before exact-call or
prefix classification, including SHOW MYDX 3D2/R 10. Numeric history arguments
remain counts; SHOW DXCC detail lookup retains its existing behavior.

## Archive History And Continuation

`SHOW DX` and `SHOW MYDX` share archive history selection and paging. Counts
range from 1 to 250, default 50; selector and count may appear in either order.
`SH DX` / `SH MYDX` work in both dialects, while `SHOW/DX` / `SH/DX` are `cc`
aliases. An unambiguous canonical CTY label selects its entity first. Other full
valid calls select their exact normalized stored DX identity; other supported
prefixes select their resolved entity. Supplied selectors require loaded CTY,
but valid full calls with unresolved countries still work as exact searches.
See [commands history](../commands/README.md#archive-history) for examples.

Every page applies current time minus configured archive retention, including
the exact cutoff, and visits at most 200,000 candidates. Decode failures,
malformed keys, rejects and non-consuming lookahead count toward that budget.
Rows are selected newest-first and displayed chronologically within the page.
A work-limit response is explicitly incomplete; count reached also offers NEXT
when older search remains. Continued output starts `Older retained history page:`.
Unreadable-record warnings accumulate across the search; exhaustion with a
warning is not proof that every matching record was readable. Archive failures
are explicit errors, and unsafe malformed cursor boundaries fail rather than
looping or silently skipping valid rows.

Follow the returned `SHOW DX NEXT <token>` on the same connection. Tokens are
`H1` plus 32 uppercase hexadecimal characters; the longest supported full NEXT
command is 49 bytes. One small cursor belongs to that connection, without a
registry, expiry worker or retained iterator. Successful page publication rotates
the handle; replaying an older handle fails. New valid searches replace the old
search immediately, even if the new scan fails. Invalid commands preserve it.
Close invalidates stored and pending work. Failed continuations preserve their
position for retry unless close, settings changes or another valid search
independently invalidated it.

Each page captures detached, coherent filter/path settings before scanning.
Relevant changes invalidate the search, including a change followed by restoring
the old value. Presentation-only preferences do not. Propagation observations
remain live; pages use fresh archive views and cutoff times. Newly inserted rows
ahead of the saved older position are not revisited. No configuration/history
lock is held during archive scanning. Publication rechecks connection state and
search generation to prevent stale results recreating invalidated cursors.

Advancement occurs when the bounded control queue accepts the page, rather than
when the socket delivers it. Queue overflow keeps the existing disconnect policy.
Network failure after queue acceptance does not undo advancement; reconnect and
start a fresh search. Archive readers close their request-owned iterators before
DB shutdown. Storage writes and timestamp range cleanup remain unchanged; no
secondary index, backfill or migration is introduced.

See [ADR-0251](../docs/decisions/ADR-0251-exact-call-paged-history.md) and
[TSR-0041](../docs/troubleshooting/TSR-0041-exact-call-history-and-scan-cap.md).

## Human Configuration Readbacks

| Command | Response |
| --- | --- |
| `SHOW FILTER` | Passing finite selections and useful exclusions wrap by name; long callsign, DXCC, grid and zone lists use counts. |
| `SHOW FILTER FULL` | Complete effective PASS/REJECT selections and ON/OFF switches. |
| `SHOW FILTER <category>` | Complete effective selections for one category. |
| `SHOW SETTINGS` | Configured preferences and effective behavior, followed by session status. |

`SHOW/FILTER` and `SH/FILTER` accept the same FULL/category arguments in the
`cc` dialect. The category names are BAND, MODE, SOURCE, EVENT, CONFIDENCE,
PATH, DXCONT, DECONT, DXZONE, DEZONE, DXGRID2, DEGRID2, DXDXCC, DEDXCC, DXSTATE,
DESTATE, DXCALL, DECALL, BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC and NEARBY.
CONF is an alias for CONFIDENCE, and PC93 is an alias for ANNOUNCE.

The overview shows finite selections by name, wrapping onto continuation
lines in stable order. This applies to BAND, MODE, SOURCE, EVENT, PATH,
CONFIDENCE and DX/DE continents. It shows passing selections and useful
explicit exclusions, without enumerating disabled choices. Unrestricted
categories say `All`; explicit blocks can say `All except 80m`. Longer explicit
band and mode selections can look like this:

```text
Bands         Only 1.25m, 10m, 12m, 13cm, 15m, 160m, 17m, 20m, 2200m, 23cm,
              2m, 30m, 33cm, 40m, 60m, 630m, 6m, 70cm, 80m
Modes         CW, FT2, FT4, FT8, JS8, LSB, MSK144, PSK, RTTY, SSTV, UNKNOWN,
              USB; unknown modes included
```

Long callsign, DXCC, grid and zone lists retain clear counts, such as
`Only 100 patterns; 3 blocked`. Those previews bound entry count and rendered
length before collecting, sorting or joining values; large lists do not build
all their detailed strings to produce counts. Finite selections preflight
aggregate escaped size before key collection, then count the complete wrapped
response. Canonical nonstandard keys retained in saved records remain visible
when reachable by the matcher. An unusually large finite selection can exceed
the response budget and return the explicit size error.

Rows describe what the matcher does. Ordinary string and integer categories
can remain restrictive when `allow_all=true` and their allow map is nonempty.
EVENT instead uses key presence: entries stored as `false` still apply,
`allow_all=true` ignores allow-list restrictions, and untagged spots always
pass the EVENT check. PATH retains the existing UNLIKELY/CLOSED relationship.
FULL/category show effective PASS/REJECT selections using ALL or NONE.
ALL means unrestricted before applying listed REJECT entries. Ordinary false
entries are omitted, and rejected entries are removed from PASS. A nonempty
false-only allow map still produces PASS: NONE. REJECT ALL hides overridden
entries. Reserved literal tokens and punctuation are quoted. For example:

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

`SHOW FILTER FULL` uses the effective format for every list category. Switches
show one ON/OFF value. DXCALL/DECALL retain ordered PASS/REJECT patterns, with
REJECT taking precedence when patterns overlap. PATH expands inherited CLOSED
behavior and honors explicit overrides. MODE explains UNKNOWN visibility;
CONFIDENCE explains exempt modes; EVENT always explains untagged inclusion.
Geography sections show PASS: ALL / REJECT: NONE with a suspension note when
NEARBY bypasses them. SELF explains its ordinary-filter bypass while TOXIC
still applies.

Machine YAML preserves stored flags, false entries and defaults. Use
`GET YAML FILTER` for exact stored rules; `DEFAULT` remains distinct from
explicit `true` or `false` there. NEARBY reports its effective ON/OFF state
separately from whether its user cells are usable.
When usable, short simple grids remain bare, such as `ON; grid FN31PR`
in detail views (`On` in the overview).
Stored grids needing escaping or wrapping use lossless quoted ASCII pieces,
such as `ON; grid "FN31PR\u00e9"`; the same lossless rule applies to overview,
FULL and NEARBY category output. Presentation preserves the original saved bytes.
Enabled NEARBY suspends ordinary location rules even when cells are unavailable;
unavailable cells reject ordinary DX spots on the affected bands:

```text
DX geography  Suspended by NEARBY; rules retained
DE geography  Suspended by NEARBY; rules retained
Nearby        On, unavailable; usable grid cells missing
              DX spots on affected bands are rejected
```

No successful FULL response omits an active selection. Inactive or overridden
stored entries do not count against human effective-content admission. The
[landing-page examples](../README.md#output-examples) show the complete aligned
overview and SETTINGS layout.

Every human readback line has at most 78 printable ASCII characters followed
by CRLF, including headers, active values, explanations, errors and pause
footers. Exact strings use Go-style double-quoted ASCII escapes for whitespace,
quotes, backslashes, control characters and non-ASCII values. Long individual
values use complete quoted pieces joined by `+`:

```text
DX calls
  PASS: "  W1ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ c"
        + "af\u00e9\t\"Q\"\\end  "
  REJECT: NONE

A + joins quoted pieces of one value; no characters are added.
```

Leading/trailing spaces inside quotes belong to the value. Indentation, line
endings and `+` are presentation only. Escapes are never split, and decoding
then joining the pieces reproduces the original string. Long active map keys
use the same mechanism without stored booleans.
String map keys sort lexicographically; integer map keys sort numerically.
Pattern lists retain supplied order and duplicates. Empty effective selections
use NONE; no word wrapping trims stored values.

`SHOW SETTINGS` uses readable labels such as Dialect, Grid, Noise, Dedupe and
Path samples. An empty configured preference is `DEFAULT`, with the effective
choice or looked-up grid explained separately. Zero PATHSAMPLES uses the
cluster minimums; zero SOLAR means Off. Effective path minimums come from the
active session override and loaded predictor configuration, not just the
stored preference. With station/beacon minimums of 21/11:

```text
Path samples  DEFAULT; stations 21, beacons 11
```

Reconnect leaves a saved minimum of 15 inactive because it does not exceed
the station floor:

```text
Path samples  15 configured; effective stations 21, beacons 11
              Override inactive: not above station minimum 21
```

An active personal minimum of 30 applies to both paths:

```text
Path samples  30 (user minimum); stations 30, beacons 30
              Cluster minimums: stations 21, beacons 11
```

Disabled or unavailable prediction is identified explicitly. These numbers
illustrate a configuration; production output reads active values.
Diagnostics, pause state and protected temporary defaults appear under
Session only. Pending delivery says `Paused for reading; at least 30s after
delivery`, without claiming that the reading countdown has begun. Both human
views show the preset row. Its `(modified)` flag compares preferences with the
applied/saved reference snapshot described below.

Every human readback, including a size error, always suppresses live spots
from command acceptance, before preparation or any transaction wait, through
queueing and delivery. It then provides the full reading interval after a
successful server write and flush. This does not measure terminal rendering.
The positive configured pause duration is used; zero selects 30 seconds for
these commands. The row threshold is ignored, including zero. A longer active
pause is preserved. Later valid, processed PAUSE/RESUME commands supersede
earlier pending completion effects; a subsequent human readback starts a fresh
hold. Suppressed counts carry from delivery into the reading interval.

Each complete response is limited to 65,536 bytes after CRLF conversion,
including its header and human footer. Generation either completes within
the limit or returns an explicit error. ASCII-escape expansion is budgeted
before unrestricted quoting or sorting. Exact-value scratch space is bounded
by the line width; finite selections and compact count previews have their
own preparation bounds.

## Client YAML Configuration

The following canonical commands work independently of the human dialect:

| Command | Purpose |
| --- | --- |
| `GET YAML FILTER [ID <id>]` | Read schema 1 filter values. |
| `GET YAML SETTINGS [ID <id>]` | Read all writable settings. |
| `GET YAML CONFIG [ID <id>]` | Read filters and settings together in one consistent snapshot. |
| `GET YAML CAPABILITIES [ID <id>]` | Discover version, fields, supported choices, availability and limits. |
| `GET YAML <resource> SCHEMA 2 [ID <id>]` | Opt in to schema 2, including state rules. |
| `PUT YAML FILTER`, `PUT YAML SETTINGS`, `PUT YAML CONFIG` | Replace the complete writable resource. |
| `PATCH YAML FILTER`, `PATCH YAML SETTINGS`, `PATCH YAML CONFIG` | Change supplied fields, preserving omissions. |
| `VALIDATE YAML CONFIG` | Check a complete proposal without applying or saving it. |

GET replies contain `schema_version`, `request_id`, `resource`, an opaque
`revision`, `configuration` and read-only `status`. GET assigns an ID if it is
omitted; a supplied ID preserves case and must contain 1-32 ASCII letters,
digits or hyphens. See the [complete SETTINGS envelope](../README.md#client-yaml-configuration)
for an example with all status fields.

FILTER's writable `configuration` directly contains the filter fields;
SETTINGS directly contains setting fields. CONFIG contains `filters` and
`settings` mappings. Extract that writable section when creating a new upload;
do not send the GET envelope's resource, revision or status fields as writable
data. Preset association, effective choices, server defaults, diagnostics and
pause state cannot be set through a configuration upload. The preset reference
snapshot itself is not exposed in GET.
`status.effective.nearby_active` means NEARBY is enabled with both user cells
available. If it is false while the configured `nearby_enabled` is true, ordinary
location rules remain suspended; invalid user cells reject spots on their bands.
CAPABILITIES is a read-only discovery resource; its `configuration` section
describes the protocol rather than writable preferences.

Existing GET commands keep schema 1 and its original field order. Schema 1
PUT/PATCH/VALIDATE preserves state rules hidden from that format; supplying
`dx_states` or `de_states` in a schema 1 upload is an unknown-field error.
Schema 2 adds those two string RuleSets, accepting the 73 uppercase US/Canadian
codes. Its complete PUT/VALIDATE requires both domains; PATCH preserves omitted
fields. Upload headers stay unchanged: the body `schema_version: 1` or `2`
selects its vocabulary. A single revision covers all preferences, so a hidden
state-only change conflicts with a stale schema 1 write. Schema selection itself
does not alter the revision, pause, preset or history continuation.

Schema 1 capabilities retain their original shape and advertise `[1, 2]` in
`schema_versions`. Request schema 2 capabilities to discover state fields and
choices. Schema 1 writes retain their existing 64 KiB projection limit; the
four hidden state maps are separately limited to 73 canonical keys each before
cloning. Schema 2 writes must fit the complete schema 2 CONFIG response. Large
ordinary human configurations remain supported; an oversized schema 2 GET
returns an explicit error, with schema 1 still available to reduce the rules.
No response is truncated.

### Writable Values

| Resource fields | Representation |
| --- | --- |
| `bands`, `modes`, `sources`, `events`, `confidence`, `path_classes`, `dx_continents`, `de_continents`, `dx_grid2`, `de_grid2` | RuleSet: `allow_all`/`block_all` booleans and `allow`/`block` maps of string keys to booleans. |
| `dx_states`, `de_states` (schema 2 only) | The same four RuleSet members, with uppercase US mailing-state and Canadian province/territory codes as string keys. |
| `dx_zones`, `de_zones`, `dx_dxcc`, `de_dxcc` | The same four RuleSet members, with integer rule keys. |
| `dx_callsigns`, `block_dx_callsigns`, `de_callsigns`, `block_de_callsigns` | Ordered string lists; duplicate patterns are preserved. |
| `include_beacons`, `allow_wwv`, `allow_wcy`, `allow_announce`, `allow_self`, `allow_toxic` | Explicit `true`, explicit `false` or the string `DEFAULT`. |
| `nearby_enabled` | Boolean. |
| `dialect`, `grid`, `noise_class`, `dedupe_policy` | Strings; an empty string preserves the explicit default/lookup selection. |
| `path_min_observation_count` | Integer; zero uses the cluster minimum. |
| `solar_summary_minutes` | Integer: 0, 15, 30 or 60; zero means OFF. |

Use canonical keys and the choices from CAPABILITIES. Both enabled and disabled
map entries are stored exactly. Preserve explicit false, zero, empty values,
empty collections and DEFAULT during a GET/edit/PUT cycle. Named preset
normalization is not machine validation: unknown or unavailable choices, such
as SLOW when disabled, fail unchanged rather than substituting another choice.
EVENT maps use key presence rather than their stored boolean values. To remove
an EVENT rule, supply its complete map without that key; setting it to `false`
keeps the rule present. The existing all-selection flags still apply.

PUT requires every field in its resource, including all four members of every
RuleSet. A missing field is an error. CONFIG requires complete filters and
settings together. PATCH preserves omitted fields, including omitted RuleSet
members. A supplied `bands.allow` replaces that entire map without merging
individual entries; the remaining band members stay unchanged. Supplied lists
replace the entire list, and `{}`/`[]` explicitly clear those collections.

### Upload And Revision Examples

A complete settings replacement uses the revision returned by your own GET:

```text
PUT YAML SETTINGS
---
schema_version: 1
request_id: settings-Ab1
if_revision: 7faea7039a0b47a1bb8e462157b4c621-3
configuration:
  dialect: ""
  grid: FN42
  noise_class: URBAN
  dedupe_policy: FAST
  path_min_observation_count: 0
  solar_summary_minutes: 0
...
```

CONFIG can change a dependent setting and filter together:

```text
PATCH YAML CONFIG
---
schema_version: 1
request_id: nearby-Ab1
if_revision: 7faea7039a0b47a1bb8e462157b4c621-3
configuration:
  filters:
    nearby_enabled: true
  settings:
    grid: FN42
...
```

This succeeds only if the server can activate NEARBY for that grid. A PATCH
can also replace just one rule map while retaining explicit false entries:

```text
PATCH YAML FILTER
---
schema_version: 1
request_id: bands-Ab1
if_revision: 7faea7039a0b47a1bb8e462157b4c621-3
configuration:
  bands:
    allow: {20m: true, 40m: false}
  dx_callsigns: ["W1*", "W1*"]
  allow_wcy: DEFAULT
...
```

Each example is a separate request; its sample revision is illustrative.
PUT/PATCH require a matching revision, an opaque ASCII string of at most
128 bytes. GET again after reconnect, server restart or a conflict before
retrying. Pause, diagnostics and login metadata do not count as configuration
edits. Reordering otherwise identical patterns does not change the revision
or `(modified)`; the displayed order and duplicate multiplicity are retained.

Uploads require supported `schema_version: 1` or `2`, `request_id` and `configuration`.
PUT/PATCH additionally require `if_revision`. VALIDATE CONFIG requires the
complete configuration; `if_revision` is optional because it does not write.
Its result reports `valid: true`, `applied: false` and `persisted: false` on
success. Successful writes report their operation, resulting revision and
`applied: true`, `persisted: true`.

Validation or persistence failure leaves both the live and durable
configuration unchanged. Machine writes preserve the associated preset and
reference; changing preferences can set or clear `(modified)`. Even an
unchanged PUT persists before success, so an earlier failed human autosave can
be repaired without resetting solar timing, NEARBY restoration, diagnostics
or pause. A lost acknowledgement does not undo a committed write; GET again
to discover the current revision and configuration.

A fully received unavailable-choice request gets a framed error, for example:

```yaml
---
schema_version: 1
request_id: settings-Ab1
resource: SETTINGS
revision: 7faea7039a0b47a1bb8e462157b4c621-3
error:
  code: invalid_configuration
  message: settings.dedupe_policy is unavailable on this server
...
```

### Framing, Limits And Pause Behavior

Send standalone `---` and `...` marker lines, terminated by LF or CRLF,
around one ordinary YAML document. Body bytes preserve case and punctuation;
terminal echo and editing controls do not modify the payload. Aliases,
anchors, merge keys, custom tags, nulls, duplicate keys, unknown/read-only
fields and additional documents are rejected. Numeric keys are checked for
duplicates after interpretation, so `1` and `01` cannot select the same rule
twice. Pattern-list duplicates are valid.

The body limit is 65,536 bytes, excluding marker lines and counting actual
LF/CRLF endings. The absolute reception deadline is 30 seconds from acceptance
of a valid header. At most one watchdog belongs to an active reception; a
completed buffered frame is still checked against that absolute deadline.
Oversized bodies, deadline expiry, incomplete or unreliable framing, and
malformed recognized PUT/PATCH/VALIDATE headers are terminal. Remaining bytes
cannot return to ordinary command dispatch. A complete malformed GET header
receives a framed error and remains usable; an early reader failure in any
recognized machine header is terminal. A complete, reliably framed invalid
document received before the deadline gets a YAML error and keeps the
connection open.
Terminal protocol rejection also skips final preference autosave: if an earlier
human command changed live preferences but its save failed, the rejected upload
does not save those preferences during disconnect. Normal disconnect saving and
session-ownership cleanup remain in effect.

Every GET response, write/validation acknowledgement and error fits within
65,536 final bytes after CRLF conversion, including all document markers and
metadata. Each complete frame is queued as one control message, so spots and
other messages can appear between documents but cannot enter a YAML document.
Successful readbacks contain every value; oversized readbacks return an
explicit error instead of truncation. PUT/PATCH/VALIDATE additionally require
the resulting complete CONFIG readback to fit, reserving the maximum response
metadata. A small PATCH to an oversized human-created configuration therefore
fails until the configuration is reduced enough to permit complete readback.

All machine operations, including success, validation and errors, have no
pause-state effects and no human pause footer. An existing pause continues
normally; suppressed-spot counts may increase as live traffic arrives.
Ordinary commands and machine headers keep the configured command-line limit
(128 bytes in the shipped config); only the framed body uses the larger limit.

## Named Presets

These commands work in both `go` and `cc` dialects:

| Command | Effect |
| --- | --- |
| `SAVE PRESET <name>` | Snapshot current filters and preferences; replace an existing name. |
| `LIST PRESET` | Show uppercase names alphabetically, with the count and limit. |
| `LOAD PRESET <name>` | Apply the snapshot and save this login call/SSID's defaults. |
| `DELETE PRESET <name>` | Delete the snapshot without changing current settings. |

Numeric SSIDs share the baseline callsign's collection. Portable prefixes and
non-numeric hyphen suffixes retain the existing baseline-call identity rules.
Other callsigns cannot address this collection through these commands. Loading
does not change another connected SSID's live state or saved default.

Names contain 1-32 ASCII letters, digits or hyphens, starting with
a letter or digit. They are case-insensitive and stored/displayed uppercase.
Each callsign can keep 20 presets, each with at most 256 KiB of standalone YAML
preferences. Replacement is allowed at capacity; a new name requires deleting
another preset. Presets remain independent snapshots after later preference
changes. The collection read limit remains 8 MiB. A valid preset can LOAD even
when its complete FULL/YAML readback exceeds the separate 65,536-byte response
budget; LOAD checks its acknowledgement rather than requiring CONFIG readback
to fit.

The snapshot includes every persistent filter field and toggle, `NEARBY`,
dialect, dedupe policy, grid, noise class, path sample minimum and solar summary
cadence. It excludes login/IP history, diagnostic mode, temporary read pause,
and derived caches. Legacy unmarked snapshots retain their migration rules;
current-version snapshots preserve exact configured values. LOAD uses server
restrictions: disabled dedupe policies use the enabled-policy fallback, and a
path sample override only applies above the current cluster minimum. Grid/H3
cells and the `NEARBY` location-filter restoration snapshot are rebuilt. Stored
`NEARBY` remains inactive with a warning when usable cells are unavailable.
Solar summaries start at the next wall-clock-aligned tick.

Successful LOAD associates the name and establishes the successfully applied
preferences as the reference. Server adjustments are reported separately.
Successful SAVE associates the exact captured preferences that were written.
Both human views and YAML status show `(modified)`/`modified: true` when the
current preferences differ from that reference. Reversing changes clears it;
this does not imply that every difference was caused by the user. Pause,
diagnostics and a temporary NEARBY dedupe override do not affect it.

The association and reference survive reconnect for the full callsign/SSID.
Deleting or overwriting the named library entry leaves the already applied
snapshot and reference intact. Failed LOAD preserves the previous preferences,
association and reference, both live and on disk.

SAVE has an explicit partial-success outcome when saving the library snapshot
succeeds but persisting the SSID association fails:

```text
Saved preset CONTEST, but could not persist its association for W1ABC-1.
Previous preset association and baseline retained.
```

The old association/reference remain recoverable from disk after reconnect,
and every ordinary preference save preserves them. The saved library snapshot
remains available. Protected temporary-defaults sessions reject SAVE before
changing the library and reject LOAD before changing configuration.

Ordinary per-SSID autosaving remains in place. Named collections are separate
runtime data at `data/users/presets/<hex-encoded-baseline-call>.yaml` under
`filter.UserDataDir`. Include that directory in user-data backups. Collections
are read on demand, with an 8 MiB read limit; no collection cache or background
worker is retained. A fixed array of 64 locks serializes updates in one cluster
process, including commands from different SSIDs. Multiple processes must not
write the same user-data directory concurrently.

SAVE/DELETE replace a complete collection using a synced, closed temporary file.
LOAD prepares detached preferences, preserves the target SSID's login metadata,
and atomically commits its user record before changing live settings. Missing,
invalid, oversized or unreadable presets produce errors. Failed writes leave the
previous file intact; a failed LOAD leaves live settings and saved defaults
unchanged. Malformed collections are not reset or overwritten automatically.
Successful SAVE/DELETE logs include collection cardinality. Handled failures
clean up their temporary files; a cleanup failure is logged for the operator.

### Persistence Format And Rollback

New records and snapshots carry `configuration_version: 2`. An absent marker
selects historical legacy migration; version 1 retains all old exact values and
initializes only the new state domains as unrestricted. Nested preset baselines
follow the same migration, preserving their modification status. New versions
are written on the next ordinary successful save; there is no bulk rewrite. Invalid markers, explicit zero and unknown
future versions are rejected; unreadable/unsupported user records are protected
by temporary-defaults sessions. Collections may contain supported legacy and
current entries together, but an unsupported entry prevents library mutation
without rewriting it.

Per-SSID preference and association saves use synced, closed temporary files
and atomic replacement. The configuration transaction covers reconnect's
record read, login metadata update and registration, including competing older
saves. It does not provide coordination between separate cluster processes.

Back up `data/users` with writers stopped before upgrading, and stop writers
before restoring a backup or changing server versions. An older user-record
writer can discard association/version metadata and normalize exact values;
an older preset reader may reject the new fields. Downgrade therefore requires
a compatible backup rather than assuming the new records are interchangeable.

## Dedupe Policies

The telnet server exposes per-user dedupe policy control through `SHOW DEDUPE` and `SET DEDUPE`.

- new users default to `dedup.default_policy` from the active config directory; the shipped default is `SLOW`
- the selected policy is persisted per callsign
- `SHOW DEDUPE` reports the saved policy, whether usable `NEARBY` changes the temporary effective policy, and whether `FAST`, `MED`, and `SLOW` are enabled server-side
- if a user requests a disabled policy, the server falls back to an enabled one and reports that in the response

Policy windows come from the active `dedup.secondary_*_window_seconds` values
in `data/config/dedupe.yaml`:

- `FAST`: shortest configured window, keyed by band + DE DXCC + DE grid2 + DX call
- `MED`: middle configured window, keyed by band + DE DXCC + DE grid2 + DX call
- `SLOW`: longest configured window, keyed by band + DE DXCC + DE CQ zone + DX call

This is why `SLOW` suppresses more repeats from one region than `FAST` or `MED`: CQ zone is broader than a 2-character grid square.

When usable `PASS NEARBY ON` is active, telnet spot delivery temporarily uses the least-suppressive available policy, normally `FAST`. The saved `SET DEDUPE` policy is not rewritten, and it resumes when `NEARBY` is off or inactive. `SHOW DEDUPE` reports the temporary lane when it differs from the saved policy. `SET DIAG DEDUPE` uses the same temporary effective policy for its compact key and policy tag.

## Bulletin Dedupe

WWV, WCY, and `TO ALL` announcement lines are control traffic, not spots, so they do not pass through the spot dedupe pipeline.

The telnet server applies a separate all-source duplicate guard before those lines enter per-client control queues:

- `telnet.bulletin_dedupe_window_seconds` sets the duplicate window; the shipped default is `600`.
- `telnet.bulletin_dedupe_window_seconds: 0` disables bulletin dedupe.
- `telnet.bulletin_dedupe_max_entries` bounds retained bulletin keys; the shipped default is `4096`.
- The key is the normalized bulletin kind plus the exact line shown to users, after newline normalization.
- Direct talk messages are not included.

If a duplicate is suppressed, slow clients do not see another control-queue enqueue. Unique bulletins still use the normal control queue, where a full queue disconnects the client.

## Read Pause

`PAUSE [seconds]` temporarily suppresses live spot lines for the current session.
`PAUSE` defaults to `30` seconds; `PAUSE 60` pauses for `60` seconds. The optional
duration accepts whole seconds from `1` to `300`. Invalid durations or extra
arguments return usage without changing the pause. The command is available in
both dialects and works even when automatic read pause is disabled.

A new `PAUSE` sets a fresh duration, including a shorter one. Automatic pauses
may extend an active pause but cannot shorten its deadline. Suppressed spot
counts carry across an active pause; a new pause after expiry starts a fresh
count. Pause state is temporary and is excluded from saved preferences and
presets.

Human configuration readbacks use the unconditional delivery/reading hold
described above. Later valid PAUSE/RESUME controls override queued readback
completion. All pause mutators share the same ordering authority, while YAML
operations do not mutate that authority.

Long command responses can temporarily pause live spot lines so users have time
to read the response before the live stream scrolls it away. The shipped config
uses `telnet.auto_read_pause_min_rows: 10` and
`telnet.auto_read_pause_seconds: 30`.

For those generic responses, either setting being zero disables the automatic
trigger. Human FILTER/SETTINGS readbacks are the explicit exception: they
ignore the row threshold and use 30 seconds when the duration is zero.

- The threshold counts rendered output rows, not bytes.
- Blank separator rows inside command output count because they scroll the
  terminal.
- The final trailing newline does not add an extra row.
- Live spot lines are suppressed during the pause and are not buffered or
  replayed.
- Command replies, errors, bulletins, announcements, talks, keepalives, and
  close messages stay on the control path and continue to send.
- Suppressed live spots do not count as slow-client queue drops and do not
  trigger extreme-drop disconnect policy.

When automatic pausing applies, the command response gets a footer showing the
effective remaining duration:

```text
Live spots paused for 30s after 14 output rows. Type RESUME to resume now.
Missed spots are not replayed.
```

Users can type `SHOW HOLD` to see remaining pause time and the suppressed spot
count. `RESUME` ends the pause immediately and discards any stale spot envelopes
queued before the resume point so old spots are not replayed.
Replies to `PAUSE`, `SHOW HOLD`, and `RESUME`, including PAUSE usage errors, do
not trigger automatic pausing themselves.

## Grid, Noise, And Nearby

- `SET GRID` stores the user's Maidenhead grid for path reliability
- `SET NOISE` stores the user's noise class; the checked-in path config applies one receive-side penalty per class
- `SET PATHSAMPLES` stores a stricter per-user path sample floor; it cannot lower the cluster default
- `SHOW PROP <call|prefix|grid> [band] [mode]` uses the saved grid and noise
  class to show a point-to-point VOACAP outlook; omitted mode defaults to CW
- `PASS NEARBY ON` requires a grid and keeps spots whose DX side or DE side falls in the user's nearby area

`NEARBY` uses H3 cells:

- coarse resolution on `160m`, `80m`, and `60m`
- finer resolution on the other supported bands

While `NEARBY` is active:

- the regular location filters are suspended
- human `PASS`/`REJECT` attempts to change `DXGRID2`, `DEGRID2`, `DXCONT`, `DECONT`, `DXZONE`, `DEZONE`, `DXDXCC`, `DEDXCC`, `DXSTATE`, and `DESTATE` are rejected with a warning
- `SHOW FILTER` reports `Nearby On` with the grid, or explains unavailable cells
- spot delivery uses the least-suppressive available dedupe policy while usable grid-backed cells are present

When `PASS NEARBY OFF` is used, the telnet layer restores the saved location-filter snapshot that existed before `NEARBY` was enabled.

`NEARBY` state is persisted. On login:

- the greeting warns when `NEARBY` is active
- if the user has no usable grid or H3 cells are unavailable, NEARBY remains enabled but unavailable; geography rules stay suspended and DX spots on affected bands are rejected

## Path Display

The telnet layer asks the path predictor for a class and glyph when path display is enabled and the user has a grid.

- normal classes come from [`../pathreliability`](../pathreliability)
- a per-user `SET PATHSAMPLES` override can only require more observations than the cluster default
- optional `R` and `G` solar-weather overrides are applied afterward
- the insufficient state is preserved and is not replaced by solar overrides

`SHOW PROP` is a command path rather than a spot-display glyph path. It resolves
an explicit grid, gridstore callsign, or CTY-derived prefix/callsign to a target
grid and asks the VOACAP fallback cache for the current UTC hour through the
configured forecast horizon. Empty or partial single-band lookups enqueue a
refresh through the existing VOACAP fallback worker and may wait briefly;
all-band lookups show cached rows while refreshing missing or partial bands in
the background. Cached rows show mode-specific glyphs for `EFF`, `RX`, and
`TX`, plus text `REL` for the merged path class. `RX` and `TX` are per-leg
projections on the same glyph scale, not independent live spot glyph decisions.
Rows whose `REL` prediction is `UNLIKELY` or `CLOSED` are hidden. Bucket p50 is
not shown.

For command HELP and dialect details, see [`../commands/README.md`](../commands/README.md).

## Cursor Validation For Developers

[`history_fuzz_test.go`](history_fuzz_test.go) provides
`FuzzHistoryCursorSequences`, a bounded stateful model that drives history
commands through fresh searches, continuation, partial read failures and
cancellation, matching-settings changes (including change then restore),
settings/search/close interleavings during a read, invalid and retired tokens,
queue overflow, close and reconnect. It checks published responses, cursor
ownership, saved positions, selection/counts and cumulative warnings after
operations. Propagation-observation and concurrent publication behavior remain
covered by the deterministic history tests.

Run from the repository root:

```sh
go test ./telnet
go test ./telnet -run '^$' -fuzz '^FuzzHistoryCursorSequences$' -fuzztime=20s -parallel=2
go test -race ./telnet -run '^$' -fuzz '^FuzzHistoryCursorSequences$' -fuzztime=30s -parallel=2
```

The seeded programs run during ordinary package tests. Mutation runs cover
sequential command sequences and scripted publication interleavings; they do
not replace the existing deterministic tests with overlapping goroutines.

## US State And Canadian Province Filters

`PASS DXSTATE CA,TX,ON,QC` selects spotted stations with those registered address
codes. `REJECT DESTATE AA,AE,AP` excludes spotters with those military postal
codes. The same commands work through `cc` SET/FILTER and UNSET/FILTER aliases.
Lists are case-insensitive and completely validated before mutation. Accepted
values are the 60 FCC codes and AB, BC, MB, NB, NL, NS, NT, NU, ON, PE, QC, SK, YT.

Unknown state passes an unrestricted or named REJECT-only category and fails
an explicit PASS list. ALL and NOFILTER follow existing resets; categories
combine with AND, values within a category with OR. SELF retains its existing
matching exception. SHOW FILTER, FULL and category details display finite
selections completely. NEARBY suspends state rules, locks their ordinary
mutation and restores them when switched off, including after schema 1 writes.

State enrichment remains active with either source's license enforcement
disabled. Registered addresses can differ from operating location. The final
corrected DX base call determines DXSTATE, and central ingest determines DESTATE.
Portable operating prefixes do not imply a state. ISED uses a club address when
club data exists, otherwise the personal address. Exact active special calls
use their listed ISED trustee; prefix substitutions use the ordinary assigned
base call. Blank or conflicting evidence leaves State unknown. Prefix matches
establish callsign plausibility, without proving event eligibility. Event dates
are inclusive UTC days; cached Canadian facts revalidate at UTC midnight.

Each registry has an independent snapshot and generation. Publication briefly
makes that source unknown; FCC rebuilding also suppresses FCC facts during its
import. Named State PASS lists exclude these unknown spots, while named REJECT
lists allow them through that category. Failed builds retain the last good
snapshot. Refresh duration depends on installation and source size; see
[FCC evidence](../docs/fcc-state-validation.md#full-sample) and
[Canadian evidence](../docs/canadian-state-validation.md).

Archive version 6 stores the observed State values. Versions 2–5 remain readable
with unknown State; history never consults current registry addresses. Existing
machine schemas 1/2 and profile disk version 2 retain their formats. Older
binaries can reject Canadian codes, so use matching backups for downgrade.
