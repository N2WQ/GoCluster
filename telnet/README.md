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
- US calls (ADIF 291) must pass FCC ULS validation when ULS is available.
- Local TEST calls with CTY-valid US prefixes, such as `W6TEST` or `W6TEST-1`,
  bypass FCC ULS validation but still require CTY validity.
- CTY or FCC data outages fail open so operators are not locked out by a
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

## Human Configuration Readbacks

| Command | Response |
| --- | --- |
| `SHOW FILTER` | Readable selections and restrictions, grouped geography and inclusion switches; long selections use counts. |
| `SHOW FILTER FULL` | Every exact rule and selection in every category. |
| `SHOW FILTER <category>` | Every exact value for one category. |
| `SHOW SETTINGS` | Configured preferences and effective behavior, followed by session status. |

`SHOW/FILTER` and `SH/FILTER` accept the same FULL/category arguments in the
`cc` dialect. The category names are BAND, MODE, SOURCE, EVENT, CONFIDENCE,
PATH, DXCONT, DECONT, DXZONE, DEZONE, DXGRID2, DEGRID2, DXDXCC, DEDXCC, DXCALL,
DECALL, BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC and NEARBY. CONF is an alias
for CONFIDENCE, and PC93 is an alias for ANNOUNCE.

The overview shows short selections directly, in stable order. When a row's
selections cannot fit, it shows a clear count, such as `Only 100 patterns; 3
blocked`. Preview preparation is bounded by entry count and rendered length
before collecting, sorting or joining values. Large selections do not build
all their detailed rule strings just to produce counts.

Rows describe what the matcher does. Ordinary string and integer categories
can remain restrictive when `allow_all=true` and their allow map is nonempty.
EVENT instead uses key presence: entries stored as `false` still apply,
`allow_all=true` ignores allow-list restrictions, and untagged spots always
pass the EVENT check. PATH retains the existing UNLIKELY/CLOSED relationship.
FULL and category responses preserve every stored flag and value:

```text
> SHOW FILTER EVENT
User          N2WQ-1
Preset        CONTEST (modified)

Events (exact rules)
  allow_all: false
  block_all: false
  allow:
    "POTA": true
    "SOTA": false
  block:
    "WWFF": false

Tagged spots: POTA or SOTA; WWFF blocked.
Untagged spots are always included.
False EVENT entries apply because matching uses key presence.

Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

`SHOW FILTER FULL` uses that exact format for each rule category, displays
ordered allow/block lists for DXCALL/DECALL, and includes all feature toggles.
`DEFAULT` remains distinct from explicit `true` or `false`. NEARBY reports its
configured On/Off selection separately from whether its user cells are usable.
When usable, short simple grids remain bare, such as `On; grid FN31PR`.
Stored grids needing escaping or wrapping use lossless quoted ASCII pieces,
such as `On; grid "FN31PR\u00e9"`; the same rule applies to overview, FULL and
NEARBY category output. Presentation preserves the original saved bytes.
Enabled NEARBY suspends ordinary location rules even when cells are unavailable;
unavailable cells reject ordinary DX spots on the affected bands:

```text
DX geography  Suspended by NEARBY; rules retained
DE geography  Suspended by NEARBY; rules retained
Nearby        On, unavailable; usable grid cells missing
              DX spots on affected bands are rejected
```

No successful FULL response omits a value. The
[landing-page examples](../README.md#output-examples) show the complete aligned
overview and SETTINGS layout.

Every human readback line has at most 78 printable ASCII characters followed
by CRLF, including headers, exact values, explanations, errors and pause
footers. Exact strings use Go-style double-quoted ASCII escapes for whitespace,
quotes, backslashes, control characters and non-ASCII values. Long individual
values use complete quoted pieces joined by `+`:

```text
DX calls (exact rules)
  allow:
    [1] "  W1ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ c"
        + "af\u00e9\t\"Q\"\\end  "
  block: []

A + joins quoted pieces of one value; no characters are added.
```

Leading/trailing spaces inside quotes belong to the value. Indentation, line
endings and `+` are presentation only. Escapes are never split, and decoding
then joining the pieces reproduces the original string. Long map keys use
the same mechanism with their associated boolean outside the quoted pieces.
String map keys sort lexicographically; integer map keys sort numerically.
Pattern lists retain supplied order and duplicates. Empty collections stay
explicit, and no word wrapping trims exact values.

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
by the line width; compact previews and counts remain bounded separately.

## Client YAML Configuration

The following canonical commands work independently of the human dialect:

| Command | Purpose |
| --- | --- |
| `GET YAML FILTER [ID <id>]` | Read every exact filter value. |
| `GET YAML SETTINGS [ID <id>]` | Read all writable settings. |
| `GET YAML CONFIG [ID <id>]` | Read filters and settings together in one consistent snapshot. |
| `GET YAML CAPABILITIES [ID <id>]` | Discover version, fields, supported choices, availability and limits. |
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

### Writable Values

| Resource fields | Representation |
| --- | --- |
| `bands`, `modes`, `sources`, `events`, `confidence`, `path_classes`, `dx_continents`, `de_continents`, `dx_grid2`, `de_grid2` | RuleSet: `allow_all`/`block_all` booleans and `allow`/`block` maps of string keys to booleans. |
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

Uploads require `schema_version: 1`, `request_id` and `configuration`.
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

New records and snapshots carry `configuration_version: 1`. Only an absent
marker selects legacy migration. Invalid markers, explicit zero and unknown
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
- human `PASS`/`REJECT` attempts to change `DXGRID2`, `DEGRID2`, `DXCONT`, `DECONT`, `DXZONE`, `DEZONE`, `DXDXCC`, and `DEDXCC` are rejected with a warning
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
