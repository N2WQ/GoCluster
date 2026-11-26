---
title: Telnet Surface
description: Login, command, configuration and delivery contracts for telnet clients.
---

This directory owns the telnet session layer: login flow, prompt handling, filter commands, client fan-out, and path display.

## Browser and Telnet on one callsign

Sign in to the browser and your logger with the same full callsign, including
any SSID suffix, to share filters and settings. One browser and one Telnet
connection can be open at once. A new connection replaces the older connection
of its own kind. The node counts the pair as one user and charges two connection
slots against its global, base-call, and per-IP limits.

If a second browser connects on the same SSID, the first browser offers
**Reconnect** and waits for the user to take over again. The paired logger
continues using the shared profile.

The browser selects GO for both connections. The CC filter bank remains saved,
and you can select it again with DIALECT CC after the browser disconnects.
SH/DX and SHOW/DX work in GO. VE7CC, spot delivery controls such as PAUSE, and
history continuations belong to the individual connection.

If one connection drops, the other keeps its feed and the live profile. A
rejoining client sees changes that are still active but unsaved. The node
retries a guarded profile save on departure when needed. Once the last
connection leaves, a later login loads the last successfully saved profile.
A failed final save can leave some changes unsaved.

If the browser switches the profile to GO and the save fails, GO is active.
The clients receive an unsaved warning, and a later browser login retries that
save. A reconnect with GO already saved still records the login time, recent
IP and enabled reputation history. Its metadata write carries forward the
saved profile. The reconnecting client receives a timestamp/IP warning if
that write fails.

The logger sees browser connection notices with the observed browser address
and its country code, or unknown. During heavy output, pending connection
notices become one summary. The browser sees logger presence only. Logging in
from a new network can affect base-call self-spot restrictions when reputation
controls are enabled.

Incoming private PC93 messages from peers go to the logger while it is
registered, or to the browser when it is alone. If the logger's network path
fails silently, a message can be lost before the operating system reports the
failure. Subsequent messages can reach the browser once the logger retires.
A full ordinary control queue still follows the normal slow-client policy.

## JSON output

After login, send `SET JSON ON` or `SET JSON ON VERSION 1`. Every connection
starts in text, so send the command again after reconnecting. The last text
line is `JSON ON 1` followed by CRLF. Starting with the very next byte, each
record is a JSON array ending in a single LF. Server echo is off. Clients
read one line even when it spans several network reads. `SET JSON OFF` waits
for current history delivery, writes the final `json-off` notice, then returns
to text and the configured echo behavior. Wait for that notice before sending
the next command. `SHOW SETTINGS` lists JSON under **Session only**.

The opening `hello` gives the protocol version, node, full login, server,
current time and limits. The two schema records name every position,
including position zero. Clients can ignore unknown record kinds and
trailing fields. Layouts can gain trailing positions in later schema
versions. The following opening illustrates a node using the shipped
taxonomy with DXSummit disabled and the ordinary command limit.

```jsonl
["hello",1,{"node":"W1ABC-2","login":"K1ABC-1","server":"GoCluster","time":"1700000000000000000","limits":{"spotBytes":4726748,"nodeBytes":256,"dxCountryBytes":256,"deCountryBytes":256}}]
["schema","s",1,["type","id","time","freq","obsFreq","band","mode","modeProv","report","comment","cwSpeeds","events","source","node","human","beacon","conf","confPct","dx","de","toxic","path","glyph"]]
["schema","station",1,["call","adif","country","cont","cq","itu","state","grid","gridDerived","grid2","iaru"]]
```

`spotBytes` bounds a single encoded spot line including its LF. Read the
node's advertised value. Supported feed limits, command size, configured
taxonomy and readable archive formats contribute to it. Each of the other
limits counts raw bytes before JSON escaping. An oversized originating node,
DX country or reporting country becomes `null` and increments its own
omission counter. Comments retain their full stored content. Invalid UTF-8
bytes become U+FFFD. Timestamps are signed decimal Unix nanoseconds in
strings, or `null` when unavailable.

The following exchange shows a live spot, then a one-row historical page.
An `s` record may also arrive between the header and trailer. Frequencies
are kHz. `id` is local to a running node and becomes `null` for a historical
row, zero or a value above 2^53−1. An enabled CW speed preference supplies
the full available list. Each station carries at most six normalized grid
characters, a two-character field when available, and an IARU region of 1,
2, 3 or `null`. Country and ITU on historical rows require agreement between
current CTY and the stored ADIF. Path and glyph reflect live recipient
settings and are `null` in history.

```jsonl
["s",42,"1700000000000000000",14025.00,null,"20m","CW",null,18,"CQ",["24 WPM"],[],"RBN",null,false,false,null,null,["W1AW",291,"United States","NA",5,8,"CT","FN31",false,"FN",2],["K1ABC",291,"United States","NA",5,8,"MA","FN42",false,"FN",2],false,null,null]
["hb",{"count":1,"older":false}]
["h",null,"1700000000000000000",14025.00,null,"20m","CW",null,18,"CQ",["24 WPM"],[],"RBN",null,false,false,null,null,["W1AW",291,"United States","NA",5,8,"CT","FN31",false,"FN",2],["K1ABC",291,"United States","NA",5,8,"MA","FN42",false,"FN",2],false,null,null]
["he","exhausted",null,{"unreadable":false,"hadMatches":true}]
```

`SH DX` and `SH DX NEXT` use the same filters, order, scan allowance and
250-row limit as text history. `hb.count` is the selected count.
`older` marks a continuation. Keep the page only after `he` with
`exhausted`, `count` or `budget`. A returned token becomes usable after the
trailer flushes. A zero-row `budget` can still supply a token. A zero-row
`busy` can be retried from the starting cursor. `restart` calls for a fresh
search. Discard an incomplete or `failed` page.

Each line of human command output or a human terminal reply uses `text`.
Machine readbacks, acknowledgements and machine error replies for schema 6
and schema 7 use a single `yaml` record. Decoding its second element recovers
the complete legacy CRLF document with its delimiters. The respective raw
document limits are 65,536 and 262,144 bytes. With JSON escaping, the line
is bounded by twice that limit plus 12 bytes. A malformed upload header can
produce a terminal YAML reply. Unreliable or expired body framing closes
the connection.

Large YAML transfers share the existing 8 MiB output budget, charged in
1 MiB slots through flush or discard. A JSON mutation also secures its place
in the output queue before committing. If the connection drops during a
write command, reconnect and GET the current configuration and revision
before retrying. The commit can complete before its reply arrives.
An unreserved machine reply, such as
an admission error, occupies the connection's single command reader until
flush or disconnect. That path retains at most one raw document of 65,536
bytes per connection. Schema 7 diagnostic replies on it are capped at 4 KiB.
The writer escapes each value in fixed scratch and streams large records.

```jsonl
["text","Session only"]
["text","JSON          On"]
["yaml","---\r\nschema_version: 7\r\nrequest_id: \"\"\r\nresource: \"\"\r\nerror:\r\n  code: invalid_header\r\n  message: PUT YAML permits only SCHEMA 7 after the resource\r\n...\r\n"]
["keepalive"]
["notice","refused","JSON REFUSED already on"]
["notice","omitted","Invalid spot frequency."]
["notice","session-replaced","Another login with your call."]
["text","73!"]
```

`BYE` writes the `73!` text record before closing. A login-time reset notice
arrives before any client can enable JSON. For example, a pending-notice
line can read `An upgrade notice is still pending. Use SHOW FILTER to inspect your`.
`session-replaced` is terminal and applies to a displaced browser whose writer
has entered JSON mode. It carries the configured duplicate-login message, or
an empty string. Use the reason code to stop browser auto-reconnect. The
writer attempts to deliver the record before closing within its write budget.
Ordinary notices and readback lines use `text` while JSON is active. Only
manual `PAUSE` and `RESUME` pause spots in JSON. Switching on cancels any
automatic hold and preserves the original manual deadline. Use
`UNSET/VE7CC` before opting into JSON if CC11 is active. Compression and
unsupported versions receive a refusal in the currently active format.

Live bulletins carry kind, local receipt or generation time, sender when
available, and the original display line. The examples show the record
kinds. The server's private-message route admits only the intended current
session. A solar message uses the scheduled generation time. Bulletin
history requested as a human command uses text lines.

```jsonl
["bulletin","WWV","1700000000000000000","W1AW","WWV de W1AW <00> : SFI=150"]
["bulletin","WCY","1700000000000000000",null,"WCY de W1AW <00> : K=2"]
["bulletin","SOLAR","1700000000000000000",null,"Solar conditions active"]
["bulletin","ANNOUNCE","1700000000000000000","W1AW","To ALL de W1AW: Test announcement"]
["bulletin","TALK","1700000000000000000","K1ABC","To W1AW de K1ABC: Hello"]
["notice","json-off",""]
```

**Fields and disclosure.** The greeting, startup information, operator
templates and login reset notices use the existing text login path.
A telnet template can show the previous login address via `<LAST_IP>`.
The JSON hello publishes the node and login identity. Spot and history rows
use the two public layouts above. IP addresses from spot ingest, TTL, raw
skimmer observations, H3 cells, toxicity details, internal caches, NEARBY
decisions and diagnostic decorations have no output positions. YAML exposes
the requested configuration, its revision and its defined status fields.
Shared-SSID advisory notices append the view's version as a decimal string.
Profile, save-status and preset versions are independent. A configuration
notice prompts a CONFIG readback, a save-status notice prompts a SETTINGS
readback, and a preset notice prompts a preset list refresh. The profile
revision for a write still comes from the schema 7 readback.

```json
["notice","config-changed","Live profile changed.","5"]
["notice","save-status-changed","GO is active but unsaved.","2"]
["notice","presets-changed","Preset library changed.","12"]
["notice","presence-changed","Logger connected.","3"]
["notice","go-selected","GO is active while a web console is connected. Use DIALECT CC after it disconnects.","5"]
```

Human replies retain the command's display contents. Bulletin routing uses
the formatted public line and sender, excluding private peer frame fields
such as PC93's IP. User-entered comments and bulletin bodies remain visible
as supplied. Keepalives carry only their type. Switch notices carry only the
reason or ending. None of these producers reads connector credentials,
upstream passwords or service API keys to construct a protocol record.

## Login And Session Behavior

- Clients log in with a callsign before commands are accepted.
- `telnet.prelogin_timeout_seconds` gives an admitted connection one budget
  from acceptance through registration, including negotiation and profile
  handoff. The shipped value is 30 seconds. After admission, timeout or
  shutdown interrupts socket activity; an expired login cannot replace an
  existing session. Admission and scheduling can delay that interruption.
  Synchronous storage or lock work already underway cannot be interrupted:
  it finishes before cleanup, and any profile changes already made remain.
  Admission-rejection notices use their separate bounded send period.
- Login rejects command-like and mode-like tokens such as numeric-only port
  strings, all-letter words, `FT8`, `NOFT8`, or `SET/NOFT8` before any
  per-user state is loaded.
- Valid login tokens must include a concrete call-like identity segment such as
  `K1ABC`, `DL6LD`, `4U1UN`, `P5/N1K`, or `W6TEST-1`.
- After existing callsign normalization, an optional login SSID must be one
  final hyphen followed by one or two ASCII digits (`0` through `99`). For
  example, `K1ABC-01` is valid and `K1ABC-100` is not. `K1ABC-1/P` is
  normalized to `K1ABC-1`. The spelling `-01` remains distinct from `-1`.
  Rejected logins do not load or change a user profile; old records are not
  migrated or deleted.
- When CTY data is loaded, login calls must resolve to a known CTY prefix.
- Base US calls (ADIF 291) must pass FCC validation; Canadian base calls (ADIF 1, 211, 252) must pass ISED assignment/plausibility checks when enabled and available. Portable location prefixes do not change the licensing source.
- Local TEST calls with CTY-valid US prefixes, such as `W6TEST` or `W6TEST-1`,
  bypass license validation but still require CTY validity.
- CTY or license data outages fail open so operators are not locked out by a
  reference-data refresh or missing local database.
- `telnet.max_connections` limits registered sessions. A reconnect with the
  same full normalized callsign and SSID can replace its existing session even
  when the server is full, subject to the IP limit below. A different SSID is a
  separate session. A new identity at capacity is rejected; the server attempts
  to send a full-server message before closing the connection. Capacity rejection
  does not change its saved profile or shared preset library.
- `telnet.max_sessions_per_base_call` limits registered sessions for one base
  callsign; the shipped value is 10, and 0 disables this limit. For example,
  `K1ABC-1` and `K1ABC-2` count toward `K1ABC`, but keep separate profiles.
  Existing own-call normalization removes numeric SSIDs; portable location
  forms such as `VE3/K1ABC` remain separate from `K1ABC`. The same full
  normalized callsign can replace its session at either limit, subject to the
  IP limit below. A different identity at the station limit is rejected with a
  station-specific notice;
  when both limits reject the login, the global full-server notice takes
  precedence. Notice delivery is best effort.
- Admission reject summaries include the station-limit reason. Sampled station
  rejection logs also include `tracked_session_bases`, the number of base
  callsigns currently held by registered sessions or pending logins.
- `telnet.max_sessions_per_ip` limits sessions from an observed connection IP;
  the shipped value is 10, and 0 disables this limit. Registered sessions and
  in-progress admissions count toward the limit. Multiple users behind a shared
  router, club network, contest network, or proxy can share one allowance.
  IPv4-mapped IPv6 addresses count with the equivalent IPv4 address. IPv6 scope
  zones are retained, so the same scoped address on different zones counts
  separately. The server uses the connection peer address, not a forwarded
  client address; this is an exact-IP limit, not a subnet limit. A station that
  changes IPv6 addresses can use separate allowances.
- A same-full-identity reconnect from the same IP can replace its owner at the
  IP limit. A reconnect from a different IP needs capacity at the destination;
  if that IP is full with registered sessions, the reconnect is rejected and
  the current owner and its saved data are preserved. If a new identity is
  full at multiple registered-session limits, notices take global, station,
  then IP precedence. Notices are best effort. With the IP limit enabled, a
  missing or invalid observed IP is rejected before profile writes. Disabling
  the IP limit preserves handling of non-nil peer addresses; a nil peer is
  handled defensively rather than causing a panic.
- When in-progress logins are using the remaining capacity under any enabled
  limit, a new identity or a cross-IP replacement can wait for their outcome
  within its existing login deadline. No ordering or fairness between waiting
  logins is guaranteed.
  Separate pre-login concurrency, rate and timeout limits still apply, including
  to reconnects. A pre-login rejection can therefore prevent a replacement.
  Blocked storage or registry access can still delay the login beyond that
  deadline.
- Shutdown is ordered with the final registration decision. If shutdown is
  signaled first, a waiting reconnect cannot replace its current session. If a
  replacement has already registered, both connections can be interrupted even
  if a replacement notification is blocked. A blocked write can first reach its
  existing deadline; scheduling and other synchronous work can delay cleanup.
  A duplicate-login notice is not guaranteed during shutdown.
- Callsign validation does not authenticate the person connecting. An accepted
  same-identity login can displace an existing session.
- The greeting can include dialect, grid, noise, and dedupe status from config.
- Dialect choice and filter state are persisted per callsign.

Restoration uses the full login call, including its SSID. Configuration,
associated preset name and its reference snapshot stay consistent through
replacement of an older connection. An old session cannot overwrite the new
session's record, including through a save already underway during reconnect.
The no-write guarantee above applies to capacity rejection. Other login
failures retain their existing persistence behavior.

If a valid record is restored but updating the login timestamp/IP fails, login
continues with those preferences and a warning.

Login resets recognized pre-seven profiles once. It discards old personal filters,
old applied associations and all six switch selections. Both GO and native CC
banks start from the same current operator mode/source defaults. Selected dialect,
unrelated configured settings and full-SSID login metadata remain. A saved CC
user stays in CC. Safe old fields are recognized structurally without compiling
expressions that will be discarded.

The shared base-call library clears old presets once. Its reset marker commits
with replacement, and valid new version-seven entries survive later profile or
notice retries from any SSID. A new SSID attempts library reset after successful
profile creation. Current-generation empty selections remain empty.

Reset completion and notification are independent. A full writer flush permits
metadata-only acknowledgement by the current connection owner; it does not prove
the user read the notice. Failed acknowledgement can repeat historical notice,
never completed reset work. Library failure is reported separately and does not
prevent an independently backed-up profile reset or unrelated preference saves.

Unsafe, malformed/future, over-budget or failed required upgrade records remain
protected. Temporary defaults and later human edits cannot overwrite them at
logout. SAVE/LOAD and machine writes cannot commit that profile. LIST/DELETE
retain their separate library behavior. Only initial authoritative profile absence
permits creation. A valid restore followed by timestamp/IP save failure retains
restored preferences and reports the metadata failure.

During upgrade, each eligible file needs admission and verified original backup
before replacement. Both backups precede library-first/profile-second writes when
both replacements are eligible. Files commit separately, with safe partial outcomes;
there is no two-file rollback. `.pre-v7.bak` backups never overwrite different
existing content. Ordinary saves and notice acknowledgements do not create backups.

Human changes admit the whole profile and promised views before publication;
late save failure leaves both banks active in memory with an unsaved warning.
Later complete save persists both. Machine updates and LOAD commit first.
Profiles stay limited to 1 MiB, with additional configuration, baseline, native
CC and decode-work bounds. See [full behavior and recovery](../docs/cc-dialect.md).

Manual DX reputation and posting allowances are shared by the baseline station:
`N2WQ-1` and `N2WQ-2` both use `N2WQ`. Their saved configuration remains separate.
An ordinary reconnect preserves an active spent allowance; normal elapsed windows
can replenish it, while configured risk penalties may tighten restrictions.
ASN/country history uses the baseline record. Old SSID history files are preserved
and are not merged. Posting counters are temporary runtime state, so restart or
state eviction can discard them; history does not persist those counters.
Gate refusals return `Spot rejected.` and queue no spot. Operators can inspect
their configured reputation-drop diagnostics for the detailed cause.

The handshake transcript tests in this package cover the visible login sequence.

## Command Pacing

Each logged-in connection has its own command allowance. The shipped settings
allow a burst of 30 attempts, replenished at 5 attempts per second. The allowance
starts full for each session. After it is used, the server waits for capacity
before processing the next attempt; it does not reject or disconnect the client
because of the rate. Commands stay in order, so pipelined commands, keepalives,
and logout may take longer. Actual startup and contest traffic have not been
measured against these values.

An attempt is a complete command line, including an empty keepalive, an unknown
command, a machine request header, or an invalid or overlong human line recovered
at a safe boundary. Telnet negotiation and the lines of a framed YAML upload do
not consume separate allowances. Login input has its existing separate controls.
An unsafe partial line or reader-level framing error keeps its existing immediate
terminal handling; a complete invalid machine header is paced before its normal
error handling. Existing machine framing and whole-line recovery remain in force.

The server reads at most one command attempt ahead of pacing, in addition to
existing bounded input buffers. Input echo, validation and recovery occur while
reading, before pacing; this is not a byte-rate limit. Shutdown or replacement
cancels a wait. After replacement, the old reader leaves its pending attempt
unprocessed and allows the existing writer to send the duplicate-login notice
and close the connection; shutdown can interrupt that notice. An attempt already
past its final check can still race with replacement. A remote
disconnect may be noticed only on the next read or a
writer failure. Each connection has its own allowance, so reconnecting starts a
new burst and several sessions do not share a rate.

Operators set `telnet.command_rate_per_second` and `telnet.command_burst` in
`runtime.yaml`; rate `0` disables pacing. The Telnet status display reports
`Command pacing waits`, the cumulative number of attempts that began waiting,
including waits later cancelled. The count resets when the server restarts; it
does not count unique users, rejected commands, or total wait time.

## Rejected Input And Recovery

An invalid or overlong human line is rejected as a whole. Text after the rejected
byte cannot execute as a second command, including while entering a callsign.
The server discards the remainder without applying editing keys or echoing it.
If it finds a safe line ending, it reports the input error and keeps the session.

Recovery discards at most 64 KiB of wire data and lasts at most five seconds.
An earlier login or session read deadline takes priority. Telnet negotiation
bytes and buffered data count toward the limit; the compatibility codec reserves
its hidden buffer conservatively, so a boundary near the limit may require an
earlier disconnect. Newlines inside subnegotiation are not command boundaries.

The server disconnects when recovery reaches either limit or cannot establish a
safe boundary. A partial line interrupted by EOF, timeout or transport failure
also disconnects, even if editing erased all its visible characters. An empty
logged-in idle timeout retains its existing retry behavior. Machine headers and
YAML uploads keep their separate terminal-error and upload limits.

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

### RBN CW speed

New user profiles start with CW speed ON. Use `SET CWSPEED OFF` to hide
available CW speeds on ordinary-text RBN spots and stored history, or use
`SET CWSPEED ON` to show them again. Existing profiles keep their saved choice,
and older profiles without the setting use OFF. The preference applies in both
GO and CC. `SHOW SETTINGS` shows the current choice.
Diagnostic mode hides speed. If a complete speed cannot fit on a spot line,
it is omitted. Comment filters and searches use the existing stored comment,
so they can match speed text that the setting hides from the displayed line.
Native CC11 uses the canonical raw comment instead; CWSPEED and diagnostic
display settings do not alter its comment.

The spot archive stores new speeds as spots arrive. For this upgrade, stop the
server and remove only the configured `archive.db_path` spot database before
starting the updated binary. Verify the `Archive: writing to` startup message:
an initialization error disables archiving. Removed spot history is not rebuilt;
new history accumulates over time. The separate bulletin archive is unaffected.

After that initial upgrade, the CW display correction needs no archive reset.
New spots use the corrected display. Older records keep their saved display,
including any affected by the earlier parsing defect, until they expire. To
remove those records sooner, stop the server and reset the configured spot
archive; this deletes all history in that archive.

## VE7CC Native Spot Output

After login, `SET/VE7CC` selects CC11-structured live spots and DX history.
`UNSET/VE7CC` restores ordinary text. Both commands are case-insensitive,
take no arguments, and work in GO and CC. They do not select a dialect or
save preferences. Every new or replacement connection starts in text, including
a connection whose saved dialect is CC. DIALECT, preset LOAD and YAML changes
preserve the current connection's native selection.

```text
SET/VE7CC
SHOW DX 10
UNSET/VE7CC
```

The existing writer applies the format transition at the acknowledgement's FIFO
position. Previously prepared output precedes it; subsequently formatted spots
use the new layout. Queued, unsent spots may therefore use the new layout.
Switching does not flush or discard backlog. Existing read pauses still suppress
spots without replay. Filters, query grammar and matching remain unchanged.

Records have 20 fields separated by 19 carets, one-decimal kHz, padded UTC dates,
`HHMMZ`, and one CRLF:

```text
CC11^frequency^DX^date^time^comment^DE^DX_ADIF^DE_ADIF^origin^DX_ITU^DX_CQ^DE_ITU^DE_CQ^DX_state^DE_state^DX_country^DE_country^DX_grid^DE_grid
```

The comment combines mode, a present report (including zero), and the canonical
stored comment, with the existing blank-beacon fallback. It does not use CWSPEED
display projections, diagnostic replacements or path glyphs. "Raw" does not mean
unvalidated wire input: native output replaces carets with tildes and
nonprintable/non-ASCII runes with spaces. Canonical spots, peer payloads and
archive data are not changed.

Stored ADIF remains authoritative. Missing positive ADIF stays blank; rendering
does not classify it again. Only missing Country/ITU descriptions may be
supplemented from current CTY when its positive ADIF matches stored positive
ADIF. Other stored metadata remains authoritative. Valid grids use the first
four characters, uppercase for reported and lowercase for derived locations.
Peer origins use the known source node; live manual spots may use this node.
Old manual-history origins and feed-label origins are not invented.

Each live record and each complete native history response is limited to
65,536 final bytes. History includes headings, warnings, continuation/status
lines and any automatic-pause footer. Conservative footer headroom may refuse a
large page; request fewer spots. Invalid/oversized live records are explicitly
omitted with bounded notices and diagnostics, never text-spot fallback. Native
history fails as a whole. A failed continuation keeps its prior token/position;
a valid fresh query has already replaced the old search. Format switches alone
do not invalidate continuation; existing settings/generation rules still apply.

Native history shares the configuration-output admission budget: 8 MiB
process-wide, a 1 MiB charge per owner and one large output owner per client.
History and configuration preparation/readbacks may return busy while slots
are held. Retry the request. The owner remains charged through writer flush
or discard; dequeue alone does not release it. This accounting is not an RSS
limit and does not cover every decoded archive-page/filter/runtime allocation.
Live spot delivery does not acquire a history-response reservation.

Stored GoCluster ADIF is not DXSpider WPXLOC numbering. A native client expecting
that table may misidentify countries, and may treat derived grids as actual.
CC12, other native event protocols, native settings readbacks, automatic client
detection and full CC User/HRD interoperability are not provided or qualified.
This port needs no archive reset or profile migration. See
[ADR-0351](../docs/decisions/ADR-0351-native-cc11-output-port.md).

## Filters

`HELP BANDS` lists individual bands and fixed meta bands: `WARC`, `HFCONTEST`,
`HF`, `VHF`, `UHF` and `VHFUP`. Meta names expand into physical members from one
shared catalogue; they do not define separate frequency ranges or add a filter
bank. GO accepts them in BAND operands, for example
`PASS RULE 0 BAND=WARC AND MODE=CW`. The slot edit still replaces the complete
rule. Readback and storage contain the expanded ordinary band list. In CC use
`SET/FILTER DXBM/PASS WARC`, preserving native incremental DXBM semantics.
See [fixed meta bands](../docs/domain-contract.md#fixed-meta-bands) for memberships,
ranges and compatibility. No profile or preset reset is required.

This describes the Boolean filter MVP in the current local checkout, not a
production deployment or a claim that new source files are published on main.
Private local-cluster acceptance remains pending. The telnet layer owns command
admission and publication. GO uses immutable Boolean rules; CC uses its own
immutable native categories and independently saved state.

GO uses the following personal-rule commands. CC owns separate native state;
see [CC commands, schema 7 and recovery](../docs/cc-dialect.md). The Boolean
examples below apply to GO.

```text
PASS RULE <expression>
PASS RULE <0..9> <expression>
REJECT RULE <0..9> <expression>
RESET FILTER RULE <0..9|ALL>
RESET FILTER
SHOW FILTER [RULE|FULL]
HELP RULE
```

Each occupied slot holds one action and one complete expression. Setting a slot
replaces both; it is not a delta or an addition to a category list.

`PASS RULE <expression>` is the rule 0 shortcut. It fills an empty rule 0 or
replaces the whole PASS expression there. Include each condition you want to
keep. Use `PASS RULE 0 <expression>` to replace a REJECT in rule 0. Every
`REJECT RULE` command requires a number. Use a free number to preserve your PASS
rules when you add a separate exclusion.

For AR-Cluster users, `SET DX FILTER BAND=20 AND NOT SKIMMER` and GoCluster's
`PASS RULE BAND=20M AND SOURCE=HUMAN` express the same 20m human-report choice.
AR-Cluster replaces its whole DX filter. GoCluster sets rule 0, and other
rules still apply. For example, a separate PASS for FT8 on any band can still
allow FT8 outside 20m. Use `HELP FILTER EXAMPLES` for both commands and
`SHOW FILTER` to inspect your saved rules. [AR-Cluster user manual](https://www.arcluster.org/user-guide/)

- Any matching REJECT vetoes delivery. Otherwise any active PASS can qualify.
- With no active PASS, personal selection is unrestricted except for matching
  REJECTs. With no rules there is no personal restriction; separate controls
  still apply.
- Slot IDs identify which rule to edit. When a PASS already exists, adding
  another PASS can let in more spots. To narrow a selection, edit or remove its
  existing PASS, including default slot 0.
- `RESET FILTER RULE` clears the selected slot or all slots.
  `RESET FILTER` clears all personal rules. Neither resets
  separate controls or profile settings, and neither reinstalls defaults.
- New accounts and recognized old-profile resets receive the same validated
  configured MODE/SOURCE selection as an ordinary editable PASS in slot 0.
  Both sources are written explicitly as SOURCE=HUMAN,SKIMMER.
  Current-generation empty rules remain empty.
- A PASS rule without SOURCE allows HUMAN and SKIMMER. SOURCE follows ordinary
  Boolean logic, including OR and NOT. Text inside COMMENT is literal text.
  Neither PASS nor REJECT adds an implicit source restriction.
  Shipped defaults select both sources; operator-configured defaults still apply.
  Existing rules with explicit SOURCE=HUMAN remain human-only, including
  previously generated defaults. Saved profiles and presets are not rewritten.
  Existing source-less PASS rules now admit matching skimmer reports too.

For example, replace slot 0 to require both the bands and modes:

```text
PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8
```

Other PASS slots can still qualify other spots. Use `SHOW FILTER RULE` before
editing; clearing all slots is deliberate, not a recipe for preserving them.
GO refuses retired category, MODE-delta, MINSNR, COMMENT-list, DXBM and NOFILTER
mutations without translation. Native CC mode, geography, DXBM and NOFILTER
commands have their own bank owner. There is no `RESET RULE`
or `SET DX FILTER` alias. `HELP RULE` is the shared expression reference.

Expressions support comparisons, the operand-free NEARBY predicate, parentheses,
NOT, AND and OR, with precedence
`NOT > AND > OR`. Equality matches any value in a bare or bracketed comma list,
for example `MODE=CW,FT8` or `MODE=[CW,FT8]`. `<>` means none of the listed values
only when the field is known. Ordinary comparisons with missing or unknown data
are false, including `<>`. NOT negates the comparison result.

Bare `ALL` is a case-insensitive wildcard in `=` and `<>` value lists for
every comparison field except DXCALL, DECALL and COMMENT. It includes missing or
unknown values. `PASS RULE 0 BAND=ALL AND MODE=CW` selects CW on any band.
`BAND=20M,ALL` also matches any band. The whole list must still be valid.
`BAND<>ALL` matches nothing. A `REJECT RULE` with `BAND=ALL` as its entire
expression vetoes every spot evaluated by the personal rules.
Telnet and YAML schemas 6 and 7 use the same expression syntax.

`DXCALL=ALL` and `DECALL=ALL` select the exact callsign. `COMMENT=ALL`
searches for the text `all`. Quote a literal catalog value named ALL, such as
`MODE="ALL"`. Use a numeric value for ordered comparisons, such as `SNR>10`.

| Fields | Values and matching |
| --- | --- |
| `BAND`, `MODE` | Active supported bands and taxonomy modes/aliases; blank MODE is the known token `UNKNOWN`. |
| `SOURCE` | `HUMAN` when `Spot.IsHuman` is true; otherwise `SKIMMER`. |
| `EVENT` | Taxonomy event bits, with `NONE` for eventless spots; no untagged exemption. |
| `DXCALL`, `DECALL` | Exact callsign patterns, one prefix/suffix `*`, or `*` alone; see below. |
| `DXCONT`, `DECONT` | Supported continent codes or bare `MYCONT`. |
| `DXZONE`, `DEZONE` | Integral CQ zones 1–40. |
| `DXDXCC`, `DEDXCC` | Positive ADIF integers, exact CTY canonical labels, or bare `MYDXCC`. Labels normalize to numeric codes. |
| `DXSTATE`, `DESTATE` | Canonical observed US/Canadian registered mailing codes; missing/invalid metadata is unknown. |
| `DXGRID2`, `DEGRID2` | Two-letter Maidenhead fields, `AA` through `RR`. |
| `FREQ` | Finite nonnegative operands in kHz; `=`/`<>` also accept inclusive low-high ranges. Spot frequency is known only when finite and positive. No extra rounding or epsilon is added. |
| `SNR` | Integral report whenever `Spot.HasReport` is true, including zero and human reports. |
| `CONF` | `?`, `S`, `C`, `P`, `V`, `B`; unknown for confidence-exempt modes, with no expression bypass. Otherwise blank confidence becomes known `?`. `CONFIDENCE` is not a field alias. |
| `PATH` | Exact `HIGH`, `MEDIUM`, `LOW`, `UNLIKELY`, `CLOSED`, `INSUFFICIENT`; absent/invalid prediction is `INSUFFICIENT`. |
| `COMMENT` | ASCII case-insensitive literal substring of the stored cleaned comment; see below. |
| `NEARBY` | Bare predicate with no operator or operands; either endpoint shares the user's band-selected H3 cell. |

Only FREQ, SNR, DXZONE and DEZONE accept `<`, `<=`, `>` and `>=`; ordered
comparisons require one value. `PATH=UNLIKELY` does not include CLOSED; use
`PATH=UNLIKELY,CLOSED` to select both. EVENT remains family-level: POTA and
POTA-1234 set the same family bit, while the reference stays in the comment.
Non-COMMENT operands are case-insensitive and normalized. Configured
GO MODE/EVENT names and mode aliases containing `_` are supported; native CC
modes use their own list and enabled shortcut grammar. Login
callsigns and machine request IDs retain their stricter rules.

DXCALL reads `DXCall`; DECALL prefers `DECallStripped`, falling back to `DECall`.
Patterns use ASCII letters, digits, `/` or `-`, with at most one `*` at either
end: `K1ABC`, `W1*`, `*ABC`, or `*`. Internal/multiple stars and `?` wildcards
are invalid. A lone star does not match an unknown empty call.

Separate controls are not expression fields. In both dialects,
`PASS/REJECT <BEACON|WWV|WCY|ANNOUNCE|SELF> [ALL]` enable/disable the
named control. PC93 aliases ANNOUNCE. These switches accept no value or one
`ALL`; `PASS BEACON OFF` is refused. Old NEARBY toggle commands are refused;
use a numbered expression such as `PASS RULE 0 MODE=CW AND NEARBY`.
CC `SET/FILTER` and `UNSET/FILTER` retain these control forms. Native geography
and DXBM also belong to SET/FILTER. Inline
`SET/FILTER <switch>/ON|/OFF` takes no extra values; UNSET has no inline flags
and there are no NEARBY control aliases. Named CC control shortcuts such as `SET/BEACON` and
`SET/SELF` take no arguments; [native mode/category commands](../docs/cc-dialect.md#terminal-commands)
have their own grammar and do not mutate GO rules.

With SELF enabled, live spots of your own call can bypass personal rules
while other delivery checks still apply. History has its own same-call
exception independent of SELF. Explicit history BAND/MODE/COMMENT clauses
still apply.

Ordinary live broadcasts keep each active session on one worker, preserving its
queue order when other clients connect or leave. Departures can leave worker
load uneven; new sessions fill the shorter shards. Direct SELF delivery remains
a separate immediate path and can overtake ordinary broadcasts. Filtering,
pause and bounded-queue drops still apply. See [ADR-0339](../docs/decisions/ADR-0339-stable-broadcast-affinity.md).

Rule limits apply before publication:

| Boundary | Limit |
| --- | --- |
| Occupied slots | 10; IDs 0–9 |
| Each expression | 1,024 raw bytes and 1,024 canonical bytes |
| Each rule | 32 atoms, 128 nodes, nesting depth 8 |
| Values per comparison atom | 32 |
| Complete rule set | 128 atoms, 512 values |
| Each COMMENT operand | 1–64 printable ASCII bytes, at least one non-space |

Nesting counts parentheses and NOT, not the height of a flat AND/OR tree.
These are distinct from the configured human line limit, machine body/final
response limits, stored-byte limits and decode-work budgets. One prepared
program per rule is retained immutably; edits prepare replacements without
changing snapshots already in use. Live, command and history matching capture
coherent controls, cells and path inputs with those rules, without another
category gate. Rendering reads prepared programs; it does not compile them.

Current checkout source routes: `filter/rule_set.go` owns preparation and
combination, `rule_compile.go`/`rule_evaluate.go`/`rule_view.go` own field
semantics. NEARBY is evaluated lazily in the same program and its result is
reused for that spot/user evaluation.
`telnet/boolean_input.go`, `boolean_rule_commands.go` and
`boolean_rule_readback.go` own human admission/display;
`matching_snapshot.go` and `history_filter.go` capture matching state.
`commands/boolean_rule_help.go` supplies the registered HELP RULE reference.

## Comment Filters And Searches

Saved COMMENT selection uses numbered expressions, not phrase lists:

```text
PASS RULE 0 BAND=20M,40M AND COMMENT="POTA"
REJECT RULE 9 COMMENT="QRT"
PASS RULE 0 COMMENT="CQ ""DX""  !;=()[]"
SHOW FILTER RULE
SHOW DX K1ABC 20 COMMENT POTA
SHOW DX K1ABC 20 BAND 20m,40m MODE CW,FT8 COMMENT POTA
SHOW MYDX 20 COMMENT up 5
```

Each COMMENT operand contains 1–64 printable ASCII bytes with at least one
non-space byte. Quote expression values containing spaces or punctuation.
Double a literal quote as `""` inside a quoted value; backslash is literal,
not an escape. Case, punctuation and all spaces inside the value are preserved.
`*` and `?` have no wildcard meaning in COMMENT. Matching is a literal
substring, not a regex, glob or word match. Only the stored cleaned comment is
searched, not removed metadata tokens, synthetic beacon fallbacks or
per-user diagnostics. Unknown/empty comments follow ordinary unknown comparison
semantics. Rules combine as described under [Filters](#filters).
New human comments retain meaningful times and `CWT`; legacy skimmer time
cleanup remains unchanged. Existing archive rows are not repaired or re-parsed.

The human RULE reader preserves expression bytes, including literal case and
spaces; compilation normalizes syntax and non-COMMENT operands. The configured
whole-command ceiling still applies (128 bytes by default). Use schema 6 YAML
for longer expressions. The [rule budgets](#filters) replace old per-list limits.
There are no COMMENT move/remove/list-reset commands.

Manual DX comments accept ASCII letters, digits, spaces, and `/ # @ - . , * ? _ +`.
Other punctuation and non-ASCII characters are rejected before posting. The
whole edited DX command uses the node's command-byte limit (128 by default),
excluding its line ending. Mode/report tokens may be parsed from the comment.
The `+` exception applies only to the comment after both DX arguments; it does
not broaden login, frequency/callsign arguments or other command grammars.

Human shorthand reports require an immediately adjacent signed integer after
the first selected FT8 or FT4 mode, within -30 through +30 and allowed by the
taxonomy. For example, `FT8 +02` is a report, while `CW UP 2` and
`FT8 CQing +2 in NC` retain their numbers as text. Explicit `dB` reports keep
their existing behavior. See [human spot comments](../spot/README.md#human-spot-comments).

Human filter and settings edits are prepared before publication; a rejected edit
leaves live preferences and saved bytes unchanged. Accepted edits retain existing
live-update-then-save behavior; a disk failure
is logged and reported, and does not roll back a human filter edit. Filter,
rule/reset and changed DIALECT commands append a warning when saving fails:

```text
warning: failed to persist; changes are active but were not saved: <reason>
```

The accepted change is active in this session. A new login restores the last
successfully saved settings. Protected temporary settings preserve the refused
record, including during logout. An unchanged DIALECT does not attempt another
save. Repeated rule sets retain save intent; removing an already empty slot is
a no-op. A later successful preference save can persist current
live settings when the record is writable and valid.

`SHOW DX`/`SHOW MYDX` accept COMMENT after their existing optional selector/count
arguments. Existing aliases retain their restrictions. Everything after COMMENT
is the search phrase; count/selector options must precede it. Unlike expression
quoting, archive phrase grammar trims surrounding ASCII spaces and treats
quotes, commas, `*`, `?`, ALL and NONE as literal text. It requires 1–64 printable
ASCII bytes and a non-space byte. This phrase is a
mandatory additional condition, including for self-spots; saved user filters
retain their existing self exceptions. Search changes no preferences. Matching
occurs before rows are counted, and returned NEXT commands retain the phrase.
Changing saved comment rules invalidates continuation and pending publication.
Retention, page counts, work budgets, errors and warnings remain as documented
under [archive history](#archive-history-and-continuation).

GO rules survive reconnects and tagged presets through [schema 7](../docs/cc-dialect.md#yaml-and-backups)
and [native v7](#persistence-format-and-rollback). The retained schema-six GO
interface edits only the active GO bank. Older COMMENT-list
mechanics in [ADR-0261](../docs/decisions/ADR-0261-literal-comment-filter-and-history.md)
are historical, not a supported compatibility engine.

## Your Operating Geography in GO Rules

Use `MYCONT` for your operating continent and `MYDXCC` for your operating DXCC
entity. CTY (the cluster's callsign geography database) supplies both from your
login callsign. `N2WQ/VE3` uses `NA` and Canada (ADIF `1`) for that connection.
The lookup uses the radio callsign without its numeric SSID, so
`N2WQ/VE3-01` also uses Canada.
Reconnect to use updated CTY data. The cluster uses the same session values for
live spots and filtered spot history. `SET GRID` supplies location for features
such as NEARBY and PATH. The login callsign supplies operating geography.

| GO expression | Selects |
| --- | --- |
| `DECONT=MYCONT` | Reports from spotters on your operating continent. |
| `DXCONT=MYCONT` | Spotted stations on your operating continent. |
| `DXDXCC=MYDXCC` | Spotted stations in your operating DXCC entity. |
| `DEDXCC=MYDXCC` | Reports from spotters in your operating DXCC entity. |
| `DECONT=MYCONT AND DXDXCC<>MYDXCC` | Reports from your continent about stations outside your entity. |

This CW example replaces PASS slot 0.

```text
PASS RULE 0 MODE=CW AND DECONT=MYCONT AND DXDXCC<>MYDXCC
```

Other PASS slots can still admit a spot. Use `SHOW FILTER RULE` to check all
slots. Use `RESUME` to end a readback pause and resume live spots.

Use the names without quotes, in either case, with `=` or `<>`. You can mix
them with fixed values in lists, for example `DECONT=MYCONT,EU` or
`DXDXCC=MYDXCC,291`. `SHOW FILTER`, YAML, profiles, and presets retain the
variable names. Copy the expression as written to use it with another login.

If CTY is unavailable at login, both session values are unknown. A comparison
against an unknown spot value or an unknown session value is false. In a list,
a known member can still satisfy `=`. For example, `DECONT=MYCONT,EU` can match
an EU spotter while `MYCONT` is unknown. `<>` needs known values to establish
that the spot differs from every list member. The `ALL` wildcard still applies
when present. `NOT` negates the result, so `NOT DXDXCC=MYDXCC` includes unknown
cases while `DXDXCC<>MYDXCC` selects known differences.

Use these operands inside GO numbered rules, including GO rules submitted by
YAML. Native CC uses its own filter syntax. History entity selectors and
`SHOW DXCC` have their own syntax. Upgrade uses the existing profile and preset
format. If rolling back to an older executable after saving either operand,
use compatible profile and preset-library backups with that executable.

## Canonical DXCC Prefixes

Numbered DXDXCC/DEDXCC comparisons accept positive ADIF integers and exact
canonical CTY labels in human commands and YAML PUT/PATCH/VALIDATE requests:

```text
PASS RULE 0 MODE=CW AND DXDXCC<>K
REJECT RULE 9 DEDXCC=IT9,3D2/R
REJECT RULE 8 DXCALL=IT9*
```

The first becomes `MODE=CW AND DXDXCC<>291`. With the corresponding CTY data,
the second becomes `DEDXCC=248,460`: I, IG9 and IT9 all select entity 248, not
only Sicily. The third matches literal callsign spelling, not a DXCC entity.
Labels match canonical Prefix fields after trimming and uppercasing, not
callsign lookup keys; W6 is not an alias merely because it looks up under K.
Slash labels retain their canonical meaning. Unknown or conflicting labels
refuse the entire edit, including rules containing NEARBY.

One lazy CTY snapshot covers all labels in one request, including every YAML
slot. Numeric-only requests need no CTY or membership check. Accepted responses,
readbacks and saved rules contain numeric expressions; reload and matching never
resolve labels. A later CTY refresh does not reinterpret saved numeric operands.
Native profile/preset files remain numeric-only, without a schema change.
Repeated operands retain their order and count toward the existing limits.
The Boolean grammar is unchanged; write a signed positive number as `"+291"`.
A spot's ADIF metadata must be positive to be known, including for `<>`.

SHOW DX and SHOW MYDX recognize canonical CTY labels before exact-call or
prefix classification, including SHOW MYDX 3D2/R 10. Numeric history arguments
remain counts; SHOW DXCC detail lookup retains its existing behavior.

## Minimum SNR

Use an explicit SNR expression to suppress weak reports. For example:

```text
REJECT RULE 1 SOURCE=SKIMMER AND MODE=CW AND SNR<8
```

This explicitly targets skimmer CW reports below 8; a report equal to 8 does
not match the rejection. SOURCE=SKIMMER is a user-selected condition, not an
implicit human exemption. SNR is known whenever `Spot.HasReport` is true,
including zero, negative and human reports. Missing SNR makes comparisons
false, including `SNR<>8`; `NOT SNR<8` is true when SNR is missing.

There is no per-mode MINSNR map, dormant threshold list or old-schema write
path. Edit/clear the numbered slot instead. Live and archive matching use the
same prepared rules with their existing self exceptions; changes invalidate
history continuation and pending pages.

## WWV and WCY Bulletin History

At login, the latest available WWV and WCY bulletins are shown regardless of
the live WWV/WCY filter settings, without an added receipt-time prefix. Later
announcements follow those filters.
Use `SHOW WWV` or `SHOW WCY` for the latest five retained bulletins and add
`FULL` for that type's retained bulletins from the last 24 hours. Both dialects
also accept `SH WWV`, `SHOW/WWV` and `SH/WWV`, with equivalent WCY forms;
put `FULL` after a space. Numeric count suffixes are not supported. Explicit
requests also bypass the live bulletin filters.

The archive uses cluster receipt time and holds at most 48 bulletins per type.
Its text is capped at 256 characters per bulletin, with longer text marked as
truncated. Capacity loss and detected or possible storage gaps are reported;
the first installation cannot recover bulletins from before it was running.
History survives orderly restarts when its dedicated storage is available;
an unclean shutdown can leave it incomplete. For 24 hours after every startup,
output warns that history may be incomplete because a previous storage failure
may have prevented recording a shutdown marker. Live bulletin text is not
truncated by this archive limit. See [command details](../commands/README.md#wwv-and-wcy-bulletins).

Archived bulletin display removes or replaces control characters, including
when reading older snapshots. Live announcements retain their existing text
and filtering behavior. Duplicate detection uses the original bulletin text.

If a snapshot cannot be loaded, the storage worker must preserve an independent
recovery copy before it replaces the snapshot. The fixed recovery paths are
`<snapshot>.recovery` and `<snapshot>.recovery.tmp`. Check the cluster log for
the storage warning and preserve these files for investigation. An unreadable
or oversized original, or a conflicting or incomplete recovery file, can block
new snapshots; new bulletins remain in memory and may be lost on restart. Once
the underlying problem is corrected, the worker retries. If a recovery slot is
occupied by unrelated or incomplete data, move it to a safe location before
retrying; do not discard the only recoverable copy. History output continues to
warn about a possible gap after storage recovers. Filesystem and hardware
failure guarantees depend on the platform and its storage. The cluster owns
the active snapshot while running: stop it before replacing or editing that
snapshot. History held only in memory can be lost when the cluster stops.

The shipped greeting places `<STARTUP_INFO>` before its prompt, after the
welcome. Custom greetings without that marker are preserved and follow the
startup information. The marker and rollback instructions are documented in
the [configuration guide](../data/config/README.md).
Live messages can interleave with startup output.

## Archive History And Continuation

History BAND lists accept the same fixed meta names. For example,
`SHOW DX 20 BAND WARC` or CC `SHOW/DX 20 BAND VHF` selects the shared physical
members without changing saved filters. A query containing a meta name
classifies each candidate's reported frequency through the shared catalogue,
including retained 4m records with an old unknown band label. The query runs
before counting and continuation; it does not rewrite archive records.

`SHOW DX` and `SHOW MYDX` share archive history selection and paging. Counts
range from 1 to 250, default 50; selector and count may appear in either order.
`SH DX`, `SH MYDX`, `SHOW/DX`, and `SH/DX` work in both dialects. An
unambiguous canonical CTY label selects its entity first. Other full
valid calls select their exact normalized stored DX identity; other supported
prefixes select their resolved entity. Supplied selectors require loaded CTY,
but valid full calls with unresolved countries still work as exact searches.
See [commands history](../commands/README.md#archive-history) for examples.

Append `BAND <list>` and/or `MODE <list>` after selector/count, then optional
`COMMENT <phrase>` last. Lists require commas between values, allow spaces around
commas, use OR within each category and AND across categories. BAND and MODE may
appear in either order, once each.
Use the existing band names/normalization (`20` or `20m`) and filter-mode
names/aliases, including `UNKNOWN` for blank modes. Duplicated values are
deduplicated; empty comma fields remain ignored. Supported BAND/MODE/COMMENT mode
aliases are values first or after a comma; otherwise standalone keywords start
clauses. Finish a list without a comma before starting another clause. Missing
commas, unsupported or missing values, repeated categories and BAND/MODE
`ALL`/`NONE` reject the request and preserve the previous cursor. Explicit query
selections are required even for self-spots, apply before counts and narrow your
saved filters. Saved-filter self exceptions are unchanged. Searches do not edit
preferences; NEXT retains the selected lists and phrase. The existing command
byte limit still applies. See
[ADR-0262](../docs/decisions/ADR-0262-history-band-mode-selections.md) and
[ADR-0263](../docs/decisions/ADR-0263-history-list-comma-boundaries.md).
With COMMENT mapped to CW, use `MODE COMMENT COMMENT POTA` for CW and POTA
text. Without that supported alias, `MODE CW COMMENT POTA` keeps its ordinary
meaning. Comma-embedded COMMENT is a list value and must be supported. See the
[COMMENT alias boundary](../docs/decisions/ADR-0273-history-comment-alias-boundary.md).

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

Type `SHOW DX NEXT` on the same connection to continue the current search.
Ordinary Telnet does not print a code. VE7CC keeps the returned
`SHOW DX NEXT <token>` form, which remains accepted. Tokens are
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
| GO: `SHOW FILTER`, `SHOW FILTER RULE` | Current settings, canonical rules and individual GO controls. |
| GO: `SHOW FILTER FULL` | Adds separate-control detail and one-program preparation counts. |
| CC: `SHOW/FILTER`, `SH/FILTER`, `SHOW/FILTER FULL` | Complete native mode/source, geography, band overrides, DXBM and CC controls. |
| CC: `SH/FILTER DOC` (or another supported category) | Inspect that native category. |
| Both: `SHOW SETTINGS` | Configured preferences and effective behavior, followed by session status. |

`SHOW/FILTER` and `SH/FILTER` are aliases in GO, with RULE/FULL arguments.
CC accepts DOC, DOS, DXCTY, DXSTATE, DXBM, MODE, SOURCE, BEACON, WWV, WCY,
ANNOUNCE, SELF and FULL. CC has no Boolean RULE view and refuses Boolean
rule mutations. `SHOW FILTER` also works as a CC alias. SETTINGS aliases are
`SHOW/SETTINGS` and `SH/SETTINGS` in both dialects.

In GO, every occupied slot appears in slot order with its action and full canonical
saved expression. No synthetic source condition is added.
FULL reports occupied slots and one program's node/atom/value counts. There are
no projected expressions, suspended rules or global NEARBY status fields.
Delivery switches such as BEACON, WWV, WCY, ANNOUNCE/PC93, and SELF
operate separately from numbered rules. When location is unavailable,
see [NEARBY behavior](#grid-noise-and-nearby).

The supplied greeting includes a short `<LOGIN_SUMMARY>`. Operators can place
`<SETTINGS_FILTERS>` in a custom greeting to show the full block. It uses
"Default settings and filters" for new accounts and "Current settings and
filters" for returning accounts. Values come from the restored or prepared
session. A profile reset gets the reset notice and first-time guidance.
The GO block shows canonical rules, and the CC block shows native selections.
The short summary includes the PASS shortcut on first GO login and directs
AR-Cluster users to `HELP FILTERS`.

`SHOW DXCC` (also `SH DXCC`) requires one prefix or callsign. Extra values
return usage without performing a lookup. `BYE`, `QUIT` and `EXIT` take no
arguments. A malformed logout returns usage and leaves the connection usable;
a valid logout sends the farewell and closes the connection.

`SET GRID`, `SET NOISE`, `SET PATHSAMPLES`, `SET SOLAR`, `SET DEDUPE` and
`SET DIAG` each take exactly one value. Extra words return usage without changing
the setting. `CONFIDENCE` is an alias for the `CONF` diagnostic mode. `SHOW DEDUPE`
takes no value. Use `DIALECT` to see your current choice, `DIALECT LIST` to list
choices, or `DIALECT <name>` with one name to switch. Malformed SOLAR commands
leave the schedule unchanged; a valid human command deliberately restarts its
schedule, even if you select the same cadence.

`SET SOLAR 15|30|60` saves the cadence for summaries at aligned UTC ticks.
Messages are sent only while solar conditions are active. On a node where solar
weather is disabled, the cadence stays saved, feedback says it is unavailable,
and machine status reports an effective cadence of zero. `SHOW SETTINGS` keeps
the configured cadence visible. `SET SOLAR OFF` stops summaries.

`SHOW DEDUPE` and `SHOW SETTINGS` keep your saved DEDUPE choice visible. If SLOW
is saved but this server only enables FAST, both show SLOW with effective FAST
and explain that SLOW is unavailable. NEARBY does not change the effective
policy. If all secondary policies are disabled, the readback says
duplicate suppression is disabled. Reading never replaces your saved choice.
Unusual saved strings remain exact, with quotes and escapes when needed;
when describing an effective lane, unrecognized values are identified separately
from unavailable policies.

Use `GET YAML FILTER SCHEMA 7` for the tagged active bank and its controls,
or `GET YAML CONFIG SCHEMA 7` for a complete both-bank backup. The legacy
`GET YAML FILTER SCHEMA 6` exposes rules/controls only while GO is active.
`DEFAULT` remains distinct from explicit `true` or `false`.
`SHOW SETTINGS` preserves exact saved GRID bytes even when they require escaped
display. A successful filter readback includes the complete selected view.

Every human readback line has at most 78 printable ASCII characters followed
by CRLF, including headers, active values, explanations, errors and pause
footers. GO's short numbered rules use directly reusable expression syntax, without
outer display quotes. Long values use Go-style double-quoted ASCII escapes for whitespace,
quotes, backslashes, control characters and non-ASCII values. Long individual
values use complete quoted pieces joined by `+`. For illustration, a displayed
expression value may be split like this:

```text
"COMMENT=\"CQ "
+ "\"\"DX\"\"\""
```

Decode the Go-quoted pieces and concatenate them to recover
`COMMENT="CQ ""DX"""`. Indentation, line endings and `+` are presentation only.
Leading/trailing spaces inside display quotes belong to the value; escapes
are never split or trimmed. This display encoding is not expression syntax,
and a long displayed row is not a copyable command. Expression quotes
use doubled quotes and literal backslashes, not Go escapes. Human commands
still have the configured line limit (128 bytes by default); use schema-seven
GO FILTER YAML, or the retained schema-six GO interface, for longer rules.

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
illustrate a configuration; runtime output reads active values.
Diagnostics, pause state and protected temporary defaults appear under
Session only. Pending delivery says `Paused for reading; at least 30s after
delivery`, without claiming that the reading countdown has begun. Both human
views show the preset row. Its `(modified)` flag compares preferences with the
applied/saved reference snapshot described below.

In text and VE7CC, every human readback, including a size error, suppresses live spots
from command acceptance, before preparation or any transaction wait, through
queueing and delivery. It then provides the full reading interval after a
successful server write and flush. This does not measure terminal rendering.
The positive configured pause duration is used; zero selects 30 seconds for
these commands. The row threshold is ignored, including zero. A longer active
pause is preserved. Later valid, processed PAUSE/RESUME commands supersede
earlier pending completion effects; a subsequent human readback starts a fresh
hold. Suppressed counts carry from delivery into the reading interval.

For a 30-second reading interval, the human footer remains:

```text
Live spots paused for 30 seconds. Missed spots aren't replayed.
Type RESUME to restart now, or SHOW HOLD to check the pause.
```

GO filter output and shared SETTINGS responses are limited to 65,536 bytes;
native CC filter output allows 262,144 bytes. Both count CRLF conversion,
headers and human footers. Generation either completes within
the limit or returns an explicit error. ASCII-escape expansion is budgeted
before unrestricted quoting. Exact-value scratch space is bounded by the line
width, and the complete rule body is admitted before output allocation.

## Client YAML Configuration

Schema 7 is the current complete both-bank interface. See the
[resource ownership table and upload rules](../docs/cc-dialect.md#yaml-and-backups).
The schema-six examples below describe the retained legacy GO/settings interface:
FILTER/CONFIG require effective GO, SETTINGS/CAPABILITIES also work in CC with
warnings and no fabricated Boolean state. Schema six is not a full backup.

The following table describes schema six. FILTER/CONFIG operations require
active GO, and CONFIG proposals must leave GO selected. SETTINGS/CAPABILITIES
work in either dialect; SETTINGS can switch the selected bank. For native CC
filter edits and complete backups, use schema seven.

| Command | Purpose |
| --- | --- |
| `GET YAML FILTER SCHEMA 6 [ID <id>]` | Read saved numbered rules, NEARBY and separate controls. |
| `GET YAML SETTINGS SCHEMA 6 [ID <id>]` | Read all six writable settings. |
| `GET YAML CONFIG SCHEMA 6 [ID <id>]` | Read filters and settings in one consistent snapshot. |
| `GET YAML CAPABILITIES SCHEMA 6 [ID <id>]` | Discover schema, rule fields/operators/vocabularies and bounds. |
| `PUT YAML FILTER`, `PUT YAML SETTINGS`, `PUT YAML CONFIG` | Replace the complete writable resource. |
| `PATCH YAML FILTER`, `PATCH YAML SETTINGS`, `PATCH YAML CONFIG` | Change supplied fields, preserving omissions. |
| `VALIDATE YAML CONFIG` | Check a complete proposal without applying or saving it. |

GET replies contain `schema_version`, `request_id`, `resource`, an opaque
`revision`, `configuration` and read-only `status`. GET assigns an ID if it is
omitted; a supplied ID preserves case and must contain 1-32 ASCII letters,
digits or hyphens. Every GET, including SETTINGS and CAPABILITIES, requires
explicit `SCHEMA 6` before its optional ID. Missing, older and unsupported
schemas are refused, not translated.

FILTER's writable `configuration` directly contains the filter fields;
SETTINGS directly contains setting fields. CONFIG contains `filters` and
`settings` mappings. Extract that writable section when creating a new upload;
do not send the GET envelope's resource, revision or status fields as writable
data. Preset association, effective choices, server defaults, diagnostics and
pause state cannot be set through a configuration upload. The preset reference
snapshot itself is not exposed in GET.
NEARBY exists only in the canonical rules. Generation 6 has no
`nearby_enabled`, `nearby_active`, projected-expression or suspended-rule fields.
A schema-6 proposal containing even `nearby_enabled: false` is refused.

Schema-six CAPABILITIES advertises `supported_schema_versions: [6]`, the seven filter and
six settings field names, lowercase rule actions, DEFAULT/boolean choices,
comparison fields/operators/geographic flags, operand-free `rule_predicates`,
active vocabularies and rule limits. NEARBY has operand_count 0 and an empty
operators list; it is distinct from comparison fields.
It also reports upload/response bounds, complete-PUT, collection-replacement
and revision requirements. It is not the former per-server settings-choice
availability matrix: runtime settings admission remains authoritative.
Read-only `status.rules` contains `configured_count`, one `program` mapping
with nodes/atoms/values, and canonical `{slot, action, expr}` entries.

Current dispatch starts in `telnet/machine_commands.go` and
`handleV7MachineCommand`. Schema-six requests delegate to the retained parser
and the `machine_v6_banked_commands.go`/`machine_v6_banked_readback.go` adapters;
`machine_v7_*.go` owns the complete both-bank interface. Retained prepared rules are shared immutably when
omitted from PATCH, not recompiled by rendering.

### Writable Values

| Resource fields | Representation |
| --- | --- |
| `rules` | Sequence of mappings requiring `slot` (integer 0–9), lowercase `action: pass` or `action: reject`, and `expr` (complete expression string). Duplicate slots fail. |
| `include_beacons`, `allow_wwv`, `allow_wcy`, `allow_announce`, `allow_self` | Explicit `true`, explicit `false` or the string `DEFAULT`. |
| `dialect`, `grid`, `noise_class`, `dedupe_policy` | Strings; an empty string preserves the explicit default/lookup selection. |
| `path_min_observation_count` | Integer; zero uses the cluster minimum. |
| `solar_summary_minutes` | Integer: 0, 15, 30 or 60; zero means OFF. |

FILTER contains exactly `rules` and the six controls listed
above. SETTINGS contains exactly `dialect`, `grid`, `noise_class`,
`dedupe_policy`, `path_min_observation_count` and `solar_summary_minutes`.
CONFIG nests those resources under `filters` and `settings`.

Preserve DEFAULT versus explicit false/true, empty default/lookup strings,
zero sentinels and `rules: []` during GET/edit/PUT. Settings strings remain
exact, subject to runtime validation; expression canonicalization normalizes
syntax/domain operands while preserving COMMENT literal bytes. Named-preset
adjustments are not machine validation: unknown/unavailable choices, such as
SLOW when disabled, fail unchanged rather than silently falling back.

PUT requires every field in its resource. CONFIG PUT and VALIDATE require
complete filters and settings together. PATCH preserves omitted fields, but a
supplied `rules` list replaces every occupied slot, not just matching slot IDs.
`rules: []` clears all slots while omitted separate controls remain unchanged.
There is no per-slot PATCH merge.

### Upload And Revision Examples

These are source-derived, codec-shaped schema-six examples, not executed server
or codec validation results. Select GO before the FILTER/CONFIG examples.
First read your own current configuration:

```text
GET YAML CONFIG SCHEMA 6 ID Read-1
```

For `VALIDATE YAML CONFIG`, copy the entire writable `configuration` from
a GET reply into a new schema-six request envelope. Preserve all fields.
For `PUT YAML CONFIG`, also include `if_revision` from the current GET.
FILTER and SETTINGS place fields directly under `configuration`.

A complete settings replacement is:

```text
PUT YAML SETTINGS
---
schema_version: 6
request_id: settings-Ab1
if_revision: "replace-with-current-GET-revision"
configuration:
  dialect: ""
  grid: FN42
  noise_class: URBAN
  dedupe_policy: FAST
  path_min_observation_count: 0
  solar_summary_minutes: 0
...
```

CONFIG can change a rule and location together, subject to ordinary GRID admission:

```text
PATCH YAML CONFIG
---
schema_version: 6
request_id: nearby-Ab1
if_revision: "replace-with-current-GET-revision"
configuration:
  filters:
    rules:
      - {slot: 0, action: pass, expr: 'MODE=CW AND NEARBY'}
  settings:
    grid: FN42
...
```

Rule-only recovery can waive mapping availability for an unchanged saved GRID;
its syntax and every other settings check remain mandatory. See
[NEARBY](#grid-noise-and-nearby).
A PATCH can clear every personal rule while preserving separate controls:

```text
PATCH YAML FILTER
---
schema_version: 6
request_id: Clear-1
if_revision: "replace-with-current-GET-revision"
configuration:
  rules: []
...
```

Each example is a separate request; its sample revision is illustrative.
PUT/PATCH require a matching revision, an opaque ASCII string of at most
128 bytes. GET again after reconnect, server restart or a conflict before
retrying. Pause, diagnostics and login metadata do not count as configuration
edits. Slot order is canonical, not a priority. Rule identity uses the
canonical action/expression in each slot.

The strict writable envelope permits only `schema_version`, `request_id`,
`if_revision` and `configuration`. Uploads require `schema_version: 6`,
`request_id` and `configuration`.
PUT/PATCH additionally require `if_revision`. VALIDATE CONFIG requires the
complete configuration; `if_revision` is optional because it does not write.
Its result reports `valid: true`, `applied: false` and `persisted: false` on
success. Successful writes report their operation, resulting revision and
`applied: true`, `persisted: true`.

Validation or persistence failure leaves both the live and durable
configuration unchanged. Machine writes preserve the associated preset and
reference; changing preferences can set or clear `(modified)`. Even an
unchanged PUT persists before success, so an earlier failed human autosave can
be repaired without resetting solar timing, GRID-owned geometry, diagnostics
or pause. A lost acknowledgement does not undo a committed write; GET again
to discover the current revision and configuration.

A fully received unavailable-choice request gets a framed error, for example:

```yaml
---
schema_version: 6
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
fields and additional documents are rejected. Rule slots are checked for
duplicates after integer interpretation, so two entries selecting slot 1 fail
even if their numeric spellings differ. Rule entries accept only `slot`,
`action` and `expr`; every entry is complete.

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

`SAVE PRESET <name>`, `LIST PRESET`, `LOAD PRESET <name>` and
`DELETE PRESET <name>` share 20 names across the base callsign's numeric SSIDs.
A preset stores the active GO or native CC bank, its six controls and shared
preferences with an explicit dialect tag. LOAD restores that bank, announces a
dialect switch, leaves the other bank intact and restarts the solar schedule.
Login/IP metadata, diagnostics, pauses and derived caches are not preset choices.

If the node cannot apply a saved dedupe choice or PATHSAMPLES override, LOAD
announces its adjustment and records the applied choice in the profile and that
bank's baseline. For example, disabled SLOW may become FAST; reconnect retains
FAST even if SLOW is later enabled. The shared named preset still contains SLOW.
Configured empty GRID and inherited defaults retain their meanings. Ordinary
login/readback preserves saved choices and reports effective fallback separately.

Each bank keeps an independent association and immutable baseline across
reconnect. Shared preference changes can mark either baseline modified. Deleting
or overwriting the library name does not change an applied baseline. Failed LOAD
changes neither live nor saved preferences. SAVE first admits both library and
profile; if library replacement succeeds but association persistence fails, it
reports that partial success and keeps both prior associations.

See [preset ownership, limits and recovery](../docs/cc-dialect.md#presets-limits-and-recovery).
Malformed/future libraries remain protected. One-time upgrade reset clears old
presets once; presets created afterward survive unfinished profile/notice retries.
Collections are read on demand under a fixed base-call lock stripe, with no
library cache or background worker. One process may write a user-data directory.

### Persistence Format And Rollback

Current files use `configuration_version: 7`. User records contain
`configuration.filters.go`, `configuration.filters.cc`, shared `settings`,
per-bank `presets`, login metadata and fixed `notice` fields. Library files
contain their version, durable `reset_generation` and tagged named entries.
This differs from the legacy schema-six machine interface and disk shape.

Profiles remain limited to 1 MiB, tagged presets to 192 KiB within the existing
256 KiB physical cap, and libraries to 8 MiB/20 names. Whole-document decode work
is bounded at 2 MiB per profile and 16 MiB per library; it is not a heap-size
claim. Native CC canonical state is limited to 128 KiB and complete schema-seven
frames to 256 KiB. Actual both-bank encodings, both baselines and human inspection
must fit before acceptance. Individually maximal collections need not fit together.

During the one-time upgrade only, each admitted replacement requires verification
of the original adjacent `.pre-v7.bak`. Different existing backups are protected.
Library reset and its marker commit together; profile/library replacement is not
a two-file transaction. An independently safe profile may reset while the shared
preset reset remains pending. Ordinary saves and notice acknowledgements use the
existing synced/closed temporary file and atomic replacement without new backups.

A notice's complete writer flush permits a strict metadata-only acknowledgement.
It proves a transmission opportunity, not an operator read. Failed acknowledgement
may repeat a historical notice; completed resets never repeat. Protected temporary
sessions cannot autosave over refused originals.

Stop writers before backups or restoration. Preserve current files before rollback,
then restore a matching old executable, profiles and shared library. This loses
later changes across affected SSIDs. Do not relabel versions or pair an older
writer with newer records. See [ADR-0330](../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md).

## Dedupe Policies

The telnet server exposes per-user dedupe policy control through `SHOW DEDUPE` and `SET DEDUPE`.

- new users default to `dedup.default_policy` from the active config directory; the shipped default is `SLOW`
- the selected policy is persisted per callsign
- `SHOW DEDUPE` reports the saved and ordinarily resolved policy, and whether `FAST`, `MED`, and `SLOW` are enabled server-side
- if a user requests a disabled policy, the server falls back to an enabled one and reports that in the response

Policy windows come from the active `dedup.secondary_*_window_seconds` values
in `data/config/dedupe.yaml`:

- `FAST`: shortest configured window, keyed by band + DE DXCC + DE grid2 + DX call
- `MED`: middle configured window, keyed by band + DE DXCC + DE grid2 + DX call
- `SLOW`: longest configured window, keyed by band + DE DXCC + DE CQ zone + DX call

This is why `SLOW` suppresses more repeats from one region than `FAST` or `MED`: CQ zone is broader than a 2-character grid square.

NEARBY never changes dedupe. Delivery, SHOW DEDUPE, settings readback and
SET DIAG DEDUPE use the ordinary resolved preference. Users retaining MED or SLOW
may see fewer local repeats than under the retired automatic FAST override.
Select SET DEDUPE FAST explicitly when that is desired and available.

### Frequency-change policy

The operator-owned `dedup.secondary_frequency_changes` settings are separate
from your FAST/MED/SLOW choice. They apply at startup and ship with `state: off`.

| State | What reaches your feed |
| --- | --- |
| `off` | Historical suppression; frequency and mode do not distinguish repeats. |
| `shadow` | The same spots and diagnostic tags as off; a separate candidate cache supplies comparison counters. |
| `on` | The candidate can admit reported frequency changes and separates skimmer modes. Human modes remain grouped together. |

Starting frequency tolerances are 500 Hz for skimmer CW/RTTY, 3000 Hz for
skimmer FT8/FT4/FT2, 1000 Hz for other skimmer modes, and 2000 Hz for humans.
A difference equal to tolerance remains a repeat. Each live entry allows up
to the configured number of ordinary frequency-change admissions, initially
two. Those admissions do not extend its window. A newly reported or stronger
representative still passes under the configured report policy at any
frequency; an outside-tolerance report exception moves the anchor and renews
the window without spending or replenishing the change allowance.

These rules recognize reported differences, not verified QSYs. Bad reports can
add repeats. Report renewal can also change expiry timing, so a spot admitted
by the historical engine may be suppressed and shown later by the candidate.
Earlier primary suppression, averaging, holds and user filters still apply.

In on, live `SET DIAG DEDUPE` uses a captured comparison anchor and canonical
mode identity from the spot's decision. Queued formatting does not look up a
newer cache anchor. Off/shadow tags and archived-history rendering remain
unchanged. The shared MED gate still controls archive and peer admission;
ring history can gain spots admitted by any enabled lane.

The on-mode tag begins with `MM@anchor`, where `MM` is the hexadecimal mode
byte and `anchor` is whole Hz (`-` means no existing anchor). Geography,
source and policy follow it. The normal fixed comment width can shorten the
later fields; the captured anchor is placed first.

Shadow counters reset per run and appear in periodic runtime summaries; they
do not provide example capture or prove usefulness. Activation requires later
review. See [required config and migration](../data/config/README.md#secondary-frequency-change-policy)
and [ADR-0334](../docs/decisions/ADR-0334-frequency-change-aware-secondary-dedupe.md).

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

Text and VE7CC configuration readbacks use the unconditional delivery/reading hold
described above. Later valid PAUSE/RESUME controls override queued readback
completion. All pause mutators share the same ordering authority, while YAML
operations do not mutate that authority.

Outside JSON mode, long command responses can temporarily pause live spot lines so users have time
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

The server's periodic heartbeat is an exact CRLF control message, configured by
`telnet.keepalive_seconds` (`0` disables it). It continues during a live-spot
pause and uses the existing full-control-queue disconnect policy. TCP keepalive
is a separate mechanism. See [TSR-0123](../docs/troubleshooting/TSR-0123-periodic-keepalive-wire.md).

When automatic pausing applies, the command response gets a footer showing the
effective remaining duration:

```text
Live spots paused for 30 seconds. Missed spots aren't replayed.
Type RESUME to restart now, or SHOW HOLD to check the pause.
```

Users can type `SHOW HOLD` to see remaining pause time and the suppressed spot
count. `RESUME` ends the pause immediately and discards any stale spot envelopes
queued before the resume point so old spots are not replayed.
`SHOW HOLD` accepts exactly two tokens, ignoring case and allowing repeated
ASCII spaces. Extra arguments return `Usage: SHOW HOLD` without changing pause
state. `SH HOLD` and slash forms are unsupported; TAB remains invalid input.
Replies to `PAUSE`, `SHOW HOLD`, and `RESUME`, including their handled usage
errors, do not trigger automatic pausing themselves. Suppression counters can
still increase independently when otherwise eligible live spots arrive.

## Grid, Noise, And Nearby

- `SET GRID` accepts exactly one valid 4 or 6 character Maidenhead locator,
  normalizes uppercase, and requires ordinary fine mapping before publishing.
  Invalid syntax or unavailable mapping leaves the grid unchanged. NEARBY adds
  no requirement that both fine and coarse mappings exist.
- `SET NOISE` stores the receiving noise class.
- `SET PATHSAMPLES` stores a stricter per-user path sample floor.
- `SHOW PROP <call|prefix|grid> [band] [mode]` uses the saved grid and noise.

NEARBY is an operand-free Boolean predicate, not a global switch:

```text
PASS RULE 0 MODE=CW AND (DXCONT=EU OR NEARBY)
REJECT RULE 9 NEARBY AND COMMENT="test"
```

The first selects CW with European DX or a nearby endpoint. The second vetoes
nearby spots whose comment contains test. Other PASS rules can qualify other
spots; slots are identifiers, not priorities. Geography, SOURCE, PATH and all
other conditions retain ordinary meaning. NEARBY=TRUE and old toggle commands
are invalid. COMMENT="NEARBY" is only literal text.

Either DX or DE can match the user's same H3 cell. An unavailable opposite
endpoint cannot veto a match. 160m, 80m and 60m use coarse resolution; other
supported bands use fine resolution. The current helper's fallback order and
cached endpoint behavior are retained exactly. Only an empty or ??? band after
fallback forces false; unsupported nonempty labels retain its existing fine-cell
behavior. A missing required user cell or no matching endpoint also makes the
predicate false. NOT NEARBY consequently includes unknown proximity; it does
not establish that the station is distant. Other OR branches can still qualify.

GRID, its explicit/derived provenance and two cached cells have one owner in
the client configuration. Rules capture that location consistently and evaluate
outside path/filter locks, independently of PATH or display enablement. Missing
resolutions are resolved separately from the captured GRID when needed. A capture
needing neither NEARBY nor PATH adds no mapping work. Prepared programs retain
no user location or cached proximity truth. Repeated reached NEARBY terms reuse
one evaluation-local result; unreached terms do no endpoint conversion.

Rules can be created or edited without a usable location. PUT, PATCH and VALIDATE
share a narrow recovery rule: a rules list must be supplied, and every other
saved setting and separate control must remain unchanged. Only mapping
availability for the unchanged GRID is waived; exact locator syntax, disabled
dedupe choices and all other settings checks remain enforced. Empty GRID retains
its existing lookup meaning. Unrelated edits preserve usable captured location
and do not rerun callsign lookup. The old independent NEARBY cell-pair contract
in TSR-0109 is retired by [ADR-0329](../docs/decisions/ADR-0329-nearby-boolean-predicate-and-generation-six.md).

History uses one detached matching snapshot per page. Matching and fingerprinting
use the same effective cells. True rule/location changes invalidate pending
pages and continuation, including change then restoration; lazy cache filling
that preserves effective geometry does not. Propagation observations remain
current and outside the configuration fingerprint. Own-call exceptions and
explicit history clauses retain their documented behavior.

RESET FILTER and RESET FILTER RULE clear the selected rules, including their
NEARBY terms, without changing settings or separate controls and without
reinstalling defaults. NEARBY never chooses a duplicate-suppression lane.

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

In GO, use numbered Boolean rules:

```text
PASS RULE 0 DXSTATE=CA,TX,ON,QC
REJECT RULE 9 DESTATE=AA,AE,AP
```

In CC, use native categories and inspect them directly:

```text
SET/FILTER DXSTATE/PASS CA,TX,ON,QC
SET/FILTER DOS/REJECT AA,AE,AP
SH/FILTER DXSTATE
SH/FILTER DOS
```

Each CC command replaces only its named category. Values within a list use OR;
enabled categories use AND. Unknown State fails PASS and survives REJECT.
CC per-band geography uses schema-seven YAML; its terminal syntax is unverified.
Boolean rule commands require GO.

Operands normalize to canonical case and accept 60 FCC codes plus AB, BC, MB,
NB, NL, NS, NT, NU, ON, PE, QC, SK, YT. They compare the spot's canonical
observed metadata; missing, invalid or noncanonical metadata is unknown.
In GO, `DXSTATE<>CA` is false for unknown State, but `NOT DXSTATE=CA` is true.
A reject-only rule set can allow unknown State because no rejection matched,
not because inequality evaluated true. Other GO PASS slots can broaden selection.
GO's NEARBY and State comparisons combine with ordinary Boolean logic.
SELF/history exceptions remain as described above.

State enrichment remains active with either source's license enforcement
disabled. Registered addresses can differ from operating location. The final
corrected DX base call determines DXSTATE, and central ingest determines DESTATE.
License lookup uses home/base-call CTY jurisdiction, not a portable operating
prefix. Canadian jurisdictions are ADIF 1/211/252; FCC enrichment uses the
explicit `spot.IsFCCJurisdiction` set, not just ADIF 291. Only available/found
registered-address evidence supplies State. ISED uses a club address when
club data exists, otherwise the personal address. Exact active special calls
use their listed ISED trustee; prefix substitutions use the ordinary assigned
base call. Blank or conflicting evidence leaves State unknown. Prefix matches
establish callsign plausibility, without proving event eligibility. Event dates
are inclusive UTC days; cached Canadian facts revalidate at UTC midnight.

Each registry has an independent snapshot and generation. Publication briefly
makes that source unknown; FCC rebuilding also suppresses FCC facts during its
import. State comparisons are then false, including `<>`; NOT negates that
result. A PASS requiring State does not qualify these spots, while a
State-only REJECT does not veto them. Failed builds retain the last good
snapshot. Refresh duration depends on installation and source size; see
[FCC evidence](../docs/fcc-state-validation.md#full-sample) and
[Canadian evidence](../docs/canadian-state-validation.md).

Archive version 6 stores the observed State values. Versions 2–5 remain readable
with unknown State; history never consults current registry addresses.
Configuration separately uses schema seven/native disk v7 and a limited schema-six GO interface, with no older-format
conversion. Use matching backups for downgrade.
