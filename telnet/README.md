---
title: Telnet Surface
description: Login, command, configuration and delivery contracts for telnet clients.
---

This directory owns the telnet session layer: login flow, prompt handling, filter commands, client fan-out, and path display.

## Login And Session Behavior

- Clients log in with a callsign before commands are accepted.
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
- Callsign validation does not authenticate the person connecting. An accepted
  same-identity login can displace an existing session.
- The greeting can include dialect, grid, noise, and dedupe status from config.
- Dialect choice and filter state are persisted per callsign.
- If `NEARBY` is active, the login greeting warns the user.

Restoration uses the full login call, including its SSID. Configuration,
associated preset name and its reference snapshot stay consistent through
replacement of an older connection. An old session cannot overwrite the new
session's record, including through a save already underway during reconnect.
The no-write guarantee above applies to capacity rejection. Other login
failures retain their existing persistence behavior.

If a valid record is restored but updating the login timestamp/IP fails, login
continues with those preferences and a warning.

Login upgrades recognized unversioned and version 1–4 profiles once. Old
personal filters, NEARBY and the applied-preset association are reset. The
replacement is an ordinary editable PASS rule in slot 0 using configured
default modes and sources, identical to a new user's starting rule. Saved settings, separate
controls and login metadata are retained; the login reports the reset.
Old shared preset entries are removed without deleting already-created v5
presets. Original files are backed up before replacement. Existing v5 profiles,
including empty rule sets, are never reset again. This does not revive old
category commands or accept old machine YAML schemas.

If the record itself is unreadable, unsafe or uses an unrecognized version,
or its required upgrade cannot be committed, login uses protected temporary
defaults without overwriting that record. Human preference changes are
temporary; readbacks remain available. SAVE PRESET is rejected before any
library write, and LOAD PRESET and PUT/PATCH cannot commit configuration.
LIST/DELETE retain their separate preset-library behavior. File replacements
are atomic individually, not one cross-file transaction; upgrade failures must
be reported and retried safely without overwriting the original backups.
Original backups use `.pre-v5.bak` beside the user record or preset library.
A different existing backup is never overwritten. A library-only cleanup
failure on an already-valid v5 profile warns without replacing that user's
settings or blocking unrelated preference saves.

Raw and normalized user records must each fit 1 MiB. The proposed saved record,
including login metadata and the applied baseline, must also fit 1 MiB.
Preferences and each baseline must fit 256 KiB when encoded as a standalone
preset. A small file can still be refused for expensive alias expansion; these
processing limits are separate from file sizes. Refused files are preserved.
Human changes are admitted before becoming live. If a connected user's file
becomes unreadable, fitting changes remain temporary with a warning. Later saves
and ordinary logout retry a repaired file using its authoritative metadata.
Sessions that started with protected defaults keep that protection until logout.

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

## Filters

This describes the Boolean filter MVP in the current local checkout, not a
production deployment or a claim that new source files are published on main.
Private local-cluster acceptance remains pending. The telnet layer owns command
admission and publication; personal selection uses one prepared Boolean engine.

Both `go` and `cc` use the same numbered-rule commands:

```text
PASS RULE <0..9> <expression>
REJECT RULE <0..9> <expression>
RESET FILTER RULE <0..9|ALL>
RESET FILTER
SHOW FILTER [RULE|FULL]
HELP RULE
```

Each occupied slot holds one action and one complete expression. Setting a slot
replaces both; it is not a delta or an addition to a category list.

- Any matching REJECT vetoes delivery. Otherwise any active PASS can qualify.
- With no active PASS, personal selection is unrestricted except for matching
  REJECTs. With no rules there is no personal restriction; separate controls and
  NEARBY still apply.
- Slot IDs are not priorities. Adding another PASS broadens selection. To narrow
  a selection, edit or remove its existing PASS, including default slot 0.
- `RESET FILTER RULE` clears the selected slot or all slots, preserving NEARBY.
  `RESET FILTER` clears all personal rules and turns NEARBY OFF. Neither resets
  separate controls or profile settings, and neither reinstalls defaults.
- New accounts and recognized old-profile resets receive the same validated
  configured MODE/SOURCE selection as an ordinary editable PASS in slot 0.
  Both sources are written explicitly as SOURCE=HUMAN,SKIMMER.
  Existing/restored Boolean empty rules remain empty.
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
Old category, MODE-delta, MINSNR, COMMENT-list, DXBM, NOFILTER and CC filter/mode
shortcut mutations are refused without translation. There is no `RESET RULE`
or `SET DX FILTER` alias. `HELP RULE` is the shared expression reference.

Expressions support comparisons, parentheses, NOT, AND and OR, with precedence
`NOT > AND > OR`. Equality matches any value in a bare or bracketed comma list,
for example `MODE=CW,FT8` or `MODE=[CW,FT8]`. `<>` means none of the listed values
only when the field is known. Every comparison on a missing/unknown value is
false, including `<>`; NOT negates that result. There are no generic
ALL/ON/OFF wildcard operands.

| Fields | Values and matching |
| --- | --- |
| `BAND`, `MODE` | Active supported bands and taxonomy modes/aliases; blank MODE is the known token `UNKNOWN`. |
| `SOURCE` | `HUMAN` when `Spot.IsHuman` is true; otherwise `SKIMMER`. |
| `EVENT` | Taxonomy event bits, with `NONE` for eventless spots; no untagged exemption. |
| `DXCALL`, `DECALL` | Exact callsign patterns, one prefix/suffix `*`, or `*` alone; see below. |
| `DXCONT`, `DECONT` | Supported continent codes. |
| `DXZONE`, `DEZONE` | Integral CQ zones 1–40. |
| `DXDXCC`, `DEDXCC` | Positive ADIF integers or exact CTY canonical labels; labels normalize to numeric codes. No callsign lookup. |
| `DXSTATE`, `DESTATE` | Canonical observed US/Canadian registered mailing codes; missing/invalid metadata is unknown. |
| `DXGRID2`, `DEGRID2` | Two-letter Maidenhead fields, `AA` through `RR`. |
| `FREQ` | Finite nonnegative operands in kHz; `=`/`<>` also accept inclusive low-high ranges. Spot frequency is known only when finite and positive. No extra rounding or epsilon is added. |
| `SNR` | Integral report whenever `Spot.HasReport` is true, including zero and human reports. |
| `CONF` | `?`, `S`, `C`, `P`, `V`, `B`; unknown for confidence-exempt modes, with no expression bypass. Otherwise blank confidence becomes known `?`. `CONFIDENCE` is not a field alias. |
| `PATH` | Exact `HIGH`, `MEDIUM`, `LOW`, `UNLIKELY`, `CLOSED`, `INSUFFICIENT`; absent/invalid prediction is `INSUFFICIENT`. |
| `COMMENT` | ASCII case-insensitive literal substring of the stored cleaned comment; see below. |

Only FREQ, SNR, DXZONE and DEZONE accept `<`, `<=`, `>` and `>=`; ordered
comparisons require one value. `PATH=UNLIKELY` does not include CLOSED; use
`PATH=UNLIKELY,CLOSED` to select both. EVENT remains family-level: POTA and
POTA-1234 set the same family bit, while the reference stays in the comment.
Non-COMMENT operands are case-insensitive and normalized. Configured
MODE/EVENT names and mode aliases containing `_` work in both dialects; login
callsigns and machine request IDs retain their stricter rules.

DXCALL reads `DXCall`; DECALL prefers `DECallStripped`, falling back to `DECall`.
Patterns use ASCII letters, digits, `/` or `-`, with at most one `*` at either
end: `K1ABC`, `W1*`, `*ABC`, or `*`. Internal/multiple stars and `?` wildcards
are invalid. A lone star does not match an unknown empty call.

Separate controls are not expression fields. In both dialects,
`PASS/REJECT <BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC> [ALL]` enable/disable the
named control. PC93 aliases ANNOUNCE. These switches accept no value or one
`ALL`; `PASS BEACON OFF` is refused. `PASS NEARBY ON|OFF` and
`REJECT NEARBY ON|OFF` follow the explicit value, not the verb.
CC `SET/FILTER` and `UNSET/FILTER` retain these controls only. Inline
`SET/FILTER <switch>/ON|/OFF` takes no extra values; UNSET has no inline flags
and NEARBY has none. Named CC control shortcuts such as `SET/BEACON` and
`SET/SELF` take no arguments; old category shortcuts are not supported.

Live self-spots require SELF enabled and bypass personal rules and NEARBY, but
still honor TOXIC. Command/history self matches retain their exception
independently of SELF, also honoring TOXIC. Explicit history BAND/MODE/COMMENT
clauses are mandatory even for self-spots.

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
response limits, stored-byte limits and decode-work budgets. Prepared full
and NEARBY programs are retained immutably; edits prepare replacements without
changing snapshots already in use. Live, command and history matching capture
coherent controls, cells and path inputs with those rules, without another
category gate. Rendering reads prepared programs; it does not compile them.

Current checkout source routes: `filter/rule_set.go` owns preparation and
combination, `rule_compile.go`/`rule_evaluate.go`/`rule_view.go` own field
semantics, and `rule_set_projection.go` owns NEARBY projection.
`telnet/boolean_input.go`, `boolean_rule_commands.go` and
`boolean_rule_readback.go` own human admission/display;
`matching_snapshot.go` and `history_filter.go` capture matching state.
`commands/boolean_rule_help.go` supplies the registered HELP RULE reference.

`PASS TOXIC` shows spots classified as toxic; `REJECT TOXIC` hides only spots
with classifier status `TOXIC`.

Toxic comment filtering is optional and fail-open. When the classifier is
disabled, unavailable, timed out, or has not seen a human comment yet, the spot
passes `REJECT TOXIC`. Routine comments that match the local ham-radio safe
gate are marked `SAFE_LOCAL` and also pass. The classifier receives only the
cleaned free-text comment, not callsigns, band, mode, source, IP, session data,
or archive records.

`SHOW FILTER` reports the current `TOXIC` toggle. The toggle applies to live
spots and archive-backed history queries. Local self-spot bypasses still honor
`REJECT TOXIC` once a spot is classified as `TOXIC`.

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
searched, not removed mode/report/time tokens, synthetic beacon fallbacks or
per-user diagnostics. Unknown/empty comments follow ordinary unknown comparison
semantics. Rules combine as described under [Filters](#filters).

The human RULE reader preserves expression bytes, including literal case and
spaces; compilation normalizes syntax and non-COMMENT operands. The configured
whole-command ceiling still applies (128 bytes by default). Use schema 5 YAML
for longer expressions. The [rule budgets](#filters) replace old per-list limits.
There are no COMMENT move/remove/list-reset commands.

Manual DX comments accept ASCII letters, digits, spaces, and `/ # @ - . , * ? _`.
Other punctuation and non-ASCII characters are rejected before posting. The
whole edited DX command uses the node's command-byte limit (128 by default),
excluding its line ending. Mode/report tokens may be parsed from the comment.

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

Rules survive reconnects and presets through the [schema 5](#client-yaml-configuration)
and [native v5](#persistence-format-and-rollback) contracts. Older COMMENT-list
mechanics in [ADR-0261](../docs/decisions/ADR-0261-literal-comment-filter-and-history.md)
are historical, not a supported compatibility engine.

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
refuse the entire edit, even with NEARBY ON.

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
the live WWV/WCY filter settings. Later announcements follow those filters.
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

`SHOW DX` and `SHOW MYDX` share archive history selection and paging. Counts
range from 1 to 250, default 50; selector and count may appear in either order.
`SH DX` / `SH MYDX` work in both dialects, while `SHOW/DX` / `SH/DX` are `cc`
aliases. An unambiguous canonical CTY label selects its entity first. Other full
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
| `SHOW FILTER` | Current settings, saved/effective rules and individual controls. |
| `SHOW FILTER RULE` | The same concise filter state. |
| `SHOW FILTER FULL` | Adds geometry detail and preparation counts. |
| `SHOW SETTINGS` | Configured preferences and effective behavior, followed by session status. |

`SHOW/FILTER` and `SH/FILTER` accept the same RULE/FULL arguments in both
dialects; their HELP topics do too. Category readbacks are unsupported.
Every occupied slot appears in slot order with its action and full canonical
saved expression. No synthetic source condition is added.
While NEARBY is ON, each also shows its projected expression or that
a geography-only rule is suspended. Full saved expressions remain editable.
FULL counts distinguish occupied slots and full, nearby and retained
nodes/atoms/values; shared operands count once in retained values.

The view also reports NEARBY geometry (fine/coarse cells available, partly
usable or unavailable) and BEACON, WWV, WCY, ANNOUNCE/PC93, SELF and TOXIC as
separate ON/OFF/DEFAULT controls. There are no hidden category/base restrictions.
The [NEARBY recovery contract](#grid-noise-and-nearby)
apply when restored geometry is unavailable. Existing SELF/TOXIC and
command/history exceptions remain distinct from ordinary rule matching.

The shipped login greeting includes a settings/filter block. New accounts see
"Default settings and filters"; returning accounts see "Current settings and
filters". Values come from actual restored/prepared state, including a looked-up
grid and effective dedupe. Recognized old profiles get the reset notice and
first-time guidance; old-library cleanup alone must not claim a profile reset.
No display flattens OR/NOT expressions into independent mode/band/source lists.
Custom greeting templates can place the block with `<SETTINGS_FILTERS>`.

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
and explain that SLOW is unavailable. Usable NEARBY can temporarily change the
effective policy. If all secondary policies are disabled, the readback says
duplicate suppression is disabled. Reading never replaces your saved choice.
Unusual saved strings remain exact, with quotes and escapes when needed;
when describing an effective lane, unrecognized values are identified separately
from unavailable policies.

Use `GET YAML FILTER SCHEMA 5` for machine-readable saved expressions and
controls; `DEFAULT` remains distinct from explicit `true` or `false`.
Configured NEARBY and geometry usability are separate facts. `SHOW SETTINGS`
preserves exact saved GRID bytes even when they require escaped display.
No successful filter readback omits a saved rule, including a suspended rule.

Every human readback line has at most 78 printable ASCII characters followed
by CRLF, including headers, active values, explanations, errors and pause
footers. Short numbered rules use directly reusable expression syntax, without
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
still have the configured line limit (128 bytes by default); use schema 5
YAML for longer rules.

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

Every human readback, including a size error, always suppresses live spots
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
Live spots paused during delivery and for at least 30s afterward.
Type RESUME when ready. Missed spots are not replayed.
```

Each complete response is limited to 65,536 bytes after CRLF conversion,
including its header and human footer. Generation either completes within
the limit or returns an explicit error. ASCII-escape expansion is budgeted
before unrestricted quoting. Exact-value scratch space is bounded by the line
width, and the complete rule body is admitted before output allocation.

## Client YAML Configuration

The following canonical commands work independently of the human dialect:

| Command | Purpose |
| --- | --- |
| `GET YAML FILTER SCHEMA 5 [ID <id>]` | Read saved numbered rules, NEARBY and separate controls. |
| `GET YAML SETTINGS SCHEMA 5 [ID <id>]` | Read all six writable settings. |
| `GET YAML CONFIG SCHEMA 5 [ID <id>]` | Read filters and settings in one consistent snapshot. |
| `GET YAML CAPABILITIES SCHEMA 5 [ID <id>]` | Discover schema, rule fields/operators/vocabularies and bounds. |
| `PUT YAML FILTER`, `PUT YAML SETTINGS`, `PUT YAML CONFIG` | Replace the complete writable resource. |
| `PATCH YAML FILTER`, `PATCH YAML SETTINGS`, `PATCH YAML CONFIG` | Change supplied fields, preserving omissions. |
| `VALIDATE YAML CONFIG` | Check a complete proposal without applying or saving it. |

GET replies contain `schema_version`, `request_id`, `resource`, an opaque
`revision`, `configuration` and read-only `status`. GET assigns an ID if it is
omitted; a supplied ID preserves case and must contain 1-32 ASCII letters,
digits or hyphens. Every GET, including SETTINGS and CAPABILITIES, requires
explicit `SCHEMA 5` before its optional ID. Missing, older and unsupported
schemas are refused, not translated.

FILTER's writable `configuration` directly contains the filter fields;
SETTINGS directly contains setting fields. CONFIG contains `filters` and
`settings` mappings. Extract that writable section when creating a new upload;
do not send the GET envelope's resource, revision or status fields as writable
data. Preset association, effective choices, server defaults, diagnostics and
pause state cannot be set through a configuration upload. The preset reference
snapshot itself is not exposed in GET.
`status.effective.nearby_active` means NEARBY is enabled with both user cells
available. It does not replace the saved `nearby_enabled` value or full rules.
The [restored-geometry recovery contract](#grid-noise-and-nearby), its
SELF/history exceptions apply.
CAPABILITIES is a read-only discovery resource; its `configuration` section
describes the protocol rather than writable preferences.

PUT/PATCH/VALIDATE headers have exactly three words and no schema suffix.
Their bodies require `schema_version: 5`. VALIDATE supports CONFIG only.
Old body schemas and category fields are errors; there is no hidden-map
preservation or older-schema success path.

CAPABILITIES advertises `supported_schema_versions: [5]`, the eight filter and
six settings field names, lowercase rule actions, DEFAULT/boolean choices,
rule fields/operators/geographic flags, active vocabularies and rule limits.
It also reports upload/response bounds, complete-PUT, collection-replacement
and revision requirements. It is not the former per-server settings-choice
availability matrix: runtime settings admission remains authoritative.
Read-only `status.rules` reports configured/effective/suspended counts,
per-slot effective expressions/suspension and full/nearby/retained
node/atom/value counts, not MINSNR or COMMENT-map status.

Current checkout routing is `telnet/machine_commands.go` to
`handleV5MachineCommand`; `machine_v5_input.go` owns headers,
`machine_v5_schema.go`/`machine_v5_schema_rules.go` own strict proposals,
`machine_v5_commands.go` owns transactions and `machine_v5_readback.go` owns
the bounded DTO response. Retained prepared rules are shared immutably when
omitted from PATCH, not recompiled by rendering.

### Writable Values

| Resource fields | Representation |
| --- | --- |
| `rules` | Sequence of mappings requiring `slot` (integer 0–9), lowercase `action: pass` or `action: reject`, and `expr` (complete expression string). Duplicate slots fail. |
| `include_beacons`, `allow_wwv`, `allow_wcy`, `allow_announce`, `allow_self`, `allow_toxic` | Explicit `true`, explicit `false` or the string `DEFAULT`. |
| `nearby_enabled` | Boolean. |
| `dialect`, `grid`, `noise_class`, `dedupe_policy` | Strings; an empty string preserves the explicit default/lookup selection. |
| `path_min_observation_count` | Integer; zero uses the cluster minimum. |
| `solar_summary_minutes` | Integer: 0, 15, 30 or 60; zero means OFF. |

FILTER contains exactly `rules`, `nearby_enabled` and the six controls listed
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
`rules: []` clears all slots while omitted NEARBY and controls remain unchanged.
There is no per-slot PATCH merge.

### Upload And Revision Examples

These are source-derived, codec-shaped examples, not executed server or codec
validation results. First read your own current configuration:

```text
GET YAML CONFIG SCHEMA 5 ID Read-1
```

This separate request validates a complete CONFIG without applying/saving it:

```text
VALIDATE YAML CONFIG
---
schema_version: 5
request_id: Validate-1
configuration:
  filters:
    rules:
      - slot: 0
        action: pass
        expr: 'MODE=CW,FT8 AND SOURCE=HUMAN'
      - slot: 9
        action: reject
        expr: 'COMMENT="CQ ""DX"""'
    nearby_enabled: false
    include_beacons: DEFAULT
    allow_wwv: true
    allow_wcy: false
    allow_announce: DEFAULT
    allow_self: DEFAULT
    allow_toxic: DEFAULT
  settings:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
...
```

For `PUT YAML CONFIG`, use the same complete shape and additionally supply
`if_revision` from the current GET. FILTER and SETTINGS place their fields
directly under `configuration`, without a `filters` or `settings` wrapper.
A complete settings replacement is:

```text
PUT YAML SETTINGS
---
schema_version: 5
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

CONFIG can change a dependent setting and filter together:

```text
PATCH YAML CONFIG
---
schema_version: 5
request_id: nearby-Ab1
if_revision: "replace-with-current-GET-revision"
configuration:
  filters:
    nearby_enabled: true
  settings:
    grid: FN42
...
```

Enabling NEARBY or changing GRID while ON requires usable geometry. For an
already-ON restored configuration, rule-only recovery has the contract and
source limitation described under [NEARBY](#grid-noise-and-nearby).
A PATCH can clear every personal rule while preserving NEARBY and controls:

```text
PATCH YAML FILTER
---
schema_version: 5
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
`if_revision` and `configuration`. Uploads require `schema_version: 5`,
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
be repaired without resetting solar timing, NEARBY geometry, diagnostics
or pause. A lost acknowledgement does not undo a committed write; GET again
to discover the current revision and configuration.

A fully received unavailable-choice request gets a framed error, for example:

```yaml
---
schema_version: 5
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
and derived caches. Native v5 snapshots preserve configured values and full
rules, including geography omitted from NEARBY's effective projection. LOAD uses
server restrictions: disabled dedupe policies use the enabled-policy fallback, and a
path sample override only applies above the current cluster minimum. Grid/H3
cells are rebuilt, not a location-filter restoration snapshot. Stored NEARBY
remains enabled but unavailable with a warning when usable cells are absent;
the [recovery contract](#grid-noise-and-nearby) retains the existing self exceptions.
LOAD restarts the chosen SOLAR schedule at the next wall-clock-aligned tick;
enabled nodes send summaries only when conditions are active.

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

SAVE prepares and admits both the library and SSID association before writing
either. Admission failure changes neither file. SAVE has an explicit
partial-success outcome when the admitted library write succeeds but the later
SSID association write fails:

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

User records, named preset entries and applied baselines require explicit
`configuration_version: 5` and nested `configuration.filters/settings`.
Strict codecs refuse older formats; login separately performs the recognized
unversioned/version 1–4 reset described above. Current library entries and
baselines must satisfy v5. Explicit-zero, malformed, future or unsafe documents
remain protected. An encoder's zero version for a newly constructed in-memory
DTO is not supported disk version 0. There is no category converter,
marker-only upgrade or compatibility engine. Reset originals have adjacent
`.pre-v5.bak` backups; existing v5 records and new presets are not reset again.

Native records are not machine envelopes: they have no `schema_version`,
`request_id` or `if_revision`. A codec-shaped record with an optional applied
reference can look like this (illustrative, not executed validation):

```yaml
configuration_version: 5
configuration:
  filters:
    rules:
      - {slot: 0, action: pass, expr: 'MODE=CW AND SOURCE=HUMAN'}
    nearby_enabled: false
    include_beacons: DEFAULT
    allow_wwv: DEFAULT
    allow_wcy: DEFAULT
    allow_announce: DEFAULT
    allow_self: DEFAULT
    allow_toxic: DEFAULT
  settings:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
preset:
  name: CONTEST
  baseline:
    configuration_version: 5
    configuration:
      filters:
        rules: []
        nearby_enabled: false
        include_beacons: DEFAULT
        allow_wwv: DEFAULT
        allow_wcy: DEFAULT
        allow_announce: DEFAULT
        allow_self: DEFAULT
        allow_toxic: DEFAULT
      settings:
        dialect: ""
        grid: ""
        noise_class: ""
        dedupe_policy: ""
        path_min_observation_count: 0
        solar_summary_minutes: 0
recent_ips:
  - 192.0.2.1
last_login_utc: 2026-10-21T18:00:00Z
```

The retained baseline may differ from the current configuration and is not
looked up afresh in the library. Login/IP metadata stays outside configuration.
A library has a `presets` mapping, with a version on each named entry, not a
top-level library version:

```yaml
presets:
  CONTEST:
    configuration_version: 5
    configuration:
      filters:
        rules:
          - {slot: 0, action: pass, expr: 'DXSTATE=NY,ON'}
        nearby_enabled: false
        include_beacons: DEFAULT
        allow_wwv: DEFAULT
        allow_wcy: DEFAULT
        allow_announce: DEFAULT
        allow_self: DEFAULT
        allow_toxic: DEFAULT
      settings:
        dialect: ""
        grid: ""
        noise_class: ""
        dedupe_policy: ""
        path_min_observation_count: 0
        solar_summary_minutes: 0
```

The native runtime record/configuration/preset wrappers use the v5 codecs in
`filter/storage_v5_io_adapter.go`, `storage_v5.go`, `storage_v5_decode.go` and
`preset_v5.go`. Those are current checkout routes, not published-main claims.
Raw, normalized and prospective user records (including metadata/baseline)
must each fit 1,048,576 bytes. Each standalone configuration/preset/baseline
must fit 262,144 bytes in preset form; libraries allow 20 names and
8,388,608 bytes. Shared physical-plus-logical decode work is separately bounded
at 2,097,152 per record and 16,777,216 per collection. Decode work is not file
size or retained heap bytes. Refusal preserves the original document.

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
`SHOW HOLD` accepts exactly two tokens, ignoring case and allowing repeated
ASCII spaces. Extra arguments return `Usage: SHOW HOLD` without changing pause
state. `SH HOLD` and slash forms are unsupported; TAB remains invalid input.
Replies to `PAUSE`, `SHOW HOLD`, and `RESUME`, including their handled usage
errors, do not trigger automatic pausing themselves. Suppression counters can
still increase independently when otherwise eligible live spots arrive.

## Grid, Noise, And Nearby

- `SET GRID` accepts exactly one valid 4 or 6 character Maidenhead locator,
  normalizes it to uppercase, and stores it for path reliability. Trailing text
  and extra arguments are refused. Valid syntax with unavailable mapping gets
  an availability response and leaves the grid unchanged. If NEARBY is on, both
  resulting user cells must be usable before a grid change is published.
- `SET NOISE` stores the user's noise class; the checked-in path config applies one receive-side penalty per class
- `SET PATHSAMPLES` stores a stricter per-user path sample floor; it cannot lower the cluster default
- `SHOW PROP <call|prefix|grid> [band] [mode]` uses the saved grid and noise
  class to show a point-to-point VOACAP outlook; omitted mode defaults to CW
- `PASS NEARBY ON` requires a grid and keeps spots whose DX side or DE side falls in the user's nearby area

`NEARBY` uses H3 cells:

- coarse resolution on `160m`, `80m`, and `60m`
- finer resolution on the other supported bands

DX and DE are checked independently. A matching side still passes when the
other side has no grid, an invalid grid, or no mapping. If neither side matches,
the spot is rejected. An unavailable user cell or an unknown band also rejects.
Other active filters still apply. See
[NEARBY endpoint troubleshooting](../docs/troubleshooting/TSR-0054-nearby-opposite-endpoint-veto.md).

While `NEARBY` is active:

- effective expressions structurally omit only geographic leaves:
  `DXGRID2`, `DEGRID2`, `DXCONT`, `DECONT`, `DXZONE`, `DEZONE`, `DXDXCC`,
  `DEDXCC`, `DXSTATE` and `DESTATE`
- removal is not substitution with true or false: an emptied subtree disappears,
  a surviving single AND/OR child replaces its parent, and NOT disappears if
  its child empties; an entirely emptied rule contributes neither PASS nor REJECT
- BAND, MODE, calls, SOURCE, EVENT, COMMENT, FREQ, SNR, CONF and PATH survive
- full saved expressions remain intact and editable, including geographic atoms
- `SHOW FILTER`, RULE and FULL show saved/effective rules, suspension and geometry
- spot delivery uses the least-suppressive available dedupe policy while usable grid-backed cells are present

For example, `DXCONT=EU OR MODE=CW` projects to `MODE=CW`, not unrestricted
true; a rule containing only `DXCONT=EU` is ignored while ON. OFF evaluates
the latest full saved expressions, including edits made while ON, not a
pre-ON restoration snapshot.

`RESET FILTER RULE <slot|ALL>` clears only those rules and preserves NEARBY.
`RESET FILTER` clears all rules and turns NEARBY OFF, without changing GRID,
other settings or separate controls. Neither restores new-user defaults.
NOFILTER forms are refused, not translated.

If NEARBY is already ON with unavailable restored
GRID/cells, human and YAML rule-only edits are allowed when every other saved
selection is unchanged. Retain the saved GRID and NEARBY ON.
With unchanged GRID, preserve each existing NEARBY cell exactly, including
unavailable cells; do not substitute path-cache cells.
When both cells
are unavailable, ordinary spots are blocked until geometry is repaired or
NEARBY is OFF. This does not override live
SELF's enabled-only bypass or command/history's SELF-independent self-match
exception; both still honor TOXIC, and explicit history clauses still apply.
Enabling NEARBY or changing GRID while ON still requires usable geometry.

The rule-only YAML recovery exception bypasses only mapping availability for
the unchanged saved GRID, not malformed GRID syntax or other settings checks.
Existing human control/settings admission is unchanged. Partly usable geometry
retains band-specific behavior: bands whose required user cell is available may
work; the other bands reject. `TestGridMappingAvailability` exercises actual
PATCH, PUT and VALIDATE with an explicit saved GRID and no mappings in a fresh
child process. Private-cluster acceptance remains pending.

NEARBY state is persisted. On login the greeting warns when it is active;
unusable restored geometry leaves it ON, with full rules retained and effective
geographic projection still selected. The recovery behavior above applies.

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

Use the shared numbered-rule grammar in either dialect:

```text
PASS RULE 0 DXSTATE=CA,TX,ON,QC
REJECT RULE 9 DESTATE=AA,AE,AP
```

Operands normalize to canonical case and accept 60 FCC codes plus AB, BC, MB,
NB, NL, NS, NT, NU, ON, PE, QC, SK, YT. They compare the spot's canonical
observed metadata; missing, invalid or noncanonical metadata is unknown.
`DXSTATE<>CA` is false for unknown State, but `NOT DXSTATE=CA` is true.
A reject-only rule set can allow unknown State because no rejection matched,
not because inequality evaluated true. Other PASS slots can broaden selection.
NEARBY projects out State atoms while retaining editable full rules; OFF uses
the latest full rules. SELF/history exceptions remain as described above.

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
Configuration is separately schema 5/native disk v5, with no older-format
conversion. Use matching backups for downgrade.
